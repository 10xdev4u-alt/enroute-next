//! The Raft transport between ledger nodes: the `Raft` gRPC service
//! (`proto/raft.proto`).
//!
//! All frames for all logs going to the same peer are coalesced into one
//! `Deliver` call per batch, the way `TiKV` batches its Raft messages; each
//! peer has one HTTP/2 connection, kept open and re-established by tonic.
//! A frame is a raft-rs message (encoded by raft-rs, which uses an older
//! prost), a wake-up for a quiet log, or an announcement of an incarnation.

use std::collections::HashMap;
use std::sync::Weak;
use std::time::Duration;

use anyhow::{Context, Result};
use parking_lot::Mutex;
use raft::prelude::Message;
use raft_prost::Message as _;
use tokio::sync::mpsc;
use tonic::transport::Channel;

use enroute_shared::proto::Record;
use enroute_shared::registry::Addresses;

use crate::proto::{self, frame, raft_client::RaftClient, raft_server};

use super::engine::{Engine, Input};
use super::log_id;

pub(crate) enum Frame {
    Raft(u64, Box<Message>),
    Wake(u64),
    /// Incarnation `epoch` of repository `repo` is hosted; with its initial
    /// state when it was just created.
    Host(String, u64, Option<Box<Record>>),
}

impl Frame {
    fn into_proto(self) -> proto::Frame {
        let (log, kind) = match self {
            Frame::Raft(log, m) => (log, frame::Kind::Raft(m.encode_to_vec().into())),
            Frame::Wake(log) => (log, frame::Kind::Wake(proto::Wake {})),
            Frame::Host(repo, epoch, state) => (
                log_id(&repo, epoch),
                frame::Kind::Host(proto::Host {
                    repo,
                    epoch,
                    state: state.map(|s| *s),
                }),
            ),
        };
        proto::Frame {
            log,
            kind: Some(kind),
        }
    }

    fn from_proto(f: proto::Frame) -> Result<Frame> {
        Ok(match f.kind.context("frame without a kind")? {
            frame::Kind::Raft(m) => Frame::Raft(f.log, Box::new(Message::decode(m.as_ref())?)),
            frame::Kind::Wake(_) => Frame::Wake(f.log),
            frame::Kind::Host(h) => Frame::Host(h.repo, h.epoch, h.state.map(Box::new)),
        })
    }
}

/// Serves `Raft` for this node's engine.
#[derive(Debug)]
pub struct RaftService(pub(super) Weak<Engine>);

#[tonic::async_trait]
impl raft_server::Raft for RaftService {
    async fn deliver(
        &self,
        req: tonic::Request<proto::Frames>,
    ) -> Result<tonic::Response<proto::Delivered>, tonic::Status> {
        let engine = self
            .0
            .upgrade()
            .ok_or_else(|| tonic::Status::unavailable("consensus engine stopped"))?;
        let frames = req
            .into_inner()
            .frames
            .into_iter()
            .map(Frame::from_proto)
            .collect::<Result<Vec<_>>>()
            .map_err(|e| tonic::Status::invalid_argument(format!("{e:#}")))?;
        engine.receive(frames);
        Ok(tonic::Response::new(proto::Delivered {}))
    }
}

/// The `Raft` service for `engine`, to mount on the node's server.
pub(super) fn server(engine: Weak<Engine>) -> raft_server::RaftServer<RaftService> {
    raft_server::RaftServer::new(RaftService(engine))
        .max_decoding_message_size(enroute_shared::proto::MAX_MESSAGE)
}

/// Sends frames to peers; one background sender per peer coalesces
/// whatever is queued into a single call. Peers are addressed by id,
/// resolved through the node registry.
pub(super) struct Transport {
    addresses: Addresses,
    senders: Mutex<HashMap<u64, mpsc::UnboundedSender<Frame>>>,
    unreachable: Unreachable,
}

impl std::fmt::Debug for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transport").finish_non_exhaustive()
    }
}

/// Told which (peer, logs) could not be delivered.
type Unreachable = mpsc::UnboundedSender<Input>;

impl Transport {
    pub(super) fn new(addresses: Addresses, unreachable: Unreachable) -> Transport {
        Transport {
            addresses,
            senders: Mutex::default(),
            unreachable,
        }
    }

    pub(super) fn send(&self, to: u64, frame: Frame) {
        let mut senders = self.senders.lock();
        let tx = senders.entry(to).or_insert_with(|| {
            let (tx, rx) = mpsc::unbounded_channel();
            tokio::spawn(sender(
                self.addresses.clone(),
                to,
                rx,
                self.unreachable.clone(),
            ));
            tx
        });
        // A closed receiver means the sender task ended; the frame is lost.
        drop(tx.send(frame));
    }
}

/// A client for `url`, connecting lazily (and again after failures).
fn client(url: &str) -> Result<RaftClient<Channel>> {
    let channel = Channel::from_shared(url.to_string())?
        .connect_timeout(Duration::from_secs(2))
        .connect_lazy();
    Ok(RaftClient::new(channel).max_encoding_message_size(enroute_shared::proto::MAX_MESSAGE))
}

async fn sender(
    addresses: Addresses,
    peer: u64,
    mut rx: mpsc::UnboundedReceiver<Frame>,
    unreachable: Unreachable,
) {
    // The peer's address, and a client for it (rebuilt if it moves).
    let mut conn: Option<(String, RaftClient<Channel>)> = None;
    while let Some(first) = rx.recv().await {
        let mut batch = vec![first];
        while let Ok(f) = rx.try_recv() {
            batch.push(f);
            if batch.len() >= 4096 {
                break;
            }
        }
        let mut logs: Vec<u64> = batch
            .iter()
            .filter_map(|f| {
                if let Frame::Raft(l, _) = f {
                    Some(*l)
                } else {
                    None
                }
            })
            .collect();
        let url = addresses.read().get(&peer).cloned();
        if let Some(url) = &url
            && conn.as_ref().is_none_or(|(u, _)| u != url)
        {
            conn = client(url).ok().map(|c| (url.clone(), c));
        }
        let ok = match &mut conn {
            // Not (yet) in the registry, or a malformed address.
            None => false,
            Some((_, c)) => {
                let frames = proto::Frames {
                    frames: batch.into_iter().map(Frame::into_proto).collect(),
                };
                let mut req = tonic::Request::new(frames);
                req.set_timeout(Duration::from_secs(5));
                c.deliver(req).await.is_ok()
            }
        };
        if !ok {
            logs.sort_unstable();
            logs.dedup();
            // A closed receiver means the engine stopped.
            drop(unreachable.send(Input::Unreachable(peer, logs)));
            // Don't spin against a dead peer; raft retransmits.
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use enroute_shared::state::RepoMeta;

    #[test]
    fn frames_roundtrip() {
        let m = Message {
            to: 2,
            from: 1,
            term: 5,
            ..Default::default()
        };
        let state = Record {
            repo: "r".into(),
            epoch: 3,
            voters: vec![1, 2],
            meta: Some(RepoMeta::default()),
            ..Default::default()
        };
        let frames = vec![
            Frame::Raft(7, Box::new(m.clone())),
            Frame::Wake(9),
            Frame::Host("r".into(), 3, Some(Box::new(state.clone()))),
        ];
        let back: Vec<Frame> = frames
            .into_iter()
            .map(|f| Frame::from_proto(f.into_proto()).unwrap())
            .collect();
        assert!(matches!(&back[0], Frame::Raft(7, x) if **x == m));
        assert!(matches!(back[1], Frame::Wake(9)));
        assert!(matches!(&back[2], Frame::Host(r, 3, Some(s)) if r == "r" && **s == state));
    }
}
