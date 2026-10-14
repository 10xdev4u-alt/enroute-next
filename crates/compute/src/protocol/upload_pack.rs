//! Protocol v2 `git-upload-pack`: capability advertisement, `ls-refs` and `fetch`.

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use tokio::sync::mpsc;

use crate::git::oid::Oid;
use crate::query::RefView;
use crate::repo::serve::{FetchArgs, Filter, Plan, plan, write_pack};
use crate::repo::view::Snapshot;

use super::AGENT;
use super::Refusal;
use super::pktline::{self, Pkt, Reader};

pub(crate) fn advertisement() -> Vec<u8> {
    let mut out = Vec::new();
    pktline::line(&mut out, "version 2\n");
    pktline::line(&mut out, &format!("agent={AGENT}\n"));
    pktline::line(&mut out, "ls-refs=unborn\n");
    pktline::line(&mut out, "fetch=shallow filter\n");
    pktline::line(&mut out, "object-format=sha1\n");
    pktline::flush(&mut out);
    out
}

pub(crate) struct Request<'a> {
    pub command: &'a str,
    pub args: Vec<&'a str>,
}

pub(crate) fn parse_request(body: &[u8]) -> Result<Request<'_>> {
    let mut r = Reader::new(body);
    let mut command = None;
    loop {
        match r.next_line()? {
            None => bail!(Refusal::new("unexpected end of request")),
            Some(Ok(l)) => {
                if let Some(c) = l.strip_prefix("command=") {
                    command = Some(c);
                }
            }
            Some(Err(Pkt::Delim)) => break,
            Some(Err(Pkt::Flush)) => {
                return Ok(Request {
                    command: command.unwrap_or(""),
                    args: vec![],
                });
            }
            Some(Err(p)) => bail!(Refusal::new(format!("unexpected packet {p:?}"))),
        }
    }
    let mut args = Vec::new();
    loop {
        match r.next_line()? {
            None | Some(Err(Pkt::Flush)) => break,
            Some(Ok(l)) => args.push(l),
            Some(Err(p)) => bail!(Refusal::new(format!("unexpected packet {p:?}"))),
        }
    }
    Ok(Request {
        command: command.context("missing command")?,
        args,
    })
}

/// What an `ls-refs` asks for.
#[derive(Debug, Default)]
pub(crate) struct LsRefs<'a> {
    pub symrefs: bool,
    pub peel: bool,
    pub unborn: bool,
    pub prefixes: Vec<&'a str>,
}

/// Parse the arguments of `ls-refs`.
pub(crate) fn parse_ls_refs<'a>(args: &[&'a str]) -> LsRefs<'a> {
    let mut ask = LsRefs::default();
    for a in args {
        match *a {
            "symrefs" => ask.symrefs = true,
            "peel" => ask.peel = true,
            "unborn" => ask.unborn = true,
            _ => ask.prefixes.extend(a.strip_prefix("ref-prefix ")),
        }
    }
    ask
}

/// Answer `ls-refs` with `view`, naming `HEAD`'s branch if `symrefs` and an
/// unborn `HEAD` if `unborn`.
pub(crate) fn ls_refs(view: &RefView<'_>, symrefs: bool, unborn: bool) -> Vec<u8> {
    let mut out = Vec::new();
    match view.head {
        Some((branch, Some(oid))) => {
            let symref = if symrefs {
                format!(" symref-target:{branch}")
            } else {
                String::new()
            };
            pktline::line(&mut out, &format!("{oid} HEAD{symref}\n"));
        }
        Some((branch, None)) if unborn => {
            pktline::line(&mut out, &format!("unborn HEAD symref-target:{branch}\n"));
        }
        _ => {}
    }
    for (name, oid) in &view.refs {
        let peeled = view
            .peeled
            .get(oid)
            .map(|p| format!(" peeled:{p}"))
            .unwrap_or_default();
        pktline::line(&mut out, &format!("{oid} {name}{peeled}\n"));
    }
    pktline::flush(&mut out);
    out
}

/// Read into `args` one line that both protocols of a fetch share, as its
/// `key` and `value`. Whether it was one.
pub(super) fn fetch_line(args: &mut FetchArgs, key: &str, value: &str) -> Result<bool> {
    match key {
        "want" => args.wants.push(Oid::from_hex(value)?),
        "have" => args.haves.push(Oid::from_hex(value)?),
        "shallow" => args.client_shallows.push(Oid::from_hex(value)?),
        "deepen" => args.deepen = Some(value.parse::<u32>().context("bad deepen")?.max(1)),
        "filter" => args.filter = Filter::parse(value)?,
        "deepen-since" | "deepen-not" => bail!(Refusal::new(format!("{key} is not supported"))),
        _ => return Ok(false),
    }
    Ok(true)
}

/// The haves that `snap` holds, in the client's order.
pub(super) fn common_haves(snap: &Snapshot, haves: &[Oid]) -> Vec<Oid> {
    haves
        .iter()
        .filter(|h| snap.lookup(h).is_some())
        .copied()
        .collect()
}

/// The `shallow` and `unshallow` lines of `plan`.
pub(super) fn write_shallow_lines(out: &mut Vec<u8>, plan: &Plan) {
    for s in &plan.shallow {
        pktline::line(out, &format!("shallow {s}\n"));
    }
    for s in &plan.unshallow {
        pktline::line(out, &format!("unshallow {s}\n"));
    }
}

pub(crate) struct FetchRequest {
    pub args: FetchArgs,
    pub done: bool,
    pub wait_for_done: bool,
}

pub(crate) fn parse_fetch(args: &[&str]) -> Result<FetchRequest> {
    let mut req = FetchRequest {
        args: FetchArgs::default(),
        done: false,
        wait_for_done: false,
    };
    for l in args {
        let (k, v) = l.split_once(' ').unwrap_or((l, ""));
        if fetch_line(&mut req.args, k, v)? {
            continue;
        }
        match k {
            "done" => req.done = true,
            "thin-pack" => req.args.thin_pack = true,
            "include-tag" => req.args.include_tag = true,
            "wait-for-done" => req.wait_for_done = true,
            "deepen-relative" => req.args.deepen_relative = true,
            "ofs-delta" => req.args.ofs_delta = true,
            "want-ref" | "packfile-uris" => bail!(Refusal::new(format!("{k} is not supported"))),
            _ => {}
        }
    }
    Ok(req)
}

/// Handle `fetch` (as [`parse_fetch`] reads it). Returns the response as a
/// stream of chunks.
pub(crate) fn fetch(snap: Arc<Snapshot>, req: FetchRequest) -> mpsc::Receiver<Result<Bytes>> {
    let mut pre = Vec::new();
    if !req.done {
        let common = common_haves(&snap, &req.args.haves);
        pktline::line(&mut pre, "acknowledgments\n");
        if common.is_empty() {
            pktline::line(&mut pre, "NAK\n");
        }
        for c in &common {
            pktline::line(&mut pre, &format!("ACK {c}\n"));
        }
        let ready = !req.wait_for_done && (!common.is_empty() || req.args.haves.is_empty());
        if !ready {
            pktline::flush(&mut pre);
            return reply_now(pre);
        }
        pktline::line(&mut pre, "ready\n");
        pktline::delim(&mut pre);
    }
    spawn_respond(snap, true, |snap| async move {
        let p = plan(&snap, &req.args).await?;
        if !p.shallow.is_empty() || !p.unshallow.is_empty() || req.args.deepen.is_some() {
            pktline::line(&mut pre, "shallow-info\n");
            write_shallow_lines(&mut pre, &p);
            pktline::delim(&mut pre);
        }
        pktline::line(&mut pre, "packfile\n");
        Ok((pre, Some(p)))
    })
}

/// [`respond`] on a task of its own, into the stream it returns. `head`
/// gets the snapshot to plan with.
pub(super) fn spawn_respond<F>(
    snap: Arc<Snapshot>,
    sideband: bool,
    head: impl FnOnce(Arc<Snapshot>) -> F,
) -> mpsc::Receiver<Result<Bytes>>
where
    F: Future<Output = Result<(Vec<u8>, Option<Plan>)>> + Send + 'static,
{
    let (tx, rx) = mpsc::channel::<Result<Bytes>>(16);
    tokio::spawn(respond(snap.clone(), head(snap), sideband, tx));
    rx
}

/// A response of `bytes` alone, with no pack and no plan: empty bytes are
/// an empty response.
pub(super) fn reply_now(bytes: Vec<u8>) -> mpsc::Receiver<Result<Bytes>> {
    let (tx, rx) = mpsc::channel::<Result<Bytes>>(1);
    if !bytes.is_empty() {
        // A new channel has room for one.
        drop(tx.try_send(Ok(Bytes::from(bytes))));
    }
    rx
}

/// Answer a fetch (either protocol): what `head` produces (the response up
/// to the pack, and the pack's plan, if one is sent), then the pack,
/// side-band multiplexed if `sideband` (and then ended by a flush). An
/// error before anything was sent is answered with `ERR`, one after on
/// side-band 3 (if multiplexed).
async fn respond(
    snap: Arc<Snapshot>,
    head: impl Future<Output = Result<(Vec<u8>, Option<Plan>)>>,
    sideband: bool,
    tx: mpsc::Sender<Result<Bytes>>,
) {
    let gone = |_| anyhow::anyhow!("client went away");
    let mut sent = false;
    let res: Result<()> = async {
        let (pre, plan) = head.await?;
        sent = true;
        tx.send(Ok(Bytes::from(pre))).await.map_err(gone)?;
        let Some(p) = plan else { return Ok(()) };
        let sink = crate::util::channel_sink(tx.clone(), move |chunk| {
            if !sideband {
                return Bytes::copy_from_slice(chunk);
            }
            let mut out = Vec::with_capacity(chunk.len() + 16);
            pktline::sideband(&mut out, 1, chunk);
            Bytes::from(out)
        });
        write_pack(snap, p, sink).await?;
        if sideband {
            let mut end = Vec::new();
            pktline::flush(&mut end);
            tx.send(Ok(Bytes::from(end))).await.map_err(gone)?;
        }
        Ok(())
    }
    .await;
    if let Err(e) = res {
        tracing::warn!("fetch failed: {e:#}");
        let mut out = Vec::new();
        match (sent, sideband) {
            (false, _) => pktline::line(&mut out, &format!("ERR {}\n", super::client_message(&e))),
            (true, true) => {
                let message = format!("enroute: {}\n", super::client_message(&e));
                pktline::sideband(&mut out, 3, message.as_bytes());
            }
            (true, false) => return,
        }
        pktline::flush(&mut out);
        // A closed receiver means the client left; nobody reads the error.
        drop(tx.send(Ok(Bytes::from(out))).await);
    }
}
