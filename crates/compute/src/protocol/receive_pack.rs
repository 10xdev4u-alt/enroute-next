//! `git-receive-pack` (protocol v0/v1, which v2 clients also use for push).

use anyhow::{Context, Result, bail};

use crate::git::oid::Oid;
use crate::query::RefView;

use super::AGENT;
use super::Refusal;
use super::pktline::{self, Pkt, Reader};

pub(crate) const CAPABILITIES: &str =
    "report-status delete-refs side-band-64k quiet atomic ofs-delta object-format=sha1";

/// The ref advertisement of `view`, which has no `HEAD` line.
pub(crate) fn advertisement(view: &RefView<'_>) -> Vec<u8> {
    let refs: Vec<(Oid, String)> = view
        .refs
        .iter()
        .map(|(name, oid)| (*oid, (*name).to_string()))
        .collect();
    super::advertise_refs(
        "git-receive-pack",
        &refs,
        &format!("{CAPABILITIES} agent={AGENT}"),
    )
}

#[derive(Debug, Clone)]
pub(crate) struct PushCommand {
    pub old: Oid,
    pub new: Oid,
    pub name: String,
}

pub(crate) struct PushRequest {
    pub commands: Vec<PushCommand>,
    pub capabilities: Vec<String>,
    /// Offset of the pack data in the request body.
    pub pack_offset: usize,
}

impl PushRequest {
    pub(crate) fn has(&self, cap: &str) -> bool {
        self.capabilities.iter().any(|c| c == cap)
    }
}

pub(crate) fn parse_request(body: &[u8]) -> Result<PushRequest> {
    let mut r = Reader::new(body);
    let mut commands = Vec::new();
    let mut capabilities = Vec::new();
    loop {
        match r.read()? {
            Some(Pkt::Flush) => break,
            None => bail!(Refusal::new("truncated command list (no flush)")),
            Some(Pkt::Data(d)) => {
                let (line, caps) = match pktline::split_once(d, 0) {
                    Some((line, caps)) => (line, Some(caps)),
                    None => (d, None),
                };
                if let Some(c) = caps {
                    let c = std::str::from_utf8(c)?.trim_end();
                    capabilities.extend(c.split(' ').filter(|s| !s.is_empty()).map(str::to_string));
                }
                let line = std::str::from_utf8(line)?.trim_end_matches('\n');
                if line.starts_with("shallow ") {
                    // The client's shallow boundary; connectivity checking
                    // rejects pushes that would need new shallow roots.
                    continue;
                }
                if line == "push-cert" || line.starts_with("push-cert\0") {
                    bail!(Refusal::new("signed pushes are not supported"));
                }
                let mut parts = line.splitn(3, ' ');
                let old = Oid::from_hex(parts.next().context("bad command")?)?;
                let new = Oid::from_hex(parts.next().context("bad command")?)?;
                let name = parts.next().context("bad command")?.to_string();
                commands.push(PushCommand { old, new, name });
            }
            Some(p) => bail!(Refusal::new(format!("unexpected packet {p:?}"))),
        }
    }
    Ok(PushRequest {
        commands,
        capabilities,
        pack_offset: r.pos,
    })
}

/// A push's report, after `messages` as progress lines (git prints them as
/// `remote:` lines). Without side-band the client has no channel for them,
/// so they are dropped, as git does.
pub(crate) fn report(
    unpack: Result<(), String>,
    results: &[(String, Option<String>)],
    sideband: bool,
    messages: &[String],
) -> Vec<u8> {
    let mut rep = Vec::new();
    match unpack {
        Ok(()) => pktline::line(&mut rep, "unpack ok\n"),
        Err(e) => pktline::line(&mut rep, &format!("unpack {}\n", e.replace('\n', " "))),
    }
    for (name, r) in results {
        match r {
            None => pktline::line(&mut rep, &format!("ok {name}\n")),
            Some(e) => pktline::line(&mut rep, &format!("ng {name} {}\n", e.replace('\n', " "))),
        }
    }
    pktline::flush(&mut rep);
    if !sideband {
        return rep;
    }
    let mut out = Vec::new();
    for m in messages {
        pktline::sideband(
            &mut out,
            2,
            format!("{}\n", m.trim_end_matches('\n')).as_bytes(),
        );
    }
    pktline::sideband(&mut out, 1, &rep);
    pktline::flush(&mut out);
    out
}
