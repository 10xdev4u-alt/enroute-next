//! The node registry in the object store: the ledger's members, their
//! addresses, and whether they are alive.
//!
//! **Members.** `ledger/members` lists the nodes every repository's log
//! should have as voters (a ledger node adds itself when it starts), and
//! the nodes an operator removed for good ([`remove_member`]). Logs only
//! lose nodes marked removed, so a stale copy of the lists can delay a
//! change but never remove a member.
//!
//! Each ledger node writes `nodes/<id>` (its store id, see
//! `enroute_ledger::logdb::store_id`) when it starts and rewrites it
//! every few seconds; the object's modification time is its heartbeat.
//! Every node lists the registry as often, learning ledger nodes' addresses
//! (for the transport, and for compute nodes to reach the ledger) and which
//! are alive (compute nodes ask live ones). Compute nodes only read it
//! ([`Registry::watch`]): nobody needs to find them.
//!
//! Safety never depends on the registry: a log's voters change only
//! through its log, and liveness only steers compute nodes' requests.

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime};

use anyhow::Result;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::store::Objects;

/// How often a node rewrites its entry and lists the others.
const HEARTBEAT: Duration = Duration::from_secs(2);
/// A node whose entry is older than this is not alive.
const ALIVE: Duration = Duration::from_secs(10);

/// Peers' addresses by id, shared with the transport.
pub type Addresses = Arc<RwLock<HashMap<u64, String>>>;

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Entry {
    id: u64,
    url: String,
}

/// The ledger's members, and the nodes removed from it.
#[derive(Serialize, Deserialize, Default, Clone, Debug, PartialEq, Eq)]
pub struct Members {
    /// Nodes that vote in the logs.
    pub members: Vec<u64>,
    /// Nodes removed for good.
    pub removed: Vec<u64>,
}

const MEMBERS: &str = "ledger/members";

/// The ledger's members. Empty if none are recorded yet.
///
/// # Errors
///
/// Returns an error if the object cannot be read or parsed.
pub async fn members(objects: &Objects) -> Result<Members> {
    Ok(objects
        .get_object(MEMBERS)
        .await?
        .map(|d| serde_json::from_slice(&d))
        .transpose()?
        .unwrap_or_default())
}

/// Change the members with `f`, by compare-and-swap.
async fn change_members(
    objects: &Objects,
    f: impl Fn(&mut Members) -> Result<()>,
) -> Result<Members> {
    loop {
        let (data, version) = objects.get_versioned(MEMBERS).await?.unzip();
        let mut m: Members = data
            .map(|d| serde_json::from_slice(&d))
            .transpose()?
            .unwrap_or_default();
        let before = m.clone();
        f(&mut m)?;
        if m == before
            || objects
                .put_if(MEMBERS, serde_json::to_vec(&m)?.into(), version.as_ref())
                .await?
                .is_some()
        {
            return Ok(m);
        }
    }
}

/// Add node `id` to the members.
///
/// # Errors
///
/// Returns an error if node `id` was removed, or the members cannot be
/// changed.
pub(crate) async fn add_member(objects: &Objects, id: u64) -> Result<Members> {
    change_members(objects, |m| {
        anyhow::ensure!(
            !m.removed.contains(&id),
            "node {id} was removed from the ledger: wipe its data directory to start it again"
        );
        if !m.members.contains(&id) {
            m.members.push(id);
        }
        Ok(())
    })
    .await
}

/// Refuse to remove node `id` if the members left would not have a live
/// majority (removing a live node while another is down, say): their logs
/// could then commit nothing, the removal included.
///
/// # Errors
///
/// Returns an error if the members left would not have a live majority, or
/// the registry cannot be read.
pub async fn check_removal(objects: &Arc<Objects>, id: u64) -> Result<()> {
    let m = members(objects).await?;
    let alive = Registry::watch(objects.clone()).await?.alive();
    let rest: Vec<u64> = m.members.into_iter().filter(|&x| x != id).collect();
    let live = rest.iter().filter(|x| alive.contains(x)).count();
    anyhow::ensure!(
        2 * live > rest.len(),
        "removing node {id} would leave {live} of {} members alive, not a majority (--force to remove it anyway)",
        rest.len()
    );
    Ok(())
}

/// Remove node `id` for good: logs then drop it.
///
/// # Errors
///
/// Returns an error if node `id` is not a member, or the members cannot be
/// changed.
pub async fn remove_member(objects: &Objects, id: u64) -> Result<Members> {
    change_members(objects, |m| {
        if !m.removed.contains(&id) {
            anyhow::ensure!(
                m.members.contains(&id),
                "node {id} is not a member of the ledger"
            );
            m.members.retain(|&x| x != id);
            m.removed.push(id);
        }
        Ok(())
    })
    .await
}

/// A node's view of the registry: the ledger's addresses and liveness.
#[derive(Debug)]
pub struct Registry {
    objects: Arc<Objects>,
    /// This node's entry, if it is a member (a ledger node).
    me: Option<Entry>,
    /// Peers' addresses, shared with the transport.
    pub addresses: Addresses,
    /// The ledger's members, as last read.
    members: RwLock<Members>,
    /// When each node's entry was last written.
    seen: RwLock<HashMap<u64, SystemTime>>,
}

fn path(id: u64) -> String {
    format!("nodes/{id:016x}")
}

impl Registry {
    /// Register this (ledger) node as a member and read the registry once.
    /// Refused while another live node registered `id` from another address
    /// (see `check_unique`).
    ///
    /// # Errors
    ///
    /// Returns an error if another live node holds `id`, or the registry
    /// cannot be read or written.
    pub async fn join(objects: Arc<Objects>, id: u64, url: String) -> Result<Arc<Registry>> {
        check_unique(&objects, id, &url, 2 * ALIVE).await?;
        add_member(&objects, id).await?;
        let r = Arc::new(Registry {
            objects,
            me: Some(Entry {
                id,
                url: url.clone(),
            }),
            addresses: Arc::new(RwLock::new(HashMap::from([(id, url)]))),
            members: RwLock::default(),
            seen: RwLock::default(),
        });
        Self::start(r).await
    }

    /// Read the registry without joining it (a compute node).
    ///
    /// # Errors
    ///
    /// Returns an error if the registry cannot be read.
    pub async fn watch(objects: Arc<Objects>) -> Result<Arc<Registry>> {
        let r = Arc::new(Registry {
            objects,
            me: None,
            addresses: Arc::default(),
            members: RwLock::default(),
            seen: RwLock::default(),
        });
        Self::start(r).await
    }

    async fn start(r: Arc<Registry>) -> Result<Arc<Registry>> {
        r.round().await?;
        let weak = Arc::downgrade(&r);
        tokio::spawn(heartbeat(weak));
        Ok(r)
    }

    /// Write this node's entry (if a ledger node), then read everyone's.
    ///
    /// # Errors
    ///
    /// Returns an error if this node's entry cannot be written, or the
    /// registry cannot be read.
    pub async fn round(&self) -> Result<()> {
        if let Some(me) = &self.me {
            self.objects
                .put_object(&path(me.id), serde_json::to_vec(me)?.into())
                .await?;
        }
        let listed = self.objects.list_modified("nodes/").await?;
        let mut seen = HashMap::new();
        for (name, at) in listed {
            let Ok(id) = u64::from_str_radix(&name, 16) else {
                continue;
            };
            seen.insert(id, at);
            // Read every round: a node (its store) moved to another machine
            // comes back at another address.
            if self.me.as_ref().is_none_or(|me| me.id != id)
                && let Some(data) = self.objects.get_object(&path(id)).await?
                && let Ok(e) = serde_json::from_slice::<Entry>(&data)
            {
                self.addresses.write().insert(e.id, e.url);
            }
        }
        *self.seen.write() = seen;
        *self.members.write() = members(&self.objects).await?;
        Ok(())
    }

    /// The members, as last read.
    pub fn members(&self) -> Members {
        self.members.read().clone()
    }

    /// A node's address.
    pub fn address(&self, id: u64) -> Option<String> {
        self.addresses.read().get(&id).cloned()
    }

    /// The ledger nodes alive now (this one included, if it is one), sorted.
    pub fn alive(&self) -> Vec<u64> {
        let now = SystemTime::now();
        let mut ids: Vec<u64> = self
            .seen
            .read()
            .iter()
            .filter(|(_, at)| now.duration_since(**at).unwrap_or_default() < ALIVE)
            .map(|(id, _)| *id)
            .filter(|id| self.addresses.read().contains_key(id))
            .collect();
        if let Some(me) = &self.me
            && !ids.contains(&me.id)
        {
            ids.push(me.id);
        }
        ids.sort_unstable();
        ids
    }
}

/// Wait (up to `wait`) until no other live node has registered `id` from
/// an address other than `url`, or fail. A node is its log store, so two
/// running copies of one store (restored on another machine while the
/// original still runs) could vote twice in an election. This only catches
/// the mistake by heartbeat age, a guess: the guarantee is to stop a node
/// before copying its store.
///
/// # Errors
///
/// Returns an error if another live node still holds `id` after `wait`.
async fn check_unique(objects: &Objects, id: u64, url: &str, wait: Duration) -> Result<()> {
    let name = format!("{id:016x}");
    let deadline = std::time::Instant::now() + wait;
    loop {
        let at = objects
            .list_modified("nodes/")
            .await?
            .into_iter()
            .find(|(n, _)| *n == name)
            .map(|(_, at)| at);
        let live =
            at.is_some_and(|at| SystemTime::now().duration_since(at).unwrap_or_default() < ALIVE);
        let other = if live {
            objects
                .get_object(&path(id))
                .await?
                .and_then(|d| serde_json::from_slice::<Entry>(&d).ok())
                .filter(|e| e.url != url)
        } else {
            None
        };
        let Some(other) = other else { return Ok(()) };
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "node {name} (this log store) is running at {}: a log store must run only once (stop it before moving its disk)",
            other.url
        );
        tokio::time::sleep(HEARTBEAT).await;
    }
}

async fn heartbeat(weak: Weak<Registry>) {
    loop {
        tokio::time::sleep(HEARTBEAT).await;
        let Some(r) = weak.upgrade() else { return };
        if let Err(e) = r.round().await {
            tracing::warn!("updating the node registry: {e:#}");
        }
    }
}

/// `ids` in `repo`'s order of preference (rendezvous hashing): the same on
/// every node, whatever order the ids come in, and a node joining or
/// leaving moves only the repositories that rank it among their first.
/// Compute nodes try ledger nodes in this order, so each repository's
/// requests go to the same ledger node first.
#[must_use]
pub fn rank(repo: &str, ids: &[u64]) -> Vec<u64> {
    let mut ranked: Vec<(u64, u64)> = ids.iter().map(|&id| (weight(repo, id), id)).collect();
    ranked.sort_unstable_by(|a, b| b.cmp(a));
    ranked.into_iter().map(|(_, id)| id).collect()
}

/// A hash of (repository, node) that is stable across builds and platforms
/// (FNV-1a, then `SplitMix64`'s finalizer to spread it).
fn weight(repo: &str, id: u64) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in repo.as_bytes().iter().chain(&id.to_le_bytes()) {
        h = (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3);
    }
    h = (h ^ (h >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h = (h ^ (h >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^ (h >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;

    /// A second node with the same store (a copy of it) is refused while
    /// the first one is alive at another address; the same address (a
    /// restart) is not.
    #[tokio::test]
    async fn a_store_runs_only_once() {
        let objects = Store::new(Arc::new(object_store::memory::InMemory::new())).objects();
        let _first = Registry::join(objects.clone(), 7, "http://a".into())
            .await
            .unwrap();
        let err = check_unique(&objects, 7, "http://b", Duration::ZERO)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("must run only once"), "{err:#}");
        check_unique(&objects, 7, "http://a", Duration::ZERO)
            .await
            .unwrap();
        check_unique(&objects, 8, "http://b", Duration::ZERO)
            .await
            .unwrap();
    }

    /// Joining makes a node a member; removing it is for good.
    #[tokio::test]
    async fn members_join_and_are_removed() {
        let objects = Store::new(Arc::new(object_store::memory::InMemory::new())).objects();
        let _a = Registry::join(objects.clone(), 9, "http://a".into())
            .await
            .unwrap();
        let _b = Registry::join(objects.clone(), 4, "http://b".into())
            .await
            .unwrap();
        assert_eq!(
            remove_member(&objects, 4).await.unwrap(),
            Members {
                members: vec![9],
                removed: vec![4]
            }
        );
        Registry::join(objects.clone(), 4, "http://b".into())
            .await
            .unwrap_err();
    }

    /// A removal must leave a live majority.
    #[tokio::test]
    async fn removals_keep_a_live_majority() {
        let objects = Store::new(Arc::new(object_store::memory::InMemory::new())).objects();
        let _a = Registry::join(objects.clone(), 1, "http://a".into())
            .await
            .unwrap();
        let _b = Registry::join(objects.clone(), 2, "http://b".into())
            .await
            .unwrap();
        add_member(&objects, 3).await.unwrap(); // never seen alive
        check_removal(&objects, 3).await.unwrap();
        let err = check_removal(&objects, 1).await.unwrap_err();
        assert!(err.to_string().contains("1 of 2 members alive"), "{err:#}");
    }

    #[test]
    fn rank_is_stable_and_spreads() {
        let ids: Vec<u64> = (1..=5).collect();
        let r = rank("repo", &ids);
        assert_eq!(r, rank("repo", &[5, 3, 1, 4, 2]));
        // Removing a node keeps the others' order.
        let without: Vec<u64> = r.iter().copied().filter(|&id| id != r[0]).collect();
        assert_eq!(rank("repo", &without), without);
        // Repositories are spread: every node is first for some of them.
        let mut first = std::collections::BTreeSet::new();
        for i in 0..200 {
            first.insert(rank(&format!("r{i}"), &ids)[0]);
        }
        assert_eq!(first.len(), 5);
    }

    /// A node rewrites its entry every `HEARTBEAT` and is counted dead after
    /// `ALIVE`. If the refresh were not the shorter of the two, every node
    /// would be dead as often as it was alive, and compute nodes would route
    /// around a healthy ledger.
    #[test]
    fn a_node_refreshes_its_entry_faster_than_it_expires() {
        assert!(
            HEARTBEAT < ALIVE,
            "refresh {HEARTBEAT:?} must be shorter than the liveness window {ALIVE:?}"
        );
    }

    /// Both lists are stored, so the encoded field names are part of the
    /// object's format and a rename makes an existing registry unreadable.
    #[test]
    fn members_round_trip_through_serde() {
        let members = Members {
            members: vec![1, 2, 3],
            removed: vec![4],
        };
        let encoded = serde_json::to_string(&members).unwrap();
        assert_eq!(encoded, r#"{"members":[1,2,3],"removed":[4]}"#);
        assert_eq!(serde_json::from_str::<Members>(&encoded).unwrap(), members);
    }

    #[test]
    fn an_empty_registry_is_the_default() {
        assert_eq!(
            Members::default(),
            Members {
                members: Vec::new(),
                removed: Vec::new(),
            }
        );
    }

    /// The stored form carries both lists. A reader that accepted a document
    /// missing one would read a truncated write as an empty registry, which is
    /// the failure `removed` exists to prevent.
    #[test]
    fn a_members_document_missing_a_list_is_refused() {
        // Refused for the missing list itself, not for a syntax error: the
        // distinction is the whole value of the case.
        let err = serde_json::from_str::<Members>("{}").unwrap_err();
        assert!(err.to_string().contains("missing field"), "{err}");
        let err = serde_json::from_str::<Members>(r#"{"members":[1]}"#).unwrap_err();
        assert!(err.to_string().contains("missing field"), "{err}");
    }
}
