//! Maintenance leases: which compute node maintains a repository (flushes its
//! inline layers, compacts it, sweeps its orphans). One of the nodes
//! following it takes `repos/<repo>/maintainer` in the object store by
//! compare-and-swap, for a while, and renews it while it has work.
//!
//! Correctness does not depend on it: every maintenance step is a manifest
//! compare-and-swap in the log, so two maintainers at once only waste a
//! merge. Hence no agreement on clocks is needed either.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use enroute_shared::store::{ObjectVersion, Objects};

use crate::util::{millis, now_ms};

#[derive(Serialize, Deserialize)]
struct Lease {
    owner: u64,
    /// Unix milliseconds.
    expires: u64,
}

enum Known {
    /// Ours, valid until then (by our clock).
    Mine(ObjectVersion, Instant),
    /// Someone else's; ask again then.
    Theirs(Instant),
}

pub(super) struct Leases {
    objects: Arc<Objects>,
    /// This node.
    owner: u64,
    /// How long a lease lasts; it is renewed once a third has passed, and
    /// another holder's is asked about again after at most a third.
    lease: Duration,
    known: Mutex<HashMap<String, Known>>,
    /// One check at a time per repository (a node's flush and compaction
    /// may ask together; the loser of two concurrent takes would otherwise
    /// note the winner, this node, as another holder).
    checking: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

fn path(repo: &str) -> String {
    format!("repos/{repo}/maintainer")
}

impl Leases {
    pub(super) fn new(objects: Arc<Objects>, owner: u64, lease: Duration) -> Leases {
        Leases {
            objects,
            owner,
            lease,
            known: Mutex::default(),
            checking: Mutex::default(),
        }
    }

    /// Whether this node holds `repo`'s lease (as last known; no request).
    pub(super) fn holds(&self, repo: &str) -> bool {
        matches!(self.known.lock().get(repo), Some(Known::Mine(_, until)) if *until > Instant::now())
    }

    /// Take or renew `repo`'s lease if it is free, expired or ours; false
    /// if another node holds it.
    pub(super) async fn hold(&self, repo: &str) -> Result<bool> {
        let lock = self
            .checking
            .lock()
            .entry(repo.to_string())
            .or_default()
            .clone();
        let result = {
            let _one = lock.lock().await;
            self.check(repo).await
        };
        let mut checking = self.checking.lock();
        // Only the map and this call hold it: nobody is waiting.
        if Arc::strong_count(&lock) == 2 {
            checking.remove(repo);
        }
        result
    }

    async fn check(&self, repo: &str) -> Result<bool> {
        let now = Instant::now();
        let ours = match self.known.lock().get(repo) {
            Some(Known::Mine(v, until)) if *until > now => {
                if *until - now > self.lease * 2 / 3 {
                    return Ok(true);
                }
                Some(v.clone())
            }
            Some(Known::Theirs(recheck)) if *recheck > now => return Ok(false),
            _ => None,
        };
        let mine = serde_json::to_vec(&Lease {
            owner: self.owner,
            expires: now_ms() + millis(self.lease),
        })?;
        // Renew from the version we wrote; else see who holds it.
        let from = match ours {
            Some(v) => Some(v),
            None => match self.objects.get_versioned(&path(repo)).await? {
                None => None,
                Some((data, v)) => {
                    let lease: Option<Lease> = serde_json::from_slice(&data).ok();
                    if let Some(l) = lease.filter(|l| l.owner != self.owner && l.expires > now_ms())
                    {
                        let left = Duration::from_millis(l.expires - now_ms());
                        self.known.lock().insert(
                            repo.to_string(),
                            Known::Theirs(now + left.min(self.lease / 3)),
                        );
                        return Ok(false);
                    }
                    Some(v)
                }
            },
        };
        if let Some(v) = self
            .objects
            .put_if(&path(repo), mine.into(), from.as_ref())
            .await?
        {
            self.known
                .lock()
                .insert(repo.to_string(), Known::Mine(v, now + self.lease));
            Ok(true)
        } else {
            // Someone else took or renewed it meanwhile.
            self.known
                .lock()
                .insert(repo.to_string(), Known::Theirs(now + self.lease / 3));
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn one_holder_at_a_time_and_takeover_after_expiry() {
        let dir = tempfile::tempdir().unwrap();
        let objects = enroute_shared::store::Store::local(dir.path())
            .unwrap()
            .objects();
        let lease = Duration::from_millis(300);
        let (a, b) = (
            Leases::new(objects.clone(), 1, lease),
            Leases::new(objects, 2, lease),
        );

        assert!(a.hold("r").await.unwrap());
        assert!(a.holds("r"));
        assert!(!b.hold("r").await.unwrap(), "held by a");
        // a renews while it keeps working.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(a.hold("r").await.unwrap());
        assert!(!b.hold("r").await.unwrap());
        // a stops; b takes over once a's lease has expired.
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(!a.holds("r"));
        assert!(b.hold("r").await.unwrap());
        assert!(!a.hold("r").await.unwrap(), "now held by b");
        // Other repositories are independent.
        assert!(a.hold("s").await.unwrap());
    }
}
