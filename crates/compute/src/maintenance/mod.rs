//! Background maintenance on a compute node: flushes and compaction
//! ([`compact`]), the layer sweep ([`sweep`]), both on the repositories
//! whose maintenance lease ([`lease`]) this node holds; eviction of idle
//! views; and, after a restart, removal of local copies nothing references.

mod compact;
mod lease;
mod sweep;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use enroute_shared::roles::{self, LedgerRef};
use enroute_shared::state::{Command, Outcome};
use enroute_shared::store::Objects;

use crate::compute::ComputeConfig;
use crate::repo::{Repo, Repos};
use crate::states::{Consistency, States};
use crate::storage::Storage;
use compact::Maintenance;
use lease::Leases;

pub use compact::settled;

/// How often idle views are evicted.
const EVICT_EVERY: Duration = Duration::from_secs(1);

/// This node's maintenance. Its loops stop once it is dropped.
pub(crate) struct Maintainer {
    states: Arc<States>,
    repos: Arc<Repos>,
    storage: Arc<Storage>,
    objects: Arc<Objects>,
    /// Maintenance proposes its own commands, outside the commit path.
    ledger: LedgerRef,
    /// Which repositories this node maintains.
    leases: Leases,
    /// Flushes and compactions in flight here.
    in_flight: Arc<Maintenance>,
    config: ComputeConfig,
}

impl Maintainer {
    /// Propose `cmd` to its repository's log (which must be live): it
    /// neither waits for the outcome nor retries.
    async fn propose(&self, cmd: Command) -> Result<Outcome> {
        roles::propose(&*self.ledger, cmd).await
    }

    /// `repo` after everything committed so far (None: it does not exist).
    async fn latest(&self, repo: &str) -> Result<Option<Arc<Repo>>> {
        self.states.open(repo, Consistency::Latest).await
    }

    /// After a restart: drop local copies of layers no manifest references
    /// any more.
    fn reconcile_local(self: &Arc<Self>) {
        let node = self.clone();
        tokio::spawn(async move {
            let referenced = |repo: &str, id: &str| {
                node.states
                    .get(repo)
                    .is_none_or(|m| m.layers.iter().any(|l| l.id == id))
            };
            let removed = node.storage.sweep_local_layers(referenced).await;
            if removed > 0 {
                tracing::info!(removed, "removed local copies of unreferenced layers");
            }
        });
    }

    fn evict_idle(&self) {
        let evicted = self.states.evict(self.config.evict_after);
        if evicted > 0 {
            tracing::debug!(evicted, "evicted idle repositories");
        }
    }
}

/// Run `work` after each `pause`, while the node lives. The loop holds the
/// node only during `work`, so a dropped node stops it.
fn every<P, W>(
    node: &Arc<Maintainer>,
    pause: impl Fn(&Maintainer) -> P + Send + 'static,
    work: impl Fn(Arc<Maintainer>) -> W + Send + 'static,
) where
    P: Future<Output = ()> + Send + 'static,
    W: Future<Output = ()> + Send + 'static,
{
    let weak = Arc::downgrade(node);
    tokio::spawn(async move {
        loop {
            let Some(wait) = weak.upgrade().map(|n| pause(&n)) else {
                return;
            };
            wait.await;
            let Some(node) = weak.upgrade() else { return };
            work(node).await;
        }
    });
}

/// Start maintenance: flushing inline layers, compaction, layer sweeps,
/// eviction of idle views, and cleanup of local copies left from before a
/// restart. It runs while the returned handle lives.
pub(crate) fn start(
    config: &ComputeConfig,
    states: Arc<States>,
    repos: Arc<Repos>,
    storage: Arc<Storage>,
    objects: Arc<Objects>,
    ledger: LedgerRef,
) -> Arc<Maintainer> {
    // Leases need only an owner unique to this process: a restarted
    // compute waits out its old leases.
    let leases = Leases::new(objects.clone(), rand::random(), config.maintenance_lease);
    let node = Arc::new(Maintainer {
        states,
        repos,
        storage,
        objects,
        ledger,
        leases,
        in_flight: Arc::default(),
        config: config.clone(),
    });
    compact::spawn(&node);
    sweep::spawn(&node);
    node.reconcile_local();
    every(
        &node,
        |_| tokio::time::sleep(EVICT_EVERY),
        |n| async move { n.evict_idle() },
    );
    node
}
