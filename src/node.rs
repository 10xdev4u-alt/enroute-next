//! A node: the parts its roles share (the object store, the local store, the
//! node registry), and the roles it runs ([`Role`]).
//!
//! The [`Ledger`] holds the replicated logs, and decides each repository's
//! incarnation and its state in the object store. The [`Compute`] holds views
//! and storage, serves git traffic and does maintenance. A node that runs both
//! calls its ledger in-process. A compute-only node reaches the ledger nodes it
//! finds in the registry over gRPC.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use serde::Deserialize;

use enroute_compute::{Compute, ComputeConfig, HostHooks, RemoteLedger, Storage};
use enroute_ledger::consensus::Engine;
use enroute_ledger::{Ledger, LedgerConfig};
use enroute_shared::registry::Registry;
use enroute_shared::roles::LedgerRef;
pub use enroute_shared::store::Store;

/// What a node runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// Both roles, for a small deployment.
    #[default]
    Both,
    /// The replicated logs only, served on the internal listener alone.
    Ledger,
    /// The git work only, with the ledger nodes found in the registry.
    Compute,
}

impl Role {
    /// Whether the node runs the ledger.
    #[must_use]
    pub fn ledger(self) -> bool {
        self != Role::Compute
    }

    /// Whether the node runs the compute.
    #[must_use]
    pub fn compute(self) -> bool {
        self != Role::Ledger
    }
}

/// How to start a [`Node`].
#[derive(Clone, Debug)]
pub struct Config {
    /// What the node runs.
    pub role: Role,
    /// Local disk for the log, the state and the layer copies.
    pub data_dir: PathBuf,
    /// Where the objects live.
    pub store: Store,
    /// The decoded object and entry caches, in bytes.
    pub cache_bytes: usize,
    /// The buffer pool for layer pages, in bytes.
    pub page_cache_bytes: usize,
    /// Workers in the shared CPU pool (0: one per core).
    pub cpu_threads: usize,
    /// Local disk for layer copies, in bytes (0: no limit).
    ///
    /// Chunks load as read and leave under pressure. With no limit, a layer
    /// opened remotely is copied in full in the background.
    pub disk_cache_bytes: u64,
    /// Files the layer storage may keep open (None: the system's soft
    /// limit less a reserve).
    ///
    /// A value above that ceiling is lowered to it.
    pub max_open_files: Option<usize>,
    /// The ledger role.
    pub ledger: LedgerConfig,
    /// The compute role.
    pub compute: ComputeConfig,
}

impl Config {
    /// A standalone node that runs both roles, with every other value at its
    /// default.
    #[must_use]
    pub fn local(data_dir: PathBuf, store: Store) -> Self {
        Config {
            role: Role::default(),
            data_dir,
            store,
            cache_bytes: 512 << 20,
            page_cache_bytes: 256 << 20,
            cpu_threads: 0,
            disk_cache_bytes: 0,
            max_open_files: None,
            ledger: LedgerConfig::default(),
            compute: ComputeConfig::default(),
        }
    }
}

/// A running node.
pub struct Node {
    /// What it runs.
    pub role: Role,
    ledger: Option<Arc<Ledger>>,
    compute: Option<Arc<Compute>>,
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("role", &self.role)
            .finish_non_exhaustive()
    }
}

impl Node {
    /// Start the roles that `config` names, calling `hooks` from the compute.
    ///
    /// # Errors
    ///
    /// Returns an error if the object store, the local stores or the registry
    /// cannot be opened.
    pub async fn start(config: Config, hooks: HostHooks) -> Result<Arc<Node>> {
        let role = config.role;
        let objects = config.store.objects();
        let ledger = if role.ledger() {
            let ledger = Ledger::open(&config.data_dir, config.ledger.clone(), objects.clone());
            Some(ledger.await?)
        } else {
            None
        };
        let compute = if role.compute() {
            let api: LedgerRef = match &ledger {
                // Its own ledger first, which may not host a repository yet.
                Some(l) => match &l.registry {
                    Some(registry) => Arc::new(RemoteLedger::new(
                        registry.clone(),
                        Some((l.engine.id, l.clone())),
                    )),
                    None => l.clone(),
                },
                // It does not join the registry: nobody dials a compute node.
                None => Arc::new(RemoteLedger::new(
                    Registry::watch(objects.clone()).await?,
                    None,
                )),
            };
            enroute_compute::init_cpu_pool(config.cpu_threads);
            let storage = Arc::new(Storage::new(
                objects.clone(),
                config.data_dir.join("local"),
                config.page_cache_bytes,
                config.disk_cache_bytes,
                config.max_open_files,
            )?);
            Some(Compute::start(
                config.compute.clone(),
                hooks,
                objects,
                storage,
                config.cache_bytes,
                api,
            ))
        } else {
            None
        };
        Ok(Arc::new(Node {
            role,
            ledger,
            compute,
        }))
    }

    /// The consensus engine, on a node that runs the ledger.
    #[must_use]
    pub fn consensus(&self) -> Option<&Arc<Engine>> {
        self.ledger.as_ref().map(|l| &l.engine)
    }

    /// The ledger role, if the node runs it.
    #[must_use]
    pub fn ledger(&self) -> Option<&Arc<Ledger>> {
        self.ledger.as_ref()
    }

    /// The compute role, if the node runs it.
    #[must_use]
    pub fn compute(&self) -> Option<&Arc<Compute>> {
        self.compute.as_ref()
    }

    /// Git's smart HTTP, on a node that runs the compute.
    #[must_use]
    pub(crate) fn git_router(&self) -> Option<axum::Router> {
        self.compute.clone().map(enroute_compute::git_router)
    }

    /// The `enroute.api.v1alpha1` services, on a node that runs the compute.
    ///
    /// A sync pushes to the remotes that `reach` allows.
    ///
    /// # Errors
    ///
    /// Returns an error if the client for remotes cannot be built, or the
    /// compiled-in descriptors do not resolve.
    pub(crate) fn api_routes(
        &self,
        reach: enroute_compute::Reach,
    ) -> Result<Option<tonic::service::Routes>> {
        let Some(compute) = self.compute.clone() else {
            return Ok(None);
        };
        let remotes = enroute_compute::RemoteClient::new(reach)?;
        Ok(Some(enroute_compute::api_router(compute, remotes)?))
    }

    /// `GET /status`, `GET /status/repos/<repo>`, and on a ledger node the
    /// `Raft` and `Ledger` gRPC services.
    ///
    /// Only other nodes and operators use them, so keep them inside the cluster.
    pub(crate) fn internal_router(&self) -> axum::Router {
        let (ledger, compute) = (self.ledger.clone(), self.compute.clone());
        let repo_ledger = ledger.clone();
        let status = move || async move {
            let mut s = compute.map_or_else(|| serde_json::json!({}), |f| f.status());
            if let Some(fields) = s.as_object_mut() {
                let consensus = ledger.as_ref().map(|l| l.engine.status());
                fields.insert("consensus".into(), serde_json::json!(consensus));
            }
            axum::Json(s)
        };
        let router = match &self.ledger {
            Some(l) => tonic::service::Routes::new(l.engine.raft_server())
                .add_service(
                    enroute_shared::proto::ledger_server::LedgerServer::from_arc(l.clone())
                        .max_decoding_message_size(enroute_shared::proto::MAX_MESSAGE),
                )
                .into_axum_router(),
            None => axum::Router::new(),
        };
        let repo_status = move |axum::extract::Path(repo): axum::extract::Path<String>| async move {
            use axum::http::StatusCode;
            use axum::response::IntoResponse;
            match repo_ledger
                .as_ref()
                .and_then(|l| l.engine.repo_status(&repo))
            {
                Some(s) => axum::Json(s).into_response(),
                None => StatusCode::NOT_FOUND.into_response(),
            }
        };
        router
            .route("/status", axum::routing::get(status))
            .route("/status/repos/{*repo}", axum::routing::get(repo_status))
    }

    /// Stop the consensus engine at once, which releases the log store.
    pub fn stop(&self) {
        if let Some(c) = self.consensus() {
            c.shutdown();
        }
    }

    /// Stop, and wait up to 10 s for the ledger to close its stores.
    pub async fn shutdown(&self) {
        self.stop();
        if let Some(engine) = self.consensus() {
            // A timeout only: shutdown goes on either way.
            drop(tokio::time::timeout(Duration::from_secs(10), engine.stopped()).await);
        }
    }
}
