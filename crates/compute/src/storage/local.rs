//! Each node's local copies of stored layer files
//! (`<data>/local/layers/...`), and the catalog that records them across
//! restarts. A copy is written without fsync, evictable, verified when
//! opened after a restart and fetched again if damaged (chunk by chunk,
//! against the hashes in the file).
//!
//! When the object store is a `file://` directory on the same filesystem,
//! a local copy is a **hard link** to the stored file instead, so each
//! layer is on disk once. Stored files are written once and never changed,
//! so the link is read-only and outside the disk budget (see
//! [`LocalCopy::linked`]). Elsewhere, or if the link fails, it is a copy.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};

use anyhow::Result;
use parking_lot::Mutex;

use enroute_shared::store::Objects;

use super::catalog::{Catalog, Journal};
use super::files::ScratchBudget;
use super::handles::Handles;
use super::key::FileKey;
use super::pool::{Access, PageSource};
use super::store::Sealed;
use super::tiered::{DiskCache, LocalCopy, RangeFetch, TieredSource};

/// Local copies younger than this are never swept (a push may be about to
/// reference them).
const SWEEP_AGE: std::time::Duration = std::time::Duration::from_secs(600);

pub(super) struct LocalCopies {
    dir: PathBuf,
    /// The object store, for the paths of its files when it is local.
    objects: Arc<Objects>,
    /// What this node holds locally, across restarts.
    pub(super) catalog: Arc<Catalog>,
    /// The disk budget for local copies, if any.
    disk: Option<Arc<DiskCache>>,
    /// Open descriptors of the local copies, bounded node-wide.
    handles: Arc<Handles>,
    /// Bounds the temporary files of new copies.
    scratch: ScratchBudget,
    /// Copies written by this process: their chunks need no checking when
    /// opened (copies found after a restart may be torn).
    trusted: Mutex<HashSet<FileKey>>,
    /// Local directories known to exist (created at most once per process,
    /// not on every push).
    dirs: Mutex<HashSet<PathBuf>>,
    /// How a local copy links to a stored file (replaced in tests).
    pub(super) link: fn(&Path, &Path) -> std::io::Result<()>,
    /// Cleared when the store's files are on another filesystem.
    pub(super) linking: AtomicBool,
    stats: LocalStats,
}

#[derive(Default)]
struct LocalStats {
    /// Files opened before a local copy existed (served by range).
    remote_opens: AtomicU64,
    /// Local copies completed in the background (no disk budget).
    hydrations: AtomicU64,
    /// Local copies reused after a restart.
    recovered: AtomicU64,
    /// Local copies made as hard links to the store's files.
    links: AtomicU64,
}

impl LocalCopies {
    /// The local copies under `dir`, reconciled with the catalog there.
    /// `disk_budget`: see [`super::Storage::new`]. `max_open`: layer files
    /// kept open.
    pub(super) fn new(
        dir: PathBuf,
        objects: Arc<Objects>,
        disk_budget: u64,
        max_open: usize,
        scratch: ScratchBudget,
    ) -> Result<Self> {
        let catalog = Arc::new(Catalog::open(dir.join("catalog.json"))?);
        reconcile(&dir, &catalog);
        let handles = Handles::new(max_open, dir.join("tmp").join("handles"))?;
        Ok(LocalCopies {
            dir,
            objects,
            catalog,
            disk: (disk_budget > 0).then(|| DiskCache::new(disk_budget)),
            handles,
            scratch,
            trusted: Mutex::default(),
            dirs: Mutex::default(),
            link: |from, to| std::fs::hard_link(from, to),
            linking: AtomicBool::new(true),
            stats: LocalStats::default(),
        })
    }

    pub(super) fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "remote_opens": self.stats.remote_opens.load(Relaxed),
            "hydrations": self.stats.hydrations.load(Relaxed),
            "recovered": self.stats.recovered.load(Relaxed),
            "links": self.stats.links.load(Relaxed),
        })
    }

    pub(super) fn disk_json(&self) -> Option<serde_json::Value> {
        self.disk.as_ref().map(|d| d.json())
    }

    fn tmp_dir(&self) -> PathBuf {
        self.dir.join("tmp")
    }

    /// Whether this process wrote the local copy of `key`.
    pub(super) fn trusted(&self, key: &FileKey) -> bool {
        self.trusted.lock().contains(key)
    }

    /// Put a complete local copy of a file in place (and in the catalog).
    pub(super) async fn put(&self, key: &FileKey, sealed: Sealed) -> Result<()> {
        let len = if let Some(len) = self.link(key).await {
            len
        } else {
            let local = key.local_path(&self.dir);
            #[expect(clippy::unwrap_used, reason = "local paths are under `dir`")]
            let parent = local.parent().unwrap();
            self.ensure_dir(parent).await?;
            // New files are written under a temporary name and moved into
            // place, so a concurrent reader never opens a partial file.
            let len = sealed.len();
            sealed
                .persist(&self.tmp_dir(), &local, &self.scratch)
                .await?;
            len
        };
        self.catalog.add(key, len, true);
        self.trusted.lock().insert(key.clone());
        Ok(())
    }

    /// Open the local copy of `key`, a file of `len` bytes: the one the
    /// catalog knows, else a new one that loads fill in from `remote`
    /// (served at once, by range).
    /// Without a disk budget, the rest of an incomplete copy is loaded in
    /// the background.
    pub(super) async fn open(
        self: &Arc<Self>,
        key: &FileKey,
        remote: Arc<dyn RangeFetch>,
        len: u64,
    ) -> Result<Arc<TieredSource>> {
        let path = key.local_path(&self.dir);
        #[expect(clippy::unwrap_used, reason = "local paths are under `dir`")]
        let parent = path.parent().unwrap();
        self.ensure_dir(parent).await?;
        let copy = self.find(key, &path, len).await?;
        let journal = Journal {
            catalog: self.catalog.clone(),
            key: key.clone(),
        };
        let source = TieredSource::open(
            &self.handles,
            remote,
            path,
            self.disk.clone(),
            Some(journal),
            copy,
        )?;
        if self.disk.is_none() && !source.complete() {
            let (this, key, source) = (self.clone(), key.clone(), source.clone());
            tokio::spawn(async move { this.hydrate(&key, &source).await });
        }
        Ok(source)
    }

    /// Load the rest of `source`'s local copy.
    async fn hydrate(&self, key: &FileKey, source: &TieredSource) {
        match source.load(0..source.len(), Access::Scan).await {
            Ok(()) => {
                self.stats.hydrations.fetch_add(1, Relaxed);
            }
            Err(e) => tracing::warn!(
                repo = key.repo,
                id = key.id,
                "completing the local copy (still served remotely): {e:#}"
            ),
        }
    }

    /// What the local copy of `key` at `path` is, recording a new one.
    async fn find(&self, key: &FileKey, path: &Path, len: u64) -> Result<LocalCopy> {
        let trusted = self.trusted(key);
        // A link made now, or one left from before that a new link could
        // not replace: either shares the stored file.
        let linked = match self.link(key).await {
            Some(len) => Some(len),
            None => self
                .objects
                .file(&key.store_path())
                .and_then(|stored| shares_inode(path, &stored)),
        };
        if let Some(len) = linked {
            self.catalog.add(key, len, true);
            return Ok(LocalCopy {
                len,
                chunks: Vec::new(),
                linked: true,
                trusted,
            });
        }
        let on_disk = tokio::fs::metadata(path).await.map(|m| m.len()).ok();
        let recorded = self.catalog.get(key).filter(|s| on_disk == Some(s.len));
        if let Some(s) = recorded {
            if !trusted {
                self.stats.recovered.fetch_add(1, Relaxed);
            }
            return Ok(LocalCopy {
                len: s.len,
                chunks: s.chunks().collect(),
                linked: false,
                trusted,
            });
        }
        self.catalog.add(key, len, false);
        self.stats.remote_opens.fetch_add(1, Relaxed);
        Ok(LocalCopy {
            len,
            chunks: Vec::new(),
            linked: false,
            trusted: false,
        })
    }

    /// Make the local copy of `key` a hard link to its file in a `file://`
    /// store, and return its length. None: the store is elsewhere, or the
    /// link failed (the caller copies instead).
    async fn link(&self, key: &FileKey) -> Option<u64> {
        if !self.linking.load(Relaxed) {
            return None;
        }
        let stored = self.objects.file(&key.store_path())?;
        let local = key.local_path(&self.dir);
        self.ensure_dir(local.parent()?).await.ok()?;
        // Linked beside, then renamed over any older copy: a reader never
        // sees the path missing.
        let tmp = self
            .tmp_dir()
            .join(format!("{}.link", rand::random::<u64>()));
        let link = self.link;
        let linked = tokio::task::spawn_blocking(move || {
            let made = link(&stored, &tmp)
                .and_then(|()| std::fs::rename(&tmp, &local))
                .and_then(|()| std::fs::metadata(&local));
            // A rename onto another link of the same file does nothing.
            drop(std::fs::remove_file(&tmp));
            made
        })
        .await
        .ok()?;
        match linked {
            Ok(meta) => {
                self.stats.links.fetch_add(1, Relaxed);
                Some(meta.len())
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::CrossesDevices {
                    tracing::info!("the object store is on another filesystem; copying layers");
                    self.linking.store(false, Relaxed);
                } else {
                    tracing::debug!(repo = key.repo, id = key.id, "linking the local copy: {e}");
                }
                None
            }
        }
    }

    /// Remove the local copy of `key`, if any.
    pub(super) async fn remove(&self, key: &FileKey) {
        self.trusted.lock().remove(key);
        let path = key.local_path(&self.dir);
        // Already missing is fine: the copy is meant to be gone.
        drop(tokio::fs::remove_file(path).await);
        self.catalog.forget(key);
    }

    /// Remove the local copies that `keep` does not keep, except recent
    /// ones (a push may be about to reference them). Returns how many went.
    pub(super) async fn sweep(&self, keep: impl Fn(&FileKey) -> bool) -> usize {
        let mut removed = 0;
        for key in self.catalog.keys() {
            if keep(&key) {
                continue;
            }
            let modified = tokio::fs::metadata(key.local_path(&self.dir))
                .await
                .and_then(|m| m.modified());
            if modified.is_ok_and(|t| t.elapsed().unwrap_or_default() >= SWEEP_AGE) {
                self.remove(&key).await;
                removed += 1;
            }
        }
        removed
    }

    async fn ensure_dir(&self, dir: &Path) -> Result<()> {
        if !self.dirs.lock().contains(dir) {
            tokio::fs::create_dir_all(dir).await?;
            self.dirs.lock().insert(dir.to_path_buf());
        }
        Ok(())
    }
}

/// The length of `local`, if it is the same file as `stored`.
fn shares_inode(local: &Path, stored: &Path) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    let (a, b) = (
        std::fs::metadata(local).ok()?,
        std::fs::metadata(stored).ok()?,
    );
    (a.dev() == b.dev() && a.ino() == b.ino()).then_some(a.len())
}

/// At startup: drop catalog entries whose local copy is gone or the wrong
/// size, and delete local files the catalog does not know (left by a crash
/// before their first record, or by an older version).
fn reconcile(dir: &Path, catalog: &Catalog) {
    let mut known = HashSet::new();
    for key in catalog.keys() {
        let path = key.local_path(dir);
        let ok = catalog
            .get(&key)
            .is_some_and(|s| std::fs::metadata(&path).is_ok_and(|m| m.len() == s.len));
        if ok {
            known.insert(path);
        } else {
            drop(std::fs::remove_file(&path));
            catalog.forget(&key);
        }
    }
    let mut stack = vec![dir.join("layers")];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in entries.flatten() {
            let p = e.path();
            if e.file_type().is_ok_and(|t| t.is_dir()) {
                stack.push(p);
            } else if !known.contains(&p) {
                drop(std::fs::remove_file(&p));
            }
        }
    }
}
