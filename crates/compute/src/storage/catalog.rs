//! The node catalog: what this node holds on local disk, kept across
//! restarts (see "Derived data, restarts and attach" in ARCHITECTURE.md).
//!
//! It records local copies of stored layers (their length and present
//! chunks). The catalog lives in
//! memory; a background thread writes all of it to `catalog.json` (to a
//! temporary file, then moved into place) about once a second when it
//! changed, and when the node stops. It serializes a copy: recording is on
//! the path of every push and every load, so it never waits for a write.
//! Nothing waits for it, and nothing needs it to be current: a chunk
//! recorded after the last write is fetched again, a record that outlives
//! its data is caught by the chunk hashes (a recovered chunk is verified
//! before it is read). (Which layer files to delete from the object store
//! is the layer sweep's, in the object store: see `maintenance/sweep.rs`.)

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use super::key::FileKey;
use super::pool::to_usize;
use super::tiered::chunk_bytes;

/// Write the catalog at most this often.
const WRITE_EVERY: Duration = Duration::from_secs(1);

/// A local copy, whose chunks fill in as they are loaded.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(super) struct Entry {
    pub len: u64,
    /// One bit per chunk.
    pub present: Vec<u64>,
}

impl Entry {
    pub(super) fn chunks(&self) -> impl Iterator<Item = u64> + '_ {
        (0..self.len.div_ceil(chunk_bytes())).filter(|c| {
            self.present
                .get(to_usize(c / 64))
                .is_some_and(|w| w & (1 << (c % 64)) != 0)
        })
    }

    fn set(&mut self, c: u64, on: bool) {
        let w = to_usize(c / 64);
        if self.present.len() <= w {
            self.present.resize(w + 1, 0);
        }
        let Some(word) = self.present.get_mut(w) else {
            return;
        };
        let bit = 1 << (c % 64);
        if on {
            *word |= bit;
        } else {
            *word &= !bit;
        }
    }
}

type Entries = HashMap<FileKey, Entry>;

pub(super) struct Catalog {
    live: Arc<Mutex<Entries>>,
    /// Changed since the writer last took a copy.
    dirty: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    writer: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Catalog {
    /// Load the catalog from `path` (empty if there is none, or it cannot be
    /// read), and keep writing it there.
    pub(super) fn open(path: PathBuf) -> Result<Catalog> {
        // A list of pairs: JSON keys are strings.
        let live: Vec<(FileKey, Entry)> = match std::fs::read(&path) {
            Ok(data) => serde_json::from_slice(&data).unwrap_or_else(|e| {
                tracing::warn!("unreadable catalog, starting empty: {e:#}");
                Vec::new()
            }),
            Err(_) => Vec::new(),
        };
        let live = Arc::new(Mutex::new(live.into_iter().collect()));
        let (dirty, stop) = (
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        );
        let (l, d, s) = (live.clone(), dirty.clone(), stop.clone());
        let writer = std::thread::Builder::new()
            .name("catalog".into())
            .spawn(move || write_until_stopped(&l, &d, &s, &path))?;
        Ok(Catalog {
            live,
            dirty,
            stop,
            writer: Mutex::new(Some(writer)),
        })
    }

    fn change(&self, f: impl FnOnce(&mut Entries)) {
        f(&mut self.live.lock());
        self.dirty.store(true, Ordering::Release);
    }

    /// A local copy of `len` bytes: complete, or with no chunks yet.
    pub(super) fn add(&self, key: &FileKey, len: u64, complete: bool) {
        let mut s = Entry {
            len,
            present: Vec::new(),
        };
        if complete {
            for c in 0..len.div_ceil(chunk_bytes()) {
                s.set(c, true);
            }
        }
        self.change(|l| {
            l.insert(key.clone(), s);
        });
    }

    /// Chunks `chunks` of a local copy are present.
    pub(super) fn present(&self, key: &FileKey, chunks: std::ops::Range<u64>) {
        self.change(|l| {
            if let Some(s) = l.get_mut(key) {
                chunks.for_each(|c| s.set(c, true));
            }
        });
    }

    /// A chunk was evicted (or failed verification).
    pub(super) fn evicted(&self, key: &FileKey, chunk: u64) {
        self.change(|l| {
            if let Some(s) = l.get_mut(key) {
                s.set(chunk, false);
            }
        });
    }

    /// The local copy is gone.
    pub(super) fn forget(&self, key: &FileKey) {
        self.change(|l| {
            l.remove(key);
        });
    }

    /// The local copy of `key`, if recorded.
    pub(super) fn get(&self, key: &FileKey) -> Option<Entry> {
        self.live.lock().get(key).cloned()
    }

    pub(super) fn keys(&self) -> Vec<FileKey> {
        self.live.lock().keys().cloned().collect()
    }
}

impl Drop for Catalog {
    /// Write what changed since the last write.
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(w) = self.writer.lock().take() {
            w.thread().unpark();
            // A panicked writer has already reported itself.
            drop(w.join());
        }
    }
}

/// The writer thread: write when changed, and once more after `stop`.
fn write_until_stopped(live: &Mutex<Entries>, dirty: &AtomicBool, stop: &AtomicBool, path: &Path) {
    loop {
        std::thread::park_timeout(WRITE_EVERY);
        let stopping = stop.load(Ordering::Acquire);
        if dirty.swap(false, Ordering::AcqRel) {
            write(live, path);
        }
        if stopping {
            return;
        }
    }
}

/// Write a copy of the catalog to `path` (so recording never waits for
/// serialization).
fn write(live: &Mutex<Entries>, path: &Path) {
    let copy: Vec<(FileKey, Entry)> = live
        .lock()
        .iter()
        .map(|(k, s)| (k.clone(), s.clone()))
        .collect();
    #[expect(
        clippy::expect_used,
        reason = "a catalog that cannot serialize is a bug"
    )]
    let data = serde_json::to_vec(&copy).expect("catalog serializes");
    let tmp = path.with_extension("json.tmp");
    if let Err(e) = std::fs::write(&tmp, data).and_then(|()| std::fs::rename(&tmp, path)) {
        tracing::warn!("writing the catalog: {e:#}");
    }
}

/// Reports a local copy's chunks to the catalog.
pub(super) struct Journal {
    pub catalog: Arc<Catalog>,
    pub key: FileKey,
}

impl Journal {
    pub(super) fn present(&self, chunks: std::ops::Range<u64>) {
        self.catalog.present(&self.key, chunks);
    }

    pub(super) fn evicted(&self, chunk: u64) {
        self.catalog.evicted(&self.key, chunk);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let k = FileKey::new("r", "abc");
        let open = || Catalog::open(dir.path().join("catalog.json")).unwrap();
        let c = open();
        c.add(&k, 10 * chunk_bytes(), false);
        c.present(&k, 2..5);
        c.evicted(&k, 3);
        drop(c);
        let c = open();
        assert_eq!(c.get(&k).unwrap().chunks().collect::<Vec<_>>(), vec![2, 4]);
        c.forget(&k);
        drop(c);
        let c = open();
        assert!(c.get(&k).is_none());
        // A damaged catalog is an empty one (the files on disk are
        // reconciled against it at startup).
        drop(c);
        std::fs::write(dir.path().join("catalog.json"), b"{not json").unwrap();
        assert!(open().keys().is_empty());
    }
}
