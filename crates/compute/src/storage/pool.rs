//! Buffer pool for immutable files.
//!
//! Layers (and later the node-private index files and object-store ranges)
//! are read through fixed-size pages held in one pool with a global byte
//! budget, instead of memory-mapping them. Everything read through the pool
//! is immutable, so the pool needs no dirty pages, write-back or latches:
//! a page is pinned simply by holding its `Arc`, and eviction skips pinned
//! pages.
//!
//! Eviction takes no shared lock, as in `DuckDB`'s buffer pool: each page in
//! memory has one entry in a lock-free queue, and a thread over budget takes
//! entries from the front. A referenced page goes to the back with its bit
//! cleared (CLOCK's second chance). Pages that can be read again are evicted
//! before temp-table pages, which can need a write first.
//!
//! Each open file has its own page table (one atomic slot per page), so a
//! hit is a single atomic load: no hash map, no lock. Misses read the page
//! with `pread` on the calling thread (callers run on blocking threads, as
//! they did when page faults did the reading); I/O errors are returned as
//! errors rather than raising `SIGBUS`.
//!
//! Large scans (streaming a full clone) insert pages as *cold* and do not
//! mark the pages they hit, so one big clone cannot evict the hot working
//! set, like Postgres's ring buffers for sequential scans.

use std::fs::File;
use std::ops::{Deref, Range};
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use anyhow::{Context, Result, ensure};
use crossbeam_queue::{ArrayQueue, SegQueue};
use futures::future::BoxFuture;
use parking_lot::{Mutex, RwLock, RwLockReadGuard};

use super::tiered::{ChunkHashes, NotResident, Pinned};

pub(super) const PAGE_SIZE: usize = 64 << 10;

/// Pages a scan's miss reads in one go (1 MiB of full pages): fewer, larger
/// reads, which a disk behind the OS cache needs more than the cache does.
const RUN_PAGES: usize = 16;

/// A page's bytes in memory, shared by its slot and its readers.
pub(super) type Page = Arc<Box<[u8]>>;

/// A page's bytes, if in memory. One lock per slot, not arc-swap: an
/// arc-swap store walks every thread's debt list, and each miss stores twice
/// (its page in, another page out).
#[derive(Default)]
pub(super) struct Slot(RwLock<Option<Arc<Box<[u8]>>>>);

impl Slot {
    pub(super) fn new(page: Option<Arc<Box<[u8]>>>) -> Slot {
        Slot(RwLock::new(page))
    }

    /// The page, borrowed while the guard lives (eviction skips it). A
    /// recursive read, so a reader that reads the page again does not wait
    /// behind a queued writer.
    pub(super) fn load(&self) -> RwLockReadGuard<'_, Option<Arc<Box<[u8]>>>> {
        self.0.read_recursive()
    }

    pub(super) fn load_full(&self) -> Option<Arc<Box<[u8]>>> {
        self.0.read_recursive().clone()
    }

    pub(super) fn swap(&self, page: Option<Arc<Box<[u8]>>>) -> Option<Arc<Box<[u8]>>> {
        std::mem::replace(&mut *self.0.write(), page)
    }

    pub(super) fn store(&self, page: Option<Arc<Box<[u8]>>>) {
        *self.0.write() = page;
    }

    /// Put `page` in the slot unless it holds one already: then the page it
    /// holds, and `page` back.
    fn install(&self, page: Box<[u8]>) -> Result<Page, (Page, Box<[u8]>)> {
        let mut slot = self.0.write();
        if let Some(held) = &*slot {
            return Err((held.clone(), page));
        }
        let page = Arc::new(page);
        *slot = Some(page.clone());
        Ok(page)
    }

    /// Empty the slot if it still holds `page` and no reader borrows it.
    pub(super) fn take_if(&self, page: &Arc<Box<[u8]>>) -> bool {
        let Some(mut slot) = self.0.try_write() else {
            return false;
        };
        if !slot.as_ref().is_some_and(|held| Arc::ptr_eq(held, page)) {
            return false;
        }
        *slot = None;
        true
    }

    /// Empty the slot unless a reader borrows or holds its page.
    fn try_evict(&self) -> Evict {
        let Some(mut slot) = self.0.try_write() else {
            return Evict::Pinned;
        };
        match slot.take() {
            None => Evict::Gone,
            Some(held) if Arc::strong_count(&held) > 1 => {
                *slot = Some(held);
                Evict::Pinned
            }
            Some(held) => Evict::Freed {
                bytes: held.len(),
                buf: Arc::try_unwrap(held).ok(),
            },
        }
    }
}

/// Panics for a broken invariant, as indexing did before.
#[expect(
    clippy::panic,
    reason = "an index or range the pool's own bookkeeping guarantees is broken"
)]
pub(super) fn fail(msg: std::fmt::Arguments<'_>) -> ! {
    panic!("{msg}")
}

/// `o`, which a broken invariant (`what`) would make `None`.
pub(super) fn must<T>(o: Option<T>, what: &str) -> T {
    o.unwrap_or_else(|| fail(format_args!("{what}")))
}

pub(super) use crate::git::cast::{to_u64, to_usize};

/// Where pages come from.
pub(crate) trait PageSource: Send + Sync {
    fn len(&self) -> u64;
    /// Fill `buf` from offset `off` (the range is within `len`). Never
    /// waits on a remote tier: bytes that are not local fail with
    /// [`NotResident`](super::tiered::NotResident).
    fn read_at(&self, off: u64, buf: &mut [u8]) -> std::io::Result<()>;
    /// Fill `bufs`, one after another, from offset `off`: like
    /// [`read_at`](Self::read_at) on each, in as few reads as the source
    /// allows.
    fn read_pages_at(&self, off: u64, bufs: &mut [&mut [u8]]) -> std::io::Result<()> {
        let mut off = off;
        for buf in bufs {
            self.read_at(off, buf)?;
            off += to_u64(buf.len());
        }
        Ok(())
    }
    /// `range` was read for a point access (a hint for the disk cache).
    fn used(&self, _range: Range<u64>) {}
    /// Keep `range` local while the pin lives (see [`Pinned`]).
    fn pin(&self, _range: Range<u64>) -> Pinned {
        Pinned::none()
    }
    /// The file's chunk hashes, to verify what is loaded (a no-op for local
    /// files, verified as a whole).
    fn set_chunk_hashes(&self, _hashes: ChunkHashes) -> Result<()> {
        Ok(())
    }
    /// The part of `range` a synchronous read would fail on, if any
    /// (without reading or fetching anything). Loading it is the only way
    /// remote bytes arrive.
    fn missing(&self, _range: Range<u64>) -> Option<NotResident> {
        None
    }
}

/// A file held in memory (an inline layer: its bytes are in the log).
pub(super) struct Memory(pub bytes::Bytes);

impl PageSource for Memory {
    fn len(&self) -> u64 {
        to_u64(self.0.len())
    }

    fn read_at(&self, off: u64, buf: &mut [u8]) -> std::io::Result<()> {
        let src = usize::try_from(off)
            .ok()
            .and_then(|o| self.0.get(o..o + buf.len()));
        buf.copy_from_slice(
            src.ok_or_else(|| std::io::Error::from(std::io::ErrorKind::UnexpectedEof))?,
        );
        Ok(())
    }

    /// Checks every chunk now: all the bytes are here, and nothing checks
    /// them later.
    fn set_chunk_hashes(&self, hashes: ChunkHashes) -> Result<()> {
        for (c, bytes) in self
            .0
            .chunks(to_usize(super::tiered::chunk_bytes()))
            .enumerate()
        {
            ensure!(
                hashes.check(to_u64(c), bytes),
                "chunk {c} does not match its hash"
            );
        }
        Ok(())
    }
}

impl PageSource for File {
    fn len(&self) -> u64 {
        self.metadata().map_or(0, |m| m.len())
    }

    fn read_at(&self, off: u64, buf: &mut [u8]) -> std::io::Result<()> {
        FileExt::read_exact_at(self, buf, off)
    }

    fn read_pages_at(&self, off: u64, bufs: &mut [&mut [u8]]) -> std::io::Result<()> {
        crate::sys::read_exact_vectored_at(self, bufs, off)
    }
}

/// How a reader uses the bytes it reads, from layer objects down to pool
/// pages and disk chunks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Access {
    /// Individual objects (lookups, delta bases): what is read is kept, a
    /// hit marks the page (or chunk) as recently used.
    Hot,
    /// Streaming much of a file (full clones, merges, replay): read through
    /// without displacing the working set; misses insert cold pages, hits
    /// don't refresh.
    Scan,
}

/// Miss and eviction counters. Hits are not counted: a shared counter
/// bumped on every access is exactly the cross-core contention the pool's
/// hit path avoids.
#[derive(Debug, Default)]
pub(crate) struct PoolStats {
    pub misses: AtomicU64,
    pub evictions: AtomicU64,
    /// Bytes read from sources on misses.
    pub read_bytes: AtomicU64,
}

impl PoolStats {
    /// Count `pages` read from a source, `bytes` in all.
    pub(crate) fn missed(&self, pages: usize, bytes: usize) {
        self.misses.fetch_add(to_u64(pages), Ordering::Relaxed);
        self.read_bytes.fetch_add(to_u64(bytes), Ordering::Relaxed);
    }
}

/// The eviction queue a page waits in. Queues are emptied in this order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Queue {
    /// Pages a scan brought in. A scan reads each once, so they go before
    /// pages read at random, which are read again. A referenced one moves
    /// to [`Queue::Reread`] instead of going back.
    Scan,
    /// Pages of files: dropping one costs nothing, it can be read again.
    Reread,
    /// Temp-table pages: a dirty one is written back first.
    WriteBack,
}

/// A page waiting in an eviction queue.
struct Candidate {
    owner: Weak<dyn PageOwner>,
    page: u64,
}

/// The shared pool: a byte budget and the eviction queues over all files'
/// pages.
pub(crate) struct BufferPool {
    budget: usize,
    used: AtomicUsize,
    /// Pages currently held (the queues may also hold entries of closed
    /// files, purged lazily).
    live: AtomicUsize,
    /// One entry per page in memory, by [`Queue`].
    scan: SegQueue<Candidate>,
    reread: SegQueue<Candidate>,
    write_back: SegQueue<Candidate>,
    /// Held by the one thread that purges a queue; the others go on.
    purging: Mutex<()>,
    /// Buffers of evicted full pages, reused by the next misses: a pool
    /// under pressure would otherwise allocate and free a 64 KiB buffer per
    /// miss, and allocators hold on to much of what is freed.
    free: ArrayQueue<Box<[u8]>>,
    pub stats: PoolStats,
}

impl std::fmt::Debug for BufferPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferPool")
            .field("budget", &self.budget)
            .field("used", &self.used)
            .field("live", &self.live)
            .finish_non_exhaustive()
    }
}

/// Evicted page buffers kept for reuse.
const FREE_BUFFERS: usize = 64;

/// Holder of pages in the pool: an immutable file ([`PagedFile`]) or a temp
/// table (writable pages written back to a scratch file on eviction).
pub(crate) trait PageOwner: Send + Sync {
    /// Whether `page` is still in memory. Queue entries for other pages are
    /// stale.
    fn resident(&self, page: u64) -> bool;
    /// Clear the recently-used bit of `page`, and return whether it was
    /// set.
    fn clear_referenced(&self, page: u64) -> bool;
    /// Drop the page from memory unless a reader holds it, writing it back
    /// first if it is dirty.
    fn try_evict(&self, page: u64) -> Evict;
}

pub(crate) enum Evict {
    /// In use (or being modified): try again later.
    Pinned,
    /// Not in memory any more.
    Gone,
    /// Evicted: `bytes` freed, and the buffer if nobody else holds it.
    Freed {
        bytes: usize,
        buf: Option<Box<[u8]>>,
    },
}

impl BufferPool {
    pub(crate) fn new(budget: usize) -> Arc<BufferPool> {
        Arc::new(BufferPool {
            budget,
            used: AtomicUsize::new(0),
            live: AtomicUsize::new(0),
            scan: SegQueue::new(),
            reread: SegQueue::new(),
            write_back: SegQueue::new(),
            purging: Mutex::new(()),
            free: ArrayQueue::new(FREE_BUFFERS),
            stats: PoolStats::default(),
        })
    }

    pub(crate) fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    pub(crate) fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "budget": self.budget,
            "used": self.used(),
            "pages": self.live.load(Ordering::Relaxed),
            "misses": self.stats.misses.load(Ordering::Relaxed),
            "evictions": self.stats.evictions.load(Ordering::Relaxed),
            "read_bytes": self.stats.read_bytes.load(Ordering::Relaxed),
        })
    }

    /// Open `source` for paged reads, in pages of [`PAGE_SIZE`] (or
    /// `ENROUTE_PAGE_SIZE` bytes in tests).
    pub(crate) fn open(self: &Arc<Self>, source: Arc<dyn PageSource>) -> Arc<PagedFile> {
        static N: OnceLock<usize> = OnceLock::new();
        let size = *N.get_or_init(|| {
            super::test_size("ENROUTE_PAGE_SIZE")
                .and_then(|n| usize::try_from(n).ok())
                .unwrap_or(PAGE_SIZE)
        });
        let len = source.len();
        self.open_paged(source, 0..len, size)
    }

    /// Open bytes `range` of `source` for paged reads in pages of
    /// `page_size` bytes: small pages for regions read at random (a miss
    /// loads little beyond what is needed), large ones for regions streamed
    /// in order.
    fn open_paged(
        self: &Arc<Self>,
        source: Arc<dyn PageSource>,
        range: Range<u64>,
        page_size: usize,
    ) -> Arc<PagedFile> {
        let len = range.end - range.start;
        let pages = to_usize(len.div_ceil(to_u64(page_size)));
        Arc::new(PagedFile {
            page_size,
            source,
            start: range.start,
            len,
            slots: (0..pages).map(|_| Slot::new(None)).collect(),
            referenced: (0..pages).map(|_| AtomicBool::new(false)).collect(),
            pool: self.clone(),
        })
    }

    /// A buffer for a page of `n` bytes, recycled when possible.
    pub(crate) fn buffer(&self, n: usize) -> Box<[u8]> {
        if n == PAGE_SIZE {
            self.free.pop()
        } else {
            None
        }
        .unwrap_or_else(|| vec![0u8; n].into_boxed_slice())
    }

    /// Charge `n` bytes to the budget, evicting first if that goes over it.
    /// As in `DuckDB`, memory is made before the read, not after.
    fn charge(&self, n: usize) {
        if self.used.fetch_add(n, Ordering::Relaxed) + n > self.budget {
            self.evict();
        }
    }

    /// [`charge`](Self::charge) `n` bytes and return a buffer for them: an
    /// evicted page's when one fits.
    pub(crate) fn reserve(&self, n: usize) -> Box<[u8]> {
        self.charge(n);
        self.buffer(n)
    }

    /// Put a page read into a [`reserve`](Self::reserve)d buffer in `slot`.
    /// Concurrent misses on a page may both read it: the first in wins, the
    /// other gives its reservation back and gets the page that won (`Err`).
    pub(super) fn install(&self, slot: &Slot, buf: Box<[u8]>) -> Result<Page, Page> {
        slot.install(buf).map_err(|(held, buf)| {
            self.unreserve(buf);
            held
        })
    }

    /// Give back a [`reserve`](Self::reserve)d buffer that holds no page
    /// (the read failed, or another reader brought the page in first).
    pub(crate) fn unreserve(&self, buf: Box<[u8]>) {
        self.used.fetch_sub(buf.len(), Ordering::Relaxed);
        self.recycle(Some(buf));
    }

    /// Account for pages an owner dropped by itself (e.g. on close).
    pub(crate) fn released(&self, bytes: usize, pages: usize) {
        self.used.fetch_sub(bytes, Ordering::Relaxed);
        self.live.fetch_sub(pages, Ordering::Relaxed);
    }

    /// Queue a page an owner just brought into memory, in a buffer from
    /// [`reserve`](Self::reserve).
    pub(crate) fn admit(&self, owner: Weak<dyn PageOwner>, page: u64, queue: Queue) {
        let live = self.live.fetch_add(1, Ordering::Relaxed) + 1;
        let q = self.queue(queue);
        q.push(Candidate { owner, page });
        // Closed owners leave entries behind; when they dominate, purge them
        // (amortized O(1) per admitted page).
        if q.len() > 2 * live + 1024 {
            self.purge(q);
        }
    }

    /// Evict until usage is back under budget, or every entry was seen
    /// twice (all pinned, or used again since). Concurrent callers take
    /// different entries, and can together evict a little more than needed.
    fn evict(&self) {
        let over = || self.used.load(Ordering::Relaxed) > self.budget;
        for (q, promote) in [
            (&self.scan, &self.reread),
            (&self.reread, &self.reread),
            (&self.write_back, &self.write_back),
        ] {
            let mut steps = 2 * q.len();
            while steps > 0
                && over()
                && let Some(c) = q.pop()
            {
                steps -= 1;
                self.evict_one(q, promote, c);
            }
            if !over() {
                return;
            }
        }
    }

    /// Evict `c`'s page, or put `c` back at the end of `q` (of `promote`
    /// if the page was used since).
    fn evict_one(&self, q: &SegQueue<Candidate>, promote: &SegQueue<Candidate>, c: Candidate) {
        let Some(owner) = c.owner.upgrade() else {
            return;
        };
        if owner.clear_referenced(c.page) {
            promote.push(c);
            return;
        }
        match owner.try_evict(c.page) {
            Evict::Pinned => q.push(c),
            Evict::Gone => {}
            Evict::Freed { bytes, buf } => {
                self.used.fetch_sub(bytes, Ordering::Relaxed);
                self.live.fetch_sub(1, Ordering::Relaxed);
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
                self.recycle(buf);
            }
        }
    }

    fn queue(&self, queue: Queue) -> &SegQueue<Candidate> {
        match queue {
            Queue::Scan => &self.scan,
            Queue::Reread => &self.reread,
            Queue::WriteBack => &self.write_back,
        }
    }

    /// Drop `q`'s entries for closed owners and pages no longer in memory.
    fn purge(&self, q: &SegQueue<Candidate>) {
        let Some(_purging) = self.purging.try_lock() else {
            return;
        };
        for _ in 0..q.len() {
            let Some(c) = q.pop() else {
                break;
            };
            if c.owner.upgrade().is_some_and(|o| o.resident(c.page)) {
                q.push(c);
            }
        }
    }

    /// Keep an evicted full-size buffer for the next miss.
    fn recycle(&self, buf: Option<Box<[u8]>>) {
        if let Some(b) = buf.filter(|b| b.len() == PAGE_SIZE) {
            // A full list drops the buffer.
            drop(self.free.push(b));
        }
    }
}

/// A file read through the pool: bytes `start..start + len` of its source.
pub(crate) struct PagedFile {
    page_size: usize,
    source: Arc<dyn PageSource>,
    start: u64,
    len: u64,
    slots: Box<[Slot]>,
    referenced: Box<[AtomicBool]>,
    pool: Arc<BufferPool>,
}

impl std::fmt::Debug for PagedFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PagedFile")
            .field("page_size", &self.page_size)
            .field("start", &self.start)
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

impl Drop for PagedFile {
    fn drop(&mut self) {
        let (mut bytes, mut pages) = (0, 0);
        for b in self.slots.iter().filter_map(|s| s.swap(None)) {
            bytes += b.len();
            pages += 1;
        }
        self.pool.released(bytes, pages);
    }
}

impl PageOwner for PagedFile {
    fn clear_referenced(&self, page: u64) -> bool {
        self.referenced_of(to_usize(page))
            .swap(false, Ordering::Relaxed)
    }

    fn resident(&self, page: u64) -> bool {
        self.slot_of(page).load().is_some()
    }

    fn try_evict(&self, page: u64) -> Evict {
        self.slot_of(page).try_evict()
    }
}

/// Bytes read through the pool: a pinned range of one page, or an owned
/// copy when the range spans pages.
#[derive(Debug)]
pub enum Chunk {
    Page(Arc<Box<[u8]>>, Range<usize>),
    Owned(Vec<u8>),
}

impl Deref for Chunk {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            // `read` checks the range against the file length.
            Chunk::Page(p, r) => p
                .get(r.clone())
                .unwrap_or_else(|| fail(format_args!("chunk range is outside its page"))),
            Chunk::Owned(v) => v,
        }
    }
}

impl PagedFile {
    pub(crate) fn len(&self) -> u64 {
        self.len
    }

    /// Mark a page recently used. Only written when it changes, so hot pages
    /// don't bounce a cache line between cores.
    fn touch(&self, p: usize, access: Access) {
        let referenced = self.referenced_of(p);
        if access == Access::Hot && !referenced.load(Ordering::Relaxed) {
            referenced.store(true, Ordering::Relaxed);
        }
    }

    /// Call `f` with page `page`, loading it on a miss. The hit path borrows
    /// the page without touching its shared reference count (arc-swap's
    /// thread-local debt slots); eviction can still drop the page from the
    /// pool meanwhile, and the borrow keeps it alive.
    pub(crate) fn with_page<R>(
        self: &Arc<Self>,
        page: u32,
        access: Access,
        f: impl FnOnce(&[u8]) -> R,
    ) -> Result<R> {
        let p = to_usize(page.into());
        let slot = self.slot(p, page)?;
        {
            let g = slot.load();
            if let Some(buf) = &*g {
                self.touch(p, access);
                return Ok(f(buf));
            }
        }
        let buf = self.page(page, access)?;
        Ok(f(&buf))
    }

    /// A view of bytes `range` of this file, paged separately in pages of
    /// `page_size` (an access method gives a region read at random its own,
    /// smaller pages). The view reads through this file's source, so every
    /// tier (local copy, remote) serves it alike.
    pub(crate) fn view(&self, range: Range<u64>, page_size: usize) -> Arc<PagedFile> {
        self.pool
            .open_paged(self.source.clone(), self.in_source(range), page_size)
    }

    /// The slot of `page`, which the pool's ring only holds for pages of
    /// the file.
    fn slot_of(&self, page: u64) -> &Slot {
        self.slots
            .get(to_usize(page))
            .unwrap_or_else(|| fail(format_args!("page {page} is outside the file")))
    }

    /// The recently-used bit of page `p`, for pages of the file only.
    fn referenced_of(&self, p: usize) -> &AtomicBool {
        self.referenced
            .get(p)
            .unwrap_or_else(|| fail(format_args!("page {p} is outside the file")))
    }

    fn slot(&self, p: usize, page: u32) -> Result<&Slot> {
        self.slots
            .get(p)
            .with_context(|| format!("page {page} beyond end of file"))
    }

    /// Fails unless `len` bytes at `off` lie inside the file.
    fn ensure_within(&self, off: u64, len: usize) -> Result<()> {
        ensure!(off + to_u64(len) <= self.len, "read past end of paged file");
        Ok(())
    }

    /// The page holding `off` and the position of `off` inside it.
    fn locate(&self, off: u64) -> Result<(u32, usize)> {
        let ps = to_u64(self.page_size);
        let page = u32::try_from(off / ps).context("offset beyond the last page")?;
        Ok((page, to_usize(off % ps)))
    }

    /// `range` of this file, as a range of its source.
    fn in_source(&self, range: Range<u64>) -> Range<u64> {
        self.start + range.start..self.start + range.end
    }

    /// Page `page`, loading it on a miss.
    pub(crate) fn page(self: &Arc<Self>, page: u32, access: Access) -> Result<Arc<Box<[u8]>>> {
        let p = to_usize(page.into());
        let slot = self.slot(p, page)?;
        if let Some(buf) = slot.load_full() {
            self.touch(p, access);
            return Ok(buf);
        }
        match access {
            Access::Hot => self.load_one(p, page, access),
            Access::Scan => self.load_run(p, page),
        }
    }

    /// Where page `p` starts in the file.
    fn page_off(&self, p: usize) -> u64 {
        to_u64(p) * to_u64(self.page_size)
    }

    /// Bytes in page `p` (the last one can be short).
    fn page_len(&self, p: usize) -> usize {
        to_usize((self.len - self.page_off(p)).min(to_u64(self.page_size)))
    }

    /// Read page `p` into memory.
    fn load_one(self: &Arc<Self>, p: usize, page: u32, access: Access) -> Result<Arc<Box<[u8]>>> {
        let (off, n) = (self.page_off(p), self.page_len(p));
        let mut buf = self.pool.reserve(n);
        if let Err(e) = self.source.read_at(self.start + off, &mut buf) {
            self.pool.unreserve(buf);
            return Err(e).with_context(|| format!("read page {page}"));
        }
        self.pool.stats.missed(1, n);
        if access == Access::Hot {
            self.source.used(self.in_source(off..off + to_u64(n)));
        }
        Ok(self.install(p, buf, access))
    }

    /// Read page `p` and the pages after it that are not in memory, in one
    /// read: a scan reads them next. At most [`RUN_PAGES`], and 1/64 of the
    /// budget: scan pages go first, so concurrent scans with runs that fill
    /// the pool evict their own pages before they read them.
    fn load_run(self: &Arc<Self>, p: usize, page: u32) -> Result<Arc<Box<[u8]>>> {
        let most = RUN_PAGES.min(self.pool.budget / 64 / self.page_size).max(1);
        let after = self
            .slots
            .get(p + 1..self.slots.len().min(p + most))
            .map_or(0, |next| {
                next.iter().take_while(|s| s.load().is_none()).count()
            });
        let end = p + 1 + after;
        let range = self.page_off(p)..self.page_off(end).min(self.len);
        // Checked before the run evicts anything for its pages: if some are
        // not local, page `p` alone reports what is missing.
        if end == p + 1 || self.source.missing(self.in_source(range.clone())).is_some() {
            return self.load_one(p, page, Access::Scan);
        }
        let bytes = to_usize(range.end - range.start);
        self.pool.charge(bytes);
        let mut head = self.pool.buffer(self.page_len(p));
        let mut tail: Vec<Box<[u8]>> = (p + 1..end)
            .map(|q| self.pool.buffer(self.page_len(q)))
            .collect();
        let read = {
            let mut parts: Vec<&mut [u8]> = std::iter::once(&mut head[..])
                .chain(tail.iter_mut().map(|b| &mut b[..]))
                .collect();
            self.source
                .read_pages_at(self.start + range.start, &mut parts)
        };
        if let Err(e) = read {
            for b in std::iter::once(head).chain(tail) {
                self.pool.unreserve(b);
            }
            return Err(e).with_context(|| format!("read pages {page}..{end}"));
        }
        self.pool.stats.missed(end - p, bytes);
        for (q, buf) in (p + 1..end).zip(tail) {
            self.install(q, buf, Access::Scan);
        }
        Ok(self.install(p, head, Access::Scan))
    }

    /// Put `buf`, just read, in page `p`'s slot and queue; the page already
    /// there if another reader was first.
    fn install(self: &Arc<Self>, p: usize, buf: Box<[u8]>, access: Access) -> Arc<Box<[u8]>> {
        let page = match self.pool.install(self.slot_of(to_u64(p)), buf) {
            Ok(page) => page,
            Err(held) => return held,
        };
        self.referenced_of(p)
            .store(access == Access::Hot, Ordering::Relaxed);
        let queue = match access {
            Access::Hot => Queue::Reread,
            Access::Scan => Queue::Scan,
        };
        let owner = Arc::downgrade(self);
        self.pool.admit(owner, to_u64(p), queue);
        page
    }

    /// `len` bytes at `off`.
    pub(crate) fn read(self: &Arc<Self>, off: u64, len: usize, access: Access) -> Result<Chunk> {
        self.ensure_within(off, len)?;
        let (first, start) = self.locate(off)?;
        if start + len <= self.page_size {
            let page = self.page(first, access)?;
            return Ok(Chunk::Page(page, start..start + len));
        }
        let mut out = vec![0u8; len];
        self.read_into(off, &mut out, access)?;
        Ok(Chunk::Owned(out))
    }

    /// Copy `buf.len()` bytes at `off` into `buf`.
    pub(crate) fn read_into(
        self: &Arc<Self>,
        mut off: u64,
        mut buf: &mut [u8],
        access: Access,
    ) -> Result<()> {
        self.ensure_within(off, buf.len())?;
        while !buf.is_empty() {
            let (page, start) = self.locate(off)?;
            let n = self.with_page(page, access, |p| copy_prefix(p, start, buf))?;
            let n = must(n, "page is shorter than its range");
            buf = must(
                buf.split_at_mut_checked(n),
                "page is shorter than its range",
            )
            .1;
            off += to_u64(n);
        }
        Ok(())
    }

    /// Call `f` with the `len` bytes at `off`, borrowed from the page when
    /// they lie in one (no copy, no reference count), else copied.
    pub(crate) fn with_bytes<R>(
        self: &Arc<Self>,
        off: u64,
        len: usize,
        access: Access,
        f: impl FnOnce(&[u8]) -> R,
    ) -> Result<R> {
        self.ensure_within(off, len)?;
        let (first, start) = self.locate(off)?;
        if start + len <= self.page_size {
            let r = self.with_page(first, access, |p| p.get(start..start + len).map(f))?;
            return Ok(must(r, "page is shorter than its range"));
        }
        let mut out = vec![0u8; len];
        self.read_into(off, &mut out, access)?;
        Ok(f(&out))
    }

    /// Tell the source the file's chunk hashes (see [`ChunkHashes`]).
    pub(crate) fn set_chunk_hashes(&self, hashes: ChunkHashes) -> Result<()> {
        self.source.set_chunk_hashes(hashes)
    }

    /// Keep `range` local (once loaded) while the pin lives.
    pub(crate) fn pin_range(&self, range: Range<u64>) -> Pinned {
        self.source.pin(self.in_source(self.page_range(range)))
    }

    /// Make `range` local and keep it so until the returned pin is dropped
    /// (for reading it later, synchronously).
    pub(super) fn load_pinned(
        &self,
        range: Range<u64>,
        access: Access,
    ) -> BoxFuture<'static, Result<Pinned>> {
        let range = self.in_source(self.page_range(range));
        // Pinned first: a range that is local now stays local.
        let pin = self.source.pin(range.clone());
        let need = self.source.missing(range);
        Box::pin(async move {
            super::tiered::load_pinned(need.into_iter().collect(), access).await?;
            Ok(pin)
        })
    }

    /// `range` widened to whole pages (what page loads read).
    fn page_range(&self, range: Range<u64>) -> Range<u64> {
        let ps = to_u64(self.page_size);
        (range.start / ps * ps).min(self.len)..(range.end.div_ceil(ps) * ps).min(self.len)
    }

    /// The part of `range` that is not local (reads of it would fail with
    /// [`NotResident`]), if any. Pages in the pool count as local.
    pub(crate) fn missing(&self, range: Range<u64>) -> Option<NotResident> {
        let range = self.page_range(range);
        let ps = to_u64(self.page_size);
        let all_paged =
            (range.start / ps..range.end.div_ceil(ps)).all(|p| self.slot_of(p).load().is_some());
        if all_paged {
            None
        } else {
            self.source.missing(self.in_source(range))
        }
    }

    /// Read directly from the source, bypassing the pool (one-off
    /// sequential reads such as checksums and section loads).
    pub(crate) fn read_direct(&self, off: u64, buf: &mut [u8]) -> Result<()> {
        self.ensure_within(off, buf.len())?;
        Ok(self.source.read_at(self.start + off, buf)?)
    }
}

/// Copies the bytes of `page` from `start` into the front of `buf`, as many
/// as fit. Returns the count, or `None` if `start` is past the page.
fn copy_prefix(page: &[u8], start: usize, buf: &mut [u8]) -> Option<usize> {
    let src = page.get(start..)?;
    let n = src.len().min(buf.len());
    buf.get_mut(..n)?.copy_from_slice(src.get(..n)?);
    Some(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Mem(Vec<u8>);
    impl PageSource for Mem {
        fn len(&self) -> u64 {
            to_u64(self.0.len())
        }
        fn read_at(&self, off: u64, buf: &mut [u8]) -> std::io::Result<()> {
            let off = to_usize(off);
            buf.copy_from_slice(&self.0[off..off + buf.len()]);
            Ok(())
        }
    }

    fn data(n: usize) -> Vec<u8> {
        (0..n).map(|i| u8::try_from(i * 7 % 251).unwrap()).collect()
    }

    #[test]
    fn views_page_separately() {
        let pool = BufferPool::new(1 << 30);
        let d = data(3 * PAGE_SIZE + 123);
        let f = pool.open(Arc::new(Mem(d.clone())));
        let (start, end) = (to_u64(PAGE_SIZE) - 100, 2 * to_u64(PAGE_SIZE) + 4000);
        let v = f.view(start..end, 4096);
        assert_eq!(v.len(), end - start);
        for (off, len) in [
            (0usize, 10usize),
            (4090, 20),
            (5000, 9000),
            (to_usize(end - start) - 7, 7),
        ] {
            let c = v.read(to_u64(off), len, Access::Hot).unwrap();
            let base = to_usize(start) + off;
            assert_eq!(&*c, &d[base..base + len]);
        }
        v.read(end - start - 3, 4, Access::Hot).unwrap_err();
    }

    #[test]
    fn reads_across_pages() {
        let pool = BufferPool::new(1 << 30);
        let d = data(3 * PAGE_SIZE + 123);
        let f = pool.open(Arc::new(Mem(d.clone())));
        for (off, len) in [
            (0, 10),
            (PAGE_SIZE - 5, 10),
            (PAGE_SIZE * 2 + 7, PAGE_SIZE + 50),
            (d.len() - 3, 3),
        ] {
            let c = f.read(to_u64(off), len, Access::Hot).unwrap();
            assert_eq!(&*c, &d[off..off + len]);
        }
        f.read(to_u64(d.len()) - 2, 3, Access::Hot).unwrap_err();
    }

    #[test]
    fn stays_within_budget_and_respects_pins() {
        let pool = BufferPool::new(2 * PAGE_SIZE);
        let d = data(10 * PAGE_SIZE);
        let f = pool.open(Arc::new(Mem(d.clone())));
        let pinned = f.read(0, 16, Access::Hot).unwrap();
        for p in 1..10u32 {
            f.page(p, Access::Hot).unwrap();
            assert!(pool.used() <= 3 * PAGE_SIZE, "used {}", pool.used());
        }
        // The pinned page survived eviction and still reads correctly.
        assert_eq!(&*pinned, &d[..16]);
        assert!(f.slots[0].load().is_some());
        assert!(pool.stats.evictions.load(Ordering::Relaxed) > 0);
        drop(f);
        assert_eq!(pool.used(), 0);
    }

    /// 2000 reads of 100 bytes at offsets from `seed`, a third of them as
    /// scans, each checked against `d`.
    fn read_at_random(f: &Arc<PagedFile>, d: &[u8], seed: u64) {
        let mut x = seed * 7919 + 1;
        for _ in 0..2000 {
            x = x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let off = to_usize(x >> 20) % (d.len() - 100);
            let access = if x.is_multiple_of(3) {
                Access::Scan
            } else {
                Access::Hot
            };
            let mut got = [0u8; 100];
            f.read_into(to_u64(off), &mut got, access).unwrap();
            assert_eq!(&got[..], &d[off..off + 100]);
        }
    }

    #[test]
    fn concurrent_readers_and_evictors_read_correct_bytes() {
        // Scans read runs of two pages at this budget.
        let pool = BufferPool::new(128 * PAGE_SIZE);
        let d = Arc::new(data(512 * PAGE_SIZE));
        let f = pool.open(Arc::new(Mem(d.to_vec())));
        std::thread::scope(|s| {
            for t in 0..8u64 {
                let (f, d) = (&f, &d);
                s.spawn(move || read_at_random(f, d, t));
            }
        });
        // Every thread may overshoot by the run it is loading.
        assert!(pool.used() <= 144 * PAGE_SIZE, "used {}", pool.used());
        assert!(pool.stats.evictions.load(Ordering::Relaxed) > 0);
        drop(f);
        assert_eq!(pool.used(), 0);
    }

    struct Failing;
    impl PageSource for Failing {
        fn len(&self) -> u64 {
            to_u64(4 * PAGE_SIZE)
        }
        fn read_at(&self, _off: u64, _buf: &mut [u8]) -> std::io::Result<()> {
            Err(std::io::ErrorKind::Other.into())
        }
    }

    #[test]
    fn a_failed_read_gives_its_reservation_back() {
        let pool = BufferPool::new(1 << 30);
        let f = pool.open(Arc::new(Failing));
        f.page(0, Access::Hot).unwrap_err();
        f.page(1, Access::Scan).unwrap_err();
        assert_eq!(pool.used(), 0);
        assert_eq!(pool.live.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn a_scan_miss_reads_the_run_after_it() {
        let pool = BufferPool::new(1 << 30);
        let d = data(64 * PAGE_SIZE);
        let f = pool.open(Arc::new(Mem(d.clone())));
        f.page(3, Access::Hot).unwrap();
        // Pages 1 and 2, up to page 3 that is in memory already.
        f.page(1, Access::Scan).unwrap();
        assert!(f.slots[2].load().is_some());
        assert_eq!(pool.stats.misses.load(Ordering::Relaxed), 3);
        // A full run from page 10.
        f.page(10, Access::Scan).unwrap();
        assert!((10..10 + RUN_PAGES).all(|q| f.slots[q].load().is_some()));
        assert!(f.slots[10 + RUN_PAGES].load().is_none());
        assert_eq!(
            pool.stats.read_bytes.load(Ordering::Relaxed),
            to_u64((3 + RUN_PAGES) * PAGE_SIZE)
        );
        assert_eq!(pool.used(), (3 + RUN_PAGES) * PAGE_SIZE);
        let c = f
            .read(to_u64(11 * PAGE_SIZE + 5), 10, Access::Scan)
            .unwrap();
        assert_eq!(&*c, &d[11 * PAGE_SIZE + 5..11 * PAGE_SIZE + 15]);
    }

    #[test]
    fn scanned_pages_used_again_are_kept_over_later_scans() {
        let pool = BufferPool::new(8 * PAGE_SIZE);
        let f = pool.open(Arc::new(Mem(data(64 * PAGE_SIZE))));
        f.page(0, Access::Scan).unwrap();
        f.page(0, Access::Hot).unwrap();
        for p in 1..64 {
            f.page(p, Access::Scan).unwrap();
        }
        assert!(f.slots[0].load().is_some());
    }

    #[test]
    fn vectored_reads_fill_every_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        let d = data(10_000);
        std::fs::write(&path, &d).unwrap();
        let file = File::open(&path).unwrap();
        let (mut a, mut b, mut c) = (vec![0u8; 4096], vec![0u8; 3000], vec![0u8; 900]);
        let mut bufs: Vec<&mut [u8]> = vec![&mut a, &mut b, &mut c];
        file.read_pages_at(100, &mut bufs).unwrap();
        assert_eq!(a, d[100..4196]);
        assert_eq!(b, d[4196..7196]);
        assert_eq!(c, d[7196..8096]);
        let mut past: Vec<&mut [u8]> = vec![&mut a];
        file.read_pages_at(9000, &mut past).unwrap_err();
    }

    #[test]
    fn closed_files_do_not_accumulate() {
        let pool = BufferPool::new(1 << 30);
        for _ in 0..5000 {
            let f = pool.open(Arc::new(Mem(data(100))));
            f.page(0, Access::Hot).unwrap();
        }
        assert_eq!(pool.used(), 0);
        assert_eq!(pool.live.load(Ordering::Relaxed), 0);
        let queued = pool.queue(Queue::Reread).len();
        assert!(queued <= 1024 + 2, "queue {queued}");
    }

    #[test]
    fn scans_do_not_evict_hot_pages() {
        let pool = BufferPool::new(4 * PAGE_SIZE);
        let d = data(64 * PAGE_SIZE);
        let f = pool.open(Arc::new(Mem(d)));
        for _ in 0..3 {
            f.page(0, Access::Hot).unwrap();
            f.page(1, Access::Hot).unwrap();
        }
        for p in 2..64 {
            f.page(p, Access::Scan).unwrap();
            // Keep the hot pages referenced as a live workload would.
            f.page(0, Access::Hot).unwrap();
            f.page(1, Access::Hot).unwrap();
        }
        assert!(f.slots[0].load().is_some() && f.slots[1].load().is_some());
    }
}
