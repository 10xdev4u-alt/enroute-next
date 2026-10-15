//! The only module allowed to use `unsafe` (the crate denies it elsewhere):
//! the OS calls that rustix does not wrap, and the libdeflate call that
//! `libdeflater` does not wrap.

#![warn(clippy::undocumented_unsafe_blocks)]

/// Free the disk space of bytes `off..off + len` of `file`: they read as
/// zeros afterwards and the file keeps its length. Filesystems free whole
/// blocks only, so the range should be block-aligned.
pub(crate) fn punch_hole(file: &std::fs::File, off: u64, len: u64) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use rustix::fs::FallocateFlags;
        let mode = FallocateFlags::PUNCH_HOLE | FallocateFlags::KEEP_SIZE;
        Ok(rustix::fs::fallocate(file, mode, off, len)?)
    }
    #[cfg(target_os = "macos")]
    {
        use std::os::fd::AsRawFd;
        let args = libc::fpunchhole_t {
            fp_flags: 0,
            reserved: 0,
            fp_offset: off_t(off)?,
            fp_length: off_t(len)?,
        };
        // SAFETY: the descriptor is open for the call's duration and `args`
        // is a valid `fpunchhole_t` that outlives it.
        let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PUNCHHOLE, &args) };
        if rc == -1 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = (file, off, len);
        Ok(())
    }
}

/// Convert a file offset or length to the OS type.
#[cfg(target_os = "macos")]
fn off_t(v: u64) -> std::io::Result<libc::off_t> {
    libc::off_t::try_from(v).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))
}

/// The soft limit on open file descriptors; `u64::MAX` when unlimited.
pub(crate) fn open_file_limit() -> u64 {
    rustix::process::getrlimit(rustix::process::Resource::Nofile)
        .current
        .unwrap_or(u64::MAX)
}

/// Fill `bufs`, one after another, from offset `off` of `file`, with as few
/// `preadv` calls as the OS allows.
pub(crate) fn read_exact_vectored_at(
    file: &std::fs::File,
    bufs: &mut [&mut [u8]],
    off: u64,
) -> std::io::Result<()> {
    use std::io::IoSliceMut;
    let mut slices: Vec<IoSliceMut<'_>> = bufs.iter_mut().map(|b| IoSliceMut::new(b)).collect();
    let mut rest = &mut slices[..];
    let mut off = off;
    while !rest.is_empty() {
        let n = rustix::io::preadv(file, rest, off)?;
        if n == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        off += u64::try_from(n).unwrap_or(u64::MAX);
        IoSliceMut::advance_slices(&mut rest, n);
    }
    Ok(())
}

/// Inflate the zlib stream at the start of `z` to exactly fill `out`; the
/// bytes the stream takes. `libdeflater` does not report that count.
pub(crate) fn zlib_decompress_prefix(z: &[u8], out: &mut [u8]) -> Option<usize> {
    use libdeflate_sys::{
        libdeflate_alloc_decompressor, libdeflate_decompressor, libdeflate_free_decompressor,
        libdeflate_result_LIBDEFLATE_SUCCESS, libdeflate_zlib_decompress_ex,
    };
    struct Decoder(*mut libdeflate_decompressor);
    impl Drop for Decoder {
        fn drop(&mut self) {
            // SAFETY: the pointer came from `libdeflate_alloc_decompressor`
            // (null is allowed) and is freed only here.
            unsafe { libdeflate_free_decompressor(self.0) }
        }
    }
    thread_local! {
        // SAFETY: allocation has no preconditions; null is checked on use.
        static DECODER: Decoder = Decoder(unsafe { libdeflate_alloc_decompressor() });
    }
    DECODER.with(|d| {
        if d.0.is_null() {
            return None;
        }
        let (mut used, mut n) = (0, 0);
        // SAFETY: the decoder is this thread's own and not in use; the
        // pointers and lengths come from live slices.
        let rc = unsafe {
            libdeflate_zlib_decompress_ex(
                d.0,
                z.as_ptr().cast(),
                z.len(),
                out.as_mut_ptr().cast(),
                out.len(),
                &raw mut used,
                &raw mut n,
            )
        };
        (rc == libdeflate_result_LIBDEFLATE_SUCCESS && n == out.len()).then_some(used)
    })
}

#[cfg(test)]
mod tests {
    use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};

    use super::*;

    /// The node plans its file budget from this on every call, so it has to
    /// read the process's own limit rather than a value fixed at build time.
    /// Raising the soft limit to the hard one is the one change a test can
    /// make to it and see.
    #[test]
    fn the_reported_limit_is_the_processs_own() {
        let limit = getrlimit(Resource::Nofile);
        let Some(hard) = limit.maximum else {
            // No hard limit: the soft limit is already the ceiling.
            assert_eq!(open_file_limit(), limit.current.unwrap_or(u64::MAX));
            return;
        };
        if limit.current.is_some_and(|soft| soft >= hard) {
            assert_eq!(open_file_limit(), hard);
            return;
        }
        setrlimit(
            Resource::Nofile,
            Rlimit {
                current: Some(hard),
                maximum: Some(hard),
            },
        )
        .expect("raising the soft limit to the hard one is allowed");
        assert_eq!(open_file_limit(), hard);
    }

    /// A file offset the OS holds whole is read whole, across several
    /// buffers: the call is what every local layer read goes through.
    #[test]
    fn a_vectored_read_fills_every_buffer_in_order() {
        use std::io::Write as _;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("layer");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(b"0123456789abcdef").unwrap();
        drop(f);
        let file = std::fs::File::open(&path).unwrap();
        let (mut a, mut b, mut c) = ([0u8; 3], [0u8; 5], [0u8; 4]);
        read_exact_vectored_at(&file, &mut [&mut a, &mut b, &mut c], 2).unwrap();
        assert_eq!((&a, &b, &c), (b"234", b"56789", b"abcd"));
        // Past the end there is not enough to fill them, which is not a
        // short read to retry: the file is the wrong size.
        let mut big = [0u8; 8];
        let mut rest = [0u8; 8];
        let err = read_exact_vectored_at(&file, &mut [&mut big, &mut rest], 12).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }
}
