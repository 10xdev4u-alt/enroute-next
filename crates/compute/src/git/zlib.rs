use std::cell::RefCell;

use anyhow::{Context, Result, anyhow, ensure};
use libdeflater::{CompressionLvl, Compressor, Decompressor};

thread_local! {
    // A new coder allocates its state; reuse keeps that off the per-object path.
    static ENCODER: RefCell<Compressor> = RefCell::new(Compressor::new(CompressionLvl::default()));
    // Level 0: stored blocks only.
    static STORED: RefCell<Compressor> = RefCell::new(Compressor::new(CompressionLvl::fastest()));
    static DECODER: RefCell<Decompressor> = RefCell::new(Decompressor::new());
}

/// Inputs below this size go in a stored block: deflate makes them larger,
/// and its setup costs more than the copy.
const STORED_BELOW: usize = 64;

/// Append the zlib stream of `data` to `out`.
pub(crate) fn compress(data: &[u8], out: &mut Vec<u8>) {
    let encoder = if data.len() < STORED_BELOW {
        &STORED
    } else {
        &ENCODER
    };
    encoder.with_borrow_mut(|c| {
        let start = out.len();
        out.resize(start + c.zlib_compress_bound(data.len()), 0);
        #[expect(
            clippy::expect_used,
            reason = "the buffer is at the compress bound, so it cannot be too small"
        )]
        let n = c
            .zlib_compress(data, out.get_mut(start..).unwrap_or_default())
            .expect("in-memory compress");
        out.truncate(start + n);
    });
}

/// Inflate the zlib stream `data` to exactly `expected` bytes.
pub(crate) fn decompress(data: &[u8], expected: usize) -> Result<Vec<u8>> {
    DECODER.with_borrow_mut(|d| {
        let mut out = vec![0; expected];
        let n = d
            .zlib_decompress(data, &mut out)
            .map_err(|e| anyhow!("{e}"))
            .context("inflate")?;
        ensure!(n == expected, "zlib size mismatch: {n} != {expected}");
        Ok(out)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zlib(data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        compress(data, &mut out);
        out
    }

    #[test]
    fn a_failed_stream_does_not_affect_the_next() {
        let data: Vec<u8> = (0..100_000u32)
            .map(|i| u8::try_from(i * 7 % 251).unwrap())
            .collect();
        let z = zlib(&data);
        decompress(&z[..z.len() / 2], data.len()).unwrap_err();
        decompress(&z, data.len() - 1).unwrap_err();
        assert_eq!(decompress(&z, data.len()).unwrap(), data);
        assert_eq!(zlib(&data), z);
        assert_eq!(decompress(&zlib(b""), 0).unwrap(), b"");
        let mut out = b"head".to_vec();
        compress(&data, &mut out);
        assert_eq!(out[..4], *b"head");
        assert_eq!(out[4..], z);
    }

    #[test]
    fn tiny_inputs_go_stored() {
        // zlib header, block header, length and complement, adler32.
        assert_eq!(zlib(&[7; STORED_BELOW - 1]).len(), STORED_BELOW - 1 + 11);
    }

    #[test]
    fn stored_and_deflated_streams_read_as_zlib() {
        for n in [0, 1, STORED_BELOW - 1, STORED_BELOW, 5000] {
            let data: Vec<u8> = (0..n).map(|i| u8::try_from(i % 7).unwrap()).collect();
            let z = zlib(&data);
            assert_eq!(decompress(&z, n).unwrap(), data);
        }
    }
}
