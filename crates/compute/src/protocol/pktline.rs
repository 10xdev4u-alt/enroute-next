//! Pkt-line framing: writers and a reader.

use anyhow::{Context, Result, bail, ensure};

use super::Refusal;

pub(crate) const MAX_DATA: usize = 65516;

/// Appends `payload` as one data pkt-line.
pub fn data(out: &mut Vec<u8>, payload: &[u8]) {
    debug_assert!(payload.len() <= MAX_DATA);
    out.extend_from_slice(format!("{:04x}", payload.len() + 4).as_bytes());
    out.extend_from_slice(payload);
}

/// `line` split around the first `at`, which neither side keeps.
pub(crate) fn split_once(line: &[u8], at: u8) -> Option<(&[u8], &[u8])> {
    let index = line.iter().position(|byte| *byte == at)?;
    let (before, after) = line.split_at_checked(index)?;
    Some((before, after.get(1..)?))
}

/// [`data`] for a payload from outside, which may not fit one pkt-line.
///
/// # Errors
///
/// `payload` is empty, which reads back as a flush, or longer than [`MAX_DATA`].
pub(crate) fn try_data(out: &mut Vec<u8>, payload: &[u8]) -> Result<()> {
    ensure!(
        (1..=MAX_DATA).contains(&payload.len()),
        "a pkt-line carries 1 to {MAX_DATA} bytes, not {}",
        payload.len()
    );
    data(out, payload);
    Ok(())
}

/// Appends `s` as one data pkt-line.
pub fn line(out: &mut Vec<u8>, s: &str) {
    data(out, s.as_bytes());
}

/// Appends a flush packet.
pub fn flush(out: &mut Vec<u8>) {
    out.extend_from_slice(b"0000");
}

pub(crate) fn delim(out: &mut Vec<u8>) {
    out.extend_from_slice(b"0001");
}

/// Wrap `payload` in side-band packets on `band` (1 = data, 2 = progress, 3 = error).
pub(crate) fn sideband(out: &mut Vec<u8>, band: u8, payload: &[u8]) {
    for chunk in payload.chunks(MAX_DATA - 1) {
        out.extend_from_slice(format!("{:04x}", chunk.len() + 5).as_bytes());
        out.push(band);
        out.extend_from_slice(chunk);
    }
}

/// One packet of a pkt-line stream.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Pkt<'a> {
    /// A flush packet (`0000`).
    Flush,
    /// A delimiter packet (`0001`).
    Delim,
    /// A response-end packet (`0002`).
    ResponseEnd,
    /// A data packet, without its length prefix.
    Data(&'a [u8]),
}

pub(crate) struct Reader<'a> {
    buf: &'a [u8],
    pub pos: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    pub(crate) fn read(&mut self) -> Result<Option<Pkt<'a>>> {
        if self.pos == self.buf.len() {
            return Ok(None);
        }
        let prefix = self
            .buf
            .get(self.pos..self.pos + 4)
            .context("truncated pkt-line")?;
        let hex = std::str::from_utf8(prefix)?;
        let len = usize::from_str_radix(hex, 16)
            .ok()
            .with_context(|| format!("bad pkt-line length {hex:?}"))?;
        self.pos += 4;
        Ok(Some(match len {
            0 => Pkt::Flush,
            1 => Pkt::Delim,
            2 => Pkt::ResponseEnd,
            3 => bail!(Refusal::new("invalid pkt-line length 3")),
            n => {
                let d = self
                    .buf
                    .get(self.pos..self.pos + n - 4)
                    .context("truncated pkt-line payload")?;
                self.pos += n - 4;
                Pkt::Data(d)
            }
        }))
    }

    /// Next data line as a string with the trailing newline removed; `None`
    /// for any special packet or end of input.
    pub(crate) fn next_line(&mut self) -> Result<Option<Result<&'a str, Pkt<'a>>>> {
        Ok(match self.read()? {
            None => None,
            Some(Pkt::Data(d)) => {
                let s = std::str::from_utf8(d)?;
                Some(Ok(s.strip_suffix('\n').unwrap_or(s)))
            }
            Some(p) => Some(Err(p)),
        })
    }
}

/// Every packet up to the end or to the first error.
impl<'a> Iterator for Reader<'a> {
    type Item = Result<Pkt<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read() {
            Ok(pkt) => pkt.map(Ok),
            Err(error) => {
                self.pos = self.buf.len();
                Some(Err(error))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_DATA, Pkt, Reader, flush, try_data};

    fn any_payloads() -> impl proptest::strategy::Strategy<Value = Vec<Vec<u8>>> {
        use proptest::prelude::*;
        proptest::collection::vec(proptest::collection::vec(any::<u8>(), 1..200), 0..8)
    }

    proptest::proptest! {
        #[test]
        fn packets_roundtrip_any_data_lines(payloads in any_payloads()) {
            let mut buf = Vec::new();
            for payload in &payloads {
                try_data(&mut buf, payload).unwrap();
            }
            flush(&mut buf);
            let read: Vec<Pkt<'_>> = Reader::new(&buf).collect::<anyhow::Result<_>>().unwrap();

            let mut expected: Vec<Pkt<'_>> = payloads.iter().map(|p| Pkt::Data(p)).collect();
            expected.push(Pkt::Flush);
            proptest::prop_assert_eq!(read, expected);
        }
    }

    // What follows a flush is the next section's.
    #[test]
    fn reads_on_past_a_flush() {
        let read: Vec<Pkt<'_>> = Reader::new(b"000ahello\n00000001")
            .collect::<anyhow::Result<_>>()
            .unwrap();
        assert_eq!(read, vec![Pkt::Data(b"hello\n"), Pkt::Flush, Pkt::Delim]);
    }

    #[test]
    fn refuses_what_is_not_a_pkt_line() {
        for bad in [&b"00"[..], b"000abc", b"zzzz"] {
            let mut reader = Reader::new(bad);
            assert!(reader.next().is_some_and(|p| p.is_err()), "{bad:?}");
            assert!(reader.next().is_none(), "{bad:?}");
        }
    }

    #[test]
    fn refuses_a_payload_no_pkt_line_carries() {
        try_data(&mut Vec::new(), b"").unwrap_err();
        try_data(&mut Vec::new(), &vec![b'x'; MAX_DATA + 1]).unwrap_err();
    }

    /// `split_once` takes the first separator and keeps it out of both halves.
    /// Its callers read the name before the space as a command and the rest as
    /// arguments, so a second separator must stay in the second half.
    #[test]
    fn split_once_takes_the_first_separator() {
        for (line, expected) in [
            (&b"a b"[..], Some((&b"a"[..], &b"b"[..]))),
            (&b"a b c"[..], Some((&b"a"[..], &b"b c"[..]))),
            (&b" a"[..], Some((&b""[..], &b"a"[..]))),
            (&b"a "[..], Some((&b"a"[..], &b""[..]))),
            (&b" "[..], Some((&b""[..], &b""[..]))),
            (&b"abc"[..], None),
            (&b""[..], None),
        ] {
            assert_eq!(super::split_once(line, b' '), expected, "{line:?}");
        }
    }

    /// The largest legal payload is exactly `MAX_DATA`, and one byte more is
    /// not. The writers bracket the reader, so this is where an off-by-one in
    /// the length prefix would show.
    #[test]
    fn the_largest_payload_is_legal_and_one_more_is_not() {
        let largest = vec![b'x'; MAX_DATA];
        let mut buf = Vec::new();
        try_data(&mut buf, &largest).unwrap();
        let read: Vec<Pkt<'_>> = Reader::new(&buf).collect::<anyhow::Result<_>>().unwrap();
        assert_eq!(read, vec![Pkt::Data(&largest)]);

        try_data(&mut Vec::new(), &vec![b'x'; MAX_DATA + 1]).unwrap_err();
    }

    /// A side-band line carries one byte of band, so a payload longer than
    /// `MAX_DATA - 1` splits. A reader must reassemble the chunks in order.
    #[test]
    fn a_side_band_payload_splits_and_reassembles() {
        use super::sideband;

        let payload: Vec<u8> = (0..(MAX_DATA * 2))
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect();
        let mut buf = Vec::new();
        sideband(&mut buf, 2, &payload);

        let mut seen = Vec::new();
        for pkt in Reader::new(&buf) {
            match pkt.unwrap() {
                Pkt::Data(line) => {
                    let (band, data) = line.split_first().expect("a band byte");
                    assert_eq!(*band, 2);
                    seen.extend_from_slice(data);
                }
                other => panic!("unexpected packet {other:?}"),
            }
        }
        assert_eq!(seen, payload);
    }
}
