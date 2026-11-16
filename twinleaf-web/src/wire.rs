//! The binary frames the page decodes, little endian. Each starts with
//!
//! ```text
//! u8 kind, 7 × u8 pad, u64 seq
//! ```
//!
//! where `seq` numbers the change, or is 0 in a snapshot, as `protocol`
//! says. Points, kind 1:
//!
//! ```text
//! u16 chunk count, then per chunk:
//!   u16 trace id, u8 break before the first point, u8 level, u32 n,
//!   n × f64 x (seconds, device time), n × f32 y
//! ```
//!
//! A chunk with its break set starts a new line. The page lifts the pen
//! before its first point, as at a gap in the samples. `level` names the
//! decimation the points are of, 0 the finest, as the hub's `FpcsLevels`
//! numbers them. Each level of a trace is a line of its own.
//!
//! Spectra, kind 2, one per trace estimated in a round:
//!
//! ```text
//! u16 spectrum count, 6 × u8 pad, then per spectrum:
//!   u16 trace id, u16 pad, u32 n,
//!   f64 noise floor (NaN when there is none),
//!   n × f32 frequency (Hz), n × f32 density (units per √Hz)
//! ```
//!
//! A capture, kind 3, goes to the pages that asked for it. It is no change
//! of what every page holds, so its `seq` is 0.
//!
//! ```text
//! u64 number (the hub's count of this RPC's captures, from 1),
//! u32 n, u8 element type, u8 element bytes, u16 pad,
//! f32 y calibration, f32 x offset, f32 x stride, u32 pad,
//! 6 × u16 bytes of route, rpc, name, units, x name, x units, u32 pad,
//! the six strings, UTF-8, then zeros up to a multiple of 8 bytes,
//! n × element, as the device sent them, then zeros up to a multiple of
//! 8 bytes, u32 m, u32 pad,
//! m × f32 frequency, m × f32 density
//! ```
//!
//! Element `i` is `element × y calibration` in `units`, at
//! `x offset + i × x stride` in the x units. The element type is the
//! protocol's code, 0x10 for u8, 0x11 i8, 0x20 u16, 0x21 i16, 0x40 u32,
//! 0x41 i32, 0x42 f32, 0x80 u64, 0x81 i64 and 0x82 f64. Its high half is
//! the bytes of one element. The elements start at a multiple of 8 bytes,
//! so a page reads them in place as an array of their type. The `m` bins
//! after them are the capture's amplitude spectral density, as the hub
//! estimates a signal's (`captures::spectrum_of`), along the reciprocal of
//! its x: frequency in cycles per x unit, density in its units per root
//! cycle per x unit; none where it has none.

use twinleaf::device::capture::CaptureReadout;

/// The bytes before a frame's body.
const HEADER: usize = 16;
/// The bytes of a capture frame before its strings.
const CAPTURE_HEAD: usize = HEADER + 48;

/// One trace's points in time order.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Chunk {
    pub(crate) id: u16,
    /// The decimation the points are of, 0 the finest.
    pub(crate) level: u8,
    /// Whether a gap comes before the first point.
    pub(crate) brk: bool,
    pub(crate) points: Vec<(f64, f32)>,
}

/// One trace's spectrum: its `(frequency, density)` bins and its noise floor.
pub(crate) struct Spectrum<'a> {
    pub(crate) id: u16,
    pub(crate) bins: &'a [(f64, f64)],
    pub(crate) floor: Option<f64>,
}

fn header(kind: u8, seq: u64, body: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(HEADER + body);
    buf.extend_from_slice(&[kind, 0, 0, 0, 0, 0, 0, 0]);
    buf.extend_from_slice(&seq.to_le_bytes());
    buf
}

/// Encodes `chunks` as one frame, change `seq`.
pub(crate) fn encode(chunks: &[Chunk], seq: u64) -> Vec<u8> {
    let points: usize = chunks.iter().map(|c| c.points.len()).sum();
    let mut buf = header(1, seq, 2 + chunks.len() * 8 + points * 12);
    buf.extend_from_slice(&(chunks.len() as u16).to_le_bytes());
    for chunk in chunks {
        buf.extend_from_slice(&chunk.id.to_le_bytes());
        buf.push(chunk.brk as u8);
        buf.push(chunk.level);
        buf.extend_from_slice(&(chunk.points.len() as u32).to_le_bytes());
        for &(x, _) in &chunk.points {
            buf.extend_from_slice(&x.to_le_bytes());
        }
        for &(_, y) in &chunk.points {
            buf.extend_from_slice(&y.to_le_bytes());
        }
    }
    buf
}

/// Encodes `spectra` as one frame, change `seq`.
pub(crate) fn encode_spectra(spectra: &[Spectrum], seq: u64) -> Vec<u8> {
    let bins: usize = spectra.iter().map(|s| s.bins.len()).sum();
    let mut buf = header(2, seq, 8 + spectra.len() * 16 + bins * 8);
    buf.extend_from_slice(&(spectra.len() as u16).to_le_bytes());
    buf.extend_from_slice(&[0; 6]);
    for spectrum in spectra {
        buf.extend_from_slice(&spectrum.id.to_le_bytes());
        buf.extend_from_slice(&[0, 0]);
        buf.extend_from_slice(&(spectrum.bins.len() as u32).to_le_bytes());
        buf.extend_from_slice(&spectrum.floor.unwrap_or(f64::NAN).to_le_bytes());
        for &(f, _) in spectrum.bins {
            buf.extend_from_slice(&(f as f32).to_le_bytes());
        }
        for &(_, a) in spectrum.bins {
            buf.extend_from_slice(&(a as f32).to_le_bytes());
        }
    }
    buf
}

/// Encodes the capture `readout` holds as one frame, capture `number` of
/// `rpc` on the device at `route`, with `spectrum`, its `(frequency,
/// density)` bins: its elements, then the bins.
pub(crate) fn encode_capture(
    route: &str,
    rpc: &str,
    number: u64,
    readout: &CaptureReadout,
    spectrum: &[(f64, f64)],
) -> Vec<u8> {
    let meta = &readout.metadata;
    let strings = [
        route,
        rpc,
        &meta.name,
        &meta.units,
        &meta.x_name,
        &meta.x_units,
    ];
    let text: usize = strings.iter().map(|string| string.len()).sum();
    let padded = text.next_multiple_of(8);
    let data = readout.data.len().next_multiple_of(8);
    let mut buf = header(3, 0, 48 + padded + data + 8 + spectrum.len() * 8);
    buf.extend_from_slice(&number.to_le_bytes());
    buf.extend_from_slice(&meta.length.to_le_bytes());
    buf.push(meta.data_type.value());
    buf.push(meta.data_type.size() as u8);
    buf.extend_from_slice(&[0; 2]);
    buf.extend_from_slice(&meta.y_calibration.to_le_bytes());
    buf.extend_from_slice(&meta.x_offset.to_le_bytes());
    buf.extend_from_slice(&meta.x_stride.to_le_bytes());
    buf.extend_from_slice(&[0; 4]);
    for string in strings {
        buf.extend_from_slice(&(string.len() as u16).to_le_bytes());
    }
    buf.extend_from_slice(&[0; 4]);
    for string in strings {
        buf.extend_from_slice(string.as_bytes());
    }
    buf.resize(CAPTURE_HEAD + padded, 0);
    buf.extend_from_slice(&readout.data);
    buf.resize(CAPTURE_HEAD + padded + data, 0);
    buf.extend_from_slice(&(spectrum.len() as u32).to_le_bytes());
    buf.extend_from_slice(&[0; 4]);
    for &(f, _) in spectrum {
        buf.extend_from_slice(&(f as f32).to_le_bytes());
    }
    for &(_, a) in spectrum {
        buf.extend_from_slice(&(a as f32).to_le_bytes());
    }
    buf
}

/// A capture frame as a page reads it.
#[cfg(test)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Captured {
    pub(crate) route: String,
    pub(crate) rpc: String,
    pub(crate) number: u64,
    pub(crate) data_type: u8,
    pub(crate) y_calibration: f32,
    pub(crate) x_offset: f32,
    pub(crate) x_stride: f32,
    pub(crate) name: String,
    pub(crate) units: String,
    pub(crate) x_name: String,
    pub(crate) x_units: String,
    /// The elements, as many bytes as the frame says they take.
    pub(crate) data: Vec<u8>,
    /// The spectrum's `(frequency, density)` bins.
    pub(crate) spectrum: Vec<(f32, f32)>,
}

/// Decodes a capture frame, as the page does.
#[cfg(test)]
pub(crate) fn decode_capture(frame: &[u8]) -> Captured {
    let u16_at = |at: usize| usize::from(u16::from_le_bytes([frame[at], frame[at + 1]]));
    let u32_at = |at: usize| u32::from_le_bytes(frame[at..at + 4].try_into().unwrap());
    let f32_at = |at: usize| f32::from_bits(u32_at(at));
    assert_eq!(frame[0], 3, "a capture frame");
    assert_eq!(frame[8..HEADER], [0; 8], "a capture is no numbered change");
    let (n, element) = (u32_at(24) as usize, usize::from(frame[29]));
    assert_eq!(element, usize::from(frame[28] >> 4));
    let mut at = CAPTURE_HEAD;
    let strings: Vec<String> = (0..6)
        .map(|index| {
            let len = u16_at(48 + 2 * index);
            let string = String::from_utf8(frame[at..at + len].to_vec()).unwrap();
            at += len;
            string
        })
        .collect();
    let data = at.next_multiple_of(8);
    assert!(frame[at..data].iter().all(|&pad| pad == 0));
    let end = data + n * element;
    let bins = end.next_multiple_of(8);
    assert!(frame[end..bins].iter().all(|&pad| pad == 0));
    let m = u32_at(bins) as usize;
    assert_eq!(
        frame.len(),
        bins + 8 + m * 8,
        "its elements and its bins and no more"
    );
    let spectrum = (0..m)
        .map(|i| (f32_at(bins + 8 + 4 * i), f32_at(bins + 8 + 4 * (m + i))))
        .collect();
    let [route, rpc, name, units, x_name, x_units] = strings.try_into().unwrap();
    Captured {
        route,
        rpc,
        number: u64::from_le_bytes(frame[16..24].try_into().unwrap()),
        data_type: frame[28],
        y_calibration: f32_at(32),
        x_offset: f32_at(36),
        x_stride: f32_at(40),
        name,
        units,
        x_name,
        x_units,
        data: frame[data..end].to_vec(),
        spectrum,
    }
}

/// Splits time-ordered points of level `level` into chunks at the gap times
/// in `breaks`, given the time of the point before the first one.
pub(crate) fn split_at_breaks(
    id: u16,
    level: u8,
    before: f64,
    points: &[(f64, f64)],
    breaks: &[f64],
) -> Vec<Chunk> {
    let mut chunks: Vec<Chunk> = Vec::new();
    let mut prev = before;
    let mut current: Vec<(f64, f32)> = Vec::new();
    let mut brk = false;
    for &(t, v) in points {
        // A break lies between the previous point and this one.
        let i = breaks.partition_point(|&b| b <= prev);
        if breaks.get(i).is_some_and(|&b| b <= t) && prev.is_finite() {
            if !current.is_empty() {
                chunks.push(Chunk {
                    id,
                    level,
                    brk,
                    points: std::mem::take(&mut current),
                });
            }
            brk = true;
        }
        current.push((t, v as f32));
        prev = t;
    }
    if !current.is_empty() {
        chunks.push(Chunk {
            id,
            level,
            brk,
            points: current,
        });
    }
    chunks
}

/// Decodes a points frame back into its chunks, as the page does.
#[cfg(test)]
pub(crate) fn decode(frame: &[u8]) -> Vec<Chunk> {
    let u16_at = |at: usize| u16::from_le_bytes([frame[at], frame[at + 1]]);
    let u32_at = |at: usize| u32::from_le_bytes(frame[at..at + 4].try_into().unwrap()) as usize;
    assert_eq!(frame[0], 1, "a points frame");
    let mut chunks = Vec::new();
    let mut at = HEADER + 2;
    for _ in 0..u16_at(HEADER) {
        let (id, brk, level, n) = (
            u16_at(at),
            frame[at + 2] == 1,
            frame[at + 3],
            u32_at(at + 4),
        );
        at += 8;
        let points = (0..n)
            .map(|i| {
                let x = f64::from_le_bytes(frame[at + i * 8..at + i * 8 + 8].try_into().unwrap());
                let y = f32::from_le_bytes(
                    frame[at + n * 8 + i * 4..at + n * 8 + i * 4 + 4]
                        .try_into()
                        .unwrap(),
                );
                (x, y)
            })
            .collect();
        at += n * 12;
        chunks.push(Chunk {
            id,
            level,
            brk,
            points,
        });
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(id: u16, brk: bool, points: &[(f64, f32)]) -> Chunk {
        Chunk {
            id,
            level: 0,
            brk,
            points: points.to_vec(),
        }
    }

    #[test]
    fn a_frame_decodes_to_its_chunks() {
        let chunks = vec![
            chunk(3, false, &[(1.5, 2.0), (2.5, 3.0)]),
            Chunk {
                level: 3,
                ..chunk(4, true, &[(9.0, -1.0)])
            },
        ];
        assert_eq!(decode(&encode(&chunks, 9)), chunks);
    }

    #[test]
    fn a_gap_between_two_points_starts_a_new_chunk() {
        let points = [(1.0, 10.0), (2.0, 20.0), (5.0, 50.0)];
        let chunks = split_at_breaks(7, 0, f64::NEG_INFINITY, &points, &[3.0]);
        assert_eq!(
            chunks,
            vec![
                chunk(7, false, &[(1.0, 10.0), (2.0, 20.0)]),
                chunk(7, true, &[(5.0, 50.0)])
            ]
        );
    }

    #[test]
    fn a_gap_before_the_first_point_of_a_flush_breaks_it() {
        let chunks = split_at_breaks(7, 0, 2.0, &[(5.0, 50.0)], &[3.0]);
        assert_eq!(chunks, vec![chunk(7, true, &[(5.0, 50.0)])]);
    }

    #[test]
    fn a_frame_holds_its_chunks_in_order() {
        let frame = encode(&[chunk(3, true, &[(1.5, 2.0)])], 258);
        let mut expected = vec![1, 0, 0, 0, 0, 0, 0, 0, 2, 1, 0, 0, 0, 0, 0, 0];
        expected.extend_from_slice(&[1, 0, 3, 0, 1, 0, 1, 0, 0, 0]);
        expected.extend_from_slice(&1.5f64.to_le_bytes());
        expected.extend_from_slice(&2.0f32.to_le_bytes());
        assert_eq!(frame, expected);
    }

    fn capture() -> CaptureReadout {
        use twinleaf::device::capture::CaptureMetadata;
        CaptureReadout {
            metadata: CaptureMetadata {
                size: 6,
                blocksize: 256,
                data_type: twinleaf::proto::data::DataType::I16,
                length: 3,
                y_calibration: 0.5,
                x_offset: 1.0,
                x_stride: 0.25,
                name: "FID".into(),
                units: "V".into(),
                x_name: "Time".into(),
                x_units: "s".into(),
            },
            data: vec![1, 0, 0xfe, 0xff, 3, 0],
        }
    }

    #[test]
    fn a_capture_frame_holds_what_the_capture_is_of_then_its_elements() {
        let frame = encode_capture(
            "/0",
            "fid.capture",
            258,
            &capture(),
            &[(1.0, 0.5), (2.0, 0.25)],
        );
        let mut expected = vec![3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        expected.extend_from_slice(&[2, 1, 0, 0, 0, 0, 0, 0]);
        expected.extend_from_slice(&[3, 0, 0, 0, 0x21, 2, 0, 0]);
        for value in [0.5f32, 1.0, 0.25] {
            expected.extend_from_slice(&value.to_le_bytes());
        }
        expected.extend_from_slice(&[0; 4]);
        expected.extend_from_slice(&[2, 0, 11, 0, 3, 0, 1, 0, 4, 0, 1, 0, 0, 0, 0, 0]);
        expected.extend_from_slice(b"/0fid.captureFIDVTimes");
        expected.extend_from_slice(&[0; 2]);
        expected.extend_from_slice(&[1, 0, 0xfe, 0xff, 3, 0]);
        expected.extend_from_slice(&[0; 2]);
        expected.extend_from_slice(&[2, 0, 0, 0, 0, 0, 0, 0]);
        for value in [1.0f32, 2.0, 0.5, 0.25] {
            expected.extend_from_slice(&value.to_le_bytes());
        }
        assert_eq!(frame, expected);
        assert_eq!(
            (frame.len() - 6 - 2 - 8 - 16) % 8,
            0,
            "the elements start on 8 bytes"
        );
        // The page's tests decode these bytes from a file, and encode them.
        let kept = include_bytes!("../ui/tests/fixtures/capture-frame.bin");
        assert_eq!(frame, &kept[..], "the frame the page's tests keep");
    }

    #[test]
    fn a_capture_frame_decodes_to_its_capture() {
        let readout = capture();
        let decoded = decode_capture(&encode_capture(
            "/",
            "fid.capture",
            1,
            &readout,
            &[(3.0, 1.5)],
        ));
        assert_eq!(
            decoded,
            Captured {
                route: "/".into(),
                rpc: "fid.capture".into(),
                number: 1,
                data_type: 0x21,
                y_calibration: 0.5,
                x_offset: 1.0,
                x_stride: 0.25,
                name: "FID".into(),
                units: "V".into(),
                x_name: "Time".into(),
                x_units: "s".into(),
                data: readout.data,
                spectrum: vec![(3.0, 1.5)],
            }
        );
    }

    #[test]
    fn a_spectra_frame_holds_each_spectrum_frequencies_then_densities() {
        let frame = encode_spectra(
            &[Spectrum {
                id: 5,
                bins: &[(1.0, 0.5), (2.0, 0.25)],
                floor: None,
            }],
            0,
        );
        let mut expected = vec![2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        expected.extend_from_slice(&[1, 0, 0, 0, 0, 0, 0, 0]);
        expected.extend_from_slice(&[5, 0, 0, 0, 2, 0, 0, 0]);
        expected.extend_from_slice(&f64::NAN.to_le_bytes());
        for v in [1.0f32, 2.0, 0.5, 0.25] {
            expected.extend_from_slice(&v.to_le_bytes());
        }
        assert_eq!(frame, expected);
    }
}
