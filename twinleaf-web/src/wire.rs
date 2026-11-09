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

/// The bytes before a frame's body.
const HEADER: usize = 16;

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
