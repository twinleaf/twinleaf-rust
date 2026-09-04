use super::progress::ByteProgress;
use bytes::{Buf, Bytes};
use std::fs::File;
use std::io::{self, BufWriter, Write};
use twinleaf::data::PacketParser;
use twinleaf::proto::packet::{Header, PacketError as HeaderError};
use twinleaf::tio::packet::Packet;
use twinleaf::DeviceRoute;

/// Packets that must decode back to back before damaged bytes are taken to
/// have ended.
///
/// Damage in a real log is not random bytes: the exponent byte of an f64
/// sample is a valid stream packet type, and runs of six header-shaped
/// windows were found inside three kilobytes of garbled samples. Eight in a
/// row was never seen in damage.
const RESYNC_CHAIN: usize = 8;

/// Skipped regions listed one by one in the report.
const LISTED_SKIPS: usize = 8;

/// Progress is shown for inputs larger than this.
const PROGRESS_THRESHOLD: u64 = 10 * 1024 * 1024;

/// What a repair pass copied and what it left out.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct RepairReport {
    /// Packets copied to the output.
    kept: u64,
    /// Well-formed packets the parser rejected, left out of the copy.
    dropped: u64,
    /// Runs of bytes that did not decode, as `(offset, len)`.
    skipped: Vec<(usize, usize)>,
}

impl RepairReport {
    fn skipped_bytes(&self) -> u64 {
        self.skipped.iter().map(|&(_, len)| len as u64).sum()
    }

    fn is_clean(&self) -> bool {
        self.dropped == 0 && self.skipped.is_empty()
    }
}

/// Copy the packets of `data` that decode, that the next header follows
/// correctly, and that the parser accepts, to `keep`, in order.
///
/// A packet is trusted only when a packet header follows it, since damage
/// that keeps a packet's length can still garble its contents. Where that
/// fails, bytes are passed over one at a time until [`RESYNC_CHAIN`] packets
/// decode back to back or the data ends. Well-formed packets that contradict
/// the metadata retained for their stream are left out too, so a tool that
/// reads the copy never meets them.
pub(super) fn repair(
    data: &Bytes,
    mut keep: impl FnMut(&[u8]) -> io::Result<()>,
    mut progress: impl FnMut(usize),
) -> io::Result<RepairReport> {
    let mut report = RepairReport::default();
    let mut parser = PacketParser::new(DeviceRoute::root(), false);
    let mut pos = 0;

    while pos < data.len() {
        if let Ok((packet, len)) = Packet::from_wire_prefix(&data.slice(pos..)) {
            if frames_correctly(&data[pos + len..]) {
                match parser.push_packet(&packet) {
                    Ok(_) => {
                        keep(packet.as_bytes())?;
                        report.kept += 1;
                    }
                    Err(error) => {
                        log::debug!("dropping packet at byte offset {pos}: {error}");
                        report.dropped += 1;
                    }
                }
                while parser.pop_batch().is_some() {}
                pos += len;
                progress(pos);
                continue;
            }
        }

        let skip = (1..data.len() - pos)
            .find(|&skip| chain_decodes(data.slice(pos + skip..)))
            .unwrap_or(data.len() - pos);
        report.skipped.push((pos, skip));
        pos += skip;
        progress(pos);
    }

    Ok(report)
}

/// Whether `rest` is empty, begins with a packet header, or is too short to
/// hold one. A cut-off tail is reported when it is reached, not here.
fn frames_correctly(rest: &[u8]) -> bool {
    matches!(
        Header::parse_prefix(rest),
        Ok(_) | Err(HeaderError::NeedMore)
    )
}

/// Whether `data` begins with [`RESYNC_CHAIN`] packets that decode, or fewer
/// followed by the end of the data.
fn chain_decodes(mut data: Bytes) -> bool {
    for _ in 0..RESYNC_CHAIN {
        if data.is_empty() {
            return true;
        }
        match Packet::from_wire_prefix(&data) {
            Ok((_, len)) => data.advance(len),
            Err(_) => return false,
        }
    }
    true
}

fn default_output_path(input: &str) -> String {
    let base = input.strip_suffix(".tio").unwrap_or(input);
    format!("{base}_repaired.tio")
}

pub fn log_repair(input: String, output: Option<String>, force: bool) -> eyre::Result<()> {
    use color_eyre::Help;
    use console::style;
    use eyre::{bail, WrapErr};

    let output_path = output.unwrap_or_else(|| default_output_path(&input));
    if output_path == input {
        bail!("output path must be different from input");
    }
    if !force && std::path::Path::new(&output_path).exists() {
        return Err(eyre::eyre!("output {} already exists", output_path)
            .suggestion("pass --force to overwrite, or use -o for a different name"));
    }

    let file = File::open(&input).wrap_err_with(|| format!("could not open {}", input))?;
    // SAFETY: the input must not be modified or truncated while it is mapped,
    // the same condition `twinleaf::data::LogFile` places on its callers.
    let mmap = unsafe { memmap2::Mmap::map(&file) }
        .wrap_err_with(|| format!("could not mmap {}", input))?;
    let data = Bytes::from_owner(mmap);
    let total_bytes = data.len() as u64;

    let out =
        File::create(&output_path).wrap_err_with(|| format!("could not create {}", output_path))?;
    let mut writer = BufWriter::new(out);
    let mut written: u64 = 0;

    let mut progress = (total_bytes > PROGRESS_THRESHOLD).then(|| ByteProgress::new(total_bytes));
    let report = repair(
        &data,
        |bytes| {
            written += bytes.len() as u64;
            writer.write_all(bytes)
        },
        |position| {
            if let Some(progress) = &mut progress {
                progress.update(position as u64);
            }
        },
    )
    .and_then(|report| writer.flush().map(|()| report))
    .wrap_err_with(|| format!("failed to write {}", output_path))?;
    if let Some(progress) = progress {
        progress.finish_and_clear(total_bytes);
    }

    if report.kept == 0 {
        drop(writer);
        let _ = std::fs::remove_file(&output_path);
        bail!("{} contains no packets that decode", input);
    }

    let mib = |bytes: u64| bytes as f64 / 1_048_576.0;
    let rule = style("─".repeat(50)).dim();
    let label = |s: &str| style(format!("{:12}", s)).bold().cyan();
    let unit = |s: &str| style(s.to_string()).dim();

    println!();
    println!("{rule}");
    println!(" {}", style("Log Repair").bold());
    println!("{rule}");
    println!(" {} {}", label("Input:"), input);
    println!(" {} {}", label("Output:"), output_path);
    println!(
        " {} {:.2} {} → {:.2} {}",
        label("Size:"),
        mib(total_bytes),
        unit("MiB"),
        mib(written),
        unit("MiB")
    );
    println!(" {} {} {}", label("Kept:"), report.kept, unit("packets"));
    if report.is_clean() {
        println!(" {} none; the input was already clean", label("Damage:"));
    } else {
        if report.dropped > 0 {
            println!(
                " {} {}",
                label("Dropped:"),
                style(format!(
                    "{} well-formed {} the parser rejected",
                    report.dropped,
                    plural(report.dropped, "packet", "packets")
                ))
                .yellow()
            );
        }
        if !report.skipped.is_empty() {
            let regions = report.skipped.len() as u64;
            println!(
                " {} {}",
                label("Skipped:"),
                style(format!(
                    "{} undecodable {} ({} bytes)",
                    regions,
                    plural(regions, "region", "regions"),
                    report.skipped_bytes()
                ))
                .yellow()
            );
            let listed: Vec<String> = report
                .skipped
                .iter()
                .take(LISTED_SKIPS)
                .map(|(offset, len)| format!("{len} B at {offset}"))
                .collect();
            let more = report.skipped.len().saturating_sub(LISTED_SKIPS);
            let tail = if more > 0 {
                format!(", and {more} more")
            } else {
                String::new()
            };
            println!(
                "        {}",
                style(format!("{}{}", listed.join(", "), tail)).dim()
            );
            if let Some(&(offset, len)) = report.skipped.last() {
                if offset + len == data.len() {
                    println!(
                        "        {}",
                        style("the last region runs to the end of the file").dim()
                    );
                }
            }
        }
    }
    println!("{rule}");

    Ok(())
}

fn plural(count: u64, one: &'static str, many: &'static str) -> &'static str {
    if count == 1 {
        one
    } else {
        many
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use twinleaf::proto::data as wire;
    use twinleaf::proto::data::DataType;
    use twinleaf::proto::sync::Epoch;
    use twinleaf::{ColumnId, SegmentId, SessionId, StreamId};

    /// Bytes that are not a packet: the type byte 0x24 does not exist.
    const DAMAGE: [u8; 9] = [0x24, 0x4f, 0xfb, 0xfe, 0xa0, 0xd9, 0x40, 0x65, 0x8e];

    const STREAM: u8 = 1;

    fn heartbeats(count: usize) -> Vec<Packet> {
        (1..=count as u32)
            .map(|session| Packet::heartbeat_session(session, DeviceRoute::root()))
            .collect()
    }

    fn encoded(packets: &[Packet]) -> Vec<u8> {
        packets.iter().flat_map(|p| p.as_bytes().to_vec()).collect()
    }

    fn run(bytes: Vec<u8>) -> (Vec<u8>, RepairReport) {
        let mut out = Vec::new();
        let report = repair(
            &Bytes::from(bytes),
            |bytes| {
                out.extend_from_slice(bytes);
                Ok(())
            },
            |_| {},
        )
        .expect("writing to a Vec cannot fail");
        (out, report)
    }

    /// The four records that let a parser decode one f32 stream with a ring
    /// of two segments.
    fn metadata_packets() -> Vec<Packet> {
        let records = [
            wire::Metadata::Device(wire::Device {
                session: SessionId::new(7),
                n_streams: 1,
                name: "d",
                serial: "s",
                firmware: "f",
            }),
            wire::Metadata::Stream(wire::Stream {
                stream_id: StreamId::new(STREAM),
                n_columns: 1,
                n_segments: 2,
                sample_size: 4,
                buf_samples: 128,
                name: "stream",
            }),
            wire::Metadata::Segment(wire::Segment {
                stream_id: StreamId::new(STREAM),
                segment_id: SegmentId::new(0),
                flags: wire::SegmentFlags::default(),
                epoch: Epoch::UNIX,
                timeref_serial: "clock",
                timeref_session: SessionId::new(7),
                start_time: 0,
                sampling_rate: 1,
                decimation: 1,
                filter_cutoff: 0.0,
                filter_type: wire::FilterType::NONE,
            }),
            wire::Metadata::Column(wire::Column {
                stream_id: StreamId::new(STREAM),
                index: ColumnId::new(0),
                data_type: DataType::F32,
                name: "col",
                units: "",
                description: "",
            }),
        ];
        records
            .into_iter()
            .map(|record| {
                Packet::metadata(record, wire::MetadataFlags::default(), DeviceRoute::root())
                    .expect("one record fits")
            })
            .collect()
    }

    #[test]
    fn a_clean_log_is_copied_whole() {
        let good = encoded(&heartbeats(3));

        let (out, report) = run(good.clone());

        assert_eq!(out, good);
        assert_eq!(
            report,
            RepairReport {
                kept: 3,
                dropped: 0,
                skipped: vec![],
            }
        );
        assert!(report.is_clean());
    }

    /// The packet right before damage goes with it: its length may be intact
    /// while its contents are not, and only the header after it would tell.
    #[test]
    fn damage_is_left_out_with_the_packet_before_it() {
        let front = heartbeats(RESYNC_CHAIN + 1);
        let back = encoded(&heartbeats(RESYNC_CHAIN));
        let mut bytes = DAMAGE.to_vec();
        bytes.extend_from_slice(&encoded(&front));
        let last_front = bytes.len() - front[RESYNC_CHAIN].as_bytes().len();
        bytes.extend_from_slice(&[0x09, 0xff, 0xff]);
        bytes.extend_from_slice(&back);

        let (out, report) = run(bytes);

        assert_eq!(out, [encoded(&front[..RESYNC_CHAIN]), back].concat());
        assert_eq!(report.kept, 2 * RESYNC_CHAIN as u64);
        assert_eq!(report.dropped, 0);
        assert_eq!(
            report.skipped,
            [
                (0, DAMAGE.len()),
                (last_front, front[RESYNC_CHAIN].as_bytes().len() + 3)
            ]
        );
        assert_eq!(report.skipped_bytes(), DAMAGE.len() as u64 + 8 + 3);
    }

    /// Damage that keeps a packet's length garbles what is inside it, so a
    /// packet counts only when a header follows it.
    #[test]
    fn a_packet_not_followed_by_a_header_goes_with_the_damage() {
        let front = encoded(&heartbeats(RESYNC_CHAIN));
        let back = encoded(&heartbeats(RESYNC_CHAIN));
        let suspect = Packet::heartbeat_session(99, DeviceRoute::root());
        let mut bytes = front.clone();
        let at = bytes.len();
        bytes.extend_from_slice(suspect.as_bytes());
        bytes.extend_from_slice(&DAMAGE[..2]);
        bytes.extend_from_slice(&back);

        let (out, report) = run(bytes);

        assert_eq!(out, [front, back].concat());
        assert_eq!(report.skipped, [(at, suspect.as_bytes().len() + 2)]);
    }

    #[test]
    fn a_cut_off_last_packet_is_left_out() {
        let good = encoded(&heartbeats(3));
        let mut bytes = good.clone();
        let at = bytes.len();
        bytes.extend_from_slice(&Packet::heartbeat_session(4, DeviceRoute::root()).as_bytes()[..5]);

        let (out, report) = run(bytes);

        assert_eq!(out, good);
        assert_eq!(report.kept, 3);
        assert_eq!(report.skipped, [(at, 5)]);
    }

    #[test]
    fn a_packet_the_parser_rejects_is_left_out() {
        let root = DeviceRoute::root();
        let metadata = metadata_packets();
        let outside_ring = Packet::samples(STREAM, 5, 0, &[0; 4], root).expect("valid samples");
        let inside_ring = Packet::samples(STREAM, 0, 0, &[0; 4], root).expect("valid samples");
        let mut bytes = encoded(&metadata);
        bytes.extend_from_slice(outside_ring.as_bytes());
        bytes.extend_from_slice(inside_ring.as_bytes());
        let mut expected = encoded(&metadata);
        expected.extend_from_slice(inside_ring.as_bytes());

        let (out, report) = run(bytes);

        assert_eq!(out, expected);
        assert_eq!(report.kept, metadata.len() as u64 + 1);
        assert_eq!(report.dropped, 1);
        assert!(report.skipped.is_empty());
    }

    #[test]
    fn nothing_decodable_is_one_skipped_region() {
        let (out, report) = run(DAMAGE.to_vec());

        assert!(out.is_empty());
        assert_eq!(report.kept, 0);
        assert_eq!(report.skipped, [(0, DAMAGE.len())]);
    }

    #[test]
    fn the_default_output_sits_beside_the_input() {
        assert_eq!(default_output_path("run.tio"), "run_repaired.tio");
        assert_eq!(default_output_path("logs/run"), "logs/run_repaired.tio");
    }
}
