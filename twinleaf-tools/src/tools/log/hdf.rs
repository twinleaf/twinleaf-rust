use super::{
    progress::ByteProgress, record_parse_result_parts, report_missing_metadata, unparseable_routes,
    LOG_BATCH_ROWS,
};
use crate::{SplitLevel, SplitPolicy};
use std::collections::HashSet;
use twinleaf::data::{LogFile, PacketParser};
use twinleaf::device::DeviceRoute;
use twinleaf::tio;

pub fn log_hdf(
    files: Vec<String>,
    output: Option<String>,
    filter: Option<String>,
    compress: bool,
    debug: bool,
    split_level: SplitLevel,
    split_policy: SplitPolicy,
) -> eyre::Result<()> {
    use eyre::WrapErr;
    use std::path::Path;
    use twinleaf::data::{export, ColumnFilter};

    let last_input = files
        .last()
        .ok_or_else(|| eyre::eyre!("missing log file"))?;

    // Determine output filename
    let output = match output {
        Some(o) => o,
        None => {
            let input_path = Path::new(last_input);
            let stem = input_path.file_stem().unwrap_or_default().to_string_lossy();
            let base = format!("{}.h5", stem);
            if !Path::new(&base).exists() {
                base
            } else {
                (1..=1000)
                    .map(|i| format!("{}_{}.h5", stem, i))
                    .find(|name| !Path::new(name).exists())
                    .ok_or_else(|| eyre::eyre!("could not find available output filename"))?
            }
        }
    };

    let filter_pattern = filter.clone();
    let col_filter = if let Some(p) = filter {
        Some(ColumnFilter::new(&p).map_err(|e| eyre::eyre!("invalid column filter: {}", e))?)
    } else {
        None
    };

    // Create writer with filter baked in
    let mut writer = export::Hdf5Appender::with_options(
        Path::new(&output),
        compress,
        debug,
        col_filter,
        split_policy.clone().into(),
        split_level.clone().into(),
    )
    .wrap_err_with(|| format!("could not create HDF5 file {}", output))?;

    let ignore_session = files.len() > 1;
    let mut parser =
        PacketParser::new(DeviceRoute::root(), ignore_session).with_batch_rows(LOG_BATCH_ROWS);
    let mut parsed_routes: HashSet<DeviceRoute> = HashSet::new();
    let mut unparsed_routes: HashSet<DeviceRoute> = HashSet::new();
    let mut total_input_bytes: u64 = 0;
    let mut truncated = false;

    println!("Processing {} files...", files.len());

    for path in &files {
        let input =
            LogFile::open(Path::new(path)).wrap_err_with(|| format!("could not mmap {}", path))?;
        let total_bytes = input.len() as u64;
        let mut packets = input.packets();
        total_input_bytes += total_bytes;
        let mut progress = ByteProgress::new(total_bytes);
        progress.set_message(path.clone());

        let mut stopped_early = false;
        loop {
            let packet_offset = packets.position();
            let pkt = match packets.next() {
                Some(Ok(packet)) => packet,
                Some(Err(error)) => {
                    log::warn!("{}: {}; stopping", path, error);
                    stopped_early = true;
                    break;
                }
                None => break,
            };
            progress.update(packets.position() as u64);

            let has_stream_data = matches!(
                &pkt.payload,
                tio::proto::Payload::StreamData(data) if !data.data.is_empty()
            );
            let samples_len = match parser.push_packet(&pkt) {
                Ok(outcome) => outcome.row_count(),
                Err(error) => {
                    log::warn!(
                        "{}: invalid data at byte offset {}: {}; stopping",
                        path,
                        packet_offset,
                        error
                    );
                    stopped_early = true;
                    break;
                }
            };
            while let Some(batch) = parser.pop_batch() {
                let key = twinleaf::data::StreamKey::new(batch.route(), batch.stream().stream_id);
                writer
                    .write_batch(batch, key)
                    .wrap_err("failed to append HDF5 batch")?;
            }
            record_parse_result_parts(
                &mut parsed_routes,
                &mut unparsed_routes,
                &pkt.routing,
                has_stream_data,
                samples_len,
            );
        }

        progress.finish_with_message(
            packets.position() as u64,
            if stopped_early {
                "Stopped at parse error"
            } else {
                "Completed"
            },
        );
        truncated |= stopped_early;
    }

    for batch in parser.finish() {
        let key = twinleaf::data::StreamKey::new(batch.route(), batch.stream().stream_id);
        writer
            .write_batch(batch, key)
            .wrap_err("failed to append final HDF5 batch")?;
    }

    let stats = writer.finish().wrap_err("failed to finalize HDF5")?;

    report_missing_metadata(unparseable_routes(&parsed_routes, &unparsed_routes));

    use console::style;

    let file_size = std::fs::metadata(&output).map(|m| m.len()).unwrap_or(0);
    let size_mib = file_size as f64 / 1_048_576.0;
    let input_mib = total_input_bytes as f64 / 1_048_576.0;
    let written = stats.streams_written.len();
    let seen = stats.streams_seen.len();
    let rule = style("─".repeat(50)).dim();
    let label = |s: &str| style(format!("{:10}", s)).bold().cyan();
    let unit = |s: &str| style(s.to_string()).dim();

    println!();
    println!("{rule}");
    println!(" {}", style("HDF5 Output Summary").bold());
    println!("{rule}");
    println!(" {} {}", label("Output:"), output);

    if compress && total_input_bytes > 0 {
        let ratio = total_input_bytes as f64 / file_size.max(1) as f64;
        println!(
            " {} {:.2} {}  ({:.1}× smaller, from {:.2} {})",
            label("Size:"),
            size_mib,
            unit("MiB"),
            ratio,
            input_mib,
            unit("MiB"),
        );
    } else {
        println!(" {} {:.2} {}", label("Size:"), size_mib, unit("MiB"));
    }
    println!(" {} {}", label("Samples:"), stats.total_samples);

    if truncated {
        println!(
            " {} {}",
            label("Input:"),
            style("⚠ stopped at a parse error; output is incomplete").yellow(),
        );
    }

    match filter_pattern.as_deref() {
        Some(pat) if written == 0 && seen > 0 => {
            println!(
                " {} {}",
                label("Streams:"),
                style(format!(
                    "⚠ 0 of {} matching \"{}\" — none matched; check pattern",
                    seen, pat
                ))
                .yellow(),
            );
        }
        Some(pat) => {
            println!(
                " {} {} of {} matching \"{}\"",
                label("Streams:"),
                written,
                seen,
                pat
            );
        }
        None => {
            println!(" {} {}", label("Streams:"), written);
        }
    }

    if !matches!(split_level, SplitLevel::None) {
        let mode = match split_level {
            SplitLevel::Stream => "per-stream",
            SplitLevel::Device => "per-device",
            SplitLevel::Global => "global",
            SplitLevel::None => unreachable!(),
        };
        println!(
            " {} {}  ({} discontinuities detected)",
            label("Split:"),
            mode,
            stats.discontinuities_detected
        );
    }

    if matches!(split_policy, SplitPolicy::Monotonic) {
        println!(" {} monotonic", label("Policy:"));
    }

    println!("{rule}");

    Ok(())
}
