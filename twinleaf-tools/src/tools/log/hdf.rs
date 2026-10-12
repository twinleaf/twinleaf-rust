//! The `tio log hdf` subcommand, which converts logs to HDF5 with
//! [`log_hdf`].

use super::{missing_metadata_error, read::LogReader, report_missing_metadata, LOG_BATCH_ROWS};
use crate::{LogHdfCli, SplitLevel, SplitPolicy};
use std::collections::HashSet;
use twinleaf::data::{ColumnFilter, PacketParser, SampleBatch};

/// The `--debug` report, which says once per column path whether the glob
/// keeps it.
struct FilterDebug {
    filter: ColumnFilter,
    seen: HashSet<String>,
}

impl FilterDebug {
    fn show(&mut self, batch: &SampleBatch) {
        if batch.is_empty() {
            return;
        }
        let route = batch.stream_key().route;
        let stream = batch.stream().name.to_string();
        for column in batch.schema() {
            let name = column.metadata().name;
            let path = self.filter.path_string(&route, &stream, name);
            if self.seen.insert(path.clone()) {
                let kept = self.filter.matches(&route, &stream, name);
                println!("[DEBUG] Filter: '{}' -> {}", path, kept);
            }
        }
    }
}

pub fn log_hdf(cli: LogHdfCli) -> eyre::Result<()> {
    use eyre::WrapErr;
    use std::path::Path;
    use twinleaf::data::export;

    let LogHdfCli {
        files,
        output,
        filter,
        compress,
        debug,
        split_level,
        split_policy,
    } = cli;

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
    let mut filter_debug = match (&filter_pattern, debug) {
        (Some(pattern), true) => Some(FilterDebug {
            filter: ColumnFilter::new(pattern).map_err(|e| eyre::eyre!(e))?,
            seen: HashSet::new(),
        }),
        _ => None,
    };

    // Open every input before creating the output. Besides validating all paths,
    // retaining the mappings ensures an output alias can never replace an input
    // between validation and writer creation.
    let reader = LogReader::open(&files)?.with_progress();

    let output_path = Path::new(&output);
    if output_path.exists() {
        let canonical_output = std::fs::canonicalize(output_path)
            .wrap_err_with(|| format!("could not inspect output path {output}"))?;
        if files.iter().any(|path| {
            std::fs::canonicalize(Path::new(path)).is_ok_and(|input| input == canonical_output)
        }) {
            return Err(eyre::eyre!(
                "output {output} refers to an input log; refusing to overwrite it"
            ));
        }
        return Err(eyre::eyre!(
            "output {output} already exists; choose a new output path"
        ));
    }

    // The appender also uses exclusive creation, closing the race between the
    // existence check and creation.
    let mut writer = export::Hdf5Appender::with_options(
        output_path,
        compress,
        col_filter,
        split_policy.clone().into(),
        split_level.clone().into(),
    )
    .wrap_err_with(|| format!("could not create HDF5 file {}", output))?;

    let parser = PacketParser::new(files.len() > 1).with_batch_rows(LOG_BATCH_ROWS);
    let total_input_bytes = reader.total_bytes();

    println!("Processing {} files...", files.len());

    let read = reader.batches(
        parser,
        |_| true,
        |batch| {
            if let Some(debug) = &mut filter_debug {
                debug.show(&batch);
            }
            writer
                .write_batch(batch)
                .wrap_err("failed to append HDF5 batch")
        },
    )?;

    let stats = writer.finish().wrap_err("failed to finalize HDF5")?;

    // An output with no samples is removed, which is safe because the
    // appender created it, and the error says why it stayed empty.
    if stats.total_samples == 0 {
        use color_eyre::Help;

        std::fs::remove_file(output_path)
            .wrap_err_with(|| format!("could not remove the empty output {output}"))?;
        return Err(match filter_pattern.as_deref() {
            Some(pattern) if !stats.streams_seen.is_empty() => eyre::eyre!(
                "0 of {} streams matching \"{pattern}\"",
                stats.streams_seen.len()
            )
            .suggestion("none matched; check pattern"),
            _ if !read.missing_metadata.is_empty() => {
                missing_metadata_error(&read.missing_metadata)
            }
            _ if read.truncated => {
                eyre::eyre!("the input stopped at a parse error before any samples")
            }
            _ => eyre::eyre!("the input holds no samples"),
        });
    }

    report_missing_metadata(&read.missing_metadata);

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

    if read.truncated {
        println!(
            " {} {}",
            label("Input:"),
            style("⚠ stopped at a parse error; output is incomplete").yellow(),
        );
    }

    match filter_pattern.as_deref() {
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

#[cfg(test)]
mod tests {
    use super::super::fixtures::{metadata, samples, temp_path, write_log, DAMAGE};
    use super::*;
    use std::path::Path;

    /// Returns the arguments converting `input` to `output`, with every
    /// option at its default.
    fn cli(input: &Path, output: &Path) -> LogHdfCli {
        LogHdfCli {
            files: vec![input.to_string_lossy().into_owned()],
            output: Some(output.to_string_lossy().into_owned()),
            filter: None,
            compress: false,
            debug: false,
            split_level: SplitLevel::None,
            split_policy: SplitPolicy::Continuous,
        }
    }

    #[test]
    fn invalid_input_does_not_create_output() {
        let input = temp_path("missing", "tio");
        let output = temp_path("missing", "h5");
        let result = log_hdf(cli(&input, &output));

        assert!(result.is_err());
        assert!(!output.exists());
    }

    #[test]
    fn input_output_alias_is_rejected_without_modifying_input() {
        let input = temp_path("alias", "tio");
        let sentinel = b"not a packet, but valid mmap input";
        std::fs::write(&input, sentinel).expect("create input");
        let result = log_hdf(cli(&input, &input));

        let error = result.expect_err("input/output alias must fail");
        assert!(error.to_string().contains("refers to an input log"));
        assert_eq!(std::fs::read(&input).expect("read input"), sentinel);
        std::fs::remove_file(input).expect("remove input");
    }

    #[test]
    fn no_samples_is_an_error_that_leaves_no_file() {
        let cases = [
            (
                "no_metadata",
                write_log("no_metadata", &samples(3), &[]),
                None,
                "metadata is missing",
            ),
            (
                "damaged",
                write_log("damaged", &[], DAMAGE),
                None,
                "parse error",
            ),
            (
                "filtered",
                write_log("filtered", &[metadata(), samples(3)].concat(), &[]),
                Some("/9"),
                "0 of 1 streams matching",
            ),
        ];
        for (label, log, filter, reason) in cases {
            let output = temp_path(label, "h5");
            let cli = LogHdfCli {
                filter: filter.map(str::to_string),
                ..cli(&log, &output)
            };
            let error = log_hdf(cli).expect_err(label);
            assert!(error.to_string().contains(reason), "{label}: {error}");
            assert!(!output.exists(), "{label}");
            std::fs::remove_file(log).expect("remove the log");
        }
    }

    #[test]
    fn partial_output_is_kept() {
        let log = write_log("partial", &[metadata(), samples(3)].concat(), DAMAGE);
        let output = temp_path("partial", "h5");
        log_hdf(cli(&log, &output)).expect("a partial conversion succeeds");
        assert!(output.exists());
        std::fs::remove_file(output).expect("remove the output");
        std::fs::remove_file(log).expect("remove the log");
    }
}
