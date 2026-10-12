//! The `tio log csv` subcommand, which writes one stream of a log to CSV
//! with [`log_csv`].

use super::{missing_metadata_error, read::LogReader, LOG_BATCH_ROWS};
use crate::{parse_csv_target, LogCsvCli, StreamSel};
use std::fmt::Write as FmtWrite;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use twinleaf::data::{PacketParser, SampleBatch, StreamMetadataSnapshot};
use twinleaf::DeviceRoute;

struct CsvOutput {
    stream: StreamSel,
    route: DeviceRoute,
    prefix: String,
    force: bool,
    path: Option<String>,
    stream_name: Option<String>,
    writer: Option<BufWriter<File>>,
    schema: Option<StreamMetadataSnapshot>,
    header: Vec<String>,
    row: String,
    rows_written: u64,
}

impl CsvOutput {
    fn new(stream: StreamSel, route: DeviceRoute, prefix: String, force: bool) -> Self {
        Self {
            stream,
            route,
            prefix,
            force,
            path: None,
            stream_name: None,
            writer: None,
            schema: None,
            header: Vec::new(),
            row: String::new(),
            rows_written: 0,
        }
    }

    fn write_batch(&mut self, batch: SampleBatch) -> eyre::Result<()> {
        use color_eyre::Help;
        use eyre::WrapErr;

        if batch.route() != self.route {
            return Ok(());
        }
        let is_match = match &self.stream {
            StreamSel::Id(id) => batch.stream().stream_id.value() == *id,
            StreamSel::Name(name) => batch.stream().name == name,
        };
        if !is_match {
            return Ok(());
        }

        if let Some(existing) = &self.schema {
            if !existing.matches_schema(&batch) {
                return Err(eyre::eyre!(
                    "stream identity or schema changed while writing {}; refusing to emit rows under a stale CSV header",
                    self.path.as_deref().unwrap_or("CSV output")
                ));
            }
        } else {
            self.schema = Some(batch.metadata());
        }

        if self.writer.is_none() {
            self.header.push("time".to_string());
            self.header.extend(
                batch
                    .schema()
                    .iter()
                    .map(|series| series.metadata().name.to_string()),
            );

            let route_label = route_filename_label(&self.route);
            let path = format!(
                "{}.{}.{}.csv",
                self.prefix,
                route_label,
                filename_component(batch.stream().name)
            );
            if !self.force && std::path::Path::new(&path).exists() {
                return Err(eyre::eyre!("output {} already exists", path)
                    .suggestion("pass --force to overwrite, or use -o for a different name"));
            }
            let file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .wrap_err_with(|| format!("could not open {}", path))?;
            self.writer = Some(BufWriter::new(file));
            self.stream_name = Some(batch.stream().name.to_string());
            self.path = Some(path);

            let output_path = self.path.as_deref().unwrap_or_default();
            writeln!(
                self.writer.as_mut().unwrap(),
                "{}",
                csv_header(&self.header)
            )
            .wrap_err_with(|| format!("failed to write {}", output_path))?;
        }

        let output_path = self.path.as_deref().unwrap_or_default();
        for sample in batch.iter() {
            self.row.clear();
            write!(&mut self.row, "{:.6}", sample.timestamp_end())
                .expect("writing to a string cannot fail");
            for value in sample.values() {
                write!(&mut self.row, ",{}", value).expect("writing to a string cannot fail");
            }
            self.row.push('\n');
            self.writer
                .as_mut()
                .unwrap()
                .write_all(self.row.as_bytes())
                .wrap_err_with(|| format!("failed to write {}", output_path))?;
            self.rows_written += 1;
        }
        Ok(())
    }

    fn flush(&mut self) -> eyre::Result<()> {
        use eyre::WrapErr;

        if let Some(writer) = &mut self.writer {
            let output_path = self.path.as_deref().unwrap_or_default();
            writer
                .flush()
                .wrap_err_with(|| format!("failed to write {}", output_path))?;
        }
        Ok(())
    }
}

pub fn log_csv(cli: LogCsvCli) -> eyre::Result<()> {
    use color_eyre::Help;

    let LogCsvCli {
        args,
        sensor,
        output,
        force,
    } = cli;

    let usage_hint = "tio log csv <stream> <log.tio>... [-s <route>]";

    if args.is_empty() {
        return Err(eyre::eyre!("missing stream name and log files").suggestion(usage_hint));
    }

    let mut stream_arg: Option<String> = None;
    let mut files: Vec<String> = Vec::new();
    for arg in args {
        if arg.ends_with(".tio") {
            files.push(arg);
        } else if stream_arg.is_none() {
            stream_arg = Some(arg);
        } else {
            return Err(eyre::eyre!("multiple stream arguments provided")
                .suggestion(usage_hint)
                .suggestion("log files should end with .tio"));
        }
    }

    let stream_arg = stream_arg.ok_or_else(|| {
        eyre::eyre!("missing stream name or id")
            .suggestion(usage_hint)
            .suggestion("log files should end with .tio")
    })?;

    if files.is_empty() {
        return Err(eyre::eyre!("missing log file").suggestion(usage_hint));
    }

    let target =
        parse_csv_target(&stream_arg).map_err(|e| eyre::eyre!(e).suggestion(usage_hint))?;

    // A route may come from the selector prefix (e.g. /0/field) or -s, but not both.
    let target_route = match (target.route, sensor) {
        (Some(_), Some(_)) => {
            return Err(eyre::eyre!(
                "route given both in the selector and with -s; specify it only once"
            )
            .suggestion(usage_hint));
        }
        (Some(r), None) | (None, Some(r)) => r,
        (None, None) => DeviceRoute::root(),
    };

    // How the user referred to the stream, for error/summary messages.
    let target_desc = match &target.stream {
        StreamSel::Id(id) => id.to_string(),
        StreamSel::Name(name) => name.clone(),
    };

    let reader = LogReader::open(&files)
        .suggestion(usage_hint)?
        .with_progress();
    let parser = PacketParser::new(files.len() > 1).with_batch_rows(LOG_BATCH_ROWS);
    let output_prefix = output.unwrap_or_else(|| files.last().cloned().unwrap_or_default());
    let mut csv = CsvOutput::new(target.stream, target_route, output_prefix, force);

    // Only the target route's stream data is decoded.
    let read = reader.batches(
        parser,
        |packet| packet.route() == target_route,
        |batch| csv.write_batch(batch),
    )?;
    csv.flush()?;

    if csv.writer.is_none() {
        if read.missing_metadata.contains(&target_route) {
            return Err(missing_metadata_error(&[target_route]));
        }
        let stopped = if read.truncated {
            " before the input stopped at a parse error"
        } else {
            ""
        };
        return Err(eyre::eyre!(
            "no data found for stream '{}' at route {}{}",
            target_desc,
            target_route,
            stopped
        )
        .suggestion(format!(
            "see available routes and streams with: tio log dump -m {}",
            files.first().unwrap_or(&"<file>".to_string())
        )));
    }

    use console::style;
    let rule = style("─".repeat(50)).dim();
    let label = |s: &str| style(format!("{:10}", s)).bold().cyan();
    let stream_label = csv.stream_name.as_deref().unwrap_or(&target_desc);

    println!();
    println!("{rule}");
    println!(" {}", style("CSV Output Summary").bold());
    println!("{rule}");
    println!(
        " {} {}",
        label("Output:"),
        csv.path.as_deref().unwrap_or_default()
    );
    println!(" {} {} @ {}", label("Stream:"), stream_label, target_route);
    println!(" {} {}", label("Rows:"), csv.rows_written);
    println!(" {} {}", label("Columns:"), csv.header.join(", "));
    println!("{rule}");

    Ok(())
}

/// Renders `route` as a label safe in a filename. The device root is `root`.
/// Other routes join their hop indices with `.`, as in `0.1` for `/0/1`.
fn route_filename_label(route: &DeviceRoute) -> String {
    if route.is_empty() {
        "root".to_string()
    } else {
        route
            .as_slice()
            .iter()
            .map(|hop| hop.to_string())
            .collect::<Vec<_>>()
            .join(".")
    }
}

fn csv_header(fields: &[String]) -> String {
    fields
        .iter()
        .map(|field| {
            if field.contains([',', '"', '\n', '\r']) {
                format!("\"{}\"", field.replace('"', "\"\""))
            } else {
                field.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn filename_component(name: &str) -> String {
    let escaped: String = name
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '_'
            }
        })
        .collect();
    if escaped.is_empty() {
        "unnamed".to_string()
    } else {
        escaped
    }
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::{metadata, samples, temp_path, write_log, DAMAGE};
    use super::*;
    use std::path::{Path, PathBuf};

    /// Returns the arguments converting stream `test` of `log`, with outputs
    /// named from `prefix`.
    fn cli(log: &Path, prefix: &Path) -> LogCsvCli {
        LogCsvCli {
            args: vec!["test".to_string(), log.to_string_lossy().into_owned()],
            sensor: None,
            output: Some(prefix.to_string_lossy().into_owned()),
            force: false,
        }
    }

    /// Returns the CSV file written for stream `test` with `prefix`.
    fn output(prefix: &Path) -> PathBuf {
        PathBuf::from(format!("{}.root.test.csv", prefix.display()))
    }

    #[test]
    fn no_samples_is_an_error_that_writes_nothing() {
        let cases = [
            (
                "no_metadata",
                write_log("no_metadata", &samples(3), &[]),
                "metadata is missing",
            ),
            ("damaged", write_log("damaged", &[], DAMAGE), "parse error"),
        ];
        for (label, log, reason) in cases {
            let prefix = temp_path(label, "out");
            let error = log_csv(cli(&log, &prefix)).expect_err(label);
            assert!(error.to_string().contains(reason), "{label}: {error}");
            assert!(!output(&prefix).exists(), "{label}");
            std::fs::remove_file(log).expect("remove the log");
        }
    }

    #[test]
    fn partial_output_is_kept() {
        let log = write_log("partial", &[metadata(), samples(3)].concat(), DAMAGE);
        let prefix = temp_path("partial", "out");
        log_csv(cli(&log, &prefix)).expect("a partial conversion succeeds");
        let csv = std::fs::read_to_string(output(&prefix)).expect("read the output");
        assert_eq!(csv.lines().count(), 4, "{csv}");
        std::fs::remove_file(output(&prefix)).expect("remove the output");
        std::fs::remove_file(log).expect("remove the log");
    }

    #[test]
    fn csv_header_quotes_special_column_names() {
        assert_eq!(
            csv_header(&["time".to_string(), "x,\"quoted\"".to_string()]),
            "time,\"x,\"\"quoted\"\"\""
        );
    }
}
