use super::{progress::ByteProgress, record_parse_result, LOG_BATCH_ROWS};
use crate::{parse_csv_target, StreamSel};
use std::collections::HashSet;
use std::fmt::Write as FmtWrite;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::sync::Arc;
use twinleaf::data::{LogFile, PacketParser, SampleBatch, StreamKey};
use twinleaf::device::DeviceRoute;
use twinleaf::tio;
use twinleaf::tio::proto::meta::ColumnMetadata;

#[derive(Clone, PartialEq)]
struct CsvSchema {
    key: StreamKey,
    columns: Vec<Arc<ColumnMetadata>>,
}

impl CsvSchema {
    fn from_batch(batch: &SampleBatch) -> Self {
        Self {
            key: batch.stream_key(),
            columns: batch
                .schema()
                .iter()
                .map(|series| series.metadata().clone())
                .collect(),
        }
    }

    fn matches(&self, batch: &SampleBatch) -> bool {
        self.key == batch.stream_key()
            && self.columns.len() == batch.schema().len()
            && self
                .columns
                .iter()
                .zip(batch.schema())
                .all(|(expected, actual)| expected.as_ref() == actual.metadata().as_ref())
    }
}

struct CsvOutput {
    stream: StreamSel,
    route: DeviceRoute,
    prefix: String,
    force: bool,
    path: Option<String>,
    stream_name: Option<String>,
    writer: Option<BufWriter<File>>,
    schema: Option<CsvSchema>,
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
            StreamSel::Id(id) => batch.stream().stream_id == *id,
            StreamSel::Name(name) => &batch.stream().name == name,
        };
        if !is_match {
            return Ok(());
        }

        if let Some(existing) = &self.schema {
            if !existing.matches(&batch) {
                return Err(eyre::eyre!(
                    "stream identity or schema changed while writing {}; refusing to emit rows under a stale CSV header",
                    self.path.as_deref().unwrap_or("CSV output")
                ));
            }
        } else {
            self.schema = Some(CsvSchema::from_batch(&batch));
        }

        if self.writer.is_none() {
            self.header.push("time".to_string());
            self.header.extend(
                batch
                    .schema()
                    .iter()
                    .map(|series| series.metadata().name.clone()),
            );

            let route_label = route_filename_label(&self.route);
            let path = format!(
                "{}.{}.{}.csv",
                self.prefix,
                route_label,
                filename_component(&batch.stream().name)
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
            self.stream_name = Some(batch.stream().name.clone());
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

pub fn log_csv(
    args: Vec<String>,
    sensor: Option<DeviceRoute>,
    output: Option<String>,
    force: bool,
) -> eyre::Result<()> {
    use color_eyre::Help;
    use eyre::WrapErr;

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

    let ignore_session = files.len() > 1;
    let mut parser =
        PacketParser::new(DeviceRoute::root(), ignore_session).with_batch_rows(LOG_BATCH_ROWS);
    let mut parsed_routes: HashSet<DeviceRoute> = HashSet::new();
    let mut unparsed_routes: HashSet<DeviceRoute> = HashSet::new();
    let output_prefix = output.unwrap_or_else(|| files.last().cloned().unwrap_or_default());
    let mut csv = CsvOutput::new(target.stream, target_route, output_prefix, force);

    for path in &files {
        let input = LogFile::open(std::path::Path::new(path))
            .wrap_err_with(|| format!("could not mmap {}", path))
            .suggestion(usage_hint)?;
        let total_bytes = input.len() as u64;
        let mut packets = input.packets();

        let mut progress = ByteProgress::new(total_bytes);
        progress.set_message(path.clone());

        loop {
            let packet_offset = packets.position();
            let pkt = match packets.next() {
                Some(Ok(packet)) => packet,
                Some(Err(error)) => {
                    log::warn!("{}: {}; stopping", path, error);
                    break;
                }
                None => break,
            };
            progress.update(packets.position() as u64);

            let samples_len = match &pkt.payload {
                tio::proto::Payload::StreamData(_) if pkt.routing != target_route => 0,
                _ => match parser.push_packet(&pkt) {
                    Ok(outcome) => outcome.row_count(),
                    Err(error) => {
                        log::warn!(
                            "{}: invalid data at byte offset {}: {}; stopping",
                            path,
                            packet_offset,
                            error
                        );
                        break;
                    }
                },
            };

            if pkt.routing == target_route {
                record_parse_result(&mut parsed_routes, &mut unparsed_routes, &pkt, samples_len);
            }

            while let Some(batch) = parser.pop_batch() {
                csv.write_batch(batch)?;
            }
        }

        progress.finish_and_clear(packets.position() as u64);
    }

    for batch in parser.finish() {
        csv.write_batch(batch)?;
    }
    csv.flush()?;

    if csv.writer.is_none() {
        if unparsed_routes.contains(&target_route) && !parsed_routes.contains(&target_route) {
            return Err(eyre::eyre!(
                "stream data at route {} could not be parsed because metadata is missing or incompatible",
                target_route
            )
            .suggestion(
                "ensure the log includes metadata or capture it with `tio log metadata`, \
                 including it as an argument before the log",
            ));
        }
        return Err(eyre::eyre!(
            "no data found for stream '{}' at route {}",
            target_desc,
            target_route
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

/// Render a route as a filename-safe label: `root` for the device root, or the
/// hop indices joined by `.` (e.g. `/0/1` -> `0.1`).
fn route_filename_label(route: &DeviceRoute) -> String {
    if route.is_empty() {
        "root".to_string()
    } else {
        route
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
    use super::*;

    #[test]
    fn csv_header_quotes_special_column_names() {
        assert_eq!(
            csv_header(&["time".to_string(), "x,\"quoted\"".to_string()]),
            "time,\"x,\"\"quoted\"\"\""
        );
    }

    #[test]
    fn csv_schema_distinguishes_duplicate_names_and_changed_columns() {
        use twinleaf::tio::proto::DataType;

        let column = |stream_id, name: &str| ColumnMetadata {
            stream_id,
            index: 0,
            data_type: DataType::Float32,
            name: name.to_string(),
            units: "V".to_string(),
            description: String::new(),
        };
        let original = CsvSchema {
            key: StreamKey::new(DeviceRoute::root(), 1),
            columns: vec![Arc::new(column(1, "value"))],
        };
        let duplicate_name = CsvSchema {
            key: StreamKey::new(DeviceRoute::root(), 2),
            columns: vec![Arc::new(column(2, "value"))],
        };
        let changed_column = CsvSchema {
            key: StreamKey::new(DeviceRoute::root(), 1),
            columns: vec![Arc::new(column(1, "renamed"))],
        };

        assert!(original != duplicate_name);
        assert!(original != changed_column);
    }
}
