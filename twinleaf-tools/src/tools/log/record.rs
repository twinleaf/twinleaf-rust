use crate::tools::recv_before;
use crate::{ProxyHelp, TioOpts};
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::time::{Duration, Instant};
use twinleaf::data::{MetadataQuery, PacketParser, SampleBatch};
use twinleaf::device::{DeviceRoute, PendingReply};
use twinleaf::tio::{self, proxy};
use twinleaf_proto::data as wire;

fn ensure_open<'a>(fo: &'a mut Option<File>, path: &str) -> eyre::Result<&'a mut File> {
    use eyre::WrapErr;
    if fo.is_none() {
        *fo = Some(
            File::create(path).wrap_err_with(|| format!("could not create log file {}", path))?,
        );
    }
    Ok(fo.as_mut().unwrap())
}

/// Owns the output file and progress bar for a `tio log` run, counting bytes
/// and dropped samples as packets are written. Clears the progress bar on drop
/// so early returns don't need to do it explicitly.
struct Recorder {
    file_out: Option<File>,
    path: String,
    bytes_written: u64,
    samples_dropped: u64,
    pb: indicatif::ProgressBar,
    static_msg: String,
    unbuffered: bool,
}

impl Recorder {
    fn new(
        pb: indicatif::ProgressBar,
        path: String,
        static_msg: String,
        unbuffered: bool,
    ) -> Recorder {
        Recorder {
            file_out: None,
            path,
            bytes_written: 0,
            samples_dropped: 0,
            pb,
            static_msg,
            unbuffered,
        }
    }

    fn render_msg(&self) -> String {
        if self.samples_dropped > 0 {
            format!("{} · ({} dropped)", self.static_msg, self.samples_dropped)
        } else {
            self.static_msg.clone()
        }
    }

    fn write(&mut self, pkt: tio::Packet) -> eyre::Result<()> {
        use eyre::WrapErr;
        let raw = pkt.as_bytes();
        let f = ensure_open(&mut self.file_out, &self.path)?;
        f.write_all(raw)
            .wrap_err_with(|| format!("failed to write {}", self.path))?;
        self.bytes_written += raw.len() as u64;
        Ok(())
    }

    fn tick(&mut self) {
        self.pb.set_position(self.bytes_written);
        self.pb.set_message(self.render_msg());
    }

    fn flush_if_needed(&mut self) -> eyre::Result<()> {
        use eyre::WrapErr;
        if self.unbuffered {
            if let Some(f) = self.file_out.as_mut() {
                f.flush()
                    .wrap_err_with(|| format!("failed to flush {}", self.path))?;
            }
        }
        Ok(())
    }
}

impl Drop for Recorder {
    fn drop(&mut self) {
        self.pb.finish_and_clear();
    }
}

/// Write a full metadata snapshot for `batch`: the device, stream, and segment
/// updates followed by one column update per series, all stamped with the
/// batch's absolute route.
fn write_metadata_snapshot(rec: &mut Recorder, batch: &SampleBatch) -> eyre::Result<()> {
    let abs_route = batch.route();
    let (device, stream, segment) = batch.records();
    rec.write(device.update(abs_route)?)?;
    rec.write(stream.update(abs_route)?)?;
    rec.write(segment.update(abs_route)?)?;
    for series in batch.schema() {
        rec.write(series.record().update(abs_route)?)?;
    }
    Ok(())
}

pub fn log(
    tio: &TioOpts,
    file: String,
    unbuffered: bool,
    raw: bool,
    depth: Option<usize>,
    duration: Option<Duration>,
) -> eyre::Result<()> {
    use indicatif::{ProgressBar, ProgressStyle};
    use std::path::Path;

    let proxy = proxy::Connection::open(&tio.root);
    let route = tio.route;

    let file_name = Path::new(&file)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(&file)
        .to_string();

    let template = match duration {
        Some(d) => format!(
            "{{spinner}} [{{elapsed_precise}}/{}] {{decimal_bytes}} → {{msg}}",
            indicatif::FormattedDuration(d),
        ),
        None => "{spinner} [{elapsed_precise}] {decimal_bytes} → {msg}".to_string(),
    };
    let pb = crate::multi_progress().add(ProgressBar::new_spinner());
    pb.set_style(ProgressStyle::with_template(&template).unwrap());
    pb.enable_steady_tick(Duration::from_millis(100));

    let static_msg = {
        let mut parts: Vec<String> = vec![file_name.clone()];
        if raw {
            parts.push("raw".into());
        }
        if unbuffered {
            parts.push("unbuf".into());
        }
        parts.join(" · ")
    };

    let deadline = duration.map(|duration| Instant::now() + duration);
    let rec = Recorder::new(pb, file, static_msg, unbuffered);
    let initial_msg = rec.render_msg();
    rec.pb.set_message(initial_msg);

    if raw {
        log_raw(&proxy, &tio.root, route, depth, rec, deadline)
    } else {
        log_parsed(&proxy, &tio.root, route, rec, deadline)
    }
}

fn log_raw(
    proxy: &proxy::Connection,
    root: &str,
    route: DeviceRoute,
    depth: Option<usize>,
    mut rec: Recorder,
    deadline: Option<Instant>,
) -> eyre::Result<()> {
    use eyre::WrapErr;

    let port_depth = depth.unwrap_or(twinleaf_proto::MAX_ROUTING_SIZE);
    let port = proxy::open_port(proxy, None, route, port_depth, true, true)
        .wrap_err_with(|| format!("could not open port on {}", root))
        .with_proxy_help()?;

    loop {
        let pkt = match recv_before(&port, deadline) {
            Ok(Some(pkt)) => pkt,
            Ok(None) => break,
            Err(error) => {
                let context = if rec.bytes_written == 0 {
                    "stream ended; no data received".to_string()
                } else {
                    format!(
                        "stream ended after writing {} bytes to {}",
                        rec.bytes_written, rec.path
                    )
                };
                return Err(eyre::Report::new(error).wrap_err(context));
            }
        };
        rec.write(pkt.with_route(route.absolute_route(&pkt.route())?))?;
        rec.tick();
        rec.flush_if_needed()?;
    }

    let bytes = rec.bytes_written;
    let path = rec.path.clone();
    drop(rec);
    if bytes == 0 {
        log::info!("no data received");
    } else {
        log::info!("wrote {} bytes to {}", bytes, path);
    }
    Ok(())
}

fn log_parsed(
    proxy: &proxy::Connection,
    root: &str,
    route: DeviceRoute,
    mut rec: Recorder,
    deadline: Option<Instant>,
) -> eyre::Result<()> {
    use eyre::WrapErr;
    use twinleaf::data::BoundaryReason;

    // Byte-faithful recorder: receive packets directly from a subtree_full port
    // and maintain per-route parsers ourselves (what DeviceTree used to do), so
    // we can hold the raw stream-data packet instead of reconstructing it from
    // a parsed sample.
    let port = proxy::open_port(proxy, None, route, usize::MAX, true, true)
        .wrap_err_with(|| format!("could not open device tree on {}", root))
        .with_proxy_help()?;
    let tree = proxy
        .tree_with(route, twinleaf_proto::MAX_ROUTING_SIZE, None)
        .wrap_err_with(|| format!("could not open device tree on {}", root))
        .with_proxy_help()?;

    let mut parser = PacketParser::new(route, false);
    let mut metadata_calls: Vec<(MetadataQuery, PendingReply)> = Vec::new();
    loop {
        for query in parser.take_metadata_queries() {
            match tree.submit(query.route, wire::METADATA_RPC_METHOD, &query.args()) {
                Ok(pending) => metadata_calls.push((query, pending)),
                Err(_) => parser.fail_metadata_query(query),
            }
        }
        metadata_calls = metadata_calls
            .into_iter()
            .filter_map(|(query, pending)| match pending.try_get() {
                Some(Ok(reply)) => {
                    parser.apply_metadata_reply(query, &reply);
                    None
                }
                Some(Err(_)) => {
                    parser.fail_metadata_query(query);
                    None
                }
                None => Some((query, pending)),
            })
            .collect();

        let pkt = match recv_before(&port, deadline) {
            Ok(Some(pkt)) => pkt,
            Ok(None) => break,
            Err(e) => {
                return Err(eyre::Report::new(e).wrap_err("stream ended"));
            }
        };

        // The parser intercepts ProxyStatus (resetting on disconnect); RpcUpdate
        // is a parser no-op. The batch carries the absolute route.
        if let Err(error) = parser.push_packet(&pkt) {
            log::warn!("dropping invalid stream packet: {error}");
            rec.tick();
            let _ = rec.flush_if_needed();
            continue;
        }

        let mut parsed_route = None;
        while let Some(batch) = parser.pop_batch() {
            let abs_route = batch.route();
            if let Some(b) = batch.boundary() {
                if let BoundaryReason::SamplesLost { expected, received } = b.reason {
                    let count = received.wrapping_sub(expected);
                    rec.samples_dropped += count as u64;
                    log::warn!(
                        "{}/{} dropped {} samples",
                        abs_route,
                        batch.stream().name,
                        count
                    );
                }
                write_metadata_snapshot(&mut rec, &batch)?;
            }
            parsed_route = Some(abs_route);
        }

        // Only record the raw stream-data packet when it actually parsed into
        // a batch, matching the old reconstruction which wrote once per
        // parseable packet.
        if let (Some(abs_route), tio::proto::Payload::Samples(_)) = (parsed_route, pkt.payload()) {
            rec.write(pkt.with_route(abs_route).with_ttl(0)?)?;
        }

        rec.tick();
        let _ = rec.flush_if_needed();
    }

    let bytes = rec.bytes_written;
    let dropped = rec.samples_dropped;
    let path = rec.path.clone();
    drop(rec);
    if bytes == 0 {
        log::info!("no data received");
    } else {
        log::info!(
            "wrote {} bytes to {} ({} samples dropped)",
            bytes,
            path,
            dropped
        );
    }
    Ok(())
}

pub fn log_metadata(tio: &TioOpts, file: String) -> eyre::Result<()> {
    use eyre::WrapErr;

    let proxy = proxy::Connection::open(&tio.root);
    let route = tio.route;

    let device = proxy.device(route);

    let meta = device
        .metadata()
        .wrap_err("failed to fetch device metadata")
        .with_proxy_help()?;

    let mut file_out: Option<File> = None;

    let write_packet = |fo: &mut Option<File>, pkt: tio::Packet| -> eyre::Result<()> {
        let f = ensure_open(fo, &file)?;
        f.write_all(pkt.as_bytes())
            .wrap_err_with(|| format!("failed to write {}", file))
    };

    write_packet(&mut file_out, meta.device.update(route)?)?;
    for (_id, stream) in meta.streams {
        write_packet(&mut file_out, stream.stream.update(route)?)?;
        write_packet(&mut file_out, stream.segment.update(route)?)?;
        for col in stream.columns {
            write_packet(&mut file_out, col.update(route)?)?;
        }
    }
    Ok(())
}

pub fn meta_reroute(input: String, route: DeviceRoute, output: Option<String>) -> eyre::Result<()> {
    use eyre::{bail, WrapErr};

    let data = std::fs::read(&input).wrap_err_with(|| format!("could not read {}", input))?;

    let mut rest: &[u8] = &data;
    let mut routes: HashSet<DeviceRoute> = HashSet::new();
    let mut packet_count = 0usize;

    while !rest.is_empty() {
        let (pkt, len) = tio::Packet::from_slice_prefix(rest)
            .wrap_err_with(|| format!("could not parse packet in {}", input))?;
        rest = &rest[len..];
        packet_count += 1;

        if pkt.ptype() != tio::proto::PacketType::METADATA {
            bail!(
                "{} does not look like a metadata file (found non-metadata packet)",
                input
            );
        }
        routes.insert(pkt.route());
    }

    if packet_count == 0 {
        bail!("{} contains no packets", input);
    }

    if routes.len() > 1 {
        let mut routes: Vec<_> = routes.into_iter().collect();
        routes.sort();
        eprintln!("{} contains multiple routes:", input);
        for route in routes.iter().take(5) {
            eprintln!("  {}", route);
        }
        if routes.len() > 5 {
            eprintln!("  ... and {} more", routes.len() - 5);
        }
        bail!("cannot reroute a file with multiple routes");
    }

    let new_route = route;
    let output_path = output.unwrap_or_else(|| {
        let base = input.strip_suffix(".tio").unwrap_or(&input);
        format!("{}_rerouted.tio", base)
    });
    if output_path == input {
        bail!("output path must be different from input");
    }

    let mut file =
        File::create(&output_path).wrap_err_with(|| format!("could not create {}", output_path))?;

    rest = &data;
    while !rest.is_empty() {
        let (pkt, len) = tio::Packet::from_slice_prefix(rest)
            .wrap_err_with(|| format!("could not parse packet in {}", input))?;
        rest = &rest[len..];
        file.write_all(pkt.with_route(new_route).as_bytes())
            .wrap_err_with(|| format!("failed to write {}", output_path))?;
    }

    Ok(())
}
