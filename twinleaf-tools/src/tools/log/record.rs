use crate::{ProxyHelp, TioOpts};
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::time::{Duration, Instant};
use twinleaf::data::{PacketParser, SampleBatch};
use twinleaf::device::{Device, DeviceRoute};
use twinleaf::tio::{self, proxy};

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
    duration: Option<Duration>,
    started: Instant,
}

impl Recorder {
    fn new(
        pb: indicatif::ProgressBar,
        path: String,
        static_msg: String,
        unbuffered: bool,
        duration: Option<Duration>,
    ) -> Recorder {
        Recorder {
            file_out: None,
            path,
            bytes_written: 0,
            samples_dropped: 0,
            pb,
            static_msg,
            unbuffered,
            duration,
            started: Instant::now(),
        }
    }

    fn duration_elapsed(&self) -> bool {
        self.duration.is_some_and(|d| self.started.elapsed() >= d)
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
        let serialized = pkt
            .serialize()
            .wrap_err("failed to serialize packet for log")?;
        let f = ensure_open(&mut self.file_out, &self.path)?;
        f.write_all(&serialized)
            .wrap_err_with(|| format!("failed to write {}", self.path))?;
        self.bytes_written += serialized.len() as u64;
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
    let abs_route = batch.route;
    rec.write(batch.device.make_update_with_route(abs_route))?;
    rec.write(batch.stream.make_update_with_route(abs_route))?;
    rec.write(batch.segment.make_update_with_route(abs_route))?;
    for series in batch.schema() {
        rec.write(series.metadata.make_update_with_route(abs_route))?;
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

    let proxy = proxy::Interface::new(&tio.root);
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

    let rec = Recorder::new(pb, file, static_msg, unbuffered, duration);
    let initial_msg = rec.render_msg();
    rec.pb.set_message(initial_msg);

    if raw {
        log_raw(&proxy, &tio.root, route, depth, rec)
    } else {
        log_parsed(&proxy, &tio.root, route, rec)
    }
}

fn log_raw(
    proxy: &proxy::Interface,
    root: &str,
    route: DeviceRoute,
    depth: Option<usize>,
    mut rec: Recorder,
) -> eyre::Result<()> {
    use eyre::WrapErr;

    let port_depth = depth.unwrap_or(tio::proto::TIO_PACKET_MAX_ROUTING_SIZE);
    let port = proxy
        .new_port(None, route, port_depth, true, true)
        .wrap_err_with(|| format!("could not open port on {}", root))
        .with_proxy_help()?;

    for pkt in port.iter() {
        if rec.duration_elapsed() {
            break;
        }
        let abs_pkt = tio::Packet {
            routing: route.absolute_route(&pkt.routing)?,
            ..pkt
        };
        rec.write(abs_pkt)?;
        rec.tick();
        rec.flush_if_needed()?;
    }

    let elapsed = rec.duration_elapsed();
    let bytes = rec.bytes_written;
    let path = rec.path.clone();
    drop(rec);
    if elapsed {
        if bytes == 0 {
            log::info!("no data received");
        } else {
            log::info!("wrote {} bytes to {}", bytes, path);
        }
        Ok(())
    } else if bytes == 0 {
        Err(eyre::eyre!("stream ended; no data received"))
    } else {
        Err(eyre::eyre!(
            "stream ended after writing {} bytes to {}",
            bytes,
            path
        ))
    }
}

fn log_parsed(
    proxy: &proxy::Interface,
    root: &str,
    route: DeviceRoute,
    mut rec: Recorder,
) -> eyre::Result<()> {
    use eyre::WrapErr;
    use twinleaf::data::BoundaryReason;

    // Byte-faithful recorder: receive packets directly from a subtree_full port
    // and maintain per-route parsers ourselves (what DeviceTree used to do), so
    // we can hold the raw stream-data packet instead of reconstructing it from
    // a parsed sample.
    let port = proxy
        .new_port(None, route, usize::MAX, true, true)
        .wrap_err_with(|| format!("could not open device tree on {}", root))
        .with_proxy_help()?;

    let mut parser = PacketParser::new(route, false);

    loop {
        if rec.duration_elapsed() {
            break;
        }

        // Drain all currently-available packets, mirroring DeviceTree::process_packet.
        loop {
            for req in parser.take_requests() {
                if let Err(e) = port.send(req) {
                    return Err(eyre::Report::new(e).wrap_err("stream ended"));
                }
            }

            let pkt = match port.try_recv() {
                Ok(pkt) => pkt,
                Err(tio::proxy::RecvError::WouldBlock) => break,
                Err(e) => {
                    return Err(eyre::Report::new(e).wrap_err("stream ended"));
                }
            };

            // The parser intercepts ProxyStatus (resetting on disconnect); RpcUpdate
            // is a parser no-op. The batch carries the absolute route.
            let parsed = parser.process_packet(&pkt);

            if let Some(batch) = &parsed {
                let abs_route = batch.route;
                if let Some(b) = &batch.boundary {
                    if let BoundaryReason::SamplesLost { expected, received } = b.reason {
                        let count = received.wrapping_sub(expected);
                        rec.samples_dropped += count as u64;
                        log::warn!(
                            "{}/{} dropped {} samples",
                            abs_route,
                            batch.stream.name,
                            count
                        );
                    }
                    write_metadata_snapshot(&mut rec, batch)?;
                }

                // Only record the raw stream-data packet when it actually parsed
                // into a batch (parser established), matching the old
                // reconstruction which wrote exactly once per parseable packet.
                if matches!(pkt.payload, tio::proto::Payload::StreamData(_)) {
                    let data_pkt = tio::Packet {
                        payload: pkt.payload,
                        routing: abs_route,
                        ttl: 0,
                    };
                    rec.write(data_pkt)?;
                }
            }
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

    let proxy = proxy::Interface::new(&tio.root);
    let route = tio.route;

    let mut device = Device::open(&proxy, route)
        .wrap_err_with(|| format!("could not open device at {}", tio.root))
        .with_proxy_help()?;

    let meta = device
        .get_metadata()
        .wrap_err("failed to fetch device metadata")?;

    let mut file_out: Option<File> = None;

    let write_packet = |fo: &mut Option<File>, pkt: tio::Packet| -> eyre::Result<()> {
        let raw = pkt
            .serialize()
            .wrap_err("failed to serialize metadata packet")?;
        let f = ensure_open(fo, &file)?;
        f.write_all(&raw)
            .wrap_err_with(|| format!("failed to write {}", file))
    };

    write_packet(&mut file_out, meta.device.make_update_with_route(route))?;
    for (_id, stream) in meta.streams {
        write_packet(&mut file_out, stream.stream.make_update_with_route(route))?;
        write_packet(&mut file_out, stream.segment.make_update_with_route(route))?;
        for col in stream.columns {
            write_packet(&mut file_out, col.make_update_with_route(route))?;
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
        let (pkt, len) = tio::Packet::deserialize(rest)
            .wrap_err_with(|| format!("could not parse packet in {}", input))?;
        rest = &rest[len..];
        packet_count += 1;

        if !matches!(pkt.payload, tio::proto::Payload::Metadata(_)) {
            bail!(
                "{} does not look like a metadata file (found non-metadata packet)",
                input
            );
        }
        routes.insert(pkt.routing);
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
        let (mut pkt, len) = tio::Packet::deserialize(rest)
            .wrap_err_with(|| format!("could not parse packet in {}", input))?;
        rest = &rest[len..];
        pkt.routing = new_route;
        let raw = pkt
            .serialize()
            .wrap_err_with(|| format!("failed to serialize packet for {}", output_path))?;
        file.write_all(&raw)
            .wrap_err_with(|| format!("failed to write {}", output_path))?;
    }

    Ok(())
}
