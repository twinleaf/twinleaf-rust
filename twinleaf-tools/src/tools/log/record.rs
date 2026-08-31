use crate::tools::recv_before;
use crate::{ProxyHelp, TioOpts};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Write;
use std::time::{Duration, Instant};
use twinleaf::data::{DeviceMetadataSnapshot, SampleBatch};
use twinleaf::device::{DeviceEvent, DeviceRoute, Event, LinkEvent, RecvError};
use twinleaf::tio;
use twinleaf::tio::proto::ProxyStatus;
use twinleaf::{Connection, Receiver, SegmentId, StreamId};

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

/// What the file currently says about each route: the snapshot last spliced
/// into it, which is what everything written after it will be decoded under.
///
/// The event lane publishes one snapshot per metadata revision, so it is the
/// single mechanism that describes a route — a revision is written once, and
/// only the packets that revision describes are written under it.
#[derive(Default)]
struct Described(HashMap<DeviceRoute, DeviceMetadataSnapshot>);

impl Described {
    /// Whether the file already describes the exact stream and segment a data
    /// packet carries. A packet the file has yet to be told about would be
    /// read back under the record its own update replaced.
    fn covers(&self, route: DeviceRoute, stream_id: StreamId, segment_id: SegmentId) -> bool {
        self.0
            .get(&route)
            .and_then(|snapshot| snapshot.stream(stream_id))
            .is_some_and(|stream| stream.segment().segment_id == segment_id)
    }

    /// Splice everything the event lane has published into the file. False
    /// once the lane has lagged: the gate no longer matches what the file
    /// says, so nothing is written until the caller resubscribes and the
    /// fresh lane's replay redescribes every stream.
    fn drain_events(&mut self, rec: &mut Recorder, events: &Receiver<Event>) -> eyre::Result<bool> {
        loop {
            match events.try_recv() {
                Ok(Some(event)) => self.apply(rec, event)?,
                Ok(None) | Err(RecvError::Disconnected) | Err(RecvError::Timeout) => {
                    return Ok(true)
                }
                Err(RecvError::Lagged(skipped)) => {
                    log::warn!("dropped {skipped} events; resubscribing to redescribe the streams");
                    self.0.clear();
                    return Ok(false);
                }
            }
        }
    }

    /// Take one fact: a published snapshot describes its route from here on,
    /// and a disconnect ends what the file said about the subtree it names —
    /// the session that follows redescribes itself, and nothing of it belongs
    /// under the records of the one before.
    fn apply(&mut self, rec: &mut Recorder, event: Event) -> eyre::Result<()> {
        match event {
            Event::Device {
                route,
                event: DeviceEvent::Metadata(snapshot),
            } => {
                for packet in snapshot.metadata_packets()? {
                    rec.write(packet)?;
                }
                self.0.insert(route, snapshot);
            }
            Event::Link {
                subtree,
                event: LinkEvent::Status(ProxyStatus::SensorDisconnected),
            } => self.0.retain(|route, _| !route.starts_with(&subtree)),
            Event::Link { .. } | Event::Tree { .. } | Event::Device { .. } => {}
        }
        Ok(())
    }

    /// Record a data packet, but only once the file describes the very segment
    /// it carries, so every recorded sample decodes from the file alone, and
    /// decodes as what it was. A status passes through ungated: it is the
    /// marker replay resets on, splitting the runs where the live stream did
    /// — the one signal left when a device reconnects within one session.
    fn write_data(&self, rec: &mut Recorder, pkt: &tio::Packet) -> eyre::Result<()> {
        match pkt.payload() {
            tio::proto::Payload::Samples(samples) => {
                if self.covers(pkt.route(), samples.stream_id, samples.segment_id) {
                    rec.write(pkt.with_ttl(0)?)?;
                }
            }
            tio::proto::Payload::ProxyStatus(_) => rec.write(pkt.with_ttl(0)?)?,
            _ => {}
        }
        Ok(())
    }
}

/// Count the samples the parsed lane reports lost. Nothing is described from
/// here: a boundary is news about continuity, not about what the file says.
fn count_lost_samples(rec: &mut Recorder, batches: &Receiver<SampleBatch>) {
    use twinleaf::data::BoundaryReason;

    loop {
        let batch = match batches.try_recv() {
            Ok(Some(batch)) => batch,
            Ok(None) | Err(RecvError::Disconnected) | Err(RecvError::Timeout) => return,
            Err(RecvError::Lagged(skipped)) => {
                log::warn!("dropped {skipped} sample batches");
                continue;
            }
        };
        let Some(boundary) = batch.boundary() else {
            continue;
        };
        let BoundaryReason::SamplesLost { expected, received } = boundary.reason else {
            continue;
        };
        let count = received.wrapping_sub(expected);
        rec.samples_dropped += count as u64;
        log::warn!(
            "{}/{} dropped {} samples",
            batch.route(),
            batch.stream().name,
            count
        );
    }
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

    let connection = Connection::open(&tio.root);
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
        log_raw(&connection, route, depth, rec, deadline)
    } else {
        log_parsed(&connection, route, rec, deadline)
    }
}

fn log_raw(
    connection: &Connection,
    route: DeviceRoute,
    depth: Option<usize>,
    mut rec: Recorder,
    deadline: Option<Instant>,
) -> eyre::Result<()> {
    let tree = connection.tree(route);
    let tree = depth.map_or_else(|| tree.clone(), |depth| tree.to_depth(depth));
    let packets = tree.packets();

    loop {
        let pkt = match recv_before(&packets, deadline, "packets") {
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
                return Err(error.wrap_err(context));
            }
        };
        rec.write(pkt)?;
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
    connection: &Connection,
    route: DeviceRoute,
    mut rec: Recorder,
    deadline: Option<Instant>,
) -> eyre::Result<()> {
    let tree = connection.tree(route);

    // Byte-faithful recorder: the raw tap supplies the stream-data packets
    // verbatim, while the parsed lanes say what describes them. The tap runs
    // ahead of the event lane by the pump's parse latency, so a packet whose
    // description has not been spliced in yet is dropped rather than recorded
    // under the description it replaces.
    let packets = tree.packets();
    let mut events = tree.events();
    let batches = tree.samples();

    let mut described = Described::default();
    loop {
        let pkt = match recv_before(&packets, deadline, "packets") {
            Ok(Some(pkt)) => pkt,
            Ok(None) => break,
            Err(error) => return Err(error.wrap_err("stream ended")),
        };

        if !described.drain_events(&mut rec, &events)? {
            events = tree.events();
        }
        count_lost_samples(&mut rec, &batches);
        described.write_data(&mut rec, &pkt)?;

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

    let connection = Connection::open(&tio.root);
    let route = tio.route;

    let device = connection.device(route);

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

    for packet in meta.metadata_packets()? {
        write_packet(&mut file_out, packet)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use twinleaf::data::{LogFile, PacketParser};
    use twinleaf::tio::proto::{DataType, Packet};
    use twinleaf::{ColumnId, SessionId};
    use twinleaf_proto::data as wire;
    use twinleaf_proto::sync::Epoch;

    const STREAM: u8 = 1;

    /// The four records that let a parser decode one f32 stream, as the device
    /// in `session` describes them.
    fn metadata_records(session: u32) -> [wire::Metadata<'static>; 4] {
        [
            wire::Metadata::Device(wire::Device {
                session: SessionId::new(session),
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
                timeref_session: SessionId::new(session),
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
        ]
    }

    /// The snapshot the pump's parser would publish for `session`.
    fn snapshot(session: u32, route: DeviceRoute) -> DeviceMetadataSnapshot {
        let mut parser = PacketParser::new(DeviceRoute::root(), false);
        for record in metadata_records(session) {
            let packet = Packet::metadata(record, wire::MetadataFlags::UPDATE, route)
                .expect("one record fits");
            parser.push_packet(&packet).expect("valid metadata");
        }
        parser.metadata(route).expect("complete metadata")
    }

    fn described(session: u32, route: DeviceRoute) -> Event {
        Event::Device {
            route,
            event: DeviceEvent::Metadata(snapshot(session, route)),
        }
    }

    fn samples_in(segment_id: u8, first: u32, route: DeviceRoute) -> Packet {
        Packet::samples(STREAM, segment_id, first, &[0; 4], route).expect("valid samples")
    }

    fn samples(first: u32, route: DeviceRoute) -> Packet {
        samples_in(0, first, route)
    }

    fn recorder(path: &str) -> Recorder {
        Recorder::new(
            indicatif::ProgressBar::hidden(),
            path.to_string(),
            String::new(),
            false,
        )
    }

    fn scratch_file(name: &str) -> String {
        std::env::temp_dir()
            .join(format!("twinleaf-{}-{}.tio", name, std::process::id()))
            .to_string_lossy()
            .into_owned()
    }

    /// Every sample the file holds, paired with the session the file says it
    /// belongs to.
    fn recorded_sessions(path: &str) -> Vec<(u32, Vec<u32>)> {
        let log = LogFile::open(path).expect("the recorded log");
        log.scan(DeviceRoute::root(), false)
            .batches(1)
            .map(|batch| {
                let batch = batch.expect("the recorded log decodes");
                let samples = batch.sample_numbers().iter().map(|n| n.value()).collect();
                (batch.device().session.value(), samples)
            })
            .collect()
    }

    /// A disconnect ends the session the file describes. The tap runs ahead of
    /// the event lane, so the next session's first packets arrive before
    /// anything describes them: they are dropped, never read back as samples
    /// of the session that ended.
    #[test]
    fn a_reconnect_never_records_samples_under_the_session_before_it() {
        let route = DeviceRoute::root();
        let path = scratch_file("reconnect");
        let mut rec = recorder(&path);
        let mut file = Described::default();

        file.apply(&mut rec, described(1, route)).unwrap();
        file.write_data(&mut rec, &samples(0, route)).unwrap();
        file.apply(
            &mut rec,
            Event::Link {
                subtree: route,
                event: LinkEvent::Status(ProxyStatus::SensorDisconnected),
            },
        )
        .unwrap();
        file.write_data(&mut rec, &samples(0, route)).unwrap();
        file.apply(&mut rec, described(2, route)).unwrap();
        file.write_data(&mut rec, &samples(1, route)).unwrap();
        drop(rec);

        let recorded = recorded_sessions(&path);
        let _ = std::fs::remove_file(&path);
        assert_eq!(
            recorded,
            [(1, vec![0]), (2, vec![1])],
            "each recorded sample is read back under the session that sent it"
        );
    }

    /// A link drop leaves the device's session, segment, and numbering
    /// untouched — the common reconnect — so the recorded status is the only
    /// thing telling replay to split the runs where the live stream did.
    #[test]
    fn a_same_session_reconnect_replays_as_two_runs() {
        let route = DeviceRoute::root();
        let path = scratch_file("same-session");
        let mut rec = recorder(&path);
        let mut file = Described::default();

        file.apply(&mut rec, described(1, route)).unwrap();
        file.write_data(&mut rec, &samples(0, route)).unwrap();
        file.write_data(
            &mut rec,
            &Packet::proxy_status(ProxyStatus::SensorDisconnected),
        )
        .unwrap();
        file.apply(
            &mut rec,
            Event::Link {
                subtree: route,
                event: LinkEvent::Status(ProxyStatus::SensorDisconnected),
            },
        )
        .unwrap();
        file.apply(&mut rec, described(1, route)).unwrap();
        file.write_data(&mut rec, &samples(5, route)).unwrap();
        drop(rec);

        let log = LogFile::open(&path).expect("the recorded log");
        let runs: Vec<(bool, u32)> = log
            .scan(DeviceRoute::root(), false)
            .batches(1)
            .map(|batch| {
                let batch = batch.expect("the recorded log decodes");
                (batch.is_initial(), batch.sample_numbers()[0].value())
            })
            .collect();
        let _ = std::fs::remove_file(&path);
        assert_eq!(
            runs,
            [(true, 0), (true, 5)],
            "the recorded disconnect opens a new run on replay"
        );
    }

    /// Behind `--mount`, the marker carries its subtree: replay resets only
    /// the mount that bounced.
    #[test]
    fn a_mounted_disconnect_marker_resets_only_its_subtree_on_replay() {
        let steady: DeviceRoute = "/1".parse().unwrap();
        let bounced: DeviceRoute = "/2".parse().unwrap();
        let path = scratch_file("mounted-marker");
        let mut rec = recorder(&path);
        let mut file = Described::default();

        for route in [steady, bounced] {
            file.apply(&mut rec, described(1, route)).unwrap();
            file.write_data(&mut rec, &samples(0, route)).unwrap();
        }
        file.write_data(
            &mut rec,
            &Packet::proxy_status(ProxyStatus::SensorDisconnected).with_route(bounced),
        )
        .unwrap();
        file.apply(
            &mut rec,
            Event::Link {
                subtree: bounced,
                event: LinkEvent::Status(ProxyStatus::SensorDisconnected),
            },
        )
        .unwrap();
        file.apply(&mut rec, described(1, bounced)).unwrap();
        for route in [steady, bounced] {
            file.write_data(&mut rec, &samples(1, route)).unwrap();
        }
        drop(rec);

        let log = LogFile::open(&path).expect("the recorded log");
        let runs: Vec<(DeviceRoute, bool, u32)> = log
            .scan(DeviceRoute::root(), false)
            .batches(1)
            .map(|batch| {
                let batch = batch.expect("the recorded log decodes");
                (
                    batch.route(),
                    batch.is_initial(),
                    batch.sample_numbers()[0].value(),
                )
            })
            .collect();
        let _ = std::fs::remove_file(&path);
        assert_eq!(
            runs,
            [
                (steady, true, 0),
                (bounced, true, 0),
                (steady, false, 1),
                (bounced, true, 1),
            ],
            "only the bounced mount's run splits at the marker"
        );
    }

    /// What `drain_events` does on `Lagged`: the gate clears, samples stop
    /// being written, and the fresh lane's replayed description reopens the
    /// file — never a sample under a description the lag made stale.
    #[test]
    fn a_lagged_event_lane_drops_samples_until_redescribed() {
        let route = DeviceRoute::root();
        let path = scratch_file("lagged");
        let mut rec = recorder(&path);
        let mut file = Described::default();

        file.apply(&mut rec, described(1, route)).unwrap();
        file.write_data(&mut rec, &samples(0, route)).unwrap();
        file.0.clear();
        file.write_data(&mut rec, &samples(1, route)).unwrap();
        file.apply(&mut rec, described(1, route)).unwrap();
        file.write_data(&mut rec, &samples(2, route)).unwrap();
        drop(rec);

        let recorded = recorded_sessions(&path);
        let _ = std::fs::remove_file(&path);
        assert_eq!(
            recorded,
            [(1, vec![0]), (1, vec![2])],
            "nothing was recorded while the gate distrusted itself"
        );
    }

    /// The tap runs ahead of the event lane, so a device's segment update is
    /// followed by samples the file has yet to be told about. They belong to
    /// the segment that arrived, not to the one its update replaced.
    #[test]
    fn samples_of_an_undescribed_segment_are_not_recorded_under_the_old_one() {
        let route = DeviceRoute::root();
        let path = scratch_file("segment");
        let mut rec = recorder(&path);
        let mut file = Described::default();

        file.apply(&mut rec, described(1, route)).unwrap();
        let described_bytes = rec.bytes_written;
        file.write_data(&mut rec, &samples_in(1, 8, route)).unwrap();
        let written = rec.bytes_written;
        drop(rec);
        let _ = std::fs::remove_file(&path);

        assert_eq!(
            written, described_bytes,
            "a segment the file does not describe is not recorded under one it does"
        );
    }

    /// One revision, one description: the event lane owns the splice, so a
    /// route is described once however many lanes reported it.
    #[test]
    fn a_revision_is_described_once() {
        let route = DeviceRoute::root();
        let path = scratch_file("revision");
        let mut rec = recorder(&path);
        let mut file = Described::default();

        file.apply(&mut rec, described(1, route)).unwrap();
        let described_bytes = rec.bytes_written;
        for first in 0..4 {
            file.write_data(&mut rec, &samples(first, route)).unwrap();
        }
        let data_bytes = rec.bytes_written - described_bytes;
        drop(rec);

        let log = LogFile::open(&path).expect("the recorded log");
        let metadata = log
            .packets()
            .filter(|packet| {
                matches!(
                    packet.as_ref().expect("a recorded packet").payload(),
                    tio::proto::Payload::Metadata(_, _)
                )
            })
            .count();
        drop(log);
        let _ = std::fs::remove_file(&path);
        assert_eq!(metadata, 4, "the snapshot's four records, written once");
        assert!(data_bytes > 0, "the samples the description covers");
    }
}
