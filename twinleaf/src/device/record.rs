//! Recording a live device tree as a log file.
//!
//! A [`Recorder`] runs its own [`PacketParser`] over a tree's packets, as the
//! [`data`](crate::data) reader does over the file. It writes only the
//! samples that parser validated, each after the metadata that decodes it.
//! [`RecordMode`] picks this or a raw copy of every packet.

use super::connection::DeviceTree;
use super::state::MetadataState;
use super::stream::{Receiver, RecvError};
use crate::data::{announces_stop, PacketParser, RowSource};
use crate::proto::DeviceRoute;
use crate::rand::HashMap;
use crate::tio;
use crate::tio::packet;
use std::io::Write;
use std::time::Instant;

/// What a recording holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordMode {
    /// Every sample the recorder could decode, after the metadata that
    /// describes it, and the markers replay needs to follow the live stream.
    Described,
    /// Every packet the tap carries, byte for byte, and no discovery.
    Raw,
}

/// Why a recording stopped.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RecordError {
    /// The output refused a write.
    #[error("could not write the recording")]
    Write(#[from] std::io::Error),
    /// A metadata record could not be encoded as a packet.
    #[error("could not encode a metadata packet")]
    Encode(#[from] packet::EncodeError),
    /// The connection ended.
    #[error("the stream ended")]
    Disconnected,
    /// The proxy stopped.
    #[error("the proxy stopped")]
    Stopped,
    /// Its owner closed the connection.
    #[error("the connection was closed")]
    Closed,
}

/// The outcome of one [`Recorder::step`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// The recording is still going.
    Running,
    /// The caller's deadline passed.
    Deadline,
}

/// The output and its state. It holds the parser that decides what may be
/// written, and the metadata revision each route was last described at.
struct Transcript<W> {
    writer: W,
    mode: RecordMode,
    parser: PacketParser,
    described: HashMap<DeviceRoute, u32>,
    bytes_written: u64,
    samples_lost: u64,
}

impl<W: Write> Transcript<W> {
    fn new(writer: W, mode: RecordMode) -> Transcript<W> {
        Transcript {
            writer,
            mode,
            parser: PacketParser::new(false),
            described: HashMap::default(),
            bytes_written: 0,
            samples_lost: 0,
        }
    }

    /// Scans one tapped packet as replay will, then writes what replay needs.
    /// That is the samples the parser validated, the markers replay resets
    /// on, and the stops a device announces, which replay tells restarts by.
    fn packet(&mut self, packet: &tio::Packet) -> Result<(), RecordError> {
        if let RecordMode::Raw = self.mode {
            return self.write(packet);
        }
        let route = packet.route();
        let (validated, lost) = match self.parser.scan_packet(packet) {
            Ok(Some(rows)) => match rows.boundary().and_then(|b| b.loss.as_ref()) {
                Some(loss) => {
                    log::warn!(
                        "{}/{} dropped {} samples",
                        rows.stream_key().route,
                        rows.schema().stream().name(),
                        loss.samples
                    );
                    (true, loss.samples)
                }
                None => (true, 0),
            },
            Ok(None) => (false, 0),
            Err(error) => {
                log::warn!("not recording an invalid packet: {error}");
                (false, 0)
            }
        };
        self.samples_lost += lost;

        match packet.payload() {
            packet::Payload::LegacyStatus(_)
            | packet::Payload::ProxyRx(packet::ProxyRx::Link(_)) => self.write(packet),
            packet::Payload::Samples(_) => {
                self.describe(route)?;
                if validated {
                    self.write(packet)?;
                }
                Ok(())
            }
            packet::Payload::Heartbeat(beat) => match beat.session() {
                Some(_) => self.write(packet),
                None => Ok(()),
            },
            packet::Payload::Metadata(..) => self.describe(route),
            packet::Payload::Setting(setting) if announces_stop(&setting) => self.write(packet),
            packet::Payload::ProxyTx(_)
            | packet::Payload::ProxyRx(
                packet::ProxyRx::Reply(_)
                | packet::ProxyRx::Error(_)
                | packet::ProxyRx::Alive
                | packet::ProxyRx::Unknown(..),
            )
            | packet::Payload::Log(_)
            | packet::Payload::RpcRequest(_)
            | packet::Payload::RpcReply(_)
            | packet::Payload::RpcError(_)
            | packet::Payload::Setting(_)
            | packet::Payload::Unknown(..) => Ok(()),
        }
    }

    /// Writes the parser's metadata for `route` if the file does not yet
    /// describe that revision.
    fn describe(&mut self, route: DeviceRoute) -> Result<(), RecordError> {
        let seen = self.described.get(&route).copied();
        let Some((revision, snapshot)) = self.parser.revised_metadata(route, seen) else {
            return Ok(());
        };
        for packet in snapshot.metadata_packets()? {
            self.write(&packet)?;
        }
        self.described.insert(route, revision);
        Ok(())
    }

    fn write(&mut self, packet: &tio::Packet) -> Result<(), RecordError> {
        let raw = packet.as_bytes();
        self.writer.write_all(raw)?;
        self.bytes_written += raw.len() as u64;
        Ok(())
    }
}

/// Records one device tree into a writer.
///
/// A recorder has its own parser, and describes each device with the
/// metadata its connection holds, which the connection collects once for
/// every view. Each [`step`](Self::step) writes what has arrived.
///
/// # Examples
///
/// ```no_run
/// use std::fs::File;
/// use std::io::BufWriter;
/// use std::time::{Duration, Instant};
/// use twinleaf::device::record::{RecordMode, Recorder, Step};
/// use twinleaf::{Connection, DeviceRoute};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let connection = Connection::connect()?;
/// let tree = connection.tree(DeviceRoute::root());
/// let file = BufWriter::new(File::create("minute.tio")?);
/// let mut recorder = Recorder::new(&tree, file, RecordMode::Described);
///
/// let deadline = Instant::now() + Duration::from_secs(60);
/// while recorder.step(Some(deadline))? == Step::Running {}
/// recorder.flush()?;
/// println!(
///     "wrote {} bytes, {} samples lost",
///     recorder.bytes_written(),
///     recorder.samples_lost()
/// );
/// # Ok(())
/// # }
/// ```
pub struct Recorder<W> {
    tree: DeviceTree,
    tap: Receiver<tio::Packet>,
    /// The metadata of each device the tap has carried.
    metadata: HashMap<DeviceRoute, Receiver<MetadataState>>,
    transcript: Transcript<W>,
}

impl<W> std::fmt::Debug for Recorder<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Recorder")
            .field("mode", &self.transcript.mode)
            .field("bytes_written", &self.transcript.bytes_written)
            .field("samples_lost", &self.transcript.samples_lost)
            .finish_non_exhaustive()
    }
}

impl<W: Write> Recorder<W> {
    /// Creates a recorder of everything `tree` covers into `writer`.
    pub fn new(tree: &DeviceTree, writer: W, mode: RecordMode) -> Recorder<W> {
        Recorder {
            tree: tree.clone(),
            tap: tree.packets(),
            metadata: HashMap::default(),
            transcript: Transcript::new(writer, mode),
        }
    }

    /// Returns the number of bytes written so far.
    pub fn bytes_written(&self) -> u64 {
        self.transcript.bytes_written
    }

    /// Returns the number of samples the recording lacks, as the devices
    /// reported them lost.
    pub fn samples_lost(&self) -> u64 {
        self.transcript.samples_lost
    }

    /// Flushes the writer.
    ///
    /// # Errors
    ///
    /// Returns [`RecordError::Write`] if the writer fails to flush.
    pub fn flush(&mut self) -> Result<(), RecordError> {
        Ok(self.transcript.writer.flush()?)
    }

    /// Records whatever has arrived. Blocks until a packet or a device's
    /// metadata arrives, or `deadline` passes.
    ///
    /// Returns [`Step::Deadline`] once `deadline` has passed, and
    /// [`Step::Running`] before then or with no deadline.
    ///
    /// # Errors
    ///
    /// Returns [`RecordError::Write`] or [`RecordError::Encode`] if a packet
    /// cannot be written. Returns [`RecordError::Stopped`],
    /// [`RecordError::Closed`] or [`RecordError::Disconnected`] once the
    /// connection has ended.
    pub fn step(&mut self, deadline: Option<Instant>) -> Result<Step, RecordError> {
        let tapped = match self.wait(deadline) {
            Some(Woken::Tap(tapped)) => tapped.map(Some),
            Some(Woken::Metadata(state)) => {
                self.describe(state)?;
                self.tap.try_recv()
            }
            None => self.tap.try_recv(),
        };
        self.drain_metadata()?;
        self.drain_tap(tapped)?;
        match deadline {
            Some(deadline) if Instant::now() >= deadline => Ok(Step::Deadline),
            Some(_) | None => Ok(Step::Running),
        }
    }

    /// Waits for a packet or a device's metadata, until the caller's
    /// deadline. Returns what woke it, or `None` once the time is up.
    fn wait(&self, deadline: Option<Instant>) -> Option<Woken> {
        let tap = crate::chan::Selector::new().recv(self.tap.as_flume(), |item| {
            Woken::Tap(self.tap.resolve(item))
        });
        let selector = self.metadata.values().fold(tap, |selector, metadata| {
            selector.recv(metadata.as_flume(), |item| {
                Woken::Metadata(metadata.resolve(item))
            })
        });
        match deadline {
            Some(deadline) => selector.wait_deadline(deadline).ok(),
            None => Some(selector.wait()),
        }
    }

    /// Describes each device whose metadata changed since it last looked.
    fn drain_metadata(&mut self) -> Result<(), RecordError> {
        let changed: Vec<_> = self
            .metadata
            .values()
            .filter_map(|metadata| metadata.try_recv().ok().flatten())
            .collect();
        for state in changed {
            self.describe(Ok(state))?;
        }
        Ok(())
    }

    /// Hands the parser a device's metadata once it is complete, as the
    /// device's own records. The file then describes the device before the
    /// samples that follow.
    fn describe(&mut self, state: Result<MetadataState, RecvError>) -> Result<(), RecordError> {
        let Ok(MetadataState::Ready(snapshot)) = state else {
            return Ok(());
        };
        for packet in snapshot.metadata_packets()? {
            self.transcript.packet(&packet)?;
        }
        Ok(())
    }

    /// Follows the metadata of the device that sent `packet`, unless the
    /// tap carried it before or the recording is raw.
    fn follow(&mut self, packet: &tio::Packet) {
        let route = packet.route();
        if self.transcript.mode == RecordMode::Raw
            || packet.link_status().is_some()
            || self.metadata.contains_key(&route)
        {
            return;
        }
        if let Ok(device) = self.tree.device(route) {
            self.metadata.insert(route, device.metadata_updates());
        }
    }

    /// Records `tapped`, and every other packet already tapped. A gap loses
    /// only the samples in it, and the parser reports it from the packets
    /// that follow.
    fn drain_tap(
        &mut self,
        mut tapped: Result<Option<tio::Packet>, RecvError>,
    ) -> Result<(), RecordError> {
        loop {
            match tapped {
                Ok(Some(packet)) => {
                    self.follow(&packet);
                    self.transcript.packet(&packet)?;
                }
                Ok(None) | Err(RecvError::Timeout) => return Ok(()),
                Err(RecvError::Lagged(skipped)) => {
                    log::warn!("dropped {skipped} packets before they could be recorded")
                }
                Err(RecvError::Stopped) => return Err(RecordError::Stopped),
                Err(RecvError::Closed) => return Err(RecordError::Closed),
                Err(RecvError::Disconnected) => return Err(RecordError::Disconnected),
            }
            tapped = self.tap.try_recv();
        }
    }
}

/// What woke a recorder's wait, with what it received.
enum Woken {
    /// A packet from the tap, or why there is none.
    Tap(Result<tio::Packet, RecvError>),
    /// A device's metadata, or why there is none.
    Metadata(Result<MetadataState, RecvError>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::{Change, LogFile, SampleBatch};
    use crate::proto::data as wire;
    use crate::proto::data::DataType;
    use crate::proto::sync::Epoch;
    use crate::proto::PacketType;
    use crate::tio::packet::{LinkStatus, Packet};
    use crate::{ColumnId, SegmentId, SessionId, StreamId};

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

    fn transcript() -> Transcript<Vec<u8>> {
        Transcript::new(Vec::new(), RecordMode::Described)
    }

    fn broadcast(record: wire::Metadata<'_>, route: DeviceRoute) -> Packet {
        Packet::metadata(record, wire::MetadataFlags::UPDATE, route).expect("one record fits")
    }

    /// Tap the broadcast a device in `session` describes itself with.
    fn describe(file: &mut Transcript<Vec<u8>>, session: u32, route: DeviceRoute) {
        for record in metadata_records(session) {
            file.packet(&broadcast(record, route))
                .expect("the recording accepts a broadcast");
        }
    }

    /// One `dev.metadata` reply carrying `records`, in wire framing.
    fn metadata_reply(records: &[wire::Metadata<'_>]) -> Vec<u8> {
        let mut reply = vec![0u8; wire::MAX_METADATA_REPLY_SIZE];
        let written = records.iter().fold(0, |written, record| {
            written
                + record
                    .write_reply_frame(&mut reply[written..])
                    .expect("the fixtures fit one reply")
        });
        reply.truncate(written);
        reply
    }

    fn samples_in(segment_id: u8, first: u32, route: DeviceRoute) -> Packet {
        Packet::samples(STREAM, segment_id, first, &[0; 4], route).expect("valid samples")
    }

    fn samples(first: u32, route: DeviceRoute) -> Packet {
        samples_in(0, first, route)
    }

    /// The packets the recording holds, in the order they were written.
    fn written(file: &Transcript<Vec<u8>>) -> Vec<Packet> {
        let mut rest: &[u8] = &file.writer;
        std::iter::from_fn(|| {
            let (packet, len) = Packet::from_slice_prefix(rest).ok()?;
            rest = &rest[len..];
            Some(packet)
        })
        .collect()
    }

    /// Every batch the recording reads back as, in file order.
    fn recorded(file: &Transcript<Vec<u8>>, name: &str) -> Vec<SampleBatch> {
        let path =
            std::env::temp_dir().join(format!("twinleaf-record-{name}-{}.tio", std::process::id()));
        std::fs::write(&path, &file.writer).expect("the scratch recording");
        let log = LogFile::open(&path).expect("the recorded log");
        let batches = log
            .scan(false)
            .batches(1)
            .map(|batch| batch.expect("the recorded log decodes"))
            .collect();
        drop(log);
        let _ = std::fs::remove_file(&path);
        batches
    }

    /// Behind `--mount`, the marker carries its subtree, so on replay only the
    /// mount that bounced learns its layout again. A live proxy sends the
    /// marker as a link packet and an old one as a legacy status, and the
    /// file keeps whichever arrived.
    #[test]
    fn a_mounted_disconnect_marker_relearns_only_its_subtree_on_replay() {
        let steady: DeviceRoute = "/1".parse().unwrap();
        let bounced: DeviceRoute = "/2".parse().unwrap();
        for (name, marker) in [
            ("mounted-link", Packet::link(LinkStatus::SensorDisconnected)),
            (
                "mounted-legacy",
                Packet::legacy_status(LinkStatus::SensorDisconnected),
            ),
        ] {
            let marker = marker.with_route(bounced);
            let mut file = transcript();

            for route in [steady, bounced] {
                describe(&mut file, 1, route);
                file.packet(&samples(0, route)).unwrap();
            }
            file.packet(&marker).unwrap();
            describe(&mut file, 1, bounced);
            for route in [steady, bounced] {
                file.packet(&samples(1, route)).unwrap();
            }

            assert!(
                written(&file)
                    .iter()
                    .any(|packet| packet.as_bytes() == marker.as_bytes()),
                "{name}: the marker is written as it arrived"
            );
            let runs: Vec<(DeviceRoute, bool, u32)> = recorded(&file, name)
                .iter()
                .map(|batch| {
                    (
                        batch.route(),
                        batch.is_initial(),
                        batch.sample_numbers()[0].value(),
                    )
                })
                .collect();
            assert_eq!(
                runs,
                [
                    (steady, true, 0),
                    (bounced, true, 0),
                    (steady, false, 1),
                    (bounced, false, 1),
                ],
                "{name}: only the bounced mount learns its layout again, and both runs go on"
            );
        }
    }

    /// A device's segment update is followed by samples of the segment it
    /// switched to, which no revision of the file describes yet.
    #[test]
    fn samples_of_an_undescribed_segment_are_not_recorded_under_the_old_one() {
        let route = DeviceRoute::root();
        let mut file = transcript();

        describe(&mut file, 1, route);
        let described = file.bytes_written;
        file.packet(&samples_in(1, 8, route)).unwrap();

        assert_eq!(
            file.bytes_written, described,
            "a segment the file does not describe is not recorded under one it does"
        );
    }

    /// A segment update is described before the first sample of the segment it
    /// switched to, and only through the snapshot: never as the update itself.
    #[test]
    fn a_segment_update_is_described_before_the_samples_that_follow_it() {
        let route = DeviceRoute::root();
        let mut file = transcript();

        describe(&mut file, 1, route);
        file.packet(&samples_in(0, 0, route)).unwrap();
        let wire::Metadata::Segment(mut switched) = metadata_records(1)[2] else {
            panic!("the third record describes a segment");
        };
        switched.segment_id = SegmentId::new(1);
        file.packet(&broadcast(wire::Metadata::Segment(switched), route))
            .unwrap();
        file.packet(&samples_in(1, 8, route)).unwrap();

        let metadata = written(&file)
            .iter()
            .filter(|packet| packet.ptype() == PacketType::METADATA)
            .count();
        assert_eq!(metadata, 8, "one snapshot each, and no update written raw");
        let segments: Vec<(u8, u32)> = recorded(&file, "segment-update")
            .iter()
            .map(|batch| {
                (
                    batch.metadata().segment().segment_id().value(),
                    batch.sample_numbers()[0].value(),
                )
            })
            .collect();
        assert_eq!(
            segments,
            [(0, 0), (1, 8)],
            "each sample reads back under the segment that produced it"
        );
    }

    /// One revision, one description, however many packets it covers.
    #[test]
    fn a_revision_is_described_once() {
        let route = DeviceRoute::root();
        let mut file = transcript();

        describe(&mut file, 1, route);
        let described = file.bytes_written;
        for first in 0..4 {
            file.packet(&samples(first, route)).unwrap();
        }

        let metadata = written(&file)
            .iter()
            .filter(|packet| packet.ptype() == PacketType::METADATA)
            .count();
        assert_eq!(metadata, 4, "the snapshot's four records, written once");
        assert!(
            file.bytes_written > described,
            "the samples the description covers"
        );
    }

    /// A tap that lagged leaves a gap in the samples and nothing else: the file
    /// still decodes, and reports the loss the recorder counted.
    #[test]
    fn a_gap_in_the_tap_is_recorded_as_lost_samples() {
        let route = DeviceRoute::root();
        let mut file = transcript();

        describe(&mut file, 1, route);
        file.packet(&samples(0, route)).unwrap();
        file.packet(&samples(5, route)).unwrap();

        assert_eq!(file.samples_lost, 4, "the samples between the two packets");
        let recorded = recorded(&file, "gap");
        let boundary = recorded[1].boundary().expect("replay reports the gap");
        let loss = boundary.loss.as_ref().expect("replay reports the gap");
        assert_eq!(boundary.change, None);
        assert_eq!(
            (loss.samples, loss.expected.map(|n| n.value())),
            (4, Some(1)),
            "replay reports the same gap the recorder did"
        );
    }

    /// Samples lost across a rollover count as lost, as the rollover names
    /// them.
    #[test]
    fn a_loss_across_a_rollover_is_counted() {
        let route = DeviceRoute::root();
        let mut file = transcript();

        describe(&mut file, 1, route);
        file.packet(&samples_in(0, 0, route)).unwrap();
        let wire::Metadata::Segment(mut next) = metadata_records(1)[2] else {
            panic!("the third record describes a segment");
        };
        (next.segment_id, next.start_time) = (SegmentId::new(1), 1);
        file.packet(&broadcast(wire::Metadata::Segment(next), route))
            .unwrap();
        file.packet(&samples_in(1, 2, route)).unwrap();

        assert_eq!(
            file.samples_lost, 2,
            "sample 0 of the first segment ends at its rollover, and sample 2 follows two lost"
        );
    }

    /// A device's stop is recorded where it came, so replay reads the
    /// samples of its next acquisition as a restart, and the recorder counts
    /// no sample lost in the pause.
    #[test]
    fn a_recorded_stop_replays_as_a_restart() {
        let route = DeviceRoute::root();
        let mut file = transcript();

        describe(&mut file, 1, route);
        file.packet(&samples_in(0, 0, route)).unwrap();
        let mut raw = [0u8; 64];
        let stopped = crate::proto::settings::Setting {
            name: b"dev.running",
            seq: Some(1),
            reply: &[0],
        };
        let len = stopped.write(&mut raw).expect("the announcement fits");
        let (stopped, _) = Packet::from_slice_prefix(&raw[..len]).expect("a packet");
        file.packet(&stopped.with_route(route)).unwrap();
        let wire::Metadata::Segment(mut next) = metadata_records(1)[2] else {
            panic!("the third record describes a segment");
        };
        (next.segment_id, next.start_time) = (SegmentId::new(1), 5);
        file.packet(&broadcast(wire::Metadata::Segment(next), route))
            .unwrap();
        file.packet(&samples_in(1, 0, route)).unwrap();

        assert_eq!(file.samples_lost, 0, "the pause holds no sample to lose");
        let recorded = recorded(&file, "restart");
        assert_eq!(recorded.len(), 2);
        assert_eq!(
            recorded[1].boundary().and_then(|b| b.change.clone()),
            Some(Change::Restarted {
                old_id: SegmentId::new(0),
                new_id: SegmentId::new(1),
            }),
            "the recorded stop marks the restart on replay"
        );
    }

    /// A reboot mid-recording: the samples of the new session wait for the
    /// metadata describing them, and replay splits the runs where it did.
    #[test]
    fn a_session_change_splits_the_recorded_runs() {
        let route = DeviceRoute::root();
        let mut file = transcript();

        describe(&mut file, 1, route);
        file.packet(&samples(0, route)).unwrap();
        file.packet(&Packet::heartbeat_session(2, route)).unwrap();
        let announced = file.bytes_written;
        file.packet(&samples(1, route)).unwrap();
        assert_eq!(
            file.bytes_written, announced,
            "nothing describes the new session yet"
        );
        describe(&mut file, 2, route);
        file.packet(&samples(2, route)).unwrap();

        let recorded = recorded(&file, "session-change");
        let runs: Vec<(u32, u32)> = recorded
            .iter()
            .map(|batch| {
                (
                    batch.metadata().device().session().value(),
                    batch.sample_numbers()[0].value(),
                )
            })
            .collect();
        assert_eq!(
            runs,
            [(1, 0), (2, 2)],
            "each recorded sample reads back under the session that sent it"
        );
        assert!(
            matches!(
                recorded[1].boundary().and_then(|b| b.change.as_ref()),
                Some(Change::SessionChanged { .. })
            ),
            "the recorded heartbeat splits the runs on replay"
        );
    }

    /// A reply the reboot overtook describes the session that ended, so the
    /// parser rejects it and the file never mentions it.
    #[test]
    fn a_stale_metadata_reply_after_a_session_change_does_not_describe_the_route() {
        let route = DeviceRoute::root();
        let mut file = transcript();

        file.packet(&broadcast(metadata_records(1)[0], route))
            .unwrap();
        let stale = file
            .parser
            .take_metadata_queries_for(route)
            .pop()
            .expect("the route still wants its stream");
        file.packet(&Packet::heartbeat_session(2, route)).unwrap();
        let announced = file.bytes_written;
        file.parser
            .apply_metadata_reply(stale, &metadata_reply(&metadata_records(1)));
        file.describe(route).unwrap();
        assert_eq!(
            file.bytes_written, announced,
            "a reply from the session before cannot describe the one after"
        );

        let fresh = file
            .parser
            .take_metadata_queries_for(route)
            .pop()
            .expect("the new session is asked again");
        file.parser
            .apply_metadata_reply(fresh, &metadata_reply(&metadata_records(2)));
        file.describe(route).unwrap();
        file.packet(&samples(0, route)).unwrap();

        let sessions: Vec<u32> = recorded(&file, "stale-reply")
            .iter()
            .map(|batch| batch.metadata().device().session().value())
            .collect();
        assert_eq!(sessions, [2], "only the session that answered is described");
    }

    /// A recording through a proxy that stops ends saying so, rather than
    /// only that its stream ended.
    #[test]
    fn a_recording_ends_saying_the_proxy_stopped() {
        use crate::device::fixtures::{self, FakeDevice, WAIT};

        let device = FakeDevice::start();
        let proxy = fixtures::proxy_serving(&device.url);
        let connection = fixtures::open_through(&proxy, "auto").unwrap();
        let tree = connection.tree(DeviceRoute::root());
        let mut recorder = Recorder::new(&tree, Vec::new(), RecordMode::Raw);
        let deadline = Instant::now() + WAIT;
        while recorder.bytes_written() == 0 {
            assert_eq!(recorder.step(Some(deadline)).unwrap(), Step::Running);
        }

        proxy.shutdown();

        let ended = loop {
            match recorder.step(Some(deadline)) {
                Ok(Step::Running) => {}
                Ok(Step::Deadline) => panic!("the recording outlived the proxy"),
                Err(error) => break error,
            }
        };
        assert!(matches!(ended, RecordError::Stopped), "{ended}");
        assert_eq!(ended.to_string(), "the proxy stopped");
    }

    /// A recording through a proxy describes the device with the metadata
    /// the proxy collected, ahead of the first sample, and asks the device
    /// nothing itself.
    #[test]
    fn a_recording_describes_a_device_with_the_metadata_the_proxy_holds() {
        use crate::device::fixtures::{self, FakeDevice, WAIT};

        let device = FakeDevice::start();
        let proxy = fixtures::proxy_serving(&device.url);
        let connection = fixtures::open_through(&proxy, "auto").unwrap();
        let route = DeviceRoute::root();
        let tree = connection.tree(route);
        let mut recorder = Recorder::new(&tree, Vec::new(), RecordMode::Described);
        let deadline = Instant::now() + WAIT;
        let mut sent = 0;
        while !written(&recorder.transcript)
            .iter()
            .any(|packet| packet.is_data())
        {
            assert!(Instant::now() < deadline, "no sample was recorded");
            device.send(samples(sent, route));
            sent += 1;
            let wake = Instant::now() + std::time::Duration::from_millis(50);
            recorder.step(Some(wake)).unwrap();
        }

        let described: Vec<Vec<u8>> = crate::proxy::fixtures::description()
            .into_iter()
            .map(|record| broadcast(record, route).as_bytes().to_vec())
            .collect();
        let head: Vec<Vec<u8>> = written(&recorder.transcript)
            .iter()
            .take_while(|packet| !packet.is_data())
            .filter(|packet| packet.ptype() == PacketType::METADATA)
            .map(|packet| packet.as_bytes().to_vec())
            .collect();
        assert_eq!(
            head, described,
            "the device's own records, ahead of its samples"
        );
        assert_eq!(device.calls(wire::METADATA_RPC_METHOD), 1);
    }

    /// A raw recording is the tap itself, whatever the parser makes of it.
    #[test]
    fn a_raw_recording_holds_every_tapped_packet() {
        let route = DeviceRoute::root();
        let mut file = Transcript::new(Vec::new(), RecordMode::Raw);

        let tapped = [samples(0, route), Packet::heartbeat(route)];
        for packet in &tapped {
            file.packet(packet).unwrap();
        }

        let expected: Vec<u8> = tapped
            .iter()
            .flat_map(|packet| packet.as_bytes().to_vec())
            .collect();
        assert_eq!(
            file.writer, expected,
            "an undescribed sample and a bare heartbeat are recorded all the same"
        );
    }

    mod end_to_end {
        //! Properties of a [`Recorder`] over a live tree.
        //!
        //! # Observations
        //!
        //! The bytes a recorder hands its writer and when it flushes them,
        //! what [`Recorder::bytes_written`], [`Recorder::samples_lost`] and
        //! [`Recorder::flush`] return, and the requests its connection sends
        //! the proxy. The tree is the root of a connection over a session
        //! whose far end the test plays, so the test sends what the tap
        //! carries.
        //!
        //! # Sources
        //!
        //! The doc of [`RecordMode::Raw`], every packet the tap carries, byte
        //! for byte, and no discovery. The doc of [`Recorder::samples_lost`],
        //! the samples the recording lacks as the devices reported them lost.
        //! The doc of [`Recorder::flush`], which flushes the writer and
        //! returns [`RecordError::Write`] if the writer fails to. The doc of
        //! [`Recorder::bytes_written`]. The doc of
        //! [`MAX_SAMPLE_NUMBER`](crate::proto::data::MAX_SAMPLE_NUMBER),
        //! sample numbers wrap at 24 bits. The doc of
        //! [`Change`](crate::data::Change), where time going backward is a
        //! change, and a change other than a rollover stands for any loss
        //! next to it, so its boundary names none.
        //!
        //! # Model
        //!
        //! The tap is what the proxy sent, in order. A device reports a loss
        //! as a gap in a stream's sample numbers, so a stream whose samples
        //! are numbered `a` then a greater `b` lacks `b - a - 1`. A stream
        //! whose numbers go back, or wrap past the 24 bit limit to small
        //! ones, went back in time, which names no loss.
        //!
        //! # Laws
        //!
        //! - R1, round trip. A raw recording is the packets the proxy sent
        //!   back to back, as a second packet receiver of the tree also
        //!   yields them, and `bytes_written` is its length. Source, the doc
        //!   of `Raw`. It rules out a raw recording that drops, reorders or
        //!   adds packets, and a stream that changes every packet for every
        //!   receiver alike.
        //! - R2, postcondition. A raw recorder sends the proxy nothing, so
        //!   the first packet the connection sends after it has recorded
        //!   everything is a request the test makes. Source, the doc of
        //!   `Raw`, no discovery. It rules out a raw recorder that follows the
        //!   metadata of the devices it hears, or calls their RPCs.
        //! - R3, postcondition. A described recording of described devices
        //!   reports the sum of the gaps in each stream's sample numbers lost,
        //!   none when nothing skips, and none for a step back or a wrap.
        //!   Source, the doc of `samples_lost` and the model. It rules out a
        //!   count fixed at 0 or 1, one that sums the gaps of a device's
        //!   streams together, and one that counts a step back as a loss.
        //! - R4, postcondition. Once `flush` returns, the writer has flushed
        //!   every byte written, and a writer that fails to flush gives
        //!   `RecordError::Write`. Source, the doc of `flush`. It rules out a
        //!   flush that never reaches the writer.
        //!
        //! # Generator obligations
        //!
        //! For R1 and R2, 1 to 16 packets of samples, heartbeats with and
        //! without a session, metadata records, link statuses and setting
        //! announcements, from routes of 0 to 2 hops. For R3, one or two
        //! devices at routes of 0 to 2 hops, each of two streams, and 1 to 24
        //! sample packets among the streams. Each stream starts at 0 or 3
        //! below the 24 bit limit. Each step skips 0 to 3 samples, no skip
        //! half the time, or goes back 1 to 3 samples one time in four, and a
        //! skip past the limit wraps. A writer that flushes and one that
        //! fails to, for each.
        //!
        //! # Exclusions
        //!
        //! What a described recording holds besides samples, metadata and the
        //! markers replay needs. [`RecordMode::Described`] does not say it
        //! holds nothing else, so whether a setting other than a stop is
        //! written is not checked. Whether a described recorder follows a
        //! device it already follows again, which only a proxy that answers
        //! the watches would show.
        //!
        //! # Examples that stay
        //!
        //! None.

        use super::*;
        use crate::device::fixtures::{self, WAIT};
        use crate::device::session::{Session, TestProxy};
        use crate::device::Connection;
        use crate::proto::data::MAX_SAMPLE_NUMBER;
        use crate::proxy::{decode, Watch, WATCH};
        use proptest::prelude::*;
        use std::collections::{BTreeMap, BTreeSet};
        use std::sync::{Arc, Mutex};
        use std::time::Duration;

        /// What a [`Spy`] saw, the bytes written, the number written at each
        /// flush, and whether it fails to flush.
        #[derive(Default)]
        struct Seen {
            written: Vec<u8>,
            flushed: Vec<usize>,
            fails: bool,
        }

        /// A writer the test reads back.
        #[derive(Clone, Default)]
        struct Spy(Arc<Mutex<Seen>>);

        impl Write for Spy {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                crate::chan::lock(&self.0).written.extend_from_slice(buf);
                Ok(buf.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                let mut seen = crate::chan::lock(&self.0);
                if seen.fails {
                    return Err(std::io::Error::other("the disk is gone"));
                }
                let written = seen.written.len();
                seen.flushed.push(written);
                Ok(())
            }
        }

        impl Spy {
            fn failing(fails: bool) -> Spy {
                let spy = Spy::default();
                crate::chan::lock(&spy.0).fails = fails;
                spy
            }

            fn written(&self) -> Vec<u8> {
                crate::chan::lock(&self.0).written.clone()
            }
        }

        fn route_of(hops: &[u8]) -> DeviceRoute {
            DeviceRoute::from_hops(hops).unwrap()
        }

        fn hops() -> impl Strategy<Value = Vec<u8>> {
            prop::collection::vec(0u8..3, 0..=2)
        }

        /// One packet a device sends, from the route of `hops`.
        fn tapped() -> impl Strategy<Value = Packet> {
            let route = hops().prop_map(|hops| route_of(&hops));
            (route, 0u8..6, any::<u16>()).prop_map(|(route, kind, n)| match kind {
                0 => fixtures::samples(u32::from(n), route),
                1 => Packet::heartbeat(route),
                2 => Packet::heartbeat_session(u32::from(n), route),
                3 => fixtures::broadcast(fixtures::metadata_records(1)[usize::from(n % 4)], route),
                4 => Packet::link(LinkStatus::SensorReconnected).with_route(route),
                _ => fixtures::announcement("test.amplitude", Some(u32::from(n)), &[1], route),
            })
        }

        /// A connection's root tree over a session whose far end the test
        /// plays.
        fn tree() -> (Connection, DeviceTree, TestProxy) {
            let (session, proxy) = Session::test_pair();
            let connection = Connection::over(session);
            let tree = connection.tree(DeviceRoute::root());
            (connection, tree, proxy)
        }

        /// Steps `recorder` until `done` holds, or fails the case at
        /// [`WAIT`].
        fn record_until<W: Write>(
            recorder: &mut Recorder<W>,
            done: impl Fn(&Recorder<W>) -> bool,
        ) -> Result<(), TestCaseError> {
            let deadline = Instant::now() + WAIT;
            while !done(recorder) {
                prop_assert!(Instant::now() < deadline, "the recorder never caught up");
                let wake = Instant::now() + Duration::from_millis(20);
                prop_assert!(recorder.step(Some(wake)).is_ok());
            }
            Ok(())
        }

        /// Checks R4 of `recorder` over `spy`.
        fn check_flush(recorder: &mut Recorder<Spy>, spy: &Spy) -> Result<(), TestCaseError> {
            let fails = crate::chan::lock(&spy.0).fails;
            match (fails, recorder.flush()) {
                (false, Ok(())) => {
                    let seen = crate::chan::lock(&spy.0);
                    prop_assert_eq!(seen.flushed.last().copied(), Some(seen.written.len()));
                }
                (true, Err(RecordError::Write(_))) => {}
                (fails, outcome) => {
                    prop_assert!(
                        false,
                        "a writer that fails {} flushed with {:?}",
                        fails,
                        outcome
                    )
                }
            }
            Ok(())
        }

        /// Whether `packet` asks the proxy to watch the device at `route`.
        fn watches(packet: &Packet, route: DeviceRoute) -> bool {
            let packet::Payload::ProxyTx(request) = packet.payload() else {
                return false;
            };
            request.method == crate::proto::rpc::Method::ByName(WATCH.as_bytes())
                && decode::<Watch>(request.args).is_ok_and(|watch| watch.route == route)
        }

        /// Every packet the connection sends, up to and including the watch
        /// of `last`.
        fn sent_until(proxy: &TestProxy, last: DeviceRoute) -> Vec<Packet> {
            let mut sent = Vec::new();
            loop {
                let packet = proxy
                    .sent
                    .recv_timeout(WAIT)
                    .expect("the watch of the last route");
                let done = watches(&packet, last);
                sent.push(packet);
                if done {
                    return sent;
                }
            }
        }

        /// The records of a device in session 1 with streams 1 and 2, each as
        /// the fixtures' one stream describes it.
        fn two_streams(route: DeviceRoute) -> Vec<Packet> {
            let [device, stream, segment, column] = fixtures::metadata_records(1);
            let (
                wire::Metadata::Device(device),
                wire::Metadata::Stream(stream),
                wire::Metadata::Segment(segment),
                wire::Metadata::Column(column),
            ) = (device, stream, segment, column)
            else {
                panic!("the fixtures describe a device, a stream, a segment and a column");
            };
            let mut records = vec![wire::Metadata::Device(wire::Device {
                n_streams: 2,
                ..device
            })];
            for id in [StreamId::new(1), StreamId::new(2)] {
                records.extend([
                    wire::Metadata::Stream(wire::Stream {
                        stream_id: id,
                        ..stream
                    }),
                    wire::Metadata::Segment(wire::Segment {
                        stream_id: id,
                        ..segment
                    }),
                    wire::Metadata::Column(wire::Column {
                        stream_id: id,
                        ..column
                    }),
                ]);
            }
            records
                .into_iter()
                .map(|record| fixtures::broadcast(record, route))
                .collect()
        }

        /// One step of a stream's sample numbers, a skip of 0 to 3 samples
        /// forward or a step back of 1 to 3.
        #[derive(Debug, Clone, Copy)]
        enum Stride {
            Skip(u32),
            Back(u32),
        }

        fn stride() -> impl Strategy<Value = Stride> {
            prop_oneof![
                2 => Just(Stride::Skip(0)),
                1 => (1u32..4).prop_map(Stride::Skip),
                1 => (1u32..4).prop_map(Stride::Back),
            ]
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(32))]

            /// R1, R2 and R4. A raw recording is the tap, and asks the proxy
            /// for nothing.
            #[test]
            fn a_raw_recording_is_the_tap_and_asks_nothing(
                sent in prop::collection::vec(tapped(), 1..=16),
                fails in any::<bool>(),
            ) {
                let (_connection, tree, proxy) = tree();
                let tap = tree.packets();
                let spy = Spy::failing(fails);
                let mut recorder = Recorder::new(&tree, spy.clone(), RecordMode::Raw);
                for packet in &sent {
                    proxy.send(packet.clone());
                }
                let expected: Vec<u8> = sent
                    .iter()
                    .flat_map(|packet| packet.as_bytes().to_vec())
                    .collect();
                let mut tapped = Vec::new();
                for _ in &sent {
                    let packet = tap.recv_timeout(WAIT).expect("the tap carries each packet");
                    tapped.extend_from_slice(packet.as_bytes());
                }
                prop_assert_eq!(&tapped, &expected, "the tap is what the proxy sent");

                record_until(&mut recorder, |recorder| {
                    recorder.bytes_written() >= expected.len() as u64
                })?;
                prop_assert_eq!(spy.written(), expected.clone());
                prop_assert_eq!(recorder.bytes_written(), expected.len() as u64);
                check_flush(&mut recorder, &spy)?;

                // Every route the generator makes sorts before `/255`, so a
                // watch of one is requested no later than this one, and
                // anything else the recorder sent is already on its way.
                let last = route_of(&[255]);
                let _followed = tree.device(last).unwrap().metadata_updates();
                let asked = sent_until(&proxy, last);
                prop_assert_eq!(asked.len(), 1, "the recorder sent {:?}", asked);
            }

            /// R3 and R4. A described recording counts the samples its
            /// devices' streams skipped.
            #[test]
            fn a_described_recording_counts_the_samples_skipped(
                devices in prop::collection::btree_set(hops(), 1..=2),
                start in prop_oneof![Just(0), Just(MAX_SAMPLE_NUMBER - 3)],
                samples in prop::collection::vec(
                    (any::<prop::sample::Index>(), 1u8..=2, stride()),
                    1..=24,
                ),
                fails in any::<bool>(),
            ) {
                let devices: Vec<DeviceRoute> = devices.iter().map(|hops| route_of(hops)).collect();
                let (_connection, tree, proxy) = tree();
                let spy = Spy::failing(fails);
                let mut recorder = Recorder::new(&tree, spy.clone(), RecordMode::Described);
                for &route in &devices {
                    for packet in two_streams(route) {
                        proxy.send(packet);
                    }
                }
                let mut last_number: BTreeMap<(DeviceRoute, u8), u32> = BTreeMap::new();
                // Rows numbered at or below the last rows of their segment
                // are of a new acquisition whose record never comes, so the
                // stream's later rows wait and count no loss.
                let mut waiting = BTreeSet::new();
                let mut lost = 0;
                for (device, stream, stride) in &samples {
                    let route = devices[device.index(devices.len())];
                    let key = (route, *stream);
                    let number = match (last_number.get(&key), stride) {
                        (None, _) => start,
                        (Some(&before), Stride::Back(back)) if before >= *back => before - back,
                        (Some(&before), Stride::Back(_)) => before + 1,
                        (Some(&before), Stride::Skip(skip)) => {
                            let after = before + 1 + skip;
                            match after > MAX_SAMPLE_NUMBER {
                                true => after - MAX_SAMPLE_NUMBER - 1,
                                false => {
                                    if !waiting.contains(&key) {
                                        lost += u64::from(*skip);
                                    }
                                    after
                                }
                            }
                        }
                    };
                    if last_number.get(&key).is_some_and(|&before| number <= before) {
                        waiting.insert(key);
                    }
                    last_number.insert(key, number);
                    let packet = Packet::samples(*stream, 0, number, &[0; 4], route).unwrap();
                    proxy.send(packet);
                }
                let last = Packet::heartbeat_session(1, devices[0]);
                proxy.send(last.clone());

                record_until(&mut recorder, |_| spy.written().ends_with(last.as_bytes()))?;
                prop_assert_eq!(recorder.samples_lost(), lost);
                prop_assert_eq!(recorder.bytes_written(), spy.written().len() as u64);
                check_flush(&mut recorder, &spy)?;
            }
        }
    }
}
