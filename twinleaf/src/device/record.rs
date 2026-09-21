//! Recording a live device tree as a log the reader decodes the same way.
//!
//! [`Recorder`] is the reader run forwards: its own [`PacketParser`] scans a
//! tree's packet tap, and only what that parser validated is written, behind
//! the metadata that decodes it. Nothing else decides what the file says.

use super::connection::DeviceTree;
use super::stream::{Completed, Discovery, Receiver, RecvError};
use crate::data::{BoundaryReason, PacketParser};
use crate::proto::DeviceRoute;
use crate::tio;
use crate::tio::packet;
use crossbeam::channel;
use std::collections::HashMap;
use std::io::Write;
use std::time::Instant;

/// What a recording holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordMode {
    /// Every sample the recorder could decode, behind the metadata that
    /// describes it, plus the markers replay needs to follow the live stream.
    Described,
    /// Every packet the tap carries, byte for byte, and no discovery.
    Raw,
}

/// Why a recording stopped.
#[derive(Debug, thiserror::Error)]
pub enum RecordError {
    /// The output refused a write.
    #[error("could not write the recording")]
    Write(#[from] std::io::Error),
    /// A metadata record could not be encoded as a packet.
    #[error("could not encode a metadata packet")]
    Encode(#[from] packet::EncodeError),
    /// The connection stopped, so no further packet can arrive.
    #[error("the stream ended")]
    Disconnected,
}

/// What one [`Recorder::step`] settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// The recording is still going.
    Running,
    /// The caller's deadline passed.
    Deadline,
}

/// The output and what it currently says: the parser deciding what may be
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
            parser: PacketParser::new(DeviceRoute::root(), false),
            described: HashMap::new(),
            bytes_written: 0,
            samples_lost: 0,
        }
    }

    /// Scan one tapped packet as replay will, then write what replay needs:
    /// the samples it validated, and the markers it resets on.
    fn packet(&mut self, packet: &tio::Packet) -> Result<(), RecordError> {
        if let RecordMode::Raw = self.mode {
            return self.write(packet);
        }
        let route = packet.route();
        let (validated, lost) = match self.parser.scan_packet(packet) {
            Ok(Some(rows)) => match rows.boundary() {
                Some(BoundaryReason::SamplesLost { expected, received }) => {
                    let lost = received.wrapping_sub(*expected);
                    log::warn!(
                        "{}/{} dropped {lost} samples",
                        rows.stream_key().route,
                        rows.stream().get().name
                    );
                    (true, u64::from(lost))
                }
                Some(_) | None => (true, 0),
            },
            Ok(None) => (false, 0),
            Err(error) => {
                log::warn!("not recording an invalid packet: {error}");
                (false, 0)
            }
        };
        self.samples_lost += lost;

        match packet.payload() {
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
            packet::Payload::ProxyStatus(_) => self.write(packet),
            packet::Payload::Log(_)
            | packet::Payload::RpcRequest(_)
            | packet::Payload::RpcReply(_)
            | packet::Payload::RpcError(_)
            | packet::Payload::Setting(_)
            | packet::Payload::Unknown(..) => Ok(()),
        }
    }

    /// Splice in what the parser knows about `route` when the file has yet to
    /// describe that revision of it.
    fn describe(&mut self, route: DeviceRoute) -> Result<(), RecordError> {
        let Some(revision) = self.parser.metadata_revision(route) else {
            return Ok(());
        };
        if self.described.get(&route) == Some(&revision) {
            return Ok(());
        }
        let Some(snapshot) = self.parser.metadata(route) else {
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
/// The tap neither decodes nor discovers on the connection's behalf, so a
/// recorder owns its parser and its own `dev.metadata` queries.
pub struct Recorder<W> {
    tap: Receiver<tio::Packet>,
    discovery: Discovery,
    transcript: Transcript<W>,
}

impl<W: Write> Recorder<W> {
    /// Record everything `tree` covers into `writer`.
    pub fn new(tree: &DeviceTree, writer: W, mode: RecordMode) -> Recorder<W> {
        Recorder {
            tap: tree.packets(),
            discovery: Discovery::new(tree.endpoint().clone()),
            transcript: Transcript::new(writer, mode),
        }
    }

    /// Bytes written so far.
    pub fn bytes_written(&self) -> u64 {
        self.transcript.bytes_written
    }

    /// Samples the recording is missing, as the devices reported them lost.
    pub fn samples_lost(&self) -> u64 {
        self.transcript.samples_lost
    }

    /// Flush the writer, so what has been recorded is durable so far.
    pub fn flush(&mut self) -> Result<(), RecordError> {
        Ok(self.transcript.writer.flush()?)
    }

    /// Record whatever has arrived, waiting until `deadline` for the first of
    /// it. Without a deadline, a recorder waits as long as the connection lives.
    pub fn step(&mut self, deadline: Option<Instant>) -> Result<Step, RecordError> {
        for route in self
            .discovery
            .due(self.transcript.parser.routes(), Instant::now())
        {
            self.discovery.submit(&mut self.transcript.parser, route);
        }
        match self.wait(deadline) {
            Step::Deadline => Ok(Step::Deadline),
            Step::Running => {
                self.drain_metadata_replies()?;
                self.drain_tap()?;
                Ok(Step::Running)
            }
        }
    }

    /// Wait for a packet or a reply, giving up at the caller's deadline or at
    /// the moment a backed-off route wants asking again.
    fn wait(&self, deadline: Option<Instant>) -> Step {
        let mut select = channel::Select::new();
        select.recv(self.tap.receiver());
        select.recv(self.discovery.replies());
        match [deadline, self.discovery.next_retry()]
            .into_iter()
            .flatten()
            .min()
        {
            Some(wake) => {
                let _ = select.ready_deadline(wake);
            }
            None => {
                select.ready();
            }
        }
        match deadline {
            Some(deadline) if Instant::now() >= deadline => Step::Deadline,
            Some(_) | None => Step::Running,
        }
    }

    fn drain_metadata_replies(&mut self) -> Result<(), RecordError> {
        while let Ok((query, result)) = self.discovery.replies().try_recv() {
            let route = query.route;
            match self
                .discovery
                .complete(&mut self.transcript.parser, query, result)
            {
                Completed::Applied => self.transcript.describe(route)?,
                Completed::Unsupported | Completed::BackedOff => {}
            }
        }
        Ok(())
    }

    /// Record every packet already tapped. A gap costs the samples it swallowed
    /// and nothing more, as the parser reports it from the packets that arrive.
    fn drain_tap(&mut self) -> Result<(), RecordError> {
        loop {
            match self.tap.try_recv() {
                Ok(Some(packet)) => {
                    if let packet::Payload::ProxyStatus(packet::ProxyStatus::SensorDisconnected) =
                        packet.payload()
                    {
                        self.discovery.forget(packet.route());
                    }
                    self.transcript.packet(&packet)?;
                }
                Ok(None) | Err(RecvError::Timeout) => return Ok(()),
                Err(RecvError::Lagged(skipped)) => {
                    log::warn!("dropped {skipped} packets before they could be recorded")
                }
                Err(RecvError::Disconnected) => return Err(RecordError::Disconnected),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::{LogFile, SampleBatch};
    use crate::proto::data as wire;
    use crate::proto::data::DataType;
    use crate::proto::sync::Epoch;
    use crate::proto::PacketType;
    use crate::tio::packet::{Packet, ProxyStatus};
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
            .scan(DeviceRoute::root(), false)
            .batches(1)
            .map(|batch| batch.expect("the recorded log decodes"))
            .collect();
        drop(log);
        let _ = std::fs::remove_file(&path);
        batches
    }

    /// Behind `--mount`, the marker carries its subtree: replay resets only the
    /// mount that bounced.
    #[test]
    fn a_mounted_disconnect_marker_resets_only_its_subtree_on_replay() {
        let steady: DeviceRoute = "/1".parse().unwrap();
        let bounced: DeviceRoute = "/2".parse().unwrap();
        let mut file = transcript();

        for route in [steady, bounced] {
            describe(&mut file, 1, route);
            file.packet(&samples(0, route)).unwrap();
        }
        file.packet(&Packet::proxy_status(ProxyStatus::SensorDisconnected).with_route(bounced))
            .unwrap();
        describe(&mut file, 1, bounced);
        for route in [steady, bounced] {
            file.packet(&samples(1, route)).unwrap();
        }

        let runs: Vec<(DeviceRoute, bool, u32)> = recorded(&file, "mounted-marker")
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
                (bounced, true, 1),
            ],
            "only the bounced mount's run splits at the marker"
        );
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
                    batch.segment().segment_id.value(),
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
        let Some(BoundaryReason::SamplesLost { expected, received }) = recorded[1].boundary()
        else {
            panic!("replay reports the same gap the recorder did");
        };
        assert_eq!((expected.value(), received.value()), (1, 5));
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
                    batch.device().session.value(),
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
                recorded[1].boundary(),
                Some(BoundaryReason::SessionChanged { .. })
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
            .map(|batch| batch.device().session.value())
            .collect();
        assert_eq!(sessions, [2], "only the session that answered is described");
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
}
