//! Writing a recording that [`LogFile`](crate::LogFile) replays.

use crate::decode::parser::PacketParser;
use crate::model::sample::RowSource;
use crate::model::schema::DeviceMetadataSnapshot;
use crate::proto::packet::{Checked, Payload};
use crate::proto::proxy::ProxyRx;
use crate::proto::DeviceRoute;
use std::io::{self, Write};

/// What a recording holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordMode {
    /// Every sample the writer could decode, after the metadata that
    /// describes it, and the markers replay needs to follow the live stream.
    Described,
    /// Every packet the writer is handed, byte for byte, and no discovery.
    Raw,
}

/// Writes packets as a log file that [`LogFile`](crate::LogFile)
/// replays.
///
/// A described file holds the samples its own [`PacketParser`] validated.
/// Each comes after the metadata that decodes it, which the file holds once
/// for each revision of a device's metadata and never as the update packets
/// that revised it. The file also holds the link markers replay resets on,
/// the proxy's counts of the samples it dropped, and the heartbeats that name
/// a session, which replay tells a reboot by.
///
/// A raw file holds every packet the writer is handed, byte for byte.
///
/// # Examples
///
/// ```
/// use twinleaf_data::{LogFile, LogWriter, RecordMode};
/// use twinleaf_data::proto::packet::Checked;
/// # use twinleaf_data::proto::data::{self as wire, DataType};
/// # use twinleaf_data::proto::sync::Epoch;
/// # use twinleaf_data::proto::{ColumnId, SampleNumber, SegmentId, SessionId, StreamId};
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let (id, mut buf) = (StreamId::new(1), [0u8; 512]);
/// # let mut packets = Vec::new();
/// # for record in [
/// #     wire::Metadata::Device(wire::Device { session: SessionId::new(1), n_streams: 1, name: "sensor", serial: "S1", firmware: "1", protocol: 0 }),
/// #     wire::Metadata::Stream(wire::Stream { stream_id: id, n_columns: 1, n_segments: 1, sample_size: 4, buf_samples: 128, name: "field" }),
/// #     wire::Metadata::Segment(wire::Segment { stream_id: id, segment_id: SegmentId::new(0), flags: wire::SegmentFlags::VALID, epoch: Epoch::UNIX, timeref_serial: "", timeref_session: SessionId::new(1), start_time: 0, sampling_rate: 10, decimation: 1, filter_cutoff: 0.0, filter_type: wire::FilterType::NONE }),
/// #     wire::Metadata::Column(wire::Column { stream_id: id, index: ColumnId::new(0), data_type: DataType::F32, name: "x", units: "nT", description: "" }),
/// # ] {
/// #     let len = record.write(wire::MetadataFlags::UPDATE, &mut buf)?;
/// #     packets.push(buf[..len].to_vec());
/// # }
/// # let data: Vec<u8> = [1.0f32, 2.0, 3.0].iter().flat_map(|x| x.to_le_bytes()).collect();
/// # let samples = wire::Samples { stream_id: id, segment_id: SegmentId::new(0), first: SampleNumber::new(0), data: &data };
/// # let len = samples.write(&mut buf)?;
/// # packets.push(buf[..len].to_vec());
/// let mut writer = LogWriter::new(Vec::new(), RecordMode::Described);
/// for packet in &packets {
///     writer.packet(Checked::exact(packet)?)?;
/// }
/// writer.flush()?;
/// assert_eq!(writer.samples_lost(), 0);
/// # Ok(())
/// # }
/// ```
pub struct LogWriter<W> {
    writer: W,
    mode: RecordMode,
    /// Decides which samples may be written, as replay will read them.
    parser: PacketParser,
    bytes_written: u64,
    samples_lost: u64,
}

impl<W> std::fmt::Debug for LogWriter<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogWriter")
            .field("mode", &self.mode)
            .field("bytes_written", &self.bytes_written)
            .field("samples_lost", &self.samples_lost)
            .finish_non_exhaustive()
    }
}

impl<W: Write> LogWriter<W> {
    /// Creates a writer of a recording in `mode` into `writer`.
    pub fn new(writer: W, mode: RecordMode) -> LogWriter<W> {
        LogWriter {
            writer,
            mode,
            parser: PacketParser::new(false),
            bytes_written: 0,
            samples_lost: 0,
        }
    }
}

impl<W> LogWriter<W> {
    /// Returns the mode the writer records in.
    pub fn mode(&self) -> RecordMode {
        self.mode
    }

    /// Returns the number of bytes written so far.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Returns the number of samples the recording lacks, as the devices
    /// reported them lost and as [`lagged`](Self::lagged) said they were
    /// dropped before the writer.
    pub fn samples_lost(&self) -> u64 {
        self.samples_lost
    }

    /// Tells the writer that packets meant for it were dropped before they
    /// reached it, as by a recorder whose queue had no room for them. The
    /// rows each stream lacks before its next rows then count as the
    /// recorder's.
    pub fn lagged(&mut self) {
        self.parser.note_dropped_packets();
    }
}

impl<W: Write> LogWriter<W> {
    /// Flushes the output.
    ///
    /// # Errors
    ///
    /// Returns the error of the output's flush.
    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    /// Takes one checked packet and writes what replay needs of it.
    ///
    /// A described recording scans the packet as replay will. It writes the
    /// samples the parser validated, the markers replay resets on, the
    /// proxy's counts of the samples it dropped, which replay tells those
    /// gaps by, and the heartbeats that name a session, which replay tells a
    /// reboot by. A sample or metadata
    /// packet first writes the metadata of its route if the file does not
    /// yet describe that revision. A raw recording writes the packet as it
    /// is.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidData`] if a metadata record does not
    /// fit a packet. Returns the error of the output if it refuses a write.
    pub fn packet<'a>(&mut self, packet: impl Into<Checked<'a>>) -> io::Result<()> {
        let packet = packet.into();
        let (bytes, route, payload) = (packet.as_bytes(), packet.route(), packet.payload());
        if let RecordMode::Raw = self.mode {
            return self.write(bytes);
        }
        let (validated, lost) = match self.parser.scan_packet(packet) {
            Ok(Some(rows)) => match rows.boundary().and_then(|b| b.loss.as_ref()) {
                Some(loss) => {
                    let (route, name) = (rows.stream_key().route, rows.schema().stream().name());
                    match loss.dropped_samples {
                        0 => log::warn!("{route}/{name} dropped {} samples", loss.samples),
                        behind => {
                            log::warn!(
                                "the recorder fell behind on {behind} samples of {route}/{name}"
                            )
                        }
                    }
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

        match payload {
            Payload::LegacyStatus(_)
            | Payload::ProxyRx(ProxyRx::Link(_) | ProxyRx::Dropped { .. }) => self.write(bytes),
            Payload::Samples(_) => {
                self.describe(route)?;
                if validated {
                    self.write(bytes)?;
                }
                Ok(())
            }
            Payload::Heartbeat(beat) => match beat.session() {
                Some(_) => self.write(bytes),
                None => Ok(()),
            },
            Payload::Metadata(..) => self.describe(route),
            Payload::ProxyTx(_)
            | Payload::ProxyRx(
                ProxyRx::Reply(_) | ProxyRx::Error(_) | ProxyRx::Alive | ProxyRx::Unknown(..),
            )
            | Payload::Log(_)
            | Payload::RpcRequest(_)
            | Payload::RpcReply(_)
            | Payload::RpcError(_)
            | Payload::Setting(_)
            | Payload::Unknown(..) => Ok(()),
        }
    }

    /// Takes a device's metadata as a value, and writes what replay needs of
    /// it. A described recording learns each record as the device's own
    /// broadcast of it, and after each writes the metadata of the route if
    /// the file does not yet describe that revision. A raw recording writes
    /// nothing, as the metadata is no packet it was handed.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidData`] if a metadata record does not
    /// fit a packet. Returns the error of the output if it refuses a write.
    pub fn metadata(&mut self, snapshot: &DeviceMetadataSnapshot) -> io::Result<()> {
        if let RecordMode::Raw = self.mode {
            return Ok(());
        }
        let route = snapshot.route();
        for record in snapshot.records() {
            self.parser.learn(route, record);
            self.describe(route)?;
        }
        Ok(())
    }

    /// Writes the parser's metadata for `route` if the file does not yet
    /// describe that revision.
    fn describe(&mut self, route: DeviceRoute) -> io::Result<()> {
        let Some((_, snapshot)) = self.parser.revised_metadata(route) else {
            return Ok(());
        };
        let packets = snapshot
            .metadata_packets(snapshot.route())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        for packet in packets {
            self.write(&packet)?;
        }
        Ok(())
    }

    fn write(&mut self, raw: &[u8]) -> io::Result<()> {
        self.writer.write_all(raw)?;
        self.bytes_written += raw.len() as u64;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::{checked, metadata_records, metadata_reply, packet};
    use crate::proto::data as wire;
    use crate::proto::packet::PacketType;
    use crate::proto::proxy::LinkStatus;
    use crate::proto::SegmentId;
    use crate::{Change, LogFile, SampleBatch};

    const STREAM: u8 = 1;

    fn writer() -> LogWriter<Vec<u8>> {
        LogWriter::new(Vec::new(), RecordMode::Described)
    }

    fn broadcast(record: wire::Metadata<'_>, route: DeviceRoute) -> Vec<u8> {
        packet::metadata(record, wire::MetadataFlags::UPDATE, route)
    }

    /// Hands the writer the broadcast a device in `session` describes itself
    /// with.
    fn describe(file: &mut LogWriter<Vec<u8>>, session: u32, route: DeviceRoute) {
        for record in metadata_records(session) {
            file.packet(checked(&broadcast(record, route)))
                .expect("the recording accepts a broadcast");
        }
    }

    fn samples_in(segment_id: u8, first: u32, route: DeviceRoute) -> Vec<u8> {
        packet::samples(STREAM, segment_id, first, &[0; 4], route)
    }

    fn samples(first: u32, route: DeviceRoute) -> Vec<u8> {
        samples_in(0, first, route)
    }

    /// The bytes of each packet the recording holds, in the order they were
    /// written.
    fn written(file: &LogWriter<Vec<u8>>) -> Vec<&[u8]> {
        let mut rest: &[u8] = &file.writer;
        std::iter::from_fn(|| {
            let (_, len) = Checked::prefix(rest).ok()?;
            let (packet, after) = rest.split_at(len);
            rest = after;
            Some(packet)
        })
        .collect()
    }

    /// The type of the packet `bytes` hold.
    fn ptype(bytes: &[u8]) -> PacketType {
        checked(bytes).header().ptype
    }

    /// Every batch the recording reads back as, in file order.
    fn recorded(file: &LogWriter<Vec<u8>>) -> Vec<SampleBatch> {
        LogFile::from_bytes(file.writer.clone())
            .scan(false)
            .batches(1)
            .map(|batch| batch.expect("the recorded log decodes"))
            .collect()
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
            (
                "mounted-link",
                packet::link(LinkStatus::SensorDisconnected, bounced),
            ),
            (
                "mounted-legacy",
                packet::legacy_status(LinkStatus::SensorDisconnected, bounced),
            ),
        ] {
            let mut file = writer();

            for route in [steady, bounced] {
                describe(&mut file, 1, route);
                file.packet(checked(&samples(0, route))).unwrap();
            }
            file.packet(checked(&marker)).unwrap();
            describe(&mut file, 1, bounced);
            for route in [steady, bounced] {
                file.packet(checked(&samples(1, route))).unwrap();
            }

            assert!(
                written(&file).iter().any(|packet| *packet == marker),
                "{name}: the marker is written as it arrived"
            );
            let runs: Vec<(DeviceRoute, bool, u32)> = recorded(&file)
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
        let mut file = writer();

        describe(&mut file, 1, route);
        let described = file.bytes_written;
        file.packet(checked(&samples_in(1, 8, route))).unwrap();

        assert_eq!(
            file.bytes_written, described,
            "a segment the file does not describe is not recorded under one it does"
        );
    }

    /// A segment update is described before the first sample of the segment it
    /// switched to, and only through the snapshot, never as the update itself.
    #[test]
    fn a_segment_update_is_described_before_the_samples_that_follow_it() {
        let route = DeviceRoute::root();
        let mut file = writer();

        describe(&mut file, 1, route);
        file.packet(checked(&samples_in(0, 0, route))).unwrap();
        let wire::Metadata::Segment(mut switched) = metadata_records(1)[2] else {
            panic!("the third record describes a segment");
        };
        switched.segment_id = SegmentId::new(1);
        file.packet(checked(&broadcast(
            wire::Metadata::Segment(switched),
            route,
        )))
        .unwrap();
        file.packet(checked(&samples_in(1, 8, route))).unwrap();

        let metadata = written(&file)
            .iter()
            .filter(|packet| ptype(packet) == PacketType::METADATA)
            .count();
        assert_eq!(metadata, 8, "one snapshot each, and no update written raw");
        let segments: Vec<(u8, u32)> = recorded(&file)
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

    /// A tap that lagged leaves a gap in the samples and nothing else. The
    /// file still decodes, and reports the loss the writer counted.
    #[test]
    fn a_gap_in_the_tap_is_recorded_as_lost_samples() {
        let route = DeviceRoute::root();
        let mut file = writer();

        describe(&mut file, 1, route);
        file.packet(checked(&samples(0, route))).unwrap();
        file.packet(checked(&samples(5, route))).unwrap();

        assert_eq!(file.samples_lost, 4, "the samples between the two packets");
        let recorded = recorded(&file);
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
        let mut file = writer();

        describe(&mut file, 1, route);
        file.packet(checked(&samples_in(0, 0, route))).unwrap();
        let wire::Metadata::Segment(mut next) = metadata_records(1)[2] else {
            panic!("the third record describes a segment");
        };
        (next.segment_id, next.start_time) = (SegmentId::new(1), 1);
        file.packet(checked(&broadcast(wire::Metadata::Segment(next), route)))
            .unwrap();
        file.packet(checked(&samples_in(1, 2, route))).unwrap();

        assert_eq!(
            file.samples_lost, 2,
            "sample 0 of the first segment ends at its rollover, and sample 2 follows two lost"
        );
    }

    /// A device that declares a revision marks the segment a start opens, and the recording keeps
    /// the mark, so replay reads the next acquisition as a restart and counts no sample lost.
    #[test]
    fn a_recorded_start_replays_as_a_restart() {
        let route = DeviceRoute::root();
        let mut file = writer();
        let [wire::Metadata::Device(device), stream, wire::Metadata::Segment(segment), column] =
            metadata_records(1)
        else {
            panic!("the records are a device's, a stream's, a segment's and a column's");
        };
        let declared = wire::Metadata::Device(wire::Device {
            protocol: 1,
            ..device
        });
        for record in [declared, stream, wire::Metadata::Segment(segment), column] {
            file.packet(checked(&broadcast(record, route))).unwrap();
        }
        file.packet(checked(&samples_in(0, 0, route))).unwrap();
        let next = wire::Segment {
            segment_id: SegmentId::new(1),
            start_time: 5,
            flags: segment.flags | wire::SegmentFlags::START,
            ..segment
        };
        file.packet(checked(&broadcast(wire::Metadata::Segment(next), route)))
            .unwrap();
        file.packet(checked(&samples_in(1, 0, route))).unwrap();

        assert_eq!(file.samples_lost, 0, "the pause holds no sample to lose");
        let recorded = recorded(&file);
        assert_eq!(recorded.len(), 2);
        assert_eq!(
            recorded[1].boundary().and_then(|b| b.change.clone()),
            Some(Change::Restarted {
                old_id: SegmentId::new(0),
                new_id: SegmentId::new(1),
            }),
            "the recorded start marks the restart on replay"
        );
    }

    /// After a reboot mid-recording, the samples of the new session wait for
    /// the metadata describing them, and replay splits the runs where it did.
    #[test]
    fn a_session_change_splits_the_recorded_runs() {
        let route = DeviceRoute::root();
        let mut file = writer();

        describe(&mut file, 1, route);
        file.packet(checked(&samples(0, route))).unwrap();
        file.packet(checked(&packet::heartbeat_session(2, route)))
            .unwrap();
        let announced = file.bytes_written;
        file.packet(checked(&samples(1, route))).unwrap();
        assert_eq!(
            file.bytes_written, announced,
            "nothing describes the new session yet"
        );
        describe(&mut file, 2, route);
        file.packet(checked(&samples(2, route))).unwrap();

        let recorded = recorded(&file);
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
        let mut file = writer();

        file.packet(checked(&broadcast(metadata_records(1)[0], route)))
            .unwrap();
        let stale = file
            .parser
            .take_metadata_queries_for(route)
            .pop()
            .expect("the route still wants its stream");
        file.packet(checked(&packet::heartbeat_session(2, route)))
            .unwrap();
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
        file.packet(checked(&samples(0, route))).unwrap();

        let sessions: Vec<u32> = recorded(&file)
            .iter()
            .map(|batch| batch.metadata().device().session().value())
            .collect();
        assert_eq!(sessions, [2], "only the session that answered is described");
    }

    /// A raw recording is the tap itself, whatever the parser makes of it.
    #[test]
    fn a_raw_recording_holds_every_tapped_packet() {
        let route = DeviceRoute::root();
        let mut file = LogWriter::new(Vec::new(), RecordMode::Raw);

        let tapped = [samples(0, route), packet::heartbeat(route)];
        for packet in &tapped {
            file.packet(checked(packet)).unwrap();
        }

        let expected: Vec<u8> = tapped.concat();
        assert_eq!(
            file.writer, expected,
            "an undescribed sample and a bare heartbeat are recorded all the same"
        );
    }
}
