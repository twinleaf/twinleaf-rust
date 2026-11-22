//! Ordered metadata, schema, and continuity state for routed TIO packets.
//!
//! This module validates stream payloads and describes their rows without
//! materializing sample values. Packet consumers decide whether to decode the
//! validated bytes immediately or retain only their metadata and boundaries.

use crate::decode::body::{decode_samples, decodes};
use crate::model::continuity::{Boundary, Change, Generations, Loss, LossSite};
use crate::model::sample::{BatchContext, RowSource, SampleBatchBuilder, SampleClock};
use crate::model::schema::{
    ColumnRecord, DeviceMetadataSnapshot, DeviceRecord, MetadataQuery, Record, SegmentRecord,
    StreamKey, StreamMetadataSnapshot, StreamRecord, StreamSchema,
};
use crate::proto::data as wire;
use crate::proto::data::MAX_SAMPLE_NUMBER;
use crate::proto::heartbeat::Heartbeat;
use crate::proto::packet::Payload;
use crate::proto::DeviceRoute;
use crate::proto::{SampleNumber, SegmentId, SessionId, StreamId};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Why a well-formed packet cannot be applied to the data state.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DataError {
    /// A stream-data payload contradicts the metadata retained for its stream.
    ///
    /// The rows are dropped, but the retained metadata survives for later
    /// packets.
    #[error("invalid data for {route} stream {stream_id}: {source}")]
    Stream {
        /// Absolute route of the device that sent the data.
        route: DeviceRoute,
        /// Stream the payload belongs to, within that device.
        stream_id: StreamId,
        /// Which part of the payload or schema was contradictory.
        #[source]
        source: StreamDataError,
    },
}

/// A stream-data payload contradicts the metadata needed to interpret it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum StreamDataError {
    /// The stream record advertises an empty segment ring, so no segment id is
    /// valid.
    #[error("stream advertises a zero-length segment ring")]
    ZeroSegmentCount,
    /// The data packet names a segment the stream record cannot describe.
    #[error("segment {segment_id} is outside the advertised ring of {segment_count}")]
    SegmentOutOfRange {
        /// Segment id carried by the data packet.
        segment_id: SegmentId,
        /// Ring size from the stream record. Valid ids fall below it.
        segment_count: u8,
    },
    /// More column records are retained than the stream record now admits.
    ///
    /// Too few is not an error. The parser waits for the rest to arrive.
    #[error("received {actual} columns for a schema containing {expected}")]
    ColumnCount {
        /// Column count the stream record advertises.
        expected: usize,
        /// Column records retained for the stream.
        actual: usize,
    },
    /// The packed column types do not fit the stream's declared sample stride.
    ///
    /// A stride with room to spare is accepted. Only overflow is rejected.
    #[error("column data occupies {column_bytes} bytes, exceeding sample size {sample_size}")]
    ColumnsExceedSample {
        /// Packed size of every column type in the schema.
        column_bytes: usize,
        /// Per-sample stride the stream record declares.
        sample_size: usize,
    },
    /// The stream record's stride leaves the payload's row count undefined.
    #[error("stream advertises a zero-byte sample")]
    ZeroSampleSize,
    /// The payload ends mid-sample, as in a truncated or corrupt packet.
    #[error("payload length {payload_len} is not a multiple of sample size {sample_size}")]
    MisalignedPayload {
        /// Byte length of the encoded sample payload.
        payload_len: usize,
        /// Stride the payload length must be a multiple of.
        sample_size: usize,
    },
    /// The sender emitted a data packet carrying no rows to apply.
    #[error("stream-data payload contains no rows")]
    EmptyPayload,
    /// The packet's rows run past the 24-bit wrap of the sample counter.
    #[error("{row_count} rows beginning at sample {first_sample_n} exceed the sample counter")]
    SampleNumberOverflow {
        /// Sample number of the packet's first row.
        first_sample_n: SampleNumber,
        /// Rows the payload decodes to, starting at `first_sample_n`.
        row_count: usize,
    },
    /// The segment's timebase is undefined, so no row can be timestamped.
    #[error("segment has invalid sampling rate {sampling_rate} and decimation {decimation}")]
    InvalidRate {
        /// Samples per second before decimation, from the segment record.
        sampling_rate: u32,
        /// Divisor on the sampling rate. The two give the output rate.
        decimation: u32,
    },
}

/// What a stream's parser knows of the segment slot its rows are expected
/// in. A device sends a slot's record before the slot's first row, so the
/// parser keeps the record of that one slot.
#[derive(Debug, Clone)]
enum Slot {
    /// The parser holds no record of slot `slot`, and asks for one.
    Unknown { slot: SegmentId },
    /// The device said it holds nothing in the slot. The record is kept for
    /// the snapshot, and rows of the slot wait for a populated record.
    Unpopulated(SegmentRecord),
    /// A populated record of the segment in the slot. Rows of the slot
    /// decode with it.
    Held(SegmentRecord),
}

impl Slot {
    /// The slot `record` fills, by whether it is populated.
    fn of(record: SegmentRecord) -> Self {
        if record.get().is_populated() {
            Slot::Held(record)
        } else {
            Slot::Unpopulated(record)
        }
    }

    /// The number of the slot.
    fn id(&self) -> SegmentId {
        match self {
            Slot::Unknown { slot } => *slot,
            Slot::Unpopulated(record) | Slot::Held(record) => record.get().segment_id(),
        }
    }

    /// Returns the populated record the slot holds.
    fn populated(&self) -> Option<&SegmentRecord> {
        match self {
            Slot::Held(record) => Some(record),
            Slot::Unknown { .. } | Slot::Unpopulated(_) => None,
        }
    }

    /// Returns the record the device sent for the slot, populated or not.
    fn record(&self) -> Option<&SegmentRecord> {
        match self {
            Slot::Held(record) | Slot::Unpopulated(record) => Some(record),
            Slot::Unknown { .. } => None,
        }
    }
}

/// The records that describe a stream, as far as they have arrived.
#[derive(Debug, Clone, Default)]
struct Described {
    stream: Option<StreamRecord>,
    /// The column records from index 0 up to the first one missing.
    columns: Vec<ColumnRecord>,
    /// Built from the stream record and the columns once every column has
    /// arrived, and dropped whenever either changes.
    schema: Option<Arc<StreamSchema>>,
}

/// A stream's last accepted rows, which the next rows are told against.
#[derive(Debug, Clone)]
struct Last {
    /// The record of the segment the rows were in, whose start and clock
    /// place the rows a rollover lost.
    record: SegmentRecord,
    /// The number of the last row.
    sample_number: SampleNumber,
    /// The device session the rows came in.
    session: SessionId,
    /// The layout of the rows. Rows in another layout in the same session
    /// start a new run, as nothing ties them to the rows before.
    schema: Arc<StreamSchema>,
    since: Since,
}

impl Last {
    /// The output rate of the rows in hertz.
    fn rate_hz(&self) -> f64 {
        self.record
            .get()
            .rate_hz()
            .expect("rows are accepted only under a segment with a rate")
    }

    /// The time at the end of the last row, in seconds.
    fn timestamp(&self) -> f64 {
        SampleClock::of(self.record.get()).end_of(self.sample_number)
    }
}

/// What the parser was told of a stream since its last accepted rows.
#[derive(Debug, Clone, Copy, Default)]
struct Since {
    /// Whether the device sent a record of the last rows' slot that may
    /// open a new acquisition. Such a record is of another segment than the
    /// last rows', or came after the stream's records were dropped to be
    /// learned again. The last rows' own record sent again leaves the mark
    /// as it is.
    new_record: bool,
    /// Whether the stream's records were dropped to be learned again, as
    /// after its link was lost. Rows of another segment then may be of a
    /// restart whose stop went unseen.
    lost_track: bool,
    /// Whether the device broadcast a closing record, or sent the record a
    /// start opened of a segment other than the last rows'.
    stop_or_start: bool,
    /// Where the rows lost before the stream's next rows were lost, as far
    /// as the parser was told.
    blamed: LossSite,
}

/// Metadata and continuity state for one stream on one device.
#[derive(Debug, Clone)]
struct StreamTracker {
    key: StreamKey,
    described: Described,
    /// The slot rows are expected in, and what is known of it. It is the
    /// slot rows or a broadcast populated record named last, or one a
    /// bootstrap reply or an unpopulated broadcast named while it was
    /// unknown.
    slot: Slot,
    /// The last accepted rows. They survive metadata being dropped, so the
    /// first rows under new metadata are told against them.
    last: Option<Last>,
    /// The number of runs the stream's rows have begun, which numbers its
    /// runs. Its first rows begin one, as each break after them does.
    breaks: u32,
}

impl StreamTracker {
    fn new(key: StreamKey) -> Self {
        Self {
            key,
            described: Described::default(),
            slot: Slot::Unknown {
                slot: SegmentId::new(0),
            },
            last: None,
            breaks: 0,
        }
    }

    /// Blames `site` for the rows lost before the stream's next rows, unless
    /// a place that explains more is blamed already.
    fn blame(&mut self, site: LossSite) {
        if let Some(last) = &mut self.last {
            last.since.blamed = last.since.blamed.max(site);
        }
    }

    /// Whether `segment` is the segment of the last accepted rows.
    fn is_last(&self, segment: wire::SegmentView<'_>) -> bool {
        self.last
            .as_ref()
            .is_some_and(|last| last.record.get().is_same_segment(&segment))
    }

    /// Learns the stream record of a stream that had none.
    fn learn_stream(&mut self, stream: StreamRecord) {
        self.described.stream = Some(stream);
        self.update_schema();
    }

    /// Appends the column record whose index is the number of columns known.
    fn append_column(&mut self, column: ColumnRecord) {
        self.described.columns.push(column);
        self.update_schema();
    }

    /// Builds the schema once the stream record and all of its columns are
    /// known, and drops it otherwise.
    fn update_schema(&mut self) {
        let described = &mut self.described;
        described.schema = match &described.stream {
            Some(stream) if described.columns.len() == usize::from(stream.get().n_columns()) => {
                Some(Arc::new(StreamSchema::new(
                    self.key,
                    stream.clone(),
                    described.columns.clone(),
                )))
            }
            _ => None,
        };
    }

    /// Returns selectors for the records this stream lacks for a complete
    /// snapshot. A slot the device said holds nothing lacks no record,
    /// though no rows decode in it.
    fn missing_metadata(&self) -> Vec<wire::MetadataSelector> {
        let stream_id = self.key.stream_id.value();
        let mut missing = Vec::new();
        match self.described.stream.as_ref() {
            Some(stream) => {
                for index in self.described.columns.len()..usize::from(stream.get().n_columns()) {
                    missing.push(wire::MetadataSelector::column(stream_id, index as u8))
                }
            }
            None => {
                missing.push(wire::MetadataSelector::stream(stream_id));
            }
        }
        if let Slot::Unknown { slot } = self.slot {
            missing.push(wire::MetadataSelector::segment(stream_id, slot.value()));
        }
        missing
    }

    /// Takes a segment record. A populated record of the expected slot
    /// fills it while it holds no populated record. While it holds one, a
    /// broadcast replaces it, and a reply, which can be older, is dropped.
    /// An unpopulated record of the expected slot fills it only while the
    /// slot is unknown. A record of another slot moves the expected slot
    /// when it is a broadcast populated record, or a bootstrap record or an
    /// unpopulated broadcast while the slot is unknown. Any other record is
    /// dropped whole. Returns whether a complete snapshot may have changed.
    fn take_segment(&mut self, incoming: SegmentRecord, source: MetadataSource) -> bool {
        let segment_id = incoming.get().segment_id();
        let unknown = matches!(self.slot, Slot::Unknown { .. });
        if segment_id != self.slot.id() {
            let moves = match (source, incoming.get().is_populated()) {
                (MetadataSource::Broadcast, true) => true,
                (MetadataSource::Broadcast | MetadataSource::Bootstrap, _) => unknown,
                (MetadataSource::Reply, _) => false,
            };
            if !moves {
                return false;
            }
            self.mark(incoming.get(), source);
            self.slot = Slot::of(incoming);
            return true;
        }
        let held = matches!(self.slot, Slot::Held(_));
        match Slot::of(incoming) {
            Slot::Held(_) if held && source != MetadataSource::Broadcast => false,
            Slot::Held(incoming) => {
                self.mark(incoming.get(), source);
                let learned = self.slot.populated() != Some(&incoming);
                self.slot = Slot::Held(incoming);
                learned
            }
            unpopulated => {
                // An unpopulated record only fills a slot with no record.
                if unknown {
                    self.slot = unpopulated;
                }
                unknown
            }
        }
    }

    /// Marks what a segment record from `source` that the stream takes says
    /// of the rows after the last rows. An unpopulated record neither
    /// closes nor opens a segment.
    fn mark(&mut self, segment: wire::SegmentView<'_>, source: MetadataSource) {
        if !segment.is_populated() {
            return;
        }
        let is_last = self.is_last(segment);
        let Some(last) = &mut self.last else {
            return;
        };
        let since = &mut last.since;
        let closes = source == MetadataSource::Broadcast
            && !segment.flags().contains(wire::SegmentFlags::ACTIVE);
        since.stop_or_start |=
            closes || segment.flags().contains(wire::SegmentFlags::START) && !is_last;
        let in_last_slot = last.record.get().segment_id() == segment.segment_id();
        since.new_record |= in_last_slot && (!is_last || since.lost_track);
    }

    fn detect_boundary(
        &self,
        first_sample_n: SampleNumber,
        first_timestamp: f64,
        device: wire::DeviceView<'_>,
        segment: wire::SegmentView<'_>,
        new_rate: f64,
    ) -> Option<Boundary> {
        let Some(rows) = self.last.as_ref() else {
            return Some(Boundary::of(Change::Initial));
        };
        let last = rows.record.get();

        let declared = device.marks_starts_and_stops();
        if !declared && device.session() != rows.session {
            return Some(Boundary::of(Change::SessionChanged {
                old: rows.session,
                new: device.session(),
            }));
        }

        // A new session may bring a new layout. In the same session, rows in
        // another layout are of another stream.
        if self.described.schema.as_ref() != Some(&rows.schema) {
            return Some(Boundary::of(Change::Initial));
        }

        if segment.time_reference() != last.time_reference() {
            return Some(Boundary::of(Change::TimeRefSessionChanged {
                old: last.timeref_session(),
                new: segment.timeref_session(),
            }));
        }

        if (new_rate - rows.rate_hz()).abs() > 1e-9 {
            return Some(Boundary::of(Change::RateChanged {
                old_rate: rows.rate_hz(),
                new_rate,
            }));
        }

        let half_period = 0.5 / new_rate;
        let time_gap = first_timestamp - rows.timestamp();

        if time_gap < -half_period {
            return Some(Boundary::of(Change::TimeBackward {
                gap_seconds: -time_gap,
            }));
        }

        let expected = SampleNumber::new(rows.sample_number.value() + 1);
        let lost = |samples: u64| Loss::at(rows.since.blamed, samples, Some(expected));

        // Rows in the last rows' slot are of a later lap of the ring when
        // that slot's segment starts at another time. On one time reference a
        // device never moves a segment's start, and never starts two
        // acquisitions of one slot in the same second.
        // Rows are of the current segment. A device that declares a revision marks the segment a
        // start opens and closes the one a stop ends, so the time between holds no sample to
        // lose. Rows in another segment after the records were dropped may be of a restart that
        // went unseen, whose time cannot be told from time lost.
        let opened = !self.is_last(segment);
        let started =
            segment.flags().contains(wire::SegmentFlags::START) || rows.since.stop_or_start;
        if opened && (rows.since.lost_track || declared && started) {
            return Some(Boundary::of(Change::Restarted {
                old_id: last.segment_id(),
                new_id: segment.segment_id(),
            }));
        }
        if opened {
            let missing = self.missing_before(segment, first_sample_n);
            return Some(Boundary {
                change: Some(Change::SegmentRollover {
                    old_id: last.segment_id(),
                    new_id: segment.segment_id(),
                }),
                loss: u64::try_from(missing).ok().filter(|&n| n >= 1).map(lost),
            });
        }

        if first_sample_n != expected {
            // Time runs with the sample numbers in one segment, and a
            // backward jump returned above, so the rows came after a loss.
            if time_gap.abs() > half_period {
                let samples = first_sample_n.value() - expected.value();
                return Some(Boundary {
                    change: None,
                    loss: Some(lost(u64::from(samples))),
                });
            }
        }

        None
    }

    /// Returns how many rows were missed between the last accepted row and
    /// row `first` of `segment`, a later segment at the same rate.
    ///
    /// A device switches segments on a whole second of its undecimated
    /// clock, and drops the group of inputs that switch leaves incomplete.
    /// So the whole output periods from the last segment's start to the new
    /// one's are the rows the last segment held, and a split group leaves no
    /// gap of a whole period. The count is below 1 when no row was missed.
    ///
    /// The count rests on law `S6` of the contract in `twinleaf_proto::contract`, which says
    /// how far a segment's rows number before the next segment on the same time reference. The
    /// parser reaches this count only for a segment on the last rows' time reference.
    fn missing_before(&self, segment: wire::SegmentView<'_>, first: SampleNumber) -> i128 {
        let Some(rows) = self.last.as_ref() else {
            return 0;
        };
        let last = rows.record.get();
        let elapsed = i128::from(segment.start_time()) - i128::from(last.start_time());
        let held =
            (elapsed * i128::from(last.sampling_rate())).div_euclid(i128::from(last.decimation()));
        held + i128::from(first.value()) - i128::from(rows.sample_number.value()) - 1
    }

    /// Validates one encoded payload and updates continuity state.
    ///
    /// Missing metadata yields `PacketEvent::WaitingForMetadata`. A payload that
    /// contradicts complete metadata is an error.
    fn validate_rows<'a>(
        &'a mut self,
        data: wire::Samples<'a>,
        device_metadata: DeviceRecord,
        device_generation: &mut u32,
        global_generation: &mut u32,
        metadata_revision: &mut u32,
    ) -> Result<PacketEvent<'a>, StreamDataError> {
        let Some(stream_metadata) = self.described.stream.clone() else {
            return Ok(PacketEvent::WaitingForMetadata);
        };
        let stream = stream_metadata.get();
        let n_segments = stream.n_segments();
        let device = device_metadata.get();

        // Segment ids index a fixed ring, so a wire value outside it is corrupt.
        if n_segments == 0 {
            return Err(StreamDataError::ZeroSegmentCount);
        }
        if data.segment_id.value() >= n_segments {
            return Err(StreamDataError::SegmentOutOfRange {
                segment_id: data.segment_id,
                segment_count: n_segments,
            });
        }
        // The expected slot is the one rows or a broadcast populated record
        // named last, or one a bootstrap reply or an unpopulated broadcast
        // named while it was unknown. So a segment other than the last rows'
        // is announced when the device broadcast its record since them.
        // A segment's sample numbers only go up, so rows numbered at or
        // below the last rows, in their segment, while the device has sent
        // no record of that slot since them that renews it, are of a new
        // acquisition in the slot whose record has not come. A device sends
        // a slot's record before its first row, so such rows mean the parser
        // missed that record, after a reboot or a full lap of the ring.
        let behind = self.last.as_ref().is_some_and(|last| {
            !last.since.new_record
                && last.record.get().segment_id() == data.segment_id
                && data.first.value() <= last.sample_number.value()
        });
        // Rows in a slot the device said holds nothing are of a segment whose
        // record has not come, as rows of a slot the device did not announce
        // are.
        let unpopulated = matches!(self.slot, Slot::Unpopulated(_));
        let announced = self.slot.id() == data.segment_id && !behind && !unpopulated;
        // A device sends a segment's record before the segment's first
        // sample, so the record of the segment rows arrive in is announced.
        // Rows in a segment the device did not announce drop the record the
        // parser holds. The rows wait for the segment's record, and name
        // the segment expected, so the record a metadata reply brings for it
        // stands.
        if !announced {
            self.slot = Slot::Unknown {
                slot: data.segment_id,
            };
            *metadata_revision = metadata_revision.wrapping_add(1);
        }

        let n_columns = usize::from(stream.n_columns());
        match self.described.columns.len().cmp(&n_columns) {
            Ordering::Less => return Ok(PacketEvent::WaitingForMetadata),
            Ordering::Greater => {
                return Err(StreamDataError::ColumnCount {
                    expected: n_columns,
                    actual: self.described.columns.len(),
                });
            }
            Ordering::Equal => {}
        }
        // A stream with a column this build cannot decode is refused whole,
        // so every batch has one array per column of its stream.
        if !self
            .described
            .columns
            .iter()
            .all(|column| decodes(column.get().data_type()))
        {
            return Ok(PacketEvent::Unsupported);
        }

        let sample_size = usize::from(stream.sample_size());
        let expected_sample_size: usize = self
            .described
            .columns
            .iter()
            .map(|column| column.get().data_type().size())
            .sum();
        if expected_sample_size > sample_size {
            return Err(StreamDataError::ColumnsExceedSample {
                column_bytes: expected_sample_size,
                sample_size,
            });
        }
        if sample_size == 0 {
            return Err(StreamDataError::ZeroSampleSize);
        }
        if !data.data.len().is_multiple_of(sample_size) {
            return Err(StreamDataError::MisalignedPayload {
                payload_len: data.data.len(),
                sample_size,
            });
        }

        let row_count = data.data.len() / sample_size;
        if row_count == 0 {
            return Err(StreamDataError::EmptyPayload);
        }
        let sample_span =
            u32::try_from(row_count - 1).map_err(|_| StreamDataError::SampleNumberOverflow {
                first_sample_n: data.first,
                row_count,
            })?;
        let last_sample_n = SampleNumber::new(data.first.value().checked_add(sample_span).ok_or(
            StreamDataError::SampleNumberOverflow {
                first_sample_n: data.first,
                row_count,
            },
        )?);
        if last_sample_n.value() > MAX_SAMPLE_NUMBER {
            return Err(StreamDataError::SampleNumberOverflow {
                first_sample_n: data.first,
                row_count,
            });
        }

        // Rows decode only with their own segment's record. Rows without it
        // are lost, and the first batch once it is known reports them.
        let Some(segment_record) = self.slot.populated().cloned() else {
            return Ok(PacketEvent::WaitingForMetadata);
        };
        let segment = segment_record.get();

        let Some(new_rate) = segment.rate_hz() else {
            return Err(StreamDataError::InvalidRate {
                sampling_rate: segment.sampling_rate(),
                decimation: segment.decimation(),
            });
        };

        let boundary = self.detect_boundary(
            data.first,
            SampleClock::of(segment).begin_of(data.first),
            device,
            segment,
            new_rate,
        );

        if let Some(boundary) = boundary.as_ref().filter(|b| !b.is_continuous()) {
            self.breaks += 1;
            // Streams start up independently, so their first data must not bump
            // the generations their peers share. A stream that starts over
            // bumps them as any other break does.
            if !boundary.is_initial() || self.last.is_some() {
                *device_generation += 1;
                *global_generation += 1;
            }
        }

        let schema = self
            .described
            .schema
            .as_ref()
            .expect("a stream with every column has a schema");
        debug_assert!(schema.describes(&stream_metadata, &self.described.columns));
        self.last = Some(Last {
            record: segment_record.clone(),
            sample_number: last_sample_n,
            session: device.session(),
            schema: schema.clone(),
            since: Since::default(),
        });
        Ok(PacketEvent::Rows(ScannedRows {
            schema,
            device: device_metadata,
            segment: segment_record,
            boundary,
            generations: Generations {
                stream: self.breaks,
                device: *device_generation,
                global: *global_generation,
            },
            first_sample_n: data.first,
            last_sample_n,
            row_count,
            encoded: data.data,
        }))
    }

    /// Drops the stream's records to learn them again. Its expected slot
    /// keeps its number. Its last rows stay, so the rows under the new
    /// records are told against them.
    fn relearn(&mut self) {
        self.described = Described::default();
        self.slot = Slot::Unknown {
            slot: self.slot.id(),
        };
        if let Some(last) = &mut self.last {
            last.since.lost_track = true;
        }
    }

    /// Forgets metadata and ends the current run. The expected slot keeps
    /// its number. The break counter is kept, and the rebuilt stream numbers
    /// its runs after it.
    fn reset(&mut self) {
        self.relearn();
        self.last = None;
    }

    fn metadata_snapshot(&self, device: DeviceRecord) -> Option<StreamMetadataSnapshot> {
        Some(StreamMetadataSnapshot::new(
            self.described.schema.clone()?,
            device,
            self.slot.record()?.clone(),
        ))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum MetadataSource {
    /// A reply to a query that names its records may describe an older ring
    /// segment. A record it holds never moves the expected slot.
    Reply,
    /// Broadcasts describe the current output segment.
    Broadcast,
    /// A bootstrap reply, to a query that names no records, holds the
    /// records the device chose, its current segments among them. It moves
    /// the expected slot only while the parser holds no record of it, as a
    /// broadcast since the query names a newer segment.
    Bootstrap,
}

/// Why a route logs a metadata record.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Logged {
    /// The record does not parse as the type it claims, and is dropped.
    Unparsed,
    /// The record names a stream the device record does not count, and is
    /// dropped.
    AboveCount,
    /// The record is kept, and a field's text holds U+FFFD.
    Replacement,
}

/// The names of the `fields` whose text holds U+FFFD.
fn replacements(fields: &[(&'static str, &str)]) -> Vec<&'static str> {
    fields
        .iter()
        .filter(|(_, text)| text.contains(char::REPLACEMENT_CHARACTER))
        .map(|&(field, _)| field)
        .collect()
}

/// A metadata record a route logs, by its type, the stream and segment or
/// column it names, and why.
type LoggedKey = (wire::MetadataType, Option<StreamId>, Option<u8>, Logged);

/// Metadata and stream state for one absolute device route.
#[derive(Clone)]
struct DeviceState {
    route: DeviceRoute,
    metadata: Option<DeviceRecord>,
    /// The record held when the metadata was last dropped to be learned
    /// again, which the next record is told against until it arrives.
    previous: Option<DeviceRecord>,
    streams: Vec<Option<StreamTracker>>,
    ignore_session: bool,
    query_in_flight: bool,
    metadata_generation: u32,
    /// Bumps whenever an applied record may change what a complete snapshot of
    /// this route would contain.
    metadata_revision: u32,
    /// The revision last handed out, until the route is forgotten.
    handed_out: Option<u32>,
    generation: u32,
    /// Each record logged as a warning since the metadata was last reset. A
    /// record logged again for the same reason logs at debug level.
    logged: Vec<LoggedKey>,
}

impl DeviceState {
    fn new(route: DeviceRoute, ignore_session: bool) -> Self {
        Self {
            route,
            metadata: None,
            previous: None,
            streams: Vec::new(),
            ignore_session,
            query_in_flight: false,
            metadata_generation: 0,
            metadata_revision: 0,
            handed_out: None,
            generation: 0,
            logged: Vec::new(),
        }
    }

    fn revise_metadata(&mut self) {
        self.metadata_revision = self.metadata_revision.wrapping_add(1);
    }

    fn stream_mut(&mut self, stream_id: StreamId) -> &mut StreamTracker {
        let index = usize::from(stream_id.value());
        if self.streams.len() <= index {
            self.streams.resize_with(index + 1, || None);
        }
        let key = StreamKey::new(self.route, stream_id);
        self.streams[index].get_or_insert_with(|| StreamTracker::new(key))
    }

    /// Bootstraps this route again after metadata contradicted the state used
    /// to decode earlier rows. Each stream keeps its break counter, and the
    /// global generation bumps.
    fn forget_all_metadata(&mut self, global_generation: &mut u32) {
        self.reset();
        *global_generation += 1;
    }

    /// Resets every stream as [`StreamTracker::reset`] does, and opens a new
    /// device generation.
    fn reset(&mut self) {
        self.metadata = None;
        self.previous = None;
        self.logged.clear();
        self.rediscover_metadata();
        self.generation += 1;
        for stream in self.streams.iter_mut().flatten() {
            stream.reset();
        }
    }

    /// Drops the device's metadata to learn it again, as after its link was
    /// lost or it reported a new session. Each stream keeps its last rows,
    /// which the rows under the new metadata are told against. The next
    /// device record is told against the one held now, and one with another
    /// serial resets every stream.
    fn relearn_metadata(&mut self) {
        if let Some(held) = self.metadata.take() {
            self.previous = Some(held);
        }
        for stream in self.streams.iter_mut().flatten() {
            stream.relearn();
        }
        self.rediscover_metadata();
    }

    /// Opens a new metadata generation and arms discovery again. Queries
    /// minted before this point no longer apply.
    fn rediscover_metadata(&mut self) {
        self.metadata_generation = self.metadata_generation.wrapping_add(1);
        self.query_in_flight = false;
    }

    /// Whether the device record counts `stream_id`. A record or rows of a
    /// stream above the count are refused, and the rest is kept.
    fn accepts_stream(&self, stream_id: StreamId) -> bool {
        !self
            .metadata
            .as_ref()
            .is_some_and(|device| stream_id.value() > device.get().n_streams())
    }

    /// Logs `message` about the metadata record `key` names, after the
    /// route. The first time since the last reset is a warning, and the same
    /// key again is logged at debug level.
    fn log_once(&mut self, key: LoggedKey, message: std::fmt::Arguments<'_>) {
        let level = match self.logged.contains(&key) {
            true => log::Level::Debug,
            false => {
                self.logged.push(key);
                log::Level::Warn
            }
        };
        log::log!(level, "{} {message}", self.route);
    }

    /// Logs a metadata record this route drops, and why, once per kind,
    /// stream and reason.
    fn log_dropped(
        &mut self,
        kind: wire::MetadataType,
        stream_id: Option<StreamId>,
        reason: Logged,
        why: std::fmt::Arguments<'_>,
    ) {
        self.log_once(
            (kind, stream_id, None, reason),
            format_args!("dropped a {kind:?} metadata record {why}"),
        );
    }

    /// Logs a record that does not parse.
    fn log_unparsed(&mut self, kind: wire::MetadataType) {
        self.log_dropped(
            kind,
            None,
            Logged::Unparsed,
            format_args!("as it does not parse"),
        );
    }

    /// Logs a kept record whose `marked` fields hold U+FFFD, naming them,
    /// once per record. `stream_id` and `index` name the record, with
    /// `index` its segment or column. Nothing is logged when no field is
    /// marked.
    fn log_replacement(
        &mut self,
        kind: wire::MetadataType,
        stream_id: Option<StreamId>,
        index: Option<(&str, u8)>,
        marked: &[&str],
    ) {
        if marked.is_empty() {
            return;
        }
        let stream = stream_id.map_or_else(String::new, |id| format!(" of stream {id}"));
        let index_name = index.map_or_else(String::new, |(name, value)| format!(" {name} {value}"));
        self.log_once(
            (kind, stream_id, index.map(|(_, value)| value), Logged::Replacement),
            format_args!(
                "kept a {kind:?} metadata record{stream}{index_name} with U+FFFD in its {}, the mark of text that could not be read as UTF-8",
                marked.join(" and ")
            ),
        );
    }

    /// Logs a record of a stream the device record does not count.
    fn log_dropped_above(&mut self, kind: wire::MetadataType, stream_id: StreamId) {
        let counted = self
            .metadata
            .as_ref()
            .map_or(0, |device| device.get().n_streams());
        self.log_dropped(
            kind,
            Some(stream_id),
            Logged::AboveCount,
            format_args!("of stream {stream_id}, above the {counted} streams its device counts"),
        );
    }

    /// Merges one metadata record of `kind` into this route's decoding state,
    /// as [`learn_metadata`](Self::learn_metadata) does. A record that does
    /// not parse as the type it claims is dropped and logged. A record of a
    /// kind this build does not know is dropped.
    fn apply_metadata(
        &mut self,
        kind: wire::MetadataType,
        record: &[u8],
        source: MetadataSource,
        global_generation: &mut u32,
    ) {
        if let wire::MetadataType::Unknown(_) = kind {
            return;
        }
        match Record::new(kind, record) {
            Some(record) => self.learn_metadata(record, source, global_generation),
            None => self.log_unparsed(kind),
        }
    }

    /// Merges one retained metadata record into this route's decoding state.
    /// A record of a stream the device record does not count, or a column
    /// past the next one its stream can append, is dropped and logged. Only
    /// the column waits at debug level, as it is asked for again in its turn.
    /// A record that is kept with a field whose text holds U+FFFD is logged
    /// once.
    fn learn_metadata(
        &mut self,
        record: Record,
        source: MetadataSource,
        global_generation: &mut u32,
    ) {
        match record {
            Record::Device(incoming) => {
                let kind = wire::MetadataType::Device;
                let device = incoming.get();
                let marked = replacements(&[
                    ("name", device.name()),
                    ("serial", device.serial()),
                    ("firmware", device.firmware()),
                ]);
                if self.metadata.as_ref() != Some(&incoming) {
                    self.revise_metadata();
                }
                // A record in force is replaced. A record from before the
                // metadata was dropped tells only whether the device is the
                // same one, as the rest is already being learned again.
                let held = self.metadata.take();
                let in_force = held.is_some();
                if let Some(previous) = held.or_else(|| self.previous.take()) {
                    let (old, new) = (previous.get(), incoming.get());
                    if old.serial() != new.serial() {
                        self.forget_all_metadata(global_generation);
                    } else if old.session() != new.session() && !new.marks_starts_and_stops() {
                        // Keeping continuity state lets the first rows under the
                        // new metadata report a specific SessionChanged boundary.
                        if in_force {
                            self.relearn_metadata();
                        }
                    } else if old.firmware() != new.firmware() {
                        // A firmware change can alter a schema without changing
                        // the session or timeline, so it must explicitly end all
                        // current runs rather than relying on boundary detection.
                        self.forget_all_metadata(global_generation);
                    }
                }
                self.metadata = Some(incoming);
                self.log_replacement(kind, None, None, &marked);
            }
            Record::Stream(incoming) => {
                let kind = wire::MetadataType::Stream;
                let stream_id = incoming.get().stream_id();
                let marked = replacements(&[("name", incoming.get().name())]);
                if !self.accepts_stream(stream_id) {
                    self.log_dropped_above(kind, stream_id);
                    return;
                }
                let stream = self.stream_mut(stream_id);
                let (contradicted, learned) = match &stream.described.stream {
                    Some(current) => (*current != incoming, false),
                    None => {
                        stream.learn_stream(incoming);
                        (false, true)
                    }
                };
                if contradicted {
                    // This should never happen: stream metadata is constant.
                    self.forget_all_metadata(global_generation);
                    return;
                } else if learned {
                    self.revise_metadata();
                }
                self.log_replacement(kind, Some(stream_id), None, &marked);
            }
            Record::Segment(incoming) => {
                let kind = wire::MetadataType::Segment;
                let segment = incoming.get();
                let (stream_id, segment_id) = (segment.stream_id(), segment.segment_id());
                let marked = replacements(&[("timeref serial", segment.timeref_serial())]);
                if !self.accepts_stream(stream_id) {
                    self.log_dropped_above(kind, stream_id);
                    return;
                }
                let stream = self.stream_mut(stream_id);
                let taken = stream.take_segment(incoming.clone(), source);
                let retained = stream.slot.record() == Some(&incoming);
                if taken {
                    self.revise_metadata();
                }
                if retained {
                    let index = Some(("segment", segment_id.value()));
                    self.log_replacement(kind, Some(stream_id), index, &marked);
                }
            }
            Record::Column(incoming) => {
                let kind = wire::MetadataType::Column;
                let column = incoming.get();
                let (stream_id, index) = (column.stream_id(), column.index().index());
                let marked = replacements(&[
                    ("name", column.name()),
                    ("units", column.units()),
                    ("description", column.description()),
                ]);
                let column_id = column.index().value();
                if !self.accepts_stream(stream_id) {
                    self.log_dropped_above(kind, stream_id);
                    return;
                }
                let stream = self.stream_mut(stream_id);
                let held = stream.described.columns.len();
                let (contradicted, learned) = match stream.described.columns.get(index) {
                    Some(current) => (*current != incoming, false),
                    None => {
                        let appended = index == held;
                        if appended {
                            if !decodes(column.data_type()) {
                                log::warn!(
                                    "stream {stream_id} column {index} has data type {}, which this build cannot decode, so the stream is refused",
                                    column.data_type()
                                );
                            }
                            stream.append_column(incoming);
                        }
                        (false, appended)
                    }
                };
                if index > held {
                    // A parser that learns from broadcasts meets later
                    // columns first, so this drop is no fault.
                    log::debug!(
                        "{} dropped a {kind:?} metadata record of stream {stream_id} column {index}, as column {held} comes first",
                        self.route
                    );
                }
                if contradicted {
                    // This should never happen: columns are constant.
                    self.forget_all_metadata(global_generation);
                } else if learned {
                    self.revise_metadata();
                }
                if !contradicted && index <= held {
                    let index = Some(("column", column_id));
                    self.log_replacement(kind, Some(stream_id), index, &marked);
                }
            }
        }
    }

    /// Applies a packet that updates decoding state but carries no sample rows.
    /// `bytes` are the payload's bytes.
    fn apply_control_packet(
        &mut self,
        payload: Payload<'_>,
        bytes: &[u8],
        global_generation: &mut u32,
    ) {
        match payload {
            Payload::Metadata(..) => {
                let (kind, _, record) = wire::split_metadata(bytes)
                    .expect("a metadata payload was framed when it was read");
                self.apply_metadata(
                    kind.into(),
                    record,
                    MetadataSource::Broadcast,
                    global_generation,
                );
            }
            Payload::Heartbeat(Heartbeat::Session { session, .. }) => {
                if let Some(device) = &self.metadata {
                    let device = device.get();
                    let rebooted = device.session() != session && !device.marks_starts_and_stops();
                    if rebooted && !self.ignore_session {
                        self.relearn_metadata();
                    }
                }
            }
            _ => {}
        }
    }

    /// Validates one stream-data packet against this route's metadata.
    fn validate_stream_data<'a>(
        &'a mut self,
        data: wire::Samples<'a>,
        global_generation: &mut u32,
    ) -> Result<PacketEvent<'a>, StreamDataError> {
        let Some(device_metadata) = self.metadata.clone() else {
            return Ok(PacketEvent::WaitingForMetadata);
        };
        if !self.accepts_stream(data.stream_id) {
            return Ok(PacketEvent::WaitingForMetadata);
        }

        let index = usize::from(data.stream_id.value());
        if self.streams.len() <= index {
            self.streams.resize_with(index + 1, || None);
        }
        let key = StreamKey::new(self.route, data.stream_id);
        self.streams[index]
            .get_or_insert_with(|| StreamTracker::new(key))
            .validate_rows(
                data,
                device_metadata,
                &mut self.generation,
                global_generation,
                &mut self.metadata_revision,
            )
    }

    /// Returns one query for as much of the missing metadata as a request may
    /// carry, or `None` while a query is outstanding or nothing is missing.
    fn take_metadata_query(&mut self) -> Option<MetadataQuery> {
        if self.query_in_flight {
            return None;
        }
        let missing = self.missing_metadata();
        let selectors = match missing.as_slice() {
            [] => return None,
            [only] if only.mtype == wire::MetadataType::Device => Vec::new(),
            missing => missing[..missing.len().min(wire::MAX_METADATA_SELECTORS)].to_vec(),
        };
        self.query_in_flight = true;
        Some(MetadataQuery {
            route: self.route,
            selectors,
            generation: self.metadata_generation,
        })
    }

    /// Completes `query` and opens a new metadata generation. Returns `false`
    /// if the route has already left the query's generation, including by
    /// completing that same query.
    fn complete_metadata_query(&mut self, query: &MetadataQuery) -> bool {
        if query.generation != self.metadata_generation {
            return false;
        }
        self.rediscover_metadata();
        true
    }

    fn apply_metadata_reply(
        &mut self,
        query: MetadataQuery,
        reply: &[u8],
        global_generation: &mut u32,
    ) {
        if !self.complete_metadata_query(&query) {
            return;
        }
        let source = match query.selectors.is_empty() {
            true => MetadataSource::Bootstrap,
            false => MetadataSource::Reply,
        };
        match wire::MetadataReply::parse(reply) {
            Some(records) => {
                for (kind, record) in records {
                    self.apply_metadata(kind, record, source, global_generation);
                }
            }
            None => log::warn!(
                "{} dropped a dev.metadata reply with broken framing",
                self.route
            ),
        }
    }

    fn fail_metadata_query(&mut self, query: MetadataQuery) -> bool {
        self.complete_metadata_query(&query)
    }

    /// Returns selectors for every piece of metadata this route still lacks.
    /// It asks for streams 1 to `n_streams`, which law `M1` of the contract in
    /// `twinleaf_proto::contract` says are the streams a device names.
    fn missing_metadata(&self) -> Vec<wire::MetadataSelector> {
        match self.metadata.as_ref() {
            Some(device) => (1..=device.get().n_streams())
                .flat_map(|stream_id| {
                    match self
                        .streams
                        .get(usize::from(stream_id))
                        .and_then(Option::as_ref)
                    {
                        Some(stream) => stream.missing_metadata(),
                        None => vec![wire::MetadataSelector::stream(stream_id)],
                    }
                })
                .collect(),
            None => vec![wire::MetadataSelector::device()],
        }
    }

    fn metadata_snapshot(&self) -> Option<DeviceMetadataSnapshot> {
        if !self.missing_metadata().is_empty() {
            return None;
        }
        let device = self.metadata.as_ref()?.clone();
        let streams = (1..=device.get().n_streams())
            .map(|stream_id| {
                let stream = self
                    .streams
                    .get(usize::from(stream_id))?
                    .as_ref()?
                    .metadata_snapshot(device.clone())?;
                Some((StreamId::new(stream_id), stream))
            })
            .collect::<Option<BTreeMap<_, _>>>()?;
        Some(DeviceMetadataSnapshot::new(self.route, device, streams))
    }
}

/// Result of applying one packet to the ordered metadata and continuity state.
pub(crate) enum PacketEvent<'a> {
    Applied,
    WaitingForMetadata,
    /// The stream has a column of a type this build cannot decode.
    Unsupported,
    Reset,
    Rows(ScannedRows<'a>),
}

/// Route-aware state machine shared by incremental and seekable packet readers.
#[derive(Clone)]
pub(crate) struct ParseState {
    ignore_session: bool,
    devices: Vec<DeviceState>,
    global_generation: u32,
}

impl ParseState {
    pub(crate) fn new(ignore_session: bool) -> Self {
        Self {
            ignore_session,
            devices: Vec::new(),
            global_generation: 0,
        }
    }

    fn device_index(&mut self, route: DeviceRoute) -> usize {
        match self.devices.iter().position(|device| device.route == route) {
            Some(index) => index,
            None => {
                self.devices
                    .push(DeviceState::new(route, self.ignore_session));
                self.devices.len() - 1
            }
        }
    }

    fn device(&self, route: DeviceRoute) -> Option<&DeviceState> {
        self.devices.iter().find(|device| device.route == route)
    }

    /// Applies one packet from `route` that carries `payload`, whose bytes
    /// are `bytes`.
    pub(crate) fn apply_packet<'a>(
        &'a mut self,
        route: DeviceRoute,
        payload: Payload<'a>,
        bytes: &[u8],
    ) -> Result<PacketEvent<'a>, DataError> {
        match payload {
            Payload::Samples(data) => {
                let index = self.device_index(route);
                self.devices[index]
                    .validate_stream_data(data, &mut self.global_generation)
                    .map_err(|source| DataError::Stream {
                        route,
                        stream_id: data.stream_id,
                        source,
                    })
            }
            _ => {
                let index = self.device_index(route);
                self.devices[index].apply_control_packet(
                    payload,
                    bytes,
                    &mut self.global_generation,
                );
                Ok(PacketEvent::Applied)
            }
        }
    }

    /// Blames `site` for the rows each stream of every route `routes`
    /// accepts loses before its next rows.
    pub(crate) fn blame(&mut self, site: LossSite, routes: impl Fn(&DeviceRoute) -> bool) {
        for device in &mut self.devices {
            if routes(&device.route) {
                for stream in device.streams.iter_mut().flatten() {
                    stream.blame(site);
                }
            }
        }
    }

    /// Blames `site` for the rows stream `stream_id` of `route` loses before
    /// its next rows, creating the route's state if it has none.
    pub(crate) fn blame_stream(&mut self, route: DeviceRoute, stream_id: StreamId, site: LossSite) {
        let index = self.device_index(route);
        self.devices[index].stream_mut(stream_id).blame(site);
    }

    /// Drops the metadata of every route `routes` accepts, to learn it again,
    /// as after the link that serves them was lost. A link may bring back
    /// another device, or the same one with another layout. Each stream keeps
    /// its last rows, and each device's next record says whether it is the
    /// same device, as [`DeviceState::relearn_metadata`] does.
    pub(crate) fn relearn(&mut self, routes: impl Fn(&DeviceRoute) -> bool) {
        for device in &mut self.devices {
            if routes(&device.route) {
                device.relearn_metadata();
                device.handed_out = None;
            }
        }
    }

    /// Forgets the metadata of every route `routes` accepts and ends its
    /// runs. The generation counters keep counting, and no stamp from before
    /// the reset recurs after it.
    pub(crate) fn reset(&mut self, routes: impl Fn(&DeviceRoute) -> bool) {
        self.global_generation += 1;
        for device in &mut self.devices {
            if routes(&device.route) {
                device.reset();
                device.handed_out = None;
            }
        }
    }

    pub(crate) fn take_metadata_queries_for(&mut self, route: DeviceRoute) -> Vec<MetadataQuery> {
        let index = self.device_index(route);
        self.devices[index]
            .take_metadata_query()
            .into_iter()
            .collect()
    }

    /// Merges `record` into the state of `route` as a broadcast of it would.
    pub(crate) fn learn(&mut self, route: DeviceRoute, record: Record) {
        let index = self.device_index(route);
        let source = MetadataSource::Broadcast;
        self.devices[index].learn_metadata(record, source, &mut self.global_generation);
    }

    pub(crate) fn apply_metadata_reply(&mut self, query: MetadataQuery, reply: &[u8]) {
        let index = self.device_index(query.route);
        self.devices[index].apply_metadata_reply(query, reply, &mut self.global_generation);
    }

    /// Gives up on `query`. Returns `false` if the query was already stale.
    pub(crate) fn fail_metadata_query(&mut self, query: MetadataQuery) -> bool {
        let index = self.device_index(query.route);
        self.devices[index].fail_metadata_query(query)
    }

    pub(crate) fn metadata(&self, route: DeviceRoute) -> Option<DeviceMetadataSnapshot> {
        self.device(route)?.metadata_snapshot()
    }

    pub(crate) fn metadata_generation(&self, route: DeviceRoute) -> Option<u32> {
        self.device(route).map(|device| device.metadata_generation)
    }

    #[cfg(test)]
    pub(crate) fn metadata_revision(&self, route: DeviceRoute) -> Option<u32> {
        self.device(route).map(|device| device.metadata_revision)
    }

    pub(crate) fn revised_metadata(
        &mut self,
        route: DeviceRoute,
    ) -> Option<(bool, DeviceMetadataSnapshot)> {
        let device = self.devices.iter_mut().find(|d| d.route == route)?;
        let revision = device.metadata_revision;
        if device.handed_out == Some(revision) {
            return None;
        }
        let snapshot = device.metadata_snapshot()?;
        let first = device.handed_out.replace(revision).is_none();
        Some((first, snapshot))
    }

    pub(crate) fn routes(&self) -> Vec<DeviceRoute> {
        self.devices.iter().map(|device| device.route).collect()
    }
}

/// Stream bytes validated against the metadata and continuity state in effect
/// at their position in the packet sequence, described without decoding them.
pub(crate) struct ScannedRows<'a> {
    schema: &'a Arc<StreamSchema>,
    device: DeviceRecord,
    segment: SegmentRecord,
    boundary: Option<Boundary>,
    generations: Generations,
    first_sample_n: SampleNumber,
    last_sample_n: SampleNumber,
    row_count: usize,
    encoded: &'a [u8],
}

impl ScannedRows<'_> {
    pub(crate) fn stream_key(&self) -> StreamKey {
        self.schema.stream_key()
    }

    pub(crate) fn device(&self) -> &DeviceRecord {
        &self.device
    }

    /// Returns the metadata these rows carry, as a batch of them would.
    pub(crate) fn metadata(&self) -> StreamMetadataSnapshot {
        StreamMetadataSnapshot::new(
            self.schema.clone(),
            self.device.clone(),
            self.segment.clone(),
        )
    }

    pub(crate) fn sample_number_bounds(&self) -> (SampleNumber, SampleNumber) {
        (self.first_sample_n, self.last_sample_n)
    }
}

impl RowSource for ScannedRows<'_> {
    fn len(&self) -> usize {
        self.row_count
    }

    fn boundary(&self) -> Option<&Boundary> {
        self.boundary.as_ref()
    }

    fn generations(&self) -> Generations {
        self.generations
    }

    fn schema(&self) -> &Arc<StreamSchema> {
        self.schema
    }

    fn segment_record(&self) -> &SegmentRecord {
        &self.segment
    }

    fn start_builder(&self, capacity: usize) -> SampleBatchBuilder {
        SampleBatchBuilder::new(
            BatchContext {
                metadata: self.metadata(),
                boundary: self.boundary.clone(),
                generations: self.generations,
            },
            capacity,
        )
    }

    fn append_to(&self, tail: &mut SampleBatchBuilder) {
        let columns = tail.push_samples(self.first_sample_n, self.row_count);
        decode_samples(self.schema, self.encoded, self.row_count, columns);
    }
}
