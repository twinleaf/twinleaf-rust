//! Publishing a pipeline's outputs as the streams of a TIO device.
//!
//! Each output is a stream. Its channels are the columns, all doubles, and
//! its samples are numbered so that a reader reconstructs the very times the
//! graph computed them for. A stream's time is told the way its sources'
//! was, by the same epoch and the same time reference, so a reader can lay
//! an output over the sensors it came from.
//!
//! A segment counts samples from a whole second, in 24 bits. An output's
//! sample `j` is at `origin + j / rate`, so a segment is begun at a second
//! that falls on a sample, and a new one is begun before the count runs out.
//! A gap in an output is a jump in the sample number, which a reader already
//! knows as lost data.

use crate::run::{Emitted, OutputInfo};
use twinleaf::proto::data::{self as wire, DataType};
use twinleaf::proto::SessionId;
use twinleaf::tio::Packet;
use twinleaf::{ColumnId, DeviceRoute, SegmentId, StreamId};
use twinleaf_device::data::Streams;

/// The most streams a device can have, since stream ids are 1 to 127.
pub const MAX_STREAMS: usize = 127;

/// How many segment ids a stream cycles through.
const SEGMENTS: u8 = 128;

/// The most a sample number can be.
const MAX_SAMPLE: u64 = (1 << 24) - 1;

/// The most payload a packet carries, less the header of a sample packet.
const MAX_SAMPLE_BYTES: usize = 500 - 4;

/// Why outputs could not be published.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum PublishError {
    #[error("a device has at most {MAX_STREAMS} streams, and the spec has {0} outputs")]
    TooManyOutputs(usize),
    #[error("output `{name}` has {channels} channels, more than fit in one packet")]
    TooWide { name: String, channels: usize },
    #[error(
        "output `{name}` runs at {rate} Hz, which is not a ratio of whole numbers a stream can state"
    )]
    Rate { name: String, rate: f64 },
    #[error(
        "output `{name}` begins at {origin} s, which is not a whole second a segment can start on"
    )]
    Origin { name: String, origin: f64 },
    #[error("cannot encode a packet: {0}")]
    Encode(String),
}

/// What identifies the device that publishes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub name: String,
    pub serial: String,
    pub firmware: String,
    pub session: SessionId,
}

/// Turns output blocks into the packets of a device.
pub struct Publisher {
    identity: Identity,
    route: DeviceRoute,
    streams: Vec<StreamOut>,
}

struct StreamOut {
    info: OutputInfo,
    id: u8,
    /// The rate as the ratio a segment states it by.
    sampling_rate: u32,
    decimation: u32,
    /// The whole second sample 0 of the output's domain falls on.
    origin: u32,
    /// The segment samples are being numbered in. There always is one, so
    /// that the stream can be described before its first sample.
    segment: Segment,
}

#[derive(Clone, Copy)]
struct Segment {
    id: u8,
    start_time: u32,
    /// The output index that is sample number 0 of this segment.
    base: u64,
}

impl Publisher {
    /// Creates a publisher with no streams. Its packets carry `route`, which
    /// is the root for a device that something else mounts.
    pub fn new(identity: Identity, route: DeviceRoute) -> Self {
        Self {
            identity,
            route,
            streams: Vec::new(),
        }
    }

    pub fn identity(&self) -> &Identity {
        &self.identity
    }

    /// Declares the streams, one per output, in place of any declared
    /// before. Returns the metadata that describes them, or nothing if they
    /// are the streams already declared.
    pub fn configure(&mut self, outputs: Vec<OutputInfo>) -> Result<Vec<Packet>, PublishError> {
        if outputs.len() > MAX_STREAMS {
            return Err(PublishError::TooManyOutputs(outputs.len()));
        }
        let mut changed = outputs.len() != self.streams.len();
        let mut streams = Vec::with_capacity(outputs.len());
        for (index, info) in outputs.into_iter().enumerate() {
            let name = || info.name.clone();
            if info.channels.len() * 8 > MAX_SAMPLE_BYTES {
                return Err(PublishError::TooWide {
                    name: name(),
                    channels: info.channels.len(),
                });
            }
            let rate = info.domain.rate;
            let (sampling_rate, decimation) =
                ratio(rate).ok_or_else(|| PublishError::Rate { name: name(), rate })?;
            let origin = info.domain.origin;
            if origin.fract() != 0.0 || !(0.0..=f64::from(u32::MAX)).contains(&origin) {
                return Err(PublishError::Origin {
                    name: name(),
                    origin,
                });
            }
            // A stream that is what it was carries on in its segment, so a
            // reader sees no break. One that changed begins a new segment,
            // numbered on from its last, so a reader does not take it for
            // the old one resumed.
            let before = self.streams.get(index);
            let segment = match before {
                Some(before) if same_stream(&before.info, &info) => before.segment,
                _ => {
                    changed = true;
                    Segment {
                        id: before.map_or(0, |before| (before.segment.id + 1) % SEGMENTS),
                        // Sample 0 of the output is sample 0 of its segment.
                        start_time: origin as u32,
                        base: 0,
                    }
                }
            };
            streams.push(StreamOut {
                info,
                id: index as u8 + 1,
                sampling_rate,
                decimation,
                origin: origin as u32,
                segment,
            });
        }
        self.streams = streams;
        // Nothing to say if every stream is what it was.
        if changed {
            self.metadata()
        } else {
            Ok(Vec::new())
        }
    }

    /// Everything a reader needs to decode the streams. That is the device,
    /// and each stream with its current segment and its columns.
    pub fn metadata(&self) -> Result<Vec<Packet>, PublishError> {
        let mut packets = vec![self.record(wire::Metadata::Device(self.device()))?];
        for stream in &self.streams {
            packets.push(self.record(wire::Metadata::Stream(stream.shape()))?);
            packets.push(self.record(wire::Metadata::Segment(stream.current()))?);
            for index in 0..stream.info.channels.len() {
                packets.push(self.record(wire::Metadata::Column(stream.column(index)))?);
            }
        }
        Ok(packets)
    }

    /// The record that describes the device.
    pub fn device(&self) -> wire::Device<'_> {
        wire::Device {
            session: self.identity.session,
            n_streams: self.streams.len() as _,
            name: &self.identity.name,
            serial: &self.identity.serial,
            firmware: &self.identity.firmware,
            protocol: 0,
        }
    }

    fn record(&self, record: wire::Metadata<'_>) -> Result<Packet, PublishError> {
        Packet::metadata(record, wire::MetadataFlags::UPDATE, self.route)
            .map_err(|error| PublishError::Encode(error.to_string()))
    }

    /// Encodes one emitted block as sample packets, ahead of them the
    /// segment record if a new segment had to be begun.
    pub fn publish(&mut self, emitted: &Emitted) -> Result<Vec<Packet>, PublishError> {
        let block = &emitted.block;
        let Some(stream) = self.streams.get(emitted.output) else {
            return Ok(Vec::new());
        };
        let end = block.first + block.frames as u64;
        let segment = stream.segment;
        let mut packets = Vec::new();
        if block.first < segment.base || end - segment.base > MAX_SAMPLE {
            // Begin a segment at the last whole second at or before the
            // block that falls on a sample. At `up / down` samples a second,
            // that is a second whose count of samples is whole.
            let (up, down) = (
                u64::from(stream.sampling_rate),
                u64::from(stream.decimation),
            );
            let second = block.first * down / up;
            let aligned = (0..=down)
                .map(|back| second.saturating_sub(back))
                .find(|second| (second * up) % down == 0)
                .unwrap_or(0);
            let begun = Segment {
                id: (segment.id + 1) % SEGMENTS,
                start_time: stream.origin + aligned as u32,
                base: aligned * up / down,
            };
            self.streams[emitted.output].segment = begun;
            let stream = &self.streams[emitted.output];
            packets.push(self.record(wire::Metadata::Segment(stream.current()))?);
        }
        let stream = &self.streams[emitted.output];
        let segment = stream.segment;

        let channels = block.channels.len();
        let per_packet = (MAX_SAMPLE_BYTES / (channels * 8)).max(1);
        let mut data = Vec::with_capacity(per_packet * channels * 8);
        for start in (0..block.frames).step_by(per_packet) {
            let frames = per_packet.min(block.frames - start);
            data.clear();
            for frame in start..start + frames {
                for channel in 0..channels {
                    let sample = block.data[channel * block.frames + frame];
                    data.extend_from_slice(&sample.to_le_bytes());
                }
            }
            let number = (block.first + start as u64 - segment.base) as u32;
            let packet = Packet::samples(stream.id, segment.id, number, &data, self.route)
                .map_err(|error| PublishError::Encode(error.to_string()))?;
            packets.push(packet);
        }
        Ok(packets)
    }

    fn stream(&self, id: u8) -> Option<&StreamOut> {
        self.streams.get(usize::from(id).checked_sub(1)?)
    }
}

impl StreamOut {
    fn shape(&self) -> wire::Stream<'_> {
        let channels = self.info.channels.len();
        wire::Stream {
            stream_id: StreamId::new(self.id),
            n_columns: channels as _,
            n_segments: SEGMENTS as _,
            sample_size: (channels * 8) as _,
            buf_samples: (MAX_SAMPLE_BYTES / (channels * 8)) as _,
            name: &self.info.name,
        }
    }

    fn current(&self) -> wire::Segment<'_> {
        wire::Segment {
            stream_id: StreamId::new(self.id),
            segment_id: SegmentId::new(self.segment.id),
            flags: wire::SegmentFlags::default(),
            epoch: self.info.domain.epoch,
            timeref_serial: &self.info.timebase.serial,
            timeref_session: self.info.timebase.session,
            start_time: self.segment.start_time,
            sampling_rate: self.sampling_rate,
            decimation: self.decimation,
            filter_cutoff: 0.0,
            filter_type: wire::FilterType::NONE,
        }
    }

    fn column(&self, index: usize) -> wire::Column<'_> {
        let channel = &self.info.channels[index];
        wire::Column {
            stream_id: StreamId::new(self.id),
            index: ColumnId::new(index as u8),
            data_type: DataType::F64,
            name: &channel.name,
            units: channel.unit.symbol(),
            description: "",
        }
    }
}

/// What a `dev.metadata` request is answered from.
impl Streams for Publisher {
    fn ids(&self) -> impl Iterator<Item = u8> {
        self.streams.iter().map(|stream| stream.id)
    }

    fn stream(&self, stream_id: u8) -> Option<wire::Stream<'_>> {
        self.stream(stream_id).map(StreamOut::shape)
    }

    fn segment(&self, stream_id: u8, index: u8) -> Option<wire::Segment<'_>> {
        let stream = self.stream(stream_id)?;
        // Only the segment being written is still known.
        (index == wire::CURRENT_SEGMENT || index == stream.segment.id).then(|| stream.current())
    }

    fn column(&self, stream_id: u8, index: u8) -> Option<wire::Column<'_>> {
        let stream = self.stream(stream_id)?;
        (usize::from(index) < stream.info.channels.len()).then(|| stream.column(usize::from(index)))
    }
}

/// Whether two outputs are one stream to a reader, with the same name and
/// columns, at the same rate, on the same clock.
fn same_stream(a: &OutputInfo, b: &OutputInfo) -> bool {
    a.name == b.name
        && a.channels == b.channels
        && a.domain.rate == b.domain.rate
        && a.domain.origin == b.domain.origin
        && a.domain.epoch == b.domain.epoch
        && a.timebase.serial == b.timebase.serial
        && a.timebase.session == b.timebase.session
}

/// A rate as a ratio of two whole numbers, the smaller the better. A rate
/// that is not one, to within rounding, has none.
fn ratio(rate: f64) -> Option<(u32, u32)> {
    if !(rate.is_finite() && rate > 0.0) {
        return None;
    }
    (1..=100_000u32).find_map(|decimation| {
        let sampling = rate * f64::from(decimation);
        let whole = sampling.round();
        let exact = (sampling - whole).abs() <= 1e-12 * sampling.max(1.0);
        (exact && whole >= 1.0 && whole <= f64::from(u32::MAX))
            .then_some((whole as u32, decimation))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run::Timebase;
    use crate::signal::{Block, Channel, ChannelSet, Domain, DomainId, RunId, Unit};
    use twinleaf::data::PacketParser;
    use twinleaf::proto::sync::Epoch;

    #[test]
    fn a_rate_is_stated_as_the_smallest_ratio() {
        assert_eq!(ratio(500.0), Some((500, 1)));
        assert_eq!(ratio(1000.0 / 3.0), Some((1000, 3)));
        assert_eq!(ratio(0.5), Some((1, 2)));
        assert_eq!(ratio(44_100.0), Some((44_100, 1)));
        assert_eq!(ratio(std::f64::consts::PI), None);
        assert_eq!(ratio(0.0), None);
    }

    fn output(rate: f64) -> OutputInfo {
        OutputInfo {
            name: "gradient".to_owned(),
            channels: vec![Channel {
                name: "x".to_owned(),
                unit: Unit::new("nT"),
            }],
            domain: Domain {
                id: DomainId(0),
                epoch: Epoch::UNIX,
                session: SessionId::new(1),
                rate,
                origin: 1_700_000_000.0,
            },
            timebase: Timebase {
                serial: "hub".to_owned(),
                session: SessionId::new(7),
                key: "hub#7".to_owned(),
            },
        }
    }

    fn block(first: u64, frames: usize) -> Emitted {
        let channels = ChannelSet::uniform(["x"], &Unit::new("nT")).unwrap();
        Emitted {
            output: 0,
            build: 1,
            block: Block {
                domain: DomainId(0),
                run: RunId(1),
                first,
                frames,
                channels,
                data: (0..frames).map(|i| (first + i as u64) as f64).collect(),
            },
        }
    }

    /// Publishes blocks and reads the packets back the way any client would.
    fn round_trip(rate: f64, blocks: &[(u64, usize)]) -> Vec<(bool, f64, f64, f64)> {
        let identity = Identity {
            name: "tiodsp".to_owned(),
            serial: "test".to_owned(),
            firmware: "0".to_owned(),
            session: SessionId::new(99),
        };
        let mut publisher = Publisher::new(identity, DeviceRoute::root());
        let mut packets = publisher.configure(vec![output(rate)]).unwrap();
        for (first, frames) in blocks {
            packets.extend(publisher.publish(&block(*first, *frames)).unwrap());
        }
        let mut parser = PacketParser::new(false);
        for packet in &packets {
            parser.push_packet(packet).unwrap();
        }
        // Each row: whether it follows a break, its time, its value, the rate.
        let mut rows = Vec::new();
        for batch in parser.finish() {
            assert_eq!(batch.metadata().schema().stream().name(), "gradient");
            assert_eq!(batch.metadata().segment().timeref_serial(), "hub");
            let values = batch.columns()[0].to_f64();
            for index in 0..batch.len() {
                let broken = index == 0 && !batch.is_continuous();
                let time = batch.row(index).unwrap().timestamp_begin();
                rows.push((broken, time, values[index], batch.metadata().rate_hz()));
            }
        }
        rows
    }

    #[test]
    fn samples_read_back_at_the_times_they_were_computed_for() {
        // Sample j is at origin + j / rate. The value of each sample here is
        // its own index, so the two can be checked against each other.
        let rows = round_trip(500.0, &[(29, 100), (129, 300), (700, 50)]);
        assert_eq!(rows.len(), 450);
        for (_, time, value, rate) in &rows {
            assert_eq!(*rate, 500.0);
            assert!((time - (1_700_000_000.0 + value / 500.0)).abs() < 1e-6);
        }
        // The first sample opens the stream, and the gap from 429 to 700 is
        // seen as one. Nothing else breaks it.
        let breaks: Vec<f64> = rows.iter().filter(|r| r.0).map(|r| r.2).collect();
        assert_eq!(breaks, [29.0, 700.0]);
    }

    #[test]
    fn a_rate_that_is_not_whole_starts_its_segment_on_a_sample() {
        // At 1000/3 Hz only every third second falls on a sample.
        let rows = round_trip(1000.0 / 3.0, &[(1000, 40)]);
        for (_, time, value, _) in &rows {
            assert!((time - (1_700_000_000.0 + value * 3.0 / 1000.0)).abs() < 1e-6);
        }
        assert_eq!(rows.len(), 40);
    }

    #[test]
    fn a_new_segment_is_begun_before_the_sample_count_runs_out() {
        // A sample number has 24 bits. This stretch crosses that many
        // samples from where its segment began, and reads back unbroken.
        let near = (1 << 24) - 60;
        let rows = round_trip(500.0, &[(0, 10), (10, 30), (near, 40), (near + 40, 40)]);
        let tail: Vec<_> = rows.iter().filter(|r| r.2 >= near as f64).collect();
        assert_eq!(tail.len(), 80);
        for (index, (broken, time, value, _)) in tail.iter().enumerate() {
            assert_eq!(*value, (near + index as u64) as f64);
            assert!((time - (1_700_000_000.0 + value / 500.0)).abs() < 1e-6);
            // Only the jump from sample 40 to here is a break.
            assert_eq!(*broken, index == 0);
        }
    }
}
