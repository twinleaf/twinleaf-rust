//! Logs written for the tests of the log tools.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use twinleaf::proto::data::{self as wire, DataType};
use twinleaf::proto::sync::Epoch;
use twinleaf::proto::{ColumnId, SegmentId, SessionId, StreamId};
use twinleaf::tio::Packet;
use twinleaf::DeviceRoute;

/// Bytes that do not start a packet. A log stops at them.
pub(in crate::tools::log) const DAMAGE: &[u8] = &[0];

/// Returns a path in the temporary directory that no other test uses.
pub(in crate::tools::log) fn temp_path(label: &str, extension: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "twinleaf_log_tools_{label}_{}_{}.{extension}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed),
    ))
}

/// Returns the metadata of one device at the root, with one 1 Hz stream
/// named `test` of one `f32` column named `value`.
pub(in crate::tools::log) fn metadata() -> Vec<Packet> {
    let records = [
        wire::Metadata::Device(wire::Device {
            session: SessionId::new(1),
            n_streams: 1,
            name: "device",
            serial: "SN1",
            firmware: "fw",
            protocol: 0,
        }),
        wire::Metadata::Stream(wire::Stream {
            stream_id: StreamId::new(1),
            n_columns: 1,
            n_segments: 1,
            sample_size: 4,
            buf_samples: 128,
            name: "test",
        }),
        wire::Metadata::Segment(wire::Segment {
            stream_id: StreamId::new(1),
            segment_id: SegmentId::new(0),
            flags: wire::SegmentFlags::default(),
            epoch: Epoch::UNIX,
            timeref_serial: "clock",
            timeref_session: SessionId::new(1),
            start_time: 0,
            sampling_rate: 1,
            decimation: 1,
            filter_cutoff: 0.0,
            filter_type: wire::FilterType::NONE,
        }),
        wire::Metadata::Column(wire::Column {
            stream_id: StreamId::new(1),
            index: ColumnId::new(0),
            data_type: DataType::F32,
            name: "value",
            units: "",
            description: "",
        }),
    ];
    records
        .into_iter()
        .map(|record| {
            Packet::metadata(record, wire::MetadataFlags::default(), DeviceRoute::root())
                .expect("a valid metadata record")
        })
        .collect()
}

/// Returns `count` samples of that stream from sample 0, one to a packet.
pub(in crate::tools::log) fn samples(count: u32) -> Vec<Packet> {
    (0..count)
        .map(|n| {
            Packet::samples(1, 0, n, &(n as f32).to_le_bytes(), DeviceRoute::root())
                .expect("valid samples")
        })
        .collect()
}

/// Writes `packets` and then `tail` to a new log, and returns its path.
pub(in crate::tools::log) fn write_log(label: &str, packets: &[Packet], tail: &[u8]) -> PathBuf {
    let path = temp_path(label, "tio");
    let mut bytes: Vec<u8> = packets
        .iter()
        .flat_map(|packet| packet.as_bytes().to_vec())
        .collect();
    bytes.extend_from_slice(tail);
    std::fs::write(&path, bytes).expect("write the log");
    path
}
