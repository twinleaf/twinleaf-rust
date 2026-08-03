//! Incremental conversion from routed TIO packets to columnar sample batches.
//!
//! [`PacketParser`] applies packets in arrival order. Shared metadata and
//! continuity state validates each stream payload, after which this module
//! decodes its rows and accumulates them into [`SampleBatch`] values.
//!
//! Stream data can arrive before its metadata. Such packets produce no rows;
//! [`PacketParser::take_requests`] returns the metadata RPCs needed to decode
//! subsequent packets.

use super::sample::{ColumnData, SampleBatch, Series};
use super::state::{DeviceMetadataSnapshot, PacketEvent, ParseState, ValidatedRows};
use crate::data::ColumnVec;
use crate::tio::{self, proto};
use proto::identifiers::StreamKey;
use proto::DeviceRoute;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

impl ValidatedRows<'_> {
    fn can_append_to(&self, batch: &SampleBatch) -> bool {
        (Arc::ptr_eq(&batch.segment, &self.segment)
            || batch.segment.as_ref() == self.segment.as_ref())
            && batch.columns.len() == self.columns.len()
            && batch
                .columns
                .iter()
                .zip(self.columns)
                .all(|(series, metadata)| {
                    Arc::ptr_eq(&series.metadata, metadata)
                        || series.metadata.as_ref() == metadata.as_ref()
                })
    }

    fn decode_into(&self, sample_numbers: &mut Vec<u32>, columns: &mut [Series]) {
        for row in 0..self.row_count {
            let start = row * self.sample_size;
            let mut raw = &self.encoded[start..start + self.sample_size];
            sample_numbers.push(self.first_sample_n.wrapping_add(row as u32));
            for (series, metadata) in columns.iter_mut().zip(self.columns) {
                series
                    .values
                    .push_data(&ColumnData::from_le_bytes(raw, metadata.data_type));
                raw = &raw[metadata.data_type.size()..];
            }
        }
    }

    fn append_to(&self, batch: &mut SampleBatch) {
        debug_assert!(self.can_append_to(batch));
        self.decode_into(&mut batch.sample_numbers, &mut batch.columns);
    }

    fn into_batch(self, route: DeviceRoute, capacity: usize) -> SampleBatch {
        let mut columns: Vec<_> = self
            .columns
            .iter()
            .map(|metadata| Series {
                index: metadata.index,
                metadata: metadata.clone(),
                values: ColumnVec::with_capacity_for(metadata.data_type.buffer_type(), capacity),
            })
            .collect();
        let mut sample_numbers = Vec::with_capacity(capacity);
        self.decode_into(&mut sample_numbers, &mut columns);
        SampleBatch::new(
            route,
            self.boundary,
            sample_numbers,
            columns,
            self.segment,
            self.stream,
            self.device,
        )
    }
}

fn flush_pending_batches(
    pending: &mut HashMap<StreamKey, SampleBatch>,
    ready: &mut VecDeque<SampleBatch>,
) {
    let mut keys: Vec<_> = pending.keys().copied().collect();
    keys.sort_unstable();
    for key in keys {
        ready.push_back(pending.remove(&key).unwrap());
    }
}

/// Decode validated rows and either append, hold, or emit the resulting batch.
fn queue_rows(
    route: DeviceRoute,
    stream_id: u8,
    input: ValidatedRows<'_>,
    target_rows: Option<usize>,
    pending: &mut HashMap<StreamKey, SampleBatch>,
    ready: &mut VecDeque<SampleBatch>,
) -> usize {
    let rows = input.row_count;
    let key = StreamKey::new(route, stream_id);
    let starts_boundary = input.boundary.is_some();

    // Make a boundary observable immediately and preserve global ordering for
    // exporters that split every stream at a discontinuity.
    if starts_boundary {
        flush_pending_batches(pending, ready);
    }

    match pending.entry(key) {
        Entry::Occupied(mut entry) if !starts_boundary && input.can_append_to(entry.get()) => {
            input.append_to(entry.get_mut());
            if target_rows.is_none() || entry.get().len() >= target_rows.expect("checked above") {
                ready.push_back(entry.remove());
            }
        }
        Entry::Occupied(entry) => {
            ready.push_back(entry.remove());
            let capacity = target_rows.unwrap_or(rows).max(rows);
            let batch = input.into_batch(route, capacity);
            if starts_boundary || target_rows.is_none() || batch.len() >= capacity {
                ready.push_back(batch);
            } else {
                pending.insert(key, batch);
            }
        }
        Entry::Vacant(entry) => {
            let capacity = target_rows.unwrap_or(rows).max(rows);
            let batch = input.into_batch(route, capacity);
            if starts_boundary || target_rows.is_none() || batch.len() >= capacity {
                ready.push_back(batch);
            } else {
                entry.insert(batch);
            }
        }
    }

    rows
}

/// Incremental, route-aware parser for TIO packets.
///
/// The caller owns packet I/O and supplies packets in arrival order. By
/// default each decodable stream-data packet emits one [`SampleBatch`].
/// [`PacketParser::with_batch_rows`] instead accumulates compatible packets for
/// bulk consumers. Boundaries and schema changes always end the current batch.
pub struct PacketParser {
    state: ParseState,
    batch_target_rows: Option<usize>,
    pending_batches: HashMap<StreamKey, SampleBatch>,
    ready_batches: VecDeque<SampleBatch>,
}

impl PacketParser {
    pub(super) fn from_state(state: ParseState) -> Self {
        Self {
            state,
            batch_target_rows: None,
            pending_batches: HashMap::new(),
            ready_batches: VecDeque::new(),
        }
    }

    /// Create a parser for packets whose routes are relative to `root_route`.
    ///
    /// When `ignore_session` is false, a heartbeat session change invalidates
    /// cached metadata and starts metadata discovery again.
    pub fn new(root_route: DeviceRoute, ignore_session: bool) -> Self {
        Self::from_state(ParseState::new(root_route, ignore_session))
    }

    /// Accumulate approximately `rows` decoded samples per stream before
    /// emitting a batch. Boundaries and schema changes always end a batch.
    pub fn with_batch_rows(mut self, rows: usize) -> Self {
        assert!(rows > 0, "batch row target must be nonzero");
        self.batch_target_rows = Some(rows);
        self
    }

    /// Ingest one port-relative packet and return the number of rows decoded.
    ///
    /// Metadata and control packets update parser state and return zero. Stream
    /// data also returns zero when its metadata is not ready; call
    /// [`PacketParser::take_requests`] to obtain the required metadata RPCs.
    /// Completed batches are available through [`PacketParser::next_batch`].
    pub fn push_packet(&mut self, packet: &tio::Packet) -> usize {
        match self.state.apply_packet(packet) {
            PacketEvent::None => 0,
            PacketEvent::Reset => {
                self.flush();
                0
            }
            PacketEvent::Rows {
                route,
                stream_id,
                rows,
            } => queue_rows(
                route,
                stream_id,
                rows,
                self.batch_target_rows,
                &mut self.pending_batches,
                &mut self.ready_batches,
            ),
        }
    }

    /// Convenience for low-latency consumers: ingest a packet and pop the next
    /// completed batch, if any. A previously queued batch may be returned.
    pub fn process_packet(&mut self, packet: &tio::Packet) -> Option<SampleBatch> {
        self.push_packet(packet);
        self.next_batch()
    }

    /// Pop the oldest completed batch.
    pub fn next_batch(&mut self) -> Option<SampleBatch> {
        self.ready_batches.pop_front()
    }

    /// End every partially accumulated stream batch without resetting metadata.
    pub fn flush(&mut self) {
        flush_pending_batches(&mut self.pending_batches, &mut self.ready_batches);
    }

    /// Drain batches that are already ready without flushing partial batches.
    pub fn drain_batches(&mut self) -> Vec<SampleBatch> {
        self.ready_batches.drain(..).collect()
    }

    /// Flush accumulated samples and return every completed batch.
    pub fn finish(mut self) -> Vec<SampleBatch> {
        self.flush();
        self.drain_batches()
    }

    /// Forget device metadata. Any decoded rows already accumulated remain
    /// available through `next_batch`.
    pub fn reset(&mut self) {
        self.flush();
        self.state.reset();
    }

    /// Pending metadata requests across all routes. Each packet's routing is
    /// stamped relative to this parser's root and can be sent directly on the
    /// corresponding port.
    pub fn take_requests(&mut self) -> Vec<tio::Packet> {
        self.state.take_requests()
    }

    /// Ensure a route has parser state and take its pending metadata requests.
    pub fn take_requests_for(&mut self, route: DeviceRoute) -> Vec<tio::Packet> {
        self.state.take_requests_for(route)
    }

    /// Return complete metadata for an absolute route once it is available.
    pub fn metadata(&self, route: DeviceRoute) -> Option<DeviceMetadataSnapshot> {
        self.state.metadata(route)
    }

    /// Absolute routes for which this parser has observed state.
    pub fn routes(&self) -> Vec<DeviceRoute> {
        self.state.routes()
    }
}
