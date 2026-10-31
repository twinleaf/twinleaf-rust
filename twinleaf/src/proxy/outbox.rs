//! What the proxy queues for one client.
//!
//! An [`Outbox`] holds two queues. Its FIFO holds what is never dropped, in
//! the order it was queued. That is every packet but samples, the updates of
//! the client's watches, and the answers to its requests. Its sample buffer
//! holds the samples that wait for room in the FIFO, and drops the oldest
//! once it holds more than [`SAMPLE_BUDGET`] bytes. A sample joins the FIFO
//! at once while no sample waits and the FIFO holds less than [`WATERMARK`]
//! bytes. The writer moves waiting samples into the FIFO up to the same mark,
//! then takes the FIFO whole. A client whose FIFO outgrows [`HARD_LIMIT`]
//! bytes is disconnected, as nothing in it is dropped.
//!
//! Dropping samples is the one way the outbox departs from the order the
//! link took its packets in. A segment record that differs from the one the
//! client was sent for its route, stream and segment drops the waiting
//! samples of that segment, which were taken under the record it replaces.
//! A device record with a new session, serial or firmware drops every
//! waiting sample of its route. A record sent again unchanged drops nothing.
//!
//! The outbox also holds the client's watches. Whatever adds to it first
//! moves each update ready on a watch into the FIFO, so an update goes out
//! ahead of every packet its link took after the change. An [`Outlet`]
//! shares one client's outbox between the client's task, which writes it,
//! and the links the client mounts, which fill it.

use super::client::ClientError;
use super::control::{self, Refusal, Update};
use crate::proto::data as wire;
use crate::proto::rpc::RpcError;
use crate::proto::{DeviceRoute, SegmentId, SessionId, StreamId};
use crate::tio::packet::{LinkStatus, Packet, Payload};
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Wake, Waker};
use tokio::sync::Notify;
use tokio_stream::{StreamMap, StreamNotifyClose};

/// The bytes of samples the FIFO takes before samples wait in the buffer.
/// A client that keeps up gets its samples in order with everything else,
/// and a packet queued behind samples waits for at most this many bytes of
/// them, 52 ms of a 10 Mbit/s link.
pub(super) const WATERMARK: usize = 64 * 1024;

/// The bytes of samples the buffer holds before it drops the oldest. It is
/// one second of samples arriving at 8 Mbit/s, so a client that stalls for
/// less than that behind one fast device loses none.
pub(super) const SAMPLE_BUDGET: usize = 1024 * 1024;

/// The bytes the FIFO holds before its client is disconnected. A device's
/// heartbeats, statuses and state updates take under 1 KiB a second, so
/// this is over two hours of them. Only a client that stopped reading, or a
/// flood of packets that are not samples, reaches it.
pub(super) const HARD_LIMIT: usize = 8 * 1024 * 1024;

/// What waits to go to one client.
#[derive(Default)]
pub(super) struct Outbox {
    fifo: VecDeque<Packet>,
    fifo_bytes: usize,
    samples: VecDeque<Packet>,
    sample_bytes: usize,
    /// How many samples were dropped since the writer last took the FIFO.
    dropped: u64,
    /// The session, serial and firmware of each route's device record.
    devices: HashMap<DeviceRoute, (SessionId, String, String)>,
    /// The record of each segment the client was sent, by route, stream and
    /// segment.
    segments: HashMap<(DeviceRoute, StreamId, SegmentId), Vec<u8>>,
    /// The updates of each watch, by the id of its `proxy.watch`, with the
    /// route it follows as the client sees it. Each ends with `None`.
    watches: StreamMap<u16, StreamNotifyClose<BoxStream<'static, (DeviceRoute, Update)>>>,
    /// Why the client must be let go, once something it mounts has ended.
    ended: Option<ClientError>,
}

impl Outbox {
    /// Queues `packet`, a sample as the buffer takes it and anything else on
    /// the FIFO. Queues nothing once the FIFO holds more than [`HARD_LIMIT`]
    /// bytes.
    pub(super) fn push(&mut self, packet: Packet) {
        if self.over() {
            return;
        }
        let size = packet.as_bytes().len();
        let sample = matches!(packet.payload(), Payload::Samples(_));
        if sample && (!self.samples.is_empty() || self.fifo_bytes >= WATERMARK) {
            self.sample_bytes += size;
            self.samples.push_back(packet);
            while self.sample_bytes > SAMPLE_BUDGET {
                let oldest = self.samples.pop_front().expect("a sample over the budget");
                self.sample_bytes -= oldest.as_bytes().len();
                self.dropped += 1;
            }
            return;
        }
        self.fifo_bytes += size;
        self.fifo.push_back(packet);
    }

    /// Returns `true` once the FIFO holds more than [`HARD_LIMIT`] bytes.
    pub(super) fn over(&self) -> bool {
        self.fifo_bytes > HARD_LIMIT
    }

    /// Takes what goes out next, the whole FIFO once waiting samples have
    /// topped it up to the watermark, and the count of samples dropped since
    /// the last take.
    pub(super) fn take(&mut self) -> (VecDeque<Packet>, u64) {
        while self.fifo_bytes < WATERMARK {
            let Some(sample) = self.samples.pop_front() else {
                break;
            };
            let size = sample.as_bytes().len();
            self.sample_bytes -= size;
            self.fifo_bytes += size;
            self.fifo.push_back(sample);
        }
        self.fifo_bytes = 0;
        let taken = std::mem::take(&mut self.fifo);
        (taken, std::mem::take(&mut self.dropped))
    }

    /// Notes the metadata `records` of the device at `route` that go to the
    /// client next, and drops the waiting samples they supersede.
    pub(super) fn metadata(
        &mut self,
        route: DeviceRoute,
        records: impl IntoIterator<Item = Packet>,
    ) {
        for record in records {
            match record.payload() {
                Payload::Metadata(wire::Metadata::Device(device), _) => {
                    let held = (device.session, device.serial.into(), device.firmware.into());
                    let old = self.devices.insert(route, held.clone());
                    if old.is_some_and(|old| old != held) {
                        self.discard(|sample, _| sample.route() == route);
                    }
                }
                Payload::Metadata(wire::Metadata::Segment(segment), _) => {
                    let key = (route, segment.stream_id, segment.segment_id);
                    let held = wire::split_metadata(record.payload_bytes());
                    let held = held.map_or(Vec::new(), |(_, _, held)| held.to_vec());
                    let old = self.segments.insert(key, held.clone());
                    if old.is_some_and(|old| old != held) {
                        self.discard(|sample, data| {
                            (sample.route(), data.stream_id, data.segment_id) == key
                        });
                    }
                }
                _ => {}
            }
        }
    }

    /// Drops each waiting sample that `superseded` picks.
    fn discard(&mut self, superseded: impl Fn(&Packet, &wire::Samples<'_>) -> bool) {
        let held = self.samples.len();
        self.samples.retain(|sample| match sample.payload() {
            Payload::Samples(data) => !superseded(sample, &data),
            _ => true,
        });
        self.dropped += (held - self.samples.len()) as u64;
        self.sample_bytes = self
            .samples
            .iter()
            .map(|sample| sample.as_bytes().len())
            .sum();
    }

    /// Queues each update ready on a watch, and a refusal for each watch
    /// that ended. A metadata update first drops the waiting samples it
    /// supersedes. `outlet` is woken once the next update is ready.
    fn updates(&mut self, outlet: &Arc<Outlet>) {
        let waker = Waker::from(Arc::clone(outlet));
        let mut cx = Context::from_waker(&waker);
        while let Poll::Ready(Some((id, update))) = self.watches.poll_next_unpin(&mut cx) {
            let packets = match update {
                Some((route, update)) => {
                    if let Update::Metadata(Some(records)) = &update {
                        let records = records
                            .iter()
                            .map(|record| Packet::from_slice_prefix(record));
                        self.metadata(route, records.filter_map(|record| Some(record.ok()?.0)));
                    }
                    control::parts(id, &update)
                }
                None => {
                    let ended = "the proxy no longer follows this device";
                    vec![control::answer(
                        id,
                        Err(Refusal::new(RpcError::State, ended)),
                    )]
                }
            };
            for packet in packets {
                self.push(packet);
            }
        }
    }
}

/// One client's outbox, shared by the client's task and the links it
/// mounts. It wakes the client's task whenever it has something to write.
#[derive(Default)]
pub(super) struct Outlet {
    outbox: Mutex<Outbox>,
    /// Wakes the client's task to write.
    pub(super) ready: Notify,
}

impl Outlet {
    fn lock(&self) -> MutexGuard<'_, Outbox> {
        self.outbox.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Queues `packet` for the client after each update ready on its
    /// watches. A sample may be dropped as the [`Outbox`] says, and nothing
    /// else is.
    pub(super) fn push(self: &Arc<Self>, packet: Packet) {
        let mut outbox = self.lock();
        outbox.updates(self);
        outbox.push(packet);
        drop(outbox);
        self.ready.notify_one();
    }

    /// Follows watch `id` of the device at `route`, whose updates `updates`
    /// yields, in place of any watch the client had under that id.
    pub(super) fn watch(&self, id: u16, route: DeviceRoute, updates: BoxStream<'static, Update>) {
        let updates = updates.map(move |update| (route, update)).boxed();
        let watch = StreamNotifyClose::new(updates);
        self.lock().watches.insert(id, watch);
        self.ready.notify_one();
    }

    /// Stops watch `id`.
    pub(super) fn unwatch(&self, id: u16) {
        self.lock().watches.remove(&id);
    }

    /// Lets the client go for `error` once what is queued is written, unless
    /// it is let go for another reason already.
    pub(super) fn end(&self, error: ClientError) {
        self.lock().ended.get_or_insert(error);
        self.ready.notify_one();
    }

    /// Stops every watch, and queues the status that the proxy stopped, at
    /// the root.
    pub(super) fn stop(&self) {
        let mut outbox = self.lock();
        outbox.watches.clear();
        outbox.push(Packet::link(LinkStatus::Stopped));
    }

    /// Takes what the client is sent next, with the count of samples dropped
    /// since the last take.
    ///
    /// Returns why the client must be let go, if something it mounts ended
    /// or its FIFO outgrew [`HARD_LIMIT`].
    pub(super) fn take(self: &Arc<Self>) -> Result<(VecDeque<Packet>, u64), ClientError> {
        let mut outbox = self.lock();
        outbox.updates(self);
        if let Some(ended) = outbox.ended.take() {
            return Err(ended);
        }
        if outbox.over() {
            return Err(ClientError::TooSlow);
        }
        Ok(outbox.take())
    }

    /// Takes what is queued, as the connection closes.
    pub(super) fn drain(&self) -> VecDeque<Packet> {
        self.lock().take().0
    }
}

/// A watch with an update ready wakes the client's task.
impl Wake for Outlet {
    fn wake(self: Arc<Self>) {
        self.ready.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::fixtures::{broadcast, segment_record};

    /// A sample of stream 1 in `segment` at `route`, numbered `first`, that
    /// takes `size` bytes of data.
    fn sample(route: &str, segment: u8, first: u32, size: usize) -> Packet {
        let data = vec![0; size];
        Packet::samples(1, segment, first, &data, route.parse().unwrap()).unwrap()
    }

    /// The sample numbers of the samples in `packets`, in order.
    fn numbers(packets: &VecDeque<Packet>) -> Vec<u32> {
        let samples = packets.iter().filter_map(|packet| match packet.payload() {
            Payload::Samples(data) => Some(data.first.value()),
            _ => None,
        });
        samples.collect()
    }

    /// An outbox whose FIFO holds the watermark already in heartbeats, so
    /// every sample waits.
    fn full() -> Outbox {
        let mut outbox = Outbox::default();
        while outbox.fifo_bytes < WATERMARK {
            outbox.push(Packet::heartbeat(DeviceRoute::root()));
        }
        outbox
    }

    /// The device record of `session`, at the root.
    fn device(session: u32, serial: &'static str) -> Packet {
        let record = wire::Device {
            session: SessionId::new(session),
            n_streams: 1,
            name: "d",
            serial,
            firmware: "f",
        };
        broadcast(wire::Metadata::Device(record), DeviceRoute::root())
    }

    /// The record of `segment`, starting at `start`, at the root.
    fn segment(segment: u8, start: u32) -> Packet {
        let record = wire::Segment {
            start_time: start,
            ..segment_record(segment)
        };
        broadcast(wire::Metadata::Segment(record), DeviceRoute::root())
    }

    /// Takes everything `outbox` holds, as the writer would in turn, and the
    /// count of samples it dropped.
    fn take_all(outbox: &mut Outbox) -> (VecDeque<Packet>, u64) {
        let (mut taken, mut dropped) = (VecDeque::new(), 0);
        while !outbox.fifo.is_empty() || !outbox.samples.is_empty() {
            let (more, lost) = outbox.take();
            taken.extend(more);
            dropped += lost;
        }
        (taken, dropped)
    }

    /// Samples beyond the budget drop the oldest first, each counted once,
    /// and the buffer never holds more than the budget.
    #[test]
    fn samples_over_the_budget_drop_the_oldest_and_are_counted() {
        let mut outbox = full();
        let size = 400;
        let sent = 2 * SAMPLE_BUDGET / size;
        for first in 0..sent {
            outbox.push(sample("/", 0, first as u32, size));
            assert!(outbox.sample_bytes <= SAMPLE_BUDGET);
        }

        let (taken, dropped) = take_all(&mut outbox);
        let kept = numbers(&taken);
        assert!(dropped > 0);
        assert_eq!(
            dropped + kept.len() as u64,
            sent as u64,
            "each drop counts once"
        );
        assert_eq!(u64::from(kept[0]), dropped, "the oldest went first");
        assert!(kept.windows(2).all(|pair| pair[0] + 1 == pair[1]));
    }

    /// Nothing but samples is dropped, however many samples come between,
    /// and what is kept goes out in the order it was queued.
    #[test]
    fn packets_that_are_not_samples_are_never_dropped() {
        let mut outbox = Outbox::default();
        for session in 0..1000 {
            outbox.push(Packet::heartbeat_session(session, DeviceRoute::root()));
            for first in 0..100 {
                outbox.push(sample("/", 0, session * 100 + first, 400));
            }
        }

        let (taken, dropped) = take_all(&mut outbox);
        let sessions: Vec<u32> = taken
            .iter()
            .filter_map(|packet| match packet.payload() {
                Payload::Heartbeat(beat) => beat.session().map(SessionId::value),
                _ => None,
            })
            .collect();
        assert_eq!(sessions, (0..1000).collect::<Vec<_>>());
        assert!(dropped > 0, "samples were dropped meanwhile");
        let kept = numbers(&taken);
        assert!(kept.windows(2).all(|pair| pair[0] < pair[1]));
    }

    /// A FIFO that holds more than the hard limit refuses what comes next,
    /// and its client is let go as too slow rather than sent less.
    #[test]
    fn a_fifo_over_the_hard_limit_lets_its_client_go() {
        let outlet = Arc::<Outlet>::default();
        let status = Packet::link(LinkStatus::SensorReconnected);
        let per = status.as_bytes().len();
        for _ in 0..HARD_LIMIT / per {
            outlet.push(status.clone());
        }
        assert!(outlet.take().is_ok(), "the FIFO holds up to the limit");
        for _ in 0..=HARD_LIMIT / per {
            outlet.push(status.clone());
        }

        let held = outlet.lock().fifo.len();
        outlet.push(status);
        outlet.push(sample("/", 0, 0, 4));
        assert_eq!(outlet.lock().fifo.len(), held, "nothing more was queued");
        assert!(matches!(outlet.take(), Err(ClientError::TooSlow)));
    }

    /// Samples join the FIFO at once below the watermark. Past it they wait,
    /// and the writer moves them into the FIFO up to the watermark, in
    /// order, when it takes it.
    #[test]
    fn waiting_samples_refill_the_fifo_up_to_the_watermark() {
        let mut outbox = Outbox::default();
        let size = 400;
        let fit = WATERMARK.div_ceil(sample("/", 0, 0, size).as_bytes().len());
        for first in 0..3 * fit {
            outbox.push(sample("/", 0, first as u32, size));
        }
        assert_eq!(outbox.fifo.len(), fit, "the FIFO takes samples to the mark");
        assert_eq!(outbox.samples.len(), 2 * fit);
        outbox.push(Packet::heartbeat(DeviceRoute::root()));
        assert_eq!(outbox.fifo.len(), fit + 1, "anything else joins at once");

        let (first, _) = outbox.take();
        assert_eq!(numbers(&first), (0..fit as u32).collect::<Vec<_>>());
        let (second, _) = outbox.take();
        assert_eq!(second.len(), fit, "the writer tops the FIFO up to the mark");
        let (third, dropped) = outbox.take();
        assert_eq!(
            numbers(&third),
            (2 * fit as u32..3 * fit as u32).collect::<Vec<_>>()
        );
        assert_eq!(dropped, 0);
    }

    /// A segment record that differs from the one held drops the waiting
    /// samples of its own route, stream and segment, and no others. The same
    /// record again drops nothing.
    #[test]
    fn a_changed_segment_record_drops_only_its_own_segments_waiting_samples() {
        let mut outbox = full();
        outbox.metadata(
            DeviceRoute::root(),
            [device(1, "s"), segment(0, 0), segment(1, 1)],
        );
        let waiting = [
            sample("/", 0, 0, 4),
            sample("/", 1, 1, 4),
            sample("/2", 0, 2, 4),
            Packet::samples(2, 0, 3, &[0; 4], DeviceRoute::root()).unwrap(),
            sample("/", 0, 4, 4),
        ];
        for packet in waiting {
            outbox.push(packet);
        }

        outbox.metadata(
            DeviceRoute::root(),
            [device(1, "s"), segment(0, 0), segment(1, 1)],
        );
        assert_eq!(outbox.samples.len(), 5, "an identical record drops nothing");

        outbox.metadata(DeviceRoute::root(), [segment(0, 10)]);
        assert_eq!(numbers(&outbox.samples), [1, 2, 3]);
        assert_eq!(outbox.dropped, 2);
    }

    /// A device record with a new session, serial or firmware drops every
    /// waiting sample of its route, and none of another route.
    #[test]
    fn a_changed_device_record_drops_its_whole_routes_waiting_samples() {
        for changed in [device(2, "s"), device(1, "t")] {
            let mut outbox = full();
            outbox.metadata(DeviceRoute::root(), [device(1, "s")]);
            for (route, segment) in [("/", 0), ("/", 1), ("/2", 0)] {
                outbox.push(sample(route, segment, 0, 4));
            }

            outbox.metadata(DeviceRoute::root(), [changed]);

            let routes: Vec<_> = outbox.samples.iter().map(Packet::route).collect();
            assert_eq!(routes, ["/2".parse().unwrap()]);
        }
    }
}
