//! A deterministic host platform for a [`Device`] and its streams, built to the lane rules.
//!
//! Each [`Port`] holds the control, data and event lanes it has yet to read, every packet in its
//! lane's write order. A lane that outgrows its capacity drops its oldest packet and counts it, so
//! a port that falls behind loses a contiguous run and delays no one. Time is the nanosecond each
//! call is handed, never the system clock, and the configuration and firmware slot are memory.

extern crate std;

use std::collections::VecDeque;
use std::vec::Vec;

use twinleaf_proto::log::LogLevel;
use twinleaf_proto::packet::{lane_of, Lane, PacketView};
use twinleaf_proto::rpc::RpcError;
use twinleaf_proto::sync::Epoch;
use twinleaf_proto::{RpcRequestId, SessionId};

use crate::data::{DataStream, Params, Timeref};
use crate::device::{
    self, Actions, Deferred, Entries, Entry, FlashOp, Group, Identity, Input, Lanes, Pending,
    SyncRequest,
};
use crate::rpc::{Reply, RpcSpec};
use crate::storage::update::{Package, Take, Upload};
use crate::{Device, Sink};

/// Nanoseconds in a second.
const SECOND: u64 = 1_000_000_000;

/// A board's entries and the streams each of its boots builds.
pub trait Board<const N: usize>: Group {
    /// Its streams, stopped, as a boot builds them.
    fn streams(&self) -> Vec<DataStream<N>>;
}

/// One lane of one port: the packets written to it that the port has yet to read.
#[derive(Debug, Default)]
pub struct Queue {
    packets: VecDeque<(u64, Vec<u8>)>,
    capacity: usize,
    /// The packets it dropped as the port fell behind.
    pub dropped: u64,
}

impl Queue {
    fn push(&mut self, written: u64, packet: &[u8]) {
        self.packets.push_back((written, packet.to_vec()));
        while self.packets.len() > self.capacity {
            self.packets.pop_front();
            self.dropped += 1;
        }
    }

    /// The oldest packet still queued.
    pub fn pop(&mut self) -> Option<Vec<u8>> {
        self.packets.pop_front().map(|(_, packet)| packet)
    }

    /// Whether no packet is queued.
    pub fn is_empty(&self) -> bool {
        self.packets.is_empty()
    }
}

/// A port: its three lanes, and whether a host holds it open.
#[derive(Debug)]
pub struct Port {
    /// Its announcements, heartbeats and device, stream and column records, with the replies to
    /// what it asked.
    pub control: Queue,
    /// Every stream's rows and segment records.
    pub data: Queue,
    /// Logs.
    pub event: Queue,
    open: bool,
}

impl Port {
    /// An open port whose every lane holds `capacity` packets.
    pub fn new(capacity: usize) -> Self {
        let queue = || Queue {
            capacity,
            ..Queue::default()
        };
        Self {
            control: queue(),
            data: queue(),
            event: queue(),
            open: true,
        }
    }

    /// The queue of `lane`.
    pub fn lane(&mut self, lane: Lane) -> &mut Queue {
        match lane {
            Lane::Control => &mut self.control,
            Lane::Data => &mut self.data,
            Lane::Event => &mut self.event,
        }
    }

    /// Whether every lane is empty.
    pub fn is_empty(&self) -> bool {
        self.control.is_empty() && self.data.is_empty() && self.event.is_empty()
    }

    /// The packets its lanes dropped as it fell behind.
    pub fn dropped(&self) -> u64 {
        self.control.dropped + self.data.dropped + self.event.dropped
    }

    /// The packet written first of those queued on any lane.
    pub fn written(&mut self) -> Option<Vec<u8>> {
        [Lane::Control, Lane::Data, Lane::Event]
            .into_iter()
            .filter_map(|lane| Some((self.lane(lane).packets.front()?.0, lane)))
            .min()
            .and_then(|(_, lane)| self.lane(lane).pop())
    }

    /// The next packet as firmware egress sends it: control, then logs, then data.
    pub fn egress(&mut self) -> Option<Vec<u8>> {
        [Lane::Control, Lane::Event, Lane::Data]
            .into_iter()
            .find_map(|lane| self.lane(lane).pop())
    }

    fn send(&mut self, lane: Lane, written: u64, packet: &[u8]) {
        if self.open {
            self.lane(lane).push(written, packet);
        }
    }
}

/// A firmware slot, as the kind of device being simulated keeps it.
pub enum Slot {
    /// One that takes images, counting the image bytes of each chunk past its `envelope`.
    Image {
        /// The bytes of a chunk that are envelope rather than image.
        envelope: usize,
        /// The image bytes taken so far.
        uploaded: u32,
    },
    /// A signed package for `Package`, through this crate's own cursor; no key checks it.
    Signed(Package, Upload),
}

impl Slot {
    fn upload(&mut self, chunk: &[u8], now_ns: u64) -> Result<Vec<u8>, RpcError> {
        match self {
            Self::Image { uploaded, .. } if chunk.is_empty() => Ok(uploaded.to_le_bytes().to_vec()),
            Self::Image { envelope, uploaded } => {
                *uploaded = uploaded.saturating_add(chunk.len().saturating_sub(*envelope) as u32);
                Ok(Vec::new())
            }
            Self::Signed(_, upload) if chunk.is_empty() => {
                Ok(upload.cursor(now_ns).to_le_bytes().to_vec())
            }
            Self::Signed(package, upload) => match upload.chunk(package, chunk, now_ns)? {
                Take::Header { .. } => Ok(Vec::new()),
                Take::Image(image) => {
                    image.written();
                    Ok(Vec::new())
                }
            },
        }
    }

    fn upgrade(&mut self, now_ns: u64) -> Result<(), RpcError> {
        match self {
            Self::Image { .. } => Ok(()),
            Self::Signed(_, upload) => {
                upload.upgrade(now_ns)?;
                upload.armed();
                Ok(())
            }
        }
    }

    fn abort(&mut self) -> Result<(), RpcError> {
        match self {
            Self::Image { .. } => Err(RpcError::NotFound),
            Self::Signed(_, upload) => upload.abort(),
        }
    }

    fn emptied(&mut self) {
        match self {
            Self::Image { uploaded, .. } => *uploaded = 0,
            Self::Signed(_, upload) => *upload = Upload::new(),
        }
    }
}

/// A request no table entry answers, handed back for the board to answer with [`Sim::answer`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Call {
    /// The port that asked.
    pub port: usize,
    /// The id its answer carries.
    pub id: RpcRequestId,
    /// The name the table declares.
    pub name: &'static str,
    /// Its argument bytes.
    pub args: Vec<u8>,
}

/// An acquisition under way: when it began, the second its sample zero labels, and each stream's
/// samples since.
struct Acquiring {
    started_ns: u64,
    second: u32,
    taken: Vec<u64>,
}

/// A device, its streams and its ports, with the storage and acquisition a board would own.
pub struct Sim<B: Board<N>, const N: usize> {
    /// The device.
    pub device: Device<B>,
    /// Its streams, in id order.
    pub streams: Vec<DataStream<N>>,
    /// Its ports.
    pub ports: Vec<Port>,
    /// The configuration `dev.conf.save` or an autosave stored.
    pub stored: Option<Entries>,
    /// The firmware slot, or `None` for a device that refuses an upload.
    pub slot: Option<Slot>,
    /// The identity a firmware upgrade boots into, or `None` to keep this one.
    pub upgraded: Option<Identity>,
    /// The time reference a start labels its seconds by, or `None` for the device's own clock.
    pub reference: Option<Timeref>,
    /// Seconds the time reference runs ahead of the clock.
    pub offset: i64,
    acquiring: Option<Acquiring>,
    resume: u32,
    autostart: Option<u64>,
    written: u64,
}

impl<B: Board<N>, const N: usize> Sim<B, N> {
    /// A device come up at `now_ns` in `session` on `ports`, its own clock its time reference.
    pub fn new(
        identity: Identity,
        session: SessionId,
        table: &'static [RpcSpec],
        board: B,
        password: &str,
        now_ns: u64,
        mut ports: Vec<Port>,
    ) -> Self {
        let streams = board.streams();
        let mut written = 0;
        let device = Device::new(
            identity,
            session,
            table,
            board,
            password,
            now_ns,
            &streams[..],
            &mut Wires::new(&mut ports, &mut written, None),
        );
        let mut sim = Self {
            device,
            streams,
            ports,
            stored: None,
            slot: None,
            upgraded: None,
            reference: None,
            offset: 0,
            acquiring: None,
            resume: 0,
            autostart: None,
            written,
        };
        sim.arm(now_ns);
        sim
    }

    /// The device reads `packet` from `port`; a call no entry answers comes back.
    pub fn request(&mut self, port: usize, packet: &[u8], now_ns: u64) -> Option<Call> {
        let (view, _) = PacketView::parse_prefix(packet).ok()?;
        let actions = self.device.handle(
            &self.streams[..],
            Input::Packet(view),
            now_ns,
            &mut Wires::new(&mut self.ports, &mut self.written, Some(port)),
        );
        self.carry_out(port, actions, now_ns)
    }

    /// Answers `call` on the port that asked.
    pub fn answer(&mut self, call: &Call, result: Result<&[u8], RpcError>) {
        device::answer(
            Pending::local(call.id),
            result,
            &mut self.wires(Some(call.port)).reply_lane(),
        );
    }

    /// Sends what is due at `now_ns`, stores what an autosave hands back, and starts once
    /// `dev.autostart` has passed.
    pub fn tick(&mut self, now_ns: u64) {
        if let Some(image) = self.device.tick(
            now_ns,
            &mut Wires::new(&mut self.ports, &mut self.written, None),
        ) {
            self.stored = Some(image);
            self.device.saved();
        }
        if self.autostart.is_some_and(|at| now_ns >= at) {
            let _ = self.start(now_ns);
        }
    }

    /// Stream `stream` takes one sample.
    pub fn push(&mut self, stream: usize, sample: &[u8]) {
        self.took(stream, 1);
        let mut data = Wires::new(&mut self.ports, &mut self.written, None);
        self.streams[stream].push(sample, &mut data);
    }

    /// Stream `stream` loses `count` samples.
    pub fn skip(&mut self, stream: usize, count: u32) {
        self.took(stream, u64::from(count));
        let mut data = Wires::new(&mut self.ports, &mut self.written, None);
        self.streams[stream].skip(count, &mut data);
    }

    /// Every stream sends the packet it is filling.
    pub fn flush(&mut self) {
        let mut data = Wires::new(&mut self.ports, &mut self.written, None);
        self.streams
            .iter_mut()
            .for_each(|stream| stream.flush(&mut data));
    }

    /// Stream `stream` acquires with `params` from its next segment.
    pub fn retune(&mut self, stream: usize, params: Params) {
        let mut data = Wires::new(&mut self.ports, &mut self.written, None);
        self.streams[stream].retune(params, &mut data);
    }

    /// Stream `stream` begins a new segment.
    pub fn rollover(&mut self, stream: usize) {
        let mut data = Wires::new(&mut self.ports, &mut self.written, None);
        self.streams[stream].rollover(&mut data);
    }

    /// The time reference becomes `session`'s, `shift` seconds ahead of this one, for every
    /// stream's next segment.
    pub fn relabel(&mut self, shift: i64, session: SessionId, now_ns: u64) {
        let from = self.second(now_ns);
        self.offset += shift;
        let to = Timeref {
            session,
            ..self.timeref(now_ns)
        };
        self.reference = Some(to.clone());
        let mut data = Wires::new(&mut self.ports, &mut self.written, None);
        self.streams
            .iter_mut()
            .for_each(|stream| stream.relabel(from, to.clone(), &mut data));
    }

    /// Every stream starts at the clock's second, or the first after the last acquisition ended
    /// when that is later, so time never goes back; refused while acquiring.
    pub fn start(&mut self, now_ns: u64) -> Result<(), RpcError> {
        self.autostart = None;
        if self.acquiring.is_some() {
            return Err(RpcError::State);
        }
        let timeref = Timeref {
            start_time: self.second(now_ns).max(self.resume),
            ..self.timeref(now_ns)
        };
        let mut data = Wires::new(&mut self.ports, &mut self.written, None);
        self.streams.iter_mut().for_each(|stream| {
            stream
                .start(timeref.clone(), &mut data)
                .expect("a stopped stream starts")
        });
        self.acquiring = Some(Acquiring {
            started_ns: now_ns,
            second: timeref.start_time,
            taken: std::vec![0; self.streams.len()],
        });
        Ok(())
    }

    /// Every stream stops, each sending its staged rows and then its closing record.
    pub fn stop(&mut self) {
        self.autostart = None;
        if let Some(acquiring) = self.acquiring.take() {
            let seconds = acquiring
                .taken
                .iter()
                .zip(&self.streams)
                .map(|(taken, stream)| {
                    taken.div_ceil(u64::from(stream.current().params().rate.get()))
                })
                .max()
                .unwrap_or(0);
            self.resume = acquiring.second.saturating_add(seconds as u32);
        }
        let mut data = Wires::new(&mut self.ports, &mut self.written, None);
        self.streams
            .iter_mut()
            .for_each(|stream| stream.stop(&mut data));
    }

    /// When the acquisition under way began, `None` while stopped.
    pub fn started(&self) -> Option<u64> {
        self.acquiring
            .as_ref()
            .map(|acquiring| acquiring.started_ns)
    }

    /// Power-cycles into `session`: every lane empties, the board's entries go back to their
    /// defaults and then to what is stored, and the streams are built anew.
    pub fn reboot(&mut self, session: SessionId, now_ns: u64) {
        self.ports.iter_mut().for_each(|port| {
            [Lane::Control, Lane::Data, Lane::Event]
                .into_iter()
                .for_each(|lane| port.lane(lane).packets.clear())
        });
        self.boot(session, now_ns);
    }

    /// Comes up in `session` behind what the ports still hold, as a reset a request asked for
    /// does once each port has sent it.
    fn boot(&mut self, session: SessionId, now_ns: u64) {
        if let Some(identity) = self.upgraded.take() {
            self.device.identity = identity;
        }
        self.device
            .settings_mut()
            .entries(&mut |entry| match entry {
                Entry::Setting(cell) => {
                    let _ = cell.reset(&mut Reply::new());
                }
                Entry::Diagnostic(_) | Entry::Action(_) => {}
            });
        self.streams = self.device.settings().streams();
        self.acquiring = None;
        self.device.reboot(
            session,
            now_ns,
            &self.streams[..],
            &mut Wires::new(&mut self.ports, &mut self.written, None),
        );
        if let Some(stored) = self.stored.clone() {
            let actions = self.device.handle(
                &self.streams[..],
                Input::Loaded(None, Ok(&stored)),
                now_ns,
                &mut Wires::new(&mut self.ports, &mut self.written, None),
            );
            self.carry_out(0, actions, now_ns);
        }
        if let Some(slot) = &mut self.slot {
            slot.emptied();
        }
        self.arm(now_ns);
    }

    /// A host opens `port`, which hears a heartbeat and every record on their lanes.
    pub fn connect(&mut self, port: usize, now_ns: u64) {
        self.ports[port].open = true;
        let mut wires = Wires::new(&mut self.ports, &mut self.written, Some(port));
        self.device
            .connected(&self.streams[..], now_ns, &mut wires.only());
    }

    /// The host lets `port` go, and the port drops what it queued and discards what comes.
    pub fn disconnect(&mut self, port: usize) {
        let port = &mut self.ports[port];
        port.open = false;
        [Lane::Control, Lane::Data, Lane::Event]
            .into_iter()
            .for_each(|lane| port.lane(lane).packets.clear());
    }

    /// Sends `packet`, which a child sent up wearing its hop, on every port's lane [`lane_of`]
    /// names, as a hub forwards it.
    pub fn forward(&mut self, packet: &[u8]) {
        self.wires(None).write(lane(packet), None, packet);
    }

    fn wires(&mut self, asking: Option<usize>) -> Wires<'_> {
        Wires::new(&mut self.ports, &mut self.written, asking)
    }

    fn second(&self, now_ns: u64) -> u32 {
        ((now_ns / SECOND) as i64 + self.offset) as u32
    }

    fn timeref(&self, now_ns: u64) -> Timeref {
        let start_time = self.second(now_ns);
        match &self.reference {
            Some(reference) => Timeref {
                start_time,
                ..reference.clone()
            },
            None => Timeref::new(
                Epoch::UNIX,
                start_time,
                self.device.session,
                &self.device.identity.serial,
            )
            .expect("a serial that fits an identity fits a time reference"),
        }
    }

    fn took(&mut self, stream: usize, count: u64) {
        if let Some(acquiring) = &mut self.acquiring {
            acquiring.taken[stream] += count;
        }
    }

    fn arm(&mut self, now_ns: u64) {
        self.autostart = match self.device.autostart() {
            0 => None,
            seconds => Some(now_ns + u64::from(seconds) * SECOND),
        };
    }

    /// Does what the device left to its platform for a request from `port`.
    fn carry_out(&mut self, port: usize, actions: Actions<'_>, now_ns: u64) -> Option<Call> {
        let Actions {
            call,
            deferred,
            reboot,
            log,
            ..
        } = actions;
        if let Some((level, message)) = log {
            self.log(level, 0, message);
        }
        let call = call.map(|(pending, name, args)| Call {
            port,
            id: pending.id,
            name,
            args: args.to_vec(),
        });
        match deferred {
            Some((pending, Deferred::Flash(op))) => self.flash(port, pending.id, op, now_ns),
            Some((pending, Deferred::Acquire(request))) => {
                let answered = match request {
                    SyncRequest::Start => self.start(now_ns),
                    SyncRequest::Stop => {
                        self.stop();
                        Ok(())
                    }
                    SyncRequest::Restart => {
                        self.stop();
                        self.start(now_ns)
                    }
                };
                let reply = answered.map(|()| &[][..]);
                device::answer(pending, reply, &mut self.wires(Some(port)).reply_lane());
            }
            None => {}
        }
        if reboot {
            let session = SessionId::new(self.device.session.value().wrapping_add(1));
            self.boot(session, now_ns);
        }
        call
    }

    /// What the flash answers, in memory.
    fn flash(&mut self, port: usize, id: RpcRequestId, op: FlashOp, now_ns: u64) {
        let answered: Result<Vec<u8>, RpcError> = match op {
            FlashOp::ConfSave(image) => {
                self.stored = Some(image);
                Ok(Vec::new())
            }
            FlashOp::ConfReset => {
                self.stored = None;
                Ok(Vec::new())
            }
            FlashOp::ConfLoad => {
                let stored = self.stored.clone();
                let actions = self.device.handle(
                    &self.streams[..],
                    Input::Loaded(
                        Some(Pending::local(id)),
                        stored.as_deref().ok_or(RpcError::State),
                    ),
                    now_ns,
                    &mut Wires::new(&mut self.ports, &mut self.written, Some(port)),
                );
                self.carry_out(port, actions, now_ns);
                return;
            }
            FlashOp::Upload(chunk) => self
                .slot
                .as_mut()
                .map_or(Err(RpcError::State), |slot| slot.upload(&chunk, now_ns)),
            FlashOp::Upgrade => {
                let upgraded = self
                    .slot
                    .as_mut()
                    .map_or(Err(RpcError::State), |slot| slot.upgrade(now_ns));
                let reply = upgraded.map(|()| &[][..]);
                device::answer(
                    Pending::local(id),
                    reply,
                    &mut self.wires(Some(port)).reply_lane(),
                );
                if upgraded.is_ok() {
                    let session = SessionId::new(self.device.session.value().wrapping_add(1));
                    self.boot(session, now_ns);
                }
                return;
            }
            FlashOp::Abort => self.slot.as_mut().map_or(Err(RpcError::State), |slot| {
                slot.abort().map(|()| Vec::new())
            }),
        };
        device::answer(
            Pending::local(id),
            answered.as_deref().map_err(|&error| error),
            &mut self.wires(Some(port)).reply_lane(),
        );
    }

    /// Logs `message` with `data` on every port's event lane, as the device's threshold lets it.
    pub fn log(&mut self, level: LogLevel, data: u32, message: &str) {
        let mut wires = Wires::new(&mut self.ports, &mut self.written, None);
        self.device.log(level, data, message, &mut wires.events());
    }
}

/// Every port's lanes as the device writes into them, a reply going only to the port that asked.
struct Wires<'a> {
    ports: &'a mut [Port],
    written: &'a mut u64,
    asking: Option<usize>,
}

impl<'a> Wires<'a> {
    fn new(ports: &'a mut [Port], written: &'a mut u64, asking: Option<usize>) -> Self {
        Self {
            ports,
            written,
            asking,
        }
    }

    fn write(&mut self, lane: Lane, to: Option<usize>, packet: &[u8]) {
        *self.written += 1;
        let written = *self.written;
        self.ports
            .iter_mut()
            .enumerate()
            .filter(|(at, _)| to.is_none_or(|to| to == *at))
            .for_each(|(_, port)| port.send(lane, written, packet));
    }

    fn reply_lane(&mut self) -> Wire<'_, 'a> {
        Wire {
            wires: self,
            route: Route::Reply,
        }
    }

    fn events(&mut self) -> Wire<'_, 'a> {
        Wire {
            wires: self,
            route: Route::Event,
        }
    }

    fn only(&mut self) -> Wire<'_, 'a> {
        Wire {
            wires: self,
            route: Route::Only,
        }
    }
}

impl Lanes for Wires<'_> {
    fn reply(&mut self, packet: &[u8]) {
        if let Some(asking) = self.asking {
            self.write(Lane::Control, Some(asking), packet);
        }
    }

    fn announce(&mut self, packet: &[u8]) {
        self.write(Lane::Control, None, packet);
    }

    fn event(&mut self, packet: &[u8]) {
        self.write(Lane::Event, None, packet);
    }
}

impl Sink for Wires<'_> {
    fn send(&mut self, packet: &[u8]) {
        self.write(Lane::Data, None, packet);
    }
}

/// Where a [`Wire`] sends: the asking port's replies, every port's events, or the asking port's
/// lane [`lane_of`] names.
#[derive(Clone, Copy)]
enum Route {
    Reply,
    Event,
    Only,
}

/// One route of [`Wires`] as a [`Sink`].
struct Wire<'w, 'a> {
    wires: &'w mut Wires<'a>,
    route: Route,
}

impl Sink for Wire<'_, '_> {
    fn send(&mut self, packet: &[u8]) {
        match self.route {
            Route::Reply => self.wires.reply(packet),
            Route::Event => self.wires.event(packet),
            Route::Only => {
                let asking = self.wires.asking;
                self.wires.write(lane(packet), asking, packet);
            }
        }
    }
}

/// The lane [`lane_of`] names for `packet`, the event lane, which keeps no order, for any other.
fn lane(packet: &[u8]) -> Lane {
    PacketView::parse_prefix(packet)
        .ok()
        .and_then(|(view, _)| lane_of(view))
        .unwrap_or(Lane::Event)
}
