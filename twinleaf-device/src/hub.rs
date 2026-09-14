//! Child ports: what a hub routes, what it hears from them, and what it asks.
//!
//! The hub owns no buffers and no transport. Packets from the host go down to
//! the port their route names, packets from a child go up wearing the port
//! they came from, and an RPC crossing either way passes through [`Calls`] so
//! its answer can be matched. The runtime supplies the two sinks: [`Sink`] for
//! the host and [`PortSink`] for the children.
//!
//! The downward SYNC packet is not produced here. The hub role's
//! `Synchronizer` is the one emitter, and [`Hub::announce`] writes what it
//! yields to every present port.

use twinleaf_proto::heartbeat::Heartbeat;
use twinleaf_proto::packet::{Header, Packet, PacketType, PacketView};
use twinleaf_proto::route::push_hop;
use twinleaf_proto::rpc::{self, Answer, Method, Request, RpcError};
use twinleaf_proto::{DeviceRoute, RpcRequestId, SessionId};

use crate::calls::{CallCounters, Calls, Full, Origin};
use crate::device::HEARTBEAT_INTERVAL;
use crate::sync::Announce;
use crate::Sink;

/// Silence after which a child is unplugged: two heartbeats' worth.
pub const UNPLUG_NS: u64 = 2 * HEARTBEAT_INTERVAL;

/// Requests a hub has in flight at once unless sized otherwise, as tl-chibi's
/// remap has.
pub const CALL_SLOTS: usize = 32;

/// Where a hub's downward packets go, one child port at a time.
pub trait PortSink {
    /// Send one complete packet to a child port.
    fn send(&mut self, port: u8, packet: &[u8]);
}

/// Where what a hub noticed goes, as [`Sink`] is where its packets go. Taken
/// one at a time rather than returned: an answer borrows the packet it came in.
pub trait Events<P> {
    /// One thing the hub noticed.
    fn event(&mut self, event: Event<'_, P>);
}

/// Whether a child has been heard from, and which boot of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Presence {
    /// No child is answering on this port.
    Absent,
    /// A child announced `session` and was last heard at `last_heard_ns`.
    Present {
        /// Which boot of the child this is.
        session: SessionId,
        /// When its last packet arrived.
        last_heard_ns: u64,
    },
}

/// One thing that reached the hub.
#[derive(Debug)]
pub enum Input<'p> {
    /// A packet from the host, routed at one of the children.
    FromHost(PacketView<'p>),
    /// A packet a child sent up.
    FromChild {
        /// The port it arrived on.
        port: u8,
        /// The packet, as the child wrote it.
        packet: PacketView<'p>,
    },
    /// Nothing arrived; expire what is due.
    Tick,
}

/// Something the hub noticed while handling an input.
#[derive(Debug, PartialEq, Eq)]
pub enum Event<'a, P> {
    /// A child is answering on this port.
    Plugged(u8),
    /// A child stopped answering, or rebooted into another session.
    Unplugged(u8),
    /// The answer to a call of the hub's own.
    Answered(P, Result<&'a [u8], RpcError>),
}

/// Why the hub could not ask a child.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallError {
    /// Nothing is plugged into that port.
    Absent,
    /// Every call slot holds a request still waiting.
    Full,
    /// The method name and arguments do not fit one packet.
    TooLong,
}

/// What the hub refused or dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HubCounters {
    /// Host packets dropped: no route, or a port with nothing on it.
    pub dropped_down: u32,
    /// Child packets dropped: an unknown port, an unmatched answer, or a route
    /// with no room for another hop.
    pub dropped_up: u32,
    /// What the call table refused or dropped.
    pub calls: CallCounters,
}

/// A device's child ports and the requests it is waiting on. Port `n` is the
/// route `/n`, `P` is what a call of the hub's own carries, and `SLOTS` (a
/// power of two) is how many requests can be waiting.
pub struct Hub<P, const PORTS: usize, const SLOTS: usize = CALL_SLOTS> {
    ports: [Presence; PORTS],
    calls: Calls<P, SLOTS>,
    dropped_down: u32,
    dropped_up: u32,
}

impl<P, const PORTS: usize, const SLOTS: usize> Hub<P, PORTS, SLOTS> {
    /// A hub with nothing plugged into it.
    pub const fn new() -> Self {
        Self {
            ports: [Presence::Absent; PORTS],
            calls: Calls::new(),
            dropped_down: 0,
            dropped_up: 0,
        }
    }

    /// Act on one input.
    pub fn handle(
        &mut self,
        input: Input<'_>,
        now_ns: u64,
        up: &mut impl Sink,
        down: &mut impl PortSink,
        events: &mut impl Events<P>,
    ) {
        match input {
            Input::FromHost(packet) => self.host_packet(packet, now_ns, up, down),
            Input::FromChild { port, packet } => {
                self.child_packet(port, packet, now_ns, up, events)
            }
            Input::Tick => self.tick(now_ns, up, events),
        }
    }

    /// Ask a child a question of the hub's own, whose answer comes back as
    /// [`Event::Answered`] carrying `purpose`, and return the id it went under.
    pub fn call(
        &mut self,
        port: u8,
        method: &str,
        args: &[u8],
        purpose: P,
        now_ns: u64,
        down: &mut impl PortSink,
    ) -> Result<RpcRequestId, CallError> {
        let mut buf = [0u8; Packet::MAX_SIZE];
        let (id, len) = self.request(port, method, args, purpose, now_ns, &mut buf)?;
        down.send(port, &buf[..len]);
        Ok(id)
    }

    /// Compose a call of the hub's own into `buf`, for a caller that sends it
    /// on a wire of its own; the id and the packet length come back.
    pub fn request(
        &mut self,
        port: u8,
        method: &str,
        args: &[u8],
        purpose: P,
        now_ns: u64,
        buf: &mut [u8; Packet::MAX_SIZE],
    ) -> Result<(RpcRequestId, usize), CallError> {
        if !self.present(port) {
            return Err(CallError::Absent);
        }
        let method = Method::ByName(method.as_bytes());
        if rpc::request_payload_len(method, args).is_none_or(|len| len > Packet::MAX_PAYLOAD) {
            return Err(CallError::TooLong);
        }
        let id = self
            .calls
            .call(port, purpose, now_ns)
            .map_err(|Full| CallError::Full)?;
        let len = rpc::write_request(buf, id, method, args)
            .expect("a request that fits a payload fits a packet");
        Ok((id, len))
    }

    /// Give up on a call of the hub's own, returning what it carried, or
    /// `None` if it was already answered, timed out, or cancelled.
    pub fn cancel(&mut self, port: u8, id: RpcRequestId) -> Option<P> {
        match self.calls.release(port, id)? {
            Origin::Internal(purpose) => Some(purpose),
            Origin::Forwarded { .. } => None,
        }
    }

    /// Send one second's time reference to every present child.
    pub fn announce(&self, announce: &Announce, down: &mut impl PortSink) {
        let mut buf = [0u8; Packet::MAX_SIZE];
        let len = announce
            .timeref
            .timeref()
            .write_with_pad(&mut buf, announce.pad)
            .expect("a time reference fits a packet");
        self.present_ports()
            .for_each(|port| down.send(port, &buf[..len]));
    }

    /// What is plugged into a port.
    pub fn presence(&self, port: u8) -> Presence {
        self.ports
            .get(usize::from(port))
            .copied()
            .unwrap_or(Presence::Absent)
    }

    /// Every port with a child on it.
    pub fn present_ports(&self) -> impl Iterator<Item = u8> + '_ {
        self.ports
            .iter()
            .enumerate()
            .filter(|(_, presence)| matches!(presence, Presence::Present { .. }))
            .map(|(port, _)| port as u8)
    }

    /// When [`Input::Tick`] next has something to do.
    pub fn deadline_ns(&self) -> Option<u64> {
        let unplug = self
            .ports
            .iter()
            .filter_map(|presence| match presence {
                Presence::Absent => None,
                Presence::Present { last_heard_ns, .. } => Some(last_heard_ns + UNPLUG_NS),
            })
            .min();
        [self.calls.deadline_ns(), unplug]
            .into_iter()
            .flatten()
            .min()
    }

    /// What the hub refused or dropped.
    pub fn counters(&self) -> HubCounters {
        HubCounters {
            dropped_down: self.dropped_down,
            dropped_up: self.dropped_up,
            calls: self.calls.counters(),
        }
    }

    /// Route one host packet to the port its route names, taking a request
    /// through the call table so its answer can be matched.
    fn host_packet(
        &mut self,
        view: PacketView<'_>,
        now_ns: u64,
        up: &mut impl Sink,
        down: &mut impl PortSink,
    ) {
        let Some((&port, hops)) = view.routing.split_last() else {
            self.dropped_down = self.dropped_down.saturating_add(1);
            return;
        };
        if !self.present(port) {
            self.dropped_down = self.dropped_down.saturating_add(1);
            return;
        }
        let request = (view.header.ptype == PacketType::RPC_REQ)
            .then(|| Request::parse(view.payload))
            .flatten();
        let route = route_of(view.routing);
        let minted = match request {
            None => None,
            Some(request) => match self.calls.forward(port, request.id, route, now_ns) {
                Ok(minted) => Some(minted),
                Err(Full) => return answer_up(up, route, request.id, RpcError::NoBufs),
            },
        };
        let mut buf = [0u8; Packet::MAX_SIZE];
        let len = relay(&mut buf, view, hops, minted);
        down.send(port, &buf[..len]);
    }

    /// Take one packet from a child: what it says about the child, and where
    /// it goes next.
    fn child_packet(
        &mut self,
        port: u8,
        view: PacketView<'_>,
        now_ns: u64,
        up: &mut impl Sink,
        events: &mut impl Events<P>,
    ) {
        if usize::from(port) >= PORTS {
            self.dropped_up = self.dropped_up.saturating_add(1);
            return;
        }
        if let (PacketType::HEARTBEAT, true, Some(session)) = (
            view.header.ptype,
            view.routing.is_empty(),
            Heartbeat::parse(view.payload).and_then(|beat| beat.session()),
        ) {
            self.observe(port, session, now_ns, events);
        }
        if let Presence::Present { last_heard_ns, .. } = &mut self.ports[usize::from(port)] {
            *last_heard_ns = now_ns;
        }
        let mut hops = [0u8; DeviceRoute::MAX_HOPS];
        let Some(answer) = Answer::parse(view.header.ptype, view.payload) else {
            let hop = view.routing.len();
            if hop == DeviceRoute::MAX_HOPS {
                self.dropped_up = self.dropped_up.saturating_add(1);
                return;
            }
            hops[..hop].copy_from_slice(view.routing);
            hops[hop] = port;
            send_up(view, &hops[..hop + 1], None, up);
            return;
        };
        let (id, route) = match self.calls.answered(port, answer) {
            None => {
                self.dropped_up = self.dropped_up.saturating_add(1);
                return;
            }
            Some(Origin::Internal(purpose)) => {
                events.event(Event::Answered(purpose, value_of(answer)));
                return;
            }
            Some(Origin::Forwarded { id, route }) => (id, route),
        };
        let len = route
            .write_wire(&mut hops)
            .expect("a route fits its own hops");
        send_up(view, &hops[..len], Some(id), up);
    }

    /// Unplug the ports that have gone quiet and time out the calls nobody
    /// answered.
    fn tick(&mut self, now_ns: u64, up: &mut impl Sink, events: &mut impl Events<P>) {
        for port in 0..PORTS {
            let Presence::Present { last_heard_ns, .. } = self.ports[port] else {
                continue;
            };
            if now_ns.saturating_sub(last_heard_ns) >= UNPLUG_NS {
                self.ports[port] = Presence::Absent;
                events.event(Event::Unplugged(port as u8));
            }
        }
        while let Some(origin) = self.calls.expired(now_ns) {
            match origin {
                Origin::Forwarded { id, route } => answer_up(up, route, id, RpcError::Timeout),
                Origin::Internal(purpose) => {
                    events.event(Event::Answered(purpose, Err(RpcError::Timeout)))
                }
            }
        }
    }

    /// A heartbeat from a child: which boot of it this is, and whether that is
    /// news.
    fn observe(&mut self, port: u8, session: SessionId, now_ns: u64, events: &mut impl Events<P>) {
        match self.ports[usize::from(port)] {
            Presence::Absent => events.event(Event::Plugged(port)),
            Presence::Present { session: known, .. } if known == session => {}
            Presence::Present { .. } => {
                events.event(Event::Unplugged(port));
                events.event(Event::Plugged(port));
            }
        }
        self.ports[usize::from(port)] = Presence::Present {
            session,
            last_heard_ns: now_ns,
        };
    }

    fn present(&self, port: u8) -> bool {
        matches!(self.presence(port), Presence::Present { .. })
    }
}

impl<P, const PORTS: usize, const SLOTS: usize> Default for Hub<P, PORTS, SLOTS> {
    fn default() -> Self {
        Self::new()
    }
}

/// Send one packet up to the host, with `routing` in place of the hops it
/// carried and, for an answer, the id its requester used.
fn send_up(view: PacketView<'_>, routing: &[u8], id: Option<RpcRequestId>, up: &mut impl Sink) {
    let mut buf = [0u8; Packet::MAX_SIZE];
    let len = relay(&mut buf, view, routing, id);
    up.send(&buf[..len]);
}

/// Rebuild `view` with `routing` in place of the hops it carried, and, for an
/// RPC, `id` in place of the id it named. Returns the packet's length.
fn relay(
    buf: &mut [u8; Packet::MAX_SIZE],
    view: PacketView<'_>,
    routing: &[u8],
    id: Option<RpcRequestId>,
) -> usize {
    let header = Header {
        routing_size: routing.len() as u8,
        ..view.header
    };
    header.write((&mut buf[..Header::SIZE]).try_into().unwrap());
    let payload = header.payload_range();
    buf[payload.clone()].copy_from_slice(view.payload);
    buf[payload.end..payload.end + routing.len()].copy_from_slice(routing);
    if let Some(id) = id {
        rpc::set_req_id(&mut buf[payload], id);
    }
    header.packet_len()
}

/// Answer a request the hub could not see through, along the route it came.
fn answer_up(up: &mut impl Sink, route: DeviceRoute, id: RpcRequestId, error: RpcError) {
    let mut buf = [0u8; Packet::MAX_SIZE];
    let mut len = rpc::write_error(&mut buf, id, error).expect("an error fits a packet");
    let mut hops = [0u8; DeviceRoute::MAX_HOPS];
    let written = route
        .write_wire(&mut hops)
        .expect("a route fits its own hops");
    for &hop in &hops[..written] {
        len = push_hop(&mut buf, hop).expect("a request's route fits its answer");
    }
    up.send(&buf[..len]);
}

/// The route a packet's hops name. A header caps them at eight.
fn route_of(routing: &[u8]) -> DeviceRoute {
    DeviceRoute::from_wire(routing).expect("a packet's hops fit a route")
}

/// What an answer says, as the caller that asked reads it.
fn value_of(answer: Answer<'_>) -> Result<&[u8], RpcError> {
    match answer {
        Answer::Reply(reply) => Ok(reply.value),
        Answer::Error(error) => Err(error.error()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::{Reference, ReferenceIdentity};
    use twinleaf_proto::sync::Epoch;

    /// What a hub asks a child for on its own behalf.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Ask {
        Name,
    }

    const NOW: u64 = 1_000_000_000;

    #[derive(Default)]
    struct Sent(Vec<Vec<u8>>);

    impl Sink for Sent {
        fn send(&mut self, packet: &[u8]) {
            self.0.push(packet.to_vec());
        }
    }

    impl Sent {
        fn views(&self) -> Vec<PacketView<'_>> {
            self.0
                .iter()
                .map(|packet| PacketView::parse_prefix(packet).unwrap().0)
                .collect()
        }

        fn one(&self) -> PacketView<'_> {
            let [view] = self.views()[..] else {
                panic!("one packet");
            };
            view
        }
    }

    #[derive(Default)]
    struct Down(Vec<(u8, Vec<u8>)>);

    impl PortSink for Down {
        fn send(&mut self, port: u8, packet: &[u8]) {
            self.0.push((port, packet.to_vec()));
        }
    }

    impl Down {
        fn one(&self) -> (u8, PacketView<'_>) {
            let [(port, ref packet)] = self.0[..] else {
                panic!("one packet");
            };
            (port, PacketView::parse_prefix(packet).unwrap().0)
        }
    }

    /// An event as a test keeps it, with the answer's bytes copied out.
    #[derive(Debug, PartialEq, Eq)]
    enum Seen {
        Plugged(u8),
        Unplugged(u8),
        Answered(Ask, Result<Vec<u8>, RpcError>),
    }

    #[derive(Default)]
    struct Log(Vec<Seen>);

    impl Events<Ask> for Log {
        fn event(&mut self, event: Event<'_, Ask>) {
            self.0.push(match event {
                Event::Plugged(port) => Seen::Plugged(port),
                Event::Unplugged(port) => Seen::Unplugged(port),
                Event::Answered(purpose, answer) => {
                    Seen::Answered(purpose, answer.map(<[u8]>::to_vec))
                }
            });
        }
    }

    /// Everything one call of the hub produced.
    #[derive(Default)]
    struct Out {
        up: Sent,
        down: Down,
        log: Log,
    }

    fn deliver<const SLOTS: usize>(
        hub: &mut Hub<Ask, 4, SLOTS>,
        input: Input<'_>,
        now_ns: u64,
    ) -> Out {
        let mut out = Out::default();
        hub.handle(input, now_ns, &mut out.up, &mut out.down, &mut out.log);
        out
    }

    fn view(packet: &[u8]) -> PacketView<'_> {
        PacketView::parse_prefix(packet).unwrap().0
    }

    fn heartbeat(session: u32) -> Vec<u8> {
        let mut buf = [0u8; 32];
        let len = Heartbeat::Session(SessionId::new(session))
            .write(&mut buf)
            .unwrap();
        buf[..len].to_vec()
    }

    /// A request for `hops`, which are in wire order: the next hop last.
    fn request(id: u16, name: &[u8], hops: &[u8]) -> Vec<u8> {
        let mut buf = [0u8; Packet::MAX_SIZE];
        let mut len =
            rpc::write_request(&mut buf, RpcRequestId::new(id), Method::ByName(name), &[]).unwrap();
        for &hop in hops {
            len = push_hop(&mut buf, hop).unwrap();
        }
        buf[..len].to_vec()
    }

    fn reply(id: RpcRequestId, value: &[u8]) -> Vec<u8> {
        let mut buf = [0u8; Packet::MAX_SIZE];
        let len = rpc::write_reply(&mut buf, id, value).unwrap();
        buf[..len].to_vec()
    }

    fn answered(view: PacketView<'_>) -> Answer<'_> {
        Answer::parse(view.header.ptype, view.payload).expect("an answer")
    }

    /// A hub with a child on port 1.
    fn hub() -> Hub<Ask, 4> {
        let mut hub = Hub::new();
        let beat = heartbeat(7);
        let out = deliver(&mut hub, child(1, &beat), NOW);
        assert_eq!(out.log.0, [Seen::Plugged(1)]);
        hub
    }

    fn child<'p>(port: u8, packet: &'p [u8]) -> Input<'p> {
        Input::FromChild {
            port,
            packet: view(packet),
        }
    }

    /// The id the hub minted for the request it sent down.
    fn minted(down: &Down) -> RpcRequestId {
        let (_, view) = down.one();
        Request::parse(view.payload).expect("a request").id
    }

    #[test]
    fn a_host_packet_reaches_the_port_its_route_names_with_that_hop_gone() {
        let mut hub = hub();
        let packet = request(7, b"dev.name", &[3, 1]);
        let out = deliver(&mut hub, Input::FromHost(view(&packet)), NOW);

        let (port, view) = out.down.one();
        assert_eq!(port, 1);
        assert_eq!(view.routing, [3]);
        assert!(out.up.0.is_empty());
    }

    #[test]
    fn a_packet_for_an_absent_port_is_dropped_and_counted() {
        let mut hub = hub();
        for hops in [&[2][..], &[9][..], &[][..]] {
            let packet = request(7, b"dev.name", hops);
            let out = deliver(&mut hub, Input::FromHost(view(&packet)), NOW);
            assert!(out.down.0.is_empty());
            assert!(out.up.0.is_empty());
        }
        assert_eq!(hub.counters().dropped_down, 3);
    }

    #[test]
    fn a_forwarded_request_goes_out_remapped_and_comes_back_restored() {
        let mut hub = hub();
        let packet = request(7, b"dev.name", &[1]);
        let out = deliver(&mut hub, Input::FromHost(view(&packet)), NOW);
        let minted = minted(&out.down);
        assert_ne!(minted, RpcRequestId::new(7));

        let answer = reply(minted, b"tio-test");
        let out = deliver(&mut hub, child(1, &answer), NOW);
        let view = out.up.one();
        assert_eq!(view.routing, [1]);
        let Answer::Reply(reply) = answered(view) else {
            panic!("a reply");
        };
        assert_eq!(
            (reply.req_id, reply.value),
            (RpcRequestId::new(7), &b"tio-test"[..])
        );
    }

    #[test]
    fn a_child_packet_goes_up_wearing_the_port_it_came_from() {
        let mut hub = hub();
        let beat = heartbeat(7);
        let out = deliver(&mut hub, child(1, &beat), NOW);
        let view = out.up.one();
        assert_eq!(view.header.ptype, PacketType::HEARTBEAT);
        assert_eq!(view.routing, [1]);
        assert!(out.log.0.is_empty());
    }

    #[test]
    fn an_answer_to_no_request_of_the_hubs_is_dropped_and_counted() {
        let mut hub = hub();
        let answer = reply(RpcRequestId::new(3), b"whose?");
        let out = deliver(&mut hub, child(1, &answer), NOW);
        assert!(out.up.0.is_empty());
        assert_eq!(hub.counters().dropped_up, 1);
        assert_eq!(hub.counters().calls.unmatched, 1);
    }

    #[test]
    fn a_new_session_on_a_port_is_a_replug() {
        let mut hub = hub();
        let same = heartbeat(7);
        assert!(deliver(&mut hub, child(1, &same), NOW).log.0.is_empty());

        let rebooted = heartbeat(8);
        let out = deliver(&mut hub, child(1, &rebooted), NOW);
        assert_eq!(out.log.0, [Seen::Unplugged(1), Seen::Plugged(1)]);
        assert_eq!(
            hub.presence(1),
            Presence::Present {
                session: SessionId::new(8),
                last_heard_ns: NOW
            }
        );
    }

    #[test]
    fn silence_unplugs_a_port_and_any_packet_keeps_it() {
        let mut hub = hub();
        assert_eq!(hub.deadline_ns(), Some(NOW + UNPLUG_NS));

        let out = deliver(&mut hub, Input::Tick, NOW + UNPLUG_NS - 1);
        assert!(out.log.0.is_empty());

        let answer = reply(RpcRequestId::new(3), b"whose?");
        deliver(&mut hub, child(1, &answer), NOW + UNPLUG_NS - 1);
        assert_eq!(hub.deadline_ns(), Some(NOW + 2 * UNPLUG_NS - 1));
        let out = deliver(&mut hub, Input::Tick, NOW + UNPLUG_NS);
        assert!(out.log.0.is_empty());

        let out = deliver(&mut hub, Input::Tick, NOW + 2 * UNPLUG_NS - 1);
        assert_eq!(out.log.0, [Seen::Unplugged(1)]);
        assert_eq!(hub.presence(1), Presence::Absent);
        assert_eq!(hub.deadline_ns(), None);
    }

    #[test]
    fn a_call_nobody_answers_times_out_for_the_host_and_for_the_hub() {
        let mut hub = hub();
        let packet = request(7, b"dev.name", &[1]);
        deliver(&mut hub, Input::FromHost(view(&packet)), NOW);
        hub.call(1, "dev.name", &[], Ask::Name, NOW, &mut Down::default())
            .unwrap();

        let out = deliver(&mut hub, Input::Tick, NOW + crate::calls::DEADLINE_NS);
        let view = out.up.one();
        assert_eq!(view.routing, [1]);
        let Answer::Error(error) = answered(view) else {
            panic!("an error");
        };
        assert_eq!(
            (error.req_id, error.error()),
            (RpcRequestId::new(7), RpcError::Timeout)
        );
        assert_eq!(
            out.log.0,
            [
                Seen::Unplugged(1),
                Seen::Answered(Ask::Name, Err(RpcError::Timeout))
            ]
        );
        assert_eq!(hub.counters().calls.expired, 2);
    }

    #[test]
    fn a_full_table_refuses_the_host_with_nobufs_and_the_hub_with_full() {
        let mut hub = hub();
        for id in 0..CALL_SLOTS as u16 {
            let packet = request(id, b"dev.name", &[1]);
            deliver(&mut hub, Input::FromHost(view(&packet)), NOW);
        }

        let packet = request(99, b"dev.name", &[1]);
        let out = deliver(&mut hub, Input::FromHost(view(&packet)), NOW);
        assert!(out.down.0.is_empty());
        let view = out.up.one();
        assert_eq!(view.routing, [1]);
        let Answer::Error(error) = answered(view) else {
            panic!("an error");
        };
        assert_eq!(
            (error.req_id, error.error()),
            (RpcRequestId::new(99), RpcError::NoBufs)
        );

        assert_eq!(
            hub.call(1, "dev.name", &[], Ask::Name, NOW, &mut Down::default()),
            Err(CallError::Full)
        );
    }

    #[test]
    fn a_hub_sized_to_two_calls_is_full_at_the_third() {
        let mut hub: Hub<Ask, 4, 2> = Hub::new();
        deliver(&mut hub, child(1, &heartbeat(7)), NOW);
        let mut buf = [0u8; Packet::MAX_SIZE];
        let outcomes: Vec<_> = (0..3)
            .map(|_| {
                hub.request(1, "dev.name", &[], Ask::Name, NOW, &mut buf)
                    .map(|(_, len)| len > 0)
            })
            .collect();
        assert_eq!(outcomes, [Ok(true), Ok(true), Err(CallError::Full)]);
    }

    #[test]
    fn a_composed_request_is_the_packet_a_call_sends() {
        let mut composed = hub();
        let mut sent = hub();
        let mut buf = [0u8; Packet::MAX_SIZE];
        let (id, len) = composed
            .request(1, "dev.name", &[], Ask::Name, NOW, &mut buf)
            .unwrap();
        let mut down = Down::default();
        assert_eq!(
            sent.call(1, "dev.name", &[], Ask::Name, NOW, &mut down),
            Ok(id)
        );
        assert_eq!(down.0, [(1, buf[..len].to_vec())]);
    }

    #[test]
    fn a_cancelled_call_gives_back_its_purpose_and_its_answer_is_unmatched() {
        let mut hub = hub();
        let mut down = Down::default();
        let id = hub
            .call(1, "dev.name", &[], Ask::Name, NOW, &mut down)
            .unwrap();
        assert_eq!(id, minted(&down));
        assert_eq!(hub.cancel(1, id), Some(Ask::Name));
        assert_eq!(hub.cancel(1, id), None);

        let answer = reply(id, b"tio-hub");
        let out = deliver(&mut hub, child(1, &answer), NOW);
        assert!(out.log.0.is_empty());
        assert_eq!(hub.counters().calls.unmatched, 1);
        assert_eq!(hub.deadline_ns(), Some(NOW + UNPLUG_NS));
    }

    #[test]
    fn a_hub_call_is_answered_to_its_purpose_and_goes_no_further() {
        let mut hub = hub();
        assert_eq!(
            hub.call(2, "dev.name", &[], Ask::Name, NOW, &mut Down::default()),
            Err(CallError::Absent)
        );

        let mut down = Down::default();
        hub.call(1, "dev.name", &[], Ask::Name, NOW, &mut down)
            .unwrap();
        let (port, view) = down.one();
        assert_eq!((port, view.header.ptype), (1, PacketType::RPC_REQ));

        let answer = reply(minted(&down), b"tio-hub");
        let out = deliver(&mut hub, child(1, &answer), NOW);
        assert!(out.up.0.is_empty());
        assert_eq!(
            out.log.0,
            [Seen::Answered(Ask::Name, Ok(b"tio-hub".to_vec()))]
        );
    }

    #[test]
    fn an_announcement_reaches_every_present_port() {
        let mut hub = hub();
        let beat = heartbeat(11);
        deliver(&mut hub, child(3, &beat), NOW);

        let announce = Announce {
            timeref: Reference {
                identity: ReferenceIdentity::new(Epoch::UNIX, SessionId::new(5), b"HUB-SIM"),
                second: 1_800_000_000,
            },
            pad: 0xC1,
        };
        let mut down = Down::default();
        hub.announce(&announce, &mut down);

        let ports: Vec<u8> = down.0.iter().map(|(port, _)| *port).collect();
        assert_eq!(ports, [1, 3]);
        let view = PacketView::parse_prefix(&down.0[0].1).unwrap().0;
        assert_eq!(view.header.ptype, PacketType::SYNC);
        assert_eq!(
            twinleaf_proto::sync::Timeref::parse(view.payload),
            Some(announce.timeref.timeref())
        );
        assert_eq!(twinleaf_proto::sync::Timeref::pad(view.payload), 0xC1);
    }
}
