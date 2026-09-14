//! The table of requests a hub is waiting for answers to.
//!
//! Every request that leaves for a child carries an id minted here, so no id a
//! host chose ever reaches the child-facing wire and an answer names its entry
//! by lookup rather than by convention. The id holds a slot and a generation:
//! an answer to a slot that has since been reused fails the generation check
//! instead of reaching whoever holds it now.

use twinleaf_proto::rpc::Answer;
use twinleaf_proto::{DeviceRoute, RpcRequestId};

/// How long an entry waits for its answer, as tl-chibi's remap does.
pub const DEADLINE_NS: u64 = 10_000_000_000;

/// Every slot holds a request still waiting for its answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Full;

/// Who is waiting for the answer to a request the hub sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin<P> {
    /// A host asked, and its answer travels back the way its request came.
    Forwarded {
        /// The id the host chose, put back before the answer goes up.
        id: RpcRequestId,
        /// The hops the request carried, which its answer retraces.
        route: DeviceRoute,
    },
    /// The hub asked, for a purpose of its own. The answer stops here.
    Internal(P),
}

/// What the table refused or dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CallCounters {
    /// Answers that named no live entry, and were dropped rather than sent on
    /// wearing an id no host would recognize.
    pub unmatched: u32,
    /// Requests refused because every slot was live.
    pub exhausted: u32,
    /// Entries that reached their deadline with no answer.
    pub expired: u32,
}

/// A request that has gone out and not been answered.
struct Live<P> {
    origin: Origin<P>,
    /// The port it went to. An answer from anywhere else is not it.
    port: u8,
    deadline_ns: u64,
}

struct Slot<P> {
    /// The generation of the live entry, and the one a late answer to a freed
    /// slot is checked against.
    generation: u16,
    live: Option<Live<P>>,
}

impl<P> Slot<P> {
    const fn new() -> Self {
        Self {
            generation: 0,
            live: None,
        }
    }
}

/// The outstanding requests of a hub, indexed by the ids it minted: `N` slots,
/// and `P` what an entry of the hub's own carries.
pub struct Calls<P, const N: usize> {
    slots: [Slot<P>; N],
    /// Where the next allocation starts looking, so slots are handed out in
    /// rotation rather than one being reused while the rest sit idle.
    next: usize,
    unmatched: u32,
    exhausted: u32,
    expired: u32,
}

impl<P, const N: usize> Calls<P, N> {
    /// Bits of a minted id naming the slot; the rest carry the generation.
    const SLOT_BITS: u32 = N.trailing_zeros();
    const SLOT_MASK: u16 = N as u16 - 1;
    /// Distinct generations a slot cycles through, so reusing a whole id takes
    /// `N * GENERATIONS` = 65536 requests.
    const GENERATIONS: u16 = (1u32 << (16 - N.trailing_zeros())) as u16;
    /// Checked wherever a table is built, so a bad `N` fails the build.
    const SHAPE: () = assert!(
        N.is_power_of_two() && N > 1,
        "call slots must be a power of two above one"
    );

    /// A table with nothing outstanding. `N` is a power of two above one, so
    /// the id's low bits are the slot and the rest is the generation.
    pub const fn new() -> Self {
        let () = Self::SHAPE;
        Self {
            slots: [const { Slot::new() }; N],
            next: 0,
            unmatched: 0,
            exhausted: 0,
            expired: 0,
        }
    }

    /// Take a slot for a host's request going out to `port`, and return the id
    /// to send it under in place of `id`.
    pub fn forward(
        &mut self,
        port: u8,
        id: RpcRequestId,
        route: DeviceRoute,
        now_ns: u64,
    ) -> Result<RpcRequestId, Full> {
        self.mint(port, Origin::Forwarded { id, route }, now_ns)
    }

    /// Take a slot for a request of the hub's own, and return the id to send
    /// it under.
    pub fn call(&mut self, port: u8, purpose: P, now_ns: u64) -> Result<RpcRequestId, Full> {
        self.mint(port, Origin::Internal(purpose), now_ns)
    }

    /// Take the entry an answer from `port` names, if one is live under the id
    /// and generation it carries. Anything else is counted and dropped.
    pub fn answered(&mut self, port: u8, answer: Answer<'_>) -> Option<Origin<P>> {
        let claimed = self.claim(port, answer.req_id());
        if claimed.is_none() {
            self.unmatched = self.unmatched.saturating_add(1);
        }
        claimed
    }

    /// Give up on the request sent to `port` under `id`; a late answer to it
    /// is then unmatched.
    pub fn release(&mut self, port: u8, id: RpcRequestId) -> Option<Origin<P>> {
        self.claim(port, id)
    }

    fn claim(&mut self, port: u8, id: RpcRequestId) -> Option<Origin<P>> {
        let slot = &mut self.slots[Self::slot_of(id)];
        (slot.generation == Self::generation_of(id))
            .then(|| slot.live.take_if(|live| live.port == port))
            .flatten()
            .map(|live| live.origin)
    }

    /// Free one entry whose deadline has passed. Call until it returns `None`.
    pub fn expired(&mut self, now_ns: u64) -> Option<Origin<P>> {
        let origin = self
            .slots
            .iter_mut()
            .find_map(|slot| slot.live.take_if(|live| live.deadline_ns <= now_ns))
            .map(|live| live.origin)?;
        self.expired = self.expired.saturating_add(1);
        Some(origin)
    }

    /// When [`Calls::expired`] next has something to report.
    pub fn deadline_ns(&self) -> Option<u64> {
        self.slots
            .iter()
            .filter_map(|slot| slot.live.as_ref())
            .map(|live| live.deadline_ns)
            .min()
    }

    /// What the table refused or dropped.
    pub fn counters(&self) -> CallCounters {
        CallCounters {
            unmatched: self.unmatched,
            exhausted: self.exhausted,
            expired: self.expired,
        }
    }

    fn mint(&mut self, port: u8, origin: Origin<P>, now_ns: u64) -> Result<RpcRequestId, Full> {
        let free = (0..N)
            .map(|step| (self.next + step) % N)
            .find(|&index| self.slots[index].live.is_none());
        let Some(index) = free else {
            self.exhausted = self.exhausted.saturating_add(1);
            return Err(Full);
        };
        self.next = (index + 1) % N;
        let slot = &mut self.slots[index];
        slot.generation = (slot.generation + 1) % Self::GENERATIONS;
        slot.live = Some(Live {
            origin,
            port,
            deadline_ns: now_ns.saturating_add(DEADLINE_NS),
        });
        Ok(Self::minted_id(index, slot.generation))
    }

    /// The id that goes on the wire for a slot: generation above, index below.
    const fn minted_id(index: usize, generation: u16) -> RpcRequestId {
        RpcRequestId::new((generation << Self::SLOT_BITS) | (index as u16 & Self::SLOT_MASK))
    }

    const fn slot_of(id: RpcRequestId) -> usize {
        (id.value() & Self::SLOT_MASK) as usize
    }

    const fn generation_of(id: RpcRequestId) -> u16 {
        id.value() >> Self::SLOT_BITS
    }
}

impl<P, const N: usize> Default for Calls<P, N> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use twinleaf_proto::packet::PacketType;
    use twinleaf_proto::rpc::{self, RpcError};

    /// What a hub asks a child for on its own behalf.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Ask {
        Name,
        Rate,
    }

    const NOW: u64 = 1_000_000_000;

    fn table() -> Calls<Ask, 4> {
        Calls::new()
    }

    fn route(hops: &[u8]) -> DeviceRoute {
        DeviceRoute::from_hops(hops).unwrap()
    }

    fn reply(id: RpcRequestId, value: &[u8]) -> Vec<u8> {
        let mut buf = [0u8; 64];
        let len = rpc::write_reply(&mut buf, id, value).unwrap();
        buf[..len].to_vec()
    }

    fn answer(payload: &[u8]) -> Answer<'_> {
        Answer::parse(PacketType::RPC_REP, &payload[4..]).unwrap()
    }

    fn forwarded(table: &mut Calls<Ask, 4>, port: u8, id: u16) -> RpcRequestId {
        table
            .forward(port, RpcRequestId::new(id), route(&[port]), NOW)
            .unwrap()
    }

    #[test]
    fn a_forwarded_request_is_answered_under_the_hosts_own_id() {
        let mut table = table();
        let minted = forwarded(&mut table, 1, 7);
        assert_ne!(minted, RpcRequestId::new(7));

        let packet = reply(minted, b"tio-test");
        assert_eq!(
            table.answered(1, answer(&packet)),
            Some(Origin::Forwarded {
                id: RpcRequestId::new(7),
                route: route(&[1])
            })
        );
        assert_eq!(table.answered(1, answer(&packet)), None);
        assert_eq!(table.counters().unmatched, 1);
    }

    #[test]
    fn a_hub_call_keeps_its_purpose_and_goes_no_further() {
        let mut table = table();
        let minted = table.call(2, Ask::Name, NOW).unwrap();
        let packet = reply(minted, b"hub");
        assert_eq!(
            table.answered(2, answer(&packet)),
            Some(Origin::Internal(Ask::Name))
        );
    }

    /// The id names a port as well as a slot: the same number from another
    /// child is not the answer that was waited for.
    #[test]
    fn an_answer_from_another_port_matches_nothing() {
        let mut table = table();
        let minted = forwarded(&mut table, 1, 7);
        let packet = reply(minted, b"");
        assert_eq!(table.answered(3, answer(&packet)), None);
        assert_eq!(table.counters().unmatched, 1);
        assert!(table.answered(1, answer(&packet)).is_some());
    }

    /// A late answer lands on a slot somebody else holds; the generation in
    /// the id is what tells them apart.
    #[test]
    fn an_answer_to_a_reused_slot_is_dropped() {
        let mut table = table();
        let stale = forwarded(&mut table, 0, 1);
        table.answered(0, answer(&reply(stale, b"")));

        let reused = (0..4)
            .map(|index| forwarded(&mut table, 0, index + 2))
            .find(|id| Calls::<Ask, 4>::slot_of(*id) == Calls::<Ask, 4>::slot_of(stale))
            .expect("the rotation comes back to the slot");
        assert_ne!(reused, stale);
        assert_eq!(table.answered(0, answer(&reply(stale, b""))), None);
        assert!(table.answered(0, answer(&reply(reused, b""))).is_some());
    }

    #[test]
    fn an_error_answers_its_entry_like_a_reply() {
        let mut table = table();
        let minted = table.call(0, Ask::Rate, NOW).unwrap();
        let mut buf = [0u8; 16];
        let len = rpc::write_error(&mut buf, minted, RpcError::NotFound).unwrap();
        let answer = Answer::parse(PacketType::RPC_ERROR, &buf[4..len]).unwrap();
        assert_eq!(table.answered(0, answer), Some(Origin::Internal(Ask::Rate)));
    }

    #[test]
    fn a_full_table_refuses_and_counts_every_further_request() {
        let mut table = table();
        for index in 0..4 {
            forwarded(&mut table, 0, index);
        }
        assert_eq!(
            table.forward(0, RpcRequestId::new(9), route(&[0]), NOW),
            Err(Full)
        );
        assert_eq!(table.call(0, Ask::Name, NOW), Err(Full));
        assert_eq!(table.counters().exhausted, 2);
    }

    #[test]
    fn entries_expire_one_at_a_time_at_their_deadline() {
        let mut table = table();
        forwarded(&mut table, 1, 5);
        table.call(2, Ask::Name, NOW + 1).unwrap();
        assert_eq!(table.deadline_ns(), Some(NOW + DEADLINE_NS));

        assert_eq!(table.expired(NOW + DEADLINE_NS - 1), None);
        assert_eq!(
            table.expired(NOW + DEADLINE_NS),
            Some(Origin::Forwarded {
                id: RpcRequestId::new(5),
                route: route(&[1])
            })
        );
        assert_eq!(table.expired(NOW + DEADLINE_NS), None);
        assert_eq!(table.deadline_ns(), Some(NOW + 1 + DEADLINE_NS));

        assert_eq!(
            table.expired(NOW + 1 + DEADLINE_NS),
            Some(Origin::Internal(Ask::Name))
        );
        assert_eq!(table.deadline_ns(), None);
        assert_eq!(table.counters().expired, 2);
    }

    /// An expired entry frees its slot, and the answer that finally arrives
    /// finds nothing: it was answered with a timeout long ago.
    #[test]
    fn an_expired_entry_leaves_its_slot_free() {
        let mut table = table();
        let minted = forwarded(&mut table, 1, 5);
        table.expired(NOW + DEADLINE_NS);
        assert_eq!(table.answered(1, answer(&reply(minted, b""))), None);
        assert!(table
            .forward(1, RpcRequestId::new(6), route(&[1]), NOW)
            .is_ok());
    }
}
