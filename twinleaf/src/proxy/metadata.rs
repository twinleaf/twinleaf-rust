//! The [`MetadataSchedule`] of `dev.metadata` queries. It decides which
//! routes may be asked now and when a failed one is asked again, and it
//! collects the replies.

use crate::data::{MetadataQuery, PacketParser};
use crate::device::rpc::{CallError, RpcErrorPayload};
use crate::device::session::{Handle, RawCallResult};
use crate::proto::data as wire;
use crate::proto::rpc as wire_rpc;
use crate::proto::DeviceRoute;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::Instant;

/// The delay before the first metadata retry, and the most it doubles up to.
pub(super) const RETRY_FIRST: Duration = Duration::from_millis(250);
const RETRY_MAX: Duration = Duration::from_secs(8);

/// Why a route is not being asked for metadata right now.
enum Schedule {
    /// The last query failed. The route is asked again once `until` passes.
    Backoff { until: Instant, delay: Duration },
    /// A backed-off route that has come due. It may be asked again, and
    /// `delay` is what the next failure doubles.
    Due { delay: Duration },
    /// The device answered `NotFound`. Its firmware has no `dev.metadata`.
    Unsupported,
}

/// What a finished `dev.metadata` call left behind.
pub(super) enum Completed {
    /// The reply reached the parser, which may now describe more of its route.
    Applied,
    /// The route's firmware has no `dev.metadata`, learned by this call.
    Unsupported,
    /// The call failed, or answered a query already overtaken. The route keeps
    /// whatever schedule it had.
    BackedOff,
}

/// When each route is asked for metadata, through one view's session handle.
/// It holds each route's backoff and the channel finished calls arrive on.
pub(super) struct MetadataSchedule {
    handle: Handle,
    schedule: HashMap<DeviceRoute, Schedule>,
    resolve: flume::Sender<(MetadataQuery, RawCallResult)>,
    replies: flume::Receiver<(MetadataQuery, RawCallResult)>,
}

impl MetadataSchedule {
    /// Creates a schedule that asks through `handle`.
    pub(super) fn new(handle: Handle) -> MetadataSchedule {
        let (resolve, replies) = flume::unbounded();
        MetadataSchedule {
            handle,
            schedule: HashMap::new(),
            resolve,
            replies,
        }
    }

    /// Returns the channel of finished calls, to receive from or select on.
    pub(super) fn replies(&self) -> &flume::Receiver<(MetadataQuery, RawCallResult)> {
        &self.replies
    }

    /// Returns which of `routes` may be asked now. Marks each backoff that
    /// has passed as due, and [`next_retry`](Self::next_retry) then skips it.
    pub(super) fn due(
        &mut self,
        routes: impl IntoIterator<Item = DeviceRoute>,
        now: Instant,
    ) -> Vec<DeviceRoute> {
        let due = |state: Option<&mut Schedule>| match state {
            None | Some(Schedule::Due { .. }) => true,
            Some(state @ Schedule::Backoff { .. }) => match *state {
                Schedule::Backoff { until, delay } if until <= now => {
                    *state = Schedule::Due { delay };
                    true
                }
                _ => false,
            },
            Some(Schedule::Unsupported) => false,
        };
        let routes = routes.into_iter();
        routes
            .filter(|route| due(self.schedule.get_mut(route)))
            .collect()
    }

    /// Asks `route` for whatever metadata `parser` still wants.
    pub(super) fn submit(&mut self, parser: &mut PacketParser, route: DeviceRoute) {
        for query in parser.take_metadata_queries_for(route) {
            let resolve = self.resolve.clone();
            let args = query.args();
            self.handle.submit_with(
                route,
                wire::METADATA_RPC_METHOD,
                &args,
                Box::new(move |result| {
                    let _ = resolve.send((query, result));
                }),
            );
        }
    }

    /// Applies a finished call to `parser`, or schedules when to ask again.
    pub(super) fn complete(
        &mut self,
        parser: &mut PacketParser,
        query: MetadataQuery,
        result: RawCallResult,
    ) -> Completed {
        let route = query.route;
        match result {
            Ok(reply) => {
                parser.apply_metadata_reply(query, &reply);
                self.schedule.remove(&route);
                Completed::Applied
            }
            Err(CallError::DeviceError(RpcErrorPayload {
                error: wire_rpc::RpcError::NotFound,
                ..
            })) => self.give_up(parser, query),
            Err(_) => self.back_off(parser, query),
        }
    }

    /// Returns the earliest moment one of `routes` that is backed off is
    /// due again. The caller passes the routes it asks, the same ones it
    /// passes to [`due`](Self::due), so a backoff of a route it does not ask
    /// never wakes it.
    pub(super) fn next_retry(
        &self,
        routes: impl IntoIterator<Item = DeviceRoute>,
    ) -> Option<Instant> {
        let until = |route| match self.schedule.get(&route) {
            Some(Schedule::Backoff { until, .. }) => Some(*until),
            Some(Schedule::Due { .. } | Schedule::Unsupported) | None => None,
        };
        routes.into_iter().filter_map(until).min()
    }

    /// Clears the schedule of every route in `subtree`. The next session
    /// there is asked from the start.
    pub(super) fn forget(&mut self, subtree: DeviceRoute) {
        self.schedule
            .retain(|route, _| !route.starts_with(&subtree));
    }

    /// Holds a route off for a doubling delay, unless a reset or a new
    /// session already overtook its query.
    fn back_off(&mut self, parser: &mut PacketParser, query: MetadataQuery) -> Completed {
        let route = query.route;
        if !parser.fail_metadata_query(query) {
            return Completed::BackedOff;
        }
        let delay = match self.schedule.get(&route) {
            None => RETRY_FIRST,
            Some(Schedule::Backoff { delay, .. }) | Some(Schedule::Due { delay }) => {
                (*delay * 2).min(RETRY_MAX)
            }
            Some(Schedule::Unsupported) => return Completed::BackedOff,
        };
        self.schedule.insert(
            route,
            Schedule::Backoff {
                until: Instant::now() + delay,
                delay,
            },
        );
        Completed::BackedOff
    }

    /// Stops asking a route whose firmware does not implement `dev.metadata`.
    fn give_up(&mut self, parser: &mut PacketParser, query: MetadataQuery) -> Completed {
        let route = query.route;
        if !parser.fail_metadata_query(query) {
            return Completed::BackedOff;
        }
        if matches!(
            self.schedule.insert(route, Schedule::Unsupported),
            Some(Schedule::Unsupported)
        ) {
            return Completed::BackedOff;
        }
        log::warn!("{route} has no dev.metadata; its streams cannot be decoded");
        Completed::Unsupported
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schedule() -> MetadataSchedule {
        let (session, _proxy) = crate::device::session::Session::test_pair();
        MetadataSchedule::new(Handle::root(session))
    }

    /// An elapsed deadline that stayed in the schedule would make every wait
    /// return at once, spinning for as long as the route stayed uncovered.
    #[test]
    fn a_route_that_comes_due_stops_asking_the_caller_to_wake() {
        let mut parser = PacketParser::new(false);
        let mut schedule = schedule();
        let route = DeviceRoute::root();
        schedule.schedule.insert(
            route,
            Schedule::Backoff {
                until: Instant::now() - Duration::from_millis(1),
                delay: RETRY_FIRST,
            },
        );

        assert_eq!(schedule.due([route], Instant::now()), [route]);
        assert!(
            schedule.next_retry([route]).is_none(),
            "an elapsed deadline never wakes the caller again"
        );
        let query = parser
            .take_metadata_queries_for(route)
            .pop()
            .expect("the route is still bootstrapping");
        schedule.back_off(&mut parser, query);
        assert!(
            matches!(schedule.schedule.get(&route), Some(Schedule::Backoff { delay, .. }) if *delay == RETRY_FIRST * 2),
            "coming due keeps the delay the next failure doubles"
        );
    }
}
