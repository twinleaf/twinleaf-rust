//! The schedule around the parser's `dev.metadata` protocol: which routes may
//! be asked now, when a failed one is asked again, and where replies land.

use crate::data::{MetadataQuery, PacketParser};
use crate::proto::data as wire;
use crate::proto::rpc as wire_rpc;
use crate::proto::DeviceRoute;
use crate::tio::proxy;
use crossbeam::channel;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Wait before the first metadata retry, and the ceiling it doubles up to.
const RETRY_FIRST: Duration = Duration::from_millis(250);
const RETRY_MAX: Duration = Duration::from_secs(8);

/// Why a route is not being asked for metadata right now.
enum Schedule {
    /// The last query failed. Ask again once `until` passes.
    Backoff { until: Instant, delay: Duration },
    /// A backed-off route that has come due: eligible again, and remembering
    /// the delay the next failure doubles.
    Due { delay: Duration },
    /// The device answered `NotFound`: its firmware has no `dev.metadata`.
    Unsupported,
}

/// What a finished `dev.metadata` call left behind.
pub(crate) enum Completed {
    /// The reply reached the parser, which may now describe more of its route.
    Applied,
    /// The route's firmware has no `dev.metadata`, learned by this call.
    Unsupported,
    /// The call failed, or answered a query already overtaken. The route keeps
    /// whatever schedule it had.
    BackedOff,
}

/// Metadata discovery for one RPC capability: the backoff schedule per route
/// and the channel finished calls resolve on.
pub(crate) struct Discovery {
    endpoint: proxy::RpcEndpoint,
    schedule: HashMap<DeviceRoute, Schedule>,
    resolve: channel::Sender<(MetadataQuery, proxy::RawCallResult)>,
    replies: channel::Receiver<(MetadataQuery, proxy::RawCallResult)>,
}

impl Discovery {
    pub(crate) fn new(endpoint: proxy::RpcEndpoint) -> Discovery {
        let (resolve, replies) = channel::unbounded();
        Discovery {
            endpoint,
            schedule: HashMap::new(),
            resolve,
            replies,
        }
    }

    /// The finished calls, to receive from or select on.
    pub(crate) fn replies(&self) -> &channel::Receiver<(MetadataQuery, proxy::RawCallResult)> {
        &self.replies
    }

    /// Which of `routes` may be asked now, expiring the backoffs that bring
    /// them due so no elapsed deadline is left to wake the caller again.
    pub(crate) fn due(
        &mut self,
        routes: impl IntoIterator<Item = DeviceRoute>,
        now: Instant,
    ) -> Vec<DeviceRoute> {
        let due: Vec<_> = routes
            .into_iter()
            .filter(|route| match self.schedule.get(route) {
                None | Some(Schedule::Due { .. }) => true,
                Some(Schedule::Backoff { until, .. }) => *until <= now,
                Some(Schedule::Unsupported) => false,
            })
            .collect();
        for route in &due {
            if let Some(Schedule::Backoff { delay, .. }) = self.schedule.get(route) {
                self.schedule
                    .insert(*route, Schedule::Due { delay: *delay });
            }
        }
        due
    }

    /// Ask `route` for whatever metadata `parser` still wants.
    pub(crate) fn submit(&mut self, parser: &mut PacketParser, route: DeviceRoute) {
        for query in parser.take_metadata_queries_for(route) {
            let resolve = self.resolve.clone();
            let args = query.args();
            self.endpoint.submit_with(
                route,
                wire::METADATA_RPC_METHOD,
                &args,
                Box::new(move |result| {
                    let _ = resolve.send((query, result));
                }),
            );
        }
    }

    /// Apply a finished call to `parser`, or decide when to ask again.
    pub(crate) fn complete(
        &mut self,
        parser: &mut PacketParser,
        query: MetadataQuery,
        result: proxy::RawCallResult,
    ) -> Completed {
        let route = query.route;
        match result {
            Ok(reply) => {
                parser.apply_metadata_reply(query, &reply);
                self.schedule.remove(&route);
                Completed::Applied
            }
            Err(proxy::RawCallError::Device {
                error: wire_rpc::RpcError::NotFound,
                ..
            }) => self.give_up(parser, query),
            Err(proxy::RawCallError::Device { .. })
            | Err(proxy::RawCallError::InvalidRoute(_))
            | Err(proxy::RawCallError::RequestNotSubmitted)
            | Err(proxy::RawCallError::Timeout)
            | Err(proxy::RawCallError::DeviceDisconnected)
            | Err(proxy::RawCallError::ProxyClosed) => self.back_off(parser, query),
        }
    }

    /// The earliest moment a backed-off route wants to be asked again.
    pub(crate) fn next_retry(&self) -> Option<Instant> {
        self.schedule
            .values()
            .filter_map(|state| match state {
                Schedule::Backoff { until, .. } => Some(*until),
                Schedule::Due { .. } | Schedule::Unsupported => None,
            })
            .min()
    }

    /// Drop what this session taught us about `subtree`, so the next session
    /// there is asked again from the start.
    pub(crate) fn forget(&mut self, subtree: DeviceRoute) {
        self.schedule
            .retain(|route, _| !route.starts_with(&subtree));
    }

    /// Hold a route off for a growing delay, unless a reset or a new session
    /// already overtook its query.
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

    /// Stop asking a route whose firmware does not implement `dev.metadata`.
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

    fn discovery() -> Discovery {
        let (endpoint, _commands, _worker) =
            proxy::RpcEndpoint::test_pair(DeviceRoute::root(), DeviceRoute::MAX_HOPS);
        Discovery::new(endpoint)
    }

    /// An elapsed deadline that stayed in the schedule would make every wait
    /// return at once, spinning for as long as the route stayed uncovered.
    #[test]
    fn a_route_that_comes_due_stops_asking_the_caller_to_wake() {
        let mut parser = PacketParser::new(DeviceRoute::root(), false);
        let mut discovery = discovery();
        let route = DeviceRoute::root();
        discovery.schedule.insert(
            route,
            Schedule::Backoff {
                until: Instant::now() - Duration::from_millis(1),
                delay: RETRY_FIRST,
            },
        );

        assert_eq!(discovery.due([route], Instant::now()), [route]);
        assert!(
            discovery.next_retry().is_none(),
            "an elapsed deadline never wakes the caller again"
        );
        let query = parser
            .take_metadata_queries_for(route)
            .pop()
            .expect("the route is still bootstrapping");
        discovery.back_off(&mut parser, query);
        assert!(
            matches!(discovery.schedule.get(&route), Some(Schedule::Backoff { delay, .. }) if *delay == RETRY_FIRST * 2),
            "coming due keeps the delay the next failure doubles"
        );
    }
}
