//! The vocabulary a stream publishes: one [`Event`] type, scoped by subject.

use super::subscription::Scope;
use crate::data::DeviceMetadataSnapshot;
use crate::proto::DeviceRoute;
use crate::proto::SessionId;
use crate::tio::packet::{self, RpcMethod};

/// What a link is doing. It belongs to the transport, so it concerns a whole
/// subtree: a direct connection, or one mount behind `tio proxy --mount`.
/// A `SensorDisconnected` status is the disconnection signal on every
/// transport, since a proxy's TCP link can stay open after it.
#[derive(Debug, Clone, Copy)]
pub enum LinkEvent {
    /// Connection status changed.
    Status(packet::ProxyStatus),
    /// The engine's own inlet overflowed: packets were lost for every
    /// subscriber, continuity boundaries mark the gap, and the stream goes on.
    InputOverrun,
}

/// What the population of devices behind the link is doing.
#[derive(Debug, Clone, Copy)]
pub enum TreeEvent {
    /// First packet received from the route.
    RouteDiscovered,
}

/// What one device is doing. Shallow by construction: link facts are never
/// repeated per device.
#[derive(Debug, Clone)]
pub enum DeviceEvent {
    /// The parser has complete metadata for this device, and it has changed
    /// since the last time it was published.
    Metadata(DeviceMetadataSnapshot),
    /// Device heartbeat, including the session id for the standard format.
    Heartbeat {
        /// The session the device reports, when its heartbeat carries one.
        session_id: Option<SessionId>,
    },
    /// The device answered `dev.metadata` with `NotFound`: its firmware cannot
    /// describe its streams, so nothing on this route will ever decode.
    MetadataUnavailable,
    /// Another client completed an RPC that can invalidate a cached value.
    RpcInvalidated(RpcMethod),
    /// `Some(hash)` comes from a settings packet. `None` requests a refresh
    /// after reconnection.
    NewHash(Option<u32>),
}

/// One fact from the connection, scoped by subject. Every view subscribes to
/// the same vocabulary. Only how much of it reaches the view changes.
#[derive(Debug, Clone)]
pub enum Event {
    /// A link, heard by the views its subtree touches.
    Link {
        /// Every route the link serves.
        subtree: DeviceRoute,
        /// What happened to it.
        event: LinkEvent,
    },
    /// The population, heard by the views that cover the route.
    Tree {
        /// The route concerned.
        route: DeviceRoute,
        /// What happened at it.
        event: TreeEvent,
    },
    /// One device, heard by the views that cover it.
    Device {
        /// The device's route.
        route: DeviceRoute,
        /// What the device did.
        event: DeviceEvent,
    },
}

impl Event {
    /// The region this fact concerns, which delivery intersects with each
    /// view's scope: a whole subtree for link facts, one route otherwise.
    pub(crate) fn scope(&self) -> Scope {
        match self {
            Event::Link { subtree, .. } => Scope::subtree(*subtree),
            Event::Tree { route, .. } | Event::Device { route, .. } => Scope::point(*route),
        }
    }
}

/// A discovered route paired with its device's `dev.name` (`None` if the device
/// didn't answer). Returned by [`DeviceTree::named_routes`](crate::device::DeviceTree::named_routes).
#[derive(Debug, Clone)]
pub struct NamedRoute {
    /// The route discovered.
    pub route: DeviceRoute,
    /// Its `dev.name`, when the device answered.
    pub name: Option<String>,
}
