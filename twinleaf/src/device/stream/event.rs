//! The vocabulary a stream publishes: one [`Event`] type, scoped by subject.

use super::subscription::Scope;
use crate::data::DeviceMetadataSnapshot;
use crate::device::RpcMethod;
use crate::tio::proto::{self, DeviceRoute};
use twinleaf_proto::SessionId;

/// What a link is doing. It belongs to a transport rather than to any one
/// route, so it concerns a whole subtree: everything on a direct connection,
/// one mount behind a `tio proxy --mount` fan-in.
///
/// These events arrive via both direct serial and tio-proxy connections. A
/// direct serial connection closes after `SensorDisconnected`; a tio-proxy TCP
/// connection can remain open, making the status event the disconnection
/// signal shared by both transports.
#[derive(Debug, Clone, Copy)]
pub enum LinkEvent {
    /// Connection status changed.
    Status(proto::ProxyStatus),
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
    Heartbeat { session_id: Option<SessionId> },
    /// The device answered `dev.metadata` with `NotFound`: its firmware cannot
    /// describe its streams, so nothing on this route will ever decode.
    MetadataUnavailable,
    /// Another client completed an RPC that can invalidate a cached value.
    RpcInvalidated(RpcMethod),
    /// `Some(hash)` comes from a settings packet; `None` requests a refresh
    /// after reconnection.
    NewHash(Option<u32>),
}

/// One fact from the connection, scoped by subject. Every view subscribes to
/// the same vocabulary; only how much of it reaches the view changes.
#[derive(Debug, Clone)]
pub enum Event {
    /// A link, heard by the views its subtree touches.
    Link {
        subtree: DeviceRoute,
        event: LinkEvent,
    },
    /// The population, heard by the views that cover the route.
    Tree {
        route: DeviceRoute,
        event: TreeEvent,
    },
    /// One device, heard by the views that cover it.
    Device {
        route: DeviceRoute,
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
    pub route: DeviceRoute,
    pub name: Option<String>,
}
