//! The [`Event`]s a stream publishes, and the [`NamedRoute`] of route discovery.

use super::subscription::Scope;
use crate::data::DeviceMetadataSnapshot;
use crate::proto::DeviceRoute;
use crate::proto::SessionId;
use crate::tio::packet;

/// A change in a link, the transport that serves a subtree.
///
/// The subtree is the root, or one mount of what the proxy serves, as
/// `tio proxy --mount` or a selection in `tio list` lays it out. A
/// [`SensorDisconnected`](packet::LinkStatus::SensorDisconnected) status
/// signals a lost device on every transport. The connection to the proxy
/// stays open.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum LinkEvent {
    /// The link's status changed.
    Status(packet::LinkStatus),
    /// The connection's inlet overflowed and packets were lost for every
    /// subscriber. Continuity boundaries in the samples mark the gap, and
    /// the stream goes on.
    InputOverrun,
}

/// A fact about one device. Link facts arrive as [`LinkEvent`]s and are not
/// repeated per device.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum DeviceEvent {
    /// The device's complete metadata, sent when it first completes and
    /// again whenever it changes.
    Metadata(DeviceMetadataSnapshot),
    /// A heartbeat from the device.
    Heartbeat {
        /// The session the device reports, when its heartbeat carries one.
        session_id: Option<SessionId>,
        /// The sequence number of the device's last setting announcement, when
        /// its heartbeat carries one.
        settings: Option<u32>,
    },
    /// The device answered `dev.metadata` with `NotFound`. Its firmware
    /// cannot describe its streams, and no samples from this route decode.
    MetadataUnavailable,
    /// A new RPC table hash. `Some(hash)` comes from a settings packet.
    /// `None` follows a reconnection and asks for a refresh.
    NewHash(Option<u32>),
    /// A setting changed, as the device announced it. The `rpc.hash`
    /// announcement arrives here as well as in [`NewHash`](Self::NewHash).
    /// An announcement whose name is not UTF-8 is dropped.
    Setting {
        /// The setting's RPC name.
        name: String,
        /// Its new value, the bytes that RPC would reply.
        reply: Vec<u8>,
    },
}

/// One fact from the connection, scoped by its subject. A view receives each
/// event whose subject overlaps its scope.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Event {
    /// A link, heard by the views its subtree touches.
    Link {
        /// Every route the link serves.
        subtree: DeviceRoute,
        /// What happened to it.
        event: LinkEvent,
    },
    /// The first packet from a route, heard by the views that cover it.
    RouteDiscovered {
        /// The route that answered.
        route: DeviceRoute,
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
    /// Returns the region this fact concerns. That is the whole subtree for
    /// a link fact and the one route for any other.
    pub(crate) fn scope(&self) -> Scope {
        match self {
            Event::Link { subtree, .. } => Scope::subtree(*subtree),
            Event::RouteDiscovered { route } | Event::Device { route, .. } => Scope::point(*route),
        }
    }
}

/// A discovered route and its device's `dev.name`, from
/// [`DeviceTree::named_routes`](crate::device::DeviceTree::named_routes).
#[derive(Debug, Clone)]
pub struct NamedRoute {
    /// The route discovered.
    pub route: DeviceRoute,
    /// Its `dev.name`, when the device answered.
    pub name: Option<String>,
}
