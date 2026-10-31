//! The [`DeviceEvent`]s a device's worker follows, and the [`NamedRoute`] of
//! route discovery.

use crate::proto::DeviceRoute;
use crate::proto::SessionId;

/// A fact about one device, which its worker follows to keep the device's
/// RPC table and settings current.
#[derive(Debug, Clone)]
pub(crate) enum DeviceEvent {
    /// A heartbeat from the device.
    Heartbeat {
        /// The session the device reports, when its heartbeat carries one.
        session_id: Option<SessionId>,
        /// The sequence number of the device's last setting announcement, when
        /// its heartbeat carries one.
        settings: Option<u32>,
    },
    /// A new RPC table hash. `Some(hash)` comes from the `rpc.hash`
    /// announcement firmware whose table changes with `dev.priv` sends.
    /// `None` follows a reconnection and asks for a refresh.
    NewHash(Option<u32>),
    /// A setting changed, as the device announced it. A legacy `rpc.hash`
    /// announcement arrives here as well as in [`NewHash`](Self::NewHash).
    /// An announcement whose name is not UTF-8 is dropped.
    Setting {
        /// The setting's RPC name.
        name: String,
        /// The device's `settings.version` once this change is counted, or
        /// `None` from firmware that does not number its announcements.
        seq: Option<u32>,
        /// Its new value, the bytes that RPC would reply.
        reply: Vec<u8>,
    },
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
