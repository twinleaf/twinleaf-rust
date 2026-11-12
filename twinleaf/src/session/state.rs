//! The state of each device that a connection keeps current. That is whether
//! the device is there, its metadata, its RPC table and its settings.
//!
//! Each piece is a whole value. A [`Device`](crate::Device) returns the
//! current one, or a [`Receiver`](crate::Receiver) that yields the current
//! one first and then each new one. A reader that falls behind skips to the
//! newest, so it never lags.

use super::rpc::{RpcRegistry, RpcRegistryError, RpcValue, RpcValueDecodeError, RpcValueTypeExt};
use crate::data::DeviceMetadataSnapshot;
use crate::proto::SessionId;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

/// Whether a device is there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Presence {
    /// Nothing has been heard from the device yet, or its link reconnected
    /// and it has not spoken since.
    Connecting,
    /// The device is sending. It stays alive until its link loses it, which
    /// the proxy does once the link goes quiet for 1 s, or 3 s over TCP.
    Alive {
        /// The session its last heartbeat reported, when one did.
        session: Option<SessionId>,
    },
    /// The proxy lost the device, as the link that serves it lost it or
    /// stopped, or as the device was silent for 3 s.
    Lost {
        /// When the proxy lost it.
        since: Instant,
    },
}

/// Why the proxy lost a device.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum LostCause {
    /// The device went quiet on its link, or the link's transport closed.
    Disconnected,
    /// The proxy stopped.
    Stopped,
    /// The link's transport did not open.
    FailedToConnect,
    /// The proxy gave up reconnecting the link.
    FailedToReconnect,
    /// The device was silent for 3 s while its link stayed up.
    Quiet,
}

impl LostCause {
    /// The cause as a watch's update carries it.
    pub(crate) fn code(self) -> u8 {
        match self {
            LostCause::Disconnected => 0,
            LostCause::Stopped => 1,
            LostCause::FailedToConnect => 2,
            LostCause::FailedToReconnect => 3,
            LostCause::Quiet => 4,
        }
    }

    /// The cause a watch's update carries as `code`, or `None` for a code
    /// this build does not know.
    pub(crate) fn from_code(code: u8) -> Option<LostCause> {
        let known = [
            LostCause::Disconnected,
            LostCause::Stopped,
            LostCause::FailedToConnect,
            LostCause::FailedToReconnect,
            LostCause::Quiet,
        ];
        known.into_iter().find(|cause| cause.code() == code)
    }
}

/// The link that serves a device, as the proxy says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkState {
    /// The URL the proxy opened the link at.
    pub url: String,
    /// Why the proxy last lost the device, or `None` if it has not.
    pub lost: Option<LostCause>,
}

/// How far a device's metadata has been collected.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum MetadataState {
    /// The metadata is still being collected, or was dropped with the
    /// device's last session.
    Pending,
    /// The device's complete metadata.
    Ready(DeviceMetadataSnapshot),
    /// The device has no `dev.metadata`. Its firmware cannot describe its
    /// streams, and no samples from it decode.
    Unsupported,
}

/// A device's RPC table as last loaded, or why it could not be loaded.
pub type RegistryState = Result<Arc<RpcRegistry>, RpcRegistryError>;

/// One setting a device announced, in the order it announced them, with the
/// RPC table it was recorded under.
///
/// The table is the one in force for the device when the change was
/// recorded, and the change keeps it after the device moves on to another.
/// A table is never changed in place, so [`value`](Self::value) decodes the
/// reply as the device meant it, without waiting for anything.
///
/// A table is in force from its load until the device reconnects, the
/// device reports another session, or the device announces another
/// `rpc.hash`, and the next table is in force once it loads. A device
/// reconnects as its link reconnects, and as the proxy hears it again after
/// losing it. Only a load that started after the device last reconnected
/// counts, so a table read before a reconnect is never in force after it.
/// No table is in force before the first loads or while a load fails. A
/// change recorded while no table is in force carries none, and its value
/// does not decode. Its reply still holds the bytes.
#[derive(Debug, Clone)]
pub struct SettingChange {
    /// The setting's RPC name.
    pub name: String,
    /// The device's `settings.version` once this change is counted, or `None`
    /// from firmware that does not number its announcements.
    pub seq: Option<u32>,
    /// Its new value, the bytes that RPC would reply.
    pub reply: Vec<u8>,
    table: Option<Arc<RpcRegistry>>,
}

impl SettingChange {
    /// Creates the change of `name` to `reply`, counted as `seq`, recorded
    /// under `table`.
    pub(crate) fn new(
        name: String,
        seq: Option<u32>,
        reply: Vec<u8>,
        table: Option<Arc<RpcRegistry>>,
    ) -> SettingChange {
        SettingChange {
            name,
            seq,
            reply,
            table,
        }
    }

    /// Returns the RPC table the change was recorded under, or `None` for a
    /// change recorded while no table was in force.
    pub fn rpc_registry(&self) -> Option<&Arc<RpcRegistry>> {
        self.table.as_ref()
    }

    /// Decodes the new value with the type its entry has in the table the
    /// change was recorded under.
    ///
    /// # Errors
    ///
    /// Returns a [`SettingValueError`] if no table was in force when the
    /// change was recorded, if that table has no entry of its name, or if
    /// the reply does not decode as the entry's type.
    pub fn value(&self) -> Result<RpcValue, SettingValueError> {
        let table = self.table.as_ref().ok_or(SettingValueError::NoTable)?;
        let entry = table.find(&self.name).ok_or(SettingValueError::NoEntry)?;
        Ok(entry.meta.kind().decode(&self.reply)?)
    }
}

/// Why a [`SettingChange`] has no value.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SettingValueError {
    /// No RPC table was in force when the change was recorded.
    #[error("no RPC table was in force when the setting changed")]
    NoTable,
    /// The table the change was recorded under has no entry of its name.
    #[error("the RPC table the setting changed under has no such entry")]
    NoEntry,
    /// The reply does not decode as the type of its entry.
    #[error(transparent)]
    Decode(#[from] RpcValueDecodeError),
}

/// The values of a device's announced settings, read with one RPC table.
///
/// It holds every readable, announced entry of the table that is neither a
/// capture nor privileged. Three numbers place the values. The table's
/// `rpc.hash` identifies the table's contents. The table's session is the
/// one boot of the device the values were read in. The `settings.version`
/// orders the device's announcements within that session, and the values
/// are exact at it.
///
/// The values are exact while [`version`](Self::version) is `Some`. A device
/// without `settings.version`, or one that does not number its
/// announcements, has no values here.
#[derive(Debug, Clone)]
pub struct Settings {
    registry: Arc<RpcRegistry>,
    version: Option<u32>,
    values: BTreeMap<String, Vec<u8>>,
}

impl Settings {
    /// Creates the settings read with `registry`, exact at `version`,
    /// holding `values` by name.
    pub(crate) fn new(
        registry: Arc<RpcRegistry>,
        version: Option<u32>,
        values: BTreeMap<String, Vec<u8>>,
    ) -> Settings {
        Settings {
            registry,
            version,
            values,
        }
    }

    /// Returns the value of `name` as the bytes its RPC replies, or `None`
    /// for a setting not held.
    pub fn get(&self, name: &str) -> Option<&[u8]> {
        self.values.get(name).map(Vec::as_slice)
    }

    /// Returns each setting held, by name, with its value.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &[u8])> + '_ {
        self.values
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_slice()))
    }

    /// Returns the device's `settings.version` that the values are exact
    /// at, or `None` while they wait for the next snapshot.
    pub fn version(&self) -> Option<u32> {
        self.version
    }

    /// Returns the RPC table the values were read with, which gives each
    /// value its type, and its hash and the session the values were read in.
    pub fn rpc_registry(&self) -> &Arc<RpcRegistry> {
        &self.registry
    }
}
