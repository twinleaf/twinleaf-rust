//! The state of each device that a connection keeps current. That is whether
//! the device is there, its metadata, its RPC table and its settings.
//!
//! Each piece is a whole value. A [`Device`](crate::Device) returns the
//! current one, or a [`Receiver`](crate::Receiver) that yields the current
//! one first and then each new one. A reader that falls behind skips to the
//! newest, so it never lags.

use super::rpc::{CacheAction, PendingReply, RpcRegistry, RpcRegistryError, SettingsCache};
use super::session::Handle;
use super::stream::DeviceEvent;
use crate::data::DeviceMetadataSnapshot;
use crate::proto::{DeviceRoute, SessionId};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// How long a worker waits after its RPC table failed to load before it
/// loads it again, unless the device announces a new table first.
const RELOAD_DELAY: Duration = Duration::from_secs(5);

/// Whether a device is there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Presence {
    /// Nothing has been heard from the device yet, or its link reconnected
    /// and it has not spoken since.
    Connecting,
    /// The device is sending.
    Alive {
        /// The session its last heartbeat reported, when one did.
        session: Option<SessionId>,
        /// When its last heartbeat arrived, if one has.
        heartbeat: Option<Instant>,
    },
    /// The link that serves the device lost it or stopped.
    Lost {
        /// When the link reported it.
        since: Instant,
    },
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

/// One setting a device announced, in the order it announced them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettingChange {
    /// The setting's RPC name.
    pub name: String,
    /// The device's `settings.version` once this change is counted, or `None`
    /// from firmware that does not number its announcements.
    pub seq: Option<u32>,
    /// Its new value, the bytes that RPC would reply.
    pub reply: Vec<u8>,
}

/// The values of a device's announced settings, read with one RPC table.
///
/// It holds every readable, announced entry of the table that is neither a
/// capture nor privileged. The values are exact while
/// [`version`](Self::version) is `Some`. A device without `settings.version`,
/// or one that does not number its announcements, has no values here.
#[derive(Debug, Clone)]
pub struct Settings {
    registry: Arc<RpcRegistry>,
    session: Option<SessionId>,
    version: Option<u32>,
    values: BTreeMap<String, Vec<u8>>,
}

impl Settings {
    /// Creates the settings read with `registry` in `session`, exact at
    /// `version`, holding `values` by name.
    pub(crate) fn new(
        registry: Arc<RpcRegistry>,
        session: Option<SessionId>,
        version: Option<u32>,
        values: BTreeMap<String, Vec<u8>>,
    ) -> Settings {
        Settings {
            registry,
            session,
            version,
            values,
        }
    }

    fn of(registry: &Arc<RpcRegistry>, cache: &SettingsCache) -> Settings {
        Settings::new(
            Arc::clone(registry),
            cache.session(),
            cache.version(),
            cache.values().clone(),
        )
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

    /// Returns the session the values were read in.
    pub fn session(&self) -> Option<SessionId> {
        self.session
    }

    /// Returns the RPC table the values were read with, which gives each
    /// value its type.
    pub fn rpc_registry(&self) -> &Arc<RpcRegistry> {
        &self.registry
    }
}

/// What a device's worker learned.
pub(crate) enum Learned {
    Registry(RegistryState),
    Settings(Arc<Settings>),
}

/// Starts the worker that loads the RPC table of the device at `route`
/// through `handle` and keeps its settings, and returns the sender of the
/// device's events it follows. It sends what it learns on `learned`, and
/// stops once the sender is dropped.
pub(crate) fn start_worker(
    handle: Handle,
    route: DeviceRoute,
    learned: mpsc::UnboundedSender<(DeviceRoute, Learned)>,
) -> flume::Sender<DeviceEvent> {
    let (events, received) = flume::unbounded();
    let worker = Worker {
        handle,
        route,
        events: received,
        learned,
    };
    let started = std::thread::Builder::new()
        .name(format!("twinleaf settings {route}"))
        .spawn(move || worker.run());
    if let Err(error) = started {
        log::warn!("could not start the settings worker of {route}: {error}");
    }
    events
}

/// The worker of one device. Its calls block, so it runs on a thread of its
/// own.
struct Worker {
    handle: Handle,
    route: DeviceRoute,
    events: flume::Receiver<DeviceEvent>,
    learned: mpsc::UnboundedSender<(DeviceRoute, Learned)>,
}

impl Worker {
    /// Loads the table and follows the device until a new table or a new
    /// session calls for loading it again, until the pump lets go.
    fn run(self) {
        loop {
            let loaded = RpcRegistry::load_with(|name, arg| self.submit(name, arg));
            let registry = match loaded {
                Ok(registry) => Arc::new(registry),
                Err(error) => {
                    log::warn!("could not load the RPC table of {}: {error}", self.route);
                    if !self.tell(Learned::Registry(Err(error))) || !self.await_new_table() {
                        return;
                    }
                    continue;
                }
            };
            if !self.tell(Learned::Registry(Ok(Arc::clone(&registry)))) || !self.follow(&registry) {
                return;
            }
        }
    }

    /// Keeps the settings of `registry` until it is out of date. Returns
    /// `false` once the pump lets go.
    fn follow(&self, registry: &Arc<RpcRegistry>) -> bool {
        let mut cache = SettingsCache::new(registry);
        if !self.snapshot(registry, &mut cache) {
            return false;
        }
        loop {
            let Ok(event) = self.events.recv() else {
                return false;
            };
            match &event {
                DeviceEvent::NewHash(None) => return true,
                DeviceEvent::NewHash(Some(hash)) if Some(*hash) != registry.hash => return true,
                DeviceEvent::Heartbeat {
                    session_id: Some(session),
                    ..
                } if registry.session.is_some_and(|read_in| read_in != *session) => return true,
                _ => {}
            }
            let before = cache.version();
            let kept = match cache.observe(&event) {
                CacheAction::Keep if cache.version() == before => true,
                CacheAction::Keep => {
                    self.tell(Learned::Settings(Arc::new(Settings::of(registry, &cache))))
                }
                CacheAction::Snapshot => self.snapshot(registry, &mut cache),
                CacheAction::Reload => return true,
            };
            if !kept {
                return false;
            }
        }
    }

    /// Fills `cache` with a snapshot and sends the settings it holds then.
    /// A failed snapshot is logged, and the cache asks for another at the
    /// next heartbeat. Returns `false` once the pump lets go.
    fn snapshot(&self, registry: &Arc<RpcRegistry>, cache: &mut SettingsCache) -> bool {
        if let Err(error) = cache.snapshot_with(|name, arg| self.submit(name, arg)) {
            log::warn!("could not read the settings of {}: {error}", self.route);
        }
        self.tell(Learned::Settings(Arc::new(Settings::of(registry, cache))))
    }

    /// Waits for the device to announce a new table, or for the delay
    /// before loading again. Returns `false` once the pump lets go.
    fn await_new_table(&self) -> bool {
        let deadline = Instant::now() + RELOAD_DELAY;
        loop {
            match self.events.recv_deadline(deadline) {
                Ok(DeviceEvent::NewHash(_)) | Err(flume::RecvTimeoutError::Timeout) => return true,
                Ok(_) => {}
                Err(flume::RecvTimeoutError::Disconnected) => return false,
            }
        }
    }

    fn submit(&self, name: &str, arg: &[u8]) -> PendingReply {
        PendingReply {
            reply: self.handle.submit(self.route, name, arg),
        }
    }

    /// Sends what the worker learned to the pump. Returns `false` once the
    /// pump is gone.
    fn tell(&self, learned: Learned) -> bool {
        self.learned.send((self.route, learned)).is_ok()
    }
}
