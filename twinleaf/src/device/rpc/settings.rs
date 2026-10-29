//! The [`SettingsCache`] of setting values a device announces.

use super::error::CallError;
use super::registry::RpcRegistry;
use super::reply::{pipelined, PendingReply};
use crate::proto::rpc::RpcMetaFlags;
use crate::proto::SessionId;
use crate::tio::rpc::RpcReply;
use crate::DeviceEvent;
use std::collections::{BTreeMap, BTreeSet};

/// The counter a device raises on every setting announcement, numbering each.
const SETTINGS_VERSION: &str = "settings.version";

/// The most reads in flight at once during a snapshot.
const SNAPSHOT_WINDOW: usize = 8;

/// The most snapshots one call takes of values that keep moving.
const SNAPSHOT_ATTEMPTS: usize = 3;

/// What a host does next to keep a [`SettingsCache`] exact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheAction {
    /// Nothing, as the cache is as exact as the events so far show.
    Keep,
    /// Take a snapshot with
    /// [`Device::snapshot_settings`](crate::Device::snapshot_settings), since
    /// an announcement was missed.
    Snapshot,
    /// The device started a new session, which may run another table. Read
    /// the registry again and build a new cache from it.
    Reload,
}

/// The values of a device's announced settings and reports, exactly as the
/// device announced them.
///
/// It holds every readable, announced entry that is neither a capture nor
/// privileged, as the registry describes them, read in one snapshot and kept
/// by the numbered announcements after it. A sequence number out of order, or
/// a heartbeat ahead of the last one applied, drops every value until the next
/// snapshot, and a new session drops the cache. A value it holds is at most
/// one heartbeat old. A device without `settings.version`, or one that does
/// not number its announcements, is never cached.
#[derive(Debug)]
pub struct SettingsCache {
    cached: BTreeSet<String>,
    values: BTreeMap<String, Vec<u8>>,
    session: Option<SessionId>,
    applied: Option<u32>,
}

impl SettingsCache {
    /// Creates an empty cache for the device `registry` describes, to be
    /// filled by a snapshot.
    pub fn new(registry: &RpcRegistry) -> SettingsCache {
        let cached = match registry.find(SETTINGS_VERSION) {
            None => BTreeSet::new(),
            Some(_) => registry
                .iter()
                .filter(|rpc| {
                    let flags = rpc.meta.flags();
                    flags.contains(RpcMetaFlags::READABLE | RpcMetaFlags::ANNOUNCED)
                        && !flags.contains(RpcMetaFlags::CAPTURE)
                        && !flags.contains(RpcMetaFlags::PRIVILEGED)
                })
                .map(|rpc| rpc.full_name.clone())
                .collect(),
        };
        SettingsCache {
            cached,
            values: BTreeMap::new(),
            session: registry.session,
            applied: None,
        }
    }

    /// Takes one event from the device, applying an announcement in sequence
    /// and checking a heartbeat against the last one applied, and says what
    /// the host does next.
    pub fn observe(&mut self, event: &DeviceEvent) -> CacheAction {
        if self.cached.is_empty() {
            return CacheAction::Keep;
        }
        match event {
            DeviceEvent::Heartbeat {
                session_id: Some(session),
                settings,
            } => {
                if self.session.is_some_and(|known| known != *session) {
                    self.forget();
                    return CacheAction::Reload;
                }
                self.session = Some(*session);
                let Some(settings) = settings else {
                    self.forget();
                    return CacheAction::Keep;
                };
                match self.applied {
                    Some(applied) if ahead(*settings, applied) => self.drop_values(),
                    Some(_) => CacheAction::Keep,
                    None => CacheAction::Snapshot,
                }
            }
            DeviceEvent::Setting { seq: None, .. } => {
                self.forget();
                CacheAction::Keep
            }
            DeviceEvent::Setting {
                name,
                seq: Some(seq),
                reply,
            } => match self.applied {
                None => CacheAction::Snapshot,
                Some(applied) if *seq == applied.wrapping_add(1) => {
                    if self.cached.contains(name) {
                        self.values.insert(name.clone(), reply.clone());
                    }
                    self.applied = Some(*seq);
                    CacheAction::Keep
                }
                Some(applied) if ahead(*seq, applied) => self.drop_values(),
                Some(_) => CacheAction::Keep,
            },
            DeviceEvent::Heartbeat {
                session_id: None, ..
            }
            | DeviceEvent::Metadata(_)
            | DeviceEvent::MetadataUnavailable
            | DeviceEvent::NewHash(_) => CacheAction::Keep,
        }
    }

    /// Returns the value the device last announced for `name`, or `None` for
    /// an entry the cache does not hold or while it waits for a snapshot.
    pub fn get(&self, name: &str) -> Option<&[u8]> {
        self.values.get(name).map(Vec::as_slice)
    }

    /// Returns the sequence number of the last announcement applied, or
    /// `None` while the cache waits for a snapshot.
    pub fn version(&self) -> Option<u32> {
        self.applied
    }

    /// Reads `settings.version`, every cached value, and `settings.version`
    /// again through `submit`, until both reads agree. If they never do, the
    /// last read is kept at the first version, and the announcements between
    /// the two, still to be observed, bring it up to date.
    pub(crate) fn snapshot_with(
        &mut self,
        mut submit: impl FnMut(&str, &[u8]) -> PendingReply,
    ) -> Result<(), CallError> {
        if self.cached.is_empty() {
            return Ok(());
        }
        let version = |reply: PendingReply| {
            u32::decode_reply(&reply.wait()?).map_err(CallError::InvalidReply)
        };
        let mut attempt = 1;
        loop {
            let before = version(submit(SETTINGS_VERSION, &[]))?;
            let calls = self.cached.iter().map(|name| submit(name, &[]));
            let values = self
                .cached
                .iter()
                .cloned()
                .zip(pipelined(calls, SNAPSHOT_WINDOW))
                .map(|(name, reply)| Ok((name, reply?)))
                .collect::<Result<BTreeMap<String, Vec<u8>>, CallError>>()?;
            let after = version(submit(SETTINGS_VERSION, &[]))?;
            if after == before || attempt == SNAPSHOT_ATTEMPTS {
                self.values = values;
                self.applied = Some(before);
                return Ok(());
            }
            attempt += 1;
        }
    }

    /// Drops every value until the next snapshot.
    fn drop_values(&mut self) -> CacheAction {
        self.values.clear();
        self.applied = None;
        CacheAction::Snapshot
    }

    /// Stops caching for good, as the device cannot be kept exact.
    fn forget(&mut self) {
        self.cached.clear();
        self.values.clear();
        self.applied = None;
    }
}

/// Whether sequence number `seq` comes after `applied`, across a wrap.
fn ahead(seq: u32, applied: u32) -> bool {
    (seq.wrapping_sub(applied) as i32) > 0
}

#[cfg(test)]
mod tests {
    use super::super::registry::RpcDescriptor;
    use super::super::reply::resolved;
    use super::*;

    /// The metadata words of an announced read-write byte, an announced
    /// read-only one, a clock, a privileged announced byte, an announced
    /// capture, and the counter.
    const SETTING: u16 = 0xA310;
    const REPORT: u16 = 0xA110;
    const CLOCK: u16 = 0x8140;
    const PRIVILEGED: u16 = 0xE310;
    const CAPTURE: u16 = 0xB110;
    const COUNTER: u16 = 0x8140;

    const SESSION: SessionId = SessionId::new(1);

    fn registry(counter: bool) -> RpcRegistry {
        let mut registry = RpcRegistry::new(
            [
                ("test.amplitude", SETTING),
                ("test.status", REPORT),
                ("dev.uptime", CLOCK),
                ("test.secret", PRIVILEGED),
                ("test.capture", CAPTURE),
            ]
            .into_iter()
            .chain(counter.then_some((SETTINGS_VERSION, COUNTER)))
            .map(|(name, meta)| RpcDescriptor::from_meta(meta, name.to_string()))
            .collect(),
        );
        registry.session = Some(SESSION);
        registry
    }

    /// Answer `replies` in order, recording what was asked.
    fn snapshot(
        cache: &mut SettingsCache,
        replies: &[&[u8]],
    ) -> (Vec<String>, Result<(), CallError>) {
        let mut asked = Vec::new();
        let mut replies = replies.iter().map(|reply| reply.to_vec());
        let taken = cache.snapshot_with(|name, _| {
            asked.push(name.to_string());
            resolved(replies.next())
        });
        (asked, taken)
    }

    /// A cache snapshotted at `version` with both values at 1.
    fn snapshotted(version: u32) -> SettingsCache {
        let mut cache = SettingsCache::new(&registry(true));
        let v = version.to_le_bytes();
        snapshot(&mut cache, &[&v, &[1], &[1], &v]).1.unwrap();
        cache
    }

    fn setting(name: &str, seq: Option<u32>, reply: u8) -> DeviceEvent {
        DeviceEvent::Setting {
            name: name.to_string(),
            seq,
            reply: vec![reply],
        }
    }

    fn heartbeat(session: SessionId, settings: Option<u32>) -> DeviceEvent {
        DeviceEvent::Heartbeat {
            session_id: Some(session),
            settings,
        }
    }

    /// Only what is readable, announced, and neither privileged nor a
    /// capture is read, between two reads of the counter.
    #[test]
    fn a_snapshot_reads_the_announced_values_between_two_counter_reads() {
        let mut cache = SettingsCache::new(&registry(true));
        let (asked, taken) = snapshot(&mut cache, &[&[4, 0, 0, 0], &[7], &[2], &[4, 0, 0, 0]]);

        assert!(taken.is_ok());
        assert_eq!(
            asked,
            [
                SETTINGS_VERSION,
                "test.amplitude",
                "test.status",
                SETTINGS_VERSION
            ]
        );
        assert_eq!(cache.get("test.amplitude"), Some(&[7][..]));
        assert_eq!(cache.get("test.status"), Some(&[2][..]));
        assert_eq!(cache.version(), Some(4));
    }

    /// A counter that moves during a read is read again; one that never
    /// settles keeps the last read at its first count, for the announcements
    /// between to finish.
    #[test]
    fn a_counter_that_moves_during_a_snapshot_takes_it_again() {
        let mut cache = SettingsCache::new(&registry(true));
        let (asked, taken) = snapshot(
            &mut cache,
            &[
                &[1, 0, 0, 0],
                &[1],
                &[1],
                &[2, 0, 0, 0],
                &[2, 0, 0, 0],
                &[2],
                &[2],
                &[2, 0, 0, 0],
            ],
        );
        assert!(taken.is_ok());
        assert_eq!(asked.len(), 8);
        assert_eq!(cache.get("test.amplitude"), Some(&[2][..]));
        assert_eq!(cache.version(), Some(2));

        let mut cache = SettingsCache::new(&registry(true));
        let moving: Vec<[u8; 4]> = (0..2 * SNAPSHOT_ATTEMPTS as u32)
            .map(u32::to_le_bytes)
            .collect();
        let replies: Vec<&[u8]> = moving
            .chunks(2)
            .flat_map(|pair| [&pair[0][..], &[3], &[3], &pair[1][..]])
            .collect();
        assert!(snapshot(&mut cache, &replies).1.is_ok());
        assert_eq!(cache.version(), Some(2 * SNAPSHOT_ATTEMPTS as u32 - 2));
        assert_eq!(
            cache.observe(&setting("test.amplitude", Some(5), 9)),
            CacheAction::Keep
        );
        assert_eq!(cache.get("test.amplitude"), Some(&[9][..]));
    }

    /// The next announcement is applied, one the snapshot already holds is
    /// skipped, and one past a gap drops every value.
    #[test]
    fn announcements_apply_in_sequence_and_a_gap_drops_the_values() {
        let mut cache = snapshotted(4);
        assert_eq!(
            cache.observe(&setting("test.amplitude", Some(4), 8)),
            CacheAction::Keep
        );
        assert_eq!(cache.get("test.amplitude"), Some(&[1][..]), "already held");
        assert_eq!(
            cache.observe(&setting("test.amplitude", Some(5), 8)),
            CacheAction::Keep
        );
        assert_eq!(
            cache.observe(&setting("dev.reboot", Some(6), 0)),
            CacheAction::Keep
        );
        assert_eq!(cache.get("test.amplitude"), Some(&[8][..]));
        assert_eq!(cache.get("dev.reboot"), None, "counted but not held");
        assert_eq!(cache.version(), Some(6));

        assert_eq!(
            cache.observe(&setting("test.status", Some(8), 3)),
            CacheAction::Snapshot
        );
        assert_eq!(cache.get("test.amplitude"), None);
        assert_eq!(cache.version(), None);
        assert_eq!(
            cache.observe(&setting("test.status", Some(9), 3)),
            CacheAction::Snapshot,
            "still waiting"
        );
    }

    /// A heartbeat ahead of the last announcement applied shows one was
    /// missed; one behind or level does not.
    #[test]
    fn a_heartbeat_ahead_of_the_cache_drops_the_values() {
        let mut cache = snapshotted(4);
        assert_eq!(
            cache.observe(&heartbeat(SESSION, Some(4))),
            CacheAction::Keep
        );
        assert_eq!(
            cache.observe(&heartbeat(SESSION, Some(3))),
            CacheAction::Keep
        );
        assert_eq!(
            cache.observe(&heartbeat(SESSION, Some(5))),
            CacheAction::Snapshot
        );
        assert_eq!(cache.get("test.amplitude"), None);

        let mut cache = snapshotted(u32::MAX);
        assert_eq!(
            cache.observe(&setting("test.amplitude", Some(0), 2)),
            CacheAction::Keep,
            "the count wraps"
        );
        assert_eq!(
            cache.observe(&heartbeat(SESSION, Some(1))),
            CacheAction::Snapshot
        );
    }

    /// A new session may run another table, so the cache is dropped and the
    /// registry read again.
    #[test]
    fn a_new_session_drops_the_cache() {
        let mut cache = snapshotted(4);
        assert_eq!(
            cache.observe(&heartbeat(SessionId::new(2), Some(0))),
            CacheAction::Reload
        );
        assert_eq!(cache.get("test.amplitude"), None);
        assert_eq!(
            cache.observe(&setting("test.amplitude", Some(1), 1)),
            CacheAction::Keep
        );
        assert_eq!(cache.get("test.amplitude"), None);
    }

    /// Firmware without the counter, or that does not number what it
    /// announces, cannot be kept exact, and is never read or cached.
    #[test]
    fn a_device_that_cannot_be_kept_exact_is_never_cached() {
        let mut cache = SettingsCache::new(&registry(false));
        let (asked, taken) = snapshot(&mut cache, &[]);
        assert!(asked.is_empty() && taken.is_ok());
        assert_eq!(
            cache.observe(&setting("test.amplitude", Some(1), 1)),
            CacheAction::Keep
        );
        assert_eq!(cache.get("test.amplitude"), None);

        for legacy in [heartbeat(SESSION, None), setting("test.amplitude", None, 1)] {
            let mut cache = snapshotted(4);
            assert_eq!(cache.observe(&legacy), CacheAction::Keep);
            assert_eq!(cache.get("test.amplitude"), None);
            let (asked, _) = snapshot(&mut cache, &[]);
            assert!(asked.is_empty(), "{legacy:?} is never read again");
        }
    }
}
