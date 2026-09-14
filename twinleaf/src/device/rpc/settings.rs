//! The setting values a device announces, and the `settings.version` rule that
//! keeps them true.

use super::codec::RpcReply;
use super::error::CallError;
use super::registry::{RpcRegistry, SETTINGS_VERSION};
use super::reply::{pipelined, PendingReply};
use crate::proto::rpc::RpcAccess;
use std::collections::{BTreeMap, BTreeSet};

/// Most re-reads in flight at once during a resync, bounding the request burst
/// as the table walk does.
const RESYNC_WINDOW: usize = 8;

/// The announcement carrying the RPC table's hash, which is not a setting and
/// which a device does not count.
const HASH: &str = "rpc.hash";

/// Every readable setting a device has announced, at the value it announced.
///
/// A device counts each announcement in `settings.version`, so a version the
/// announcements do not add up to means some were missed and the cached values
/// are re-read. Firmware without the counter is never counted or resynced.
pub struct SettingsCache {
    values: BTreeMap<String, Vec<u8>>,
    readable: BTreeSet<String>,
    expected: Option<u32>,
}

impl SettingsCache {
    /// An empty cache for the device `registry` describes, counting on from the
    /// `settings.version` read with its table.
    pub fn new(registry: &RpcRegistry) -> SettingsCache {
        SettingsCache {
            values: BTreeMap::new(),
            readable: registry
                .iter()
                .filter(|rpc| {
                    matches!(
                        rpc.meta.access(),
                        RpcAccess::ReadWrite | RpcAccess::ReadOnly
                    )
                })
                .map(|rpc| rpc.full_name.clone())
                .collect(),
            expected: registry.settings_version,
        }
    }

    /// Take one announcement, as
    /// [`DeviceEvent::Setting`](crate::DeviceEvent::Setting) carries it. Only a
    /// readable setting is kept, since only one can be re-read.
    pub fn apply(&mut self, name: &str, reply: Vec<u8>) {
        if name == HASH {
            return;
        }
        if self.readable.contains(name) {
            self.values.insert(name.to_string(), reply);
        }
        self.expected = self.expected.map(|version| version.wrapping_add(1));
    }

    /// The value the device last announced for `name`.
    pub fn get(&self, name: &str) -> Option<&[u8]> {
        self.values.get(name).map(Vec::as_slice)
    }

    /// The `settings.version` the announcements taken so far add up to, `None`
    /// for a device without the counter.
    pub fn version(&self) -> Option<u32> {
        self.expected
    }

    /// Read `settings.version` through `submit` and, when it is not the one the
    /// announcements counted, re-read every cached setting.
    pub(crate) fn resync_with(
        &mut self,
        mut submit: impl FnMut(&str, &[u8]) -> PendingReply,
    ) -> Result<bool, CallError> {
        let Some(expected) = self.expected else {
            return Ok(false);
        };
        let reply = submit(SETTINGS_VERSION, &[]).wait()?;
        let version = u32::decode_reply(&reply).map_err(CallError::InvalidReply)?;
        if version == expected {
            return Ok(false);
        }
        let names: Vec<String> = self.values.keys().cloned().collect();
        let calls = names.iter().map(|name| submit(name, &[]));
        self.values = names
            .iter()
            .cloned()
            .zip(pipelined(calls, RESYNC_WINDOW))
            .map(|(name, reply)| Ok((name, reply?)))
            .collect::<Result<BTreeMap<String, Vec<u8>>, CallError>>()?;
        self.expected = Some(version);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::super::registry::RpcDescriptor;
    use super::super::reply::resolved;
    use super::*;

    /// The metadata word of a readable, writable byte, and of an action.
    const RW_U8: u16 = 0x0301;
    const ACTION: u16 = 0x0000;

    /// A cache for a device with two readable settings and one action, whose
    /// table was read at `version`.
    fn cache(version: Option<u32>) -> SettingsCache {
        let mut registry = RpcRegistry::new(vec![
            RpcDescriptor::from_meta(RW_U8, "test.amplitude".to_string()),
            RpcDescriptor::from_meta(RW_U8, "test.enable".to_string()),
            RpcDescriptor::from_meta(ACTION, "test.reset".to_string()),
        ]);
        registry.settings_version = version;
        SettingsCache::new(&registry)
    }

    /// Resync answering `replies` in order, recording what was asked.
    fn resync(
        cache: &mut SettingsCache,
        replies: Vec<Vec<u8>>,
    ) -> (Vec<String>, Result<bool, CallError>) {
        let mut asked = Vec::new();
        let mut replies = replies.into_iter();
        let resynced = cache.resync_with(|name, _| {
            asked.push(name.to_string());
            resolved(replies.next())
        });
        (asked, resynced)
    }

    #[test]
    fn an_announcement_lands_in_the_cache_and_counts() {
        let mut cache = cache(Some(3));
        cache.apply("test.amplitude", vec![1, 2, 3, 4]);

        assert_eq!(cache.get("test.amplitude"), Some(&[1, 2, 3, 4][..]));
        assert_eq!(cache.version(), Some(4));
    }

    /// The table hash travels as an announcement, but a device neither stores
    /// nor counts it.
    #[test]
    fn the_table_hash_is_not_a_setting() {
        let mut cache = cache(Some(3));
        cache.apply("rpc.hash", 0xdead_beefu32.to_le_bytes().to_vec());

        assert_eq!(cache.get("rpc.hash"), None);
        assert_eq!(cache.version(), Some(3));
    }

    #[test]
    fn an_announced_action_is_counted_but_never_cached_or_re_read() {
        let mut cache = cache(Some(3));
        cache.apply("test.reset", Vec::new());
        cache.apply("test.amplitude", vec![1]);

        assert_eq!(cache.get("test.reset"), None);
        assert_eq!(cache.version(), Some(5));
        let (asked, resynced) = resync(&mut cache, vec![9u32.to_le_bytes().to_vec(), vec![2]]);
        assert_eq!(asked, [SETTINGS_VERSION, "test.amplitude"]);
        assert!(matches!(resynced, Ok(true)));
    }

    #[test]
    fn a_device_without_the_counter_neither_counts_nor_resyncs() {
        let mut cache = cache(None);
        cache.apply("test.amplitude", vec![1]);

        assert_eq!(cache.get("test.amplitude"), Some(&[1][..]));
        assert_eq!(cache.version(), None);
        let (asked, resynced) = resync(&mut cache, Vec::new());
        assert_eq!(asked, Vec::<String>::new(), "no version was ever read");
        assert!(matches!(resynced, Ok(false)));
    }

    #[test]
    fn a_version_that_matches_leaves_the_cache_alone() {
        let mut cache = cache(Some(3));
        cache.apply("test.amplitude", vec![1]);
        let (asked, resynced) = resync(&mut cache, vec![4u32.to_le_bytes().to_vec()]);

        assert_eq!(asked, [SETTINGS_VERSION]);
        assert!(matches!(resynced, Ok(false)));
        assert_eq!(cache.get("test.amplitude"), Some(&[1][..]));
        assert_eq!(cache.version(), Some(4));
    }

    #[test]
    fn a_version_gap_re_reads_every_cached_setting() {
        let mut cache = cache(Some(3));
        cache.apply("test.amplitude", vec![1]);
        cache.apply("test.enable", vec![0]);
        let (asked, resynced) = resync(
            &mut cache,
            vec![9u32.to_le_bytes().to_vec(), vec![7], vec![1]],
        );

        assert_eq!(asked, [SETTINGS_VERSION, "test.amplitude", "test.enable"]);
        assert!(matches!(resynced, Ok(true)));
        assert_eq!(cache.get("test.amplitude"), Some(&[7][..]));
        assert_eq!(cache.get("test.enable"), Some(&[1][..]));
        assert_eq!(cache.version(), Some(9));
    }

    /// A re-read that fails leaves the count alone, so the next check retries.
    #[test]
    fn a_failed_re_read_leaves_the_cache_to_try_again() {
        let mut cache = cache(Some(3));
        cache.apply("test.amplitude", vec![1]);
        let (_asked, resynced) = resync(&mut cache, vec![9u32.to_le_bytes().to_vec()]);

        assert!(matches!(resynced, Err(CallError::ResponseLost)));
        assert_eq!(cache.get("test.amplitude"), Some(&[1][..]));
        assert_eq!(cache.version(), Some(4));
    }
}
