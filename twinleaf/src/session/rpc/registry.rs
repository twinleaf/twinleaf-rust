//! The [`RpcRegistry`] of RPCs a device offers, read with `rpc.listinfo`.

use super::cache;
use super::error::CallError;
use super::reply::{PendingReply, Pipeline};
use crate::proto::rpc::{RpcError, RpcMeta};
use crate::proto::SessionId;
use crate::tio::rpc::RpcReply;
use std::collections::BTreeMap;

/// The most `rpc.listinfo` calls in flight at once during a walk.
const WALK_WINDOW: usize = 16;

/// The most walks one load makes of a table that keeps changing.
const WALK_ATTEMPTS: usize = 3;

/// The boot a device is in, which a heartbeat also carries.
const SESSION: &str = "dev.session";

/// Why a device's RPC table could not be loaded.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum RpcRegistryError {
    /// A call during the walk failed.
    #[error("RPC error: {0}")]
    DeviceRpcError(#[from] CallError),
    /// `rpc.hash` changed during every walk, as `dev.priv` and `dev.priv.lock`
    /// change it on firmware that lists a different table once unlocked.
    /// Loading again once it settles succeeds.
    #[error("the RPC table changed during the walk")]
    TableChanged,
}

/// One RPC a device offers.
#[derive(Debug, Clone)]
pub struct RpcDescriptor {
    /// The name a call uses, such as `dev.name`.
    pub full_name: String,
    /// Its type, access, and flags.
    pub meta: RpcMeta,
}

impl RpcDescriptor {
    /// Creates a descriptor of `name` from the metadata word the device sends.
    pub fn from_meta(meta: u16, name: String) -> RpcDescriptor {
        RpcDescriptor {
            full_name: name,
            meta: RpcMeta::from_bits(meta),
        }
    }
}

/// The RPCs a device offers, by name, read once per session. The table is
/// fixed for a firmware image, which only a new session can change.
#[derive(Debug)]
pub struct RpcRegistry {
    rpcs: BTreeMap<String, RpcDescriptor>,
    /// The device's `rpc.hash`, when the table came from a device.
    pub hash: Option<u32>,
    /// The session the table was read in, `None` where the firmware has no
    /// `dev.session`.
    pub session: Option<SessionId>,
}

impl RpcRegistry {
    /// Creates a registry of `specs`, with no hash or session.
    pub fn new(specs: Vec<RpcDescriptor>) -> Self {
        let rpcs = specs
            .into_iter()
            .map(|spec| (spec.full_name.clone(), spec))
            .collect();
        Self {
            rpcs,
            hash: None,
            session: None,
        }
    }

    /// Loads one device's RPC table through `submit`.
    ///
    /// Reads `dev.name`, `rpc.hash`, and `dev.session` first. On a cache
    /// miss it walks `rpc.listinfo` with [`WALK_WINDOW`] calls in flight. A
    /// walk counts only when `rpc.hash` reads the same after it, and only
    /// then is it cached. Otherwise the table is walked again under the new
    /// hash, up to [`WALK_ATTEMPTS`] times.
    pub(crate) async fn load_with(
        mut submit: impl FnMut(&str, &[u8]) -> PendingReply,
    ) -> Result<Self, RpcRegistryError> {
        let name_reply = submit("dev.name", &[]);
        let hash_reply = submit("rpc.hash", &[]);
        let session_reply = submit(SESSION, &[]);
        let dev_name: String = decode(name_reply.await?)?;
        let mut hash: u32 = decode(hash_reply.await?)?;
        let session = optional(session_reply.await)?.map(SessionId::new);
        for _ in 0..WALK_ATTEMPTS {
            let path = cache::path(&dev_name, hash);
            if let Some(entries) = path.as_deref().and_then(cache::load) {
                return Ok(Self::from_entries(entries, Some(hash), session));
            }
            let total: u16 = decode(submit("rpc.listinfo", &[]).await?)?;
            let mut entries = cache::Entries::with_capacity(total.into());
            {
                let descriptors =
                    (0..total).map(|index| submit("rpc.listinfo", &index.to_le_bytes()));
                let mut replies = Pipeline::new(descriptors, WALK_WINDOW);
                while let Some(reply) = replies.next().await {
                    let (meta, name) = decode::<(u16, String)>(reply?)?;
                    entries.push((name, meta));
                }
            }
            let after: u32 = decode(submit("rpc.hash", &[]).await?)?;
            if after == hash {
                if let Some(path) = &path {
                    cache::store(path, &entries);
                }
                return Ok(Self::from_entries(entries, Some(hash), session));
            }
            hash = after;
        }
        Err(RpcRegistryError::TableChanged)
    }

    /// Creates a registry of `entries`, each a name and its metadata word,
    /// with `hash` and `session`.
    pub(crate) fn from_entries(
        entries: Vec<(String, u16)>,
        hash: Option<u32>,
        session: Option<SessionId>,
    ) -> Self {
        let specs = entries
            .into_iter()
            .map(|(name, meta)| RpcDescriptor::from_meta(meta, name))
            .collect();
        let mut registry = Self::new(specs);
        registry.hash = hash;
        registry.session = session;
        registry
    }

    /// Returns the RPC called `name`, if the device offers it.
    pub fn find(&self, name: &str) -> Option<&RpcDescriptor> {
        self.rpcs.get(name)
    }

    /// Returns every RPC name, sorted.
    pub fn names(&self) -> Vec<String> {
        self.rpcs.keys().cloned().collect()
    }

    /// Returns an iterator over every RPC, sorted by name.
    pub fn iter(&self) -> impl Iterator<Item = &RpcDescriptor> + '_ {
        self.rpcs.values()
    }
}

/// Decodes the reply of an RPC older firmware may lack, or `None` when the
/// device has no such RPC.
fn optional(reply: Result<Vec<u8>, CallError>) -> Result<Option<u32>, RpcRegistryError> {
    match reply {
        Ok(reply) => decode(reply).map(Some),
        Err(CallError::DeviceError(payload)) if payload.error == RpcError::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn decode<T: RpcReply>(reply: Vec<u8>) -> Result<T, RpcRegistryError> {
    T::decode_reply(&reply)
        .map_err(|error| RpcRegistryError::DeviceRpcError(CallError::InvalidReply(error)))
}

#[cfg(test)]
mod tests {
    use super::super::error::RpcErrorPayload;
    use super::super::reply::{poll_once, resolved};
    use super::*;
    use std::task::Poll;

    fn descriptor(name: &str) -> Vec<u8> {
        let mut reply = 0x0300u16.to_le_bytes().to_vec();
        reply.extend_from_slice(name.as_bytes());
        reply
    }

    type Asked = Vec<(String, Vec<u8>)>;

    /// Loads a registry that answers `replies` in order, recording what was
    /// asked. Each call after the last reply fails.
    fn load_recording(replies: Vec<Vec<u8>>) -> (Asked, Result<RpcRegistry, RpcRegistryError>) {
        let mut asked = Vec::new();
        let mut replies = replies.into_iter();
        let loaded = poll_once(std::pin::pin!(RpcRegistry::load_with(|name, arg| {
            asked.push((name.to_string(), arg.to_vec()));
            resolved(replies.next())
        })));
        let Poll::Ready(loaded) = loaded else {
            panic!("the load waits on a reply it was given");
        };
        (asked, loaded)
    }

    /// Loads as [`load_recording`] does, for a walk that runs out of replies.
    fn walk_recording(replies: Vec<Vec<u8>>) -> (Asked, RpcRegistryError) {
        let (asked, loaded) = load_recording(replies);
        let Err(error) = loaded else {
            panic!("the walk runs out of replies");
        };
        (asked, error)
    }

    /// The replies of a device called `name` whose table of `dev.name` alone
    /// keeps the hash `hash` through the walk.
    fn finished_walk(name: &str, hash: u32) -> Vec<Vec<u8>> {
        vec![
            name.as_bytes().to_vec(),
            hash.to_le_bytes().to_vec(),
            7u32.to_le_bytes().to_vec(),
            1u16.to_le_bytes().to_vec(),
            descriptor("dev.name"),
            hash.to_le_bytes().to_vec(),
        ]
    }

    /// A walk whose hash holds is cached, and the next load of that hash
    /// reads the table from the cache after the name, hash and session.
    #[test]
    fn a_finished_walk_is_cached_and_the_next_load_reads_the_cache() {
        let _cache = cache::Scratch::new();
        let hash = 0x0bad_cafe;
        let (_, walked) = load_recording(finished_walk("test-device", hash));
        let walked = walked.expect("the walk finishes");

        let (asked, cached) = load_recording(finished_walk("test-device", hash)[..3].to_vec());
        let cached = cached.expect("the table loads from the cache");

        assert_eq!(asked.len(), 3, "the cached table was walked again");
        assert_eq!(cached.names(), walked.names());
        assert_eq!(cached.names(), ["dev.name"]);
        assert_eq!(
            (cached.hash, cached.session),
            (Some(hash), Some(SessionId::new(7)))
        );
    }

    /// A test holds no cache directory of its own unless it asks for one, so
    /// a walk that finishes, as one does under a mutation that ignores a
    /// moved hash, leaves no file in the user's cache for a later run to
    /// load.
    #[test]
    fn a_finished_walk_writes_nothing_to_the_users_cache() {
        const NAME: &str = "registry-test-without-a-cache";
        let hash = 0x5eed_cafe;
        let (_, loaded) = load_recording(finished_walk(NAME, hash));

        assert!(loaded.is_ok(), "the walk finishes");
        assert_eq!(cache::path(NAME, hash), None);
        let users = directories::BaseDirs::new()
            .map(|dirs| dirs.cache_dir().join("twinleaf"))
            .map(|dir| dir.join(format!("{NAME}.{hash:x}.rpcs")));
        assert!(
            !users.is_some_and(|file| file.exists()),
            "the walk wrote the user's cache"
        );
    }

    #[test]
    fn the_walk_asks_for_the_name_the_hash_the_session_then_each_descriptor() {
        // A test has no cache, so the walk has to enumerate.
        let (asked, error) = walk_recording(vec![
            b"test-device".to_vec(),
            0xdead_beefu32.to_le_bytes().to_vec(),
            7u32.to_le_bytes().to_vec(),
            2u16.to_le_bytes().to_vec(),
            descriptor("dev.name"),
        ]);

        assert_eq!(
            asked,
            [
                ("dev.name".to_string(), Vec::new()),
                ("rpc.hash".to_string(), Vec::new()),
                (SESSION.to_string(), Vec::new()),
                ("rpc.listinfo".to_string(), Vec::new()),
                ("rpc.listinfo".to_string(), vec![0, 0]),
                ("rpc.listinfo".to_string(), vec![1, 0]),
            ]
        );
        assert!(matches!(
            error,
            RpcRegistryError::DeviceRpcError(CallError::ResponseLost)
        ));
    }

    /// After `dev.priv` mid-walk, the descriptors came partly from each table,
    /// so the walk is neither cached nor returned, and the new table is walked.
    #[test]
    fn a_hash_that_moves_during_the_walk_walks_the_new_table() {
        let _cache = cache::Scratch::new();
        let (before, after) = (0x0bad_c0deu32, 0x0bad_c0dfu32);
        let (asked, error) = walk_recording(vec![
            b"test-device".to_vec(),
            before.to_le_bytes().to_vec(),
            7u32.to_le_bytes().to_vec(),
            1u16.to_le_bytes().to_vec(),
            descriptor("dev.name"),
            after.to_le_bytes().to_vec(),
        ]);

        assert_eq!(
            asked,
            [
                ("dev.name".to_string(), Vec::new()),
                ("rpc.hash".to_string(), Vec::new()),
                (SESSION.to_string(), Vec::new()),
                ("rpc.listinfo".to_string(), Vec::new()),
                ("rpc.listinfo".to_string(), vec![0, 0]),
                ("rpc.hash".to_string(), Vec::new()),
                ("rpc.listinfo".to_string(), Vec::new()),
            ]
        );
        assert!(matches!(
            error,
            RpcRegistryError::DeviceRpcError(CallError::ResponseLost)
        ));
        let stale = cache::path("test-device", before).expect("the scratch directory");
        assert!(!stale.exists(), "the mixed walk was cached");
    }

    #[test]
    fn a_hash_that_never_settles_fails_the_load() {
        let mut replies = vec![
            b"test-device".to_vec(),
            0x0bad_d00du32.to_le_bytes().to_vec(),
            7u32.to_le_bytes().to_vec(),
        ];
        for attempt in 1..=WALK_ATTEMPTS as u32 {
            replies.push(0u16.to_le_bytes().to_vec());
            replies.push((0x0bad_d00d + attempt).to_le_bytes().to_vec());
        }
        let (asked, error) = walk_recording(replies);

        assert_eq!(asked.len(), 3 + 2 * WALK_ATTEMPTS);
        assert!(matches!(error, RpcRegistryError::TableChanged));
    }

    #[test]
    fn a_reply_that_does_not_decode_fails_the_walk() {
        let (asked, error) = walk_recording(vec![b"test-device".to_vec(), vec![0, 1]]);

        assert_eq!(asked.len(), 3, "a two-byte hash should not decode");
        assert!(matches!(
            error,
            RpcRegistryError::DeviceRpcError(CallError::InvalidReply(_))
        ));
    }

    /// Firmware without an RPC the walk reads refuses it, and the walk goes on.
    #[test]
    fn a_device_without_an_optional_rpc_refuses_it_and_the_walk_goes_on() {
        let refused = Err(CallError::DeviceError(RpcErrorPayload {
            error: RpcError::NotFound,
            extra: Vec::new(),
        }));
        assert!(matches!(optional(refused), Ok(None)));
        assert!(matches!(
            optional(Ok(9u32.to_le_bytes().to_vec())),
            Ok(Some(9))
        ));
        assert!(matches!(
            optional(Err(CallError::Timeout)),
            Err(RpcRegistryError::DeviceRpcError(CallError::Timeout))
        ));
    }
}
