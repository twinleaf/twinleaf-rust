//! The [`RpcRegistry`] of RPCs a device offers, read with `rpc.listinfo`.

use super::cache;
use super::error::CallError;
use super::reply::{pipelined, PendingReply};
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
#[derive(Debug, thiserror::Error)]
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
    pub(crate) fn load_with(
        mut submit: impl FnMut(&str, &[u8]) -> PendingReply,
    ) -> Result<Self, RpcRegistryError> {
        let name_reply = submit("dev.name", &[]);
        let hash_reply = submit("rpc.hash", &[]);
        let session_reply = submit(SESSION, &[]);
        let dev_name: String = decode(name_reply.wait()?)?;
        let mut hash: u32 = decode(hash_reply.wait()?)?;
        let session = optional(session_reply.wait())?.map(SessionId::new);
        for _ in 0..WALK_ATTEMPTS {
            let path = cache::path(&dev_name, hash);
            if let Some(entries) = path.as_deref().and_then(cache::load) {
                return Ok(Self::from_entries(entries, hash, session));
            }
            let total: u16 = decode(submit("rpc.listinfo", &[]).wait()?)?;
            let descriptors = (0..total).map(|index| submit("rpc.listinfo", &index.to_le_bytes()));
            let entries = pipelined(descriptors, WALK_WINDOW)
                .map(|reply| decode::<(u16, String)>(reply?).map(|(meta, name)| (name, meta)))
                .collect::<Result<cache::Entries, _>>()?;
            let after: u32 = decode(submit("rpc.hash", &[]).wait()?)?;
            if after == hash {
                if let Some(path) = &path {
                    cache::store(path, &entries);
                }
                return Ok(Self::from_entries(entries, hash, session));
            }
            hash = after;
        }
        Err(RpcRegistryError::TableChanged)
    }

    fn from_entries(entries: cache::Entries, hash: u32, session: Option<SessionId>) -> Self {
        let specs = entries
            .into_iter()
            .map(|(name, meta)| RpcDescriptor::from_meta(meta, name))
            .collect();
        let mut registry = Self::new(specs);
        registry.hash = Some(hash);
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
    use super::super::reply::resolved;
    use super::*;

    fn descriptor(name: &str) -> Vec<u8> {
        let mut reply = 0x0300u16.to_le_bytes().to_vec();
        reply.extend_from_slice(name.as_bytes());
        reply
    }

    /// Answer `replies` in order, recording what was asked. A run out of
    /// replies fails the walk, so no test ever writes the on-disk cache.
    fn walk_recording(replies: Vec<Vec<u8>>) -> (Vec<(String, Vec<u8>)>, RpcRegistryError) {
        let mut asked = Vec::new();
        let mut replies = replies.into_iter();
        let Err(error) = RpcRegistry::load_with(|name, arg| {
            asked.push((name.to_string(), arg.to_vec()));
            resolved(replies.next())
        }) else {
            panic!("the walk runs out of replies");
        };
        (asked, error)
    }

    #[test]
    fn the_walk_asks_for_the_name_the_hash_the_session_then_each_descriptor() {
        // A hash nothing has cached, so the walk has to enumerate.
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

    /// `dev.priv` mid-walk: the descriptors came partly from each table, so
    /// the walk is neither cached nor returned, and the new table is walked.
    #[test]
    fn a_hash_that_moves_during_the_walk_walks_the_new_table() {
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
        let stale = cache::path("test-device", before).expect("a cache directory");
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
