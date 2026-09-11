//! The set of RPCs a device offers, learned by walking `rpc.listinfo`.

use super::cache;
use super::codec::RpcReply;
use super::error::CallError;
use super::reply::{pipelined, PendingReply};
use crate::proto::rpc::RpcMeta;
use std::collections::BTreeMap;
use std::io;

/// Most `rpc.listinfo` fetches in flight at once during a walk, bounding the
/// request burst a memory-tight device must absorb.
const WALK_WINDOW: usize = 8;

/// Why a device's RPC table could not be loaded.
#[derive(Debug, thiserror::Error)]
pub enum RpcRegistryError {
    /// No cache directory exists on this host.
    #[error("could not locate cache directory")]
    CacheDirError,
    /// The cache file could not be read or written.
    #[error("cache file I/O error: {0}")]
    CacheFileError(#[from] io::Error),
    /// A call during the walk failed.
    #[error("RPC error: {0}")]
    DeviceRpcError(#[from] CallError),
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
    /// A descriptor from the metadata word as the device sends it.
    pub fn from_meta(meta: u16, name: String) -> RpcDescriptor {
        RpcDescriptor {
            full_name: name,
            meta: RpcMeta::from_bits(meta),
        }
    }
}

/// The RPCs a device offers, by name.
pub struct RpcRegistry {
    rpcs: BTreeMap<String, RpcDescriptor>,
    /// The device's `rpc.hash`, when the table came from a device.
    pub hash: Option<u32>,
}

impl RpcRegistry {
    /// A registry over `specs`, with no hash.
    pub fn new(specs: Vec<RpcDescriptor>) -> Self {
        let rpcs = specs
            .into_iter()
            .map(|spec| (spec.full_name.clone(), spec))
            .collect();
        Self { rpcs, hash: None }
    }

    /// Walk one device's RPC table through `submit`, [`WALK_WINDOW`] fetches in
    /// flight. Only a cache miss on `dev.name` and `rpc.hash` enumerates it.
    pub(crate) fn load_with(
        mut submit: impl FnMut(&str, &[u8]) -> PendingReply,
    ) -> Result<Self, RpcRegistryError> {
        let name_reply = submit("dev.name", &[]);
        let hash_reply = submit("rpc.hash", &[]);
        let dev_name: String = decode(name_reply.wait()?)?;
        let hash: u32 = decode(hash_reply.wait()?)?;
        let path = cache::path(&dev_name, hash).ok_or(RpcRegistryError::CacheDirError)?;
        if let Some(entries) = cache::load(&path)? {
            return Ok(Self::from_entries(entries, hash));
        }
        let total: u16 = decode(submit("rpc.listinfo", &[]).wait()?)?;
        let descriptors = (0..total).map(|index| submit("rpc.listinfo", &index.to_le_bytes()));
        let entries = pipelined(descriptors, WALK_WINDOW)
            .map(|reply| decode::<(u16, String)>(reply?).map(|(meta, name)| (name, meta)))
            .collect::<Result<cache::Entries, _>>()?;
        cache::store(&path, &entries);
        Ok(Self::from_entries(entries, hash))
    }

    fn from_entries(entries: cache::Entries, hash: u32) -> Self {
        let specs = entries
            .into_iter()
            .map(|(name, meta)| RpcDescriptor::from_meta(meta, name))
            .collect();
        let mut registry = Self::new(specs);
        registry.hash = Some(hash);
        registry
    }

    /// The RPC called `name`.
    pub fn find(&self, name: &str) -> Option<&RpcDescriptor> {
        self.rpcs.get(name)
    }

    /// Every name, sorted.
    pub fn names(&self) -> Vec<String> {
        self.rpcs.keys().cloned().collect()
    }

    /// Every RPC, sorted by name.
    pub fn iter(&self) -> impl Iterator<Item = &RpcDescriptor> + '_ {
        self.rpcs.values()
    }
}

fn decode<T: RpcReply>(reply: Vec<u8>) -> Result<T, RpcRegistryError> {
    T::decode_reply(&reply)
        .map_err(|error| RpcRegistryError::DeviceRpcError(CallError::InvalidReply(error)))
}

#[cfg(test)]
mod tests {
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
    fn the_walk_asks_for_the_name_the_hash_then_each_descriptor() {
        // A hash nothing has cached, so the walk has to enumerate.
        let (asked, error) = walk_recording(vec![
            b"test-device".to_vec(),
            0xdead_beefu32.to_le_bytes().to_vec(),
            2u16.to_le_bytes().to_vec(),
            descriptor("dev.name"),
        ]);

        assert_eq!(
            asked,
            [
                ("dev.name".to_string(), Vec::new()),
                ("rpc.hash".to_string(), Vec::new()),
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

    #[test]
    fn a_reply_that_does_not_decode_fails_the_walk() {
        let (asked, error) = walk_recording(vec![b"test-device".to_vec(), vec![0, 1]]);

        assert_eq!(asked.len(), 2, "a two-byte hash should not decode");
        assert!(matches!(
            error,
            RpcRegistryError::DeviceRpcError(CallError::InvalidReply(_))
        ));
    }
}
