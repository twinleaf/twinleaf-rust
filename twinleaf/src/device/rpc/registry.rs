//! The set of RPCs a device offers, learned by walking `rpc.listinfo`.

use super::cache;
use super::{CallError, RpcReply};

use std::collections::BTreeMap;
use std::io;
use twinleaf_proto::rpc::RpcMeta;

#[derive(Debug, thiserror::Error)]
pub enum RpcRegistryError {
    #[error("could not locate cache directory")]
    CacheDirError,
    #[error("cache file I/O error: {0}")]
    CacheFileError(#[from] io::Error),
    #[error("RPC error: {0}")]
    DeviceRpcError(CallError),
}

#[derive(Debug, Clone)]
pub struct RpcDescriptor {
    pub full_name: String,
    pub meta: RpcMeta,
}

impl RpcDescriptor {
    pub fn from_meta(meta: u16, name: String) -> RpcDescriptor {
        RpcDescriptor {
            full_name: name,
            meta: RpcMeta::from_bits(meta),
        }
    }
}

pub struct RpcRegistry {
    rpcs: BTreeMap<String, RpcDescriptor>,
    pub hash: Option<u32>,
}

impl RpcRegistry {
    pub fn new(specs: Vec<RpcDescriptor>) -> Self {
        let rpcs = specs
            .into_iter()
            .map(|spec| (spec.full_name.clone(), spec))
            .collect();
        Self { rpcs, hash: None }
    }

    /// Walk one device's RPC table, a call at a time, on a blocking surface.
    ///
    /// `dev.name` and `rpc.hash` identify the on-disk cache entry; only a miss
    /// enumerates `rpc.listinfo`.
    pub(crate) fn load_with(
        mut call: impl FnMut(&str, &[u8]) -> Result<Vec<u8>, CallError>,
    ) -> Result<Self, RpcRegistryError> {
        let mut ask =
            |name: &str, arg: &[u8]| call(name, arg).map_err(RpcRegistryError::DeviceRpcError);
        let dev_name: String = decode(ask("dev.name", &[])?)?;
        let hash: u32 = decode(ask("rpc.hash", &[])?)?;
        let path = cache::path(&dev_name, hash).ok_or(RpcRegistryError::CacheDirError)?;
        if let Some(entries) = cache::load(&path)? {
            return Ok(Self::from_entries(entries, hash));
        }
        let total: u16 = decode(ask("rpc.listinfo", &[])?)?;
        let entries = (0..total)
            .map(|index| {
                decode::<(u16, String)>(ask("rpc.listinfo", &index.to_le_bytes())?)
                    .map(|(meta, name)| (name, meta))
            })
            .collect::<Result<cache::Entries, RpcRegistryError>>()?;
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

    pub fn find(&self, name: &str) -> Option<&RpcDescriptor> {
        self.rpcs.get(name)
    }

    pub fn names(&self) -> Vec<String> {
        self.rpcs.keys().cloned().collect()
    }

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
            replies.next().ok_or(CallError::Timeout)
        }) else {
            panic!("the walk runs out of replies");
        };
        (asked, error)
    }

    #[test]
    fn the_walk_asks_for_the_name_the_hash_then_each_descriptor() {
        // A hash nothing has cached, so the walk has to enumerate.
        let (asked, _) = walk_recording(vec![
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
    }

    #[test]
    fn a_reply_that_does_not_decode_fails_the_walk() {
        let (asked, error) = walk_recording(vec![b"test-device".to_vec(), vec![0, 1]]);

        assert_eq!(asked.len(), 2, "a two-byte hash should not decode");
        assert!(matches!(error, RpcRegistryError::DeviceRpcError(_)));
    }
}
