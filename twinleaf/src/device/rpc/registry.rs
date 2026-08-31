//! The set of RPCs a device offers, learned by walking `rpc.listinfo`.

use super::cache;
use super::{CallError, RpcReply};

use std::collections::{BTreeMap, VecDeque};
use std::io;
use twinleaf_proto::rpc::RpcMeta;

/// Most `rpc.listinfo` fetches in flight at once during a walk, bounding the
/// request burst a memory-tight device must absorb.
const WALK_WINDOW: usize = 8;

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

    /// Walk one device's RPC table on a blocking surface, keeping up to
    /// [`WALK_WINDOW`] independent fetches in flight to hide round trips.
    ///
    /// `dev.name` and `rpc.hash` identify the on-disk cache entry; only a miss
    /// enumerates `rpc.listinfo`. `submit` starts a call; the closure it
    /// returns blocks for that call's reply.
    pub(crate) fn load_with<Wait: FnOnce() -> Result<Vec<u8>, CallError>>(
        mut submit: impl FnMut(&str, &[u8]) -> Result<Wait, CallError>,
    ) -> Result<Self, RpcRegistryError> {
        let mut start =
            |name: &str, arg: &[u8]| submit(name, arg).map_err(RpcRegistryError::DeviceRpcError);
        let finish = |wait: Wait| wait().map_err(RpcRegistryError::DeviceRpcError);
        let entry = |reply| decode::<(u16, String)>(reply).map(|(meta, name)| (name, meta));

        let name_wait = start("dev.name", &[])?;
        let hash_wait = start("rpc.hash", &[])?;
        let dev_name: String = decode(finish(name_wait)?)?;
        let hash: u32 = decode(finish(hash_wait)?)?;
        let path = cache::path(&dev_name, hash).ok_or(RpcRegistryError::CacheDirError)?;
        if let Some(entries) = cache::load(&path)? {
            return Ok(Self::from_entries(entries, hash));
        }
        let total: u16 = decode(finish(start("rpc.listinfo", &[])?)?)?;
        let mut in_flight = VecDeque::with_capacity(WALK_WINDOW);
        let mut entries = cache::Entries::with_capacity(total.into());
        for index in 0..total {
            if in_flight.len() == WALK_WINDOW {
                let oldest = in_flight.pop_front().expect("full window is nonempty");
                entries.push(entry(finish(oldest)?)?);
            }
            in_flight.push_back(start("rpc.listinfo", &index.to_le_bytes())?);
        }
        for wait in in_flight {
            entries.push(entry(finish(wait)?)?);
        }
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
            let reply = replies.next();
            Ok(move || reply.ok_or(CallError::Timeout))
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

    /// One reply short of `total`, so the walk fails on the last wait and
    /// never writes the on-disk cache; every submission still happens first.
    #[test]
    fn descriptor_fetches_run_a_window_ahead_of_their_replies() {
        let total = 10u16;
        let events = std::cell::RefCell::new(Vec::new());
        let mut replies = [
            b"test-device".to_vec(),
            0xfeed_f00du32.to_le_bytes().to_vec(),
            total.to_le_bytes().to_vec(),
        ]
        .into_iter()
        .chain((0..total - 1).map(|i| descriptor(&format!("rpc{i}"))));

        let result = RpcRegistry::load_with(|name, arg| {
            events.borrow_mut().push(format!("ask {name} {arg:?}"));
            let reply = replies.next();
            let events = &events;
            Ok(move || {
                events.borrow_mut().push("wait".to_string());
                reply.ok_or(CallError::Timeout)
            })
        });

        assert!(matches!(
            result,
            Err(RpcRegistryError::DeviceRpcError(CallError::Timeout))
        ));
        let events = events.into_inner();
        let pos = |needle: &str| {
            events
                .iter()
                .position(|e| e == needle)
                .expect("event missing")
        };
        let descriptor_waits: Vec<usize> = events
            .iter()
            .enumerate()
            .filter(|(_, e)| *e == "wait")
            .map(|(i, _)| i)
            .skip(3)
            .collect();
        assert!(pos("ask rpc.listinfo [7, 0]") < descriptor_waits[0]);
        assert!(pos("ask rpc.listinfo [9, 0]") < *descriptor_waits.last().unwrap());
    }

    #[test]
    fn a_reply_that_does_not_decode_fails_the_walk() {
        let (asked, error) = walk_recording(vec![b"test-device".to_vec(), vec![0, 1]]);

        assert_eq!(asked.len(), 2, "a two-byte hash should not decode");
        assert!(matches!(error, RpcRegistryError::DeviceRpcError(_)));
    }
}
