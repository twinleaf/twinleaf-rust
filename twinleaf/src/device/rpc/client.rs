use super::cache;
use super::{RpcDescriptor, RpcRegistry};
use crate::tio::{proto, proto::DeviceRoute, proto::RpcArgs, proto::RpcReply, proxy};

use directories::BaseDirs;
use std::fs;
use std::io;

#[derive(Debug, thiserror::Error)]
pub enum RpcRegistryError {
    #[error("could not locate cache directory")]
    CacheDirError,
    #[error("cache file I/O error: {0}")]
    CacheFileError(#[from] io::Error),
    #[error("RPC error: {0}")]
    DeviceRpcError(proxy::RpcError),
}

pub struct RpcClient {
    port: proxy::Port,
}

/// Cache filename stem for a device-supplied name. The name is untrusted — a
/// network device could report `../..` — so keep it to a charset that cannot
/// escape the cache directory; the hash still makes the filename unique.
fn cache_stem(dev_name: &str) -> String {
    dev_name
        .chars()
        .take(64)
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => c,
            _ => '_',
        })
        .collect()
}

/// Keeping the cache tidy is an optimization, never a reason to fail a registry
/// the device already answered for — an unwritable cache directory only costs a
/// round-trip next time.
fn warn_cache(result: io::Result<()>, action: &str, path: &std::path::Path) {
    if let Err(error) = result {
        log::warn!("could not {action} RPC cache {}: {error}", path.display());
    }
}

fn make_registry(entries: cache::Entries, hash: u32) -> RpcRegistry {
    let specs = entries
        .into_iter()
        .map(|(name, meta)| RpcDescriptor::from_meta(meta, name))
        .collect();
    let mut registry = RpcRegistry::new(specs);
    registry.hash = Some(hash);
    registry
}

impl RpcClient {
    pub fn new(port: proxy::Port) -> Self {
        Self { port }
    }

    pub fn open(proxy: &proxy::Interface, route: DeviceRoute) -> Result<Self, proxy::PortError> {
        Ok(Self::new(proxy.subtree_rpc(route)?))
    }

    pub fn root_route(&self) -> DeviceRoute {
        self.port.scope()
    }

    pub fn raw_rpc(
        &self,
        route: &DeviceRoute,
        name: &str,
        arg: &[u8],
    ) -> Result<Vec<u8>, proxy::RpcError> {
        let relative = self
            .port
            .scope()
            .relative_route(route)
            .map_err(|_| proxy::RpcError::InvalidRoute)?;

        let req = proto::Packet::rpc_request(name, arg, 0, relative);
        self.port.send(req)?;

        loop {
            let pkt = self
                .port
                .recv()
                .map_err(|_| proxy::RpcError::ResponseLost)?;
            match pkt.payload {
                crate::tio::proto::Payload::RpcReply(rep) => return Ok(rep.reply),
                crate::tio::proto::Payload::RpcError(err) => {
                    return Err(proxy::RpcError::DeviceError(err))
                }
                _ => continue,
            }
        }
    }

    pub fn rpc<Req, Rep>(
        &self,
        route: &DeviceRoute,
        name: &str,
        arg: Req,
    ) -> Result<Rep, proxy::RpcError>
    where
        Req: RpcArgs,
        Rep: RpcReply,
    {
        let ret = self.raw_rpc(route, name, &arg.encode_args())?;
        Rep::decode_reply(&ret).map_err(proxy::RpcError::InvalidReply)
    }

    pub fn action(&self, route: &DeviceRoute, name: &str) -> Result<(), proxy::RpcError> {
        self.rpc(route, name, ())
    }

    pub fn get<T: RpcReply>(&self, route: &DeviceRoute, name: &str) -> Result<T, proxy::RpcError> {
        self.rpc(route, name, ())
    }

    fn fetch_registry_entries(
        &self,
        route: &DeviceRoute,
    ) -> Result<cache::Entries, RpcRegistryError> {
        let nrpcs: u16 = self
            .get(route, "rpc.listinfo")
            .map_err(RpcRegistryError::DeviceRpcError)?;
        let mut entries = Vec::with_capacity(nrpcs.into());

        for id in 0..nrpcs {
            let (meta, name): (u16, String) = self
                .rpc(route, "rpc.listinfo", id)
                .map_err(RpcRegistryError::DeviceRpcError)?;
            entries.push((name, meta));
        }
        Ok(entries)
    }

    pub fn registry(&self, route: &DeviceRoute) -> Result<RpcRegistry, RpcRegistryError> {
        let cache_dir = BaseDirs::new()
            .ok_or(RpcRegistryError::CacheDirError)?
            .cache_dir()
            .join("twinleaf");
        fs::create_dir_all(&cache_dir).map_err(|_| RpcRegistryError::CacheDirError)?;

        let dev_name: String = self
            .get(route, "dev.name")
            .map_err(RpcRegistryError::DeviceRpcError)?;
        let hash: u32 = self
            .get(route, "rpc.hash")
            .map_err(RpcRegistryError::DeviceRpcError)?;
        // TODO: evict stale cache files from old firmware versions (<dev_name>.*.rpcs)
        let cache_path = cache_dir.join(format!("{}.{hash:x}.rpcs", cache_stem(&dev_name)));

        match fs::File::open(&cache_path) {
            Ok(file) => match cache::read(file)? {
                Some(entries) => return Ok(make_registry(entries, hash)),
                None => warn_cache(fs::remove_file(&cache_path), "discard stale", &cache_path),
            },
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(RpcRegistryError::CacheFileError(err)),
        }

        let entries = self.fetch_registry_entries(route)?;
        warn_cache(cache::write(&cache_path, &entries), "write", &cache_path);
        Ok(make_registry(entries, hash))
    }
}

#[cfg(test)]
mod tests {
    use super::cache_stem;

    #[test]
    fn cache_stem_cannot_escape_the_cache_directory() {
        assert_eq!(cache_stem("../../etc/passwd"), "______etc_passwd");
        assert_eq!(cache_stem("sync-v2"), "sync-v2");
    }
}
