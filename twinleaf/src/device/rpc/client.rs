use crate::tio::{proto::DeviceRoute, proxy, util as tio_util};
use std::collections::HashMap;

use directories::BaseDirs;
use std::fs;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{self, BufRead, Write};

#[derive(Debug, thiserror::Error)]
pub enum RpcListError {
    #[error("could not locate cache directory")]
    CacheDirError,
    #[error("cached RPC list is corrupted or outdated")]
    InvalidCacheError,
    #[error("cache file I/O error: {0}")]
    CacheFileError(#[from] io::Error),
    #[error("RPC error: {0}")]
    DeviceRpcError(proxy::RpcError),
}

#[derive(Debug, Clone)]
pub struct RpcList {
    pub route: DeviceRoute,
    pub hash: u32,
    pub vec: Vec<(String, u16)>,
    pub map: HashMap<String, u16>,
}

pub struct RpcClient {
    port: proxy::Port,
    root_route: DeviceRoute,
}

impl RpcClient {
    pub fn new(port: proxy::Port, root_route: DeviceRoute) -> Self {
        Self { port, root_route }
    }

    pub fn open(proxy: &proxy::Interface, route: DeviceRoute) -> Result<Self, proxy::PortError> {
        let port = proxy.subtree_rpc(route.clone())?;
        Ok(Self::new(port, route))
    }

    pub fn root_route(&self) -> &DeviceRoute {
        &self.root_route
    }

    pub fn raw_rpc(
        &self,
        route: &DeviceRoute,
        name: &str,
        arg: &[u8],
    ) -> Result<Vec<u8>, proxy::RpcError> {
        let relative = self
            .root_route
            .relative_route(route)
            .unwrap_or_else(|_| route.clone());

        let req = tio_util::PacketBuilder::make_rpc_request(name, arg, 0, relative);
        self.port.send(req)?;

        loop {
            let pkt = self.port.recv()?;
            match pkt.payload {
                crate::tio::proto::Payload::RpcReply(rep) => return Ok(rep.reply),
                crate::tio::proto::Payload::RpcError(err) => {
                    return Err(proxy::RpcError::ExecError(err))
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
        Req: tio_util::TioRpcRequestable<Req>,
        Rep: tio_util::TioRpcReplyable<Rep>,
    {
        let ret = self.raw_rpc(route, name, &arg.to_request())?;
        Rep::from_reply(&ret).map_err(|_| proxy::RpcError::TypeError)
    }

    pub fn action(&self, route: &DeviceRoute, name: &str) -> Result<(), proxy::RpcError> {
        self.rpc(route, name, ())
    }

    pub fn get<T: tio_util::TioRpcReplyable<T>>(
        &self,
        route: &DeviceRoute,
        name: &str,
    ) -> Result<T, proxy::RpcError> {
        self.rpc(route, name, ())
    }

    fn read_rpc_cache(
        &self,
        route: &DeviceRoute,
        hash: u32,
        file: fs::File,
    ) -> Result<RpcList, RpcListError> {
        let reader = io::BufReader::new(file);
        let mut vec: Vec<(String, u16)> = Vec::new();
        let mut map: HashMap<String, u16> = HashMap::new();
        let mut hasher = DefaultHasher::new();
        let mut hash_line: Option<String> = None;

        for line in reader.lines() {
            let line = line?;

            let Some((meta, name)) = line.split_once(' ') else {
                // Line without a space is the trailing checksum
                hash_line = Some(line);
                break;
            };

            let meta =
                u16::from_str_radix(meta, 16).map_err(|_| RpcListError::InvalidCacheError)?;
            let name = name.trim().to_string();

            vec.push((name.clone(), meta));
            map.insert(name.clone(), meta);
            (name, meta).hash(&mut hasher);
        }

        match hash_line {
            Some(line) => {
                let cached_hash =
                    u64::from_str_radix(&line, 16).map_err(|_| RpcListError::InvalidCacheError)?;
                if cached_hash == hasher.finish() {
                    Ok(RpcList {
                        route: route.clone(),
                        hash,
                        vec,
                        map,
                    })
                } else {
                    Err(RpcListError::InvalidCacheError)
                }
            }
            None => Err(RpcListError::InvalidCacheError),
        }
    }

    fn write_rpc_cache(
        &self,
        route: &DeviceRoute,
        hash: u32,
        file: fs::File,
    ) -> Result<RpcList, RpcListError> {
        let mut writer = io::BufWriter::new(file);
        let mut vec: Vec<(String, u16)> = Vec::new();
        let mut map: HashMap<String, u16> = HashMap::new();
        let mut hasher = DefaultHasher::new();

        let nrpcs: u16 = self
            .get(route, "rpc.listinfo")
            .map_err(RpcListError::DeviceRpcError)?;

        for id in 0..nrpcs {
            let (meta, name): (u16, String) = self
                .rpc(route, "rpc.listinfo", id)
                .map_err(RpcListError::DeviceRpcError)?;
            writeln!(writer, "{:04x} {}", meta, name)?;

            vec.push((name.clone(), meta));
            map.insert(name.clone(), meta);
            (name, meta).hash(&mut hasher);
        }

        writeln!(writer, "{:016x}", hasher.finish())?;
        Ok(RpcList {
            route: route.clone(),
            hash,
            vec,
            map,
        })
    }

    pub fn rpc_list(&self, route: &DeviceRoute) -> Result<RpcList, RpcListError> {
        let tl_cache_dir = BaseDirs::new()
            .ok_or(RpcListError::CacheDirError)?
            .cache_dir()
            .join("twinleaf");
        fs::create_dir_all(&tl_cache_dir).map_err(|_| RpcListError::CacheDirError)?;

        let dev_name: String = self
            .get(route, "dev.name")
            .map_err(RpcListError::DeviceRpcError)?;
        let hash: u32 = self
            .get(route, "rpc.hash")
            .map_err(RpcListError::DeviceRpcError)?;
        // TODO: evict stale cache files from old firmware versions (<dev_name>.*.rpcs)
        let base_name = format!("{}.{:x}.rpcs", dev_name, hash);
        let file_path = tl_cache_dir.join(&base_name);

        // Remove empty files left by aborted writes
        match fs::metadata(&file_path) {
            Ok(metadata) => match metadata.len() {
                0 => fs::remove_file(&file_path),
                _ => Ok(()),
            },
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(other_err) => Err(other_err),
        }?;

        let cache_file = fs::File::open(&file_path);
        match cache_file {
            Ok(file) => match self.read_rpc_cache(route, hash, file) {
                Ok(rpclist) => Ok(rpclist),
                Err(RpcListError::InvalidCacheError) => {
                    fs::remove_file(&file_path)?;
                    let cache_file = fs::File::create(&file_path)?;
                    self.write_rpc_cache(route, hash, cache_file)
                }
                Err(other_err) => Err(other_err),
            },
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                let cache_file = fs::File::create(&file_path)?;
                self.write_rpc_cache(route, hash, cache_file)
            }
            Err(other_err) => Err(RpcListError::CacheFileError(other_err)),
        }
    }
}
