use crate::device::util;
use crate::tio::{proto::DeviceRoute, proxy, util as tio_util};
use std::collections::{BTreeMap, HashMap};

// File I/O for cacheing
use std::io::{self, BufRead, Write};
use std::fs;
use dirs_next::cache_dir;

#[derive(Debug, Clone)]
pub enum RpcValue {
    Unit,
    U64(u64),
    I64(i64),
    F64(f64),
    Str(String),
    Bytes(Vec<u8>),
}

#[derive(Debug)]
pub enum EncodeError {
    ParseInt(std::num::ParseIntError),
    ParseFloat(std::num::ParseFloatError),
    StringTooLong { max: u16, actual: usize },
    UnsupportedIntSize(u8),
    UnsupportedFloatSize(u8),
    NotEncodableForKind(&'static str),
}

impl From<std::num::ParseIntError> for EncodeError {
    fn from(e: std::num::ParseIntError) -> Self {
        EncodeError::ParseInt(e)
    }
}
impl From<std::num::ParseFloatError> for EncodeError {
    fn from(e: std::num::ParseFloatError) -> Self {
        EncodeError::ParseFloat(e)
    }
}

#[derive(Debug)]
pub enum DecodeError {
    InsufficientBytes { expected: usize, got: usize },
    Utf8(std::str::Utf8Error),
    UnsupportedIntSize(u8),
    UnsupportedFloatSize(u8),
}

impl From<std::str::Utf8Error> for DecodeError {
    fn from(e: std::str::Utf8Error) -> Self {
        DecodeError::Utf8(e)
    }
}

#[derive(Debug, Clone)]
pub enum RpcDataKind {
    Unit,
    Int { signed: bool, size: u8 },
    Float { size: u8 },
    String { max_len: Option<u16> },
    Raw { meta: u16 },
}

#[derive(Debug, Clone)]
pub struct RpcMeta {
    pub full_name: String,     // "dev.port.rate.min"
    pub segments: Vec<String>, // ["dev", "port", "rate", "min"]
    pub data_kind: RpcDataKind,
    pub readable: bool,
    pub writable: bool,
    pub persistent: bool,
    pub meta_raw: u16,
}

impl RpcMeta {
    pub fn is_unknown(&self) -> bool {
        self.meta_raw == 0
    }

    pub fn perm_str(&self) -> String {
        if self.is_unknown() {
            "???".to_string()
        } else {
            format!(
                "{}{}{}",
                if self.readable { "R" } else { "-" },
                if self.writable { "W" } else { "-" },
                if self.persistent { "P" } else { "-" },
            )
        }
    }

    pub fn type_str(&self) -> String {
        match self.data_kind {
            RpcDataKind::Unit => "".to_string(),
            RpcDataKind::Int { signed, size } => {
                let bits = (size as usize) * 8;
                if signed {
                    format!("i{bits}")
                } else {
                    format!("u{bits}")
                }
            }
            RpcDataKind::Float { size } => {
                let bits = (size as usize) * 8;
                format!("f{bits}")
            }
            RpcDataKind::String { max_len } => {
                if let Some(n) = max_len {
                    format!("string<{n}>")
                } else {
                    "string".to_string()
                }
            }
            RpcDataKind::Raw { .. } => "".to_string(),
        }
    }

    pub fn size_bytes(&self) -> Option<usize> {
        match self.data_kind {
            RpcDataKind::Unit => Some(0),
            RpcDataKind::Int { size, .. } => Some(size as usize),
            RpcDataKind::Float { size } => Some(size as usize),
            RpcDataKind::String { .. } => None,
            RpcDataKind::Raw { .. } => None,
        }
    }
}

#[derive(Default)]
struct RpcNode {
    children: BTreeMap<String, RpcNode>,
    rpc: Option<RpcMeta>,
}

impl RpcNode {
    fn insert(&mut self, segments: &[String], spec: RpcMeta) {
        if let Some((first, rest)) = segments.split_first() {
            let child = self.children.entry(first.clone()).or_default();
            child.insert(rest, spec);
        } else {
            self.rpc = Some(spec);
        }
    }

    fn completions(&self, prefix_segments: &[String]) -> Vec<String> {
        if let Some((first, rest)) = prefix_segments.split_first() {
            if let Some(child) = self.children.get(first) {
                return child.completions(rest);
            }

            if rest.is_empty() {
                return self
                    .children
                    .keys()
                    .filter(|k| k.starts_with(first))
                    .cloned()
                    .collect();
            }
            return vec![];
        }

        self.children.keys().cloned().collect()
    }

    fn find(&self, segments: &[String]) -> Option<&RpcMeta> {
        if let Some((first, rest)) = segments.split_first() {
            self.children.get(first)?.find(rest)
        } else {
            self.rpc.as_ref()
        }
    }
}

pub struct RpcRegistry {
    root: RpcNode,
    flat: HashMap<String, RpcMeta>,
}

impl RpcRegistry {
    pub fn new(specs: Vec<RpcMeta>) -> Self {
        let mut root = RpcNode::default();
        let mut flat = HashMap::new();

        for spec in specs {
            flat.insert(spec.full_name.clone(), spec.clone());
            root.insert(&spec.segments, spec.clone());
        }
        Self { root, flat }
    }

    pub fn find(&self, name: &str) -> Option<&RpcMeta> {
        let parts: Vec<String> = name.split('.').map(|s| s.to_string()).collect();
        self.root.find(&parts)
    }

    pub fn suggest(&self, query: &str) -> Vec<String> {
        let parts: Vec<String> = query.split('.').map(|s| s.to_string()).collect();
        let suffixes = self.root.completions(&parts);

        if parts.is_empty() {
            return suffixes;
        }

        let prefix = parts.join(".");

        suffixes
            .into_iter()
            .map(|s| format!("{prefix}.{s}"))
            .collect()
    }

    pub fn children_of(&self, prefix: &str) -> Vec<String> {
        if prefix.is_empty() {
            return self.root.children.keys().cloned().collect();
        }

        let parts: Vec<String> = prefix.split('.').map(|s| s.to_string()).collect();

        let mut current = &self.root;
        for part in &parts {
            match current.children.get(part) {
                Some(node) => current = node,
                None => return vec![],
            }
        }

        current.children.keys().cloned().collect()
    }

    pub fn prepare_request(&self, input_line: &str) -> Result<(String, Vec<u8>), String> {
        let parts: Vec<&str> = input_line.split_whitespace().collect();
        if parts.is_empty() {
            return Err("Empty command".into());
        }

        let name = parts[0];
        let arg_str = parts.get(1).unwrap_or(&"");

        let meta = self
            .flat
            .get(name)
            .ok_or_else(|| format!("Unknown RPC: {}", name))?;

        let payload = util::rpc_encode_arg(arg_str, &meta.data_kind)
            .map_err(|e| format!("Encoding error: {:?}", e))?;

        Ok((name.to_string(), payload))
    }

    pub fn decode_response(&self, name: &str, data: &[u8]) -> Result<String, String> {
        let meta = self
            .flat
            .get(name)
            .ok_or_else(|| format!("Unknown RPC: {}", name))?;

        let val = util::rpc_decode_reply(data, &meta.data_kind)
            .map_err(|e| format!("Decode error: {:?}", e))?;

        Ok(util::format_rpc_value_for_cli(&val, &meta.data_kind))
    }
}

impl From<io::Error> for RpcListError {
    fn from(e: io::Error) -> Self {
        RpcListError::CacheFileError(e)
    }
}

#[derive(Debug)]
pub enum RpcListError {
    CacheDirError,
    CacheCreateError,

    DevNameRpcError,
    RpcHashError,

    NumRpcsError,
    RpcListError,
    CacheWriteError,
    RemoveBadCacheError,

    CacheReadError,
    InvalidCacheError,

    CacheFileError(io::Error),
}

#[derive(Debug)]
pub struct RpcList {
    pub route: DeviceRoute,
    pub hash: u32,
    pub list: Vec<(u16, String)>,
}

pub struct RpcClient {
    port: proxy::Port,
    root_route: DeviceRoute,
}

impl RpcClient {
    pub fn new(port: proxy::Port, root_route: DeviceRoute) -> Self {
        Self { port, root_route, }
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

    pub fn get<T: tio_util::TioRpcReplyable<T>>( &self, route: &DeviceRoute,
        name: &str,
    ) -> Result<T, proxy::RpcError> {
        self.rpc(route, name, ())
    }

    fn read_rpc_cache(&self, file: fs::File) -> Result<Vec<(u16, String)>, RpcListError> {
        let mut list: Vec<(u16, String)> = Vec::new();
        let reader = io::BufReader::new(file);

        for line in reader.lines() {
            let line = line.map_err(|_| RpcListError::CacheReadError)?;
            let (meta, name) = line.split_once(' ').ok_or(RpcListError::InvalidCacheError)?;
            let meta_hex = u16::from_str_radix(meta, 16).map_err(|_|
                RpcListError::InvalidCacheError)?;
            let name_string = name.trim().to_string();
            list.push((meta_hex, name_string));
        }

        return Ok(list);
    }

    fn write_rpc_cache(&self, route: &DeviceRoute, file: fs::File) -> Result<Vec<(u16, String)>, RpcListError> {
        let mut list: Vec<(u16, String)> = Vec::new();
        let mut writer = io::BufWriter::new(file);

        let nrpcs: u16 = self.get(route, "rpc.listinfo").map_err(|_| RpcListError::NumRpcsError)?;

        for id in 0..nrpcs {
            let (meta, name): (u16, String) = self.rpc(route, "rpc.listinfo", id).map_err(|_|
                RpcListError::RpcListError)?;
            writeln!(writer, "{:04x} {}", meta, name).map_err(|_|
                RpcListError::CacheWriteError)?;
            list.push((meta, name));
        }

        return Ok(list);
    }

    pub fn rpc_list(&self, route: &DeviceRoute) -> Result<RpcList, RpcListError> {
        // Get/create cache directory
        let cache_parent_dir = cache_dir().ok_or(RpcListError::CacheDirError)?;
        let tl_cache_dir = cache_parent_dir.join("twinleaf");
        fs::create_dir_all(&tl_cache_dir).map_err(|_| RpcListError::CacheDirError)?;

        // Get cache file path
        let dev_name: String = self.get(route, "dev.name").map_err(|_| RpcListError::DevNameRpcError)?;
        let hash: u32 = self.get(route, "rpc.hash").map_err(|_| RpcListError::RpcHashError)?;
        let base_name = format!("{}.{:x}.rpcs", dev_name, hash);
        let file_path = tl_cache_dir.join(&base_name);

        let cache_file = fs::File::open(&file_path);

        // TODO: write more identifying info to cache file?
        // Date created, firmware version, validation hash, etc.
        // Maybe on an ignored line at the top

        match cache_file {
            Ok(file) => {
                let list = self.read_rpc_cache(file)?;
                Ok(RpcList { route: route.clone(), hash, list } )
            }

            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                let cache_file = fs::File::create(&file_path).map_err(|_|
                    RpcListError::CacheCreateError)?;

                // Try to write, and if we fail, remove the file we created
                let list = self.write_rpc_cache(route, cache_file).map_err(|orig_error| {
                    match fs::remove_file(file_path) {
                        Ok(_) => orig_error,
                        Err(_) => RpcListError::RemoveBadCacheError,
                    }
                })?;
                Ok(RpcList{ route: route.clone(), hash, list} )
            },

            // TODO: what other io errors to handle?
            Err(other_err) => Err(RpcListError::CacheFileError(other_err)),
        }
    }
}

