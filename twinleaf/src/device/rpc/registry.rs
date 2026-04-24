use super::client::RpcList;
use super::value::RpcValueType;
use crate::device::util;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct RpcDescriptor {
    pub full_name: String,
    pub segments: Vec<String>,
    pub data_kind: RpcValueType,
    pub readable: bool,
    pub writable: bool,
    pub persistent: bool,
    pub meta_raw: u16,
}

impl RpcDescriptor {
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
            RpcValueType::Unit => "".to_string(),
            RpcValueType::Int { signed, size } => {
                let bits = (size as usize) * 8;
                if signed {
                    format!("i{bits}")
                } else {
                    format!("u{bits}")
                }
            }
            RpcValueType::Float { size } => {
                let bits = (size as usize) * 8;
                format!("f{bits}")
            }
            RpcValueType::String { max_len } => {
                if let Some(n) = max_len {
                    format!("string<{n}>")
                } else {
                    "string".to_string()
                }
            }
            RpcValueType::Raw { .. } => "".to_string(),
        }
    }

    pub fn size_bytes(&self) -> Option<usize> {
        match self.data_kind {
            RpcValueType::Unit => Some(0),
            RpcValueType::Int { size, .. } => Some(size as usize),
            RpcValueType::Float { size } => Some(size as usize),
            RpcValueType::String { .. } => None,
            RpcValueType::Raw { .. } => None,
        }
    }
}

#[derive(Default)]
struct RpcNode {
    children: BTreeMap<String, RpcNode>,
    rpc: Option<RpcDescriptor>,
}

impl RpcNode {
    fn insert(&mut self, segments: &[String], spec: RpcDescriptor) {
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

    fn find(&self, segments: &[String]) -> Option<&RpcDescriptor> {
        if let Some((first, rest)) = segments.split_first() {
            self.children.get(first)?.find(rest)
        } else {
            self.rpc.as_ref()
        }
    }
}

pub struct RpcRegistry {
    root: RpcNode,
    names: Vec<String>,
    pub hash: Option<u32>,
}

impl RpcRegistry {
    pub fn new(specs: Vec<RpcDescriptor>) -> Self {
        let mut root = RpcNode::default();
        let mut names = Vec::with_capacity(specs.len());
        for spec in specs {
            names.push(spec.full_name.clone());
            let segments = spec.segments.clone();
            root.insert(&segments, spec);
        }
        Self {
            root,
            names,
            hash: None,
        }
    }

    pub fn find(&self, name: &str) -> Option<&RpcDescriptor> {
        let parts: Vec<String> = name.split('.').map(|s| s.to_string()).collect();
        self.root.find(&parts)
    }

    pub fn names(&self) -> &[String] {
        &self.names
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

    /// Substring search across all RPC names. Returns prefix matches first,
    /// then non-prefix substring matches.
    pub fn search(&self, query: &str) -> Vec<String> {
        let mut prefix = Vec::new();
        let mut substring = Vec::new();
        for name in &self.names {
            if name.starts_with(query) {
                prefix.push(name.clone());
            } else if name.contains(query) {
                substring.push(name.clone());
            }
        }
        prefix.sort();
        substring.sort();
        prefix.extend(substring);
        prefix
    }
}

impl From<&RpcList> for RpcRegistry {
    fn from(list: &RpcList) -> Self {
        let specs: Vec<RpcDescriptor> = list
            .vec
            .iter()
            .map(|(name, meta)| util::parse_rpc_spec(*meta, name.clone()))
            .collect();
        let mut registry = Self::new(specs);
        registry.hash = Some(list.hash);
        registry
    }
}
