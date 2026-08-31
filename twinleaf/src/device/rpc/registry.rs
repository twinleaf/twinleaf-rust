use crate::tio::proto::RpcMeta;
use std::collections::{BTreeMap, BTreeSet};

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

    pub fn find(&self, name: &str) -> Option<&RpcDescriptor> {
        self.rpcs.get(name)
    }

    pub fn names(&self) -> Vec<String> {
        self.rpcs.keys().cloned().collect()
    }

    pub fn name_set(&self) -> BTreeSet<String> {
        self.rpcs.keys().cloned().collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = &RpcDescriptor> + '_ {
        self.rpcs.values()
    }

    pub fn children_of(&self, prefix: &str) -> Vec<String> {
        let lo = if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}.")
        };
        let mut out: Vec<String> = Vec::new();
        for (name, _) in self.rpcs.range(lo.clone()..) {
            let Some(rest) = name.strip_prefix(&lo) else {
                break;
            };
            let seg = rest.split('.').next().unwrap_or(rest);
            if out.last().map(String::as_str) != Some(seg) {
                out.push(seg.to_string());
            }
        }
        out
    }
}
