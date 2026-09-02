//! The two RPC things a TUI event loop selects on: the palette's call, and the
//! registry loads it asks for. Each runs on one worker thread so the loop only
//! ever receives finished results.

use crossbeam::channel::{self, Receiver, Sender};
use std::collections::HashSet;
use twinleaf::device::rpc::{RpcRegistry, RpcValueTypeExt};
use twinleaf::device::{DeviceRoute, DeviceTree};

use crate::tools::rpc::{encode_rpc_argument, format_rpc_value, resolve_rpc_type};
use crate::tui::rpc_palette::RpcReq;

/// One route's finished registry load, or why it failed.
pub type LoadedRegistry = (DeviceRoute, Result<RpcRegistry, String>);

fn spawn<Req: Send + 'static, Res: Send + 'static>(
    name: &str,
    mut serve: impl FnMut(Req) -> Res + Send + 'static,
) -> (Sender<Req>, Receiver<Res>) {
    let (requests, incoming) = channel::unbounded::<Req>();
    let (outgoing, results) = channel::unbounded::<Res>();
    std::thread::Builder::new()
        .name(name.into())
        .spawn(move || {
            for request in incoming {
                if outgoing.send(serve(request)).is_err() {
                    return;
                }
            }
        })
        .expect("failed to spawn a TUI RPC worker thread");
    (requests, results)
}

/// Registry loads for a tree of devices, one walk at a time.
pub struct RegistryQueue {
    requests: Sender<DeviceRoute>,
    results: Receiver<LoadedRegistry>,
    outstanding: HashSet<DeviceRoute>,
}

impl RegistryQueue {
    pub fn new(tree: DeviceTree) -> RegistryQueue {
        let (requests, results) = spawn("twinleaf-registry", move |route| {
            let registry = tree.rpc_registry(route).map_err(|e| e.to_string());
            (route, registry)
        });
        RegistryQueue {
            requests,
            results,
            outstanding: HashSet::new(),
        }
    }

    /// The channel the next registry arrives on.
    pub fn receiver(&self) -> &Receiver<LoadedRegistry> {
        &self.results
    }

    /// Ask for `route`'s registry, unless its last request is still running.
    pub fn fetch(&mut self, route: DeviceRoute) {
        if self.outstanding.insert(route) && self.requests.send(route).is_err() {
            self.outstanding.remove(&route);
        }
    }

    /// Feed back a message taken from [`receiver`](Self::receiver), freeing its
    /// route to be fetched again.
    pub fn resolve(&mut self, loaded: LoadedRegistry) -> LoadedRegistry {
        self.outstanding.remove(&loaded.0);
        loaded
    }
}

/// The palette's in-flight call.
///
/// A listed RPC carries its type in the registry; a hand-typed name that is not
/// listed needs an `rpc.info` round trip first, which the worker does inline.
pub struct PendingRpc {
    requests: Sender<RpcReq>,
    results: Receiver<Result<String, String>>,
}

impl PendingRpc {
    pub fn new(tree: DeviceTree) -> PendingRpc {
        let (requests, results) = spawn("twinleaf-rpc", move |req| call(&tree, req));
        PendingRpc { requests, results }
    }

    /// The channel the next result arrives on.
    pub fn receiver(&self) -> &Receiver<Result<String, String>> {
        &self.results
    }

    /// Issue `req`. `Some` only when it could not be handed to the worker.
    pub fn start(&mut self, req: RpcReq) -> Option<Result<String, String>> {
        self.requests
            .send(req)
            .err()
            .map(|_| Err("the RPC worker stopped".to_string()))
    }
}

fn call(tree: &DeviceTree, req: RpcReq) -> Result<String, String> {
    // Honor an explicit `-t` override, else the registry's metadata, else ask
    // the device what the argument is.
    let kind = match req
        .req_type
        .or_else(|| req.meta.map(|meta| resolve_rpc_type(Some(meta))))
    {
        Some(kind) => kind,
        None => resolve_rpc_type(
            tree.raw_rpc(req.route, "rpc.info", req.method.as_bytes())
                .ok()
                .and_then(|raw| <[u8; 2]>::try_from(raw.as_slice()).ok())
                .map(u16::from_le_bytes),
        ),
    };

    let payload = match req.arg.as_deref() {
        Some(arg) => encode_rpc_argument(arg, kind).map_err(|error| error.to_string())?,
        None => Vec::new(),
    };
    let reply = tree
        .raw_rpc(req.route, &req.method, &payload)
        .map_err(|error| error.to_string())?;
    if reply.is_empty() {
        return Ok("OK".to_string());
    }
    // `-T` overrides the reply type; default to the request type.
    req.rep_type
        .unwrap_or(kind)
        .decode(&reply)
        .map(|value| format_rpc_value(&value))
        .map_err(|error| error.to_string())
}
