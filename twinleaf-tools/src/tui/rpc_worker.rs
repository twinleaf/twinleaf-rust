//! Background-thread worker for `RpcClient` calls, for TUI hosts whose event
//! loops can't block on RPC round-trips.
//!
//! Hosts call [`spawn_rpc_worker`] once, get a request/response channel pair,
//! send [`RpcWorkerReq`] variants (fetch RPC registry, execute RPC) and consume
//! [`RpcWorkerResp`] from the response side. The palette widget itself has no
//! awareness of this module — hosts wire palette events to worker requests.

use crossbeam::channel::{self, Receiver, Sender};
use twinleaf::device::{DeviceRoute, RpcClient, RpcRegistry};

use crate::tools::rpc::{encode_rpc_argument, format_rpc_value, resolve_rpc_type};
use crate::tui::rpc_palette::{RpcReq, RpcResp};

pub enum RpcWorkerReq {
    FetchRegistry(DeviceRoute),
    Execute(RpcReq),
}

pub enum RpcWorkerResp {
    Registry {
        route: DeviceRoute,
        registry: RpcRegistry,
    },
    RegistryErr {
        route: DeviceRoute,
        error: String,
    },
    RpcResult(RpcResp),
}

pub fn exec_rpc(client: &RpcClient, req: &RpcReq) -> Result<String, String> {
    // Honor an explicit `-t` override; otherwise infer from metadata.
    let kind = req.req_type.unwrap_or_else(|| {
        let meta = req.meta.or_else(|| {
            client
                .rpc::<&String, u16>(&req.route, "rpc.info", &req.method)
                .ok()
        });
        resolve_rpc_type(meta)
    });

    let payload = if let Some(ref s) = req.arg {
        encode_rpc_argument(s, kind).map_err(|e| e.to_string())?
    } else {
        Vec::new()
    };

    let reply_bytes = client
        .raw_rpc(&req.route, &req.method, &payload)
        .map_err(|e| e.to_string())?;

    if reply_bytes.is_empty() {
        return Ok("OK".to_string());
    }

    // `-T` overrides the reply type; default to decoding as the request type.
    let reply_kind = req.rep_type.unwrap_or(kind);
    let value = reply_kind.decode(&reply_bytes).map_err(|e| e.to_string())?;

    Ok(format_rpc_value(&value))
}

/// Spawn a worker thread that owns the given [`RpcClient`]. The returned
/// channel pair is suitable for embedding in a `crossbeam::select!` loop.
pub fn spawn_rpc_worker(client: RpcClient) -> (Sender<RpcWorkerReq>, Receiver<RpcWorkerResp>) {
    let (req_tx, req_rx) = channel::unbounded::<RpcWorkerReq>();
    let (resp_tx, resp_rx) = channel::unbounded::<RpcWorkerResp>();

    std::thread::spawn(move || {
        while let Ok(req) = req_rx.recv() {
            let resp = match req {
                RpcWorkerReq::FetchRegistry(route) => match client.registry(&route) {
                    Ok(registry) => Some(RpcWorkerResp::Registry { route, registry }),
                    Err(err) => Some(RpcWorkerResp::RegistryErr {
                        route,
                        error: err.to_string(),
                    }),
                },
                RpcWorkerReq::Execute(rpc_req) => {
                    let result = exec_rpc(&client, &rpc_req);
                    Some(RpcWorkerResp::RpcResult(RpcResp { result }))
                }
            };
            if let Some(resp) = resp {
                if resp_tx.send(resp).is_err() {
                    return;
                }
            }
        }
    });

    (req_tx, resp_rx)
}
