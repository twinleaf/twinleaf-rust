//! Background-thread worker for `RpcClient` calls, for TUI hosts whose event
//! loops can't block on RPC round-trips.
//!
//! Hosts call [`spawn_rpc_worker`] once, get a request/response channel pair,
//! send [`RpcWorkerReq`] variants (fetch RPC list, execute RPC) and consume
//! [`RpcWorkerResp`] from the response side. The palette widget itself has no
//! awareness of this module — hosts wire palette events to worker requests.

use crossbeam::channel::{self, Receiver, Sender};
use tio::proto::DeviceRoute;
use twinleaf::{
    device::{util, RpcClient, RpcList, RpcValue},
    tio,
};

use crate::tui::rpc_palette::{RpcReq, RpcResp};

pub enum RpcWorkerReq {
    FetchList(DeviceRoute),
    Execute(RpcReq),
}

pub enum RpcWorkerResp {
    List(RpcList),
    RpcResult(RpcResp),
}

pub fn exec_rpc(client: &RpcClient, req: &RpcReq) -> Result<String, String> {
    let meta = match req.meta {
        Some(m) => m,
        None => client
            .rpc(&req.route, "rpc.info", &req.method)
            .map_err(|_| format!("Unknown RPC: {}", req.method))?,
    };

    let spec = util::parse_rpc_spec(meta, req.method.clone());

    let payload = if let Some(ref s) = req.arg {
        util::rpc_encode_arg(s, &spec.data_kind).map_err(|e| format!("{:?}", e))?
    } else {
        Vec::new()
    };

    let reply_bytes = client
        .raw_rpc(&req.route, &req.method, &payload)
        .map_err(|e| format!("{:?}", e))?;

    if reply_bytes.is_empty() {
        return Ok("OK".to_string());
    }

    let value =
        util::rpc_decode_reply(&reply_bytes, &spec.data_kind).map_err(|e| format!("{:?}", e))?;

    Ok(match &value {
        RpcValue::Str(s) => format!("\"{}\" {:?}", s, s.as_bytes()),
        RpcValue::Bytes(b) => format!("{:?}", b),
        other => format!("{}", other),
    })
}

/// Spawn a worker thread that owns the given [`RpcClient`]. The returned
/// channel pair is suitable for embedding in a `crossbeam::select!` loop.
pub fn spawn_rpc_worker(client: RpcClient) -> (Sender<RpcWorkerReq>, Receiver<RpcWorkerResp>) {
    let (req_tx, req_rx) = channel::unbounded::<RpcWorkerReq>();
    let (resp_tx, resp_rx) = channel::unbounded::<RpcWorkerResp>();

    std::thread::spawn(move || {
        while let Ok(req) = req_rx.recv() {
            let resp = match req {
                RpcWorkerReq::FetchList(route) => match client.rpc_list(&route) {
                    Ok(list) => Some(RpcWorkerResp::List(list)),
                    Err(_) => None,
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
