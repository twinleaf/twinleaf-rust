//! The door for agents: an MCP endpoint at `/mcp`, over Streamable HTTP on
//! the hub's own connection, behind the same guard as every other route.
//!
//! It answers 404 while closed. Open, it serves the tools below, each a
//! translation of what the hub and the core already read into text an agent
//! reads cheaply, and an image where a picture says more. It holds no state
//! of its own. Each request is served on its own, with no session, and its
//! reply is one JSON message.
//!
//! - `list_devices` names each device, its streams and the signals in them.
//! - `list_rpcs` lists a device's RPCs and the values it announced.
//! - `call` calls one RPC on several devices, all or none.
//! - `glance` sums signals up over a span and draws them, in time or as
//!   spectra.
//! - `read_rows` gives exact rows of one signal, a few at a time.
//! - `export` writes every row of a span to a file.
//! - `log` gives the newest lines of the hub's log.
//!
//! A signal is named as the page names it, `stream.column` on the device at
//! the root and `0/stream.column` on the device at `/0`. Times are each
//! stream's own, in seconds. A span is the newest `seconds` of the signals,
//! or what `t0` and `t1` bound.
//!
//! Each call is a line of the log, said by the part `agent`, so whoever
//! watches the page sees what an agent did. The line goes through the
//! console's path to the log of the hub that lives, as the hub offers its
//! log no other way.

mod glance;
mod picture;
mod reads;
mod tools;

#[cfg(test)]
mod tests;

use crate::protocol::Refusal;
use crate::routes::Hubs;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{any, MethodRouter};
use rmcp::transport::streamable_http_server::session::never::NeverSessionManager;
use rmcp::transport::{StreamableHttpServerConfig, StreamableHttpService};
use std::sync::Arc;
use tools::Door;

/// The route `/mcp` serves: 404 while the door is closed, and the MCP
/// endpoint over `hubs` while it is open.
pub(crate) fn route(hubs: &Hubs) -> MethodRouter<Hubs> {
    let for_door = hubs.clone();
    let endpoint = StreamableHttpService::new(
        move || Ok(Door::new(for_door.clone())),
        Arc::new(NeverSessionManager::default()),
        // The guard checks every request's host and origin, so the
        // endpoint does not check them again.
        StreamableHttpServerConfig::default()
            .with_legacy_session_mode(false)
            .with_json_response(true)
            .disable_allowed_hosts()
            .disable_allowed_origins(),
    );
    any(move |State(hubs): State<Hubs>, request: Request| {
        let endpoint = endpoint.clone();
        async move {
            if !hubs.mcp() {
                return Refusal::new(StatusCode::NOT_FOUND, "the MCP endpoint is off")
                    .into_response();
            }
            endpoint.handle(request).await.into_response()
        }
    })
}
