//! The web page of the Twinleaf proxy. It plots a device's data live in a
//! browser, served on the proxy's own port.
//!
//! [`handler`] returns what a proxy hosted with it in
//! [`HostOptions::foreign`](twinleaf::proxy::HostOptions::foreign) hands
//! each connection that sends HTTP. `tio proxy`, built with its `web`
//! feature, hosts one so, and the page is at `http://localhost:7855`.
//!
//! The page reads its data as any client of the proxy does, through a
//! [`Connection`](twinleaf::Connection) to the proxy. One connection, the
//! hub, serves every page open at once. It opens what a page last mounted,
//! or the proxy's default, with the first request that needs it, and again
//! after it retires.
//!
//! What a page hears, and what it may ask, is in `protocol`, and the binary
//! frames of points, spectra and captures are in `wire`. The hub is in
//! `hub`, the devices and their RPCs in `devices`, the captures taken
//! for the pages that ask in `captures`, the devices' firmware and the
//! upgrades the pages ask for in `firmware`, the log's lines in `logbook`,
//! the routes in `routes`, and the checks every connection and request pass
//! in `guard`.
//!
//! An agent drives the same hub through `mcp`, a door at `/mcp` that speaks
//! the Model Context Protocol. It answers 404 unless [`handler`] opens it,
//! or a page opens it through `/api/mcp`, and every tool an agent calls is
//! a line of the log.

mod assets;
mod captures;
mod devices;
mod discover;
mod export;
mod feed;
mod firmware;
mod guard;
mod hub;
mod logbook;
mod loss;
mod mcp;
mod protocol;
#[cfg(test)]
mod recorded;
mod routes;
mod wire;

use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::Builder;
use hyper_util::service::TowerToHyperService;
use std::net::SocketAddr;
use std::time::Duration;
use twinleaf::proxy::Foreign;

/// Takes a record the process that hosts the proxy printed on its console,
/// at `level`, from `source` as the console names it, saying `text`, as a
/// line of the page's log. The line is the proxy's, of no device, said by
/// the part `source`. A record that comes while no hub lives waits, the
/// newest 200 of them, and is in the log of the next hub. A device's own
/// log record is not for this, as the hub reads the device's log packets.
pub fn console(level: log::Level, source: &str, text: &str) {
    logbook::CONSOLE.take(level, source, text);
}

/// How long a connection may take to send a request's headers.
const HEADER_TIMEOUT: Duration = Duration::from_secs(10);

/// Returns the handler that serves the page on each connection a proxy
/// hands it. It closes a connection from another machine unread. Its MCP
/// door at `/mcp` is open from the start when `mcp`, and otherwise answers
/// 404 until a page opens it. It must be called on a tokio runtime, as a
/// proxy calls it.
pub fn handler(mcp: bool) -> Foreign {
    let router = routes::router(mcp);
    Foreign::new(move |stream, peer| serve(router.clone(), stream, peer))
}

/// Serves `router` on `stream`, a connection from `peer`, in a task of its
/// own, or closes `stream` unread when `peer` is not this machine.
fn serve(router: axum::Router, stream: twinleaf::proxy::ForeignStream, peer: SocketAddr) {
    if !guard::answers(peer) {
        log::debug!("Closing the web page's connection from {peer}, another machine");
        return;
    }
    let service = TowerToHyperService::new(router);
    tokio::spawn(async move {
        let mut builder = Builder::new(TokioExecutor::new());
        builder
            .http1()
            .timer(TokioTimer::new())
            .header_read_timeout(HEADER_TIMEOUT);
        let connection = builder.serve_connection_with_upgrades(TokioIo::new(stream), service);
        if let Err(error) = connection.await {
            log::debug!("The web page's connection from {peer} ended: {error}");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    /// Sends a GET of `/api/mcp` on a loopback connection that the handler
    /// is told comes from `peer`, and returns all it answers before it
    /// closes.
    async fn answer_as_from(peer: &str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        serve(routes::router(false), stream, peer.parse().unwrap());
        client
            .write_all(b"GET /api/mcp HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut answer = Vec::new();
        let _ = client.read_to_end(&mut answer).await;
        String::from_utf8_lossy(&answer).into_owned()
    }

    #[tokio::test]
    async fn another_machine_is_closed_unread() {
        for peer in [
            "192.168.1.5:50000",
            "[::ffff:10.0.0.1]:50000",
            "[fe80::1]:50000",
        ] {
            assert_eq!(answer_as_from(peer).await, "", "{peer} was answered");
        }
    }

    #[tokio::test]
    async fn this_machine_is_served() {
        for peer in ["127.0.0.1:50000", "[::1]:50000", "[::ffff:127.0.0.1]:50000"] {
            let answer = answer_as_from(peer).await;
            assert!(answer.starts_with("HTTP/1.1 200"), "{peer} got {answer:?}");
        }
    }
}
