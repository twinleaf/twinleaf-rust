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
//! frames of points and spectra are in `wire`. The hub is in `hub`, the
//! decimations a paused view's windows are read from in `pyramid`, the
//! devices and their RPCs in `devices`, the routes in `routes`, and the
//! checks every request passes in `guard`.

mod assets;
mod devices;
mod discover;
mod export;
mod feed;
mod guard;
mod hub;
mod protocol;
mod pyramid;
mod routes;
mod wire;

use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto::Builder;
use hyper_util::service::TowerToHyperService;
use std::time::Duration;
use twinleaf::proxy::Foreign;

/// How long a connection may take to send a request's headers.
const HEADER_TIMEOUT: Duration = Duration::from_secs(10);

/// Returns the handler that serves the page on each connection a proxy
/// hands it. It must be called on a tokio runtime, as a proxy calls it.
pub fn handler() -> Foreign {
    let router = routes::router();
    Foreign::new(move |stream, peer| {
        let service = TowerToHyperService::new(router.clone());
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
    })
}
