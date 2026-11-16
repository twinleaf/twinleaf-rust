//! The checks every request passes before it is served.
//!
//! A page on any site a browser has open can send requests to
//! `localhost:7855`. One that makes its own name resolve to `127.0.0.1`, a
//! DNS rebinding, can read the answers too. So a request must name this
//! machine as its `Host`. A request that carries an `Origin`, as a
//! browser's WebSocket upgrade and its requests other than GET do, must come
//! from this server's own origin.
//! A request without one comes from no browser page, and passes. The server
//! sends no CORS headers, so no other origin may read what it answers.

use crate::protocol::Refusal;
use axum::extract::Request;
use axum::http::{header, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// Serves `request` if it passes the checks, and refuses it with 403
/// otherwise, as a [`Refusal`] like every other.
pub(crate) async fn guard(request: Request, next: Next) -> Response {
    let headers = request.headers();
    let host = headers.get(header::HOST).and_then(|v| v.to_str().ok());
    let Some(host) = host.filter(|host| names_this_machine(host)) else {
        return Refusal::new(
            StatusCode::FORBIDDEN,
            "this server answers only to localhost",
        )
        .into_response();
    };
    let origin = headers
        .get(header::ORIGIN)
        .map(|v| v.to_str().unwrap_or(""));
    let checked = request.method() != Method::GET || headers.contains_key(header::UPGRADE);
    if checked && origin.is_some_and(|origin| origin != format!("http://{host}")) {
        return Refusal::new(
            StatusCode::FORBIDDEN,
            "requests from other sites are refused",
        )
        .into_response();
    }
    next.run(request).await
}

/// Whether `host`, a `Host` header, names this machine by a loopback name
/// or address, with or without a port.
fn names_this_machine(host: &str) -> bool {
    // An IPv6 address comes in brackets, so its colons are no port's.
    let (name, port) = match host.strip_prefix('[') {
        Some(rest) => match rest.split_once(']') {
            Some((address, port)) => (address, port),
            None => return false,
        },
        None => match host.find(':') {
            Some(colon) => host.split_at(colon),
            None => (host, ""),
        },
    };
    let port_ok = port.is_empty()
        || port
            .strip_prefix(':')
            .is_some_and(|p| p.parse::<u16>().is_ok());
    let name = name.to_ascii_lowercase();
    port_ok
        && (name == "localhost"
            || name.ends_with(".localhost")
            || name == "127.0.0.1"
            || name == "::1")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::routing::{get, post};
    use axum::Router;
    use tower::ServiceExt;

    #[test]
    fn loopback_names_pass_and_others_do_not() {
        for host in [
            "localhost",
            "localhost:7855",
            "LOCALHOST:7855",
            "plot.localhost:7855",
            "127.0.0.1:7855",
            "[::1]:7855",
            "[::1]",
        ] {
            assert!(names_this_machine(host), "{host} was refused");
        }
        for host in [
            "evil.com",
            "evil.com:7855",
            "localhost.evil.com",
            "127.0.0.2:7855",
            "192.168.1.5:7855",
            "[::2]:7855",
            "localhost:x",
            "",
        ] {
            assert!(!names_this_machine(host), "{host} passed");
        }
    }

    fn app() -> Router {
        Router::new()
            .route("/", get(|| async { "page" }))
            .route("/set", post(|| async { "set" }))
            .layer(axum::middleware::from_fn(guard))
    }

    async fn status(request: Request<Body>) -> StatusCode {
        app().oneshot(request).await.unwrap().status()
    }

    fn request(method: &str, path: &str, headers: &[(&str, &str)]) -> Request<Body> {
        let mut builder = axum::http::Request::builder().method(method).uri(path);
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        builder.body(Body::empty()).unwrap()
    }

    #[tokio::test]
    async fn a_rebound_host_is_refused() {
        assert_eq!(
            status(request("GET", "/", &[("host", "evil.com:7855")])).await,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            status(request("GET", "/", &[])).await,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            status(request("GET", "/", &[("host", "localhost:7855")])).await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn a_change_from_another_site_is_refused() {
        let local = ("host", "localhost:7855");
        let elsewhere = [local, ("origin", "http://evil.com")];
        assert_eq!(
            status(request("POST", "/set", &elsewhere)).await,
            StatusCode::FORBIDDEN
        );
        let own = [local, ("origin", "http://localhost:7855")];
        assert_eq!(status(request("POST", "/set", &own)).await, StatusCode::OK);
        // No origin means no browser page sent it.
        assert_eq!(
            status(request("POST", "/set", &[local])).await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn an_upgrade_from_another_site_is_refused() {
        let upgrade = [
            ("host", "localhost:7855"),
            ("upgrade", "websocket"),
            ("origin", "http://evil.com"),
        ];
        assert_eq!(
            status(request("GET", "/", &upgrade)).await,
            StatusCode::FORBIDDEN
        );
        // A plain page load from a link on another site is allowed.
        let navigation = [("host", "localhost:7855"), ("origin", "http://evil.com")];
        assert_eq!(
            status(request("GET", "/", &navigation)).await,
            StatusCode::OK
        );
    }
}
