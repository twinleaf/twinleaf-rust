//! The built page, `ui/dist`, which a release build embeds in the binary.
//!
//! A debug build reads the files from `ui/dist` as they are when asked, so
//! a page rebuilt with `npm run build` shows on the next reload. A release
//! build of a checkout without `ui/dist` builds it first, in `build.rs`,
//! where Node and npm are installed. A binary built without `ui/dist` still
//! runs, and its page says how to build it.

use axum::http::{header, StatusCode, Uri};
use axum::response::{Html, IntoResponse, Response};

#[derive(rust_embed::Embed)]
#[folder = "ui/dist"]
#[allow_missing = true]
struct Page;

/// What `/` says when the page is not built.
const NOT_BUILT: &str = "<!doctype html><title>Twinleaf</title>\
<p>The web page is not built. In <code>twinleaf-web/ui</code>, run \
<code>npm install</code> and <code>npm run build</code>, then build \
<code>tio</code> again.</p>";

/// Serves the file at `uri`. A path that names no file, such as a route of
/// the page's own, gets `index.html`.
pub(crate) async fn serve(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };
    match Page::get(path).or_else(|| Page::get("index.html")) {
        Some(file) => {
            let mime = file.metadata.mimetype().to_string();
            ([(header::CONTENT_TYPE, mime)], file.data).into_response()
        }
        None => (StatusCode::NOT_FOUND, Html(NOT_BUILT)).into_response(),
    }
}
