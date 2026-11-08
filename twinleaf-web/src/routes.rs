//! The page's routes. `/ws` streams what the hub sends, and `/api` calls
//! RPCs, finds devices and mounts them, changes the decimation and reads a
//! window of raw rows. What each says is in `protocol`.

use crate::hub::Live;
use crate::protocol::{
    CallRequest, DiscoverRequest, Done, Frame, HistoryRequest, MountArg, MountRequest, PageMessage,
    PpsRequest, ReadRequest, Refusal, RowsRequest, WindowRequest,
};
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Query, State, WebSocketUpgrade};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};
use twinleaf::proxy::{Layout, Mount};
use twinleaf::DeviceRoute;

/// The routes, guarded, with the page's files behind them.
pub(crate) fn router() -> Router {
    Router::new()
        .route("/ws", get(ws))
        .route("/api/call", post(call))
        .route("/api/read", post(read))
        .route("/api/discover", get(discover))
        .route("/api/mount", post(mount))
        .route("/api/pps", post(set_pps))
        .route("/api/history", post(set_history))
        .route("/api/window", get(window))
        .route("/api/rows", get(rows))
        .fallback(crate::assets::serve)
        .with_state(Hubs::default())
        .layer(axum::middleware::from_fn(crate::guard::guard))
}

/// The hub every page shares, and the layout it opens. The hub starts with
/// the first request that needs it, and again after it retires, of the
/// layout a page last mounted.
#[derive(Clone, Default)]
struct Hubs(Arc<Mutex<Current>>);

#[derive(Default)]
struct Current {
    live: Option<Arc<Live>>,
    /// The layout the page chose, or `None` for the proxy's default.
    layout: Option<Layout>,
}

/// Starts a hub of `layout` off the runtime's threads, as opening blocks.
async fn start(layout: Option<Layout>) -> Result<Live, Refusal> {
    let runtime = tokio::runtime::Handle::current();
    blocking(move || Live::start(layout.as_ref(), &runtime))
        .await?
        .map_err(|error| Refusal::new(StatusCode::SERVICE_UNAVAILABLE, error))
}

impl Hubs {
    async fn live(&self) -> Result<Arc<Live>, Refusal> {
        let mut current = self.0.lock().await;
        if let Some(live) = current.live.as_ref().filter(|live| !live.is_retired()) {
            return Ok(live.clone());
        }
        let live = Arc::new(start(current.layout.clone()).await?);
        current.live = Some(live.clone());
        Ok(live)
    }

    /// Opens `layout` in place of the hub's, and retires the hub it
    /// replaces. A layout the proxy refuses leaves the hub as it was.
    async fn mount(&self, layout: Option<Layout>) -> Result<(), Refusal> {
        let mut current = self.0.lock().await;
        let live = Arc::new(start(layout.clone()).await?);
        if let Some(old) = current.live.replace(live) {
            old.retire();
        }
        current.layout = layout;
        Ok(())
    }
}

async fn ws(upgrade: WebSocketUpgrade, State(hubs): State<Hubs>) -> Response {
    match hubs.live().await {
        Ok(live) => upgrade.on_upgrade(move |socket| client(socket, live)),
        Err(refused) => refused.into_response(),
    }
}

/// Sends a page the hub's snapshot, then every change the hub sends, until
/// the page goes or the hub retires. A page that falls so far behind that the
/// channel drops changes it has not read gets a snapshot again, which
/// replaces all it holds. What the page sends names the spectra it shows.
async fn client(mut socket: WebSocket, live: Arc<Live>) {
    let page = live.page();
    let mut retired = live.retired();
    // A hub that retired before the page subscribed sends it no change.
    if live.is_retired() {
        return;
    }
    let (snapshot, mut rx) = live.snapshot();
    if send_all(&mut socket, &snapshot).await.is_err() {
        return;
    }
    loop {
        tokio::select! {
            frame = rx.recv() => match frame {
                Ok(frame) => if send(&mut socket, &frame).await.is_err() { return },
                Err(broadcast::error::RecvError::Lagged(frames)) => {
                    log::debug!("A page fell {frames} frames behind and is sent a snapshot");
                    let snapshot;
                    (snapshot, rx) = live.snapshot();
                    if send_all(&mut socket, &snapshot).await.is_err() { return }
                }
                Err(broadcast::error::RecvError::Closed) => return,
            },
            _ = retired.changed() => return,
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Text(text))) => match serde_json::from_str::<PageMessage>(&text) {
                    Ok(PageMessage::Spectra { traces }) => {
                        page.show_spectra(traces.into_iter().collect());
                    }
                    Ok(PageMessage::Hold { on }) => page.hold(on),
                    Err(error) => log::debug!("A page sent what this server does not read: {error}"),
                },
                Some(Ok(_)) => {}
                _ => return,
            },
        }
    }
}

async fn send_all(socket: &mut WebSocket, frames: &[Frame]) -> Result<(), axum::Error> {
    for frame in frames {
        send(socket, frame).await?;
    }
    Ok(())
}

async fn send(socket: &mut WebSocket, frame: &Frame) -> Result<(), axum::Error> {
    match frame {
        Frame::Text(text) => socket.send(Message::Text(text.clone().into())).await,
        Frame::Binary(bytes) => socket.send(Message::Binary(bytes.clone().into())).await,
    }
}

/// Calls one RPC on every device the request names, with one argument, and
/// replies each device's outcome.
async fn call(
    State(hubs): State<Hubs>,
    Json(args): Json<CallRequest>,
) -> Result<Response, Refusal> {
    let live = hubs.live().await?;
    let reply = crate::devices::call(
        &live.tree,
        &live.devices,
        &args.name,
        &args.value,
        &args.routes,
    )
    .await;
    Ok(Json(reply).into_response())
}

/// Reads entries of one device that no announcement keeps current.
async fn read(
    State(hubs): State<Hubs>,
    Json(args): Json<ReadRequest>,
) -> Result<Response, Refusal> {
    let live = hubs.live().await?;
    let reply = crate::devices::read(&live.tree, &live.devices, &args.route, &args.names).await?;
    Ok(Json(reply).into_response())
}

/// The devices this host reaches, as `tio list` finds them.
async fn discover(Query(args): Query<DiscoverRequest>) -> Response {
    Json(crate::discover::scan(args.network).await).into_response()
}

/// Opens the devices the request names in place of the hub's.
async fn mount(
    State(hubs): State<Hubs>,
    Json(args): Json<MountRequest>,
) -> Result<Response, Refusal> {
    let layout =
        layout_of(args.mounts).map_err(|error| Refusal::new(StatusCode::BAD_REQUEST, error))?;
    hubs.mount(layout).await?;
    Ok(Json(Done {}).into_response())
}

/// The layout that mounts `mounts`, or `None` for none, the proxy's default.
///
/// One device mounts at the root. Several mount at `/0`, `/1` and on, in the
/// order of their URL and then their subtree, so the same devices get the
/// same routes however they were picked, and a saved plot that names a
/// signal by its route finds it again. The same device named twice mounts
/// once.
fn layout_of(mut mounts: Vec<MountArg>) -> Result<Option<Layout>, String> {
    let mut parsed = Vec::new();
    for arg in mounts.drain(..) {
        let source = match arg.source.as_deref() {
            None => DeviceRoute::root(),
            Some(text) => text
                .parse::<DeviceRoute>()
                .map_err(|error| format!("{text}: {error}"))?,
        };
        parsed.push((arg.url, source));
    }
    parsed.sort_by_key(|(url, source)| (url.clone(), source.to_string()));
    parsed.dedup();
    let single = parsed.len() == 1;
    let mounts: Vec<Mount> = parsed
        .into_iter()
        .enumerate()
        .map(|(index, (url, source))| Mount {
            url,
            source,
            prefix: if single {
                DeviceRoute::root()
            } else {
                DeviceRoute::from_hops(&[index as u8]).expect("one hop is a route")
            },
        })
        .collect();
    Ok((!mounts.is_empty()).then_some(Layout { mounts }))
}

/// Changes the decimation, off the runtime's threads, as it re-decimates the
/// history.
async fn set_pps(
    State(hubs): State<Hubs>,
    Json(args): Json<PpsRequest>,
) -> Result<Response, Refusal> {
    let live = hubs.live().await?;
    let pps = args.pps.clamp(10.0, 100_000.0);
    blocking(move || live.set_points_per_second(pps)).await?;
    Ok(Json(PpsRequest { pps }).into_response())
}

/// The raw history a page may ask for: from 10 s to a day of each stream,
/// in 16 MB to 64 GB.
const HISTORY_SECONDS: (f64, f64) = (10.0, 86_400.0);
const HISTORY_BYTES: (u64, u64) = (16_000_000, 64_000_000_000);

async fn set_history(
    State(hubs): State<Hubs>,
    Json(args): Json<HistoryRequest>,
) -> Result<Response, Refusal> {
    let live = hubs.live().await?;
    let seconds = args.seconds.clamp(HISTORY_SECONDS.0, HISTORY_SECONDS.1);
    let bytes = args.bytes.clamp(HISTORY_BYTES.0, HISTORY_BYTES.1);
    blocking(move || live.set_history(seconds, bytes as usize)).await?;
    Ok(Json(HistoryRequest { seconds, bytes }).into_response())
}

/// Reads a window of one trace of the hub the page names. A page still on a
/// hub that has retired is refused, as its trace ids name other traces now.
async fn window(
    State(hubs): State<Hubs>,
    Query(args): Query<WindowRequest>,
) -> Result<Response, Refusal> {
    let live = hubs.live().await?;
    if live.id != args.hub {
        return Err(Refusal::new(StatusCode::CONFLICT, "that hub has retired"));
    }
    let window =
        blocking(move || live.window(args.id, args.t0, args.t1, args.width.min(8192))).await?;
    match window {
        Some(window) => Ok(Json(window).into_response()),
        None => Err(Refusal::new(StatusCode::NOT_FOUND, "no such trace")),
    }
}

/// Reads every raw row of one trace of the hub the page names in a range, for
/// copying, or how many there are past the page's limit.
async fn rows(
    State(hubs): State<Hubs>,
    Query(args): Query<RowsRequest>,
) -> Result<Response, Refusal> {
    let live = hubs.live().await?;
    if live.id != args.hub {
        return Err(Refusal::new(StatusCode::CONFLICT, "that hub has retired"));
    }
    let rows = blocking(move || live.rows(args.id, args.t0, args.t1, args.limit)).await?;
    match rows {
        Some(rows) => Ok(Json(rows).into_response()),
        None => Err(Refusal::new(StatusCode::NOT_FOUND, "no such trace")),
    }
}

/// Runs `work` off the runtime's threads.
async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, Refusal> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| Refusal::new(StatusCode::INTERNAL_SERVER_ERROR, error))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arg(url: &str, source: Option<&str>) -> MountArg {
        MountArg {
            url: url.to_string(),
            source: source.map(str::to_string),
        }
    }

    fn prefixes(mounts: Vec<MountArg>) -> Vec<(String, String, String)> {
        layout_of(mounts)
            .unwrap()
            .map(|layout| layout.mounts)
            .unwrap_or_default()
            .into_iter()
            .map(|m| (m.url, m.source.to_string(), m.prefix.to_string()))
            .collect()
    }

    #[test]
    fn one_device_mounts_at_the_root() {
        assert_eq!(
            prefixes(vec![arg("udp://b", None)]),
            [("udp://b".into(), "/".into(), "/".into())]
        );
    }

    #[test]
    fn several_devices_mount_in_one_order_however_they_were_picked() {
        let picked = [
            arg("udp://b", None),
            arg("udp://a", Some("/1")),
            arg("udp://a", None),
        ];
        let reversed: Vec<MountArg> = picked.iter().rev().cloned().collect();
        let expected: Vec<(String, String, String)> = [
            ("udp://a", "/", "/0"),
            ("udp://a", "/1", "/1"),
            ("udp://b", "/", "/2"),
        ]
        .iter()
        .map(|(u, s, p)| (u.to_string(), s.to_string(), p.to_string()))
        .collect();
        assert_eq!(prefixes(picked.to_vec()), expected);
        assert_eq!(prefixes(reversed), expected);
    }

    #[test]
    fn a_device_named_twice_mounts_once_and_none_is_the_default() {
        assert_eq!(
            prefixes(vec![arg("udp://a", None), arg("udp://a", Some("/"))]).len(),
            1
        );
        assert!(layout_of(Vec::new()).unwrap().is_none());
        assert!(layout_of(vec![arg("udp://a", Some("x"))]).is_err());
    }
}
