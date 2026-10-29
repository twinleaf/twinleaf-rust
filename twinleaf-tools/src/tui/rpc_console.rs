//! The [`RpcConsole`] that `tio monitor` and `tio health` share. It holds the
//! palette, the state of each device's RPC list, and two worker threads that
//! load RPC lists and make calls. The screen loop receives only finished
//! results.

use super::rpc_palette::{PaletteEvent, RpcPalette, RpcReq};
use ratatui::{
    crossterm::event::{KeyEvent, MouseEvent},
    layout::{Position, Rect},
    style::Color,
    Frame,
};
use std::collections::{HashMap, HashSet};
use std::time::Instant;
use twinleaf::device::rpc::{RpcMeta, RpcMetaExt, RpcRegistry, RpcValueType, RpcValueTypeExt};
use twinleaf::device::{DeviceEvent, DeviceTree, Event, LinkEvent};
use twinleaf::flume::{self, Receiver, Sender};
use twinleaf::tio::packet::LinkStatus;
use twinleaf::{DeviceRoute, SessionId};

/// One route's finished RPC list load, with no list when it failed.
pub(super) type LoadedRegistry = (DeviceRoute, Option<RpcRegistry>);

/// A call's reply as text, or why it failed.
pub(super) type Reply = Result<String, String>;

/// The RPC palette of a full screen tool, with the RPC lists it suggests
/// from and the calls it makes.
pub struct RpcConsole {
    palette: RpcPalette,
    /// Whether the palette is open, taking keys and drawn in the footer.
    open: bool,
    /// The route picked to call in place of the tool's default.
    picked: Option<DeviceRoute>,
    /// Where the palette was drawn last.
    area: Rect,
    /// When the console was made, which times the blink of the cursor.
    started: Instant,
    routes: HashMap<DeviceRoute, RouteRpcState>,
    /// Routes whose RPC list is being loaded.
    loading: HashSet<DeviceRoute>,
    load_requests: Sender<DeviceRoute>,
    loaded: Receiver<LoadedRegistry>,
    calls: Sender<RpcReq>,
    replies: Receiver<Reply>,
}

impl RpcConsole {
    /// Creates a console that lists and calls the RPCs of the devices in
    /// `tree`.
    pub fn new(tree: DeviceTree) -> RpcConsole {
        let lister = tree.clone();
        let (load_requests, loaded) = spawn("twinleaf-registry", move |route| {
            let registry = lister
                .rpc_registry(route)
                .inspect_err(|error| log::warn!("could not list the RPCs of {route}: {error}"))
                .ok();
            (route, registry)
        });
        let (calls, replies) = spawn("twinleaf-rpc", move |req| call(&tree, req));
        RpcConsole {
            palette: RpcPalette::default(),
            open: false,
            picked: None,
            area: Rect::default(),
            started: Instant::now(),
            routes: HashMap::new(),
            loading: HashSet::new(),
            load_requests,
            loaded,
            calls,
            replies,
        }
    }

    /// Returns `true` if the palette is open.
    pub fn is_open(&self) -> bool {
        self.open
    }

    /// Returns the route the palette calls, which is the one picked last or
    /// else `default`.
    pub fn target(&self, default: DeviceRoute) -> DeviceRoute {
        self.picked.unwrap_or(default)
    }

    /// Makes `route` the one the palette calls until another is picked, here
    /// or in the palette.
    pub fn pick(&mut self, route: DeviceRoute) {
        self.picked = Some(route);
        self.palette
            .update_suggestions(registry(&self.routes, route));
    }

    /// Forgets the route picked last. The palette then calls the default.
    pub fn unpick(&mut self) {
        self.picked = None;
    }

    /// Opens the palette on its target.
    pub fn open(&mut self, default: DeviceRoute) {
        let target = self.target(default);
        self.palette.enter(registry(&self.routes, target));
        self.open = true;
    }

    /// Closes the palette. The route picked stays picked.
    pub fn close(&mut self) {
        self.open = false;
        self.palette.exit();
    }

    /// Suggests again from the RPC list of the target, as needed after
    /// `default` changes while the palette is open.
    pub fn refresh(&mut self, default: DeviceRoute) {
        if self.open {
            let target = self.target(default);
            self.palette
                .update_suggestions(registry(&self.routes, target));
        }
    }

    /// Notes what `event` means for the RPC lists, and loads each list it
    /// calls for.
    pub(super) fn handle_event(&mut self, event: &Event) {
        match event {
            Event::Link {
                subtree,
                event: LinkEvent::Status(status),
            } => {
                let wanted: Vec<DeviceRoute> = self
                    .routes
                    .iter_mut()
                    .filter(|(route, _)| route.starts_with(subtree))
                    .filter_map(|(route, rpc)| rpc.on_status(*status).then_some(*route))
                    .collect();
                for route in wanted {
                    self.load(route);
                }
            }
            Event::RouteDiscovered { route } => {
                self.update(*route, RouteRpcState::on_route_discovered)
            }
            Event::Device {
                route,
                event: DeviceEvent::Heartbeat { session_id, .. },
            } => self.update(*route, |rpc| rpc.on_heartbeat(*session_id)),
            Event::Device {
                route,
                event: DeviceEvent::NewHash(hash),
            } => self.update(*route, |rpc| rpc.on_new_hash(*hash)),
            _ => {}
        }
    }

    /// Takes a key press while the palette is open. `routes` are the ones
    /// its route picker offers.
    pub fn handle_key(&mut self, key: KeyEvent, default: DeviceRoute, routes: &[DeviceRoute]) {
        let target = self.target(default);
        let event = self.palette.handle_key(
            key,
            registry(&self.routes, target),
            &target,
            routes,
            self.area.height,
        );
        self.apply(event);
    }

    /// Takes a mouse event. Returns `true` if the open palette took it, which
    /// it does for every event over it. Elsewhere the event only clears the
    /// hover highlight of the palette.
    pub fn handle_mouse(
        &mut self,
        mouse: MouseEvent,
        default: DeviceRoute,
        routes: &[DeviceRoute],
    ) -> bool {
        if !self.open {
            return false;
        }
        if !self.area.contains(Position::new(mouse.column, mouse.row)) {
            self.palette.clear_hover();
            return false;
        }
        let target = self.target(default);
        let event = self.palette.handle_mouse(
            mouse,
            registry(&self.routes, target),
            &target,
            routes,
            self.area.height,
        );
        self.apply(event);
        true
    }

    /// Returns the footer rows the open palette asks for.
    pub fn height(&self) -> u16 {
        // The suggestions with their borders, then the result line and the
        // two rows of input.
        self.palette.suggestion_rows() + 5
    }

    /// Draws the open palette in `area`.
    pub fn render(&mut self, f: &mut Frame, area: Rect, default: DeviceRoute) {
        self.area = area;
        let target = self.target(default);
        let unheard = RouteRpcState::default();
        let rpc = self.routes.get(&target).unwrap_or(&unheard);
        let blink = (self.started.elapsed().as_millis() / 500).is_multiple_of(2);
        self.palette.render(f, area, &target, rpc, blink);
    }

    /// Returns the channel each loaded RPC list arrives on.
    pub(super) fn loaded(&self) -> &Receiver<LoadedRegistry> {
        &self.loaded
    }

    /// Returns the channel each reply arrives on.
    pub(super) fn replies(&self) -> &Receiver<Reply> {
        &self.replies
    }

    /// Takes an RPC list from [`loaded`](Self::loaded), and suggests from it
    /// when it belongs to the target.
    pub(super) fn on_loaded(&mut self, (route, registry): LoadedRegistry, default: DeviceRoute) {
        self.loading.remove(&route);
        let rpc = self.routes.entry(route).or_default();
        match registry {
            Some(registry) => rpc.on_fetch_success(registry),
            None => rpc.on_fetch_error(),
        }
        if self.target(default) == route {
            self.refresh(default);
        }
    }

    /// Shows a reply from [`replies`](Self::replies) in the status line of
    /// the palette.
    pub(super) fn on_reply(&mut self, reply: Reply) {
        let (msg, color) = match reply {
            Ok(reply) => (
                format!("{}: {}", self.palette.last_rpc_command(), reply),
                Color::Green,
            ),
            Err(error) => (format!("ERR: {}", error), Color::Red),
        };
        self.palette.set_rpc_result(msg, color);
    }

    /// Acts on what the palette made of an input event.
    fn apply(&mut self, event: PaletteEvent) {
        match event {
            PaletteEvent::Submit(req) => {
                if self.calls.send(req).is_err() {
                    self.on_reply(Err("the RPC worker stopped".to_string()));
                }
            }
            PaletteEvent::SelectRoute(route) => self.pick(route),
            PaletteEvent::Exit => self.close(),
            PaletteEvent::Consumed => {}
        }
    }

    /// Applies `change` to the RPC list state of `route`, and loads the list
    /// when the change calls for it.
    fn update(&mut self, route: DeviceRoute, change: impl FnOnce(&mut RouteRpcState) -> bool) {
        if change(self.routes.entry(route).or_default()) {
            self.load(route);
        }
    }

    /// Loads the RPC list of `route`, unless its last load is still running.
    fn load(&mut self, route: DeviceRoute) {
        if self.loading.insert(route) && self.load_requests.send(route).is_err() {
            self.loading.remove(&route);
        }
    }
}

/// Returns the RPC list of `route` in `routes`, if one has loaded.
fn registry(
    routes: &HashMap<DeviceRoute, RouteRpcState>,
    route: DeviceRoute,
) -> Option<&RpcRegistry> {
    routes.get(&route).and_then(RouteRpcState::registry)
}

/// Spawns a thread named `name` that serves each request, and returns the
/// request sender and the result receiver. The thread ends when either is
/// dropped.
fn spawn<Req: Send + 'static, Res: Send + 'static>(
    name: &str,
    mut serve: impl FnMut(Req) -> Res + Send + 'static,
) -> (Sender<Req>, Receiver<Res>) {
    let (requests, incoming) = flume::unbounded::<Req>();
    let (outgoing, results) = flume::unbounded::<Res>();
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

/// Makes the call `req` asks for. A listed RPC carries its type in the
/// registry. A typed name that is not listed takes an `rpc.info` round trip
/// first.
fn call(tree: &DeviceTree, req: RpcReq) -> Reply {
    // Honor an explicit `-t` override, else the registry's metadata, else ask
    // the device what the argument is.
    let kind = req.req_type.unwrap_or_else(|| {
        req.meta
            .map(RpcMeta::from_bits)
            .or_else(|| tree.rpc_meta(req.route, &req.method).ok())
            .map(|meta| meta.text_type())
            .unwrap_or(RpcValueType::String { max_len: None })
    });

    let payload = match req.arg.as_deref() {
        Some(arg) => kind.encode_text(arg).map_err(|error| error.to_string())?,
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
        .map(|value| value.to_text())
        .map_err(|error| error.to_string())
}

enum RouteRpcPhase {
    Empty,
    Fetching,
    Ready { registry: RpcRegistry },
    Refreshing { registry: RpcRegistry },
    Disconnected { registry: Option<RpcRegistry> },
    Failed { registry: Option<RpcRegistry> },
}

/// What one device's RPC list is doing, and the list once it has loaded.
pub(super) struct RouteRpcState {
    phase: RouteRpcPhase,
    last_session_id: Option<SessionId>,
}

impl Default for RouteRpcState {
    fn default() -> Self {
        Self {
            phase: RouteRpcPhase::Empty,
            last_session_id: None,
        }
    }
}

impl RouteRpcState {
    pub(super) fn registry(&self) -> Option<&RpcRegistry> {
        match &self.phase {
            RouteRpcPhase::Ready { registry } | RouteRpcPhase::Refreshing { registry, .. } => {
                Some(registry)
            }
            RouteRpcPhase::Disconnected {
                registry: Some(registry),
            }
            | RouteRpcPhase::Failed {
                registry: Some(registry),
            } => Some(registry),
            _ => None,
        }
    }

    /// Returns what the palette says in place of suggestions while there is
    /// no list, and the word its title shows for the state of the list.
    pub(super) fn status(&self) -> (&'static str, &'static str) {
        match &self.phase {
            RouteRpcPhase::Empty => ("Waiting for RPCs...", "Waiting"),
            RouteRpcPhase::Fetching => ("Generating RPC list...", "Generating"),
            RouteRpcPhase::Ready { .. } => ("", ""),
            RouteRpcPhase::Refreshing { .. } => ("Refreshing RPC list...", "Refreshing"),
            RouteRpcPhase::Disconnected { .. } => ("Device disconnected", "Disconnected"),
            RouteRpcPhase::Failed { registry: None } => ("RPC list unavailable", "Unavailable"),
            RouteRpcPhase::Failed { registry: Some(_) } => {
                ("RPC list failed; using previous list", "Using previous")
            }
        }
    }

    fn on_route_discovered(&mut self) -> bool {
        match &self.phase {
            RouteRpcPhase::Empty
            | RouteRpcPhase::Disconnected { registry: None }
            | RouteRpcPhase::Failed { registry: None } => {
                self.phase = RouteRpcPhase::Fetching;
                true
            }
            _ => false,
        }
    }

    fn on_status(&mut self, status: LinkStatus) -> bool {
        match status {
            LinkStatus::SensorDisconnected
            | LinkStatus::FailedToConnect
            | LinkStatus::FailedToReconnect
            | LinkStatus::Stopped => {
                let registry = self.take_registry();
                self.phase = RouteRpcPhase::Disconnected { registry };
                false
            }
            LinkStatus::SensorReconnected => self.begin_fetch(),
            LinkStatus::Unknown(_) => false,
            _ => false,
        }
    }

    fn on_heartbeat(&mut self, session_id: Option<SessionId>) -> bool {
        let changed = match (self.last_session_id, session_id) {
            (Some(old), Some(new)) => old != new,
            _ => false,
        };
        if let Some(new) = session_id {
            self.last_session_id = Some(new);
        }

        if changed {
            return self.begin_fetch();
        }

        match &self.phase {
            RouteRpcPhase::Empty | RouteRpcPhase::Failed { registry: None } => self.begin_fetch(),
            _ => false,
        }
    }

    fn on_new_hash(&mut self, hash: Option<u32>) -> bool {
        match hash {
            Some(hash) => {
                if self.registry().and_then(|reg| reg.hash) == Some(hash) {
                    if let Some(registry) = self.take_registry() {
                        self.phase = RouteRpcPhase::Ready { registry };
                    }
                    false
                } else {
                    self.begin_fetch()
                }
            }
            None => self.begin_fetch(),
        }
    }

    fn on_fetch_success(&mut self, registry: RpcRegistry) {
        self.phase = RouteRpcPhase::Ready { registry };
    }

    fn on_fetch_error(&mut self) {
        let registry = self.take_registry();
        self.phase = RouteRpcPhase::Failed { registry };
    }

    fn begin_fetch(&mut self) -> bool {
        match &self.phase {
            RouteRpcPhase::Fetching | RouteRpcPhase::Refreshing { .. } => false,
            _ => {
                if let Some(registry) = self.take_registry() {
                    self.phase = RouteRpcPhase::Refreshing { registry };
                } else {
                    self.phase = RouteRpcPhase::Fetching;
                }
                true
            }
        }
    }

    fn take_registry(&mut self) -> Option<RpcRegistry> {
        let phase = std::mem::replace(&mut self.phase, RouteRpcPhase::Empty);
        match phase {
            RouteRpcPhase::Ready { registry } | RouteRpcPhase::Refreshing { registry, .. } => {
                Some(registry)
            }
            RouteRpcPhase::Disconnected { registry } | RouteRpcPhase::Failed { registry } => {
                registry
            }
            RouteRpcPhase::Empty | RouteRpcPhase::Fetching => None,
        }
    }
}
