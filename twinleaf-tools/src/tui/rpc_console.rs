//! The [`RpcConsole`] that `tio monitor` and `tio health` share. It holds the
//! palette, each device's RPC list and settings as the connection keeps them,
//! and a worker thread that makes calls. The screen loop receives only
//! finished results.

use super::rpc_palette::{PaletteEvent, RpcPalette, RpcReq};
use ratatui::{
    crossterm::event::{KeyEvent, MouseEvent},
    layout::{Position, Rect},
    style::Color,
    Frame,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use twinleaf::device::rpc::{RpcMeta, RpcMetaExt, RpcRegistry, RpcValueType, RpcValueTypeExt};
use twinleaf::device::{DeviceTree, Presence, RegistryState, Settings};
use twinleaf::flume::{self, Receiver, Sender};
use twinleaf::DeviceRoute;

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
    /// The RPC list and settings of each route heard, as the connection
    /// keeps them.
    watched: Vec<(
        DeviceRoute,
        twinleaf::Receiver<RegistryState>,
        twinleaf::Receiver<Arc<Settings>>,
    )>,
    tree: DeviceTree,
    calls: Sender<RpcReq>,
    replies: Receiver<Reply>,
}

impl RpcConsole {
    /// Creates a console that lists and calls the RPCs of the devices in
    /// `tree`.
    pub fn new(tree: DeviceTree) -> RpcConsole {
        let caller = tree.clone();
        let (calls, replies) = spawn("twinleaf-rpc", move |req| call(&caller, req));
        RpcConsole {
            palette: RpcPalette::default(),
            open: false,
            picked: None,
            area: Rect::default(),
            started: Instant::now(),
            routes: HashMap::new(),
            watched: Vec::new(),
            tree,
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

    /// Follows the RPC list and settings of each route in `routes` not yet
    /// followed.
    pub(super) fn on_routes(&mut self, routes: &[DeviceRoute]) {
        for &route in routes {
            if self.routes.contains_key(&route) {
                continue;
            }
            let Ok(device) = self.tree.device(route) else {
                continue;
            };
            self.routes.insert(route, RouteRpcState::default());
            self.watched.push((
                route,
                device.rpc_registry_updates(),
                device.settings_updates(),
            ));
        }
    }

    /// Notes whether the device at `route` is there.
    pub(super) fn on_presence(&mut self, route: DeviceRoute, presence: Presence) {
        if let Some(rpc) = self.routes.get_mut(&route) {
            rpc.lost = matches!(presence, Presence::Lost { .. });
        }
    }

    /// Returns each followed route with the receivers of its RPC list and
    /// settings.
    pub(super) fn watched(
        &self,
    ) -> impl Iterator<
        Item = (
            DeviceRoute,
            &twinleaf::Receiver<RegistryState>,
            &twinleaf::Receiver<Arc<Settings>>,
        ),
    > + '_ {
        self.watched
            .iter()
            .map(|(route, registry, settings)| (*route, registry, settings))
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
        let unheard = RouteRpcState {
            waiting: true,
            ..RouteRpcState::default()
        };
        let rpc = self.routes.get(&target).unwrap_or(&unheard);
        let blink = (self.started.elapsed().as_millis() / 500).is_multiple_of(2);
        self.palette.render(f, area, &target, rpc, blink);
    }

    /// Returns the channel each reply arrives on.
    pub(super) fn replies(&self) -> &Receiver<Reply> {
        &self.replies
    }

    /// Takes the RPC list of `route`, or why it could not be loaded, and
    /// suggests from it when it belongs to the target. A failed load keeps
    /// the list before it.
    pub(super) fn on_registry(
        &mut self,
        route: DeviceRoute,
        registry: RegistryState,
        default: DeviceRoute,
    ) {
        let rpc = self.routes.entry(route).or_default();
        match registry {
            Ok(registry) => {
                rpc.registry = Some(registry);
                rpc.failed = false;
            }
            Err(_) => rpc.failed = true,
        }
        if self.target(default) == route {
            self.refresh(default);
        }
    }

    /// Takes the settings of `route`, whose values the palette shows.
    pub(super) fn on_settings(&mut self, route: DeviceRoute, settings: Arc<Settings>) {
        self.routes.entry(route).or_default().settings = Some(settings);
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

/// One device's RPC list and settings as the connection keeps them, and
/// whether the list could be shown.
#[derive(Default)]
pub(super) struct RouteRpcState {
    registry: Option<Arc<RpcRegistry>>,
    settings: Option<Arc<Settings>>,
    /// The last load failed. A list from before it is still shown.
    failed: bool,
    /// The device's link lost it.
    lost: bool,
    /// The route has not been heard, so there is nothing to load yet.
    waiting: bool,
}

impl RouteRpcState {
    pub(super) fn registry(&self) -> Option<&RpcRegistry> {
        self.registry.as_deref()
    }

    /// Returns the settings the palette shows the values of.
    pub(super) fn settings(&self) -> Option<&Settings> {
        self.settings.as_deref()
    }

    /// Returns what the palette says in place of suggestions, and the word
    /// its title shows, for where the list stands.
    pub(super) fn status(&self) -> (&'static str, &'static str) {
        self.list_status().words()
    }

    /// Returns where the list stands. A route not yet heard, and a device its
    /// link lost, are reported whatever list is held.
    fn list_status(&self) -> ListStatus {
        if self.waiting {
            return ListStatus::Unheard;
        }
        if self.lost {
            return ListStatus::Lost;
        }
        match (&self.registry, self.failed) {
            (None, false) => ListStatus::Loading,
            (None, true) => ListStatus::Unavailable,
            (Some(_), true) => ListStatus::Previous,
            (Some(_), false) => ListStatus::Current,
        }
    }
}

/// Where one device's RPC list stands, as the palette reports it.
#[derive(Clone, Copy)]
enum ListStatus {
    /// No packet has come from the route, so there is no device to ask.
    Unheard,
    /// The link lost the device. Any list held may be out of date.
    Lost,
    /// The first list is being loaded.
    Loading,
    /// The list failed to load, and none loaded before.
    Unavailable,
    /// The latest load failed, so the list shown is the one before it.
    Previous,
    /// The list shown is the device's current one.
    Current,
}

impl ListStatus {
    /// Returns what the palette says in place of suggestions, and the word
    /// its title shows. A current list shows its suggestions and says nothing.
    fn words(self) -> (&'static str, &'static str) {
        match self {
            ListStatus::Unheard => ("Waiting for RPCs...", "Waiting"),
            ListStatus::Lost => ("Device disconnected", "Disconnected"),
            ListStatus::Loading => ("Generating RPC list...", "Generating"),
            ListStatus::Unavailable => ("RPC list unavailable", "Unavailable"),
            ListStatus::Previous => ("RPC list failed; using previous list", "Using previous"),
            ListStatus::Current => ("", ""),
        }
    }
}
