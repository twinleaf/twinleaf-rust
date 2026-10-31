//! The screen loop that `tio monitor` and `tio health` share.

use super::rpc_console::Reply;
use super::RpcConsole;
use ratatui::{
    crossterm::{
        event::{self as term, DisableMouseCapture, EnableMouseCapture},
        execute,
    },
    DefaultTerminal,
};
use std::{
    io,
    sync::Arc,
    time::{Duration, Instant},
};
use twinleaf::{
    device::{DeviceTree, MetadataState, Presence, RecvError, RegistryState, Settings},
    flume::{self, select::SelectError, Receiver, Selector},
    DeviceRoute, SampleBatch,
};

/// A full screen tool, as the loop that [`run`] drives sees it.
pub trait Screen {
    /// Returns the RPC console the tool holds.
    fn console(&mut self) -> &mut RpcConsole;

    /// Returns the route the console calls when none was picked in it.
    fn rpc_default(&self) -> DeviceRoute;

    /// Takes a batch of samples.
    fn on_batch(&mut self, batch: SampleBatch);

    /// Takes every route heard in the tree so far, each time a new one is.
    fn on_routes(&mut self, routes: &[DeviceRoute]);

    /// Takes the new presence of the device at `route`, which the console has
    /// taken already.
    fn on_presence(&mut self, route: DeviceRoute, presence: Presence);

    /// Takes how far the metadata of the device at `route` is collected now.
    fn on_metadata(&mut self, route: DeviceRoute, metadata: MetadataState);

    /// Takes note that the tool fell behind and lost what `error` says.
    fn on_lagged(&mut self, error: RecvError);

    /// Takes a key or mouse event. Returns `true` to quit.
    fn on_input(&mut self, event: term::Event) -> bool;

    /// Brings the screen up to date and draws it.
    fn draw(&mut self, terminal: &mut DefaultTerminal) -> io::Result<()>;
}

/// Runs `screen` on the terminal, drawing `fps` frames a second, until it
/// quits or the stream from `tree` ends. The terminal is put back with mouse
/// capture off on every return and on a panic.
///
/// # Errors
///
/// Returns an error if the terminal cannot be set up, or the stream from
/// `tree` ends.
///
/// # Panics
///
/// Panics if `fps` is zero.
pub fn run(tree: &DeviceTree, fps: u64, screen: &mut impl Screen) -> eyre::Result<()> {
    let batches = tree.samples();
    let routes = tree.routes_updates();
    let mut devices: Vec<Watched> = Vec::new();
    let replies = screen.console().replies().clone();
    let input = read_input();
    let frame = Duration::from_millis(1000 / fps);
    let mut next_frame = Instant::now() + frame;
    let _restore = Restore;
    let mut terminal = enter()?;

    loop {
        if Instant::now() >= next_frame {
            next_frame = Instant::now() + frame;
            if screen.draw(&mut terminal).is_err() {
                return Ok(());
            }
        }
        let mut selector = Selector::new()
            .recv(batches.as_flume(), |batch| {
                batches
                    .resolve(batch)
                    .map_or_else(Woken::Failed, Woken::Batch)
            })
            .recv(routes.as_flume(), |heard| {
                routes
                    .resolve(heard)
                    .map_or_else(Woken::Failed, Woken::Routes)
            })
            .recv(&input, |event| event.map_or(Woken::Closed, Woken::Input))
            .recv(&replies, |reply| reply.map_or(Woken::Closed, Woken::Reply));
        for device in &devices {
            let route = device.route;
            selector = selector
                .recv(device.presence.as_flume(), move |presence| {
                    device
                        .presence
                        .resolve(presence)
                        .map_or_else(Woken::Failed, |presence| Woken::Presence(route, presence))
                })
                .recv(device.metadata.as_flume(), move |metadata| {
                    device
                        .metadata
                        .resolve(metadata)
                        .map_or_else(Woken::Failed, |metadata| Woken::Metadata(route, metadata))
                });
        }
        for (route, registry, settings) in screen.console().watched() {
            selector = selector
                .recv(registry.as_flume(), move |loaded| {
                    registry
                        .resolve(loaded)
                        .map_or_else(Woken::Failed, |loaded| Woken::Registry(route, loaded))
                })
                .recv(settings.as_flume(), move |read| {
                    settings
                        .resolve(read)
                        .map_or_else(Woken::Failed, |read| Woken::Settings(route, read))
                });
        }
        match selector.wait_deadline(next_frame) {
            Ok(Woken::Batch(batch)) => screen.on_batch(batch),
            Ok(Woken::Routes(heard)) => {
                for &route in &heard {
                    if devices.iter().all(|device| device.route != route) {
                        devices.push(Watched::new(tree, route)?);
                    }
                }
                screen.console().on_routes(&heard);
                screen.on_routes(&heard);
            }
            Ok(Woken::Presence(route, presence)) => {
                screen.console().on_presence(route, presence);
                screen.on_presence(route, presence);
            }
            Ok(Woken::Metadata(route, metadata)) => screen.on_metadata(route, metadata),
            Ok(Woken::Registry(route, registry)) => {
                let default = screen.rpc_default();
                screen.console().on_registry(route, registry, default);
            }
            Ok(Woken::Settings(route, settings)) => screen.console().on_settings(route, settings),
            Ok(Woken::Input(event)) => {
                if screen.on_input(event) {
                    return Ok(());
                }
            }
            Ok(Woken::Reply(reply)) => screen.console().on_reply(reply),
            Ok(Woken::Failed(error @ RecvError::Lagged(_))) => screen.on_lagged(error),
            Ok(Woken::Failed(error)) => return Err(error.into()),
            Ok(Woken::Closed) => {}
            Err(SelectError::Timeout) => {}
        }
    }
}

/// The state of one device the screen loop follows.
struct Watched {
    route: DeviceRoute,
    presence: twinleaf::Receiver<Presence>,
    metadata: twinleaf::Receiver<MetadataState>,
}

impl Watched {
    /// Follows the device at `route` in `tree`.
    ///
    /// # Errors
    ///
    /// Returns an error if `route` lies outside `tree`.
    fn new(tree: &DeviceTree, route: DeviceRoute) -> eyre::Result<Watched> {
        let device = tree.device(route)?;
        Ok(Watched {
            route,
            presence: device.presence_updates(),
            metadata: device.metadata_updates(),
        })
    }
}

/// What woke the screen loop, with what it received.
enum Woken {
    Batch(SampleBatch),
    Routes(Vec<DeviceRoute>),
    Presence(DeviceRoute, Presence),
    Metadata(DeviceRoute, MetadataState),
    Registry(DeviceRoute, RegistryState),
    Settings(DeviceRoute, Arc<Settings>),
    Input(term::Event),
    Reply(Reply),
    /// The stream lagged or ended.
    Failed(RecvError),
    /// The input reader or an RPC worker has stopped.
    Closed,
}

/// Spawns a thread that reads the terminal's key and mouse events, and
/// returns the receiver of them.
fn read_input() -> Receiver<term::Event> {
    let (events, input) = flume::unbounded();
    std::thread::spawn(move || loop {
        if let Ok(event) = term::read() {
            if events.send(event).is_err() {
                return;
            }
        }
    });
    input
}

/// Puts the terminal in raw mode on the alternate screen, with mouse capture
/// on and the cursor hidden. A panic from here on puts it back before its
/// message prints.
fn enter() -> io::Result<DefaultTerminal> {
    // The hook ratatui installs restores the terminal and then calls this
    // one, which turns mouse capture off.
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = execute!(io::stdout(), DisableMouseCapture);
        hook(info);
    }));
    let mut terminal = ratatui::try_init()?;
    let _ = terminal.hide_cursor();
    let _ = execute!(io::stdout(), EnableMouseCapture);
    Ok(terminal)
}

/// Puts the terminal back when dropped, with mouse capture off, raw mode
/// off and the main screen shown.
struct Restore;

impl Drop for Restore {
    fn drop(&mut self) {
        // A panic put the terminal back in its hook, and leaving the
        // alternate screen twice would move the cursor over its message.
        if std::thread::panicking() {
            return;
        }
        let _ = execute!(io::stdout(), DisableMouseCapture);
        ratatui::restore();
    }
}
