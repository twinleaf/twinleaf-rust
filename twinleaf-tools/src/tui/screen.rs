//! The screen loop that `tio monitor` and `tio health` share.

use super::rpc_console::{LoadedRegistry, Reply};
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
    time::{Duration, Instant},
};
use twinleaf::{
    device::{DeviceTree, Event, RecvError},
    flume::{self, Receiver, Selector},
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

    /// Takes an event, which the console has taken already.
    fn on_event(&mut self, event: Event);

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
    let events = tree.events();
    let loaded = screen.console().loaded().clone();
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
        let woken = Selector::new()
            .recv(batches.receiver(), |batch| {
                batches.resolve(batch).map(Woken::Batch)
            })
            .recv(events.receiver(), |event| {
                events.resolve(event).map(Woken::Event)
            })
            .recv(
                &input,
                |event| Ok(event.map_or(Woken::Closed, Woken::Input)),
            )
            .recv(&loaded, |registry| {
                Ok(registry.map_or(Woken::Closed, Woken::Loaded))
            })
            .recv(&replies, |reply| {
                Ok(reply.map_or(Woken::Closed, Woken::Reply))
            })
            .wait_deadline(next_frame);
        match woken {
            Ok(Ok(Woken::Batch(batch))) => screen.on_batch(batch),
            Ok(Ok(Woken::Event(event))) => {
                screen.console().handle_event(&event);
                screen.on_event(event);
            }
            Ok(Ok(Woken::Input(event))) => {
                if screen.on_input(event) {
                    return Ok(());
                }
            }
            Ok(Ok(Woken::Loaded(registry))) => {
                let default = screen.rpc_default();
                screen.console().on_loaded(registry, default);
            }
            Ok(Ok(Woken::Reply(reply))) => screen.console().on_reply(reply),
            Ok(Err(error @ RecvError::Lagged(_))) => screen.on_lagged(error),
            Ok(Err(error)) => return Err(error.into()),
            Ok(Ok(Woken::Closed)) | Err(_) => {}
        }
    }
}

/// What woke the screen loop, with what it received.
enum Woken {
    Batch(SampleBatch),
    Event(Event),
    Input(term::Event),
    Loaded(LoadedRegistry),
    Reply(Reply),
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
