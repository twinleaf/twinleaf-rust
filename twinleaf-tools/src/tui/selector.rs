//! Inline device selector shared by `tio list` and `tio proxy`.
//!
//! Both commands drive the same [`Discovery`] stream through one Ratatui
//! *inline* viewport. Without a TTY both fall back to plain text.

use std::io::{IsTerminal, Write};
use std::time::{Duration, Instant};

use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{List, ListItem, ListState, Paragraph};
use ratatui::{Frame, TerminalOptions, Viewport};

use twinleaf::device::discovery::{
    DiscoveredDevice, Discovery, DiscoveryConfig, DiscoveryEvent, PortInterface,
};

const SPINNER: [char; 8] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧'];

const VIEWPORT_HEIGHT: u16 = 12;

/// Live state of the selector
struct DeviceList {
    devices: Vec<DiscoveredDevice>,
    selected: usize,
}

impl DeviceList {
    fn new() -> Self {
        Self {
            devices: Vec::new(),
            selected: 0,
        }
    }

    fn apply(&mut self, event: DiscoveryEvent) {
        match event {
            DiscoveryEvent::Added(device) => {
                if !self.devices.iter().any(|d| d.url == device.url) {
                    self.devices.push(device);
                }
            }
            DiscoveryEvent::Named { url, name } => {
                if let Some(d) = self.devices.iter_mut().find(|d| d.url == url) {
                    d.name = Some(name);
                }
            }
            DiscoveryEvent::Removed { url } => {
                self.devices.retain(|d| d.url != url);
            }
        }
        if self.selected >= self.devices.len() {
            self.selected = self.devices.len().saturating_sub(1);
        }
    }

    fn drain(&mut self, discovery: &Discovery) {
        while let Ok(event) = discovery.events().try_recv() {
            self.apply(event);
        }
    }

    fn up(&mut self) {
        self.selected = self.selected.saturating_sub(1);
    }

    fn down(&mut self) {
        if self.selected + 1 < self.devices.len() {
            self.selected += 1;
        }
    }

    fn current(&self) -> Option<&DiscoveredDevice> {
        self.devices.get(self.selected)
    }
}

fn interface_tag(interface: &PortInterface) -> &'static str {
    match interface {
        PortInterface::FTDI | PortInterface::STM32 => "usb",
        PortInterface::Network => "net",
        PortInterface::Unknown(..) => "ser",
    }
}

fn device_name(device: &DiscoveredDevice) -> &str {
    device.name.as_deref().unwrap_or("(resolving…)")
}

fn render(frame: &mut Frame, list: &DeviceList, browsing: bool, spin: usize, prompt: &str) {
    let [title_area, list_area, footer_area] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(1),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    let status = if browsing {
        format!(
            "{} browsing · {} found",
            SPINNER[spin % SPINNER.len()],
            list.devices.len()
        )
    } else {
        format!("· {} found", list.devices.len())
    };
    let title = Line::from(vec![
        Span::styled(prompt, Style::new().add_modifier(Modifier::BOLD)),
        Span::raw("   "),
        Span::styled(status, Style::new().fg(Color::DarkGray)),
    ]);
    frame.render_widget(Paragraph::new(title), title_area);

    if list.devices.is_empty() {
        let msg = if browsing {
            "searching…"
        } else {
            "no devices found"
        };
        frame.render_widget(
            Paragraph::new(Span::styled(msg, Style::new().fg(Color::DarkGray))),
            list_area,
        );
    } else {
        let items: Vec<ListItem> = list
            .devices
            .iter()
            .map(|d| {
                ListItem::new(Line::from(vec![
                    Span::styled(
                        format!("{:<22}", device_name(d)),
                        Style::new().add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(
                        format!("{}  ", interface_tag(&d.interface)),
                        Style::new().fg(Color::Cyan),
                    ),
                    Span::styled(d.url.clone(), Style::new().fg(Color::DarkGray)),
                ]))
            })
            .collect();
        let mut state = ListState::default();
        state.select(Some(list.selected.min(list.devices.len() - 1)));
        let widget = List::new(items)
            .highlight_symbol("❯ ")
            .highlight_style(Style::new().add_modifier(Modifier::REVERSED));
        frame.render_stateful_widget(widget, list_area, &mut state);
    }

    let footer = Line::from(Span::styled(
        "↑↓ move · enter select · q cancel",
        Style::new().fg(Color::DarkGray),
    ));
    frame.render_widget(Paragraph::new(footer), footer_area);
}

enum Outcome {
    Selected(DiscoveredDevice),
    Cancelled,
}

fn run_picker(config: DiscoveryConfig, browse: Duration, prompt: &str) -> eyre::Result<Outcome> {
    let discovery = Discovery::start(config);
    let mut terminal = ratatui::init_with_options(TerminalOptions {
        viewport: Viewport::Inline(VIEWPORT_HEIGHT),
    });

    let deadline = Instant::now() + browse;
    let mut list = DeviceList::new();
    let mut spin = 0usize;

    let outcome = loop {
        list.drain(&discovery);
        let browsing = Instant::now() < deadline;
        if let Err(e) = terminal.draw(|f| render(f, &list, browsing, spin, prompt)) {
            break Err(eyre::Report::new(e));
        }

        match event::poll(Duration::from_millis(120)) {
            Ok(true) => match event::read() {
                Ok(Event::Key(key)) if key.kind == KeyEventKind::Press => match key.code {
                    KeyCode::Up | KeyCode::Char('k') => list.up(),
                    KeyCode::Down | KeyCode::Char('j') => list.down(),
                    KeyCode::Enter => {
                        if let Some(device) = list.current().cloned() {
                            break Ok(Outcome::Selected(device));
                        }
                    }
                    KeyCode::Esc | KeyCode::Char('q') => break Ok(Outcome::Cancelled),
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        break Ok(Outcome::Cancelled)
                    }
                    _ => {}
                },
                Ok(_) => {}
                Err(e) => break Err(eyre::Report::new(e)),
            },
            Ok(false) => spin = spin.wrapping_add(1),
            Err(e) => break Err(eyre::Report::new(e)),
        }
    };

    ratatui::restore();
    outcome
}

// Used by the non-interactive (non-TTY) paths.
fn collect(config: DiscoveryConfig, window: Duration) -> Vec<DiscoveredDevice> {
    let discovery = Discovery::start(config);
    let mut list = DeviceList::new();
    let deadline = Instant::now() + window;
    while Instant::now() < deadline {
        list.drain(&discovery);
        std::thread::sleep(Duration::from_millis(50));
    }
    list.drain(&discovery);
    list.devices.sort_by(|a, b| {
        interface_tag(&a.interface)
            .cmp(interface_tag(&b.interface))
            .then(a.url.cmp(&b.url))
    });
    list.devices
}

/// Print a device list as plain text
fn print_devices(devices: &[DiscoveredDevice]) {
    if devices.is_empty() {
        println!("No devices found.");
        return;
    }
    let width = devices.iter().map(|d| d.url.len()).max().unwrap_or(0);
    println!("Twinleaf devices:");
    for d in devices {
        println!(
            "  {:<width$}  {}  {}",
            d.url,
            interface_tag(&d.interface),
            device_name(d),
            width = width
        );
    }
}

/// Interactively pick one device for the proxy. Returns `None` if the user
/// cancels. Without a TTY, auto-selects when exactly one device is present and
/// otherwise errors with the ambiguous list (so scripts must pass an explicit
/// URL).
pub fn pick_device(
    config: DiscoveryConfig,
    browse: Duration,
) -> eyre::Result<Option<DiscoveredDevice>> {
    if !std::io::stdout().is_terminal() {
        let devices = collect(config, browse);
        return match devices.len() {
            0 => Ok(None),
            1 => Ok(Some(devices.into_iter().next().unwrap())),
            _ => {
                eprintln!("multiple devices detected:");
                for d in &devices {
                    eprintln!("  {}  {}", d.url, device_name(d));
                }
                Err(eyre::eyre!(
                    "multiple devices detected; specify one with a URL"
                ))
            }
        };
    }

    match run_picker(config, browse, "tio proxy · select a device")? {
        Outcome::Selected(device) => Ok(Some(device)),
        Outcome::Cancelled => Ok(None),
    }
}

/// Show the live device list for `browse`, then leave a plain-text snapshot
/// behind. Without a TTY, just collects for the window and prints.
pub fn list_devices(config: DiscoveryConfig, browse: Duration) -> eyre::Result<()> {
    if !std::io::stdout().is_terminal() {
        print_devices(&collect(config, browse));
        return Ok(());
    }

    let discovery = Discovery::start(config);
    let mut terminal = ratatui::init_with_options(TerminalOptions {
        viewport: Viewport::Inline(VIEWPORT_HEIGHT),
    });

    let deadline = Instant::now() + browse;
    let mut list = DeviceList::new();
    let mut spin = 0usize;
    let result = loop {
        list.drain(&discovery);
        let browsing = Instant::now() < deadline;
        if let Err(e) = terminal.draw(|f| render(f, &list, browsing, spin, "tio list")) {
            break Err(eyre::Report::new(e));
        }
        // Once browsing is done, hold briefly for late resolutions, then exit.
        if !browsing {
            break Ok(());
        }
        match event::poll(Duration::from_millis(120)) {
            Ok(true) => match event::read() {
                Ok(Event::Key(key))
                    if key.kind == KeyEventKind::Press
                        && matches!(
                            key.code,
                            KeyCode::Esc | KeyCode::Char('q') | KeyCode::Enter
                        ) =>
                {
                    break Ok(())
                }
                Ok(_) => {}
                Err(e) => break Err(eyre::Report::new(e)),
            },
            Ok(false) => spin = spin.wrapping_add(1),
            Err(e) => break Err(eyre::Report::new(e)),
        }
    };

    list.drain(&discovery);
    let mut devices = std::mem::take(&mut list.devices);
    devices.sort_by(|a, b| {
        interface_tag(&a.interface)
            .cmp(interface_tag(&b.interface))
            .then(a.url.cmp(&b.url))
    });
    ratatui::restore();
    let _ = std::io::stdout().flush();
    result?;
    print_devices(&devices);
    Ok(())
}
