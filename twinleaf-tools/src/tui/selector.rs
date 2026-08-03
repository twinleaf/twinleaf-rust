//! Device discovery frontend for `tio list`, driven by the shared
//! [`Discovery`] stream. On a TTY it runs a live picker: selecting a device
//! hands it back for the caller to proxy, quitting prints the device tree.
//! Without a TTY it prints the tree after a fixed scan window.

use std::io::IsTerminal;
use std::time::{Duration, Instant};

use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{List, ListItem, ListState, Paragraph};
use ratatui::Frame;

use twinleaf::device::discovery::{
    DiscoveredDevice, Discovery, DiscoveryConfig, DiscoveryEvent, PortInterface,
};
use twinleaf::device::{DeviceRoute, NamedRoute};

const SPINNER: [char; 8] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧'];

/// A discovered device together with the subdevice routes probed behind it
/// and, when marked with Space, the route prefix it will be mounted at.
struct Entry {
    device: DiscoveredDevice,
    routes: Vec<NamedRoute>,
    mount: Option<u8>,
}

fn mount_route(n: u8) -> DeviceRoute {
    n.to_string()
        .parse::<DeviceRoute>()
        .expect("single-segment route")
}

/// Live state of the selector
struct DeviceList {
    entries: Vec<Entry>,
    selected: usize,
}

impl DeviceList {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            selected: 0,
        }
    }

    fn apply(&mut self, event: DiscoveryEvent) {
        match event {
            DiscoveryEvent::Added(device) => {
                if !self.entries.iter().any(|e| e.device.url == device.url) {
                    self.entries.push(Entry {
                        device,
                        routes: Vec::new(),
                        mount: None,
                    });
                }
            }
            DiscoveryEvent::Named { url, name } => {
                if let Some(e) = self.entries.iter_mut().find(|e| e.device.url == url) {
                    e.device.name = Some(name);
                }
            }
            DiscoveryEvent::Subdevices { url, mut routes } => {
                if let Some(e) = self.entries.iter_mut().find(|e| e.device.url == url) {
                    // Adopt the new snapshot, keeping names already resolved.
                    for r in routes.iter_mut().filter(|r| r.name.is_none()) {
                        r.name = e
                            .routes
                            .iter()
                            .find(|k| k.route == r.route)
                            .and_then(|k| k.name.clone());
                    }
                    e.routes = routes;
                }
            }
            DiscoveryEvent::Removed { url } => {
                self.entries.retain(|e| e.device.url != url);
            }
        }
        if self.selected >= self.entries.len() {
            self.selected = self.entries.len().saturating_sub(1);
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
        if self.selected + 1 < self.entries.len() {
            self.selected += 1;
        }
    }

    fn current(&self) -> Option<&DiscoveredDevice> {
        self.entries.get(self.selected).map(|e| &e.device)
    }

    /// Mark/unmark the highlighted device for mounting; marking assigns the
    /// lowest free prefix.
    fn toggle_mount(&mut self) {
        let taken: Vec<u8> = self.entries.iter().filter_map(|e| e.mount).collect();
        let Some(e) = self.entries.get_mut(self.selected) else {
            return;
        };
        e.mount = match e.mount {
            Some(_) => None,
            None => (0..=u8::MAX).find(|n| !taken.contains(n)),
        };
    }

    /// Marked devices with their mount prefixes, in prefix order.
    fn mounts(&self) -> Vec<(DiscoveredDevice, DeviceRoute)> {
        let mut marked: Vec<(u8, &Entry)> = self
            .entries
            .iter()
            .filter_map(|e| e.mount.map(|n| (n, e)))
            .collect();
        marked.sort_by_key(|(n, _)| *n);
        marked
            .into_iter()
            .map(|(n, e)| (e.device.clone(), mount_route(n)))
            .collect()
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

fn route_name(route: &NamedRoute) -> &str {
    route.name.as_deref().unwrap_or("(no name)")
}

fn is_direct_child(parent: &DeviceRoute, candidate: &DeviceRoute) -> bool {
    candidate.len() == parent.len() + 1
        && parent
            .iter()
            .zip(candidate.iter())
            .take(parent.len())
            .all(|(a, b)| a == b)
}

/// Flatten sorted subdevice routes into `(glyph prefix, route)` lines in
/// depth-first order, mirroring the termtree layout of the final printout.
fn tree_lines<'a>(
    parent: &DeviceRoute,
    routes: &'a [NamedRoute],
    indent: &str,
    out: &mut Vec<(String, &'a NamedRoute)>,
) {
    let children: Vec<&NamedRoute> = routes
        .iter()
        .filter(|r| is_direct_child(parent, &r.route))
        .collect();
    for (i, child) in children.iter().enumerate() {
        let last = i + 1 == children.len();
        let glyph = if last { "└── " } else { "├── " };
        out.push((format!("{indent}{glyph}"), child));
        let next = format!("{indent}{}", if last { "    " } else { "│   " });
        tree_lines(&child.route, routes, &next, out);
    }
}

fn render(frame: &mut Frame, list: &DeviceList, spin: usize) {
    let [title_area, list_area, footer_area] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(1),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    let status = format!(
        "{} browsing · {} found",
        SPINNER[spin % SPINNER.len()],
        list.entries.len()
    );
    let title = Line::from(vec![
        Span::styled("tio list", Style::new().add_modifier(Modifier::BOLD)),
        Span::raw("   "),
        Span::styled(status, Style::new().fg(Color::DarkGray)),
    ]);
    frame.render_widget(Paragraph::new(title), title_area);

    if list.entries.is_empty() {
        frame.render_widget(
            Paragraph::new(Span::styled("searching…", Style::new().fg(Color::DarkGray))),
            list_area,
        );
    } else {
        // One row per line: the cursor is the reversed root line, with the
        // subtree tied to it by branch color. Once anything is marked, a
        // gutter shows mount prefixes and routes render as clients will see
        // them.
        let mounting = list.entries.iter().any(|e| e.mount.is_some());
        let mut rows: Vec<ListItem> = Vec::new();
        let mut cursor_row = 0;
        for (idx, e) in list.entries.iter().enumerate() {
            let selected = idx == list.selected;
            if selected {
                cursor_row = rows.len();
            }
            let mut root_spans = Vec::new();
            if mounting {
                let badge = match e.mount {
                    Some(n) => format!("{:<5}", format!("[{}]", mount_route(n))),
                    None => "     ".to_string(),
                };
                root_spans.push(Span::styled(
                    badge,
                    Style::new().fg(Color::Cyan).add_modifier(Modifier::BOLD),
                ));
            }
            root_spans.extend([
                Span::styled(
                    format!("{:<22}", device_name(&e.device)),
                    Style::new().add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    format!("{}  ", interface_tag(&e.device.interface)),
                    Style::new().fg(Color::Cyan),
                ),
                Span::styled(e.device.url.clone(), Style::new().fg(Color::DarkGray)),
            ]);
            let root = Line::from(root_spans);
            rows.push(ListItem::new(if selected {
                root.style(Style::new().add_modifier(Modifier::REVERSED))
            } else {
                root
            }));

            let branch = if selected {
                Style::new().fg(Color::Cyan)
            } else {
                Style::new().fg(Color::DarkGray)
            };
            let gutter = if mounting { "     " } else { "" };
            let mount_prefix = e.mount.map(mount_route);
            let mut children = Vec::new();
            tree_lines(&DeviceRoute::root(), &e.routes, "", &mut children);
            for (prefix, route) in children {
                let shown_route = match &mount_prefix {
                    Some(p) => p.absolute_route(&route.route).unwrap_or(route.route),
                    None => route.route,
                };
                rows.push(ListItem::new(Line::from(vec![
                    Span::raw(gutter),
                    Span::styled(prefix, branch),
                    Span::styled(format!("{}  ", shown_route), branch),
                    Span::raw(route_name(route).to_string()),
                ])));
            }
        }
        let mut state = ListState::default();
        state.select(Some(cursor_row));
        let widget = List::new(rows).highlight_symbol("❯ ");
        frame.render_stateful_widget(widget, list_area, &mut state);
    }

    let mounted = list.entries.iter().filter(|e| e.mount.is_some()).count();
    let footer_text = match mounted {
        0 => "↑↓ move · enter proxy · space mount · q list".to_string(),
        1 => "↑↓ move · space mount · enter proxy 1 mount · q list".to_string(),
        n => format!("↑↓ move · space mount · enter proxy {n} mounts · q list"),
    };
    let footer = Line::from(Span::styled(footer_text, Style::new().fg(Color::DarkGray)));
    frame.render_widget(Paragraph::new(footer), footer_area);
}

enum Outcome {
    Selected(Vec<(DiscoveredDevice, DeviceRoute)>),
    Cancelled(Vec<Entry>),
}

fn run_picker(discovery: Discovery) -> eyre::Result<Outcome> {
    let mut terminal = ratatui::init();
    let mut list = DeviceList::new();
    let mut spin = 0usize;

    let outcome = loop {
        list.drain(&discovery);
        if let Err(e) = terminal.draw(|f| render(f, &list, spin)) {
            break Err(eyre::Report::new(e));
        }

        match event::poll(Duration::from_millis(120)) {
            Ok(true) => match event::read() {
                Ok(Event::Key(key)) if key.kind == KeyEventKind::Press => match key.code {
                    KeyCode::Up | KeyCode::Char('k') => list.up(),
                    KeyCode::Down | KeyCode::Char('j') => list.down(),
                    KeyCode::Char(' ') => list.toggle_mount(),
                    KeyCode::Enter => {
                        // Marked mounts win over the highlighted device.
                        let mounts = list.mounts();
                        if !mounts.is_empty() {
                            break Ok(Outcome::Selected(mounts));
                        }
                        if let Some(device) = list.current().cloned() {
                            break Ok(Outcome::Selected(vec![(device, DeviceRoute::root())]));
                        }
                    }
                    KeyCode::Esc | KeyCode::Char('q') => {
                        break Ok(Outcome::Cancelled(std::mem::take(&mut list.entries)))
                    }
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        break Ok(Outcome::Cancelled(std::mem::take(&mut list.entries)))
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

fn sort_entries(entries: &mut [Entry]) {
    entries.sort_by(|a, b| {
        interface_tag(&a.device.interface)
            .cmp(interface_tag(&b.device.interface))
            .then(a.device.url.cmp(&b.device.url))
    });
}

// Used by the non-interactive (non-TTY) paths.
fn collect(config: DiscoveryConfig, window: Duration) -> Vec<Entry> {
    let discovery = Discovery::start(config);
    let mut list = DeviceList::new();
    let deadline = Instant::now() + window;
    while Instant::now() < deadline {
        list.drain(&discovery);
        std::thread::sleep(Duration::from_millis(50));
    }
    list.drain(&discovery);
    sort_entries(&mut list.entries);
    list.entries
}

/// Nest `routes` under `label` by parent/child relationship (routes are
/// sorted, so parents precede their children).
fn build_tree(
    parent: &DeviceRoute,
    label: String,
    routes: &[NamedRoute],
) -> termtree::Tree<String> {
    let mut node = termtree::Tree::new(label);
    for r in routes {
        if is_direct_child(parent, &r.route) {
            let child_label = format!("{}  {}", r.route, route_name(r));
            node.push(build_tree(&r.route, child_label, routes));
        }
    }
    node
}

/// Print the device tree as plain text
fn print_devices(entries: &[Entry]) {
    if entries.is_empty() {
        println!("No devices found.");
        return;
    }
    let width = entries
        .iter()
        .map(|e| e.device.url.len())
        .max()
        .unwrap_or(0);
    println!("Twinleaf devices:");
    for e in entries {
        let root_label = format!(
            "{:<width$}  {}  {}",
            e.device.url,
            interface_tag(&e.device.interface),
            device_name(&e.device),
            width = width
        );
        let tree = build_tree(&DeviceRoute::root(), root_label, &e.routes);
        for line in tree.to_string().lines() {
            println!("  {}", line);
        }
    }
}

/// `tio list`: on a TTY, open the live picker immediately — Enter returns
/// the devices to proxy with their mount prefixes (a single root mount
/// unless some were marked with Space), quitting prints the device tree
/// gathered so far. Without a TTY, collect for `browse` and print.
pub fn list_devices(
    config: DiscoveryConfig,
    browse: Duration,
) -> eyre::Result<Option<Vec<(DiscoveredDevice, DeviceRoute)>>> {
    if !std::io::stdout().is_terminal() {
        print_devices(&collect(config, browse));
        return Ok(None);
    }
    match run_picker(Discovery::start(config))? {
        Outcome::Selected(mounts) => Ok(Some(mounts)),
        Outcome::Cancelled(mut entries) => {
            sort_entries(&mut entries);
            print_devices(&entries);
            Ok(None)
        }
    }
}

/// Discover devices for `browse`, then print the plain-text device tree.
pub fn print_device_list(config: DiscoveryConfig, browse: Duration) {
    print_devices(&collect(config, browse));
}
