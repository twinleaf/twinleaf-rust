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
use twinleaf::device::NamedRoute;
use twinleaf::DeviceRoute;
const SPINNER: [char; 8] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧'];

/// A connection, its discovered routes, and the source subtrees marked for mounting.
struct Entry {
    device: DiscoveredDevice,
    routes: Vec<NamedRoute>,
    mounts: Vec<(DeviceRoute, u8)>,
    collapsed: Vec<DeviceRoute>,
}

impl Entry {
    fn pick(&self, source: DeviceRoute, prefix: DeviceRoute) -> PickedSubtree {
        let mut device = self.device.clone();
        if !source.is_empty() {
            device.name = self
                .routes
                .iter()
                .find(|r| r.route == source)
                .and_then(|r| r.name.clone());
        }
        PickedSubtree {
            device,
            source,
            prefix,
        }
    }
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
    network_error: Option<String>,
    notice: Option<String>,
}

impl DeviceList {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
            selected: 0,
            network_error: None,
            notice: None,
        }
    }

    fn apply(&mut self, event: DiscoveryEvent) {
        let current = self
            .current()
            .map(|(e, route)| (e.device.url.clone(), route));
        match event {
            DiscoveryEvent::Added(device) => {
                if !self.entries.iter().any(|e| e.device.url == device.url) {
                    self.entries.push(Entry {
                        device,
                        routes: Vec::new(),
                        mounts: Vec::new(),
                        collapsed: Vec::new(),
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
                    e.mounts.retain(|(source, _)| {
                        source.is_empty() || routes.iter().any(|r| r.route == *source)
                    });
                    e.routes = routes;
                }
            }
            DiscoveryEvent::Removed { url } => {
                self.entries.retain(|e| e.device.url != url);
            }
            DiscoveryEvent::NetworkUnavailable { reason } => {
                self.network_error = Some(reason);
            }
        }
        let rows = self.rows();
        self.selected = current
            .and_then(|(url, mut route)| loop {
                if let Some(index) = rows
                    .iter()
                    .position(|(i, r, _)| self.entries[*i].device.url == url && *r == route)
                {
                    break Some(index);
                }
                if route.is_empty() {
                    break None;
                }
                route = parent_route(route);
            })
            .unwrap_or_else(|| self.selected.min(rows.len().saturating_sub(1)));
    }

    fn drain(&mut self, discovery: &Discovery) {
        while let Ok(event) = discovery.events().try_recv() {
            self.apply(event);
        }
    }

    fn up(&mut self) {
        self.notice = None;
        self.selected = self.selected.saturating_sub(1);
    }

    fn down(&mut self) {
        self.notice = None;
        if self.selected + 1 < self.rows().len() {
            self.selected += 1;
        }
    }

    /// The same visible rows drive cursor movement and rendering.
    fn rows(&self) -> Vec<(usize, DeviceRoute, String)> {
        let mut rows = Vec::new();
        for (index, entry) in self.entries.iter().enumerate() {
            rows.push((index, DeviceRoute::root(), String::new()));
            let mut children = Vec::new();
            tree_lines(&DeviceRoute::root(), &entry.routes, "", &mut children);
            for (glyph, named) in children {
                if !entry
                    .collapsed
                    .iter()
                    .any(|r| *r != named.route && r.relative_route(&named.route).is_ok())
                {
                    rows.push((index, named.route, glyph));
                }
            }
        }
        rows
    }

    fn current(&self) -> Option<(&Entry, DeviceRoute)> {
        self.rows()
            .get(self.selected)
            .map(|(i, r, _)| (&self.entries[*i], *r))
    }

    fn right(&mut self) {
        self.notice = None;
        let rows = self.rows();
        let Some((index, route, _)) = rows.get(self.selected) else {
            return;
        };
        let entry = &mut self.entries[*index];
        if let Some(at) = entry.collapsed.iter().position(|r| r == route) {
            entry.collapsed.remove(at);
        }
    }

    fn left(&mut self) {
        self.notice = None;
        let rows = self.rows();
        let Some((index, route, _)) = rows.get(self.selected) else {
            return;
        };
        let entry = &mut self.entries[*index];
        if !entry.collapsed.contains(route)
            && entry
                .routes
                .iter()
                .any(|r| is_direct_child(route, &r.route))
        {
            entry.collapsed.push(*route);
        }
    }

    /// Jump between connection roots, wrapping at either end.
    fn next_device(&mut self, backwards: bool) {
        self.notice = None;
        let rows = self.rows();
        let Some((index, _, _)) = rows.get(self.selected) else {
            return;
        };
        let count = self.entries.len();
        let next = if backwards {
            (index + count - 1) % count
        } else {
            (index + 1) % count
        };
        if let Some(at) = rows
            .iter()
            .position(|(i, route, _)| *i == next && route.is_empty())
        {
            self.selected = at;
        }
    }

    /// Mark/unmark the highlighted device for mounting; marking assigns the
    /// lowest free prefix.
    fn toggle_mount(&mut self) {
        self.notice = None;
        let taken: Vec<u8> = self
            .entries
            .iter()
            .flat_map(|e| e.mounts.iter().map(|(_, n)| *n))
            .collect();
        let rows = self.rows();
        let Some((index, route, _)) = rows.get(self.selected) else {
            return;
        };
        let entry = &mut self.entries[*index];
        if let Some(at) = entry.mounts.iter().position(|(r, _)| r == route) {
            entry.mounts.remove(at);
        } else if entry
            .mounts
            .iter()
            .any(|(r, _)| r.relative_route(route).is_ok() || route.relative_route(r).is_ok())
        {
            self.notice = Some("Unmark the overlapping parent or child first".into());
        } else if let Some(n) = (0..=u8::MAX).find(|n| !taken.contains(n)) {
            entry.mounts.push((*route, n));
        } else {
            self.notice = Some("All mount prefixes are in use".into());
        }
    }

    /// Marked devices with their mount prefixes, in prefix order.
    fn mounts(&self) -> Vec<PickedSubtree> {
        let mut mounts: Vec<_> = self
            .entries
            .iter()
            .flat_map(|e| {
                e.mounts
                    .iter()
                    .map(|(source, n)| e.pick(*source, mount_route(*n)))
            })
            .collect();
        mounts.sort_by_key(|m| m.prefix);
        mounts
    }

    fn picked(&self) -> Vec<PickedSubtree> {
        let mounts = self.mounts();
        if !mounts.is_empty() {
            return mounts;
        }
        self.current()
            .map(|(entry, source)| vec![entry.pick(source, DeviceRoute::root())])
            .unwrap_or_default()
    }
}

/// A source subtree rebased at a destination in the served tree.
#[derive(Debug, Clone)]
pub struct PickedSubtree {
    pub device: DiscoveredDevice,
    pub source: DeviceRoute,
    pub prefix: DeviceRoute,
}

fn parent_route(route: DeviceRoute) -> DeviceRoute {
    DeviceRoute::from_hops(&route.as_slice()[..route.len().saturating_sub(1)])
        .expect("parent route")
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
    candidate.len() == parent.len() + 1 && candidate.as_slice().starts_with(parent.as_slice())
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
        Constraint::Length(2),
    ])
    .areas(frame.area());

    let status = format!(
        "{} browsing · {} found",
        SPINNER[spin % SPINNER.len()],
        list.entries.len()
    );
    let mut title_spans = vec![
        Span::styled("tio list", Style::new().add_modifier(Modifier::BOLD)),
        Span::raw("   "),
        Span::styled(status, Style::new().fg(Color::DarkGray)),
    ];
    if let Some(reason) = &list.network_error {
        title_spans.push(Span::styled(
            format!("   network off: {reason}"),
            Style::new().fg(Color::Yellow),
        ));
    }
    let title = Line::from(title_spans);
    frame.render_widget(Paragraph::new(title), title_area);

    if list.entries.is_empty() {
        frame.render_widget(
            Paragraph::new(Span::styled("searching…", Style::new().fg(Color::DarkGray))),
            list_area,
        );
    } else {
        let visible = list.rows();
        let mut rows: Vec<ListItem> = Vec::new();
        for (index, route, glyph) in &visible {
            let entry = &list.entries[*index];
            let has_children = entry
                .routes
                .iter()
                .any(|r| is_direct_child(route, &r.route));
            let arrow = if has_children {
                if entry.collapsed.contains(route) {
                    "▸ "
                } else {
                    "▾ "
                }
            } else {
                "  "
            };
            let name = if route.is_empty() {
                device_name(&entry.device)
            } else {
                entry
                    .routes
                    .iter()
                    .find(|r| r.route == *route)
                    .map(route_name)
                    .unwrap_or("(no name)")
            };
            let mounted = entry.mounts.iter().find_map(|(source, n)| {
                source
                    .relative_route(route)
                    .ok()
                    .map(|relative| (source, mount_route(*n).absolute_route(&relative)))
            });
            let mut spans = vec![
                Span::raw(glyph.clone()),
                Span::raw(arrow),
                Span::raw(format!("{route}  {name}")),
            ];
            if let Some((source, Ok(destination))) = mounted {
                spans.push(Span::styled(
                    format!(
                        "  → {destination}{}",
                        if source == route { " [mount]" } else { "" }
                    ),
                    Style::new().fg(Color::Cyan),
                ));
            }
            if route.is_empty() {
                spans.push(Span::styled(
                    format!(
                        "  {}  {}",
                        interface_tag(&entry.device.interface),
                        entry.device.url
                    ),
                    Style::new().fg(Color::DarkGray),
                ));
            }
            rows.push(ListItem::new(Line::from(spans)));
        }
        let mut state = ListState::default();
        state.select(Some(list.selected));
        let widget = List::new(rows)
            .highlight_symbol("❯ ")
            .highlight_style(Style::new().add_modifier(Modifier::REVERSED));
        frame.render_stateful_widget(widget, list_area, &mut state);
    }

    let mounted = list.mounts().len();
    let action = if mounted > 0 {
        format!("enter serve {mounted} mounts")
    } else {
        list.current()
            .map(|(_, r)| format!("enter use {r} as root"))
            .unwrap_or_else(|| "enter use subtree".into())
    };
    let footer_text = list.notice.clone().unwrap_or(action);
    let footer = vec![
        Line::from(Span::styled(footer_text, Style::new().fg(Color::Cyan))),
        Line::from(Span::styled(
            "↑↓ move · ← collapse · → expand · tab/shift-tab devices · space mount · q list",
            Style::new().fg(Color::DarkGray),
        )),
    ];
    frame.render_widget(Paragraph::new(footer), footer_area);
}

enum Outcome {
    Selected(Vec<PickedSubtree>),
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
                    KeyCode::Left | KeyCode::Char('h') => list.left(),
                    KeyCode::Right | KeyCode::Char('l') => list.right(),
                    KeyCode::Tab => list.next_device(key.modifiers.contains(KeyModifiers::SHIFT)),
                    KeyCode::BackTab => list.next_device(true),
                    KeyCode::Char(' ') => list.toggle_mount(),
                    KeyCode::Enter => {
                        let picked = list.picked();
                        if !picked.is_empty() {
                            break Ok(Outcome::Selected(picked));
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
) -> eyre::Result<Option<Vec<PickedSubtree>>> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(list: &mut DeviceList, url: &str, paths: &[&str]) {
        list.apply(DiscoveryEvent::Subdevices {
            url: url.into(),
            routes: paths
                .iter()
                .map(|path| NamedRoute {
                    route: path.parse().unwrap(),
                    name: Some(format!("sensor {path}")),
                })
                .collect(),
        });
    }

    fn tree() -> DeviceList {
        let mut list = DeviceList::new();
        list.apply(DiscoveryEvent::Added(DiscoveredDevice {
            url: "tcp://test".into(),
            interface: PortInterface::Network,
            name: Some("hub".into()),
        }));
        snapshot(&mut list, "tcp://test", &["/1", "/1/1", "/1/2", "/2"]);
        list
    }

    fn route(list: &DeviceList) -> String {
        list.current().unwrap().1.to_string()
    }

    #[test]
    fn arrows_only_collapse_expand_and_move_through_visible_rows() {
        let mut list = tree();
        assert_eq!(list.rows().len(), 5); // Fully expanded on discovery.
        list.right();
        assert_eq!(route(&list), "/");
        list.down();
        list.right();
        assert_eq!(route(&list), "/1");
        list.left();
        list.left();
        assert_eq!(route(&list), "/1");
        assert_eq!(list.rows().len(), 3);
        list.down();
        assert_eq!(route(&list), "/2");
        list.left();
        list.right();
        assert_eq!(route(&list), "/2"); // Leaves do not navigate sideways.
        list.up();
        list.right();
        list.right();
        assert_eq!(route(&list), "/1");
        assert_eq!(list.rows().len(), 5);
        list.down();
        assert_eq!(route(&list), "/1/1");
    }

    #[test]
    fn tab_jumps_between_device_roots_and_wraps() {
        let mut list = tree();
        list.apply(DiscoveryEvent::Added(DiscoveredDevice {
            url: "tcp://second".into(),
            interface: PortInterface::Network,
            name: None,
        }));
        list.down();
        list.down();
        list.next_device(false);
        assert_eq!(list.current().unwrap().0.device.url, "tcp://second");
        assert_eq!(route(&list), "/");
        list.next_device(false);
        assert_eq!(list.current().unwrap().0.device.url, "tcp://test");
        list.left(); // Collapsing a device must not change Tab's grouping.
        list.next_device(true);
        assert_eq!(list.current().unwrap().0.device.url, "tcp://second");
        list.next_device(true);
        assert_eq!(list.current().unwrap().0.device.url, "tcp://test");
        assert_eq!(route(&list), "/");
        assert_eq!(list.rows().len(), 2);
        let mut empty = DeviceList::new();
        empty.next_device(false);
        empty.next_device(true);
    }

    #[test]
    fn enter_rebases_the_highlighted_subtree_at_root() {
        let mut list = tree();
        list.down();
        let picked = list.picked();
        assert_eq!(picked[0].source.to_string(), "/1");
        assert!(picked[0].prefix.is_empty());
        assert_eq!(picked[0].device.url, "tcp://test");
        assert_eq!(picked[0].device.name.as_deref(), Some("sensor /1"));
    }

    #[test]
    fn disjoint_mounts_share_a_source_and_marks_win_over_the_cursor() {
        let mut list = tree();
        list.down();
        list.toggle_mount();
        list.down();
        list.toggle_mount();
        assert!(list.notice.is_some());
        assert_eq!(list.mounts().len(), 1);
        list.down();
        list.down();
        list.toggle_mount();
        list.selected = 0;
        list.toggle_mount();
        assert!(list.notice.is_some());
        let picked = list.picked();
        assert_eq!(picked.len(), 2);
        assert_eq!(picked[0].source.to_string(), "/1");
        assert_eq!(picked[0].prefix.to_string(), "/0");
        assert_eq!(picked[1].source.to_string(), "/2");
        assert_eq!(picked[1].prefix.to_string(), "/1");
        assert_eq!(picked[0].device.url, picked[1].device.url);
    }

    #[test]
    fn discovery_keeps_selection_by_identity_and_removes_missing_marks() {
        let mut list = tree();
        list.selected = 4;
        list.toggle_mount();
        snapshot(&mut list, "tcp://test", &["/1", "/2"]);
        assert_eq!(route(&list), "/2");
        assert_eq!(list.mounts().len(), 1);
        snapshot(&mut list, "tcp://test", &["/1"]);
        assert_eq!(route(&list), "/");
        assert!(list.mounts().is_empty());
    }

    #[test]
    fn collapsed_marks_survive_and_freed_prefixes_are_reused() {
        let mut list = tree();
        list.selected = 2;
        list.toggle_mount();
        list.up();
        list.left();
        assert_eq!(list.mounts().len(), 1);
        list.right();
        list.down();
        list.toggle_mount();
        assert!(list.mounts().is_empty());
        list.selected = 4;
        list.toggle_mount();
        assert_eq!(list.mounts()[0].prefix.to_string(), "/0");
    }

    #[test]
    fn render_shows_source_destination_and_inherited_routes() {
        let mut list = tree();
        list.down();
        list.toggle_mount();
        let backend = ratatui::backend::TestBackend::new(120, 12);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        terminal.draw(|frame| render(frame, &list, 0)).unwrap();
        let screen = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|c| c.symbol())
            .collect::<String>();
        assert!(screen.contains("/1  sensor /1  → /0 [mount]"));
        assert!(screen.contains("/1/1  sensor /1/1  → /0/1"));
        assert!(screen.contains("enter serve 1 mounts"));
    }
}
