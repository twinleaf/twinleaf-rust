//! The `tio monitor` subcommand, a live display of sensor data with plots and
//! FFTs. [`run_monitor`] runs it.

use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{self, Read},
    str::FromStr,
    time::{Duration, Instant},
};

use crate::tui::{
    self,
    scroll::{follow_scroll, NAV_MARGIN},
    RpcConsole, Screen,
};
use crate::MonitorCli;
use ratatui::{
    crossterm::event::{Event, KeyCode, KeyEventKind, KeyModifiers, MouseButton, MouseEventKind},
    layout::{Constraint, Direction, Layout, Position, Rect},
    style::{Color, Modifier, Style, Stylize},
    symbols,
    text::{Line, Span},
    widgets::{
        Axis, Block, BorderType, Borders, Chart, Dataset, GraphType, LegendPosition, Paragraph,
        Scrollbar, ScrollbarOrientation, ScrollbarState,
    },
    DefaultTerminal, Frame,
};
use toml_edit::{DocumentMut, InlineTable, Value};
use twinleaf::{
    data::{
        ops::{Fpcs, NoSpectrum, Spectrum, Welch},
        Buffer, ColumnData, ColumnKey, ColumnProcessor, DeviceMetadataSnapshot, Run, SampleBatch,
        StreamKey,
    },
    device::{DeviceEvent, Event as StreamEvent, RecvError},
    Connection, DeviceRoute,
};

const MIN_PLOT_WINDOW_SECONDS: f64 = 0.5;
const MAX_PLOT_WINDOW_SECONDS: f64 = 60.0;
const PLOT_WINDOW_FINE_STEP_SECONDS: f64 = 0.5;
const PLOT_WINDOW_COARSE_STEP_SECONDS: f64 = 5.0;
const MIN_PLOT_BUCKETS: usize = 64;
const PLOT_POINTS_PER_CELL: usize = 4;
const MONITOR_BUFFER_CAPACITY_SAMPLES: usize = 2_000_000;

/// The legend size limits, which hide the legend only when it cannot fit in
/// the graph area at all. The `l` key toggles it.
// Tighter limits let long names, such as route suffixes, blank the legend.
const LEGEND_CONSTRAINTS: (Constraint, Constraint) =
    (Constraint::Percentage(100), Constraint::Percentage(100));

const SERIES_COLORS: [Color; 7] = [
    Color::Green,
    Color::Cyan,
    Color::Yellow,
    Color::Magenta,
    Color::LightBlue,
    Color::LightRed,
    Color::LightGreen,
];

fn series_color(i: usize) -> Color {
    SERIES_COLORS[i % SERIES_COLORS.len()]
}

#[derive(Debug, Clone)]
struct PlotSeries {
    key: ColumnKey,
    label: String,
    units: String,
    color_idx: usize,
    points: Vec<(f64, f64)>,
}

#[derive(Debug, Clone)]
enum NavPos {
    EmptyDevice {
        device_idx: usize,
        route: DeviceRoute,
    },
    Column {
        device_idx: usize,
        stream_idx: usize,
        spec: ColumnKey,
    },
}

impl NavPos {
    fn device_idx(&self) -> usize {
        match self {
            NavPos::EmptyDevice { device_idx, .. } => *device_idx,
            NavPos::Column { device_idx, .. } => *device_idx,
        }
    }

    fn route(&self) -> &DeviceRoute {
        match self {
            NavPos::EmptyDevice { route, .. } => route,
            NavPos::Column { spec, .. } => &spec.route,
        }
    }

    fn stream_idx(&self) -> Option<usize> {
        match self {
            NavPos::EmptyDevice { .. } => None,
            NavPos::Column { stream_idx, .. } => Some(*stream_idx),
        }
    }

    fn column_idx(&self) -> Option<usize> {
        match self {
            NavPos::EmptyDevice { .. } => None,
            NavPos::Column { spec, .. } => Some(spec.column_id.index()),
        }
    }

    fn spec(&self) -> Option<&ColumnKey> {
        match self {
            NavPos::EmptyDevice { .. } => None,
            NavPos::Column { spec, .. } => Some(spec),
        }
    }
}

/// A cursor movement. Each one re-engages `follow_selection`. A `NavScroll`
/// moves the viewport and leaves the cursor behind.
#[derive(Debug, Clone, Copy)]
enum NavMove {
    Up,
    Down,
    Left,
    Right,
    DeviceNext,
    DevicePrev,
    Home,
    End,
}

#[derive(Debug, Clone, Default)]
struct Nav {
    idx: usize,
}

impl Nav {
    fn step(&mut self, mv: NavMove, items: &[NavPos]) {
        match mv {
            NavMove::Up => self.step_linear(items, true),
            NavMove::Down => self.step_linear(items, false),
            NavMove::Left => self.step_between_streams(items, true),
            NavMove::Right => self.step_between_streams(items, false),
            NavMove::DevicePrev => self.step_device(items, true),
            NavMove::DeviceNext => self.step_device(items, false),
            NavMove::Home => self.home(items),
            NavMove::End => self.end(items),
        }
    }

    /// Steps to the previous or next item of the flattened tree, for Up and
    /// Down.
    fn step_linear(&mut self, items: &[NavPos], backward: bool) {
        if items.is_empty() {
            return;
        }
        let len = items.len();
        self.idx = if backward {
            (self.idx + len - 1) % len
        } else {
            (self.idx + 1) % len
        };
    }

    /// Jumps to the previous or next stream across all devices, keeping the
    /// column position, for Left and Right.
    fn step_between_streams(&mut self, items: &[NavPos], backward: bool) {
        if items.is_empty() {
            return;
        }
        let cur = &items[self.idx];
        let (cur_dev, cur_stream, cur_column) = match cur {
            NavPos::EmptyDevice { .. } => return,
            NavPos::Column {
                device_idx,
                stream_idx,
                spec,
            } => (*device_idx, *stream_idx, spec.column_id),
        };

        let mut streams: Vec<(usize, usize)> = items
            .iter()
            .filter_map(|pos| match pos {
                NavPos::Column {
                    device_idx,
                    stream_idx,
                    ..
                } => Some((*device_idx, *stream_idx)),
                _ => None,
            })
            .collect();
        streams.dedup();

        if streams.len() <= 1 {
            return;
        }

        let pos = streams
            .iter()
            .position(|&s| s == (cur_dev, cur_stream))
            .unwrap_or(0);
        let len = streams.len();
        let (target_dev, target_stream) = streams[if backward {
            (pos + len - 1) % len
        } else {
            (pos + 1) % len
        }];

        self.idx = items
            .iter()
            .enumerate()
            .filter(|(_, pos)| {
                matches!(pos,
                    NavPos::Column { device_idx, stream_idx, .. }
                        if *device_idx == target_dev && *stream_idx == target_stream)
            })
            .min_by_key(|(_, pos)| {
                (pos.column_idx().unwrap_or(0) as isize - cur_column.index() as isize).abs()
            })
            .map(|(i, _)| i)
            .unwrap_or(self.idx);
    }

    /// Jumps to the previous or next device at the best matching position,
    /// for Shift+Tab and Tab.
    fn step_device(&mut self, items: &[NavPos], backward: bool) {
        if items.is_empty() {
            return;
        }

        let cur = &items[self.idx];
        let cur_device = cur.device_idx();
        let cur_stream = cur.stream_idx().unwrap_or(0);
        let cur_column = cur.column_idx().unwrap_or(0);

        let mut device_indices: Vec<usize> = items.iter().map(|p| p.device_idx()).collect();
        device_indices.sort();
        device_indices.dedup();

        if device_indices.len() <= 1 {
            return;
        }

        let dev_pos = device_indices
            .iter()
            .position(|&d| d == cur_device)
            .unwrap_or(0);
        let len = device_indices.len();
        let new_dev_pos = if backward {
            (dev_pos + len - 1) % len
        } else {
            (dev_pos + 1) % len
        };
        let target_device = device_indices[new_dev_pos];

        self.idx = items
            .iter()
            .enumerate()
            .filter(|(_, pos)| pos.device_idx() == target_device)
            .map(|(i, pos)| {
                let dist = match pos {
                    NavPos::EmptyDevice { .. } => (0, 0),
                    NavPos::Column {
                        stream_idx, spec, ..
                    } => {
                        let s = (*stream_idx as isize - cur_stream as isize).abs();
                        let c = (spec.column_id.index() as isize - cur_column as isize).abs();
                        (s, c)
                    }
                };
                (i, dist)
            })
            .min_by_key(|&(_, dist)| dist)
            .map(|(i, _)| i)
            .unwrap_or(self.idx);
    }

    fn home(&mut self, items: &[NavPos]) {
        if !items.is_empty() {
            self.idx = 0;
        }
    }

    fn end(&mut self, items: &[NavPos]) {
        if !items.is_empty() {
            self.idx = items.len() - 1;
        }
    }
}

/// A drag over the left pane's nav rows. `anchor` stays at the press and
/// `cursor` follows the pointer. The sweep pins every column in the range,
/// or unpins them when the anchor started out pinned (`unpin`).
#[derive(Debug, Clone)]
struct DragState {
    anchor: usize,
    cursor: usize,
    unpin: bool,
    /// Set once the cursor leaves the anchor row, which makes the drag a
    /// sweep, not a click.
    moved: bool,
    origin: DragOrigin,
}

/// What the press landed on, which decides what a motionless release does.
#[derive(Debug, Clone)]
enum DragOrigin {
    /// A column row. Releasing selects it.
    Column,
    /// A device header. Releasing opens the palette at this route.
    Device(DeviceRoute),
    /// The blank separator after a device block. Releasing does nothing.
    Gap,
}

impl DragState {
    fn range(&self) -> (usize, usize) {
        (self.anchor.min(self.cursor), self.anchor.max(self.cursor))
    }
}

#[derive(Debug, Clone, Default)]
struct Theme {
    value_bounds: HashMap<String, (std::ops::RangeInclusive<f64>, bool)>,
}

impl Theme {
    fn get_value_color(&self, stream: &str, col: &str, val: f64) -> Option<Color> {
        if val.is_nan() {
            return Some(Color::LightYellow);
        }
        let key = format!("{}.{}", stream, col);
        if let Some((range, is_temp)) = self.value_bounds.get(&key) {
            if val < *range.start() {
                Some(if *is_temp { Color::Blue } else { Color::Red })
            } else if val > *range.end() {
                Some(Color::Red)
            } else {
                Some(Color::Green)
            }
        } else {
            None
        }
    }
}

fn row_style(color: Color, selected: bool, stale: bool) -> Style {
    let mut s = Style::default().fg(color);
    if stale {
        s = s.add_modifier(Modifier::DIM);
    }
    if selected {
        s = s.add_modifier(Modifier::BOLD);
        s = s.reversed();
    }
    s
}

#[derive(Debug, Clone, Default)]
struct DeviceStatus {
    last_heartbeat: Option<Instant>,
}

impl DeviceStatus {
    fn on_heartbeat(&mut self) {
        self.last_heartbeat = Some(Instant::now());
    }

    fn is_alive(&self, timeout: Duration) -> bool {
        self.last_heartbeat
            .map(|t| t.elapsed() < timeout)
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone)]
enum Action {
    Quit,
    OpenConsole,
    Nav(NavMove),
    NavScroll(i16),
    ClickNavIdx(usize),
    DragStart(usize, DragOrigin),
    DragTo(usize),
    DragEnd,
    OpenPaletteRoute(DeviceRoute),
    TogglePlot,
    TogglePlotSeries,
    ClosePlot,
    ToggleFft,
    ToggleFooter,
    ToggleRoutes,
    ToggleLegend,
    AdjustWindow(f64),
    AdjustPlotWidth(i16),
    AdjustPrecision(i8),
}

#[derive(Debug, Clone)]
struct ViewConfig {
    show_plot: bool,
    show_footer: bool,
    show_routes: bool,
    show_legend: bool,
    show_fft: bool,
    plot_window_seconds: f64,
    plot_width_percent: u16,
    axis_precision: usize,
    follow_selection: bool,
    scroll: u16,
    desc_width: usize,
    units_width: usize,
    theme: Theme,
}

impl Default for ViewConfig {
    fn default() -> Self {
        Self {
            show_plot: false,
            show_footer: true,
            show_routes: false,
            show_legend: true,
            show_fft: false,
            plot_window_seconds: 5.0,
            plot_width_percent: 70,
            axis_precision: 3,
            follow_selection: true,
            scroll: 0,
            desc_width: 0,
            units_width: 0,
            theme: Theme::default(),
        }
    }
}

struct FftSeries {
    key: ColumnKey,
    label: String,
    units: String,
    color_idx: usize,
    data: Spectrum,
}

/// Why the FFT pane shows no spectrum.
#[derive(Debug, Clone)]
enum FftStatus {
    WaitingForSelection,
    /// The estimate for a channel sampled at `sampling_hz` gave none.
    Missing {
        why: NoSpectrum,
        sampling_hz: f64,
    },
}

struct MonitorState {
    parent_route: DeviceRoute,
    view: ViewConfig,

    nav: Nav,
    nav_items: Vec<NavPos>,

    discovered_routes: HashSet<DeviceRoute>,
    device_status: HashMap<DeviceRoute, DeviceStatus>,
    device_metadata: HashMap<DeviceRoute, DeviceMetadataSnapshot>,
    fft_series: Vec<FftSeries>,
    fft_status: FftStatus,
    plot_series: Vec<PlotSeries>,
    plot_pipes: HashMap<ColumnKey, ColumnProcessor<Fpcs>>,
    fft_pipes: HashMap<ColumnKey, ColumnProcessor<Welch>>,
    /// Channels pinned with Space, in selection order. When it is empty, the
    /// plot follows the cursor.
    plotted: Vec<ColumnKey>,
    /// The color slot of each pinned channel, which also sets its draw order.
    /// A channel keeps its slot while it stays pinned.
    plot_slots: HashMap<ColumnKey, usize>,
    /// Shared wall-clock x-axis window `[t_start, t_end]` for all series.
    plot_x_bounds: Option<[f64; 2]>,

    console: RpcConsole,

    left_inner: Rect,
    col_map: HashMap<usize, usize>,
    device_map: HashMap<usize, DeviceRoute>,
    /// The hit width of each left-pane line. A row's hitbox is its text.
    line_widths: Vec<u16>,
    drag: Option<DragState>,
    /// Last mouse position, for deriving the hovered row at render time.
    last_mouse: Option<(u16, u16)>,
}

impl MonitorState {
    fn new(parent_route: DeviceRoute, console: RpcConsole) -> Self {
        Self {
            parent_route,
            view: ViewConfig::default(),
            nav: Nav::default(),
            nav_items: Vec::new(),
            discovered_routes: HashSet::new(),
            device_status: HashMap::new(),
            device_metadata: HashMap::new(),
            fft_series: Vec::new(),
            fft_status: FftStatus::WaitingForSelection,
            plot_series: Vec::new(),
            plot_pipes: HashMap::new(),
            fft_pipes: HashMap::new(),
            plotted: Vec::new(),
            plot_slots: HashMap::new(),
            plot_x_bounds: None,
            console,
            left_inner: Rect::default(),
            col_map: HashMap::new(),
            device_map: HashMap::new(),
            line_widths: Vec::new(),
            drag: None,
            last_mouse: None,
        }
    }

    fn update(&mut self, action: Action) -> bool {
        match action {
            Action::Quit => return true,
            Action::OpenConsole => {
                // The palette follows the selected device until a route is
                // picked in it.
                self.console.unpick();
                self.console.open(self.current_route());
            }
            Action::OpenPaletteRoute(route) => {
                if self.console.is_open() {
                    // Same device toggles the palette closed; another retargets it.
                    if self.console.target(self.current_route()) == route {
                        self.console.close();
                    } else {
                        self.console.pick(route);
                    }
                } else {
                    self.console.pick(route);
                    self.console.open(self.current_route());
                }
            }
            Action::Nav(mv) => {
                self.view.follow_selection = true;
                self.nav.step(mv, &self.nav_items);
            }
            Action::NavScroll(delta) => {
                self.view.follow_selection = false;
                self.view.scroll = if delta < 0 {
                    self.view.scroll.saturating_sub(delta.unsigned_abs())
                } else {
                    self.view.scroll.saturating_add(delta as u16)
                };
            }
            Action::ClickNavIdx(idx) => {
                if idx < self.nav_items.len() {
                    let was_selected = self.nav.idx == idx;
                    // A clicked row is already visible; the view must not move.
                    self.view.follow_selection = false;
                    self.nav.idx = idx;

                    let is_column = matches!(self.nav_items[idx], NavPos::Column { .. });
                    if is_column && (was_selected || !self.plotted.is_empty()) {
                        self.toggle_pin_current();
                    }
                    self.console.refresh(self.current_route());
                }
            }
            Action::DragStart(idx, origin) => {
                if idx < self.nav_items.len() {
                    let unpin = self.nav_items[idx]
                        .spec()
                        .is_some_and(|col| self.plotted.contains(col));
                    self.drag = Some(DragState {
                        anchor: idx,
                        cursor: idx,
                        unpin,
                        moved: false,
                        origin,
                    });
                }
            }
            Action::DragTo(idx) => {
                if idx < self.nav_items.len() {
                    // Same-row jitter isn't movement; a wobbly click stays a click.
                    let cursor_changed = self.drag.as_ref().is_some_and(|d| d.cursor != idx);
                    if cursor_changed {
                        if let Some(drag) = &mut self.drag {
                            drag.cursor = idx;
                            drag.moved = true;
                        }
                        self.nav.idx = idx;
                        self.view.follow_selection = false;

                        // Advance the view one row when the drag reaches its edge.
                        if let Some(&line) = self.col_map.get(&idx) {
                            let scroll = self.view.scroll as usize;
                            let view_h = self.left_inner.height as usize;
                            if line + 1 >= scroll + view_h {
                                self.view.scroll = self.view.scroll.saturating_add(1);
                            } else if line == scroll && scroll > 0 {
                                self.view.scroll -= 1;
                            }
                        }
                    }
                }
            }
            Action::DragEnd => {
                if let Some(drag) = self.drag.take() {
                    if !self.nav_items.is_empty() {
                        let max_idx = self.nav_items.len() - 1;
                        if !drag.moved {
                            let click = match drag.origin {
                                DragOrigin::Device(route) => Some(Action::OpenPaletteRoute(route)),
                                DragOrigin::Column => {
                                    Some(Action::ClickNavIdx(drag.anchor.min(max_idx)))
                                }
                                DragOrigin::Gap => None,
                            };
                            if let Some(click) = click {
                                self.update(click);
                            }
                        } else {
                            let (lo, hi) = drag.range();
                            self.paint_range(lo.min(max_idx), hi.min(max_idx), drag.unpin);
                            self.nav.idx = drag.cursor.min(max_idx);
                            self.view.follow_selection = false;
                        }
                    }
                }
            }
            Action::TogglePlot => {
                if self.current_selection().is_some() {
                    self.view.show_plot = !self.view.show_plot;
                }
            }
            Action::TogglePlotSeries => self.toggle_pin_current(),
            Action::ClosePlot => {
                self.view.show_plot = false;
                self.plotted.clear();
                self.plot_slots.clear();
            }
            Action::ToggleFft => {
                if self.view.show_plot {
                    self.view.show_fft = !self.view.show_fft;
                }
            }
            Action::ToggleFooter => self.view.show_footer = !self.view.show_footer,
            Action::ToggleRoutes => self.view.show_routes = !self.view.show_routes,
            Action::ToggleLegend => self.view.show_legend = !self.view.show_legend,
            Action::AdjustWindow(d) => {
                self.view.plot_window_seconds = (self.view.plot_window_seconds + d)
                    .clamp(MIN_PLOT_WINDOW_SECONDS, MAX_PLOT_WINDOW_SECONDS)
            }
            Action::AdjustPlotWidth(d) => {
                self.view.plot_width_percent =
                    (self.view.plot_width_percent as i16 + d).clamp(20, 90) as u16
            }
            Action::AdjustPrecision(delta) => {
                let new_p = self.view.axis_precision as i16 + delta as i16;
                self.view.axis_precision = new_p.clamp(0, 5) as usize;
            }
        }
        false
    }

    fn toggle_pin_current(&mut self) {
        if let Some(col) = self.current_selection() {
            if self.plotted.contains(&col) {
                self.unpin_column(&col);
            } else {
                self.pin_column(col);
            }
        }
    }

    /// Pins `col` if it isn't already, assigning the lowest free color slot.
    fn pin_column(&mut self, col: ColumnKey) {
        if self.plotted.contains(&col) {
            return;
        }
        let lowest_free_slot = (0..)
            .find(|slot| !self.plot_slots.values().any(|used| used == slot))
            .unwrap();
        self.plot_slots.insert(col, lowest_free_slot);
        self.plotted.push(col);
        self.view.show_plot = true;
    }

    fn unpin_column(&mut self, col: &ColumnKey) {
        if let Some(pos) = self.plotted.iter().position(|k| k == col) {
            self.plotted.remove(pos);
            self.plot_slots.remove(col);
        }
    }

    /// Pins every column in nav range `[lo, hi]`, or unpins them when `unpin`.
    fn paint_range(&mut self, lo: usize, hi: usize, unpin: bool) {
        let cols: Vec<ColumnKey> = self.nav_items[lo..=hi]
            .iter()
            .filter_map(|p| p.spec().cloned())
            .collect();
        for col in cols {
            if unpin {
                self.unpin_column(&col);
            } else {
                self.pin_column(col);
            }
        }
    }

    fn visible_routes(&self) -> Vec<DeviceRoute> {
        let mut routes: Vec<_> = self.discovered_routes.iter().copied().collect();
        routes.sort();
        routes
    }

    fn rebuild_nav_items(&mut self, buffer: &Buffer) {
        let prev_selection = self.nav_items.get(self.nav.idx).cloned();

        let routes = self.visible_routes();
        let mut new_items = Vec::new();

        for (dev_idx, route) in routes.iter().enumerate() {
            let mut stream_ids: Vec<_> = buffer
                .stream_keys()
                .filter(|k| &k.route == route)
                .map(|k| k.stream_id)
                .collect();
            stream_ids.sort();
            stream_ids.dedup();

            if stream_ids.is_empty() {
                new_items.push(NavPos::EmptyDevice {
                    device_idx: dev_idx,
                    route: *route,
                });
            } else {
                for (stream_idx, sid) in stream_ids.iter().enumerate() {
                    let key = StreamKey::new(*route, *sid);
                    if let Some(row) = buffer.latest_row(&key) {
                        for (column_idx, _) in row.schema().iter().enumerate() {
                            new_items.push(NavPos::Column {
                                device_idx: dev_idx,
                                stream_idx,
                                spec: ColumnKey {
                                    route: *route,
                                    stream_id: *sid,
                                    column_id: twinleaf::ColumnId::new(
                                        u8::try_from(column_idx)
                                            .expect("wire schemas contain at most 256 columns"),
                                    ),
                                },
                            });
                        }
                    }
                }
            }
        }

        self.nav_items = new_items;

        if self.nav_items.is_empty() {
            self.nav.idx = 0;
            return;
        }

        // Re-find the previous selection by identity, not position.
        self.nav.idx = prev_selection
            .and_then(|prev| {
                let prev_spec = prev.spec().cloned();
                let prev_route = *prev.route();
                self.nav_items
                    .iter()
                    .position(|pos| match (&prev_spec, pos.spec()) {
                        (Some(a), Some(b)) => a == b,
                        (None, _) => pos.route() == &prev_route,
                        _ => false,
                    })
            })
            .unwrap_or_else(|| self.nav.idx.min(self.nav_items.len() - 1));
    }

    fn current_pos(&self) -> Option<&NavPos> {
        self.nav_items.get(self.nav.idx)
    }

    fn current_selection(&self) -> Option<ColumnKey> {
        self.current_pos().and_then(|p| p.spec().cloned())
    }

    fn current_route(&self) -> DeviceRoute {
        self.current_pos()
            .map(|p| *p.route())
            .unwrap_or_else(|| self.parent_route)
    }

    fn current_device_index(&self) -> usize {
        self.current_pos().map(|p| p.device_idx()).unwrap_or(0)
    }

    fn device_count(&self) -> usize {
        self.visible_routes().len()
    }

    fn handle_event(&mut self, event: StreamEvent) {
        match event {
            StreamEvent::RouteDiscovered { route } => {
                self.discovered_routes.insert(route);
                self.device_status.entry(route).or_default();
            }
            StreamEvent::Device {
                route,
                event: DeviceEvent::Heartbeat { .. },
            } => self.device_status.entry(route).or_default().on_heartbeat(),
            StreamEvent::Device {
                route,
                event: DeviceEvent::Metadata(snapshot),
            } => {
                self.device_metadata.insert(route, snapshot);
            }
            _ => {}
        }
    }

    fn slot_of(&self, key: &ColumnKey) -> usize {
        self.plot_slots.get(key).copied().unwrap_or(0)
    }

    /// Returns how many samples of `run` span the plot window, at least 10.
    fn window_samples(&self, run: &Run) -> usize {
        (self.view.plot_window_seconds * run.effective_rate())
            .ceil()
            .max(10.0) as usize
    }

    fn update_plot_window(&mut self, buffer: &Buffer, term_width: u16) {
        self.plot_series = Vec::new();
        self.fft_series = Vec::new();
        self.fft_status = FftStatus::WaitingForSelection;
        self.plot_x_bounds = None;

        if !self.view.show_plot {
            self.plot_pipes.clear();
            self.fft_pipes.clear();
        } else if self.view.show_fft {
            self.plot_pipes.clear();
            self.update_fft_series(buffer);
        } else {
            self.fft_pipes.clear();
            self.update_time_series(buffer, term_width);
        }
    }

    /// Returns the channels to plot, in slot order, which is also draw order.
    /// These are the pinned channels, or the focused channel when nothing is
    /// pinned.
    fn plotted_keys(&self) -> Vec<ColumnKey> {
        let mut keys = if self.plotted.is_empty() {
            self.current_selection().into_iter().collect()
        } else {
            self.plotted.clone()
        };
        keys.sort_by_key(|k| self.slot_of(k));
        keys
    }

    fn update_fft_series(&mut self, buffer: &Buffer) {
        let keys = self.plotted_keys();
        let keys_set: HashSet<&ColumnKey> = keys.iter().collect();
        self.fft_pipes.retain(|k, _| keys_set.contains(k));

        for key in keys.into_iter() {
            let color_idx = self.slot_of(&key);
            let Some(run) = buffer.run(&key.stream_key()) else {
                continue;
            };
            let window_samples = self.window_samples(run);
            let sampling_hz = run.effective_rate();

            let pipe = self.fft_pipes.entry(key).or_insert_with(|| {
                ColumnProcessor::new(key, Welch::new(window_samples, sampling_hz))
            });
            if pipe.op().window_samples() != window_samples
                || pipe.op().sampling_hz() != sampling_hz
            {
                pipe.op_mut().configure(window_samples, sampling_hz);
                pipe.invalidate();
            }
            pipe.catch_up(buffer);

            match pipe.output() {
                Ok(data) => {
                    let Some((label, units)) = column_label_units(buffer, &key) else {
                        continue;
                    };
                    self.fft_series.push(FftSeries {
                        key,
                        label,
                        units,
                        color_idx,
                        data: data.clone(),
                    });
                }
                Err(why) => {
                    self.fft_status = FftStatus::Missing {
                        why: why.clone(),
                        sampling_hz,
                    };
                }
            }
        }
    }

    /// Decimates every plotted channel over one shared wall-clock window
    /// `[t_end - window, t_end]`, where `t_end` is the newest sample across
    /// the plotted channels. A lagging channel stops short of the right edge.
    fn update_time_series(&mut self, buffer: &Buffer, term_width: u16) {
        let keys = self.plotted_keys();

        let t_end = keys
            .iter()
            .filter_map(|k| buffer.run(&k.stream_key()).and_then(Run::last_timestamp))
            .fold(f64::NEG_INFINITY, f64::max);
        if !t_end.is_finite() {
            return;
        }
        let t_start = t_end - self.view.plot_window_seconds;
        self.plot_x_bounds = Some([t_start, t_end]);

        let buckets = (term_width as usize * PLOT_POINTS_PER_CELL).max(MIN_PLOT_BUCKETS);

        let keys_set: HashSet<&ColumnKey> = keys.iter().collect();
        self.plot_pipes.retain(|k, _| keys_set.contains(k));

        for key in &keys {
            let window_samples = buffer
                .run(&key.stream_key())
                .map(|run| self.window_samples(run));
            let pipe = self.plot_pipes.entry(*key).or_insert_with(|| {
                ColumnProcessor::new(*key, Fpcs::new(1, MAX_PLOT_WINDOW_SECONDS * 1.25))
            });
            if let Some(window_samples) = window_samples {
                let ratio = ((window_samples as f64) / (buckets as f64)).ceil().max(1.0) as usize;
                if ratio != pipe.op().ratio() {
                    pipe.op_mut().set_ratio(ratio);
                    pipe.invalidate();
                }
            }
            pipe.catch_up(buffer);
        }

        for key in keys.into_iter() {
            let color_idx = self.slot_of(&key);
            let Some(pipe) = self.plot_pipes.get(&key) else {
                continue;
            };
            let out = pipe.output();
            let start = out.partition_point(|&(t, _)| t < t_start);
            let end = out.partition_point(|&(t, _)| t <= t_end);
            if start >= end {
                continue;
            }
            let Some((label, units)) = column_label_units(buffer, &key) else {
                continue;
            };
            self.plot_series.push(PlotSeries {
                label,
                units,
                key,
                color_idx,
                points: out[start..end].to_vec(),
            });
        }
    }
}

fn column_label_units(buffer: &Buffer, key: &ColumnKey) -> Option<(String, String)> {
    buffer
        .column_metadata(key)
        .map(|metadata| (metadata.description.to_string(), metadata.units.to_string()))
}

fn get_action(ev: Event, app: &mut MonitorState) -> Option<Action> {
    if let Event::Key(k) = ev {
        if k.kind != KeyEventKind::Press {
            return None;
        }
        if app.console.is_open() {
            let routes = app.visible_routes();
            app.console.handle_key(k, app.current_route(), &routes);
            return None;
        }
        match k.code {
            KeyCode::Char(':') => Some(Action::OpenConsole),
            KeyCode::Char('q') => Some(Action::Quit),
            KeyCode::Char('c') if k.modifiers == KeyModifiers::CONTROL => Some(Action::Quit),
            KeyCode::Esc => Some(Action::ClosePlot),
            KeyCode::Up | KeyCode::Char('k') => Some(Action::Nav(NavMove::Up)),
            KeyCode::Down | KeyCode::Char('j') => Some(Action::Nav(NavMove::Down)),
            KeyCode::Left => Some(Action::Nav(NavMove::Left)),
            KeyCode::Right => Some(Action::Nav(NavMove::Right)),
            KeyCode::BackTab => Some(Action::Nav(NavMove::DevicePrev)),
            KeyCode::Tab => Some(Action::Nav(NavMove::DeviceNext)),
            KeyCode::PageUp => Some(Action::NavScroll(-10)),
            KeyCode::PageDown => Some(Action::NavScroll(10)),
            KeyCode::Home => Some(Action::Nav(NavMove::Home)),
            KeyCode::End => Some(Action::Nav(NavMove::End)),
            KeyCode::Enter => Some(Action::TogglePlot),
            KeyCode::Char(' ') => Some(Action::TogglePlotSeries),
            KeyCode::Char('f') => Some(Action::ToggleFft),
            KeyCode::Char('h') => Some(Action::ToggleFooter),
            KeyCode::Char('r') => Some(Action::ToggleRoutes),
            KeyCode::Char('l') => Some(Action::ToggleLegend),
            KeyCode::Char('=') => Some(Action::AdjustWindow(PLOT_WINDOW_FINE_STEP_SECONDS)),
            KeyCode::Char('-') => Some(Action::AdjustWindow(-PLOT_WINDOW_FINE_STEP_SECONDS)),
            KeyCode::Char('+') => Some(Action::AdjustWindow(PLOT_WINDOW_COARSE_STEP_SECONDS)),
            KeyCode::Char('_') => Some(Action::AdjustWindow(-PLOT_WINDOW_COARSE_STEP_SECONDS)),
            KeyCode::Char('[') => Some(Action::AdjustPlotWidth(5)),
            KeyCode::Char(']') => Some(Action::AdjustPlotWidth(-5)),
            KeyCode::Char(',') | KeyCode::Char('<') => Some(Action::AdjustPrecision(-1)),
            KeyCode::Char('.') | KeyCode::Char('>') => Some(Action::AdjustPrecision(1)),
            _ => None,
        }
    } else if let Event::Mouse(m) = ev {
        app.last_mouse = Some((m.column, m.row));
        if matches!(m.kind, MouseEventKind::Up(MouseButton::Left)) && app.drag.is_some() {
            return Some(Action::DragEnd);
        }
        let routes = app.visible_routes();
        if app.console.handle_mouse(m, app.current_route(), &routes) {
            return None;
        }
        if app.left_inner.contains(Position::new(m.column, m.row)) {
            return match m.kind {
                MouseEventKind::ScrollDown => Some(Action::NavScroll(3)),
                MouseEventKind::ScrollUp => Some(Action::NavScroll(-3)),
                MouseEventKind::Down(MouseButton::Left)
                    if within_row_text(app, m.column, m.row) =>
                {
                    match nav_idx_at_row(app, m.row) {
                        Some(idx) => Some(Action::DragStart(idx, DragOrigin::Column)),
                        None => device_route_at_row(app, m.row).map(|route| {
                            // Headers anchor at the device's first nav row so
                            // a sweep can start from them.
                            match app.nav_items.iter().position(|p| p.route() == &route) {
                                Some(idx) => Action::DragStart(idx, DragOrigin::Device(route)),
                                None => Action::OpenPaletteRoute(route),
                            }
                        }),
                    }
                }
                MouseEventKind::Down(MouseButton::Left) => gap_nav_idx_at_row(app, m.row)
                    .map(|idx| Action::DragStart(idx, DragOrigin::Gap)),
                MouseEventKind::Drag(MouseButton::Left) if app.drag.is_some() => {
                    nav_idx_at_row(app, m.row).map(Action::DragTo)
                }
                _ => None,
            };
        }
        None
    } else {
        None
    }
}

/// Returns the left-pane line index at screen row `row`.
fn line_at_row(app: &MonitorState, row: u16) -> usize {
    app.view.scroll as usize + row.saturating_sub(app.left_inner.y) as usize
}

/// Returns `true` if `col` falls within the rendered text of the line at
/// `row`.
fn within_row_text(app: &MonitorState, col: u16, row: u16) -> bool {
    app.line_widths
        .get(line_at_row(app, row))
        .is_some_and(|&w| col < app.left_inner.x + w)
}

/// Returns the nav index rendered at screen row `row`, if any.
fn nav_idx_at_row(app: &MonitorState, row: u16) -> Option<usize> {
    let line = line_at_row(app, row);
    app.col_map
        .iter()
        .find(|(_, &mapped)| mapped == line)
        .map(|(&nav_idx, _)| nav_idx)
}

/// Returns the device header rendered at screen row `row`, if any.
fn device_route_at_row(app: &MonitorState, row: u16) -> Option<DeviceRoute> {
    app.device_map.get(&line_at_row(app, row)).cloned()
}

/// Returns the nav index that a press at screen row `row` anchors to, if the
/// row is the blank separator after a device block. That is the block's last
/// column, as a header anchors to its first.
fn gap_nav_idx_at_row(app: &MonitorState, row: u16) -> Option<usize> {
    let line = line_at_row(app, row);
    if app.line_widths.get(line).copied() != Some(0) {
        return None;
    }
    let above = line.checked_sub(1)?;
    app.col_map
        .iter()
        .find(|(_, &mapped)| mapped == above)
        .map(|(&nav_idx, _)| nav_idx)
}

fn draw_ui(
    terminal: &mut DefaultTerminal,
    app: &mut MonitorState,
    buffer: &Buffer,
) -> Result<(), io::Error> {
    terminal.draw(|f| {
        let size = f.area();
        let height = size.height;

        let (main_area, footer_area) = {
            let (main_constraint, footer_constraint) = if app.console.is_open() {
                if height >= 18 {
                    (
                        Constraint::Min(10),
                        Constraint::Length(app.console.height()),
                    )
                } else if height >= 12 {
                    (Constraint::Min(2), Constraint::Length(8))
                } else if height >= 5 {
                    (Constraint::Min(2), Constraint::Length(3))
                } else {
                    (Constraint::Min(0), Constraint::Length(2))
                }
            } else if app.view.show_footer {
                (Constraint::Min(10), Constraint::Length(6))
            } else {
                (Constraint::Min(10), Constraint::Length(2))
            };
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([main_constraint, footer_constraint])
                .split(size);
            (chunks[0], Some(chunks[1]))
        };

        let (left, right) = if app.console.is_open() && height < 3 {
            (None, None)
        } else if app.view.show_plot {
            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Percentage(100 - app.view.plot_width_percent),
                    Constraint::Percentage(app.view.plot_width_percent),
                ])
                .split(main_area);
            (Some(chunks[0]), Some(chunks[1]))
        } else {
            (Some(main_area), None)
        };

        if let Some(l) = left {
            render_monitor_panel(f, app, l, Instant::now(), buffer);
        }
        if let Some(r) = right {
            render_graphics_panel(f, app, r);
        }
        if let Some(foot) = footer_area {
            render_footer(f, app, foot);
        }
    })?;
    Ok(())
}

fn render_monitor_panel(
    f: &mut Frame,
    app: &mut MonitorState,
    area: Rect,
    now: Instant,
    buffer: &Buffer,
) {
    let inner = Rect {
        x: area.x,
        y: area.y,
        width: area.width.saturating_sub(1),
        height: area.height,
    };
    let (mut lines, col_map, device_map) = build_left_lines(app, now, buffer);
    app.left_inner = inner;
    app.col_map = col_map.clone();
    app.device_map = device_map;
    app.line_widths = row_hit_widths(&lines, &col_map);
    let total = lines.len();
    let view_h = inner.height as usize;

    if app.view.follow_selection {
        if let Some(&line_idx) = col_map.get(&app.nav.idx) {
            app.view.scroll = follow_scroll(
                app.view.scroll as usize,
                line_idx,
                view_h,
                total,
                NAV_MARGIN,
            ) as u16;
        }
    }
    app.view.scroll = (app.view.scroll as usize).min(total.saturating_sub(view_h)) as u16;

    // Hover tint under the cursor; drags paint their own selection instead.
    if app.drag.is_none() {
        let over_inner = |&(c, r): &(u16, u16)| inner.contains(Position::new(c, r));
        if let Some((mc, mr)) = app.last_mouse.filter(over_inner) {
            let line = app.view.scroll as usize + (mr - inner.y) as usize;
            let hoverable = within_row_text(app, mc, mr)
                && (app.device_map.contains_key(&line)
                    || app.col_map.values().any(|&mapped| mapped == line));
            if hoverable {
                if let Some(l) = lines.get_mut(line) {
                    *l = std::mem::take(l).bg(Color::DarkGray);
                }
            }
        }
    }
    f.render_widget(Paragraph::new(lines).scroll((app.view.scroll, 0)), inner);

    if total > view_h {
        let mut sb_state = ScrollbarState::new(total - view_h + 1)
            .viewport_content_length(view_h)
            .position(app.view.scroll as usize);
        f.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::VerticalRight)
                .begin_symbol(None)
                .end_symbol(None)
                .thumb_style(Style::default().fg(Color::DarkGray))
                .track_style(Style::default().fg(Color::DarkGray)),
            area,
            &mut sb_state,
        );
    }
}

/// Returns how long a stream at `rate` samples per second may go quiet
/// before it is stale.
fn stale_threshold(rate: f64) -> Duration {
    let period_ms = if rate > 0.0 { 1000.0 / rate } else { 0.0 };
    Duration::from_millis((period_ms * 2.0).max(1200.0) as u64)
}

fn stream_color(name: &str) -> Color {
    use std::hash::{Hash, Hasher};
    const PALETTE: [Color; 10] = [
        Color::Cyan,
        Color::Green,
        Color::Yellow,
        Color::Blue,
        Color::Magenta,
        Color::LightRed,
        Color::LightGreen,
        Color::LightBlue,
        Color::LightMagenta,
        Color::LightCyan,
    ];
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    name.hash(&mut hasher);
    PALETTE[(hasher.finish() % PALETTE.len() as u64) as usize]
}

/// Returns the hit width of each left-pane line. A text row reaches the
/// widest column row, or its own text if longer. A blank separator has zero
/// width.
fn row_hit_widths(lines: &[Line], col_map: &HashMap<usize, usize>) -> Vec<u16> {
    let table_w = col_map
        .values()
        .filter_map(|&l| lines.get(l))
        .map(|l| l.width() as u16)
        .max()
        .unwrap_or(0);
    lines
        .iter()
        .map(|l| match l.width() as u16 {
            0 => 0,
            w => w.max(table_w),
        })
        .collect()
}

fn build_left_lines(
    app: &mut MonitorState,
    now: Instant,
    buffer: &Buffer,
) -> (
    Vec<Line<'static>>,
    HashMap<usize, usize>,
    HashMap<usize, DeviceRoute>,
) {
    let mut lines = Vec::new();
    let mut map = HashMap::new();
    let mut device_map = HashMap::new();

    let routes = app.visible_routes();

    if routes.is_empty() {
        lines.push(Line::from("Waiting for data..."));
        return (lines, map, device_map);
    }

    let latest: HashMap<StreamKey, SampleBatch> = buffer
        .stream_keys()
        .filter_map(|k| buffer.latest_row(k).map(|row| (*k, row)))
        .collect();

    let mut global_idx = 0;
    app.view.desc_width = latest
        .values()
        .flat_map(|row| row.schema().iter())
        .map(|column| column.metadata().description.len())
        .max()
        .unwrap_or(0);
    app.view.units_width = latest
        .values()
        .flat_map(|row| row.schema().iter())
        .map(|column| column.metadata().units.len())
        .max()
        .unwrap_or(0);

    let selected_stream = app.current_selection();

    for (dev_idx, route) in routes.iter().enumerate() {
        let dev = app.device_metadata.get(route).map(|m| m.device());

        let status = app.device_status.get(route);
        let is_alive = status
            .map(|s| s.is_alive(Duration::from_millis(300)))
            .unwrap_or(false);

        let is_selected_header = if app.console.is_open() {
            *route == app.console.target(app.current_route())
        } else {
            dev_idx == app.current_device_index()
        };
        let head_style = if is_selected_header {
            Style::default().add_modifier(Modifier::BOLD | Modifier::UNDERLINED)
        } else {
            Style::default().add_modifier(Modifier::BOLD)
        };

        let header_text = if let Some(d) = dev {
            if d.serial.is_empty() {
                d.name.to_string()
            } else {
                format!("{}  Serial: {}", d.name, d.serial)
            }
        } else {
            format!("<{}>", route)
        };

        let status_indicator = if is_alive { "●" } else { "○" };
        let status_color = if is_alive {
            Color::Green
        } else {
            Color::DarkGray
        };

        let mut header_spans = vec![
            Span::styled(
                format!("{} ", status_indicator),
                Style::default().fg(status_color),
            ),
            Span::styled(header_text, head_style),
        ];
        if app.view.show_routes {
            header_spans.push(Span::raw(format!(" [{}]", route)));
        }

        device_map.insert(lines.len(), *route);
        lines.push(Line::from(header_spans));

        let mut stream_ids: Vec<_> = latest
            .keys()
            .filter(|k| &k.route == route)
            .map(|k| k.stream_id)
            .collect();
        stream_ids.sort();

        if stream_ids.is_empty() {
            map.insert(global_idx, lines.len());
            global_idx += 1;

            lines.push(Line::from(Span::styled(
                "  (no streams yet)",
                Style::default().fg(Color::DarkGray),
            )));
        }

        for sid in stream_ids {
            let key = StreamKey::new(*route, sid);
            let is_current_stream = selected_stream
                .as_ref()
                .is_some_and(|s| s.route == *route && s.stream_id == sid);
            if let Some(row) = latest.get(&key) {
                let is_stale = buffer.run(&key).is_some_and(|run| {
                    now.saturating_duration_since(run.last_seen())
                        > stale_threshold(run.effective_rate())
                });
                let sample = row.row(0).expect("latest_row is one row");
                for (col_idx, (column, value)) in
                    row.schema().iter().zip(sample.values()).enumerate()
                {
                    let metadata = column.metadata();
                    let nav_idx = global_idx;
                    global_idx += 1;
                    map.insert(nav_idx, lines.len());

                    let is_sel = match &app.drag {
                        Some(drag) if drag.moved => {
                            let (lo, hi) = drag.range();
                            (lo..=hi).contains(&nav_idx)
                        }
                        _ => app.nav.idx == nav_idx,
                    };
                    let plot_slot = app.plot_slots.get(&ColumnKey {
                        route: *route,
                        stream_id: sid,
                        column_id: twinleaf::ColumnId::new(
                            u8::try_from(col_idx)
                                .expect("wire schemas contain at most 256 columns"),
                        ),
                    });
                    let label_style = row_style(Color::Reset, is_sel, is_stale);
                    let (val_str, val_f64) = fmt_value(&value);
                    let val_col = app
                        .view
                        .theme
                        .get_value_color(row.stream().name, metadata.name, val_f64)
                        .unwrap_or(Color::Reset);
                    let val_style = row_style(val_col, is_sel, is_stale);

                    let mut desc = metadata.description.to_string();
                    if desc.len() < app.view.desc_width {
                        desc.push_str(&" ".repeat(app.view.desc_width - desc.len()));
                    }

                    let units = metadata.units.to_string();
                    let padded_units = if app.view.units_width > 0 && !units.is_empty() {
                        format!("{:>width$}", units, width = app.view.units_width)
                    } else if app.view.units_width > 0 {
                        " ".repeat(app.view.units_width)
                    } else {
                        String::new()
                    };

                    let (pipe_glyph, pipe_style) = if is_current_stream {
                        ("┃ ", Style::default().fg(stream_color(row.stream().name)))
                    } else {
                        ("│ ", Style::default().fg(Color::DarkGray))
                    };

                    let plot_marker = match plot_slot {
                        Some(&slot) => Span::styled("▆", Style::default().fg(series_color(slot))),
                        None => Span::raw(" "),
                    };

                    lines.push(Line::from(vec![
                        plot_marker,
                        Span::styled(pipe_glyph, pipe_style),
                        Span::styled(desc, label_style),
                        Span::raw("  "),
                        Span::styled(val_str, val_style),
                        Span::raw(" "),
                        Span::styled(padded_units, val_style),
                    ]));
                }
            }
        }
        lines.push(Line::from(""));
    }
    (lines, map, device_map)
}

fn render_footer(f: &mut Frame, app: &mut MonitorState, area: Rect) {
    if app.console.is_open() {
        let default = app.current_route();
        app.console.render(f, area, default);
        return;
    }

    if !app.view.show_footer {
        let minimal = Line::from(vec![
            Span::raw("  "),
            key_span("h"),
            Span::raw(" Toggle Footer"),
        ]);
        f.render_widget(
            Paragraph::new(vec![minimal]).block(
                Block::default()
                    .borders(Borders::TOP)
                    .border_style(Style::default().fg(Color::DarkGray)),
            ),
            area,
        );
        return;
    }

    let mut navigation_spans = vec![
        Span::styled(
            "  Navigation  ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        key_span("↑"),
        key_sep(),
        key_span("↓"),
        Span::raw(" All  "),
        key_span("←"),
        key_sep(),
        key_span("→"),
        Span::raw(" Streams"),
    ];

    if app.device_count() > 1 {
        navigation_spans.push(Span::raw("  "));
        navigation_spans.push(key_span("Tab"));
        navigation_spans.push(key_sep());
        navigation_spans.push(key_span("Shift+Tab"));
        navigation_spans.push(Span::raw(" Devices"));
    }
    let navigation_line = Line::from(navigation_spans);

    let toggle_line = Line::from(vec![
        Span::styled(
            "  Toggle      ",
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ),
        key_span("Enter"),
        Span::raw(" Plot  "),
        key_span("Space"),
        Span::raw(" Pin Series  "),
        key_span("f"),
        Span::raw(" FFT  "),
        key_span("h"),
        Span::raw(" Footer  "),
        key_span("r"),
        Span::raw(" Routes  "),
        key_span("l"),
        Span::raw(" Legend "),
        key_span(":"),
        Span::raw(" Cmd"),
    ]);

    let window_line = Line::from(vec![
        Span::styled(
            "  Plot        ",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        key_span("+"),
        key_sep(),
        key_span("-"),
        Span::raw(" Window (0.5s, Shift 5.0s)  "),
        key_span("["),
        key_sep(),
        key_span("]"),
        Span::raw(" Plot Width  "),
        key_span("<"),
        key_sep(),
        key_span(">"),
        Span::raw(" Plot Precision"),
    ]);

    let scroll_line = Line::from(vec![
        Span::styled(
            "  Scroll      ",
            Style::default()
                .fg(Color::Magenta)
                .add_modifier(Modifier::BOLD),
        ),
        key_span("Home"),
        key_sep(),
        key_span("End"),
        key_sep(),
        key_span("PgUp"),
        key_sep(),
        key_span("PgDn"),
    ]);

    let quit_line = Line::from(vec![
        Span::styled(
            "  Quit        ",
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ),
        key_span("q"),
        Span::raw(" / "),
        key_span("Ctrl+C"),
        Span::raw(" Quit"),
    ]);

    let lines = vec![
        navigation_line,
        toggle_line,
        window_line,
        scroll_line,
        quit_line,
    ];

    let block = Block::default()
        .borders(Borders::TOP)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(Span::styled(
            " Controls ",
            Style::default().add_modifier(Modifier::BOLD),
        ));

    f.render_widget(Paragraph::new(lines).block(block), area);
}

fn key_span(text: &str) -> Span<'static> {
    Span::styled(
        format!(" {} ", text),
        Style::default()
            .fg(Color::White)
            .bg(Color::DarkGray)
            .add_modifier(Modifier::BOLD),
    )
}

fn key_sep() -> Span<'static> {
    Span::raw(" ")
}

fn render_graphics_panel(f: &mut Frame, app: &MonitorState, area: Rect) {
    if !app.view.show_fft {
        render_plot_series(f, app, area);
        return;
    }
    if app.fft_series.is_empty() {
        render_fft_status(f, app, area);
    } else {
        render_fft_series(f, app, area);
    }
}

fn shared_units<'a>(mut units: impl Iterator<Item = &'a str>) -> String {
    match units.next() {
        Some(first) if units.all(|u| u == first) => first.to_string(),
        Some(_) => "mixed".to_string(),
        None => String::new(),
    }
}

fn series_legend_name(show_routes: bool, label: &str, route: &DeviceRoute) -> String {
    if show_routes {
        format!("{} [{}]", label, route)
    } else {
        label.to_string()
    }
}

fn series_dataset<'a>(
    label: &str,
    route: &DeviceRoute,
    show_routes: bool,
    color_idx: usize,
    data: &'a [(f64, f64)],
) -> Dataset<'a> {
    Dataset::default()
        .name(series_legend_name(show_routes, label, route))
        .marker(symbols::Marker::Braille)
        .style(Style::default().fg(series_color(color_idx)))
        .graph_type(GraphType::Line)
        .data(data)
}

fn bounds_union<'a>(points: impl Iterator<Item = &'a (f64, f64)>) -> (f64, f64, f64, f64) {
    let mut min_x = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_y = f64::NEG_INFINITY;
    for &(x, y) in points {
        min_x = min_x.min(x);
        max_x = max_x.max(x);
        min_y = min_y.min(y);
        max_y = max_y.max(y);
    }
    (min_x, max_x, min_y, max_y)
}

fn render_fft_series(f: &mut Frame, app: &MonitorState, area: Rect) {
    let secs = app.view.plot_window_seconds;
    let series = &app.fft_series;

    let units = shared_units(series.iter().map(|s| s.units.as_str()));

    let title = if series.len() == 1 {
        let s = &series[0];
        format!(
            "{} — {} ({:.1}s, FFT {} of {} samples, seg {}, hop {}) | Noise floor: {}",
            s.key.route,
            s.label,
            secs,
            s.data.sample_count,
            s.data.total_sample_count,
            s.data.segment_size,
            s.data.hop_size,
            s.data
                .noise_floor
                .map(|floor| format!("{floor:.3e} {units}/√Hz"))
                // No estimate rather than a worse one: a spectrum too short or
                // too contaminated to support a floor has no honest number to
                // put here.
                .unwrap_or_else(|| "—".to_string()),
        )
    } else {
        format!(
            "{} channels FFT ({:.1}s) [{}/√Hz]",
            series.len(),
            secs,
            units
        )
    };
    let block = Block::default().title(title).borders(Borders::ALL);

    let log_series: Vec<Vec<(f64, f64)>> = series
        .iter()
        .map(|s| {
            s.data
                .points
                .iter()
                .map(|(freq, val)| (freq.log10(), val.log10()))
                .collect()
        })
        .collect();
    let (min_f, max_f, min_d, max_d) = bounds_union(log_series.iter().flatten());

    if !(min_f.is_finite() && max_f.is_finite() && min_d.is_finite() && max_d.is_finite()) {
        f.render_widget(Paragraph::new("No valid FFT data").block(block), area);
        return;
    }

    let y_pad = if (max_d - min_d) > 0.1 {
        (max_d - min_d) * 0.1
    } else {
        0.5
    };

    let datasets: Vec<Dataset> = series
        .iter()
        .zip(log_series.iter())
        .map(|(s, data)| {
            series_dataset(
                &s.label,
                &s.key.route,
                app.view.show_routes,
                s.color_idx,
                data,
            )
        })
        .collect();

    let chart = Chart::new(datasets)
        .block(block)
        .hidden_legend_constraints(LEGEND_CONSTRAINTS)
        .legend_position(app.view.show_legend.then_some(LegendPosition::default()))
        .x_axis(
            Axis::default()
                .title("Freq [Hz] (log)")
                .bounds([min_f, max_f])
                .labels(generate_log_labels(
                    min_f,
                    max_f,
                    5,
                    app.view.axis_precision,
                )),
        )
        .y_axis(
            Axis::default()
                .title(format!("Val [{}/√Hz]", units))
                .bounds([min_d - y_pad, max_d + y_pad])
                .labels(generate_log_labels(
                    min_d - y_pad,
                    max_d + y_pad,
                    5,
                    app.view.axis_precision,
                )),
        );
    f.render_widget(chart, area);
}

fn render_fft_status(f: &mut Frame, app: &MonitorState, area: Rect) {
    let (title, message) = match &app.fft_status {
        FftStatus::WaitingForSelection => (
            "FFT unavailable - no channel selected".to_string(),
            "Select a stream column to plot FFT.".to_string(),
        ),
        FftStatus::Missing { why, sampling_hz } => match why {
            NoSpectrum::WaitingForSamples => (
                "Buffering FFT...".to_string(),
                "Waiting for samples in the selected window.".to_string(),
            ),
            NoSpectrum::TooFewSamples { have, need } => (
                format!("FFT unavailable - {} samples needed", need),
                format!(
                    "Current FFT buffer has {} of {} samples. At {:.3} Hz, current window is {:.1}s.",
                    have, need, sampling_hz, app.view.plot_window_seconds
                ),
            ),
            NoSpectrum::InvalidSampleRate => (
                "FFT unavailable - invalid sample rate".to_string(),
                format!("The stream reports {} Hz.", sampling_hz),
            ),
            NoSpectrum::NoValidFrequencyBins { sample_count } => (
                "FFT unavailable - no valid frequency bins".to_string(),
                format!(
                    "Welch produced no positive finite bins from {} samples at {:.3} Hz.",
                    sample_count, sampling_hz
                ),
            ),
            other => ("FFT unavailable".to_string(), format!("{:?}", other)),
        },
    };
    let block = Block::default().title(title).borders(Borders::ALL);
    f.render_widget(Paragraph::new(message).block(block), area);
}

fn render_plot_series(f: &mut Frame, app: &MonitorState, area: Rect) {
    let secs = app.view.plot_window_seconds;
    let series: Vec<&PlotSeries> = app
        .plot_series
        .iter()
        .filter(|s| !s.points.is_empty())
        .collect();

    if series.is_empty() {
        let block = Block::default()
            .title(format!("Plot ({:.1}s)", secs))
            .borders(Borders::ALL);
        f.render_widget(Paragraph::new("Buffering...").block(block), area);
        return;
    }

    let (min_t, max_t, min_v, max_v) = bounds_union(series.iter().flat_map(|s| s.points.iter()));
    let (min_t, max_t) = match app.plot_x_bounds {
        Some([a, b]) if a.is_finite() && b.is_finite() && a < b => (a, b),
        _ if min_t.is_finite() && max_t.is_finite() && min_t < max_t => (min_t, max_t),
        _ => (0.0, 1.0),
    };
    let pad = if (max_v - min_v).abs() > 1e-10 {
        (max_v - min_v) * 0.4
    } else {
        1.0
    };

    let units = shared_units(series.iter().map(|s| s.units.as_str()));

    let title = if series.len() == 1 {
        format!(
            "{} — {} ({:.1}s)",
            series[0].key.route, series[0].label, secs
        )
    } else {
        format!("{} channels ({:.1}s)", series.len(), secs)
    };
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded);

    let datasets: Vec<Dataset> = series
        .iter()
        .map(|s| {
            series_dataset(
                &s.label,
                &s.key.route,
                app.view.show_routes,
                s.color_idx,
                &s.points,
            )
        })
        .collect();

    let chart = Chart::new(datasets)
        .block(block)
        .hidden_legend_constraints(LEGEND_CONSTRAINTS)
        .legend_position(app.view.show_legend.then_some(LegendPosition::default()))
        .x_axis(
            Axis::default()
                .title("Time [s]")
                .bounds([min_t, max_t])
                .labels(generate_linear_labels(
                    min_t,
                    max_t,
                    3,
                    app.view.axis_precision,
                )),
        )
        .y_axis(
            Axis::default()
                .title(format!("Value [{}]", units))
                .bounds([min_v - pad, max_v + pad])
                .labels(generate_linear_labels(
                    min_v - pad,
                    max_v + pad,
                    5,
                    app.view.axis_precision,
                )),
        );
    f.render_widget(chart, area);
}

/// Returns `count` evenly spaced labels over `[min, max]`, each formatted by
/// `fmt` and right-aligned to a shared width.
fn axis_labels(
    min: f64,
    max: f64,
    count: usize,
    fmt: impl Fn(f64) -> String,
) -> Vec<Span<'static>> {
    if count < 2 {
        return vec![];
    }
    let step = (max - min) / ((count - 1) as f64);
    (0..count)
        .map(|i| Span::from(format!("{:>10}", fmt(min + i as f64 * step))))
        .collect()
}

fn generate_linear_labels(
    min: f64,
    max: f64,
    count: usize,
    precision: usize,
) -> Vec<Span<'static>> {
    axis_labels(min, max, count, |v| format!("{:.precision$}", v))
}

/// Returns `count` labels for a log axis from `min_log` to `max_log`. Every
/// label uses one notation, chosen from the axis maximum.
fn generate_log_labels(
    min_log: f64,
    max_log: f64,
    count: usize,
    precision: usize,
) -> Vec<Span<'static>> {
    let use_scientific = !(0.01..1000.0).contains(&10f64.powf(max_log.max(min_log)).abs());
    axis_labels(min_log, max_log, count, |log_val| {
        let v = 10f64.powf(log_val);
        if use_scientific {
            format!("{:.precision$e}", v)
        } else {
            format!("{:.precision$}", v)
        }
    })
}

fn fmt_value(v: &ColumnData) -> (String, f64) {
    match v {
        ColumnData::Float(x) => (format!("{:15.4}", x), *x),
        ColumnData::Int(x) => (format!("{:15}", x), *x as f64),
        ColumnData::UInt(x) => (format!("{:15}", x), *x as f64),
        _ => ("           type?".to_string(), f64::NAN),
    }
}

fn load_theme(path: &str) -> io::Result<Theme> {
    let mut s = String::new();
    File::open(path)?.read_to_string(&mut s)?;
    let doc =
        DocumentMut::from_str(&s).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let mut bounds = HashMap::new();
    for (k, v) in doc.get_values() {
        let col = k.iter().map(|k| k.get()).collect::<Vec<_>>().join(".");
        if let Value::InlineTable(it) = v {
            let (t, min) = if let Some(v) = get_num(it, "cold") {
                (true, v)
            } else {
                (false, get_num(it, "min").unwrap_or(f64::NEG_INFINITY))
            };
            let max = if let Some(v) = get_num(it, "hot") {
                v
            } else {
                get_num(it, "max").unwrap_or(f64::INFINITY)
            };
            bounds.insert(col, (min..=max, t));
        }
    }
    Ok(Theme {
        value_bounds: bounds,
    })
}

fn get_num(it: &InlineTable, k: &str) -> Option<f64> {
    it.get(k)
        .and_then(|v| v.as_float().or(v.as_integer().map(|i| i as f64)))
}
/// Monitor with the samples it shows, as the screen loop drives it.
struct Monitor {
    app: MonitorState,
    buffer: Buffer,
}

impl Screen for Monitor {
    fn console(&mut self) -> &mut RpcConsole {
        &mut self.app.console
    }

    fn rpc_default(&self) -> DeviceRoute {
        self.app.current_route()
    }

    fn on_batch(&mut self, batch: SampleBatch) {
        self.buffer.process_batch(&batch);
    }

    fn on_event(&mut self, event: StreamEvent) {
        self.app.handle_event(event);
    }

    // Monitor keeps no log to report a lag in.
    fn on_lagged(&mut self, _error: RecvError) {}

    fn on_input(&mut self, event: Event) -> bool {
        get_action(event, &mut self.app).is_some_and(|action| self.app.update(action))
    }

    fn draw(&mut self, terminal: &mut DefaultTerminal) -> io::Result<()> {
        let term_width = terminal.size().map(|s| s.width).unwrap_or(200);
        self.app.update_plot_window(&self.buffer, term_width);
        self.app.rebuild_nav_items(&self.buffer);
        draw_ui(terminal, &mut self.app, &self.buffer)
    }
}

pub fn run_monitor(cli: MonitorCli) -> eyre::Result<()> {
    use eyre::WrapErr;

    let connection = Connection::open(&cli.tio.root)?;
    let parent_route = cli.tio.route;

    // One connection: the library pumps samples, cloned trees call.
    let tree = connection.tree(parent_route);
    let tree = cli
        .depth
        .map_or_else(|| tree.clone(), |depth| tree.to_depth(depth));

    let mut app = MonitorState::new(parent_route, RpcConsole::new(tree.clone()));
    if let Some(path) = &cli.colors {
        app.view.theme =
            load_theme(path).wrap_err_with(|| format!("could not load theme file {}", path))?;
    }
    let mut monitor = Monitor {
        app,
        buffer: Buffer::new(MONITOR_BUFFER_CAPACITY_SAMPLES),
    };
    tui::run(&tree, u64::from(cli.fps), &mut monitor)
}
