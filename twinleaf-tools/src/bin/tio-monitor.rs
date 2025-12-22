// tio-monitor
// Live sensor data display with plot and FFT capabilities
// Build: cargo run --release -- <tio-url> [options]

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::File,
    io::{self, Read},
    str::FromStr,
    time::{Duration, Instant},
};

use clap::Parser;
use crossbeam::channel::{self, Sender};
use ratatui::{
    crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    layout::{Constraint, Direction, Layout, Rect},
    prelude::Backend,
    style::{Color, Modifier, Style},
    symbols,
    text::{Line, Span},
    widgets::{Axis, Block, Borders, Chart, Dataset, GraphType, Paragraph},
    Frame, Terminal,
};
use toml_edit::{DocumentMut, InlineTable, Value};
use tui_prompts::{State, TextState};
use twinleaf::{
    data::{AlignedWindow, Buffer, ColumnBatch, ColumnData, DeviceFullMetadata, Sample},
    device::{DeviceEvent, DeviceTree, TreeEvent, TreeItem, RpcClient, util},
    tio::{
        self,
        proto::{
            DeviceRoute, ProxyStatus, identifiers::{ColumnKey, StreamKey}
        },
    },
};
use twinleaf_tools::TioOpts;
use welch_sde::{Build, SpectralDensity};

#[derive(Parser, Debug)]
#[command(name = "tio-monitor", version, about = "Display live sensor data")]
struct Cli {
    #[command(flatten)]
    tio: TioOpts,
    #[arg(short = 'a', long = "all")]
    all: bool,
    #[arg(long = "fps", default_value_t = 20)]
    fps: u32,
    #[arg(short = 'c', long = "colors")]
    colors: Option<String>,
}

#[derive(Debug, Clone)]
pub enum NavPos {
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
    pub fn device_idx(&self) -> usize {
        match self {
            NavPos::EmptyDevice { device_idx, .. } => *device_idx,
            NavPos::Column { device_idx, .. } => *device_idx,
        }
    }

    pub fn route(&self) -> &DeviceRoute {
        match self {
            NavPos::EmptyDevice { route, .. } => route,
            NavPos::Column { spec, .. } => &spec.route,
        }
    }

    pub fn stream_idx(&self) -> Option<usize> {
        match self {
            NavPos::EmptyDevice { .. } => None,
            NavPos::Column { stream_idx, .. } => Some(*stream_idx),
        }
    }

    pub fn column_idx(&self) -> Option<usize> {
        match self {
            NavPos::EmptyDevice { .. } => None,
            NavPos::Column { spec, .. } => Some(spec.column_id),
        }
    }

    pub fn spec(&self) -> Option<&ColumnKey> {
        match self {
            NavPos::EmptyDevice { .. } => None,
            NavPos::Column { spec, .. } => Some(spec),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Nav {
    pub idx: usize,
}

impl Nav {
    /// Up/Down: linear traversal through flattened tree
    pub fn step_linear(&mut self, items: &[NavPos], backward: bool) {
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

    /// Left/Right: cycle within current stream's columns
    pub fn step_within_stream(&mut self, items: &[NavPos], backward: bool) {
        if items.is_empty() {
            return;
        }
        let cur = &items[self.idx];
        let (dev, stream) = match cur {
            NavPos::EmptyDevice { .. } => return,
            NavPos::Column { device_idx, stream_idx, .. } => (*device_idx, *stream_idx),
        };

        let siblings: Vec<usize> = items
            .iter()
            .enumerate()
            .filter_map(|(i, pos)| match pos {
                NavPos::Column { device_idx, stream_idx, .. }
                    if *device_idx == dev && *stream_idx == stream => Some(i),
                _ => None,
            })
            .collect();

        if let Some(pos) = siblings.iter().position(|&i| i == self.idx) {
            let len = siblings.len();
            let new_pos = if backward {
                (pos + len - 1) % len
            } else {
                (pos + 1) % len
            };
            self.idx = siblings[new_pos];
        }
    }

    /// Tab: jump to next/prev device, find best matching position
    pub fn step_device(&mut self, items: &[NavPos], backward: bool) {
        if items.is_empty() {
            return;
        }

        let cur = &items[self.idx];
        let cur_device = cur.device_idx();
        let cur_stream = cur.stream_idx().unwrap_or(0);
        let cur_column = cur.column_idx().unwrap_or(0);

        // Find all unique device indices
        let mut device_indices: Vec<usize> = items.iter().map(|p| p.device_idx()).collect();
        device_indices.sort();
        device_indices.dedup();

        if device_indices.len() <= 1 {
            return;
        }

        // Find current device position and move to next/prev
        let dev_pos = device_indices.iter().position(|&d| d == cur_device).unwrap_or(0);
        let len = device_indices.len();
        let new_dev_pos = if backward {
            (dev_pos + len - 1) % len
        } else {
            (dev_pos + 1) % len
        };
        let target_device = device_indices[new_dev_pos];

        // Find best match on target device
        self.idx = items
            .iter()
            .enumerate()
            .filter(|(_, pos)| pos.device_idx() == target_device)
            .map(|(i, pos)| {
                let dist = match pos {
                    NavPos::EmptyDevice { .. } => (0, 0),
                    NavPos::Column { stream_idx, spec, .. } => {
                        let s = (*stream_idx as isize - cur_stream as isize).abs();
                        let c = (spec.column_id as isize - cur_column as isize).abs();
                        (s, c)
                    }
                };
                (i, dist)
            })
            .min_by_key(|&(_, dist)| dist)
            .map(|(i, _)| i)
            .unwrap_or(self.idx);
    }

    pub fn home(&mut self, items: &[NavPos]) {
        if !items.is_empty() {
            self.idx = 0;
        }
    }

    pub fn end(&mut self, items: &[NavPos]) {
        if !items.is_empty() {
            self.idx = items.len() - 1;
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Theme {
    pub value_bounds: HashMap<String, (std::ops::RangeInclusive<f64>, bool)>,
}

impl Theme {
    pub fn get_value_color(&self, stream: &str, col: &str, val: f64) -> Option<Color> {
        if val.is_nan() {
            return Some(Color::Yellow);
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

pub struct StyleContext {
    pub is_selected: bool,
    pub is_stale: bool,
    pub in_plot_mode: bool,
    pub base_color: Color,
}

impl Default for StyleContext {
    fn default() -> Self {
        Self {
            is_selected: false,
            is_stale: false,
            in_plot_mode: false,
            base_color: Color::Reset,
        }
    }
}

impl StyleContext {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn selected(mut self, yes: bool) -> Self {
        self.is_selected = yes;
        self
    }
    pub fn stale(mut self, yes: bool) -> Self {
        self.is_stale = yes;
        self
    }
    pub fn plot_mode(mut self, yes: bool) -> Self {
        self.in_plot_mode = yes;
        self
    }
    pub fn color(mut self, c: Color) -> Self {
        self.base_color = c;
        self
    }

    pub fn resolve(&self) -> Style {
        let mut s = Style::default().fg(self.base_color);
        if self.is_stale {
            s = s.add_modifier(Modifier::DIM);
        }
        if self.is_selected {
            s = s.add_modifier(Modifier::BOLD);
            if !self.in_plot_mode {
                s = s.add_modifier(Modifier::RAPID_BLINK);
            }
        }
        s
    }
}

#[derive(Debug, Clone, Default)]
pub struct DeviceStatus {
    pub last_heartbeat: Option<Instant>,
    pub connected: bool,
}

impl DeviceStatus {
    pub fn on_heartbeat(&mut self) {
        self.last_heartbeat = Some(Instant::now());
        self.connected = true;
    }

    pub fn is_alive(&self, timeout: Duration) -> bool {
        self.last_heartbeat
            .map(|t| t.elapsed() < timeout)
            .unwrap_or(false)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Mode {
    Normal,
    Command,
}

#[derive(Debug, Clone)]
pub enum Action {
    Quit,
    SetMode(Mode),
    SubmitCommand,
    NavUp,
    NavDown,
    NavLeft,
    NavRight,
    NavTabNext,
    NavTabPrev,
    NavScroll(i16),
    NavHome,
    NavEnd,
    TogglePlot,
    ClosePlot,
    ToggleFft,
    ToggleFooter,
    ToggleRoutes,
    AdjustWindow(f64),
    AdjustPlotWidth(i16),
    AdjustPrecision(i8),
    HistoryNavigate(i8),
}

#[derive(Debug, Clone)]
pub struct ViewConfig {
    pub show_plot: bool,
    pub show_footer: bool,
    pub show_routes: bool,
    pub show_fft: bool,
    pub plot_window_seconds: f64,
    pub plot_width_percent: u16,
    pub axis_precision: usize,
    pub follow_selection: bool,
    pub scroll: u16,
    pub desc_width: usize,
    pub units_width: usize,
    pub theme: Theme,
}

impl Default for ViewConfig {
    fn default() -> Self {
        Self {
            show_plot: false,
            show_footer: true,
            show_routes: false,
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

#[derive(Debug)]
pub struct RpcReq {
    pub route: DeviceRoute,
    pub method: String,
    pub arg: Option<String>,
}

#[derive(Debug)]
pub struct RpcResp {
    pub result: Result<String, String>,
}

fn exec_rpc(client: &RpcClient, req: &RpcReq) -> Result<String, String> {
    let meta: u16 = client
        .rpc(&req.route, "rpc.info", &req.method)
        .map_err(|_| format!("Unknown RPC: {}", req.method))?;

    let spec = util::parse_rpc_spec(meta, req.method.clone());

    let payload = if let Some(ref s) = req.arg {
        util::rpc_encode_arg(s, &spec.data_kind).map_err(|e| format!("{:?}", e))?
    } else {
        Vec::new()
    };

    let reply_bytes = client
        .raw_rpc(&req.route, &req.method, &payload)
        .map_err(|e| format!("{:?}", e))?;

    if reply_bytes.is_empty() {
        return Ok("OK".to_string());
    }

    let value = util::rpc_decode_reply(&reply_bytes, &spec.data_kind)
        .map_err(|e| format!("{:?}", e))?;

    Ok(util::format_rpc_value_for_cli(&value, &spec.data_kind))
}

pub struct App {
    pub all: bool,
    pub parent_route: DeviceRoute,

    pub mode: Mode,
    pub view: ViewConfig,

    pub nav: Nav,
    pub nav_items: Vec<NavPos>,

    pub discovered_routes: HashSet<DeviceRoute>,
    pub device_status: HashMap<DeviceRoute, DeviceStatus>,
    pub last: BTreeMap<StreamKey, (Sample, Instant)>,
    pub device_metadata: HashMap<DeviceRoute, DeviceFullMetadata>,
    pub window_aligned: Option<AlignedWindow>,

    pub input_state: TextState<'static>,
    pub cmd_history: Vec<String>,
    pub history_ptr: Option<usize>,
    pub last_rpc_result: Option<(String, Color)>,
    pub blink_state: bool,
    pub last_blink: Instant,
}

impl App {
    pub fn new(all: bool, parent_route: &DeviceRoute) -> Self {
        Self {
            all,
            parent_route: parent_route.clone(),
            mode: Mode::Normal,
            view: ViewConfig::default(),
            nav: Nav::default(),
            nav_items: Vec::new(),
            discovered_routes: HashSet::new(),
            device_status: HashMap::new(),
            last: BTreeMap::new(),
            device_metadata: HashMap::new(),
            window_aligned: None,
            input_state: TextState::default(),
            cmd_history: Vec::new(),
            history_ptr: None,
            last_rpc_result: None,
            blink_state: true,
            last_blink: Instant::now(),
        }
    }

    pub fn update(&mut self, action: Action, rpc_tx: &Sender<RpcReq>) -> bool {
        match action {
            Action::Quit => return true,
            Action::SetMode(Mode::Command) => {
                self.mode = Mode::Command;
                self.input_state = TextState::default();
                self.input_state.focus();
                self.history_ptr = None;
            }
            Action::SetMode(Mode::Normal) => {
                self.mode = Mode::Normal;
                self.input_state.blur();
            }
            Action::SubmitCommand => self.submit_command(rpc_tx),
            Action::HistoryNavigate(dir) => self.navigate_history(dir),
            Action::NavUp => {
                self.view.follow_selection = true;
                self.nav.step_linear(&self.nav_items, true);
            }
            Action::NavDown => {
                self.view.follow_selection = true;
                self.nav.step_linear(&self.nav_items, false);
            }
            Action::NavLeft => {
                self.view.follow_selection = true;
                self.nav.step_within_stream(&self.nav_items, true);
            }
            Action::NavRight => {
                self.view.follow_selection = true;
                self.nav.step_within_stream(&self.nav_items, false);
            }
            Action::NavTabNext => {
                self.view.follow_selection = true;
                self.nav.step_device(&self.nav_items, false);
            }
            Action::NavTabPrev => {
                self.view.follow_selection = true;
                self.nav.step_device(&self.nav_items, true);
            }
            Action::NavScroll(delta) => {
                self.view.follow_selection = false;
                self.view.scroll = if delta < 0 {
                    self.view.scroll.saturating_sub(delta.abs() as u16)
                } else {
                    self.view.scroll.saturating_add(delta as u16)
                };
            }
            Action::NavHome => {
                self.view.follow_selection = true;
                self.nav.home(&self.nav_items);
            }
            Action::NavEnd => {
                self.view.follow_selection = true;
                self.nav.end(&self.nav_items);
            }
            Action::TogglePlot => {
                if self.current_selection().is_some() {
                    self.view.show_plot = !self.view.show_plot;
                }
            }
            Action::ClosePlot => {
                self.view.show_plot = false;
            }
            Action::ToggleFft => {
                if self.view.show_plot {
                    self.view.show_fft = !self.view.show_fft;
                }
            }
            Action::ToggleFooter => self.view.show_footer = !self.view.show_footer,
            Action::ToggleRoutes => self.view.show_routes = !self.view.show_routes,
            Action::AdjustWindow(d) => {
                self.view.plot_window_seconds = (self.view.plot_window_seconds + d).clamp(0.5, 10.0)
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

    fn submit_command(&mut self, rpc_tx: &Sender<RpcReq>) {
        let line = self.input_state.value().to_string();
        if line.trim().is_empty() {
            return;
        }
        if self.cmd_history.last() != Some(&line) {
            self.cmd_history.push(line.clone());
        }
        self.history_ptr = None;

        let mut parts = line.split_whitespace();
        if let Some(method) = parts.next() {
            let remainder: Vec<&str> = parts.collect();
            let arg = if remainder.is_empty() {
                None
            } else {
                Some(remainder.join(" "))
            };
            let route = self.current_route();
            let _ = rpc_tx.send(RpcReq {
                route: route.clone(),
                method: method.to_string(),
                arg,
            });
            self.last_rpc_result = Some((format!("Sent to {}...", route), Color::Yellow));
            self.input_state = TextState::default();
            self.input_state.focus();
        }
    }

    fn navigate_history(&mut self, dir: i8) {
        if self.cmd_history.is_empty() {
            return;
        }
        let new_ptr = match (self.history_ptr, dir) {
            (None, -1) => Some(self.cmd_history.len() - 1),
            (Some(i), -1) => Some(i.saturating_sub(1)),
            (Some(i), 1) => {
                if i + 1 >= self.cmd_history.len() {
                    None
                } else {
                    Some(i + 1)
                }
            }
            _ => self.history_ptr,
        };
        self.history_ptr = new_ptr;
        if let Some(i) = new_ptr {
            self.input_state = TextState::new().with_value(self.cmd_history[i].clone());
            self.input_state.focus();
            self.input_state.move_end();
        } else {
            self.input_state = TextState::default();
            self.input_state.focus();
        }
    }

    pub fn visible_routes(&self) -> Vec<DeviceRoute> {
        if self.all {
            let mut routes: Vec<_> = self.discovered_routes.iter().cloned().collect();
            routes.sort();
            routes
        } else {
            vec![self.parent_route.clone()]
        }
    }

    pub fn rebuild_nav_items(&mut self) {
        let routes = self.visible_routes();
        let mut new_items = Vec::new();

        for (dev_idx, route) in routes.iter().enumerate() {
            let mut stream_ids: Vec<_> = self
                .last
                .keys()
                .filter(|k| &k.route == route)
                .map(|k| k.stream_id)
                .collect();
            stream_ids.sort();
            stream_ids.dedup();

            if stream_ids.is_empty() {
                // Device with no streams is still a stop
                new_items.push(NavPos::EmptyDevice {
                    device_idx: dev_idx,
                    route: route.clone(),
                });
            } else {
                for (stream_idx, sid) in stream_ids.iter().enumerate() {
                    let key = StreamKey::new(route.clone(), *sid);
                    if let Some((sample, _)) = self.last.get(&key) {
                        for (column_idx, _) in sample.columns.iter().enumerate() {
                            new_items.push(NavPos::Column {
                                device_idx: dev_idx,
                                stream_idx,
                                spec: ColumnKey {
                                    route: route.clone(),
                                    stream_id: *sid,
                                    column_id: column_idx,
                                },
                            });
                        }
                    }
                }
            }
        }

        self.nav_items = new_items;

        // Clamp index
        if self.nav_items.is_empty() {
            self.nav.idx = 0;
        } else {
            self.nav.idx = self.nav.idx.min(self.nav_items.len() - 1);
        }
    }

    pub fn current_pos(&self) -> Option<&NavPos> {
        self.nav_items.get(self.nav.idx)
    }

    pub fn current_selection(&self) -> Option<ColumnKey> {
        self.current_pos().and_then(|p| p.spec().cloned())
    }

    pub fn current_route(&self) -> DeviceRoute {
        self.current_pos()
            .map(|p| p.route().clone())
            .unwrap_or_else(|| self.parent_route.clone())
    }

    pub fn current_device_index(&self) -> usize {
        self.current_pos().map(|p| p.device_idx()).unwrap_or(0)
    }

    pub fn device_count(&self) -> usize {
        self.visible_routes().len()
    }

    pub fn handle_event(&mut self, event: TreeEvent) {
        match event {
            TreeEvent::RouteDiscovered(route) => {
                self.discovered_routes.insert(route.clone());
                self.device_status.entry(route).or_default();
            }
            TreeEvent::Device {
                route,
                event: DeviceEvent::Heartbeat { .. },
            } => {
                self.device_status.entry(route).or_default().on_heartbeat();
            }
            TreeEvent::Device {
                route,
                event: DeviceEvent::Status(status),
            } => {
                let dev_status = self.device_status.entry(route).or_default();
                match status {
                    ProxyStatus::SensorDisconnected => dev_status.connected = false,
                    ProxyStatus::SensorReconnected => dev_status.connected = true,
                    _ => {}
                }
            }
            TreeEvent::Device {
                route,
                event: DeviceEvent::MetadataReady(metadata),
            } => {
                self.device_metadata.insert(route, metadata);
            }
            TreeEvent::Device {
                event: DeviceEvent::RpcInvalidated(_),
                ..
            } => {}
        }
    }

    pub fn handle_sample(&mut self, sample: Sample, route: DeviceRoute, buffer: &mut Buffer) {
        let stream_key = StreamKey::new(route.clone(), sample.stream.stream_id);
        buffer.process_sample(sample.clone(), stream_key.clone());
        self.last.insert(stream_key, (sample, Instant::now()));
    }

    pub fn update_plot_window(&mut self, buffer: &Buffer) {
        if !self.view.show_plot {
            self.window_aligned = None;
            return;
        }

        self.window_aligned = self.current_selection().and_then(|col| {
            let stream_key = col.stream_key();
            let run = buffer.get_run(&stream_key)?;
            let n_samples = (self.view.plot_window_seconds * run.effective_rate)
                .ceil()
                .max(10.0) as usize;
            buffer.read_aligned_window(&[col], n_samples).ok()
        });
    }

    pub fn get_plot_data(&self) -> Option<(Vec<(f64, f64)>, f64, f64)> {
        let spec = self.current_selection()?;
        let win = self.window_aligned.as_ref()?;
        let batch = win.columns.get(&spec)?;
        if win.timestamps.is_empty() {
            return None;
        }
        let data: Vec<(f64, f64)> = match batch {
            ColumnBatch::F64(v) => win
                .timestamps
                .iter()
                .copied()
                .zip(v.iter().copied())
                .collect(),
            ColumnBatch::I64(v) => win
                .timestamps
                .iter()
                .copied()
                .zip(v.iter().map(|&x| x as f64))
                .collect(),
            ColumnBatch::U64(v) => win
                .timestamps
                .iter()
                .copied()
                .zip(v.iter().map(|&x| x as f64))
                .collect(),
        };
        if data.is_empty() {
            return None;
        }
        let (cur_t, cur_v) = *data.last().unwrap();
        Some((data, cur_v, cur_t))
    }

    pub fn get_spectral_density_data(&self) -> Option<(Vec<(f64, f64)>, f64)> {
        let spec = self.current_selection()?;
        let win = self.window_aligned.as_ref()?;
        let stream_key = spec.stream_key();
        let md = win.segment_metadata.get(&stream_key)?;
        let sampling_hz = (md.sampling_rate / md.decimation) as f64;
        let batch = win.columns.get(&spec)?;

        let signal: Vec<f64> = match batch {
            ColumnBatch::F64(v) => v.clone(),
            ColumnBatch::I64(v) => v.iter().map(|&x| x as f64).collect(),
            ColumnBatch::U64(v) => v.iter().map(|&x| x as f64).collect(),
        };

        if signal.len() < 128 {
            return None;
        }

        let mean_val = signal.iter().sum::<f64>() / signal.len() as f64;
        let detrended: Vec<f64> = signal.iter().map(|x| x - mean_val).collect();

        let welch: SpectralDensity<f64> = SpectralDensity::builder(&detrended, sampling_hz).build();
        let sd = welch.periodogram();
        let raw: Vec<f64> = sd.iter().copied().collect();

        let valid_vals: Vec<f64> = raw
            .iter()
            .copied()
            .filter(|v| v.is_finite() && *v > 0.0)
            .collect();

        if valid_vals.is_empty() {
            return None;
        }

        let split_idx = valid_vals.len() / 2;
        let mut high_freq_vals = valid_vals[split_idx..].to_vec();
        high_freq_vals.sort_by(|a, b| a.total_cmp(b));

        let noise_floor = if high_freq_vals.is_empty() {
            let mut all = valid_vals.clone();
            all.sort_by(|a, b| a.total_cmp(b));
            all[all.len() / 2]
        } else {
            high_freq_vals[high_freq_vals.len() / 2]
        };

        let pts: Vec<(f64, f64)> = sd
            .frequency()
            .into_iter()
            .zip(raw.into_iter())
            .filter(|(_, d)| d.is_finite() && *d > 0.0)
            .collect();

        Some((pts, noise_floor))
    }

    pub fn get_focused_channel_info(&self) -> Option<(String, String)> {
        let spec = self.current_selection()?;
        let win = self.window_aligned.as_ref()?;
        let meta = win.column_metadata.get(&spec)?;
        Some((meta.description.clone(), meta.units.clone()))
    }

    pub fn tick_blink(&mut self) {
        if self.last_blink.elapsed() >= Duration::from_millis(500) {
            self.blink_state = !self.blink_state;
            self.last_blink = Instant::now();
        }
    }
}

fn get_action(ev: Event, app: &mut App) -> Option<Action> {
    if let Event::Key(k) = ev {
        if k.kind != KeyEventKind::Press {
            return None;
        }
        match app.mode {
            Mode::Command => match k.code {
                KeyCode::Esc => Some(Action::SetMode(Mode::Normal)),
                KeyCode::Up => Some(Action::HistoryNavigate(-1)),
                KeyCode::Down => Some(Action::HistoryNavigate(1)),
                KeyCode::Enter => Some(Action::SubmitCommand),
                _ => {
                    app.input_state.handle_key_event(k);
                    None
                }
            },
            Mode::Normal => match k.code {
                KeyCode::Char(':') => Some(Action::SetMode(Mode::Command)),
                KeyCode::Char('q') => Some(Action::Quit),
                KeyCode::Char('c') if k.modifiers == KeyModifiers::CONTROL => Some(Action::Quit),
                KeyCode::Esc => {
                    if app.view.show_plot {
                        Some(Action::ClosePlot)
                    } else {
                        Some(Action::Quit)
                    }
                }
                KeyCode::Up => Some(Action::NavUp),
                KeyCode::Down => Some(Action::NavDown),
                KeyCode::Left => Some(Action::NavLeft),
                KeyCode::Right => Some(Action::NavRight),
                KeyCode::BackTab => Some(Action::NavTabPrev),
                KeyCode::Tab => Some(Action::NavTabNext),
                KeyCode::PageUp => Some(Action::NavScroll(-10)),
                KeyCode::PageDown => Some(Action::NavScroll(10)),
                KeyCode::Home => Some(Action::NavHome),
                KeyCode::End => Some(Action::NavEnd),
                KeyCode::Enter => Some(Action::TogglePlot),
                KeyCode::Char('f') => Some(Action::ToggleFft),
                KeyCode::Char('h') => Some(Action::ToggleFooter),
                KeyCode::Char('r') => Some(Action::ToggleRoutes),
                KeyCode::Char('+') | KeyCode::Char('=') => Some(Action::AdjustWindow(0.5)),
                KeyCode::Char('-') | KeyCode::Char('_') => Some(Action::AdjustWindow(-0.5)),
                KeyCode::Char('[') => Some(Action::AdjustPlotWidth(5)),
                KeyCode::Char(']') => Some(Action::AdjustPlotWidth(-5)),
                KeyCode::Char(',') | KeyCode::Char('<') => Some(Action::AdjustPrecision(-1)),
                KeyCode::Char('.') | KeyCode::Char('>') => Some(Action::AdjustPrecision(1)),
                _ => None,
            },
        }
    } else {
        None
    }
}

fn draw_ui<B: Backend>(terminal: &mut Terminal<B>, app: &mut App) -> io::Result<()> {
    terminal.draw(|f| {
        let size = f.area();
        let (main_area, footer_area) = {
            let footer_h = if app.mode == Mode::Command {
                4
            } else if app.view.show_footer {
                6
            } else {
                2
            };
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(10), Constraint::Length(footer_h)])
                .split(size);
            (chunks[0], Some(chunks[1]))
        };

        let (left, right) = if app.view.show_plot {
            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Percentage(100 - app.view.plot_width_percent),
                    Constraint::Percentage(app.view.plot_width_percent),
                ])
                .split(main_area);
            (chunks[0], Some(chunks[1]))
        } else {
            (main_area, None)
        };

        render_monitor_panel(f, app, left, Instant::now());
        if let Some(r) = right {
            render_graphics_panel(f, app, r);
        }
        if let Some(foot) = footer_area {
            render_footer(f, app, foot);
        }
    })?;
    Ok(())
}

fn render_monitor_panel(f: &mut Frame, app: &mut App, area: Rect, now: Instant) {
    let inner = Rect {
        x: area.x,
        y: area.y,
        width: area.width.saturating_sub(1),
        height: area.height,
    };
    let (lines, col_map) = build_left_lines(app, now);
    let total = lines.len();
    let view_h = inner.height as usize;

    if app.view.follow_selection {
        if let Some(&line_idx) = col_map.get(&app.nav.idx) {
            if view_h > 0 && total > view_h {
                let cur = app.view.scroll as usize;
                if line_idx < cur || line_idx >= cur + view_h {
                    app.view.scroll = line_idx
                        .saturating_sub(view_h / 2)
                        .min(total.saturating_sub(view_h))
                        as u16;
                }
            } else {
                app.view.scroll = 0;
            }
        }
    }
    app.view.scroll = (app.view.scroll as usize).min(total.saturating_sub(view_h)) as u16;
    f.render_widget(Paragraph::new(lines).scroll((app.view.scroll, 0)), inner);

    if total > view_h {
        let sb_area = Rect {
            x: area.x + area.width - 1,
            y: area.y,
            width: 1,
            height: area.height,
        };
        let track_len = view_h;
        let thumb_len = (track_len * track_len / total).max(1);
        let max_thumb_pos = track_len - thumb_len;
        let scroll_max = total - track_len;
        let thumb_pos = (app.view.scroll as usize * max_thumb_pos) / scroll_max;

        for i in 0..track_len {
            let ch = if i >= thumb_pos && i < thumb_pos + thumb_len {
                "█"
            } else {
                "│"
            };
            f.render_widget(
                Paragraph::new(ch).style(Style::default().fg(Color::DarkGray)),
                Rect {
                    x: sb_area.x,
                    y: sb_area.y + i as u16,
                    width: 1,
                    height: 1,
                },
            );
        }
    }
}

fn build_left_lines(app: &mut App, now: Instant) -> (Vec<Line<'static>>, HashMap<usize, usize>) {
    let mut lines = Vec::new();
    let mut map = HashMap::new();

    let routes = app.visible_routes();

    if routes.is_empty() {
        lines.push(Line::from("Waiting for data..."));
        return (lines, map);
    }

    let mut global_idx = 0;
    app.view.desc_width = app
        .last
        .values()
        .flat_map(|(s, _)| s.columns.iter())
        .map(|c| c.desc.description.len())
        .max()
        .unwrap_or(0);
    app.view.units_width = app
        .last
        .values()
        .flat_map(|(s, _)| s.columns.iter())
        .map(|c| c.desc.units.len())
        .max()
        .unwrap_or(0);

    for (dev_idx, route) in routes.iter().enumerate() {
        let dev = app.device_metadata.get(route).map(|m| m.device.as_ref());

        let status = app.device_status.get(route);
        let is_alive = status
            .map(|s| s.is_alive(Duration::from_millis(300)))
            .unwrap_or(false);

        let head_style = if dev_idx == app.current_device_index() {
            Style::default().add_modifier(Modifier::BOLD | Modifier::UNDERLINED)
        } else {
            Style::default().add_modifier(Modifier::BOLD)
        };

        let header_text = if let Some(d) = dev {
            if d.serial_number.is_empty() {
                d.name.clone()
            } else {
                format!("{}  Serial: {}", d.name, d.serial_number)
            }
        } else {
            format!("<{}>", route)
        };

        let status_indicator = if is_alive { "●" } else { "○" };
        let status_color = if is_alive { Color::Green } else { Color::DarkGray };

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

        lines.push(Line::from(header_spans));

        let mut stream_ids: Vec<_> = app
            .last
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
            let key = StreamKey::new(route.clone(), sid);
            if let Some((sample, seen)) = app.last.get(&key) {
                let is_stale = now.saturating_duration_since(*seen) > Duration::from_millis(1200);
                for col in &sample.columns {
                    let nav_idx = global_idx;
                    global_idx += 1;
                    map.insert(nav_idx, lines.len());

                    let is_sel = app.nav.idx == nav_idx;
                    let ctx = StyleContext::new()
                        .stale(is_stale)
                        .selected(is_sel)
                        .plot_mode(app.view.show_plot);

                    let label_style = ctx.resolve();
                    let (val_str, val_f64) = fmt_value(&col.value);
                    let val_col = app
                        .view
                        .theme
                        .get_value_color(&sample.stream.name, &col.desc.name, val_f64)
                        .unwrap_or(Color::Reset);
                    let val_style = ctx.color(val_col).resolve();

                    let mut desc = col.desc.description.clone();
                    if desc.len() < app.view.desc_width {
                        desc.push_str(&" ".repeat(app.view.desc_width - desc.len()));
                    }

                    let units = col.desc.units.clone();
                    let padded_units = if app.view.units_width > 0 && !units.is_empty() {
                        format!("{:>width$}", units, width = app.view.units_width)
                    } else if app.view.units_width > 0 {
                        " ".repeat(app.view.units_width)
                    } else {
                        String::new()
                    };

                    lines.push(Line::from(vec![
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
    (lines, map)
}

fn render_footer(f: &mut Frame, app: &mut App, area: Rect) {
    if app.mode == Mode::Command {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Min(1)])
            .split(area);

        if let Some((msg, color)) = &app.last_rpc_result {
            f.render_widget(
                Paragraph::new(msg.as_str())
                    .style(Style::default().fg(*color).add_modifier(Modifier::BOLD)),
                chunks[0],
            );
        }

        let target_route = app.current_route();
        let val = app.input_state.value();
        let cursor_idx = app.input_state.position().min(val.len());

        let mut spans = vec![
            Span::styled(
                format!("[{}] ", target_route),
                Style::default().fg(Color::Blue),
            ),
            Span::raw(&val[0..cursor_idx]),
        ];

        if cursor_idx < val.len() {
            spans.push(Span::styled(
                &val[cursor_idx..cursor_idx + 1],
                if app.blink_state {
                    Style::default().bg(Color::White).fg(Color::Black)
                } else {
                    Style::default()
                },
            ));
            spans.push(Span::raw(&val[cursor_idx + 1..]));
        } else if app.blink_state {
            spans.push(Span::styled(" ", Style::default().bg(Color::White)));
        }

        let block = Block::default()
            .borders(Borders::TOP)
            .title(" Command Mode ");
        f.render_widget(Paragraph::new(Line::from(spans)).block(block), chunks[1]);
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
        Span::raw(" Columns"),
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
        key_span("f"),
        Span::raw(" FFT  "),
        key_span("h"),
        Span::raw(" Footer  "),
        key_span("r"),
        Span::raw(" Routes "),
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
        Span::raw(" Window (0.5s)  "),
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

fn render_graphics_panel(f: &mut Frame, app: &App, area: Rect) {
    if let (Some(pos), Some((desc, units))) = (app.current_pos(), app.get_focused_channel_info()) {
        let route = pos.route();
        if app.view.show_fft {
            if let Some((sd_data, noise_floor)) = app.get_spectral_density_data() {
                let title = format!(
                    "{} — {} (DC detrend {:.1}s) | High Freq Median: {:.3e} {}/√Hz",
                    route, desc, app.view.plot_window_seconds, noise_floor, units
                );
                let block = Block::default().title(title).borders(Borders::ALL);

                if !sd_data.is_empty() {
                    let log_data: Vec<(f64, f64)> = sd_data
                        .iter()
                        .filter_map(|(freq, val)| {
                            if *freq <= 0.0 || *val <= 0.0 {
                                return None;
                            }
                            let norm = val / noise_floor;
                            if !norm.is_finite() || norm <= 0.0 {
                                return None;
                            }
                            Some((freq.log10(), norm.log10()))
                        })
                        .collect();

                    let min_f = log_data.first().map(|(f, _)| *f).unwrap_or(0.0);
                    let max_f = log_data.last().map(|(f, _)| *f).unwrap_or(1.0);
                    let ds: Vec<f64> = log_data.iter().map(|(_, d)| *d).collect();
                    let min_d = ds.iter().fold(f64::INFINITY, |a, &b| a.min(b));
                    let max_d = ds.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));

                    let y_pad = if (max_d - min_d) > 0.1 {
                        (max_d - min_d) * 0.1
                    } else {
                        0.5
                    };

                    let dataset = Dataset::default()
                        .name(desc.as_str())
                        .marker(symbols::Marker::Braille)
                        .style(Style::default().fg(Color::Cyan))
                        .graph_type(GraphType::Line)
                        .data(&log_data);

                    let chart = Chart::new(vec![dataset])
                        .block(block)
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
                } else {
                    f.render_widget(Paragraph::new("No valid FFT data").block(block), area);
                }
            } else {
                let block = Block::default()
                    .title("Buffering FFT...")
                    .borders(Borders::ALL);
                f.render_widget(Paragraph::new("Need >128 samples").block(block), area);
            }
        } else {
            let title = format!(
                "{} — {} ({:.1}s)",
                route, desc, app.view.plot_window_seconds
            );
            let block = Block::default().title(title).borders(Borders::ALL);

            if let Some((data, _, _)) = app.get_plot_data() {
                let min_t = data.first().map(|(t, _)| *t).unwrap_or(0.0);
                let max_t = data.last().map(|(t, _)| *t).unwrap_or(1.0);
                let vs: Vec<f64> = data.iter().map(|(_, v)| *v).collect();
                let min_v = vs.iter().fold(f64::INFINITY, |a, &b| a.min(b));
                let max_v = vs.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));

                let pad = if (max_v - min_v).abs() > 1e-10 {
                    (max_v - min_v) * 0.4
                } else {
                    1.0
                };

                let dataset = Dataset::default()
                    .name(desc.as_str())
                    .marker(symbols::Marker::Braille)
                    .style(Style::default().fg(Color::Green))
                    .graph_type(GraphType::Line)
                    .data(&data);

                let chart = Chart::new(vec![dataset])
                    .block(block)
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
            } else {
                f.render_widget(Paragraph::new("Buffering...").block(block), area);
            }
        }
    } else {
        f.render_widget(
            Block::default()
                .title("Channel Detail")
                .borders(Borders::ALL),
            area,
        );
    }
}

fn generate_linear_labels(
    min: f64,
    max: f64,
    count: usize,
    precision: usize,
) -> Vec<Span<'static>> {
    if count < 2 {
        return vec![];
    }
    let step = (max - min) / ((count - 1) as f64);
    (0..count)
        .map(|i| {
            let v = min + (i as f64 * step);
            Span::from(format!("{:>10.p$}", v, p = precision))
        })
        .collect()
}

fn generate_log_labels(
    min_log: f64,
    max_log: f64,
    count: usize,
    precision: usize,
) -> Vec<Span<'static>> {
    if count < 2 {
        return vec![];
    }
    let step = (max_log - min_log) / ((count - 1) as f64);
    let max_val = 10f64.powf(max_log.max(min_log)).abs();
    let use_scientific = max_val < 0.01 || max_val >= 1000.0;

    (0..count)
        .map(|i| {
            let log_val = min_log + (i as f64 * step);
            let real_val = 10f64.powf(log_val);

            let s = if use_scientific {
                format!("{:.p$e}", real_val, p = precision)
            } else {
                format!("{:.p$}", real_val, p = precision)
            };
            Span::from(format!("{:>10}", s))
        })
        .collect()
}

fn fmt_value(v: &ColumnData) -> (String, f64) {
    match v {
        ColumnData::Float(x) => (format!("{:15.4}", x), *x as f64),
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
fn main() {
    let cli = Cli::parse();
    let proxy = tio::proxy::Interface::new(&cli.tio.root);
    let parent_route: DeviceRoute = cli.tio.parse_route();

    // Data thread
    let (data_tx, data_rx) = channel::unbounded::<TreeItem>();
    let tree_for_data =
        DeviceTree::open(&proxy, parent_route.clone()).expect("Failed to open device tree");
    std::thread::spawn(move || {
        let mut tree = tree_for_data;
        loop {
            match tree.next_item() {
                Ok(item) => {
                    if data_tx.send(item).is_err() {
                        return;
                    }
                }
                Err(_) => return,
            }
        }
    });

    // RPC thread
    let (rpc_tx, rpc_rx) = channel::bounded::<RpcReq>(1);
    let (rpc_resp_tx, rpc_resp_rx) = channel::bounded::<RpcResp>(1);
    let rpc_client = RpcClient::open(&proxy, parent_route.clone())
        .expect("Failed to open RPC client");
    std::thread::spawn(move || {
        while let Ok(req) = rpc_rx.recv() {
            let result = exec_rpc(&rpc_client, &req);
            if rpc_resp_tx.send(RpcResp { result }).is_err() {
                return;
            }
        }
    });

    // Key thread
    let (key_tx, key_rx) = channel::unbounded();
    std::thread::spawn(move || loop {
        if let Ok(ev) = event::read() {
            if key_tx.send(ev).is_err() {
                break;
            }
        }
    });

    // App state
    let mut app = App::new(cli.all, &parent_route);
    if let Some(path) = &cli.colors {
        if let Ok(theme) = load_theme(path) {
            app.view.theme = theme;
        } else {
            eprintln!("Failed to load theme");
        }
    }

    let mut buffer = Buffer::new(100_000);

    // UI
    let mut term = ratatui::init();
    let _ = term.hide_cursor();
    let ui_tick = channel::tick(Duration::from_millis(1000 / cli.fps as u64));

    'main: loop {
        crossbeam::select! {
            recv(data_rx) -> item => {
                match item {
                    Ok(TreeItem::Sample(sample, route)) => {
                        app.handle_sample(sample, route, &mut buffer);
                    }
                    Ok(TreeItem::Event(event)) => {
                        app.handle_event(event);
                    }
                    Err(_) => break 'main,
                }
            }

            recv(key_rx) -> ev => {
                if let Ok(ev) = ev {
                    if let Some(act) = get_action(ev, &mut app) {
                        if app.update(act, &rpc_tx) {
                            break 'main;
                        }
                    }
                }
            }

            recv(rpc_resp_rx) -> res => {
                if let Ok(res) = res {
                    let (msg, col) = match res.result {
                        Ok(s) => (format!("OK: {}", s), Color::Green),
                        Err(s) => (format!("ERR: {}", s), Color::Red),
                    };
                    app.last_rpc_result = Some((msg, col));
                }
            }

            recv(ui_tick) -> _ => {
                app.update_plot_window(&buffer);
                app.rebuild_nav_items();
                app.tick_blink();

                if draw_ui(&mut term, &mut app).is_err() {
                    break 'main;
                }
            }
        }
    }

    ratatui::restore();
}