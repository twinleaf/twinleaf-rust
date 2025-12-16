// tio-monitor
// Full implementation with Action/Mode architecture, Styling System, and Macro-based RPC
// Build: cargo run --release -- <tio-url> [options]

use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::{self, Read},
    str::FromStr,
    time::{Duration, Instant},
};

use clap::Parser;
use crossbeam::channel::{self, Receiver, Sender};
use ratatui::{
    crossterm::{
        self,
        event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    },
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
    data::{AlignedWindow, Buffer, BufferEvent, ColumnBatch, ColumnData, OverflowPolicy, Sample},
    device::DeviceTree,
    tio::{
        self,
        proto::{
            identifiers::{ColumnKey, StreamId, StreamKey},
            DeviceRoute,
        },
    },
};
use twinleaf_tools::TioOpts;
use welch_sde::{Build, SpectralDensity};

/// ---------- CLI ----------
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

/// ---------- Navigation ----------
#[derive(Debug, Clone)]
pub struct NavItem {
    pub device_idx: usize,
    pub stream_idx: usize,
    pub column_idx: usize,
    pub route: DeviceRoute,
    pub stream_id: StreamId,
    pub column_id: usize,
    pub spec: ColumnKey,
}

#[derive(Debug, Clone, Copy)]
pub struct NavScope {
    pub device: bool,
    pub stream: bool,
    pub column: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Nav {
    pub idx: Option<usize>,
}

impl Nav {
    fn step_device_axis(&mut self, items: &[NavItem], cur_item: &NavItem, backward: bool) {
        let mut all_device_indices: Vec<usize> = items.iter().map(|it| it.device_idx).collect();
        all_device_indices.sort();
        all_device_indices.dedup();

        let axis_len = all_device_indices.len();
        if axis_len == 0 {
            return;
        }

        let axis_pos = all_device_indices
            .iter()
            .position(|&d| d == cur_item.device_idx)
            .unwrap_or(0);
        let new_axis_pos = if backward {
            (axis_pos + axis_len - 1) % axis_len
        } else {
            (axis_pos + 1) % axis_len
        };
        let new_target_device_idx = all_device_indices[new_axis_pos];

        let (best_match_idx, _) = items
            .iter()
            .enumerate()
            .filter(|(_, it)| it.device_idx == new_target_device_idx)
            .map(|(idx, it)| {
                let stream_dist = (it.stream_idx as isize - cur_item.stream_idx as isize).abs();
                let col_dist = (it.column_idx as isize - cur_item.column_idx as isize).abs();
                (idx, (stream_dist, col_dist))
            })
            .min_by_key(|&(_, dist)| dist)
            .unwrap();

        self.idx = Some(best_match_idx);
    }

    fn step_contiguous(
        &mut self,
        items: &[NavItem],
        cur_item: &NavItem,
        cur_idx: usize,
        scope: NavScope,
        backward: bool,
    ) {
        let candidates: Vec<usize> = items
            .iter()
            .enumerate()
            .filter(|(_, it)| {
                (scope.device || it.device_idx == cur_item.device_idx)
                    && (scope.stream || it.stream_idx == cur_item.stream_idx)
                    && (scope.column || it.column_idx == cur_item.column_idx)
            })
            .map(|(i, _)| i)
            .collect();

        if candidates.is_empty() {
            self.idx = Some(cur_idx);
            return;
        }

        let pos_in_sub = candidates.iter().position(|&i| i == cur_idx).unwrap_or(0);
        let len = candidates.len();
        let new_pos_in_sub = if backward {
            if pos_in_sub == 0 {
                len - 1
            } else {
                pos_in_sub - 1
            }
        } else {
            (pos_in_sub + 1) % len
        };
        self.idx = Some(candidates[new_pos_in_sub]);
    }

    pub fn step(&mut self, items: &[NavItem], scope: NavScope, backward: bool) {
        if items.is_empty() {
            self.idx = None;
            return;
        }
        let cur_idx = self.idx.unwrap_or(0).min(items.len() - 1);
        let cur_item = &items[cur_idx];

        if scope.device && !scope.stream && !scope.column {
            self.step_device_axis(items, cur_item, backward);
        } else {
            self.step_contiguous(items, cur_item, cur_idx, scope, backward);
        }
    }

    pub fn home(&mut self, items: &[NavItem]) {
        if !items.is_empty() {
            self.idx = Some(0);
        }
    }

    pub fn end(&mut self, items: &[NavItem]) {
        if !items.is_empty() {
            self.idx = Some(items.len() - 1);
        }
    }
}

/// ---------- Styling System ----------
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

/// ---------- Core Application State ----------
#[derive(Debug, Clone, PartialEq)]
pub enum Mode {
    Normal,
    Command,
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

#[derive(Debug, Clone)]
pub enum Action {
    Quit,
    SetMode(Mode),
    SubmitCommand,
    NavMove(NavScope, bool),
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

pub struct App {
    pub all: bool,
    pub parent_route: DeviceRoute,

    pub mode: Mode,
    pub view: ViewConfig,

    pub nav: Nav,
    pub nav_items: Vec<NavItem>,

    pub last: BTreeMap<StreamKey, (Sample, Instant)>,
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
            last: BTreeMap::new(),
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
            Action::NavMove(scope, back) => {
                self.view.follow_selection = true;
                self.nav.step(&self.nav_items, scope, back);
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
            let route = self
                .current_item()
                .map(|i| i.route.clone())
                .unwrap_or(self.parent_route.clone());
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
            let mut routes: Vec<_> = self.last.keys().map(|k| k.route.clone()).collect();
            routes.sort();
            routes.dedup();
            routes
        } else {
            vec![self.parent_route.clone()]
        }
    }

    pub fn rebuild_nav_items(&mut self) {
        self.nav_items.clear();
        let routes = self.visible_routes();
        if routes.is_empty() || self.last.is_empty() {
            self.nav.idx = None;
            return;
        }

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
            for (stream_idx, sid) in stream_ids.iter().enumerate() {
                let key = StreamKey::new(route.clone(), *sid);
                if let Some((sample, _)) = self.last.get(&key) {
                    for (column_idx, _) in sample.columns.iter().enumerate() {
                        new_items.push(NavItem {
                            device_idx: dev_idx,
                            stream_idx,
                            column_idx,
                            route: route.clone(),
                            stream_id: *sid,
                            column_id: column_idx,
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
        self.nav_items = new_items;
        if let Some(idx) = self.nav.idx {
            if idx >= self.nav_items.len() {
                self.nav.idx = if self.nav_items.is_empty() {
                    None
                } else {
                    Some(self.nav_items.len() - 1)
                };
            }
        } else if !self.nav_items.is_empty() {
            self.nav.idx = Some(0);
        }
    }

    pub fn current_item(&self) -> Option<&NavItem> {
        self.nav.idx.and_then(|i| self.nav_items.get(i))
    }
    pub fn current_selection(&self) -> Option<ColumnKey> {
        self.current_item().map(|it| it.spec.clone())
    }
    pub fn current_device_index(&self) -> usize {
        self.current_item().map(|it| it.device_idx).unwrap_or(0)
    }
    pub fn device_count(&self) -> usize {
        self.visible_routes().len()
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
                KeyCode::Up => Some(Action::NavMove(
                    NavScope {
                        device: true,
                        stream: true,
                        column: true,
                    },
                    true,
                )),
                KeyCode::Down => Some(Action::NavMove(
                    NavScope {
                        device: true,
                        stream: true,
                        column: true,
                    },
                    false,
                )),
                KeyCode::Left => Some(Action::NavMove(
                    NavScope {
                        device: false,
                        stream: false,
                        column: true,
                    },
                    true,
                )),
                KeyCode::Right => Some(Action::NavMove(
                    NavScope {
                        device: false,
                        stream: false,
                        column: true,
                    },
                    false,
                )),
                KeyCode::BackTab => Some(Action::NavMove(
                    NavScope {
                        device: true,
                        stream: false,
                        column: false,
                    },
                    true,
                )),
                KeyCode::Tab => Some(Action::NavMove(
                    NavScope {
                        device: true,
                        stream: false,
                        column: false,
                    },
                    false,
                )),
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

/// ---------- Communication Threads ----------
#[derive(Debug, Clone)]
struct DataReq {
    all: bool,
    parent_route: DeviceRoute,
    selection: Option<ColumnKey>,
    seconds: f64,
}
#[derive(Debug, Clone)]
struct DataResp {
    last: Vec<(DeviceRoute, StreamId, Sample)>,
    window: Option<AlignedWindow>,
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

fn exec_rpc(
    tree: &mut DeviceTree,
    route: DeviceRoute,
    method: String,
    arg_str: Option<String>,
) -> Result<String, String> {
    // Macro to parse input string -> Vec<u8> (Little Endian)
    macro_rules! parse_arg {
        ($type_str:expr, $input:expr, { $($str_val:literal => $ty:ty),+ }) => {
            match $type_str.as_str() {
                $( $str_val => $input.parse::<$ty>()
                    .map_err(|_| format!("Invalid {} argument", $str_val))?
                    .to_le_bytes().to_vec(), )+
                "string" | "" => $input.as_bytes().to_vec(),
                t => return Err(format!("Unsupported type: {}", t)),
            }
        }
    }

    // Macro to format reply Vec<u8> -> String
    macro_rules! fmt_reply {
        ($type_str:expr, $bytes:expr, { $($str_val:literal => $ty:ty),+ }) => {
            match $type_str.as_str() {
                $( $str_val => {
                    let sz = std::mem::size_of::<$ty>();
                    if $bytes.len() < sz { "Error: Short Read".to_string() }
                    else {
                        let arr = $bytes[0..sz].try_into().unwrap_or_default();
                        <$ty>::from_le_bytes(arr).to_string()
                    }
                }, )+
                "string" | "" => String::from_utf8_lossy($bytes).to_string(),
                _ => format!("Hex: {:02X?}", $bytes),
            }
        }
    }

    let meta: u16 = tree
        .rpc(route.clone(), "rpc.info", &method)
        .map_err(|_| "Unknown RPC".to_string())?;

    let spec = twinleaf::device::util::parse_rpc_spec(meta, method.clone());
    let type_str = spec.type_str();

    let payload = if let Some(s) = arg_str {
        parse_arg!(type_str, s, {
            "u8" => u8, "u16" => u16, "u32" => u32, "u64" => u64,
            "i8" => i8, "i16" => i16, "i32" => i32, "i64" => i64,
            "f32" => f32, "f64" => f64
        })
    } else {
        Vec::new()
    };

    let reply_bytes = tree
        .raw_rpc(route, &method, &payload)
        .map_err(|e| format!("{:?}", e))?;

    if reply_bytes.is_empty() {
        return Ok("OK".to_string());
    }

    let reply_str = fmt_reply!(type_str, &reply_bytes, {
        "u8" => u8, "u16" => u16, "u32" => u32, "u64" => u64,
        "i8" => i8, "i16" => i16, "i32" => i32, "i64" => i64,
        "f32" => f32, "f64" => f64
    });

    Ok(reply_str)
}

fn run_data_thread(
    mut tree: DeviceTree,
    req_rx: Receiver<DataReq>,
    resp_tx: Sender<DataResp>,
    rpc_rx: Receiver<RpcReq>,
    rpc_resp_tx: Sender<RpcResp>,
    capacity: usize,
) {
    let (evt_tx, _evt_rx) = channel::unbounded::<BufferEvent>();
    let mut buffer = Buffer::new(evt_tx, capacity, false, OverflowPolicy::DropOldest);
    let mut last: BTreeMap<StreamKey, Sample> = BTreeMap::new();

    loop {
        while let Ok(req) = rpc_rx.try_recv() {
            let res = exec_rpc(&mut tree, req.route, req.method, req.arg);
            rpc_resp_tx.send(RpcResp { result: res }).ok();
        }
        let (sample, route) = match tree.next() {
            Ok(x) => x,
            Err(_) => break,
        };
        let stream_id = sample.stream.stream_id;
        buffer.process_sample(sample.clone(), route.clone());
        last.insert(StreamKey::new(route.clone(), stream_id), sample);

        while let Ok(q) = req_rx.try_recv() {
            let last_vec: Vec<_> = if q.all {
                last.iter()
                    .map(|(k, s)| (k.route.clone(), k.stream_id, s.clone()))
                    .collect()
            } else {
                last.iter()
                    .filter(|(k, _)| k.route == q.parent_route)
                    .map(|(k, s)| (k.route.clone(), k.stream_id, s.clone()))
                    .collect()
            };

            let window = q.selection.and_then(|spec| {
                let stream_key = spec.stream_key();
                let active = buffer.active_runs.get(&stream_key)?;
                let sampling_hz = active.effective_rate;
                let n_samples = (q.seconds * sampling_hz).ceil().max(10.0) as usize;
                buffer.read_aligned_window(&[spec], n_samples).ok()
            });
            if resp_tx
                .send(DataResp {
                    last: last_vec,
                    window,
                })
                .is_err()
            {
                return;
            }
        }
    }
}

/// ---------- Rendering ----------
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
        if let Some(idx) = app.nav.idx {
            if let Some(&line_idx) = col_map.get(&idx) {
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
    }
    app.view.scroll = (app.view.scroll as usize).min(total.saturating_sub(view_h)) as u16;
    f.render_widget(Paragraph::new(lines).scroll((app.view.scroll, 0)), inner);

    // Scrollbar
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
    if routes.is_empty() || app.last.is_empty() {
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
        let dev = app
            .last
            .iter()
            .find(|(k, _)| &k.route == route)
            .map(|(_, (s, _))| s.device.as_ref());
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
            "<dev>".to_string()
        };

        let mut header_spans = vec![Span::styled(header_text, head_style)];
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

        for sid in stream_ids {
            let key = StreamKey::new(route.clone(), sid);
            if let Some((sample, seen)) = app.last.get(&key) {
                let is_stale = now.saturating_duration_since(*seen) > Duration::from_millis(1200);
                for col in &sample.columns {
                    let nav_idx = global_idx;
                    global_idx += 1;
                    map.insert(nav_idx, lines.len());

                    let is_sel = app.nav.idx == Some(nav_idx);
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
    // Command Mode
    if app.mode == Mode::Command {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Min(1)])
            .split(area);

        // RPC Result
        if let Some((msg, color)) = &app.last_rpc_result {
            f.render_widget(
                Paragraph::new(msg.as_str())
                    .style(Style::default().fg(*color).add_modifier(Modifier::BOLD)),
                chunks[0],
            );
        }

        // Input Line
        let target_route = app
            .current_item()
            .map(|i| i.route.clone())
            .unwrap_or(app.parent_route.clone());
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
            "  Navigation ",
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
            "  Toggle     ",
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
            "  Plot     ",
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
            "  Scroll     ",
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
            "  Quit       ",
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
    if let (Some(item), Some((desc, units))) = (app.current_item(), app.get_focused_channel_info())
    {
        if app.view.show_fft {
            if let Some((sd_data, noise_floor)) = app.get_spectral_density_data() {
                let title = format!(
                    "{} — {} (DC detrend {:.1}s) | High Freq Median: {:.3e} {}/√Hz",
                    item.route, desc, app.view.plot_window_seconds, noise_floor, units
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
                item.route, desc, app.view.plot_window_seconds
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
    let tree = DeviceTree::open(&proxy, parent_route.clone()).expect("Failed to open device tree");

    let (req_tx, req_rx) = channel::bounded::<DataReq>(1);
    let (resp_tx, resp_rx) = channel::bounded::<DataResp>(1);
    let (rpc_tx, rpc_rx) = channel::bounded::<RpcReq>(1);
    let (rpc_resp_tx, rpc_resp_rx) = channel::bounded::<RpcResp>(1);

    std::thread::spawn(move || run_data_thread(tree, req_rx, resp_tx, rpc_rx, rpc_resp_tx, 100000));

    let mut app = App::new(cli.all, &parent_route);
    if let Some(path) = &cli.colors {
        if let Ok(theme) = load_theme(path) {
            app.view.theme = theme;
        } else {
            eprintln!("Failed to load theme");
        }
    }

    let mut term = ratatui::init();
    let frame_dur = Duration::from_millis(1000 / cli.fps as u64);
    let _ = term.hide_cursor();

    'main: loop {
        let start = Instant::now();
        while crossterm::event::poll(Duration::ZERO).unwrap_or(false) {
            if let Ok(ev) = event::read() {
                if let Some(act) = get_action(ev, &mut app) {
                    if app.update(act, &rpc_tx) {
                        break 'main;
                    }
                }
            }
        }

        let req = DataReq {
            all: app.all,
            parent_route: app.parent_route.clone(),
            selection: app.current_selection(),
            seconds: app.view.plot_window_seconds,
        };
        match req_tx.try_send(req) {
            Ok(_) => {}
            Err(crossbeam::channel::TrySendError::Full(_)) => {}
            Err(crossbeam::channel::TrySendError::Disconnected(_)) => {
                break 'main;
            }
        }

        while let Ok(r) = resp_rx.try_recv() {
            let now = Instant::now();
            for (route, sid, sample) in r.last {
                let key = StreamKey::new(route, sid);
                let seen = if let Some((prev, prev_seen)) = app.last.get(&key) {
                    if prev.n == sample.n {
                        *prev_seen
                    } else {
                        now
                    }
                } else {
                    now
                };
                app.last.insert(key, (sample, seen));
            }
            app.window_aligned = r.window;
            app.rebuild_nav_items();
        }
        while let Ok(res) = rpc_resp_rx.try_recv() {
            let (msg, col) = match res.result {
                Ok(s) => (format!("OK: {}", s), Color::Green),
                Err(s) => (format!("ERR: {}", s), Color::Red),
            };
            app.last_rpc_result = Some((msg, col));
        }

        app.tick_blink();
        if draw_ui(&mut term, &mut app).is_err() {
            break 'main;
        }

        let elapsed = start.elapsed();
        if elapsed < frame_dur {
            std::thread::sleep(frame_dur - elapsed);
        }
    }
    ratatui::restore();
}
