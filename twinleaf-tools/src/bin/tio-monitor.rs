// tio-monitor
//
// Live monitoring with basic scope and periodogram + RPC Command Interface
//
// Build: cargo run --release -- <tio-url> [route] [options]
// Quit:  q / Esc / Ctrl-C
// Cmd:   : (to enter command mode)

use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::{self, Read},
    str::FromStr,
    time::{Duration, Instant},
};

use clap::Parser;
use crossbeam::channel::{self, Receiver, Sender};
use ratatui::crossterm::{
    self,
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
};
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    prelude::Backend,
    style::{Color, Modifier, Style},
    symbols,
    text::{Line, Span},
    widgets::{Axis, Block, Borders, Chart, Dataset, GraphType, Paragraph},
    Frame, Terminal,
};
use toml_edit::{DocumentMut, InlineTable, Value};
use tui_prompts::{Prompt, State, TextPrompt, TextState};
use twinleaf::{
    data,
    device::{self, buffer::AlignedWindow, buffer::ColumnBatch, Buffer, BufferEvent, ColumnSpec, DeviceTree, StreamId},
    tio::{self, proto::DeviceRoute},
};
use twinleaf_tools::TioOpts;
use welch_sde::{Build, SpectralDensity};

/// ---------- CLI ----------
#[derive(Parser, Debug)]
#[command(name = "tio-monitor", version, about = "Display live sensor data")]
struct Cli {
    #[command(flatten)]
    tio: TioOpts,

    /// Show multiple devices under the route
    #[arg(short = 'a', long = "all")]
    all: bool,

    /// Target frames per second for UI rendering
    #[arg(
        long = "fps",
        default_value_t = 20,
        value_parser = clap::value_parser!(u32).range(1..=240)
    )]
    fps: u32,

    /// Configuration file for colorizing stream column values
    #[arg(short = 'c', long = "colors", value_name = "CONF.toml")]
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

    pub spec: ColumnSpec,
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

/// ---------- Communication Structs ----------
#[derive(Debug, Clone)]
struct DataReq {
    all: bool,
    parent_route: DeviceRoute,
    selection: Option<ColumnSpec>,
    seconds: f64,
}

#[derive(Debug, Clone)]
struct DataResp {
    last: Vec<(DeviceRoute, StreamId, data::Sample)>,
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
    // Ok(SuccessString), Err(ErrorString)
    pub result: Result<String, String>,
}

/// ---------- App State ----------
pub struct App {
    pub all: bool,
    pub parent_route: DeviceRoute,

    pub last: BTreeMap<(DeviceRoute, StreamId), (data::Sample, Instant)>,
    pub nav: Nav,
    pub nav_items: Vec<NavItem>,

    pub show_plot: bool,
    pub show_footer: bool,
    pub show_routes: bool,
    pub show_fft: bool,

    // RPC / Command Mode
    pub show_cmd: bool,
    pub input_state: TextState<'static>,
    pub cmd_history: Vec<String>,
    pub history_ptr: Option<usize>,
    pub last_rpc_result: Option<(String, Color)>,

    pub scroll: u16,
    pub desc_width: usize,
    pub follow_selection: bool,

    pub color_bounds: Option<HashMap<String, (std::ops::RangeInclusive<f64>, bool)>>,
    pub plot_window_seconds: f64,
    pub plot_width_percent: u16,

    pub window_aligned: Option<AlignedWindow>,
}

impl App {
    pub fn new(all: bool, parent_route: &DeviceRoute) -> Self {
        Self {
            all,
            parent_route: parent_route.clone(),
            last: BTreeMap::new(),
            nav: Nav::default(),
            nav_items: Vec::new(),
            show_plot: false,
            show_fft: false,
            scroll: 0,
            desc_width: 0,
            plot_window_seconds: 5.0,
            show_footer: true,
            plot_width_percent: 70,
            follow_selection: true,
            show_routes: false,
            window_aligned: None,
            color_bounds: None,

            // Command Mode Defaults
            show_cmd: false,
            input_state: TextState::default(),
            cmd_history: Vec::new(),
            history_ptr: None,
            last_rpc_result: None,
        }
    }

    pub fn enter_cmd(&mut self) {
        self.show_cmd = true;
        self.input_state = TextState::default();
        self.input_state.focus();
        self.history_ptr = None;
    }

    pub fn exit_cmd(&mut self) {
        self.show_cmd = false;
        self.input_state.blur();
    }

    pub fn visible_routes(&self) -> Vec<DeviceRoute> {
        if self.all {
            let mut routes: Vec<_> = self.last.keys().map(|(route, _)| route.clone()).collect();
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
                .filter(|(r, _sid)| r == route)
                .map(|(_, sid)| *sid)
                .collect();
            stream_ids.sort();

            for (stream_idx, sid) in stream_ids.iter().enumerate() {
                if let Some((sample, _seen)) = self.last.get(&(route.clone(), *sid)) {
                    for (column_idx, _col) in sample.columns.iter().enumerate() {
                        let spec = ColumnSpec {
                            route: route.clone(),
                            stream_id: *sid,
                            column_id: column_idx,
                        };
                        new_items.push(NavItem {
                            device_idx: dev_idx,
                            stream_idx,
                            column_idx,
                            route: route.clone(),
                            stream_id: *sid,
                            column_id: column_idx,
                            spec,
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

    pub fn current_selection(&self) -> Option<ColumnSpec> {
        self.current_item().map(|it| it.spec.clone())
    }

    pub fn current_device_index(&self) -> usize {
        self.current_item().map(|it| it.device_idx).unwrap_or(0)
    }

    pub fn device_count(&self) -> usize {
        self.visible_routes().len()
    }

    pub fn cycle(&mut self, backward: bool, device: bool, stream: bool, column: bool) {
        let scope = NavScope {
            device,
            stream,
            column,
        };
        self.nav.step(&self.nav_items, scope, backward);
    }

    pub fn increase_window(&mut self) {
        self.plot_window_seconds = (self.plot_window_seconds + 0.5).min(10.0);
    }
    pub fn decrease_window(&mut self) {
        self.plot_window_seconds = (self.plot_window_seconds - 0.5).max(0.5);
    }
    pub fn increase_plot_width(&mut self) {
        self.plot_width_percent = (self.plot_width_percent + 5).min(90);
    }
    pub fn decrease_plot_width(&mut self) {
        self.plot_width_percent = (self.plot_width_percent.saturating_sub(5)).max(20);
    }

pub fn get_plot_data(&self) -> Option<(Vec<(f64, f64)>, f64, f64)> {
        let spec = self.current_selection()?;
        let win = self.window_aligned.as_ref()?;
        let batch = win.columns.get(&spec)?;
        if win.timestamps.is_empty() {
            return None;
        }

        let data: Vec<(f64, f64)> = match batch {
            ColumnBatch::F64(vec) => {
                if vec.is_empty() { return None; }
                win.timestamps.iter().copied().zip(vec.iter().copied()).collect()
            }
            ColumnBatch::I64(vec) => {
                if vec.is_empty() { return None; }
                win.timestamps.iter().copied().zip(vec.iter().map(|&x| x as f64)).collect()
            }
            ColumnBatch::U64(vec) => {
                if vec.is_empty() { return None; }
                win.timestamps.iter().copied().zip(vec.iter().map(|&x| x as f64)).collect()
            }
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
            ColumnBatch::F64(vec) => vec.clone(),
            ColumnBatch::I64(vec) => vec.iter().map(|&x| x as f64).collect(),
            ColumnBatch::U64(vec) => vec.iter().map(|&x| x as f64).collect(),
        };

        if signal.len() < 128 {
            return None;
        }

        let welch: SpectralDensity<f64> = SpectralDensity::builder(&signal, sampling_hz).build();
        let sd = welch.periodogram();
        let raw: Vec<f64> = sd.iter().copied().collect();
        let vals: Vec<f64> = raw
            .iter()
            .copied()
            .filter(|v| v.is_finite() && *v > 0.0)
            .collect();
        if vals.is_empty() {
            return None;
        }
        let noise = vals.iter().sum::<f64>() / vals.len() as f64;
        if !noise.is_finite() || noise <= 0.0 {
            return None;
        }

        let pts: Vec<(f64, f64)> = sd
            .frequency()
            .into_iter()
            .zip(raw.into_iter())
            .filter(|(_, d)| d.is_finite() && *d > 0.0)
            .collect();

        Some((pts, noise))
    }

    pub fn get_focused_channel_info(&self) -> Option<(String, String, usize, usize)> {
        let item = self.current_item()?;
        let (sample, _) = self.last.get(&(item.route.clone(), item.stream_id))?;
        let col = sample.columns.get(item.column_id)?;
        Some((
            col.desc.description.clone(),
            col.desc.units.clone(),
            item.column_id + 1,
            sample.columns.len(),
        ))
    }
}

/// ---------- Main Logic ----------
pub fn handle_input_event(ev: Event, app: &mut App, rpc_tx: &Sender<RpcReq>) -> bool {
    match ev {
        Event::Key(k) => {
            if k.kind != KeyEventKind::Press {
                return false;
            }

            // --- Command Mode Handling ---
            if app.show_cmd {
                match k.code {
                    KeyCode::Esc => app.exit_cmd(),

                    // History Up
                    KeyCode::Up => {
                        if !app.cmd_history.is_empty() {
                            let idx = app
                                .history_ptr
                                .map(|i| i.saturating_sub(1))
                                .unwrap_or(app.cmd_history.len() - 1);
                            app.history_ptr = Some(idx);
                            app.input_state =
                                TextState::new().with_value(app.cmd_history[idx].clone());
                            app.input_state.focus();
                            app.input_state.move_end();
                        }
                    }

                    // History Down
                    KeyCode::Down => {
                        if let Some(idx) = app.history_ptr {
                            if idx + 1 < app.cmd_history.len() {
                                let new_idx = idx + 1;
                                app.history_ptr = Some(new_idx);
                                app.input_state =
                                    TextState::new().with_value(app.cmd_history[new_idx].clone());
                                app.input_state.focus();
                                app.input_state.move_end();
                            } else {
                                app.history_ptr = None;
                                app.input_state = TextState::default();
                                app.input_state.focus();
                            }
                        }
                    }

                    // Submit Command
                    KeyCode::Enter => {
                        let line = app.input_state.value().to_string();
                        if !line.trim().is_empty() {
                            // 1. Save History
                            if app.cmd_history.last() != Some(&line) {
                                app.cmd_history.push(line.clone());
                            }
                            app.history_ptr = None;

                            // 2. Parse "method arg"
                            let mut parts = line.split_whitespace();
                            if let Some(method) = parts.next() {
                                let remainder: Vec<&str> = parts.collect();
                                let arg = if remainder.is_empty() {
                                    None
                                } else {
                                    Some(remainder.join(" "))
                                };

                                // 3. Determine Target
                                let route = app
                                    .current_item()
                                    .map(|i| i.route.clone())
                                    .unwrap_or(app.parent_route.clone());

                                // 4. Send to Data Thread
                                let _ = rpc_tx.send(RpcReq {
                                    route: route.clone(),
                                    method: method.to_string(),
                                    arg,
                                });

                                app.last_rpc_result =
                                    Some((format!("Sent to {}...", route), Color::Yellow));

                                // Reset Input, keep focus
                                app.input_state = TextState::default();
                                app.input_state.focus();
                            }
                        }
                    }
                    _ => {
                        app.input_state.handle_key_event(k);
                    }
                }
                return false;
            }

            // --- Normal Mode Handling ---
            if k.code == KeyCode::Char(':') {
                app.enter_cmd();
                return false;
            }

            if k.code == KeyCode::Esc {
                if app.show_plot {
                    app.show_plot = false;
                } else {
                    return true;
                } // Quit if no plot
                return false;
            }

            let quit = matches!(k.code, KeyCode::Char('q'))
                || (k.code == KeyCode::Char('c') && k.modifiers == KeyModifiers::CONTROL);
            if quit {
                return true;
            }

            match k.code {
                KeyCode::Up => {
                    app.follow_selection = true;
                    app.cycle(true, true, true, true);
                }
                KeyCode::Down => {
                    app.follow_selection = true;
                    app.cycle(false, true, true, true);
                }
                KeyCode::Left => {
                    app.follow_selection = true;
                    app.cycle(true, false, false, true);
                }
                KeyCode::Right => {
                    app.follow_selection = true;
                    app.cycle(false, false, false, true);
                }
                KeyCode::BackTab => {
                    app.follow_selection = true;
                    app.cycle(true, true, false, false);
                }
                KeyCode::Tab => {
                    app.follow_selection = true;
                    app.cycle(false, true, false, false);
                }
                KeyCode::PageUp => {
                    app.follow_selection = false;
                    app.scroll = app.scroll.saturating_sub(10);
                }
                KeyCode::PageDown => {
                    app.follow_selection = false;
                    app.scroll = app.scroll.saturating_add(10);
                }
                KeyCode::Home => {
                    app.follow_selection = true;
                    app.nav.home(&app.nav_items);
                }
                KeyCode::End => {
                    app.follow_selection = true;
                    app.nav.end(&app.nav_items);
                }
                KeyCode::Enter => {
                    if app.current_selection().is_some() {
                        app.show_plot = !app.show_plot;
                    }
                }
                KeyCode::Char('+') | KeyCode::Char('=') => {
                    app.increase_window();
                }
                KeyCode::Char('-') | KeyCode::Char('_') => {
                    app.decrease_window();
                }
                KeyCode::Char('[') => {
                    app.increase_plot_width();
                }
                KeyCode::Char(']') => {
                    app.decrease_plot_width();
                }
                KeyCode::Char('f') => {
                    if app.show_plot {
                        app.show_fft = !app.show_fft;
                    }
                }
                KeyCode::Char('h') => {
                    app.show_footer = !app.show_footer;
                }
                KeyCode::Char('r') => {
                    app.show_routes = !app.show_routes;
                }
                _ => {}
            }
            false
        }
        Event::Resize(_, _) => false,
        _ => false,
    }
}

fn exec_rpc(
    tree: &mut DeviceTree,
    route: DeviceRoute,
    method: String,
    arg_str: Option<String>,
) -> Result<String, String> {
    let meta: u16 = tree
        .rpc(route.clone(), "rpc.info", &method)
        .map_err(|_| "Unknown RPC name".to_string())?;

    let spec = device::util::parse_rpc_spec(meta, method.clone());
    let type_str = spec.type_str();

    let payload = if let Some(s) = arg_str {
        match type_str.as_str() {
            "u8" => s
                .parse::<u8>()
                .map_err(|_| "Invalid u8")?
                .to_le_bytes()
                .to_vec(),
            "u16" => s
                .parse::<u16>()
                .map_err(|_| "Invalid u16")?
                .to_le_bytes()
                .to_vec(),
            "u32" => s
                .parse::<u32>()
                .map_err(|_| "Invalid u32")?
                .to_le_bytes()
                .to_vec(),
            "u64" => s
                .parse::<u64>()
                .map_err(|_| "Invalid u64")?
                .to_le_bytes()
                .to_vec(),
            "i8" => s
                .parse::<i8>()
                .map_err(|_| "Invalid i8")?
                .to_le_bytes()
                .to_vec(),
            "i16" => s
                .parse::<i16>()
                .map_err(|_| "Invalid i16")?
                .to_le_bytes()
                .to_vec(),
            "i32" => s
                .parse::<i32>()
                .map_err(|_| "Invalid i32")?
                .to_le_bytes()
                .to_vec(),
            "i64" => s
                .parse::<i64>()
                .map_err(|_| "Invalid i64")?
                .to_le_bytes()
                .to_vec(),
            "f32" => s
                .parse::<f32>()
                .map_err(|_| "Invalid f32")?
                .to_le_bytes()
                .to_vec(),
            "f64" => s
                .parse::<f64>()
                .map_err(|_| "Invalid f64")?
                .to_le_bytes()
                .to_vec(),
            "string" | "" => s.as_bytes().to_vec(),
            t => return Err(format!("Unsupported type: {}", t)),
        }
    } else {
        Vec::new()
    };

    let reply_bytes = tree
        .raw_rpc(route, &method, &payload)
        .map_err(|e| format!("RPC Exec Failed: {:?}", e))?;

    if reply_bytes.is_empty() {
        return Ok("OK".to_string());
    }

    let reply_str = match type_str.as_str() {
        "u8" => u8::from_le_bytes(reply_bytes[0..1].try_into().unwrap()).to_string(),
        "u16" => u16::from_le_bytes(reply_bytes[0..2].try_into().unwrap()).to_string(),
        "u32" => u32::from_le_bytes(reply_bytes[0..4].try_into().unwrap()).to_string(),
        "u64" => u64::from_le_bytes(reply_bytes[0..8].try_into().unwrap()).to_string(),
        "i8" => i8::from_le_bytes(reply_bytes[0..1].try_into().unwrap()).to_string(),
        "i16" => i16::from_le_bytes(reply_bytes[0..2].try_into().unwrap()).to_string(),
        "i32" => i32::from_le_bytes(reply_bytes[0..4].try_into().unwrap()).to_string(),
        "i64" => i64::from_le_bytes(reply_bytes[0..8].try_into().unwrap()).to_string(),
        "f32" => f32::from_le_bytes(reply_bytes[0..4].try_into().unwrap()).to_string(),
        "f64" => f64::from_le_bytes(reply_bytes[0..8].try_into().unwrap()).to_string(),
        "string" | "" => {
            if let Ok(s) = std::str::from_utf8(&reply_bytes) {
                format!("\"{}\"", s)
            } else {
                format!("Hex: {:02X?}", reply_bytes)
            }
        }
        _ => format!("{:02X?}", reply_bytes),
    };

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
    let mut buffer = Buffer::new(evt_tx, capacity, false);
    let mut last: BTreeMap<(DeviceRoute, StreamId), data::Sample> = BTreeMap::new();

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
        last.insert((route.clone(), stream_id), sample);

        while let Ok(q) = req_rx.try_recv() {
            let resp = make_response(&buffer, &last, q);
            if resp_tx.send(resp).is_err() {
                return;
            }
        }
    }

    while let Ok(q) = req_rx.try_recv() {
        let _ = resp_tx.send(make_response(&buffer, &last, q));
    }
}

fn make_response(
    buffer: &Buffer,
    last: &BTreeMap<(DeviceRoute, StreamId), data::Sample>,
    q: DataReq,
) -> DataResp {
    let last_vec: Vec<(DeviceRoute, StreamId, data::Sample)> = if q.all {
        last.iter()
            .map(|((r, sid), s)| (r.clone(), *sid, s.clone()))
            .collect()
    } else {
        last.iter()
            .filter(|((r, _), _)| *r == q.parent_route)
            .map(|((r, sid), s)| (r.clone(), *sid, s.clone()))
            .collect()
    };

    let window = q.selection.and_then(|spec| {
        let stream_key = spec.stream_key();
        let active = buffer.active_segments.get(&stream_key)?;
        let sampling_hz = (active.buffer.segment_metadata.sampling_rate
            / active.buffer.segment_metadata.decimation) as f64;
        let mut n_samples = (q.seconds * sampling_hz).ceil() as usize;
        n_samples = n_samples.max(10);
        buffer.read_aligned_window(&[spec], n_samples).ok()
    });

    DataResp {
        last: last_vec,
        window,
    }
}

/// ---------- Rendering ----------
fn draw_ui<B: Backend>(terminal: &mut Terminal<B>, app: &mut App) -> io::Result<()> {
    terminal.draw(|f| {
        let size = f.area();
        let estimated_footer_height = 6;
        let available_height = size.height.saturating_sub(estimated_footer_height);
        let show_scrollbar = will_need_scrollbar(app, available_height);

        let (main_area, footer_area) = {
            let footer_height = if app.show_cmd {
                4
            } else if app.show_footer {
                6
            } else {
                2
            };
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(10), Constraint::Length(footer_height)])
                .split(size);
            (chunks[0], Some(chunks[1]))
        };

        let (left_area, right_opt): (Rect, Option<Rect>) = if app.show_plot {
            let chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Percentage(100 - app.plot_width_percent),
                    Constraint::Percentage(app.plot_width_percent),
                ])
                .split(main_area);
            (chunks[0], Some(chunks[1]))
        } else {
            (main_area, None)
        };

        render_monitor_panel(f, app, left_area, Instant::now());

        if let Some(r) = right_opt {
            render_graphics_panel(f, app, r);
        }

        if let Some(footer) = footer_area {
            render_footer(f, show_scrollbar, app.device_count(), footer, app);
        }
    })?;
    Ok(())
}

fn render_footer(
    f: &mut Frame,
    _show_scroll: bool,
    device_count: usize,
    area: Rect,
    app: &mut App,
) {
    // --- Command Mode Render ---
    if app.show_cmd {
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

        let target_route = app
            .current_item()
            .map(|i| i.route.clone())
            .unwrap_or(app.parent_route.clone());

        let prompt_str = format!("[{}]", target_route);

        let block = Block::default()
            .borders(Borders::TOP)
            .title(" Command Mode ");

        TextPrompt::from(prompt_str)
            .with_block(block)
            .draw(f, chunks[1], &mut app.input_state);
        return;
    }

    // --- Standard Footer Render ---
    if !app.show_footer {
        let minimal_line = Line::from(vec![
            Span::raw("  "),
            key_span("h"),
            Span::raw(" Toggle Footer"),
        ]);
        let block = Block::default()
            .borders(Borders::TOP)
            .border_style(Style::default().fg(Color::DarkGray));
        f.render_widget(Paragraph::new(vec![minimal_line]).block(block), area);
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

    if device_count > 1 {
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
            "  Window     ",
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
        Span::raw(" Plot Width"),
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

fn fmt_value(v: &data::ColumnData) -> (String, f64) {
    use data::ColumnData::*;
    match v {
        Int(x) => (format!("{:10}      ", x), *x as f64),
        UInt(x) => (format!("{:10}      ", x), *x as f64),
        Float(x) => {
            if x.is_nan() {
                (format!("{:10}      ", x), f64::NAN)
            } else {
                (format!("{:15.4} ", x), *x)
            }
        }
        Unknown => (format!("{:>15} ", "unsupported"), f64::NAN),
    }
}

fn load_color_bounds(
    path: &str,
) -> io::Result<HashMap<String, (std::ops::RangeInclusive<f64>, bool)>> {
    let mut f = File::open(path)?;
    let mut contents = String::new();
    f.read_to_string(&mut contents)?;

    let doc = DocumentMut::from_str(&contents)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let mut bounds = HashMap::new();
    for (keys, value) in doc.get_values() {
        let column_name = keys.iter().map(|k| k.get()).collect::<Vec<_>>().join(".");
        let Value::InlineTable(it) = value else {
            continue;
        };

        let (is_temp, lower) = if let Some(min) = get_toml_number(it, "cold", &column_name)? {
            (true, min)
        } else if let Some(min) = get_toml_number(it, "min", &column_name)? {
            (false, min)
        } else {
            (false, f64::NEG_INFINITY)
        };

        let (is_temp, upper) = if let Some(max) = get_toml_number(it, "hot", &column_name)? {
            (true, max)
        } else if let Some(max) = get_toml_number(it, "max", &column_name)? {
            (is_temp, max)
        } else {
            (is_temp, f64::INFINITY)
        };

        bounds.insert(column_name, (lower..=upper, is_temp));
    }
    Ok(bounds)
}

fn get_toml_number(it: &InlineTable, key: &str, _col: &str) -> io::Result<Option<f64>> {
    let Some(value) = it.get(key) else {
        return Ok(None);
    };
    match value {
        Value::Float(x) => Ok(if x.clone().into_value().is_nan() {
            None
        } else {
            Some(x.clone().into_value())
        }),
        Value::Integer(x) => Ok(Some(x.clone().into_value() as f64)),
        _ => Ok(None),
    }
}

fn bounds_color(
    bounds: &HashMap<String, (std::ops::RangeInclusive<f64>, bool)>,
    s: &str,
    c: &str,
    v: f64,
) -> Color {
    if v.is_nan() {
        return Color::Yellow;
    }
    let key = format!("{}.{}", s, c);
    let Some((range, is_temp)) = bounds.get(&key) else {
        return Color::Reset;
    };
    if v < *range.start() {
        if *is_temp {
            Color::Blue
        } else {
            Color::Red
        }
    } else if v > *range.end() {
        Color::Red
    } else {
        Color::Green
    }
}

fn render_monitor_panel(f: &mut Frame, app: &mut App, area: Rect, now: Instant) {
    let left_inner = Rect {
        x: area.x,
        y: area.y,
        width: area.width.saturating_sub(1),
        height: area.height,
    };
    let (lines, col_line_idx) = build_left_lines(app, now);
    let total_lines = lines.len();
    let view_h = left_inner.height as usize;

    if app.follow_selection {
        auto_scroll_to_selected(app, &col_line_idx, left_inner.height, total_lines);
    }
    let max_scroll = total_lines.saturating_sub(view_h);
    app.scroll = (app.scroll as usize).min(max_scroll) as u16;

    let para = Paragraph::new(lines)
        .block(Block::default().borders(Borders::NONE))
        .scroll((app.scroll, 0));
    f.render_widget(para, left_inner);
    draw_scrollbar(f, area, total_lines, view_h, app.scroll as usize);
}

fn build_left_lines(app: &mut App, now: Instant) -> (Vec<Line<'static>>, HashMap<usize, usize>) {
    let mut lines: Vec<Line<'static>> = Vec::new();
    let mut col_line_idx: HashMap<usize, usize> = HashMap::new();

    app.desc_width = app.desc_width.max(
        app.last
            .values()
            .flat_map(|(s, _)| s.columns.iter())
            .map(|c| c.desc.description.len())
            .max()
            .unwrap_or(0),
    );

    let routes = app.visible_routes();
    if routes.is_empty() || app.last.is_empty() {
        lines.push(Line::from("Waiting for data..."));
        return (lines, col_line_idx);
    }

    let focused_device = app.current_device_index();
    let mut global_idx = 0;

    for (dev_idx, route) in routes.iter().enumerate() {
        let dev_meta = app
            .last
            .iter()
            .find(|((r, _), _)| r == route)
            .map(|(_, (s, _))| s.device.as_ref());
        let mut header_style = Style::default().add_modifier(Modifier::BOLD);
        if dev_idx == focused_device {
            header_style = header_style.add_modifier(Modifier::UNDERLINED);
        }

        let mut spans = Vec::new();
        if let Some(d) = dev_meta {
            spans.push(Span::styled(d.name.clone(), header_style));
            if app.show_routes {
                spans.push(Span::raw(format!(" [{}]", route)));
            }
            spans.push(Span::raw(format!(" Serial: {}", d.serial_number)));
        } else {
            spans.push(Span::styled("<device>", header_style));
        }
        lines.push(Line::from(spans));
        lines.push(Line::from(""));

        let mut stream_ids: Vec<_> = app
            .last
            .iter()
            .filter(|((r, _), _)| r == route)
            .map(|((_, sid), _)| *sid)
            .collect();
        stream_ids.sort();

        for sid in stream_ids {
            if let Some((sample, seen)) = app.last.get(&(route.clone(), sid)) {
                let age = now.saturating_duration_since(*seen);
                let base_style = if age > Duration::from_millis(1200) {
                    Style::default().add_modifier(Modifier::DIM)
                } else {
                    Style::default()
                };

                for (_, col) in sample.columns.iter().enumerate() {
                    let nav_idx = global_idx;
                    col_line_idx.insert(nav_idx, lines.len());
                    global_idx += 1;

                    let (value_str, fval) = fmt_value(&col.value);
                    let mut desc = col.desc.description.clone();
                    if desc.len() < app.desc_width {
                        desc.push_str(&" ".repeat(app.desc_width - desc.len()));
                    }

                    let is_selected = app.nav.idx == Some(nav_idx);
                    let name_style = if is_selected {
                        if app.show_plot {
                            base_style.add_modifier(Modifier::BOLD)
                        } else {
                            base_style.add_modifier(Modifier::BOLD | Modifier::RAPID_BLINK)
                        }
                    } else {
                        base_style
                    };

                    let mut value_style = base_style.add_modifier(Modifier::BOLD);
                    let mut units_style = base_style.add_modifier(Modifier::BOLD);

                    if let Some(bounds) = app.color_bounds.as_ref() {
                        let stream_name = sample.stream.as_ref().name.as_str();
                        let c = bounds_color(bounds, stream_name, col.desc.name.as_str(), fval);
                        if c != Color::Reset {
                            value_style = value_style.fg(c);
                            units_style = units_style.fg(c);
                        }
                    }

                    lines.push(Line::from(vec![
                        Span::styled(desc, name_style),
                        Span::raw("  "),
                        Span::styled(value_str, value_style),
                        Span::styled(col.desc.units.clone(), units_style),
                    ]));
                }
            }
        }
        if dev_idx + 1 < routes.len() {
            lines.push(Line::from(""));
        }
    }
    (lines, col_line_idx)
}

fn draw_scrollbar(frame: &mut Frame, area: Rect, total: usize, view_h: usize, pos: usize) {
    if view_h == 0 || total <= view_h {
        return;
    }
    let handle_h = (view_h * view_h / total).max(1);
    let scrollable = total.saturating_sub(view_h).max(1);
    let max_top = view_h.saturating_sub(handle_h);
    let top = (pos * max_top) / scrollable;

    let sb_area = Rect {
        x: area.x + area.width.saturating_sub(1),
        y: area.y,
        width: 1,
        height: area.height,
    };
    let mut lines = Vec::with_capacity(view_h);
    for i in 0..view_h {
        let ch = if i >= top && i < top + handle_h {
            "█"
        } else {
            "│"
        };
        lines.push(Line::from(Span::styled(
            ch,
            Style::default().fg(Color::Gray),
        )));
    }
    frame.render_widget(Paragraph::new(lines), sb_area);
}

fn auto_scroll_to_selected(
    app: &mut App,
    col_line_idx: &HashMap<usize, usize>,
    view_h: u16,
    total_lines: usize,
) {
    let sel = match app.nav.idx {
        Some(i) => i,
        None => return,
    };
    let line = match col_line_idx.get(&sel) {
        Some(&l) => l,
        None => return,
    };
    let view_h = view_h as usize;
    if view_h == 0 || total_lines <= view_h {
        app.scroll = 0;
        return;
    }

    let current = app.scroll as usize;
    let bottom = current.saturating_add(view_h);
    if line >= current && line < bottom {
        return;
    }

    let half = view_h / 2;
    let target = line.saturating_sub(half);
    let max = total_lines.saturating_sub(view_h);
    app.scroll = target.min(max) as u16;
}

fn render_graphics_panel(f: &mut Frame, app: &App, area: Rect) {
    if let Some((desc, units, _cur_col, _total_cols)) = app.get_focused_channel_info() {
        let item = app.current_item().unwrap();
        if app.show_fft {
            if let Some((sd_data, noise_floor)) = app.get_spectral_density_data() {
                let title = format!(
                    "{} — {} (DC detrend {:.1}s) | Mean: {:.3e} {}/√Hz",
                    item.route, desc, app.plot_window_seconds, noise_floor, units
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
                        .style(Style::default().fg(Color::Red))
                        .graph_type(GraphType::Line)
                        .data(&log_data);
                    let chart = Chart::new(vec![dataset])
                        .block(block)
                        .x_axis(
                            Axis::default()
                                .title("Freq [Hz] (log)")
                                .bounds([min_f, max_f])
                                .labels(vec![
                                    format!("{:.1e}", 10f64.powf(min_f)),
                                    format!("{:.1e}", 10f64.powf(max_f)),
                                ]),
                        )
                        .y_axis(
                            Axis::default()
                                .title(format!("Val [{}/√Hz]", units))
                                .bounds([min_d - y_pad, max_d + y_pad])
                                .labels(vec![
                                    format!("{:.1e}", 10f64.powf(min_d)),
                                    format!("{:.1e}", 10f64.powf(max_d)),
                                ]),
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
                item.route, desc, app.plot_window_seconds
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
                            .labels(vec![format!("{:.1}", min_t), format!("{:.1}", max_t)]),
                    )
                    .y_axis(
                        Axis::default()
                            .title(format!("Value [{}]", units))
                            .bounds([min_v - pad, max_v + pad])
                            .labels(vec![format!("{:.3}", min_v), format!("{:.3}", max_v)]),
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

fn will_need_scrollbar(app: &App, viewport_height: u16) -> bool {
    let routes = app.visible_routes();
    if routes.is_empty() || app.last.is_empty() {
        return false;
    }
    let mut line_count = 0;
    for (idx, route) in routes.iter().enumerate() {
        line_count += 2;
        if app.show_routes {
            line_count += 1;
        }
        let mut stream_ids: Vec<_> = app
            .last
            .iter()
            .filter(|((r, _), _)| r == route)
            .map(|((_, sid), _)| *sid)
            .collect();
        stream_ids.sort();
        stream_ids.dedup();
        for sid in stream_ids {
            if let Some((sample, _)) = app.last.get(&(route.clone(), sid)) {
                line_count += sample.columns.len();
            }
        }
        if idx + 1 < routes.len() {
            line_count += 1;
        }
    }
    line_count > viewport_height as usize
}

fn main() {
    let cli = Cli::parse();
    let proxy = tio::proxy::Interface::new(&cli.tio.root);
    let parent_route: DeviceRoute = cli.tio.parse_route();
    let tree = DeviceTree::open(&proxy, parent_route.clone()).unwrap_or_else(|e| {
        ratatui::restore();
        eprintln!("Failed to open device tree: {e:?}");
        std::process::exit(1);
    });

    let (req_tx, req_rx) = channel::bounded::<DataReq>(1);
    let (resp_tx, resp_rx) = channel::bounded::<DataResp>(1);
    let (rpc_tx, rpc_rx) = channel::bounded::<RpcReq>(1);
    let (rpc_resp_tx, rpc_resp_rx) = channel::bounded::<RpcResp>(1);

    std::thread::spawn(move || run_data_thread(tree, req_rx, resp_tx, rpc_rx, rpc_resp_tx, 100000));

    let mut app = App::new(cli.all, &parent_route);

    if let Some(path) = &cli.colors {
        match load_color_bounds(path) {
            Ok(b) => app.color_bounds = Some(b),
            Err(e) => {
                eprintln!("Failed to load color config {}: {e}", path);
                app.color_bounds = None;
            }
        }
    }

    let mut terminal: Terminal<_> = ratatui::init();
    let frame_dt = Duration::from_millis(1_000 / cli.fps as u64);

    'main: loop {
        while crossterm::event::poll(Duration::from_millis(0)).unwrap_or(false) {
            match event::read() {
                Ok(ev) => {
                    if handle_input_event(ev, &mut app, &rpc_tx) {
                        break 'main;
                    }
                }
                Err(_) => break 'main,
            }
        }

        let req = DataReq {
            all: app.all,
            parent_route: app.parent_route.clone(),
            selection: app.current_selection(),
            seconds: app.plot_window_seconds,
        };
        if req_tx.send(req).is_err() {
            break 'main;
        }
        let resp = match resp_rx.recv() {
            Ok(r) => r,
            Err(_) => break 'main,
        };

        while let Ok(rpc_res) = rpc_resp_rx.try_recv() {
            let (msg, color) = match rpc_res.result {
                Ok(s) => (format!("OK: {}", s), Color::Green),
                Err(s) => (format!("ERR: {}", s), Color::Red),
            };
            app.last_rpc_result = Some((msg, color));
        }

        let now = Instant::now();
        for (route, sid, sample) in resp.last {
            if !app.all && route != app.parent_route {
                continue;
            }
            let key = (route.clone(), sid);
            let mut seen = now;
            if let Some((prev_sample, prev_seen)) = app.last.get(&key) {
                if prev_sample.n == sample.n {
                    seen = *prev_seen;
                }
            }
            app.desc_width = app.desc_width.max(
                sample
                    .columns
                    .iter()
                    .map(|c| c.desc.description.len())
                    .max()
                    .unwrap_or(0),
            );
            app.last.insert(key, (sample, seen));
        }
        app.window_aligned = resp.window;
        app.rebuild_nav_items();

        if draw_ui(&mut terminal, &mut app).is_err() {
            break 'main;
        }
        std::thread::sleep(frame_dt);
    }

    ratatui::restore();
}
