use std::{
    collections::{BTreeMap, HashMap},
    io,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use clap::Parser;
use crossbeam::channel;
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    prelude::Backend,
    style::{Color, Modifier, Style},
    symbols,
    text::{Line, Span},
    widgets::{Axis, Block, Borders, Chart, Dataset, GraphType, Paragraph},
    Frame, Terminal,
};
use twinleaf::{
    data,
    device::{Buffer, BufferEvent, ColumnSpec, DeviceTree, StreamId},
    tio::{self, proto::DeviceRoute},
};
use twinleaf_tools::TioOpts;
use welch_sde::{Build, SpectralDensity};

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
}

#[derive(Debug, Clone)]
pub struct NavItem {
    pub device_idx: usize,
    pub stream_idx: usize,
    pub column_idx: usize,

    /// Route / stream / column identifiers needed to build ColumnSpec
    pub route: DeviceRoute,
    pub stream_id: StreamId,
    pub column_id: usize,

    /// Cached ColumnSpec for convenience
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

        let is_device_only_step = scope.device && !scope.stream && !scope.column;

        if is_device_only_step {
            self.step_device_axis(items, cur_item, backward);
        } else {
            self.step_contiguous(items, cur_item, cur_idx, scope, backward);
        }
    }

    pub fn home(&mut self, items: &[NavItem]) {
        if items.is_empty() {
            self.idx = None;
        } else {
            self.idx = Some(0);
        }
    }

    pub fn end(&mut self, items: &[NavItem]) {
        if items.is_empty() {
            self.idx = None;
        } else {
            self.idx = Some(items.len() - 1);
        }
    }
}

pub struct App {
    pub all: bool,
    pub parent_route: DeviceRoute,
    pub child_routes: Vec<DeviceRoute>,

    /// Most recent sample per (route, stream) for building nav tree and display.
    pub last: BTreeMap<(DeviceRoute, StreamId), (data::Sample, Instant)>,

    pub nav: Nav,
    pub nav_items: Vec<NavItem>,

    pub show_detail: bool,
    pub fft: bool,
    pub scroll: u16,
    pub desc_width: usize,

    pub buffer: Arc<RwLock<Buffer>>,
    pub plot_window_seconds: f64,
    pub show_footer: bool,
    pub plot_width_percent: u16,

    /// Whether the viewport should automatically follow the selected item.
    pub follow_selection: bool,
}

impl App {
    pub fn new(all: bool, parent_route: &DeviceRoute, buffer: Arc<RwLock<Buffer>>) -> Self {
        App {
            all,
            parent_route: parent_route.clone(),
            child_routes: Vec::new(),
            last: BTreeMap::new(),
            nav: Nav::default(),
            nav_items: Vec::new(),
            show_detail: false,
            fft: false,
            scroll: 0,
            desc_width: 0,
            buffer,
            plot_window_seconds: 5.0,
            show_footer: false,
            plot_width_percent: 70,
            follow_selection: true,
        }
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

        // Try to preserve existing selection; otherwise default to first.
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
        let routes = self.visible_routes();
        routes.len()
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
        self.plot_width_percent = (self.plot_width_percent + 5).min(75);
    }

    pub fn decrease_plot_width(&mut self) {
        self.plot_width_percent = (self.plot_width_percent.saturating_sub(5)).max(20);
    }

    pub fn get_plot_data(&self) -> Option<(Vec<(f64, f64)>, f64, f64)> {
        let col_spec = self.current_selection()?;
        let buffer = self.buffer.read().ok()?;
        let stream_key = col_spec.stream_key();

        let active = buffer.active_segments.get(&stream_key)?;
        let sampling_rate =
            (active.buffer.segment_metadata.sampling_rate / active.buffer.segment_metadata.decimation)
                as f64;

        let n_samples = (self.plot_window_seconds * sampling_rate).ceil() as usize;
        let n_samples = n_samples.max(10);

        let window = buffer
            .read_aligned_window(&[col_spec.clone()], n_samples)
            .ok()?;

        let current_value = window
            .columns
            .get(&col_spec)
            .and_then(|data| data.last())
            .and_then(|cd| match cd {
                data::ColumnData::Float(x) => Some(*x),
                data::ColumnData::Int(x) => Some(*x as f64),
                data::ColumnData::UInt(x) => Some(*x as f64),
                _ => None,
            })?;

        let current_time = window.timestamps.last().copied()?;

        let mut data = Vec::new();
        if let Some(column_data) = window.columns.get(&col_spec) {
            for (i, cd) in column_data.iter().enumerate() {
                if let Some(&timestamp) = window.timestamps.get(i) {
                    let value = match cd {
                        data::ColumnData::Float(x) => *x,
                        data::ColumnData::Int(x) => *x as f64,
                        data::ColumnData::UInt(x) => *x as f64,
                        _ => continue,
                    };

                    data.push((timestamp, value));
                }
            }
        }

        Some((data, current_value, current_time))
    }

    pub fn get_spectral_density_data(&self) -> Option<(Vec<(f64, f64)>, f64)> {
        let col_spec = self.current_selection()?;
        let buffer = self.buffer.read().ok()?;
        let stream_key = col_spec.stream_key();

        let active = buffer.active_segments.get(&stream_key)?;
        let sampling_rate =
            (active.buffer.segment_metadata.sampling_rate / active.buffer.segment_metadata.decimation)
                as f64;

        let n_samples = (self.plot_window_seconds * sampling_rate).ceil() as usize;
        let n_samples = n_samples.max(1024);

        let window = buffer
            .read_aligned_window(&[col_spec.clone()], n_samples)
            .ok()?;

        let signal: Vec<f64> = window
            .columns
            .get(&col_spec)?
            .iter()
            .filter_map(|cd| match cd {
                data::ColumnData::Float(x) => Some(*x),
                data::ColumnData::Int(x) => Some(*x as f64),
                data::ColumnData::UInt(x) => Some(*x as f64),
                _ => None,
            })
            .collect();

        if signal.len() < 256 {
            return None;
        }

        let welch: SpectralDensity<f64> = SpectralDensity::builder(&signal, sampling_rate).build();
        let sd = welch.periodogram();

        // Take a copy so we can filter / normalize.
        let raw_sd: Vec<f64> = sd.iter().copied().collect();

        // Use only positive, finite bins to estimate the average noise floor.
        let valid_vals: Vec<f64> = raw_sd
            .iter()
            .copied()
            .filter(|v| v.is_finite() && *v > 0.0)
            .collect();

        if valid_vals.is_empty() {
            return None;
        }

        let noise_floor = valid_vals.iter().sum::<f64>() / valid_vals.len() as f64;
        if !noise_floor.is_finite() || noise_floor <= 0.0 {
            return None;
        }

        // Keep the full (freq, PSD) data, still in linear units.
        let data: Vec<(f64, f64)> = sd
            .frequency()
            .into_iter()
            .zip(raw_sd.into_iter())
            .filter(|(_, d)| d.is_finite() && *d > 0.0)
            .collect();

        Some((data, noise_floor))
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

fn will_need_scrollbar(app: &App, viewport_height: u16) -> bool {
    let routes = app.visible_routes();
    if routes.is_empty() || app.last.is_empty() {
        return false;
    }

    let mut line_count = 0;

    for (idx, route) in routes.iter().enumerate() {
        line_count += 2;

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

pub fn handle_input_event(ev: Event, app: &mut App) -> bool {
    match ev {
        Event::Key(k) => {
            if k.kind != KeyEventKind::Press {
                return false;
            }

            // Esc: close detail, or quit if already in top-level view.
            if k.code == KeyCode::Esc {
                if app.show_detail {
                    app.show_detail = false;
                } else {
                    return true;
                }
                return false;
            }

            let quit = matches!(k.code, KeyCode::Char('q'))
                || (k.code == KeyCode::Char('c') && k.modifiers == KeyModifiers::CONTROL);
            if quit {
                return true;
            }

            match k.code {
                // Full list traversal
                KeyCode::Up => {
                    app.follow_selection = true;
                    app.cycle(true, true, true, true);
                }
                KeyCode::Down => {
                    app.follow_selection = true;
                    app.cycle(false, true, true, true);
                }

                // Within a single stream (device+stream locked, only column varies)
                KeyCode::Left => {
                    app.follow_selection = true;
                    app.cycle(true, false, false, true);
                }
                KeyCode::Right => {
                    app.follow_selection = true;
                    app.cycle(false, false, false, true);
                }

                // Cycle devices: same (stream_idx, column_idx) "kind" across devices
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
                        app.show_detail = !app.show_detail;
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
                    if app.show_detail {
                        app.fft = !app.fft;
                    }
                }

                KeyCode::Char('h') => {
                    app.show_footer = !app.show_footer;
                }

                _ => {}
            }

            false
        }
        Event::Resize(_, _) => false,
        _ => false,
    }
}

fn draw_ui<B: Backend>(terminal: &mut Terminal<B>, app: &mut App) -> io::Result<()> {
    terminal
        .draw(|f| {
            let size = f.area();

            // Calculate if we need scrollbar based on actual available height
            let estimated_footer_height = 6;
            let available_height = size.height.saturating_sub(estimated_footer_height);
            let show_scrollbar = will_need_scrollbar(app, available_height);

            let (main_area, footer_area) = if app.show_footer || true { 
                let footer_height = if app.show_footer { 6 } else { 2 };
                let chunks = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([Constraint::Min(10), Constraint::Length(footer_height)])
                    .split(size);
                (chunks[0], Some(chunks[1]))
            } else {
                (size, None)
            };

            let (left_area, right_opt): (Rect, Option<Rect>) = if app.show_detail {
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
                let device_count = app.device_count();
                render_footer(f, show_scrollbar, device_count, footer, app.show_footer);
            }
        })
        .map(|_completed_frame| ())
}

fn render_footer(f: &mut Frame, _show_scroll: bool, device_count: usize, area: Rect, show_full: bool) {
    if !show_full {
        // Minimal footer - just show the toggle hint
        let minimal_line = Line::from(vec![
            Span::raw("  "),
            key_span("h"),
            Span::raw(" Toggle Footer"),
        ]);
        
        let block = Block::default()
            .borders(Borders::TOP)
            .border_style(Style::default().fg(Color::DarkGray));
        
        let paragraph = Paragraph::new(vec![minimal_line]).block(block);
        f.render_widget(paragraph, area);
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

    // Only show Tab/Shift+Tab if there are multiple devices
    if device_count > 1 {
        navigation_spans.push(Span::raw("  "));
        navigation_spans.push(key_span("Tab"));
        navigation_spans.push(Span::raw(" / "));
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
        Span::raw(" Toggle Plot  "),
        key_span("f"),
        Span::raw(" Toggle FFT View  "),
        key_span("h"),
        Span::raw(" Toggle Footer"),
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
            Style::default()
                .fg(Color::Red)
                .add_modifier(Modifier::BOLD),
        ),
        key_span("q"),
        Span::raw(" / "),
        key_span("Ctrl+C"),
        Span::raw(" Quit"),
    ]);

    let lines = vec![navigation_line, toggle_line, window_line, scroll_line, quit_line];

    let block = Block::default()
        .borders(Borders::TOP)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(Span::styled(
            " Controls ",
            Style::default().add_modifier(Modifier::BOLD),
        ));

    let paragraph = Paragraph::new(lines).block(block);
    f.render_widget(paragraph, area);
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
            spans.push(Span::raw(format!("  Serial: {}", d.serial_number)));
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

                    let (value_str, _) = fmt_value(&col.value);

                    let mut desc = col.desc.description.clone();
                    if desc.len() < app.desc_width {
                        desc.push_str(&" ".repeat(app.desc_width - desc.len()));
                    }

                    let is_selected = app.nav.idx == Some(nav_idx);
                    let name_style = if is_selected {
                        if app.show_detail {
                            base_style.add_modifier(Modifier::BOLD)
                        } else {
                            base_style.add_modifier(Modifier::BOLD | Modifier::RAPID_BLINK)
                        }
                    } else {
                        base_style
                    };

                    lines.push(Line::from(vec![
                        Span::styled(desc, name_style),
                        Span::raw("  "),
                        Span::styled(
                            value_str,
                            Style::default().add_modifier(Modifier::BOLD),
                        ),
                        Span::styled(
                            col.desc.units.clone(),
                            Style::default().add_modifier(Modifier::BOLD),
                        ),
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

    let current_scroll = app.scroll as usize;
    let bottom = current_scroll.saturating_add(view_h);

    // Only adjust if selection is outside the current viewport.
    if line >= current_scroll && line < bottom {
        return;
    }

    let half = view_h / 2;
    let target_top = line.saturating_sub(half);
    let max_top = total_lines.saturating_sub(view_h);
    let new_scroll = target_top.min(max_top);

    app.scroll = new_scroll as u16;
}

fn render_graphics_panel(f: &mut Frame, app: &App, area: Rect) {
    if let Some((desc, units, _cur_col, _total_cols)) = app.get_focused_channel_info() {
        let item = app.current_item().unwrap();

        if app.fft {
            // FFT/Spectral Density view (average-detrended)
            if let Some((sd_data, noise_floor)) = app.get_spectral_density_data() {
                let title = format!(
                    "{} — {} (DC detrend {:.1}s) | Mean: {:.3e} {}/√Hz",
                    item.route, desc, app.plot_window_seconds, noise_floor, units
                );

                let block = Block::default().title(title).borders(Borders::ALL);

                if !sd_data.is_empty() {
                    let log_data: Vec<(f64, f64)> = sd_data
                        .iter()
                        .filter_map(|(f, d)| {
                            if *f <= 0.0 || *d <= 0.0 {
                                return None;
                            }

                            let norm = d / noise_floor;
                            if !norm.is_finite() || norm <= 0.0 {
                                return None;
                            }

                            Some((f.log10(), norm.log10()))
                        })
                        .collect();

                    if log_data.is_empty() {
                        let inner = block.inner(area);
                        f.render_widget(block, area);
                        f.render_widget(Paragraph::new("No valid data for log scale"), inner);
                        return;
                    }

                    let min_freq_log = log_data.first().map(|(f, _)| *f).unwrap_or(0.0);
                    let max_freq_log = log_data.last().map(|(f, _)| *f).unwrap_or(1.0);

                    let densities: Vec<f64> = log_data.iter().map(|(_, d)| *d).collect();
                    let min_density_log =
                        densities.iter().fold(f64::INFINITY, |a, &b| a.min(b));
                    let max_density_log =
                        densities.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));

                    let y_range = max_density_log - min_density_log;
                    let y_padding = if y_range > 0.1 { y_range * 0.1 } else { 0.5 };

                    let dataset = Dataset::default()
                        .name(desc.as_str())
                        .marker(symbols::Marker::Braille)
                        .style(Style::default().fg(Color::Red))
                        .graph_type(GraphType::Line)
                        .data(&log_data);

                    let min_freq = 10f64.powf(min_freq_log);
                    let max_freq = 10f64.powf(max_freq_log);
                    let mid_freq = 10f64.powf((min_freq_log + max_freq_log) / 2.0);

                    let min_rel = 10f64.powf(min_density_log - y_padding);
                    let max_rel = 10f64.powf(max_density_log + y_padding);
                    let mid_rel = 10f64.powf((min_density_log + max_density_log) / 2.0);

                    let chart = Chart::new(vec![dataset])
                        .block(block)
                        .x_axis(
                            Axis::default()
                                .title("Frequency [Hz] (log)")
                                .style(Style::default().fg(Color::Gray))
                                .bounds([min_freq_log, max_freq_log])
                                .labels(vec![
                                    format!("{:.1e}", min_freq),
                                    format!("{:.1e}", mid_freq),
                                    format!("{:.1e}", max_freq),
                                ]),
                        )
                        .y_axis(
                            Axis::default()
                                .title(format!(
                                    "Value [{}/√Hz]",
                                    units
                                ))
                                .style(Style::default().fg(Color::Gray))
                                .bounds([min_density_log - y_padding, max_density_log + y_padding])
                                .labels(vec![
                                    format!("{:.1e}", min_rel),
                                    format!("{:.1e}", mid_rel),
                                    format!("{:.1e}", max_rel),
                                ]),
                        );

                    f.render_widget(chart, area);
                } else {
                    let inner = block.inner(area);
                    f.render_widget(block, area);
                    f.render_widget(Paragraph::new("No data available for FFT"), inner);
                }
            } else {
                let title = format!(
                    "{} — {} Spectral Density ({:.1}s)",
                    item.route, desc, app.plot_window_seconds
                );

                let block = Block::default().title(title).borders(Borders::ALL);
                let inner = block.inner(area);

                f.render_widget(block, area);
                f.render_widget(
                    Paragraph::new("Buffering data for FFT...\nNeed at least 256 samples"),
                    inner,
                );
            }
        } else {
            // Time-domain view
            let title = format!(
                "{} — {} ({:.1}s)",
                item.route, desc, app.plot_window_seconds
            );

            let block = Block::default().title(title).borders(Borders::ALL);

            if let Some((data, _current_value, _current_time)) = app.get_plot_data() {
                if !data.is_empty() {
                    let min_time = data.first().map(|(t, _)| *t).unwrap_or(0.0);
                    let max_time = data.last().map(|(t, _)| *t).unwrap_or(1.0);

                    let values: Vec<f64> = data.iter().map(|(_, v)| *v).collect();
                    let min_val = values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
                    let max_val =
                        values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));

                    let y_range = (max_val - min_val).abs();
                    let y_padding = if y_range > 1e-10 { y_range * 0.4 } else { 1.0 };

                    let y_min = min_val - y_padding;
                    let y_max = max_val + y_padding;

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
                                .style(Style::default().fg(Color::Gray))
                                .bounds([min_time, max_time])
                                .labels(vec![
                                    format!("{:.2}", min_time),
                                    format!("{:.2}", (min_time + max_time) / 2.0),
                                    format!("{:.2}", max_time),
                                ]),
                        )
                        .y_axis(
                            Axis::default()
                                .title(format!("Value [{}]", units))
                                .style(Style::default().fg(Color::Gray))
                                .bounds([y_min, y_max])
                                .labels(vec![
                                    format!("{:.3}", y_min),
                                    format!("{:.3}", (y_min + y_max) / 2.0),
                                    format!("{:.3}", y_max),
                                ]),
                        );

                    f.render_widget(chart, area);
                } else {
                    let inner = block.inner(area);
                    f.render_widget(block, area);
                    f.render_widget(Paragraph::new("No data available"), inner);
                }
            } else {
                let inner = block.inner(area);
                f.render_widget(block, area);
                f.render_widget(Paragraph::new("Buffering data..."), inner);
            }
        }
    } else {
        f.render_widget(
            Block::default()
                .title("Channel Detail [No column selected]")
                .borders(Borders::ALL),
            area,
        );
    }
}

fn main() {
    let cli = Cli::parse();

    let proxy = tio::proxy::Interface::new(&cli.tio.root);
    let parent_route: DeviceRoute = cli.tio.parse_route();

    let tree = match DeviceTree::open(&proxy, parent_route.clone()) {
        Ok(t) => t,
        Err(e) => {
            ratatui::restore();
            eprintln!("Failed to open device tree: {:?}", e);
            std::process::exit(1);
        }
    };

    let (event_tx, event_rx) = channel::unbounded();
    let buffer = Buffer::new(
        tree,
        event_tx,
        /*capacity*/ 100_000,
        /*forward_samples*/ true,
    );
    let buffer = Arc::new(RwLock::new(buffer));

    // Drain thread: blocks inside drain() on device I/O, wakes only when data arrives.
    {
        let buffer_clone = buffer.clone();
        std::thread::spawn(move || loop {
            if buffer_clone.write().unwrap().drain().is_err() {
                break;
            }
        });
    }

    let mut app = App::new(cli.all, &parent_route, buffer.clone());
    let mut terminal: Terminal<_> = ratatui::init();

    let (key_tx, key_rx) = channel::unbounded();
    std::thread::spawn(move || loop {
        match event::read() {
            Ok(ev) => {
                if key_tx.send(ev).is_err() {
                    break;
                }
            }
            Err(_) => break,
        }
    });

    let frame_duration = Duration::from_millis(1_000 / cli.fps as u64);
    let tick_rx = channel::tick(frame_duration);

    'main: loop {
        crossbeam::select! {
            recv(event_rx) -> msg => {
                match msg {
                    Ok(ev) => {
                        match ev {
                            BufferEvent::RouteDiscovered(route) => {
                                if app.all || route == app.parent_route {
                                    if !app.child_routes.iter().any(|r| r == &route) {
                                        app.child_routes.push(route);
                                        app.child_routes.sort();
                                    }
                                }
                            }
                            BufferEvent::Samples(samples) => {
                                let now = Instant::now();
                                for (sample, route) in samples {
                                    if !app.all && route != app.parent_route {
                                        continue;
                                    }

                                    let key = (route.clone(), sample.stream.stream_id);

                                    app.desc_width = app.desc_width.max(
                                        sample
                                            .columns
                                            .iter()
                                            .map(|c| c.desc.description.len())
                                            .max()
                                            .unwrap_or(0),
                                    );

                                    app.last.insert(key, (sample, now));

                                }
                            }
                            _ => {}
                        }
                    }
                    Err(_) => {
                        break 'main;
                    }
                }
            }

            // Keyboard input
            recv(key_rx) -> msg => {
                match msg {
                    Ok(ev) => {
                        if handle_input_event(ev, &mut app) {
                            break 'main;
                        }
                    }
                    Err(_) => break 'main,
                }
            }

            // Frame tick: redraw UI at target FPS
            recv(tick_rx) -> _ => {
                app.rebuild_nav_items();
                if draw_ui(&mut terminal, &mut app).is_err() {
                    break 'main;
                }
            }
        }
    }

    ratatui::restore();
}
