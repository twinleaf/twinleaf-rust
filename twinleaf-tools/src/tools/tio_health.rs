// tio health
//
// Live timing & rate diagnostics by device route.
// Uses DeviceTree for automatic metadata handling.
//
// Build: cargo run --release -- <tio-url> [route] [options]
// Quit:  q / Ctrl-C

use chrono::{DateTime, Local};
use crossbeam::channel;
use ratatui::{
    crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState},
    Terminal,
};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    io,
    time::{Duration, Instant, SystemTime},
};
use twinleaf::{
    data::BoundaryReason,
    device::{DeviceEvent, DeviceTree, TreeEvent, TreeItem},
    tio::{
        self,
        proto::{identifiers::StreamKey, DeviceRoute},
    },
};
use crate::HealthCli;

#[derive(Default)]
struct DeviceState {
    last_heartbeat: Option<Instant>,
    heartbeat_toggle: bool,
}

impl DeviceState {
    fn heartbeat_char(&self, now: Instant) -> char {
        let fresh = self
            .last_heartbeat
            .map(|t| now.duration_since(t) < Duration::from_millis(500))
            .unwrap_or(false);

        if !fresh {
            '♡' // No recent heartbeat
        } else if self.heartbeat_toggle {
            '♥' // Filled
        } else {
            '♡' // Empty
        }
    }

    fn on_heartbeat(&mut self, now: Instant) {
        self.last_heartbeat = Some(now);
        self.heartbeat_toggle = !self.heartbeat_toggle;
    }
}

struct TimeWindow {
    buf: Vec<f64>,
    cap: usize,
    idx: usize,
    filled: bool,
}

impl TimeWindow {
    fn new(seconds: u64, hz_guess: f64) -> Self {
        let cap = ((seconds as f64 * hz_guess).round() as usize).max(16);
        Self {
            buf: vec![0.0; cap],
            cap,
            idx: 0,
            filled: false,
        }
    }

    fn push(&mut self, v: f64) {
        self.buf[self.idx] = v;
        self.idx = (self.idx + 1) % self.cap;
        if self.idx == 0 {
            self.filled = true;
        }
    }

    fn std_ms(&self) -> f64 {
        let n = if self.filled { self.cap } else { self.idx };
        if n == 0 {
            return 0.0;
        }
        let mean: f64 = self.buf[..n].iter().sum::<f64>() / (n as f64);
        let var: f64 = self.buf[..n]
            .iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>()
            / (n as f64);
        var.sqrt()
    }
}

struct OnlineSlope {
    n: u64,
    sum_x: f64,
    sum_y: f64,
    sum_xx: f64,
    sum_xy: f64,
    x0: f64,
    y0: f64,
}

impl Default for OnlineSlope {
    fn default() -> Self {
        Self {
            n: 0,
            sum_x: 0.0,
            sum_y: 0.0,
            sum_xx: 0.0,
            sum_xy: 0.0,
            x0: 0.0,
            y0: 0.0,
        }
    }
}

impl OnlineSlope {
    fn push(&mut self, x: f64, y: f64) {
        if self.n == 0 {
            self.x0 = x;
            self.y0 = y;
        }
        let dx = x - self.x0;
        let dy = y - self.y0;
        self.n += 1;
        self.sum_x += dx;
        self.sum_y += dy;
        self.sum_xx += dx * dx;
        self.sum_xy += dx * dy;
    }

    fn slope(&self) -> Option<f64> {
        if self.n < 2 {
            return None;
        }
        let denom = self.n as f64 * self.sum_xx - self.sum_x * self.sum_x;
        if denom.abs() < f64::EPSILON {
            return None;
        }
        Some((self.n as f64 * self.sum_xy - self.sum_x * self.sum_y) / denom)
    }

    fn reset(&mut self) {
        *self = Self::default();
    }
}

const MIN_DRIFT_SAMPLES: u64 = 50;

#[derive(Default)]
struct StreamStats {
    host_epoch: Option<Instant>,

    drift_slope: OnlineSlope,
    drift_s: f64,
    ppm: f64,

    last_host: Option<Instant>,
    last_data: Option<f64>,
    jitter_ms: f64,
    jitter_window: Option<TimeWindow>,

    last_n: Option<u32>,
    samples_dropped: u64,
    current_session_id: Option<u32>,

    rate_slope: OnlineSlope,
    received_count: u64,
    rate_smps: f64,

    name: String,
    last_seen: Option<Instant>,
}

impl StreamStats {
    fn on_sample(&mut self, sample_n: u32, t_data: f64, now: Instant, jitter_window_s: u64) {
        if self.host_epoch.is_none() {
            self.host_epoch = Some(now);
        }
        let host_time = now.duration_since(self.host_epoch.unwrap()).as_secs_f64();

        // Jitter (unchanged)
        if self.jitter_window.is_none() {
            self.jitter_window = Some(TimeWindow::new(jitter_window_s, 100.0));
        }
        if let (Some(lh), Some(ld)) = (self.last_host, self.last_data) {
            let dh = now.duration_since(lh).as_secs_f64();
            let dd = t_data - ld;
            if let Some(w) = &mut self.jitter_window {
                w.push((dd - dh) * 1000.0);
                self.jitter_ms = w.std_ms();
            }
        }
        self.last_host = Some(now);
        self.last_data = Some(t_data);

        // Drift / PPM via incremental OLS
        self.drift_slope.push(host_time, t_data);
        if self.drift_slope.n >= MIN_DRIFT_SAMPLES {
            if let Some(beta) = self.drift_slope.slope() {
                let host_elapsed = host_time - self.drift_slope.x0;
                self.drift_s = (beta - 1.0) * host_elapsed;
                self.ppm = (beta - 1.0) * 1e6;
            }
        }

        // Rate via incremental OLS
        self.received_count += 1;
        self.rate_slope.push(host_time, self.received_count as f64);
        if let Some(slope) = self.rate_slope.slope() {
            self.rate_smps = slope;
        }

        self.last_n = Some(sample_n);
    }

    fn reset_timing(&mut self) {
        self.drift_slope.reset();
        self.drift_s = 0.0;
        self.ppm = 0.0;
        self.last_host = None;
        self.last_data = None;
        self.jitter_ms = 0.0;
        self.jitter_window = None;
        self.last_n = None;
    }

    fn reset_for_new_session(&mut self, session_id: u32) {
        self.reset_timing();
        self.samples_dropped = 0;
        self.current_session_id = Some(session_id);
    }

    fn is_stale(&self, now: Instant, stale_dur: Duration) -> bool {
        self.last_seen
            .map(|t| now.duration_since(t) > stale_dur)
            .unwrap_or(true)
    }
}

#[derive(Clone)]
struct LoggedEvent {
    timestamp: SystemTime,
    event: String,
    color: Color,
}

fn log_event(log: &mut VecDeque<LoggedEvent>, msg: String, color: Color, cap: usize) {
    log.push_front(LoggedEvent {
        timestamp: SystemTime::now(),
        event: msg,
        color,
    });
    if log.len() > cap {
        log.pop_back();
    }
}

fn handle_boundary(
    reason: &BoundaryReason,
    route: &DeviceRoute,
    stream_name: &str,
    st: &mut StreamStats,
    event_log: &mut VecDeque<LoggedEvent>,
    cap: usize,
) {
    match reason {
        BoundaryReason::Initial => {
            log_event(
                event_log,
                format!("[{}/{}] STREAM STARTED", route, stream_name),
                Color::Green,
                cap,
            );
        }
        BoundaryReason::SessionChanged { old, new } => {
            log_event(
                event_log,
                format!("[{}/{}] SESSION: {} → {}", route, stream_name, old, new),
                Color::Green,
                cap,
            );
            st.reset_for_new_session(*new);
        }
        BoundaryReason::SamplesLost { expected, received } => {
            let count = received.wrapping_sub(*expected);
            log_event(
                event_log,
                format!("[{}/{}] DROPPED: {} samples", route, stream_name, count),
                Color::Red,
                cap,
            );
            st.samples_dropped += count as u64;
        }
        BoundaryReason::TimeBackward { gap_seconds } => {
            log_event(
                event_log,
                format!(
                    "[{}/{}] TIME BACKWARD: {:.3}s",
                    route, stream_name, gap_seconds
                ),
                Color::Yellow,
                cap,
            );
        }
        BoundaryReason::RateChanged { old_rate, new_rate } => {
            log_event(
                event_log,
                format!(
                    "[{}/{}] RATE: {:.1} → {:.1} Hz",
                    route, stream_name, old_rate, new_rate
                ),
                Color::Yellow,
                cap,
            );
            st.reset_timing();
            st.rate_slope.reset();
            st.received_count = 0;
            st.rate_smps = 0.0;
        }
        BoundaryReason::TimeRefSessionChanged { old, new } => {
            log_event(
                event_log,
                format!("[{}/{}] TIME REF: {} → {}", route, stream_name, old, new),
                Color::Yellow,
                cap,
            );
            st.reset_timing();
        }
        BoundaryReason::SegmentRollover { old_id, new_id } => {
            log_event(
                event_log,
                format!(
                    "[{}/{}] SEGMENT: {} → {}",
                    route, stream_name, old_id, new_id
                ),
                Color::Green,
                cap,
            );
        }
        BoundaryReason::SegmentChanged { old_id, new_id } => {
            log_event(
                event_log,
                format!(
                    "[{}/{}] SEGMENT CHANGED: {} → {}",
                    route, stream_name, old_id, new_id
                ),
                Color::Yellow,
                cap,
            );
            st.reset_timing();
        }
    }
}

struct DisplayRow {
    route: String,
    stream_id: u8,
    name: String,
    rate_smps: f64,
    drift_s: f64,
    ppm: f64,
    jitter_ms: f64,
    samples_dropped: u64,
    last_n: Option<u32>,
    last_data: Option<f64>,
    elapsed_time: Option<f64>,
    status: &'static str,
    color: Color,
}

impl DisplayRow {
    fn from_stats(
        route: String,
        stream_id: u8,
        st: &StreamStats,
        now: Instant,
        stale_dur: Duration,
        ppm_warn: f64,
        ppm_err: f64,
    ) -> Self {
        let stale = st.is_stale(now, stale_dur);
        let (color, status) = if stale {
            (Color::DarkGray, "STALLED")
        } else if st.ppm.abs() >= ppm_err {
            (Color::Red, "ERROR")
        } else if st.ppm.abs() >= ppm_warn {
            (Color::Yellow, "WARN")
        } else {
            (Color::Green, "OK")
        };

        let elapsed_time = st
            .host_epoch
            .map(|epoch| now.duration_since(epoch).as_secs_f64());

        DisplayRow {
            route,
            stream_id,
            name: st.name.clone(),
            rate_smps: st.rate_smps,
            drift_s: st.drift_s,
            ppm: st.ppm,
            jitter_ms: st.jitter_ms,
            samples_dropped: st.samples_dropped,
            last_n: st.last_n,
            last_data: st.last_data,
            elapsed_time,
            status,
            color,
        }
    }

    fn to_table_row(&self, show_ppm: bool, show_sample_time: bool) -> Row<'static> {
        let style = Style::default().fg(self.color);
        let drift_cell = if show_ppm {
            Cell::from(format!("{:.2}", self.ppm))
        } else {
            Cell::from(format!("{:.4}", self.drift_s))
        };
        let time_cell = if show_sample_time {
            Cell::from(format!("{:.3}", self.last_data.unwrap_or(0.0)))
        } else {
            Cell::from(format!("{:.1}", self.elapsed_time.unwrap_or(0.0)))
        };
        Row::new(vec![
            Cell::from(self.route.clone()).style(style),
            Cell::from(format!("{}", self.stream_id)).style(style),
            Cell::from(self.name.clone()).style(style),
            Cell::from(format!("{:.1}", self.rate_smps)).style(style),
            drift_cell.style(style),
            Cell::from(format!("{:.2}", self.jitter_ms)).style(style),
            Cell::from(format!("{}", self.samples_dropped)).style(style),
            Cell::from(format!("{}", self.last_n.unwrap_or(0))).style(style),
            time_cell.style(style),
            Cell::from(self.status).style(style),
        ])
    }
}

fn draw_ui(
    terminal: &mut Terminal<ratatui::backend::CrosstermBackend<io::Stdout>>,
    stats: &mut BTreeMap<StreamKey, StreamStats>,
    device_states: &HashMap<DeviceRoute, DeviceState>,
    event_log: &VecDeque<LoggedEvent>,
    event_scroll_offset: usize,
    show_heartbeat: bool,
    show_ppm: bool,
    show_sample_time: bool,
    cli: &HealthCli,
) -> io::Result<()> {
    let now = Instant::now();
    let stale_dur = cli.stale_dur();

    let mut rows: Vec<DisplayRow> = stats
        .iter_mut()
        .map(|(key, st)| {
            if st.is_stale(now, stale_dur) && st.drift_slope.n > 0 {
                st.reset_timing();
                st.rate_slope.reset();
                st.received_count = 0;
                st.rate_smps = 0.0;
                st.host_epoch = None;
            }
            DisplayRow::from_stats(
                key.route.to_string(),
                key.stream_id,
                st,
                now,
                stale_dur,
                cli.ppm_warn,
                cli.ppm_err,
            )
        })
        .collect();

    rows.sort_by(|a, b| a.route.cmp(&b.route).then(a.stream_id.cmp(&b.stream_id)));

    let mut heartbeat_entries: Vec<_> = device_states
        .iter()
        .map(|(route, state)| (route.to_string(), state.heartbeat_char(now)))
        .collect();
    heartbeat_entries.sort_by(|a, b| a.0.cmp(&b.0));
    let heartbeat_display: String = heartbeat_entries
        .iter()
        .map(|(route, ch)| format!("{}: {}", route, ch))
        .collect::<Vec<_>>()
        .join("  ");

    terminal.draw(|f| {
        let size = f.area();
        let event_block_height = if event_log.is_empty() {
            0
        } else {
            cli.event_display_lines + 2
        };
        let footer_height = if cli.quiet { 0 } else { 1 };
        let heartbeat_height = if show_heartbeat { 1 } else { 0 };

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Length(heartbeat_height),
                Constraint::Min(10),
                Constraint::Length(event_block_height),
                Constraint::Length(footer_height),
            ])
            .split(size);

        // Header
        let header_text = format!(
            "tio health — jitter={}s  warn/err={}/{}ppm  fps={}  stale={}ms",
            cli.jitter_window, cli.ppm_warn, cli.ppm_err, cli.fps, cli.stale_ms
        );
        f.render_widget(
            Paragraph::new(header_text).style(Style::default().add_modifier(Modifier::BOLD)),
            chunks[0],
        );

        // Heartbeat (conditional)
        if show_heartbeat {
            f.render_widget(
                Paragraph::new(heartbeat_display.clone()).style(Style::default().fg(Color::Cyan)),
                chunks[1],
            );
        }

        // Table
        let drift_header = if show_ppm { "ppm" } else { "drift(s)" };
        let time_header = if show_sample_time {
            "sample_time"
        } else {
            "elapsed(s)"
        };

        let header_cells = [
            "route",
            "sid",
            "stream",
            "smps/s",
            drift_header,
            "jitter(ms)",
            "dropped",
            "last_n",
            time_header,
            "status",
        ]
        .into_iter()
        .map(|h| Cell::from(h).style(Style::default().add_modifier(Modifier::BOLD)));

        let widths = [
            Constraint::Length(10),
            Constraint::Length(4),
            Constraint::Length(20),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Length(11),
            Constraint::Length(8),
            Constraint::Length(10),
            Constraint::Length(12),
            Constraint::Length(8),
        ];

        let table = Table::new(
            rows.iter()
                .map(|r| r.to_table_row(show_ppm, show_sample_time))
                .collect::<Vec<_>>(),
            widths,
        )
        .header(Row::new(header_cells).height(1))
        .column_spacing(2);

        f.render_stateful_widget(table, chunks[2], &mut TableState::default());

        // Event log
        if !event_log.is_empty() {
            let events_to_show: Vec<&LoggedEvent> = event_log
                .iter()
                .filter(|e| !cli.warnings_only || matches!(e.color, Color::Red | Color::Yellow))
                .collect();

            let total = events_to_show.len();
            let display_count = cli.event_display_lines as usize;
            let start = event_scroll_offset.min(total.saturating_sub(1));
            let end = (start + display_count).min(total);

            let visible: Vec<Line> = events_to_show[start..end]
                .iter()
                .map(|e| {
                    let dt: DateTime<Local> = e.timestamp.into();
                    Line::from(vec![
                        Span::styled(
                            format!("[{}] ", dt.format("%H:%M:%S%.3f")),
                            Style::default().fg(e.color),
                        ),
                        Span::styled(e.event.clone(), Style::default().fg(e.color)),
                    ])
                })
                .collect();

            let title = if total > display_count {
                format!("Events [{}-{}/{}] (↑/↓)", start + 1, end, total)
            } else {
                "Events".to_string()
            };

            f.render_widget(
                Paragraph::new(visible).block(
                    Block::default()
                        .title(title)
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(Color::Gray)),
                ),
                chunks[3],
            );
        }

        // Footer
        if !cli.quiet {
            let heartbeat_hint = if show_heartbeat {
                "h:hide heartbeat"
            } else {
                "h:show heartbeat"
            };
            let drift_hint = if show_ppm { "p:drift" } else { "p:ppm" };
            let time_hint = if show_sample_time {
                "s:elapsed"
            } else {
                "s:sample"
            };
            f.render_widget(
                Paragraph::new(format!(
                    "q/Ctrl+C to quit  |  {}  {}  {}  |  ↑/↓/PgUp/PgDn to scroll",
                    heartbeat_hint, drift_hint, time_hint
                ))
                .style(Style::default().fg(Color::Gray)),
                chunks[4],
            );
        }
    })?;
    Ok(())
}

/// Message type for communication from data thread to main thread
enum DataMsg {
    Item(TreeItem),
    Error(String),
}

pub fn run_health(health_cli: HealthCli) -> Result<(), ()> {
    let mut terminal = ratatui::init();

    let proxy = tio::proxy::Interface::new(&health_cli.tio.root);
    let root_route = health_cli.tio.parse_route();

    let tree = match DeviceTree::open(&proxy, root_route) {
        Ok(t) => t,
        Err(e) => {
            ratatui::restore();
            eprintln!("Failed to open device tree: {:?}", e);
            std::process::exit(1);
        }
    };

    let (data_tx, data_rx) = channel::unbounded();

    std::thread::spawn(move || {
        let mut tree = tree;

        // Main loop - blocking on next_item()
        loop {
            match tree.next_item() {
                Ok(item) => {
                    if data_tx.send(DataMsg::Item(item)).is_err() {
                        return;
                    }
                }
                Err(e) => {
                    let _ = data_tx.send(DataMsg::Error(format!("{:?}", e)));
                    return;
                }
            }
        }
    });

    // Key reading thread
    let (key_tx, key_rx) = channel::unbounded();
    std::thread::spawn(move || loop {
        if let Ok(ev) = event::read() {
            if key_tx.send(ev).is_err() {
                break;
            }
        }
    });

    let mut stats: BTreeMap<StreamKey, StreamStats> = BTreeMap::new();
    let mut device_states: HashMap<DeviceRoute, DeviceState> = HashMap::new();
    let mut event_log: VecDeque<LoggedEvent> = VecDeque::new();
    let mut event_scroll_offset: usize = 0;
    let mut show_heartbeat: bool = false;
    let mut show_ppm: bool = true;
    let mut show_sample_time: bool = true;

    let streams_filter = health_cli.streams.clone();
    let jitter_window_s = health_cli.jitter_window;
    let event_log_cap = health_cli.event_log_size as usize;

    let ui_tick = channel::tick(Duration::from_millis(1000 / health_cli.fps));

    'main: loop {
        let mut needs_redraw = false;

        crossbeam::select! {
            recv(data_rx) -> msg => {
                let now = Instant::now();

                match msg {
                    Ok(DataMsg::Item(TreeItem::Sample(sample, route))) => {
                        let sid = sample.stream.stream_id;

                        if let Some(filter) = &streams_filter {
                            if !filter.contains(&sid) { continue; }
                        }

                        let key = StreamKey::new(route.clone(), sid);
                        let st = stats.entry(key).or_insert_with(|| StreamStats {
                            name: sample.stream.name.clone(),
                            current_session_id: Some(sample.device.session_id),
                            ..Default::default()
                        });

                        st.name = sample.stream.name.clone();

                        if let Some(boundary) = &sample.boundary {
                            handle_boundary(
                                &boundary.reason,
                                &route,
                                &sample.stream.name,
                                st,
                                &mut event_log,
                                event_log_cap,
                            );
                            needs_redraw = true;
                        }

                        if st.last_n.map(|n| sample.n != n).unwrap_or(true) {
                            st.last_seen = Some(now);
                        }

                        st.on_sample(sample.n, sample.timestamp_end(), now, jitter_window_s);
                    }

                    Ok(DataMsg::Item(TreeItem::Event(event))) => {
                        match event {
                            TreeEvent::RouteDiscovered(route) => {
                                device_states.entry(route.clone()).or_default();
                                log_event(
                                    &mut event_log,
                                    format!("[{}] ROUTE DISCOVERED", route),
                                    Color::Green,
                                    event_log_cap,
                                );
                                needs_redraw = true;
                            }
                            TreeEvent::Device { route, event: DeviceEvent::Heartbeat { .. } } => {
                                device_states.entry(route).or_default().on_heartbeat(now);
                                // Heartbeat visual update handled by tick
                            }
                            TreeEvent::Device { route, event: DeviceEvent::Status(status) } => {
                                log_event(
                                    &mut event_log,
                                    format!("[{}] STATUS: {:?}", route, status),
                                    Color::Yellow,
                                    event_log_cap,
                                );
                                // On disconnect, reset all timing state for streams on this route
                                // so that rate/drift don't carry stale data into the next session.
                                if matches!(status, tio::proto::ProxyStatus::SensorDisconnected) {
                                    for (key, st) in stats.iter_mut() {
                                        if key.route == route {
                                            st.reset_timing();
                                            st.rate_slope.reset();
                                            st.received_count = 0;
                                            st.rate_smps = 0.0;
                                            st.host_epoch = None;
                                        }
                                    }
                                }
                                needs_redraw = true;
                            }
                            TreeEvent::Device { route, event: DeviceEvent::RpcInvalidated(method) } => {
                                log_event(
                                    &mut event_log,
                                    format!("[{}] RPC INVALIDATED: {:?}", route, method),
                                    Color::Cyan,
                                    event_log_cap,
                                );
                                needs_redraw = true;
                            }
                            TreeEvent::Device { route, event: DeviceEvent::MetadataReady(metadata) } => {
                                log_event(
                                    &mut event_log,
                                    format!("[{}] METADATA READY: {}", route, metadata.device.name),
                                    Color::Green,
                                    event_log_cap,
                                );
                                needs_redraw = true;
                            }
                            TreeEvent::Device { route, event: DeviceEvent::NewHash(hash) } => {
                                log_event(
                                    &mut event_log,
                                    format!("[{}] NEW HASH: {:?}", route, hash),
                                    Color::Green,
                                    event_log_cap,
                                );
                                needs_redraw = true;
                            }
                        }
                    }

                    Ok(DataMsg::Error(e)) => {
                        log_event(
                            &mut event_log,
                            format!("DATA ERROR: {}", e),
                            Color::Red,
                            event_log_cap,
                        );
                        break 'main;
                    }

                    Err(_) => {
                        // Channel closed
                        break 'main;
                    }
                }
            }

            recv(key_rx) -> ev => {
                if let Ok(Event::Key(k)) = ev {
                    if k.kind != KeyEventKind::Press { continue; }

                    if matches!(k.code, KeyCode::Char('q'))
                        || (k.code == KeyCode::Char('c') && k.modifiers == KeyModifiers::CONTROL)
                    {
                        break 'main;
                    }

                    let events_to_show: Vec<&LoggedEvent> = event_log
                        .iter()
                        .filter(|e| !health_cli.warnings_only || matches!(e.color, Color::Red | Color::Yellow))
                        .collect();
                    let total = events_to_show.len();
                    let display_count = health_cli.event_display_lines as usize;

                    match k.code {
                        KeyCode::Char('h') => {
                            show_heartbeat = !show_heartbeat;
                            needs_redraw = true;
                        }
                        KeyCode::Char('p') => {
                            show_ppm = !show_ppm;
                            needs_redraw = true;
                        }
                        KeyCode::Char('s') => {
                            show_sample_time = !show_sample_time;
                            needs_redraw = true;
                        }
                        KeyCode::Up => {
                            event_scroll_offset = event_scroll_offset.saturating_sub(1);
                            needs_redraw = true;
                        }
                        KeyCode::Down => {
                            if total > display_count {
                                event_scroll_offset = (event_scroll_offset + 1)
                                    .min(total.saturating_sub(display_count));
                            }
                            needs_redraw = true;
                        }
                        KeyCode::PageUp => {
                            event_scroll_offset = event_scroll_offset.saturating_sub(display_count);
                            needs_redraw = true;
                        }
                        KeyCode::PageDown => {
                            if total > display_count {
                                event_scroll_offset = (event_scroll_offset + display_count)
                                    .min(total.saturating_sub(display_count));
                            }
                            needs_redraw = true;
                        }
                        KeyCode::Home => {
                            event_scroll_offset = 0;
                            needs_redraw = true;
                        }
                        KeyCode::End => {
                            if total > display_count {
                                event_scroll_offset = total.saturating_sub(display_count);
                            }
                            needs_redraw = true;
                        }
                        _ => {}
                    }
                }
            }

            recv(ui_tick) -> _ => {
                needs_redraw = true; // Periodic refresh for heartbeat animation and stats display
            }
        }

        if needs_redraw {
            if draw_ui(
                &mut terminal,
                &mut stats,
                &device_states,
                &event_log,
                event_scroll_offset,
                show_heartbeat,
                show_ppm,
                show_sample_time,
                &health_cli,
            )
            .is_err()
            {
                break 'main;
            }
        }
    }

    ratatui::restore();
    Ok(())
}
