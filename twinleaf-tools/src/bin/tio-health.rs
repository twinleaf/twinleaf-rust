// tio-health
//
// Live timing & rate diagnostics by device route.
// Uses DeviceTree for automatic metadata handling.
//
// Build: cargo run --release -- <tio-url> [route] [options]
// Quit:  q / Esc / Ctrl-C

use chrono::{DateTime, Local};
use clap::Parser;
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
use twinleaf_tools::HealthCli;

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

#[derive(Default)]
struct StreamStats {
    t0_host: Option<Instant>,
    t0_data: Option<f64>,
    last_host: Option<Instant>,
    last_data: Option<f64>,
    drift_s: f64,
    ppm: f64,
    jitter_ms: f64,
    jitter_window: Option<TimeWindow>,

    last_n: Option<u32>,
    samples_dropped: u64,
    current_session_id: Option<u32>,

    arrivals: VecDeque<Instant>,
    rate_smps: f64,

    name: String,
    last_seen: Option<Instant>,
}

impl StreamStats {
    fn on_sample(&mut self, sample_n: u32, t_data: f64, now: Instant, jitter_window_s: u64) {
        if self.t0_host.is_none() {
            self.t0_host = Some(now);
        }
        if self.t0_data.is_none() {
            self.t0_data = Some(t_data);
        }

        // Jitter
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

        if let (Some(h0), Some(d0)) = (self.t0_host, self.t0_data) {
            let host_elapsed = now.duration_since(h0).as_secs_f64();
            let data_elapsed = t_data - d0;
            self.drift_s = data_elapsed - host_elapsed;
            // Only calculate PPM after 0.5 seconds to avoid startup noise
            if host_elapsed >= 0.5 {
                self.ppm = 1e6 * self.drift_s / host_elapsed;
            }
        }

        self.last_n = Some(sample_n);
        self.arrivals.push_back(now);
    }

    fn compute_rate(&mut self, now: Instant, window: Duration) {
        let cutoff = now.checked_sub(window).unwrap_or(now);
        while self.arrivals.front().map(|&t| t < cutoff).unwrap_or(false) {
            self.arrivals.pop_front();
        }

        self.rate_smps = match (self.arrivals.front(), self.arrivals.back()) {
            (Some(from), Some(to)) if from < to => {
                (self.arrivals.len() - 1) as f64 / to.duration_since(*from).as_secs_f64()
            }
            _ => 0.0,
        };
    }

    fn reset_timing(&mut self) {
        self.t0_host = None;
        self.t0_data = None;
        self.last_host = None;
        self.last_data = None;
        self.drift_s = 0.0;
        self.ppm = 0.0;
        self.jitter_ms = 0.0;
        self.jitter_window = None;
        self.last_n = None;
        self.arrivals.clear();
    }

    fn reset_for_new_session(&mut self, session_id: u32) {
        self.reset_timing();
        self.samples_dropped = 0;
        self.current_session_id = Some(session_id);
        self.arrivals.clear();
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
            status,
            color,
        }
    }

    fn to_table_row(&self) -> Row<'static> {
        let style = Style::default().fg(self.color);
        Row::new(vec![
            Cell::from(self.route.clone()).style(style),
            Cell::from(format!("{}", self.stream_id)).style(style),
            Cell::from(self.name.clone()).style(style),
            Cell::from(format!("{:.1}", self.rate_smps)).style(style),
            Cell::from(format!("{:.3}", self.drift_s)).style(style),
            Cell::from(format!("{:.0}", self.ppm)).style(style),
            Cell::from(format!("{:.2}", self.jitter_ms)).style(style),
            Cell::from(format!("{}", self.samples_dropped)).style(style),
            Cell::from(format!("{}", self.last_n.unwrap_or(0))).style(style),
            Cell::from(format!("{:.3}", self.last_data.unwrap_or(0.0))).style(style),
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
    cli: &HealthCli,
) -> io::Result<()> {
    let now = Instant::now();
    let rate_window = cli.rate_window_dur();
    let stale_dur = cli.stale_dur();

    let mut rows: Vec<DisplayRow> = stats
        .iter_mut()
        .map(|(key, st)| {
            st.compute_rate(now, rate_window);
            if st.is_stale(now, stale_dur) && st.t0_host.is_some() {
                st.reset_timing();
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
            "tio-health — rate={}s  jitter={}s  warn/err={}/{}ppm  fps={}  stale={}ms",
            cli.rate_window, cli.jitter_window, cli.ppm_warn, cli.ppm_err, cli.fps, cli.stale_ms
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
        let header_cells = [
            "route",
            "sid",
            "stream",
            "smps/s",
            "drift(s)",
            "ppm",
            "jitter(ms)",
            "dropped",
            "last_n",
            "last_time",
            "status",
        ]
        .iter()
        .map(|h| Cell::from(*h).style(Style::default().add_modifier(Modifier::BOLD)));

        let widths = [
            Constraint::Length(10),
            Constraint::Length(4),
            Constraint::Length(20),
            Constraint::Length(9),
            Constraint::Length(9),
            Constraint::Length(8),
            Constraint::Length(11),
            Constraint::Length(8),
            Constraint::Length(10),
            Constraint::Length(12),
            Constraint::Length(8),
        ];

        let table = Table::new(
            rows.iter().map(|r| r.to_table_row()).collect::<Vec<_>>(),
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
            f.render_widget(
                Paragraph::new(format!(
                    "q/Esc to quit  |  {}  |  ↑/↓/PgUp/PgDn to scroll",
                    heartbeat_hint
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

fn main() {
    let cli = HealthCli::parse();
    let mut terminal = ratatui::init();

    let proxy = tio::proxy::Interface::new(&cli.tio.root);
    let root_route = cli.tio.parse_route();

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

        // Warmup period
        let warmup_end = Instant::now() + Duration::from_millis(500);
        while Instant::now() < warmup_end {
            match tree.try_next_item() {
                Ok(Some(item)) => {
                    if data_tx.send(DataMsg::Item(item)).is_err() {
                        return;
                    }
                }
                Ok(None) => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(e) => {
                    let _ = data_tx.send(DataMsg::Error(format!("{:?}", e)));
                    return;
                }
            }
        }

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

    let streams_filter = cli.streams.clone();
    let jitter_window_s = cli.jitter_window;
    let event_log_cap = cli.event_log_size as usize;

    let ui_tick = channel::tick(Duration::from_millis(1000 / cli.fps));

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
                            TreeEvent::Device { route: _, event: DeviceEvent::NewHash(_) } => {
                                ();
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

                    if matches!(k.code, KeyCode::Char('q') | KeyCode::Esc)
                        || (k.code == KeyCode::Char('c') && k.modifiers == KeyModifiers::CONTROL)
                    {
                        break 'main;
                    }

                    let events_to_show: Vec<&LoggedEvent> = event_log
                        .iter()
                        .filter(|e| !cli.warnings_only || matches!(e.color, Color::Red | Color::Yellow))
                        .collect();
                    let total = events_to_show.len();
                    let display_count = cli.event_display_lines as usize;

                    match k.code {
                        KeyCode::Char('h') => {
                            show_heartbeat = !show_heartbeat;
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
                &cli,
            )
            .is_err()
            {
                break 'main;
            }
        }
    }

    ratatui::restore();
}
