// tio-health
//
// Live timing & rate diagnostics by device route.
//
// Build: cargo run --release -- <tio-url> [route] [options]
// Quit:  q / Esc / Ctrl-C

use chrono::{DateTime, Local};
use clap::Parser;
use crossbeam::channel;
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState};
use ratatui::Terminal;
use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::num::ParseFloatError;
use std::time::{Duration, Instant, SystemTime};
use twinleaf::device::{Buffer, BufferEvent};
use twinleaf::device::{DeviceTree, StreamKey};
use twinleaf::tio;
use twinleaf_tools::TioOpts;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "tio-health",
    version,
    about = "Live timing & rate diagnostics for TIO (Twinleaf) devices"
)]
struct Cli {
    #[command(flatten)]
    tio: TioOpts,

    /// Time window in seconds for calculating sample rate
    #[arg(
        long = "rate-window",
        default_value = "5",
        value_name = "SECONDS",
        value_parser = clap::value_parser!(u64).range(1..),
        help = "Seconds for rate calculation window (>= 1)"
    )]
    rate_window: u64,

    /// Time window in seconds for calculating jitter statistics
    #[arg(
        long = "jitter-window",
        default_value = "10",
        value_name = "SECONDS",
        value_parser = clap::value_parser!(u64).range(1..),
        help = "Seconds for jitter calculation window (>= 1)"
    )]
    jitter_window: u64,

    /// PPM threshold for yellow warning indicators
    #[arg(
        long = "ppm-warn",
        default_value = "100",
        value_name = "PPM",
        value_parser = nonneg_f64,
        help = "Warning threshold in parts per million (>= 0)"
    )]
    ppm_warn: f64,

    /// PPM threshold for red error indicators
    #[arg(
        long = "ppm-err",
        default_value = "200",
        value_name = "PPM",
        value_parser = nonneg_f64,
        help = "Error threshold in parts per million (>= 0)"
    )]
    ppm_err: f64,

    /// Filter to only show specific stream IDs (comma-separated)
    #[arg(
        long = "streams",
        value_delimiter = ',',
        value_name = "IDS",
        value_parser = clap::value_parser!(u8),
        help = "Comma-separated stream IDs to monitor (e.g., 0,1,5)"
    )]
    streams: Option<Vec<u8>>,

    /// Suppress the footer help text ("q/Esc to quit")
    #[arg(short = 'q', long = "quiet", help = "Suppress footer hint")]
    quiet: bool,

    /// UI refresh rate in frames per second
    #[arg(
        long = "fps",
        default_value = "10",
        value_name = "FPS",
        value_parser = clap::value_parser!(u64).range(1..=1000),
        help = "UI refresh rate (frames per second, 1–1000)"
    )]
    fps: u64,

    /// Time in milliseconds before marking a stream as stale
    #[arg(
        long = "stale-ms",
        default_value = "2000",
        value_name = "MS",
        value_parser = clap::value_parser!(u64).range(1..),
        help = "Mark streams as stale after this many milliseconds without data (>= 1)"
    )]
    stale_ms: u64,

    /// Maximum number of events to keep in the event log
    #[arg(
        short = 'n',
        long = "event-log-size",
        default_value = "100",
        value_name = "N",
        value_parser = clap::value_parser!(u64).range(1..),
        help = "Maximum number of events to keep in history (>= 1)"
    )]
    event_log_size: u64,

    /// Number of event lines to display on screen
    #[arg(
        long = "event-display-lines",
        default_value = "8",
        value_name = "LINES",
        value_parser = clap::value_parser!(u16).range(3..),
        help = "Number of event lines to show (>= 3)"
    )]
    event_display_lines: u16,

    /// Only show warning and error events in the log (yellow and red)
    #[arg(
        short = 'w',
        long = "warnings-only",
        help = "Show only red and yellow events in the log"
    )]
    warnings_only: bool,
}

impl Cli {
    #[inline]
    fn rate_window_dur(&self) -> Duration {
        Duration::from_secs(self.rate_window)
    }
    #[inline]
    fn jitter_window_s(&self) -> u64 {
        self.jitter_window
    }
    #[inline]
    fn stale_dur(&self) -> Duration {
        Duration::from_millis(self.stale_ms)
    }
}

fn nonneg_f64(s: &str) -> Result<f64, String> {
    let v: f64 = s.parse().map_err(|e: ParseFloatError| e.to_string())?;
    if v < 0.0 {
        Err("must be \u{2265} 0".into())
    } else {
        Ok(v)
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
            .map(|x| (x - mean) * (x - mean))
            .sum::<f64>()
            / (n as f64);
        var.sqrt()
    }
}

#[derive(Default)]
struct StreamStats {
    // Timing
    t0_host: Option<Instant>,
    t0_data: Option<f64>,
    last_host: Option<Instant>,
    last_data: Option<f64>,
    drift_s: f64,
    ppm: f64,
    jitter_ms: f64,
    jitter_window: Option<TimeWindow>,

    // Sample tracking
    last_n: Option<u32>,
    samples_dropped: u64,
    current_session_id: Option<u32>,

    // Rate
    arrivals: VecDeque<Instant>,
    rate_smps: f64,

    // Metadata
    name: String,
    last_seen: Option<Instant>,
}

impl StreamStats {
    fn update_jitter(&mut self, t_data: f64, now: Instant, window_s: u64) {
        if self.jitter_window.is_none() {
            self.jitter_window = Some(TimeWindow::new(window_s, 100.0));
        }

        if let (Some(lh), Some(ld)) = (self.last_host, self.last_data) {
            let dh = now.duration_since(lh).as_secs_f64();
            let dd = t_data - ld;
            let delta_ms = (dd - dh) * 1000.0;

            if let Some(w) = &mut self.jitter_window {
                w.push(delta_ms);
                self.jitter_ms = w.std_ms();
            }
        }
    }

    fn update_drift(&mut self, t_data: f64, now: Instant) {
        if let (Some(h0), Some(d0)) = (self.t0_host, self.t0_data) {
            let host_elapsed = now.duration_since(h0).as_secs_f64();
            let data_elapsed = t_data - d0;
            self.drift_s = data_elapsed - host_elapsed;

            if host_elapsed.abs() > 1e-9 {
                self.ppm = 1e6 * self.drift_s / host_elapsed;
            }
        }
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
    }

    fn reset_for_new_session(&mut self, session_id: u32) {
        self.reset_timing();
        self.samples_dropped = 0;
        self.current_session_id = Some(session_id);
        self.arrivals.clear();
    }

    fn on_sample(&mut self, sample_n: u32, t_data: f64, now: Instant, jitter_window_s: u64) {
        // First sample initialization
        if self.t0_host.is_none() {
            self.t0_host = Some(now);
        }
        if self.t0_data.is_none() {
            self.t0_data = Some(t_data);
        }

        self.update_jitter(t_data, now, jitter_window_s);
        self.last_host = Some(now);
        self.last_data = Some(t_data);
        self.update_drift(t_data, now);
        self.last_n = Some(sample_n);
        self.arrivals.push_back(now);
    }

    fn compute_rate(&mut self, now: Instant, window: Duration) -> f64 {
        let cutoff = now.checked_sub(window).unwrap_or(now);
        while let Some(&front) = self.arrivals.front() {
            if front < cutoff {
                self.arrivals.pop_front();
            } else {
                break;
            }
        }

        let count = self.arrivals.len() as f64;
        let win_secs = window.as_secs_f64().max(1e-6);
        self.rate_smps = count / win_secs;
        self.rate_smps
    }

    fn is_stale(&self, now: Instant, stale_dur: Duration) -> bool {
        self.last_seen
            .map(|t| now.duration_since(t) > stale_dur)
            .unwrap_or(true)
    }

    fn get_display_style(&self, ppm_warn: f64, ppm_err: f64, stale: bool) -> (Color, &'static str) {
        if stale {
            (Color::DarkGray, "STALLED")
        } else if self.ppm.abs() >= ppm_err {
            (Color::Red, "ERROR")
        } else if self.ppm.abs() >= ppm_warn {
            (Color::Yellow, "WARN")
        } else {
            (Color::Green, "OK")
        }
    }

    fn to_display_row(
        &self,
        route: String,
        stream_id: u8,
        now: Instant,
        stale_dur: Duration,
        ppm_warn: f64,
        ppm_err: f64,
    ) -> DisplayRow {
        let stale = self.is_stale(now, stale_dur);
        let (color, status) = self.get_display_style(ppm_warn, ppm_err, stale);

        DisplayRow {
            route,
            stream_id,
            name: self.name.clone(),
            rate_smps: self.rate_smps,
            drift_s: self.drift_s,
            ppm: self.ppm,
            jitter_ms: self.jitter_ms,
            samples_dropped: self.samples_dropped,
            last_n: self.last_n,
            last_data: self.last_data,
            status,
            color,
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

#[derive(Clone)]
struct LoggedEvent {
    timestamp: SystemTime,
    event: String,
    color: Color,
}

struct UiState {
    rows: Vec<DisplayRow>,
    event_log: VecDeque<LoggedEvent>,
    event_scroll_offset: usize,
}

impl UiState {
    fn filtered_events(&self, warnings_only: bool) -> Vec<&LoggedEvent> {
        self.event_log
            .iter()
            .filter(|e| !warnings_only || matches!(e.color, Color::Red | Color::Yellow))
            .collect()
    }
}

fn draw_ui(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    state: &UiState,
    cli: &Cli,
) -> io::Result<()> {
    terminal.draw(|f| {
        let size = f.area();

        // Calculate layout: header + table + event log + footer
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(10),
                Constraint::Length(if state.event_log.is_empty() { 0 } else { cli.event_display_lines + 2 }),
                Constraint::Length(if cli.quiet { 0 } else { 1 }),
            ])
            .split(size);

        // Header
        let header_text = format!(
            "tio-health — rate_window={}s  jitter_window={}s  warn/err={}ppm/{}ppm  fps={}  stale={}ms",
            cli.rate_window, cli.jitter_window, cli.ppm_warn, cli.ppm_err, cli.fps, cli.stale_ms
        );
        let header = Paragraph::new(header_text)
            .style(Style::default().add_modifier(Modifier::BOLD));
        f.render_widget(header, chunks[0]);

        // Table
        let header_cells = [
            "route", "sid", "stream_name", "smps/s", "drift(s)", "ppm",
            "jitter(ms)", "dropped", "last_n", "last_time", "status",
        ]
        .iter()
        .map(|h| Cell::from(*h).style(Style::default().add_modifier(Modifier::BOLD)));
        let header_row = Row::new(header_cells).height(1);

        let rows: Vec<Row> = state.rows.iter().map(|r| r.to_table_row()).collect();

        let widths = [
            Constraint::Length(10),  // route
            Constraint::Length(4),   // sid
            Constraint::Length(22),  // stream_name
            Constraint::Length(9),   // smps/s
            Constraint::Length(9),   // drift(s)
            Constraint::Length(8),   // ppm
            Constraint::Length(11),  // jitter(ms)
            Constraint::Length(8),   // dropped
            Constraint::Length(10),  // last_n
            Constraint::Length(12),  // last_time
            Constraint::Length(8),   // status
        ];

        let table = Table::new(rows, widths)
            .header(header_row)
            .column_spacing(2);

        f.render_stateful_widget(table, chunks[1], &mut TableState::default());

        // Event log
        if !state.event_log.is_empty() {
            let events_to_show = state.filtered_events(cli.warnings_only);

            let total_events = events_to_show.len();
            let display_count = cli.event_display_lines as usize;
            let start = state.event_scroll_offset.min(total_events.saturating_sub(1));
            let end = (start + display_count).min(total_events);
            let visible_events: Vec<Line> = events_to_show[start..end]
                .iter()
                .map(|logged| {
                    let datetime: DateTime<Local> = logged.timestamp.into();
                    Line::from(vec![
                        Span::styled(
                            format!("[{}] ", datetime.format("%H:%M:%S%.3f")),
                            Style::default().fg(logged.color),
                        ),
                        Span::styled(logged.event.clone(), Style::default().fg(logged.color)),
                    ])
                })
                .collect();

            // Show scroll indicator if there are more events
            let title = if total_events > display_count {
                format!("Recent Events [{}-{}/{}] (↑/↓ to scroll)", 
                    start + 1, end, total_events)
            } else {
                "Recent Events".to_string()
            };

            let event_block = Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Gray));

            let events_paragraph = Paragraph::new(visible_events).block(event_block);
            f.render_widget(events_paragraph, chunks[2]);
        }

        // Footer
        if !cli.quiet {
            let footer = Paragraph::new("q/Esc to quit  |  ↑/↓/PgUp/PgDn/Home/End to scroll events")
                .style(Style::default().fg(Color::Gray));
            f.render_widget(footer, chunks[3]);
        }
    })?;
    Ok(())
}

fn format_event(
    event: &BufferEvent,
    stats: &BTreeMap<StreamKey, StreamStats>,
) -> Option<(String, Color)> {
    match event {
        BufferEvent::SamplesSkipped {
            route,
            stream_id,
            session_id,
            count,
            ..
        } => {
            let stream_name = stats
                .get(&(route.clone(), *stream_id))
                .map(|s| s.name.as_str())
                .unwrap_or("unknown");
            Some((
                format!(
                    "[{}/{}] SAMPLES DROPPED: {} samples (session: {})",
                    route, stream_name, count, session_id
                ),
                Color::Red,
            ))
        }
        BufferEvent::SamplesBackward {
            route,
            stream_id,
            session_id,
            previous,
            current,
        } => {
            let stream_name = stats
                .get(&(route.clone(), *stream_id))
                .map(|s| s.name.as_str())
                .unwrap_or("unknown");
            Some((
                format!(
                    "[{}/{}] SAMPLES BACKWARD: {} → {} (session: {})",
                    route, stream_name, previous, current, session_id
                ),
                Color::Yellow,
            ))
        }
        BufferEvent::SessionChanged {
            route,
            stream_id,
            new_id,
            old_id,
        } => {
            let stream_name = stats
                .get(&(route.clone(), *stream_id))
                .map(|s| s.name.as_str())
                .unwrap_or("unknown");
            Some((
                format!(
                    "[{}/{}] SESSION CHANGED: {} → {}",
                    route, stream_name, old_id, new_id
                ),
                Color::Green,
            ))
        }
        BufferEvent::MetadataChanged(route) => {
            Some((format!("[{}] METADATA CHANGED", route), Color::Green))
        }
        BufferEvent::SegmentChanged(route) => {
            Some((format!("[{}] SEGMENT CHANGED", route), Color::Green))
        }
        BufferEvent::RouteDiscovered(route) => {
            Some((format!("[{}] ROUTE DISCOVERED", route), Color::Green))
        }
        _ => None,
    }
}

fn handle_samples_event(
    samples: Vec<(
        twinleaf::data::Sample,
        twinleaf::tio::proto::route::DeviceRoute,
    )>,
    stats: &mut BTreeMap<StreamKey, StreamStats>,
    streams_filter: Option<&[u8]>,
    jitter_window_s: u64,
) {
    let now = Instant::now();

    for (sample, route) in samples {
        let sid = sample.stream.stream_id as u8;

        if let Some(filter) = streams_filter {
            if !filter.contains(&sid) {
                continue;
            }
        }

        let key = (route.clone(), sid);
        let st = stats.entry(key).or_insert_with(|| StreamStats {
            name: sample.stream.name.clone(),
            current_session_id: Some(sample.device.session_id),
            ..Default::default()
        });

        st.name = sample.stream.name.clone();
        st.last_seen = Some(now);
        st.last_data = Some(sample.timestamp_end());
        st.on_sample(sample.n, sample.timestamp_end(), now, jitter_window_s);
    }
}

fn handle_session_changed(
    route: twinleaf::tio::proto::route::DeviceRoute,
    stream_id: u8,
    new_id: u32,
    stats: &mut BTreeMap<StreamKey, StreamStats>,
) {
    let key = (route, stream_id);
    if let Some(st) = stats.get_mut(&key) {
        st.reset_for_new_session(new_id);
    }
}

fn handle_samples_skipped(
    route: twinleaf::tio::proto::route::DeviceRoute,
    stream_id: u8,
    count: u32,
    stats: &mut BTreeMap<StreamKey, StreamStats>,
) {
    let key = (route, stream_id);
    if let Some(st) = stats.get_mut(&key) {
        st.samples_dropped += count as u64;
    }
}

fn main() {
    let cli = Cli::parse();

    let mut terminal = ratatui::init();

    let proxy = tio::proxy::Interface::new(&cli.tio.root);
    let route = cli.tio.parse_route();
    let tree = match DeviceTree::open(&proxy, route.clone()) {
        Ok(t) => t,
        Err(e) => {
            ratatui::restore();
            eprintln!("Failed to open device tree: {:?}", e);
            std::process::exit(1);
        }
    };

    let (event_tx, event_rx) = channel::unbounded();
    let buffer = Buffer::new(event_tx, 100_000, true);
    // How often we want UI/data snapshots
    let frame = Duration::from_millis(1000 / cli.fps);

    // Channel: data thread -> UI thread
    let (state_tx, state_rx) = channel::bounded::<UiState>(1);
    let streams_filter = cli.streams.clone();
    let jitter_window_s = cli.jitter_window_s();
    let rate_window = cli.rate_window_dur();
    let stale_dur = cli.stale_dur();
    let event_log_cap = cli.event_log_size;
    let ppm_warn = cli.ppm_warn;
    let ppm_err = cli.ppm_err;

    std::thread::spawn(move || {
        let mut tree = tree;
        let mut buffer = buffer;
        let mut stats = BTreeMap::<StreamKey, StreamStats>::new();
        let mut event_log = VecDeque::<LoggedEvent>::new();
        let mut last_snapshot = Instant::now();

        loop {
            // Blocking I/O - read from device tree
            let (sample, route) = match tree.next() {
                Ok(s) => s,
                Err(_) => break,
            };

            buffer.process_sample(sample, route);

            loop {
                match tree.try_next() {
                    Ok(Some((s, r))) => {
                        buffer.process_sample(s, r);
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }

            while let Ok(event) = event_rx.try_recv() {
                if let Some((event_str, event_color)) = format_event(&event, &stats) {
                    event_log.push_front(LoggedEvent {
                        timestamp: SystemTime::now(),
                        event: event_str,
                        color: event_color,
                    });
                    if event_log.len() > event_log_cap.try_into().unwrap() {
                        event_log.pop_back();
                    }
                }

                match event {
                    BufferEvent::Samples(samples) => {
                        handle_samples_event(
                            samples,
                            &mut stats,
                            streams_filter.as_deref(),
                            jitter_window_s,
                        );
                    }
                    BufferEvent::SessionChanged {
                        route,
                        stream_id,
                        new_id,
                        ..
                    } => {
                        handle_session_changed(route, stream_id, new_id, &mut stats);
                    }
                    BufferEvent::SamplesSkipped {
                        route,
                        stream_id,
                        count,
                        ..
                    } => {
                        handle_samples_skipped(route, stream_id, count, &mut stats);
                    }
                    _ => {}
                }
            }

            let now = Instant::now();
            if now.duration_since(last_snapshot) >= frame {
                let mut rows: Vec<DisplayRow> = stats
                    .iter_mut()
                    .map(|((route, stream_id), st)| {
                        st.compute_rate(now, rate_window);
                        if st.is_stale(now, stale_dur) && st.t0_host.is_some() {
                            st.reset_timing();
                        }
                        st.to_display_row(
                            route.to_string(),
                            *stream_id,
                            now,
                            stale_dur,
                            ppm_warn,
                            ppm_err,
                        )
                    })
                    .collect();

                rows.sort_by(|a, b| a.route.cmp(&b.route).then(a.stream_id.cmp(&b.stream_id)));

                let snapshot = UiState {
                    rows,
                    event_log: event_log.clone(),
                    event_scroll_offset: 0,
                };

                let _ = state_tx.try_send(snapshot);
                last_snapshot = now;
            }
        }
    });

    // Keyboard handler
    let (key_tx, key_rx) = channel::unbounded();
    std::thread::spawn(move || loop {
        if let Ok(ev) = event::read() {
            let _ = key_tx.send(ev);
        }
    });

    // Main event loop
    let mut current_state: Option<UiState> = None;

    'main: loop {
        crossbeam::select! {
            recv(key_rx) -> ev => {
                if let Ok(Event::Key(k)) = ev {
                    // Only process key press events to avoid double-triggering on Windows
                    if k.kind != KeyEventKind::Press {
                        continue;
                    }

                    let quit = matches!(k.code, KeyCode::Char('q') | KeyCode::Esc)
                            || (k.code == KeyCode::Char('c') && k.modifiers == KeyModifiers::CONTROL);
                    if quit { break 'main; }

                    // Handle scroll keys
                    if let Some(ref mut state) = current_state {
                        let events_to_show = state.filtered_events(cli.warnings_only);

                        let total_events = events_to_show.len();
                        let display_count = cli.event_display_lines as usize;

                        match k.code {
                            KeyCode::Up => {
                                if state.event_scroll_offset > 0 {
                                    state.event_scroll_offset -= 1;
                                    let _ = draw_ui(&mut terminal, state, &cli);
                                }
                            }
                            KeyCode::Down => {
                                if total_events > display_count {
                                    let max_offset = total_events.saturating_sub(display_count);
                                    if state.event_scroll_offset < max_offset {
                                        state.event_scroll_offset += 1;
                                        let _ = draw_ui(&mut terminal, state, &cli);
                                    }
                                }
                            }
                            KeyCode::PageUp => {
                                state.event_scroll_offset = state.event_scroll_offset.saturating_sub(display_count);
                                let _ = draw_ui(&mut terminal, state, &cli);
                            }
                            KeyCode::PageDown => {
                                if total_events > display_count {
                                    let max_offset = total_events.saturating_sub(display_count);
                                    state.event_scroll_offset = (state.event_scroll_offset + display_count).min(max_offset);
                                    let _ = draw_ui(&mut terminal, state, &cli);
                                }
                            }
                            KeyCode::Home => {
                                state.event_scroll_offset = 0;
                                let _ = draw_ui(&mut terminal, state, &cli);
                            }
                            KeyCode::End => {
                                if total_events > display_count {
                                    state.event_scroll_offset = total_events.saturating_sub(display_count);
                                    let _ = draw_ui(&mut terminal, state, &cli);
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }

            recv(state_rx) -> msg => {
                match msg {
                    Ok(mut state) => {
                        // Preserve scroll offset if we already have state
                        if let Some(ref prev_state) = current_state {
                            state.event_scroll_offset = prev_state.event_scroll_offset;
                        }

                        if draw_ui(&mut terminal, &state, &cli).is_err() {
                            break 'main;
                        }

                        current_state = Some(state);
                    }
                    Err(_) => {
                        // data thread exited
                        break 'main;
                    }
                }
            }
        }
    }

    ratatui::restore();
}
