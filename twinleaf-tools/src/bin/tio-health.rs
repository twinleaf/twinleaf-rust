// tio-health
//
// Live timing & rate diagnostics by device route.
//
// Build: cargo run --release -- <tio-url> [route] [options]
// Quit:  q / Esc / Ctrl-C

use chrono::{DateTime, Local};
use clap::Parser;
use crossbeam::channel;
use crossterm::style::{Attribute, Color, ResetColor, SetAttribute, SetForegroundColor};
use crossterm::{cursor, event, style, terminal, ExecutableCommand, QueueableCommand};
use std::collections::{BTreeMap, VecDeque};
use std::io::{self, Write};
use std::time::{Duration, Instant, SystemTime};
use twinleaf::device::{Buffer, BufferEvent};
use twinleaf::device::{DeviceTree, StreamKey};
use twinleaf::tio;
use twinleaf_tools::TioOpts;

#[derive(Parser, Debug)]
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
        help = "Seconds for rate calculation window"
    )]
    rate_window: u64,

    /// Time window in seconds for calculating jitter statistics
    #[arg(
        long = "jitter-window",
        default_value = "10",
        value_name = "SECONDS",
        help = "Seconds for jitter calculation window"
    )]
    jitter_window: u64,

    /// PPM threshold for yellow warning indicators
    #[arg(
        long = "ppm-warn",
        default_value = "100",
        value_name = "PPM",
        help = "Warning threshold in parts per million"
    )]
    ppm_warn: f64,

    /// PPM threshold for red error indicators
    #[arg(
        long = "ppm-err",
        default_value = "200",
        value_name = "PPM",
        help = "Error threshold in parts per million"
    )]
    ppm_err: f64,

    /// Filter to only show specific stream IDs (comma-separated)
    #[arg(
        long = "streams",
        value_delimiter = ',',
        value_name = "IDS",
        help = "Comma-separated stream IDs to monitor (e.g., 0,1,5)"
    )]
    streams: Option<Vec<u8>>,

    /// Suppress the footer help text ("q/Esc to quit")
    #[arg(long = "quiet", help = "Suppress footer hint")]
    quiet: bool,

    /// UI refresh rate in frames per second
    #[arg(
        long = "fps",
        default_value = "10",
        value_name = "FPS",
        help = "UI refresh rate (frames per second)"
    )]
    fps: u64,

    /// Time in milliseconds before marking a stream as stale
    #[arg(
        long = "stale-ms",
        default_value = "2000",
        value_name = "MS",
        help = "Mark streams as stale after this many milliseconds without data"
    )]
    stale_ms: u64,

    /// Maximum number of events to keep in the event log
    #[arg(
        long = "event-log-size",
        default_value = "5",
        value_name = "N",
        help = "Maximum number of events to display in the log"
    )]
    event_log_size: usize,

    /// Only show warning and error events in the log (yellow and red)
    #[arg(
        long = "warnings-only",
        help = "Show only red and yellow events in the log"
    )]
    warnings_only: bool,
}

struct Config {
    rate_window: Duration,
    jitter_window: u64,
    stale_dur: Duration,
    ppm_warn: f64,
    ppm_err: f64,
    fps: u64,
    event_log_size: usize,
    warnings_only: bool,
    quiet: bool,
    streams_filter: Option<Vec<u8>>,
}

impl From<&Cli> for Config {
    fn from(cli: &Cli) -> Self {
        Self {
            rate_window: Duration::from_secs(cli.rate_window.max(1)),
            jitter_window: cli.jitter_window,
            stale_dur: Duration::from_millis(cli.stale_ms.max(1)),
            ppm_warn: cli.ppm_warn,
            ppm_err: cli.ppm_err,
            fps: cli.fps,
            event_log_size: cli.event_log_size,
            warnings_only: cli.warnings_only,
            quiet: cli.quiet,
            streams_filter: cli.streams.clone(),
        }
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
    metadata_ok: bool,
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

    fn reset_for_new_session(&mut self, session_id: u32) {
        self.t0_host = None;
        self.t0_data = None;
        self.last_host = None;
        self.last_data = None;
        self.drift_s = 0.0;
        self.ppm = 0.0;
        self.jitter_ms = 0.0;
        self.jitter_window = None;
        self.last_n = None;
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

    fn get_status(&self, ppm_warn: f64, ppm_err: f64, stale: bool) -> &'static str {
        if !self.metadata_ok {
            "META"
        } else if stale {
            "STALLED"
        } else if self.ppm.abs() >= ppm_err {
            "ERROR"
        } else if self.ppm.abs() >= ppm_warn {
            "WARN"
        } else {
            "OK"
        }
    }

    fn to_display_row(
        &self,
        route: String,
        stream_id: u8,
        config: &Config,
        now: Instant,
    ) -> DisplayRow {
        let stale = self.is_stale(now, config.stale_dur);
        let status = self.get_status(config.ppm_warn, config.ppm_err, stale);

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
            metadata_ok: self.metadata_ok,
            status,
            stale,
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
    metadata_ok: bool,
    status: &'static str,
    stale: bool,
}

impl DisplayRow {
    fn get_color(&self) -> Color {
        if self.stale {
            Color::DarkGrey
        } else {
            match self.status {
                "ERROR" => Color::Red,
                "WARN" => Color::Yellow,
                "OK" => Color::Green,
                "META" => Color::Blue,
                _ => Color::White,
            }
        }
    }
}

#[derive(Clone)]
struct LoggedEvent {
    timestamp: SystemTime,
    event: String,
    color: Color,
}

struct Tui {
    stdout: io::Stdout,
}

impl Tui {
    fn setup() -> io::Result<Self> {
        let mut stdout = io::stdout();
        terminal::enable_raw_mode()?;
        stdout.execute(terminal::EnterAlternateScreen)?;
        stdout.execute(cursor::Hide)?;
        Ok(Self { stdout })
    }

    fn teardown(&mut self) {
        let _ = self.stdout.execute(cursor::Show);
        let _ = self.stdout.execute(terminal::LeaveAlternateScreen);
        let _ = terminal::disable_raw_mode();
        let _ = self.stdout.flush();
    }

    fn draw(
        &mut self,
        header: &str,
        rows: &[DisplayRow],
        event_log: &VecDeque<LoggedEvent>,
        config: &Config,
    ) -> io::Result<()> {
        self.clear_screen()?;
        self.draw_header(header)?;
        self.draw_table_header()?;
        self.draw_data_rows(rows)?;
        self.draw_event_log(event_log, config)?;
        self.draw_footer(config.quiet)?;
        self.stdout.flush()
    }

    fn clear_screen(&mut self) -> io::Result<()> {
        self.stdout.queue(cursor::MoveTo(0, 0))?;
        self.stdout
            .queue(terminal::Clear(terminal::ClearType::All))?;
        Ok(())
    }

    fn draw_header(&mut self, header: &str) -> io::Result<()> {
        self.stdout.queue(SetAttribute(Attribute::Bold))?;
        self.stdout.queue(style::Print(header))?;
        self.stdout.queue(SetAttribute(Attribute::Reset))?;
        self.stdout.queue(cursor::MoveToNextLine(1))?;
        self.stdout.queue(cursor::MoveToNextLine(1))?;
        Ok(())
    }

    fn draw_table_header(&mut self) -> io::Result<()> {
        self.stdout.queue(SetAttribute(Attribute::Bold))?;
        self.stdout.queue(style::Print(format!(
            "{:<10} {:>3}  {:<22}  {:>9}  {:>9}  {:>8}  {:>11}  {:>8}  {:>10}  {:>12}  {:>8}",
            "route",
            "sid",
            "stream_name",
            "smps/s",
            "drift(s)",
            "ppm",
            "jitter(ms)",
            "dropped",
            "last_n",
            "last_time",
            "status"
        )))?;
        self.stdout.queue(SetAttribute(Attribute::Reset))?;
        self.stdout.queue(cursor::MoveToNextLine(1))?;
        Ok(())
    }

    fn draw_data_rows(&mut self, rows: &[DisplayRow]) -> io::Result<()> {
        let mut last_route = String::new();

        for row in rows {
            if row.route != last_route {
                if !last_route.is_empty() {
                    self.stdout.queue(cursor::MoveToNextLine(1))?;
                }
                last_route = row.route.clone();
            }

            let color = row.get_color();
            self.stdout.queue(SetForegroundColor(color))?;
            self.stdout.queue(style::Print(format!(
                "{:<10} {:>3}  {:<22}  {:>9.1}  {:>9.3}  {:>8.0}  {:>11.2}  {:>8}  {:>10}  {:>12.3}  {:>8}",
                row.route,
                row.stream_id,
                row.name,
                if row.metadata_ok { row.rate_smps } else { 0.0 },
                if row.metadata_ok { row.drift_s } else { 0.0 },
                if row.metadata_ok { row.ppm } else { 0.0 },
                if row.metadata_ok { row.jitter_ms } else { 0.0 },
                if row.metadata_ok { row.samples_dropped } else { 0 },
                row.last_n.unwrap_or(0),
                row.last_data.unwrap_or(0.0),
                row.status
            )))?;
            self.stdout.queue(ResetColor)?;
            self.stdout.queue(cursor::MoveToNextLine(1))?;
        }

        Ok(())
    }

    fn draw_event_log(
        &mut self,
        event_log: &VecDeque<LoggedEvent>,
        config: &Config,
    ) -> io::Result<()> {
        if event_log.is_empty() {
            return Ok(());
        }

        let shown_count = if config.warnings_only {
            event_log
                .iter()
                .filter(|e| matches!(e.color, Color::Red | Color::Yellow))
                .count()
        } else {
            event_log.len()
        };

        self.stdout.queue(cursor::MoveToNextLine(2))?;
        self.stdout.queue(SetAttribute(Attribute::Bold))?;
        self.stdout.queue(style::Print(format!(
            "Recent Events ({} of {}):",
            shown_count, config.event_log_size
        )))?;
        self.stdout.queue(SetAttribute(Attribute::Reset))?;
        self.stdout.queue(cursor::MoveToNextLine(1))?;

        let events_to_show = event_log
            .iter()
            .filter(|e| !config.warnings_only || matches!(e.color, Color::Red | Color::Yellow));

        for logged in events_to_show {
            let datetime: DateTime<Local> = logged.timestamp.into();
            self.stdout.queue(SetForegroundColor(logged.color))?;
            self.stdout.queue(style::Print(format!(
                "[{}] {}",
                datetime.format("%H:%M:%S%.3f"),
                logged.event
            )))?;
            self.stdout.queue(ResetColor)?;
            self.stdout.queue(cursor::MoveToNextLine(1))?;
        }

        Ok(())
    }

    fn draw_footer(&mut self, quiet: bool) -> io::Result<()> {
        if !quiet {
            self.stdout.queue(cursor::MoveToNextLine(1))?;
            self.stdout.queue(style::Print("q/Esc to quit"))?;
        }
        Ok(())
    }
}

fn format_event(event: &BufferEvent, stats: &BTreeMap<StreamKey, StreamStats>) -> (String, Color) {
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
            (
                format!(
                    "[{}/{}] SAMPLES DROPPED: {} samples (session: {})",
                    route, stream_name, count, session_id
                ),
                Color::Red,
            )
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
            (
                format!(
                    "[{}/{}] SAMPLES BACKWARD: {} → {} (session: {})",
                    route, stream_name, previous, current, session_id
                ),
                Color::Yellow,
            )
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
            (
                format!(
                    "[{}/{}] SESSION CHANGED: {} → {}",
                    route, stream_name, old_id, new_id
                ),
                Color::Green,
            )
        }
        BufferEvent::MetadataChanged(route) => {
            (format!("[{}] METADATA CHANGED", route), Color::Green)
        }
        BufferEvent::SegmentChanged(route) => {
            (format!("[{}] SEGMENT CHANGED", route), Color::Green)
        }
        BufferEvent::RouteDiscovered(route) => {
            (format!("[{}] ROUTE DISCOVERED", route), Color::Green)
        }
        _ => (String::new(), Color::White),
    }
}

fn handle_samples_event(
    samples: Vec<(
        twinleaf::data::Sample,
        twinleaf::tio::proto::route::DeviceRoute,
    )>,
    stats: &mut BTreeMap<StreamKey, StreamStats>,
    config: &Config,
) {
    let now = Instant::now();

    for (sample, route) in samples {
        let sid = sample.stream.stream_id as u8;

        if let Some(ref filter) = config.streams_filter {
            if !filter.contains(&sid) {
                continue;
            }
        }

        let key = (route.clone(), sid);
        let st = stats.entry(key).or_insert_with(|| StreamStats {
            name: sample.stream.name.clone(),
            metadata_ok: true,
            current_session_id: Some(sample.device.session_id),
            ..Default::default()
        });

        st.name = sample.stream.name.clone();
        st.last_seen = Some(now);
        st.last_data = Some(sample.timestamp_end());
        st.on_sample(sample.n, sample.timestamp_end(), now, config.jitter_window);
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
    let config = Config::from(&cli);

    let mut tui = Tui::setup().expect("TUI setup failed");
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let mut t = Tui {
            stdout: io::stdout(),
        };
        t.teardown();
        original_hook(panic_info);
    }));

    let proxy = tio::proxy::Interface::new(&cli.tio.root);
    let route = cli.tio.parse_route();
    let tree = match DeviceTree::open(&proxy, route.clone()) {
        Ok(t) => t,
        Err(e) => {
            tui.teardown();
            eprintln!("Failed to open device tree: {:?}", e);
            std::process::exit(1);
        }
    };

    let (event_tx, event_rx) = channel::unbounded();
    let mut buffer = Buffer::new(tree, event_tx, 1, true);

    let stats = std::sync::Arc::new(std::sync::Mutex::new(
        BTreeMap::<StreamKey, StreamStats>::new(),
    ));
    let event_log = std::sync::Arc::new(std::sync::Mutex::new(VecDeque::<LoggedEvent>::new()));

    let frame = Duration::from_millis((1000 / config.fps.max(1)) as u64);

    let stats_clone = stats.clone();
    let event_log_clone = event_log.clone();
    let config_clone = Config::from(&cli);

    std::thread::spawn(move || loop {
        let (sample, route) = match buffer.tree.next() {
            Ok(s) => s,
            Err(_) => break,
        };

        buffer.process_sample(sample, route);

        if buffer.drain().is_err() {
            break;
        }

        while let Ok(event) = event_rx.try_recv() {
            let (event_str, event_color) = {
                let stats = stats_clone.lock().unwrap();
                format_event(&event, &stats)
            };

            if !event_str.is_empty() {
                let mut log = event_log_clone.lock().unwrap();
                log.push_front(LoggedEvent {
                    timestamp: SystemTime::now(),
                    event: event_str,
                    color: event_color,
                });
                if log.len() > config_clone.event_log_size {
                    log.pop_back();
                }
            }

            match event {
                BufferEvent::Samples(samples) => {
                    let mut stats = stats_clone.lock().unwrap();
                    handle_samples_event(samples, &mut stats, &config_clone);
                }

                BufferEvent::SessionChanged {
                    route,
                    stream_id,
                    new_id,
                    old_id: _,
                } => {
                    let mut stats = stats_clone.lock().unwrap();
                    handle_session_changed(route, stream_id, new_id, &mut stats);
                }

                BufferEvent::SamplesSkipped {
                    route,
                    stream_id,
                    count,
                    ..
                } => {
                    let mut stats = stats_clone.lock().unwrap();
                    handle_samples_skipped(route, stream_id, count, &mut stats);
                }

                _ => {}
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

    // UI loop
    let tick = channel::tick(frame);
    'main: loop {
        crossbeam::select! {
            recv(key_rx) -> ev => {
                if let Ok(event::Event::Key(k)) = ev {
                    use event::{KeyCode, KeyModifiers};
                    let quit = matches!(k.code, KeyCode::Char('q') | KeyCode::Esc)
                             || (k.code == KeyCode::Char('c') && k.modifiers == KeyModifiers::CONTROL);
                    if quit { break 'main; }
                }
            }

            recv(tick) -> _ => {
                let now = Instant::now();

                let rows: Vec<DisplayRow> = {
                    let mut stats = stats.lock().unwrap();
                    stats
                        .iter_mut()
                        .map(|((route, stream_id), st)| {
                            st.compute_rate(now, config.rate_window);
                            if st.is_stale(now, config.stale_dur) && st.t0_host.is_some() {
                                st.reset_timing();
                            }
                            st.to_display_row(route.to_string(), *stream_id, &config, now)
                        })
                        .collect()
                };

                let mut sorted_rows = rows;
                sorted_rows.sort_by(|a, b| {
                    a.route.cmp(&b.route).then(a.stream_id.cmp(&b.stream_id))
                });

                let header = format!(
                    "tio-health — rate_window={}s  jitter_window={}s  warn/err={}ppm/{}ppm  fps={}  stale={}ms",
                    cli.rate_window, cli.jitter_window, cli.ppm_warn, cli.ppm_err, cli.fps, cli.stale_ms
                );

                let log = event_log.lock().unwrap().clone();

                if tui.draw(&header, &sorted_rows, &log, &config).is_err() {
                    break 'main;
                }
            }
        }
    }

    tui.teardown();
}
