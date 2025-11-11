// tio-health
//
// Simplified live timing & rate diagnostics by device route.
//
// Build: cargo run --release -- <tio-url> [route] [options]
// Quit:  q / Esc / Ctrl-C

use chrono::{DateTime, Local};
use crossbeam::channel;
use crossterm::style::{Attribute, Color, ResetColor, SetAttribute, SetForegroundColor};
use crossterm::{cursor, event, style, terminal, ExecutableCommand, QueueableCommand};
use std::collections::{BTreeMap, VecDeque};
use std::io::{self, Write};
use std::time::{Duration, Instant, SystemTime};
use twinleaf::device::DeviceTree;
use twinleaf::device::{Buffer, BufferEvent};
use twinleaf::tio;
use twinleaf_tools::{tio_opts, tio_parseopts};

#[derive(Debug)]
struct Cli {
    rate_window_s: u64,
    jitter_window_s: u64,
    ppm_warn: f64,
    ppm_err: f64,
    streams_filter: Option<Vec<u8>>,
    quiet: bool,
    fps: u64,
    stale_ms: u64,
    event_log_size: usize,
    show_warnings_only: bool,
}

fn print_help_and_exit(opts: &getopts::Options, program: &str, code: i32) -> ! {
    let brief = format!(
        "Usage: {program} [options] <tio-url> [route]\n\n\
         Live timing & rate diagnostics for TIO devices."
    );
    let usage = opts.usage(&brief);
    eprintln!("{usage}");
    std::process::exit(code)
}

fn parse_cli() -> (String, tio::proto::DeviceRoute, Cli) {
    let mut opts = tio_opts();
    opts.optflag("h", "help", "Show help");
    opts.optopt(
        "",
        "rate-window",
        "Seconds for rate window (default 5)",
        "sec",
    );
    opts.optopt(
        "",
        "jitter-window",
        "Seconds for jitter window (default 10)",
        "sec",
    );
    opts.optopt("", "ppm-warn", "Warn threshold in ppm (default 100)", "ppm");
    opts.optopt("", "ppm-err", "Error threshold in ppm (default 200)", "ppm");
    opts.optopt(
        "",
        "streams",
        "Comma-separated stream IDs to include",
        "list",
    );
    opts.optflag("", "quiet", "Suppress footer hint");
    opts.optopt("", "fps", "UI refresh rate (default 10)", "n");
    opts.optopt(
        "",
        "stale-ms",
        "Grey rows after this many ms (default 2000)",
        "ms",
    );
    opts.optopt(
        "",
        "event-log-size",
        "Max events to show in log (default 5)",
        "n",
    );
    opts.optflag(
        "", 
        "warnings-only", 
        "Show only red and yellow events"
    );


    let (matches, root, route) = tio_parseopts(&opts, &std::env::args().collect::<Vec<_>>());
    if matches.opt_present("help") {
        print_help_and_exit(
            &opts,
            &std::env::args()
                .next()
                .unwrap_or_else(|| "tio-health".into()),
            0,
        );
    }

    let rate_window_s = matches
        .opt_str("rate-window")
        .as_deref()
        .unwrap_or("5")
        .parse()
        .unwrap_or(5);
    let jitter_window_s = matches
        .opt_str("jitter-window")
        .as_deref()
        .unwrap_or("10")
        .parse()
        .unwrap_or(10);
    let ppm_warn = matches
        .opt_str("ppm-warn")
        .as_deref()
        .unwrap_or("100")
        .parse()
        .unwrap_or(100.0);
    let ppm_err = matches
        .opt_str("ppm-err")
        .as_deref()
        .unwrap_or("200")
        .parse()
        .unwrap_or(200.0);
    let streams_filter = matches.opt_str("streams").map(|s| {
        s.split(',')
            .filter_map(|x| x.trim().parse::<u8>().ok())
            .collect()
    });
    let quiet = matches.opt_present("quiet");
    let fps = matches
        .opt_str("fps")
        .as_deref()
        .unwrap_or("10")
        .parse()
        .unwrap_or(10);
    let stale_ms = matches
        .opt_str("stale-ms")
        .as_deref()
        .unwrap_or("2000")
        .parse()
        .unwrap_or(2000);
    let event_log_size = matches
        .opt_str("event-log-size")
        .as_deref()
        .unwrap_or("5")
        .parse()
        .unwrap_or(5);
    let show_warnings_only = matches.opt_present("warnings-only");

    (
        root,
        route,
        Cli {
            rate_window_s,
            jitter_window_s,
            ppm_warn,
            ppm_err,
            streams_filter,
            quiet,
            fps,
            stale_ms,
            event_log_size,
            show_warnings_only,
        },
    )
}

#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct StreamKey {
    route: String,
    stream_id: u8,
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
    fn ensure_jitter_window(&mut self, seconds: u64) {
        if self.jitter_window.is_none() {
            self.jitter_window = Some(TimeWindow::new(seconds, 100.0));
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
        self.ensure_jitter_window(jitter_window_s);

        // First sample initialization
        if self.t0_host.is_none() {
            self.t0_host = Some(now);
        }
        if self.t0_data.is_none() {
            self.t0_data = Some(t_data);
        }

        // Compute jitter from consecutive samples
        if let (Some(lh), Some(ld)) = (self.last_host, self.last_data) {
            let dh = now.duration_since(lh).as_secs_f64();
            let dd = t_data - ld;
            let delta_ms = (dd - dh) * 1000.0;

            if let Some(w) = &mut self.jitter_window {
                w.push(delta_ms);
                self.jitter_ms = w.std_ms();
            }
        }

        self.last_host = Some(now);
        self.last_data = Some(t_data);

        // Compute drift and ppm
        let h0 = self.t0_host.unwrap();
        let d0 = self.t0_data.unwrap();
        let host_elapsed = now.duration_since(h0).as_secs_f64();
        let data_elapsed = t_data - d0;
        self.drift_s = data_elapsed - host_elapsed;

        if host_elapsed.abs() > 1e-9 {
            self.ppm = 1e6 * self.drift_s / host_elapsed;
        }

        // Note: Sample dropping is now tracked via BufferEvent::SamplesSkipped
        self.last_n = Some(sample_n);

        // Rate tracking
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
        rows: &[(
            String,
            u8,
            String,
            f64,
            f64,
            f64,
            f64,
            u64,
            u32,
            f64,
            &'static str,
            bool,
        )],
        event_log: &VecDeque<LoggedEvent>,
        event_log_size: usize,
        show_warnings_only: bool,
        quiet: bool,
    ) -> io::Result<()> {
        self.stdout.queue(cursor::MoveTo(0, 0))?;
        self.stdout
            .queue(terminal::Clear(terminal::ClearType::All))?;

        // Header
        self.stdout.queue(SetAttribute(Attribute::Bold))?;
        self.stdout.queue(style::Print(header))?;
        self.stdout.queue(SetAttribute(Attribute::Reset))?;
        self.stdout.queue(cursor::MoveToNextLine(1))?;
        self.stdout.queue(cursor::MoveToNextLine(1))?;

        // Table header
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

        // Data rows
        let mut last_route = String::new();
        for (
            route,
            sid,
            name,
            rate,
            drift,
            ppm,
            jitter,
            dropped,
            last_n,
            last_timestamp,
            status,
            stale,
        ) in rows
        {
            if *route != last_route {
                if !last_route.is_empty() {
                    self.stdout.queue(cursor::MoveToNextLine(1))?;
                }
                last_route = route.clone();
            }

            let color = if *stale {
                Color::DarkGrey
            } else {
                match *status {
                    "ERROR" => Color::Red,
                    "WARN" => Color::Yellow,
                    "OK" => Color::Green,
                    "META" => Color::Blue,
                    _ => Color::White,
                }
            };

            self.stdout.queue(SetForegroundColor(color))?;
            self.stdout.queue(style::Print(format!(
                "{:<10} {:>3}  {:<22}  {:>9.1}  {:>9.3}  {:>8.0}  {:>11.2}  {:>8}  {:>10}  {:>12.3}  {:>8}",
                route, sid, name, rate, drift, ppm, jitter, dropped, last_n, last_timestamp, status
            )))?;
            self.stdout.queue(ResetColor)?;
            self.stdout.queue(cursor::MoveToNextLine(1))?;
        }

        // Event log section
        if !event_log.is_empty() {
            let shown_count = if show_warnings_only {
                event_log.iter().filter(|e| matches!(e.color, Color::Red | Color::Yellow)).count()
            } else {
                event_log.len()
            };
            self.stdout.queue(cursor::MoveToNextLine(2))?;
            self.stdout.queue(SetAttribute(Attribute::Bold))?;
            self.stdout.queue(style::Print(format!(
                "Recent Events ({} of {}):",
                shown_count, event_log_size
            )))?;
            self.stdout.queue(SetAttribute(Attribute::Reset))?;
            self.stdout.queue(cursor::MoveToNextLine(1))?;

            // Only show red events if flag set
            let iter = event_log.iter().filter(|e| {
                !show_warnings_only || matches!(e.color, Color::Red) || matches!(e.color, Color::Yellow)
            });

            for logged in iter {
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
        }

        if !quiet {
            self.stdout.queue(cursor::MoveToNextLine(1))?;
            self.stdout.queue(style::Print("q/Esc to quit"))?;
        }

        self.stdout.flush()
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
                .iter()
                .find(|(k, _)| k.route == route.to_string() && k.stream_id == *stream_id)
                .map(|(_, s)| s.name.as_str())
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
                .iter()
                .find(|(k, _)| k.route == route.to_string() && k.stream_id == *stream_id)
                .map(|(_, s)| s.name.as_str())
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
            old_id
        } => {
            let stream_name = stats
                .iter()
                .find(|(k, _)| k.route == route.to_string() && k.stream_id == *stream_id)
                .map(|(_, s)| s.name.as_str())
                .unwrap_or("unknown");
            (
                format!("[{}/{}] SESSION CHANGED: {} → {}", route, stream_name, old_id, new_id),
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

fn main() {
    let (root, route, cli) = parse_cli();

    let mut tui = Tui::setup().expect("TUI setup failed");
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let mut t = Tui {
            stdout: io::stdout(),
        };
        t.teardown();
        original_hook(panic_info);
    }));

    let proxy = tio::proxy::Interface::new(&root);
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

    let rate_window = Duration::from_secs(cli.rate_window_s.max(1));
    let stale_dur = Duration::from_millis(cli.stale_ms.max(1));
    let frame = Duration::from_millis((1000 / cli.fps.max(1)) as u64);

    let stats_clone = stats.clone();
    let event_log_clone = event_log.clone();
    let jitter_window_s = cli.jitter_window_s;
    let event_log_size = cli.event_log_size;
    let streams_filter = cli.streams_filter.clone();

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
                if log.len() > event_log_size {
                    log.pop_back();
                }
            }

            match event {
                BufferEvent::Samples(samples) => {
                    let now = Instant::now();
                    let mut stats = stats_clone.lock().unwrap();

                    for (sample, route) in samples {
                        let route_str = route.to_string();
                        let sid = sample.stream.stream_id as u8;

                        if let Some(ref filter) = streams_filter {
                            if !filter.contains(&sid) {
                                continue;
                            }
                        }

                        let key = StreamKey {
                            route: route_str,
                            stream_id: sid,
                        };
                        let st = stats.entry(key).or_insert_with(|| StreamStats {
                            name: sample.stream.name.clone(),
                            metadata_ok: true,
                            current_session_id: Some(sample.device.session_id),
                            ..Default::default()
                        });

                        st.name = sample.stream.name.clone();
                        st.last_seen = Some(now);
                        st.last_data = Some(sample.timestamp_end());
                        st.on_sample(sample.n, sample.timestamp_end(), now, jitter_window_s);
                    }
                }

                BufferEvent::SessionChanged {
                    route,
                    stream_id,
                    new_id,
                    old_id: _
                } => {
                    let mut stats = stats_clone.lock().unwrap();
                    let key = StreamKey {
                        route: route.to_string(),
                        stream_id,
                    };
                    if let Some(st) = stats.get_mut(&key) {
                        st.reset_for_new_session(new_id);
                    }
                }

                BufferEvent::SamplesSkipped {
                    route,
                    stream_id,
                    count,
                    ..
                } => {
                    let mut stats = stats_clone.lock().unwrap();
                    let key = StreamKey {
                        route: route.to_string(),
                        stream_id,
                    };
                    if let Some(st) = stats.get_mut(&key) {
                        st.samples_dropped += count as u64;
                    }
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
                    let quit = k.code == KeyCode::Char('q')
                             || k.code == KeyCode::Esc
                             || (k.code == KeyCode::Char('c') && k.modifiers == KeyModifiers::CONTROL);
                    if quit { break 'main; }
                }
            }

            recv(tick) -> _ => {
                let now = Instant::now();

                // Build display rows
                let mut rows = Vec::new();
                {
                    let mut stats = stats.lock().unwrap();
                    for (key, st) in stats.iter_mut() {
                        let rate = st.compute_rate(now, rate_window);
                        let stale = st.last_seen.map(|t| now.duration_since(t) > stale_dur).unwrap_or(true);

                        // Reset timing on stale
                        if stale && st.t0_host.is_some() {
                            st.t0_host = None;
                            st.t0_data = None;
                            st.last_host = None;
                            st.last_data = None;
                            st.drift_s = 0.0;
                            st.ppm = 0.0;
                            st.jitter_ms = 0.0;
                            st.jitter_window = None;
                            st.last_n = None;
                        }

                        let abs_ppm = st.ppm.abs();
                        let status = if !st.metadata_ok {
                            "META"
                        } else if stale {
                            "STALLED"
                        } else if abs_ppm >= cli.ppm_err {
                            "ERROR"
                        } else if abs_ppm >= cli.ppm_warn {
                            "WARN"
                        } else {
                            "OK"
                        };

                        rows.push((
                            key.route.clone(),
                            key.stream_id,
                            st.name.clone(),
                            if st.metadata_ok { rate } else { 0.0 },
                            if st.metadata_ok { st.drift_s } else { 0.0 },
                            if st.metadata_ok { st.ppm } else { 0.0 },
                            if st.metadata_ok { st.jitter_ms } else { 0.0 },
                            if st.metadata_ok { st.samples_dropped } else { 0 },
                            st.last_n.unwrap_or(0),
                            st.last_data.unwrap_or(0.0),
                            status,
                            stale,
                        ));
                    }
                }

                rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

                let header = format!(
                    "tio-health — rate_window={}s  jitter_window={}s  warn/err={}ppm/{}ppm  fps={}  stale={}ms",
                    cli.rate_window_s, cli.jitter_window_s, cli.ppm_warn, cli.ppm_err, cli.fps, cli.stale_ms
                );

                let log = event_log.lock().unwrap().clone();

                if tui.draw(&header, &rows, &log, cli.event_log_size, cli.show_warnings_only, cli.quiet).is_err() {
                    break 'main;
                }
            }
        }
    }

    tui.teardown();
}
