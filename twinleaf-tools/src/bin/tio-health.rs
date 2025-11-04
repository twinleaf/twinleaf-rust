// tio-health
//
// Simplified live timing & rate diagnostics by device route.
//
// Deps (Cargo.toml):
//   crossterm = "0.27"
//   crossbeam = "0.8"
//   getopts = "0.2"
//   twinleaf = "*"
//   twinleaf-tools = "*"
//   chrono = "0.4"
//
// Build: cargo run --release -- <tio-url> [route] [options]
// Quit:  q / Esc / Ctrl-C

use crossbeam::channel;
use crossterm::style::{Attribute, Color, ResetColor, SetAttribute, SetForegroundColor};
use crossterm::{cursor, event, style, terminal, ExecutableCommand, QueueableCommand};
use std::collections::{BTreeMap, VecDeque};
use std::io::{self, Write};
use std::time::{Duration, Instant};
use twinleaf::device::DeviceTree;
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
    opts.optopt("", "rate-window", "Seconds for rate window (default 5)", "sec");
    opts.optopt("", "jitter-window", "Seconds for jitter window (default 10)", "sec");
    opts.optopt("", "ppm-warn", "Warn threshold in ppm (default 100)", "ppm");
    opts.optopt("", "ppm-err", "Error threshold in ppm (default 200)", "ppm");
    opts.optopt("", "streams", "Comma-separated stream IDs to include", "list");
    opts.optflag("", "quiet", "Suppress footer hint");
    opts.optopt("", "fps", "UI refresh rate (default 10)", "n");
    opts.optopt("", "stale-ms", "Grey rows after this many ms (default 2000)", "ms");

    let (matches, root, route) = tio_parseopts(&opts, &std::env::args().collect::<Vec<_>>());
    if matches.opt_present("help") {
        print_help_and_exit(&opts, &std::env::args().next().unwrap_or_else(|| "tio-health".into()), 0);
    }

    let rate_window_s = matches.opt_str("rate-window").as_deref().unwrap_or("5").parse().unwrap_or(5);
    let jitter_window_s = matches.opt_str("jitter-window").as_deref().unwrap_or("10").parse().unwrap_or(10);
    let ppm_warn = matches.opt_str("ppm-warn").as_deref().unwrap_or("100").parse().unwrap_or(100.0);
    let ppm_err = matches.opt_str("ppm-err").as_deref().unwrap_or("200").parse().unwrap_or(200.0);
    let streams_filter = matches.opt_str("streams").map(|s| {
        s.split(',').filter_map(|x| x.trim().parse::<u8>().ok()).collect()
    });
    let quiet = matches.opt_present("quiet");
    let fps = matches.opt_str("fps").as_deref().unwrap_or("10").parse().unwrap_or(10);
    let stale_ms = matches.opt_str("stale-ms").as_deref().unwrap_or("2000").parse().unwrap_or(2000);

    (root, route, Cli {
        rate_window_s,
        jitter_window_s,
        ppm_warn,
        ppm_err,
        streams_filter,
        quiet,
        fps,
        stale_ms,
    })
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
        Self { buf: vec![0.0; cap], cap, idx: 0, filled: false }
    }
    
    fn push(&mut self, v: f64) {
        self.buf[self.idx] = v;
        self.idx = (self.idx + 1) % self.cap;
        if self.idx == 0 { self.filled = true; }
    }
    
    fn std_ms(&self) -> f64 {
        let n = if self.filled { self.cap } else { self.idx };
        if n == 0 { return 0.0; }
        let mean: f64 = self.buf[..n].iter().sum::<f64>() / (n as f64);
        let var: f64 = self.buf[..n].iter().map(|x| (x - mean) * (x - mean)).sum::<f64>() / (n as f64);
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
    
    // Rate
    arrivals: VecDeque<Instant>,
    rate_smps: f64,
    
    // Metadata
    name: String,
    last_seen: Option<Instant>,
}

impl StreamStats {
    fn ensure_jitter_window(&mut self, seconds: u64) {
        if self.jitter_window.is_none() {
            self.jitter_window = Some(TimeWindow::new(seconds, 100.0));
        }
    }
    
    fn on_sample(&mut self, sample_n: u32, t_data: f64, now: Instant, jitter_window_s: u64) {
        self.ensure_jitter_window(jitter_window_s);
        
        // First sample initialization
        if self.t0_host.is_none() { self.t0_host = Some(now); }
        if self.t0_data.is_none() { self.t0_data = Some(t_data); }
        
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
        
        // Track dropped samples (wrap-safe)
        if let Some(last) = self.last_n {
            let expected = last.wrapping_add(1);
            if sample_n != expected {
                // Handle forward gaps (drops)
                if sample_n > last {
                    let missing = sample_n - last - 1;
                    self.samples_dropped += missing as u64;
                }
                // Backward/non-monotonic: just update baseline without counting
            }
        }
        self.last_n = Some(sample_n);
        
        // Rate tracking
        self.arrivals.push_back(now);
        self.last_seen = Some(now);
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
        rows: &[(String, u8, String, f64, f64, f64, f64, u64, u32, f64, &'static str, bool)],
        quiet: bool,
    ) -> io::Result<()> {
        self.stdout.queue(cursor::MoveTo(0, 0))?;
        self.stdout.queue(terminal::Clear(terminal::ClearType::All))?;
        
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
            "route", "sid", "stream_name", "smps/s", "drift(s)", "ppm", "jitter(ms)", "dropped", "last_n", "last_time", "status"
        )))?;
        self.stdout.queue(SetAttribute(Attribute::Reset))?;
        self.stdout.queue(cursor::MoveToNextLine(1))?;
        
        // Data rows
        let mut last_route = String::new();
        for (route, sid, name, rate, drift, ppm, jitter, dropped, last_n, last_timestamp, status, stale) in rows {
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
        
        if !quiet {
            self.stdout.queue(cursor::MoveToNextLine(2))?;
            self.stdout.queue(style::Print("q/Esc to quit"))?;
        }
        
        self.stdout.flush()
    }
}

fn main() {
    let (root, route, cli) = parse_cli();
    
    let mut tui = Tui::setup().expect("TUI setup failed");
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let mut t = Tui { stdout: io::stdout() };
        t.teardown();
        original_hook(panic_info);
    }));
    
    // Open device tree
    let proxy = tio::proxy::Interface::new(&root);
    let mut tree = match DeviceTree::open(&proxy, route.clone()) {
        Ok(t) => t,
        Err(e) => {
            tui.teardown();
            eprintln!("Failed to open device tree: {:?}", e);
            std::process::exit(1);
        }
    };
    
    // Per-stream statistics
    let mut stats: BTreeMap<StreamKey, StreamStats> = BTreeMap::new();
    
    // Configuration
    let rate_window = Duration::from_secs(cli.rate_window_s.max(1));
    let stale_dur = Duration::from_millis(cli.stale_ms.max(1));
    let frame = Duration::from_millis((1000 / cli.fps.max(1)) as u64);
    let tick = channel::tick(frame);
    
    // Keyboard handler
    let (key_tx, key_rx) = channel::unbounded();
    std::thread::spawn(move || loop {
        if let Ok(ev) = event::read() {
            let _ = key_tx.send(ev);
        } else {
            std::thread::sleep(Duration::from_millis(10));
        }
    });
    
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
                // Drain all available samples
                let samples = match tree.drain() {
                    Ok(s) => s,
                    Err(_) => {
                        tui.teardown();
                        eprintln!("Error draining device tree");
                        std::process::exit(1);
                    }
                };
                
                let now = Instant::now();
                
                // Process each sample
                for (sample, route) in samples {
                    let route_str = route.to_string();
                    let sid = sample.stream.stream_id as u8;
                    
                    // Apply stream filter if specified
                    if let Some(ref filter) = cli.streams_filter {
                        if !filter.contains(&sid) {
                            continue;
                        }
                    }
                    
                    let key = StreamKey { route: route_str, stream_id: sid };
                    let st = stats.entry(key).or_insert_with(|| {
                        StreamStats {
                            name: sample.stream.name.clone(),
                            ..Default::default()
                        }
                    });
                    
                    st.on_sample(
                        sample.n,
                        sample.timestamp_end(),
                        now,
                        cli.jitter_window_s,
                    );
                }
                
                // Build display rows
                let mut rows = Vec::new();
                for (key, st) in &mut stats {
                    let rate = st.compute_rate(now, rate_window);
                    let stale = st.last_seen.map(|t| now.duration_since(t) > stale_dur).unwrap_or(true);
                    
                    // Reset timing data if stream went stale (handles device reboots)
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
                    let status = if stale {
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
                        rate,
                        st.drift_s,
                        st.ppm,
                        st.jitter_ms,
                        st.samples_dropped,
                        st.last_n.unwrap_or(0),
                         st.last_data.unwrap_or(0.0),
                        status,
                        stale,
                    ));
                }
                
                rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
                
                let header = format!(
                    "tio-health — rate_window={}s  jitter_window={}s  warn/err={}ppm/{}ppm  fps={}  stale={}ms",
                    cli.rate_window_s, cli.jitter_window_s, cli.ppm_warn, cli.ppm_err, cli.fps, cli.stale_ms
                );
                
                if tui.draw(&header, &rows, cli.quiet).is_err() {
                    break 'main;
                }
            }
        }
    }
    
    tui.teardown();
}
