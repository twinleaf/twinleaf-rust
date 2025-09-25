// tio-health
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
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::{self, Write};
use std::time::{Duration, Instant, SystemTime};
use twinleaf::device::{DeviceTreeBuilder, Frozen};
use twinleaf::tio;
use twinleaf_tools::{tio_opts, tio_parseopts};

// Pretty local timestamps
use chrono::{DateTime, Local};

// ---------------- CLI ----------------
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    All,  // show time + rate columns
    Time, // only time/ppm/jitter
    Rate, // only smps/s
}

#[derive(Debug)]
struct Cli {
    mode: Mode,
    rate_window_s: u64,   // window for smps/s (sliding)
    jitter_window_s: u64, // window for jitter stddev
    ppm_warn: f64,
    ppm_err: f64,
    spike_ms: f64,
    rate_warn_pct: f64, // warn if smps/s < (1 - pct%) * baseline
    streams_filter: Option<Vec<u8>>,
    quiet: bool,
    fps: u64,
    show_columns: bool,
    stale_ms: u64, // grey rows when no samples in this time
}

fn print_help_and_exit(opts: &getopts::Options, program: &str, code: i32) -> ! {
    let brief = format!("Usage: {program} [options] <tio-url> [route]\n\nLive timing & rate diagnostics by device route.");
    let usage = opts.usage(&brief);
    eprintln!("{usage}");
    std::process::exit(code)
}

fn parse_cli() -> (String, tio::proto::DeviceRoute, Cli) {
    let mut opts = tio_opts();
    opts.optflag("h", "help", "Show help.");
    opts.optopt(
        "",
        "mode",
        "Diagnostic mode: all|time|rate (default all)",
        "m",
    );
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
    opts.optopt("", "spike-ms", "Spike threshold in ms (default 20)", "ms");
    opts.optopt(
        "",
        "rate-warn-pct",
        "Warn if smps/s drops by this % from baseline (default 5.0)",
        "%",
    );
    opts.optopt(
        "",
        "streams",
        "Comma-separated list of stream IDs to include",
        "list",
    );
    opts.optflag("", "quiet", "Suppress footer hint");
    opts.optopt("", "fps", "UI refresh frames per second (default 10)", "n");
    opts.optflag("", "columns", "Show stream->column breakdown header");
    opts.optopt(
        "",
        "stale-ms",
        "Grey rows after this many ms without samples (default 2000)",
        "ms",
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

    let mode = match matches.opt_str("mode").as_deref() {
        Some("time") => Mode::Time,
        Some("rate") => Mode::Rate,
        _ => Mode::All,
    };
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
    let spike_ms = matches
        .opt_str("spike-ms")
        .as_deref()
        .unwrap_or("20")
        .parse()
        .unwrap_or(20.0);
    let rate_warn_pct = matches
        .opt_str("rate-warn-pct")
        .as_deref()
        .unwrap_or("5.0")
        .parse()
        .unwrap_or(5.0);
    let streams_filter = matches.opt_str("streams").map(|s| {
        s.split(',')
            .filter_map(|x| x.trim().parse::<u8>().ok())
            .collect::<Vec<_>>()
    });
    let quiet = matches.opt_present("quiet");
    let fps = matches
        .opt_str("fps")
        .as_deref()
        .unwrap_or("10")
        .parse()
        .unwrap_or(10);
    let show_columns = matches.opt_present("columns");
    let stale_ms = matches
        .opt_str("stale-ms")
        .as_deref()
        .unwrap_or("2000")
        .parse()
        .unwrap_or(2000);

    (
        root,
        route,
        Cli {
            mode,
            rate_window_s,
            jitter_window_s,
            ppm_warn,
            ppm_err,
            spike_ms,
            rate_warn_pct,
            streams_filter,
            quiet,
            fps,
            show_columns,
            stale_ms,
        },
    )
}

// ---------------- Keys ----------------
#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct StreamKey {
    route: String,
    stream_id: u8,
}
impl StreamKey {
    fn new(route: String, stream_id: u8) -> Self {
        Self { route, stream_id }
    }
}

// ---------------- Time + Sample Stats ----------------
#[derive(Clone)]
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

#[derive(Default, Clone)]
struct TimeStats {
    t0_host: Option<Instant>,
    t0_data: Option<f64>,
    last_host: Option<Instant>,
    last_data: Option<f64>,
    host_elapsed: f64,
    data_elapsed: f64,
    drift_s: f64,
    ppm: f64,
    jitter_ms: f64,
    last_spike_ms: f64,
    win: Option<TimeWindow>,
    last_seen: Option<Instant>,
}
impl TimeStats {
    fn ensure_window(&mut self, seconds: u64) {
        if self.win.is_none() {
            self.win = Some(TimeWindow::new(seconds, 100.0));
        }
    }
    fn on_sample(&mut self, t_data: f64, now: Instant, spike_ms: f64, window_s: u64) {
        self.ensure_window(window_s);
        if self.t0_host.is_none() {
            self.t0_host = Some(now);
        }
        if self.t0_data.is_none() {
            self.t0_data = Some(t_data);
        }
        if let (Some(lh), Some(ld)) = (self.last_host, self.last_data) {
            let dh = now.duration_since(lh).as_secs_f64();
            let dd = t_data - ld;
            let delta_ms = (dd - dh) * 1000.0;
            if let Some(w) = &mut self.win {
                w.push(delta_ms);
                self.jitter_ms = w.std_ms();
            }
            if delta_ms.abs() > spike_ms {
                self.last_spike_ms = delta_ms;
            }
        }
        self.last_host = Some(now);
        self.last_data = Some(t_data);
        let h0 = self.t0_host.unwrap();
        let d0 = self.t0_data.unwrap();
        self.host_elapsed = now.duration_since(h0).as_secs_f64();
        self.data_elapsed = t_data - d0;
        self.drift_s = self.data_elapsed - self.host_elapsed;
        if self.host_elapsed.abs() > 1e-9 {
            self.ppm = 1e6 * self.drift_s / self.host_elapsed;
        }
        self.last_seen = Some(now);
    }
}

#[derive(Clone)]
struct SampleStats {
    arrivals: VecDeque<Instant>, // timestamps of arrivals (for sliding window)
    rate_smps: f64,              // last computed live rate
    baseline_smps: f64,          // max observed live rate
}
impl Default for SampleStats {
    fn default() -> Self {
        Self {
            arrivals: VecDeque::new(),
            rate_smps: 0.0,
            baseline_smps: 0.0,
        }
    }
}
impl SampleStats {
    fn on_sample(&mut self, now: Instant) {
        self.arrivals.push_back(now);
    }

    /// Compute sliding-window rate and update baseline.
    fn rate_living(&mut self, now: Instant, window: Duration) -> f64 {
        // Cutoff time for the window; if underflow, just use 'now'
        let cutoff = now.checked_sub(window).unwrap_or(now);

        // Drop arrivals older than the window
        while let Some(&front) = self.arrivals.front() {
            if front < cutoff {
                self.arrivals.pop_front();
            } else {
                break;
            }
        }

        // Compute rate as count per window seconds
        let count = self.arrivals.len() as f64;
        let win_secs = window.as_secs_f64().max(1e-6);
        let rate = count / win_secs;

        self.rate_smps = rate;
        if rate > self.baseline_smps {
            self.baseline_smps = rate;
        }
        rate
    }
}

// ---------------- Skip/Error tracking ----------------

#[derive(Default, Clone)]
struct SeqStats {
    last_n: Option<u32>,
}

#[derive(Clone)]
struct SkipEvent {
    when: SystemTime, // wall-clock
    route: String,
    stream_id: u8,
    from: u32,
    to: u32,
    missing: u32,
}

#[derive(Clone)]
struct ErrorEvent {
    when: SystemTime, // wall-clock
    route: String,
    stream_id: u8,
    ppm: f64,
    drift_s: f64,
}

struct DiagSnapshot {
    // Skips
    recent_skips: Vec<SkipEvent>,
    by_route_skips: Vec<(String, u64, u64)>, // route, missing_total, events_total
    skip_total_events: u64,

    // Errors
    recent_errors: Vec<ErrorEvent>,
    by_route_errors: Vec<(String, u64)>, // route, events_total
    error_total_events: u64,
}

// ---------------- TUI ----------------
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

    // widths
    const ROUTE_W: usize = 10;
    const SID_W: usize = 3;
    const NAME_W: usize = 22;
    const SMPS_W: usize = 9;
    const DRIFT_W: usize = 9;
    const PPM_W: usize = 8;
    const JIT_W: usize = 11;
    const SPIK_W: usize = 13;
    const STAT_W: usize = 8;

    // extra widths for diagnostics
    const WHEN_W: usize = 19; // "YYYY-MM-DD HH:MM:SS"
    const MISS_W: usize = 9;  // width for missing count
    const EVTS_W: usize = 6;  // width for event count

    fn section_header(&mut self, title: &str) -> io::Result<()> {
        self.stdout.queue(SetAttribute(Attribute::Bold))?;
        self.stdout.queue(style::Print(title))?;
        self.stdout.queue(SetAttribute(Attribute::Reset))?;
        self.stdout.queue(cursor::MoveToNextLine(1))?;
        Ok(())
    }

    fn fmt_when(when: SystemTime) -> String {
        let when: DateTime<Local> = DateTime::<Local>::from(when);
        when.format("%F %T").to_string()
    }

    fn render_recent_skips(&mut self, recent: &[SkipEvent], total: u64) -> io::Result<()> {
        self.section_header(&format!("Recent skips (max 5) — total events: {total}"))?;
        if recent.is_empty() {
            self.stdout.queue(style::Print("None"))?;
            self.stdout.queue(cursor::MoveToNextLine(1))?;
            return Ok(());
        }
        for ev in recent.iter().rev() {
            let when_str = Self::fmt_when(ev.when);
            self.stdout.queue(SetForegroundColor(Color::Yellow))?;
            self.stdout.queue(style::Print(format!(
                "{when:<when_w$} {route:<route_w$} {sid:>sid_w$}  {from}->{to}  (miss {miss})",
                when = when_str,
                route = ev.route,
                sid = ev.stream_id,
                from = ev.from,
                to = ev.to,
                miss = ev.missing,
                when_w = Self::WHEN_W,
                route_w = Self::ROUTE_W,
                sid_w = Self::SID_W
            )))?;
            self.stdout.queue(ResetColor)?;
            self.stdout.queue(cursor::MoveToNextLine(1))?;
        }
        Ok(())
    }

    fn render_skips_by_route(&mut self, rows: &[(String, u64, u64)]) -> io::Result<()> {
        self.section_header("Skips by route (missing / events):")?;
        if rows.is_empty() {
            self.stdout.queue(style::Print("None"))?;
            self.stdout.queue(cursor::MoveToNextLine(1))?;
            return Ok(());
        }
        self.stdout.queue(SetAttribute(Attribute::Bold))?;
        self.stdout.queue(style::Print(format!(
            "{route:<route_w$}  {miss:>miss_w$} / {evts:>evts_w$}",
            route = "route",
            miss = "missing",
            evts = "events",
            route_w = Self::ROUTE_W,
            miss_w = Self::MISS_W,
            evts_w = Self::EVTS_W,
        )))?;
        self.stdout.queue(SetAttribute(Attribute::Reset))?;
        self.stdout.queue(cursor::MoveToNextLine(1))?;
        for (route, missing, events) in rows {
            self.stdout.queue(style::Print(format!(
                "{route:<route_w$}  {miss:>miss_w$} / {evts:>evts_w$}",
                route = route,
                miss = missing,
                evts = events,
                route_w = Self::ROUTE_W,
                miss_w = Self::MISS_W,
                evts_w = Self::EVTS_W,
            )))?;
            self.stdout.queue(cursor::MoveToNextLine(1))?;
        }
        Ok(())
    }

    fn render_recent_errors(&mut self, recent: &[ErrorEvent], total: u64) -> io::Result<()> {
        self.section_header(&format!("Recent errors (max 5) — total events: {total}"))?;
        if recent.is_empty() {
            self.stdout.queue(style::Print("None"))?;
            self.stdout.queue(cursor::MoveToNextLine(1))?;
            return Ok(());
        }
        for ev in recent.iter().rev() {
            let when_str = Self::fmt_when(ev.when);
            self.stdout.queue(SetForegroundColor(Color::Red))?;
            self.stdout.queue(style::Print(format!(
                "{when:<when_w$} {route:<route_w$} {sid:>sid_w$}  ppm={ppm:>6.0}  drift={drift:+.3}s",
                when = when_str,
                route = ev.route,
                sid = ev.stream_id,
                ppm = ev.ppm,
                drift = ev.drift_s,
                when_w = Self::WHEN_W,
                route_w = Self::ROUTE_W,
                sid_w = Self::SID_W
            )))?;
            self.stdout.queue(ResetColor)?;
            self.stdout.queue(cursor::MoveToNextLine(1))?;
        }
        Ok(())
    }

    fn render_errors_by_route(&mut self, rows: &[(String, u64)]) -> io::Result<()> {
        self.section_header("Errors by route (events):")?;
        if rows.is_empty() {
            self.stdout.queue(style::Print("None"))?;
            self.stdout.queue(cursor::MoveToNextLine(1))?;
            return Ok(());
        }
        self.stdout.queue(SetAttribute(Attribute::Bold))?;
        self.stdout.queue(style::Print(format!(
            "{route:<route_w$}  {evts:>evts_w$}",
            route = "route",
            evts = "events",
            route_w = Self::ROUTE_W,
            evts_w = Self::EVTS_W,
        )))?;
        self.stdout.queue(SetAttribute(Attribute::Reset))?;
        self.stdout.queue(cursor::MoveToNextLine(1))?;
        for (route, events) in rows {
            self.stdout.queue(style::Print(format!(
                "{route:<route_w$}  {evts:>evts_w$}",
                route = route,
                evts = events,
                route_w = Self::ROUTE_W,
                evts_w = Self::EVTS_W,
            )))?;
            self.stdout.queue(cursor::MoveToNextLine(1))?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn draw(
        &mut self,
        header: &str,
        rows: &[(
            String,       // route
            u8,           // sid
            String,       // name
            f64,          // smps/s
            f64,          // drift_s
            f64,          // ppm
            f64,          // jitter_ms
            f64,          // last_spike_ms
            &'static str, // status
            bool,         // stale
            bool,         // bold_ppm (ERROR cause)
        )],
        meta_rows: &[(String, u8, String, Vec<(String, String)>)],
        show_columns: bool,
        quiet: bool,
        mode: Mode,
        diag: &DiagSnapshot,
    ) -> io::Result<()> {
        self.stdout.queue(cursor::MoveTo(0, 0))?;
        self.stdout
            .queue(terminal::Clear(terminal::ClearType::All))?;
        self.stdout.queue(SetAttribute(Attribute::Bold))?;
        self.stdout.queue(style::Print(header))?;
        self.stdout.queue(SetAttribute(Attribute::Reset))?;
        self.stdout.queue(cursor::MoveToNextLine(1))?;

        if show_columns && !meta_rows.is_empty() {
            self.section_header("Streams & columns:")?;
            let mut last_route = String::new();
            for (route, sid, sname, cols) in meta_rows {
                if *route != last_route {
                    if !last_route.is_empty() {
                        self.stdout.queue(cursor::MoveToNextLine(1))?;
                    }
                    last_route = route.clone();
                }
                self.stdout.queue(style::Print(format!(
                    "{:>sid_w$}  {:<name_w$}",
                    sid,
                    sname,
                    sid_w = Self::SID_W,
                    name_w = Self::NAME_W
                )))?;
                if !cols.is_empty() {
                    let colnames: Vec<String> = cols
                        .iter()
                        .map(|(n, u)| {
                            if u.is_empty() {
                                n.clone()
                            } else {
                                format!("{} [{}]", n, u)
                            }
                        })
                        .collect();
                    self.stdout
                        .queue(style::Print(format!("  -> {}", colnames.join(", "))))?;
                }
                self.stdout.queue(cursor::MoveToNextLine(1))?;
            }
            self.stdout.queue(cursor::MoveToNextLine(1))?;
        }

        self.section_header("Live:")?;
        self.stdout.queue(SetAttribute(Attribute::Bold))?;
        match mode {
            Mode::All | Mode::Rate => {
                self.stdout.queue(style::Print(format!(
                    "{route:<route_w$} {sid:>sid_w$}  {name:<name_w$}  {smps:>smps_w$}",
                    route = "route",
                    sid = "sid",
                    name = "stream_name",
                    smps = "smps/s",
                    route_w = Self::ROUTE_W,
                    sid_w = Self::SID_W,
                    name_w = Self::NAME_W,
                    smps_w = Self::SMPS_W
                )))?;
            }
            Mode::Time => {
                self.stdout.queue(style::Print(format!(
                    "{route:<route_w$} {sid:>sid_w$}  {name:<name_w$}",
                    route = "route",
                    sid = "sid",
                    name = "stream_name",
                    route_w = Self::ROUTE_W,
                    sid_w = Self::SID_W,
                    name_w = Self::NAME_W
                )))?;
            }
        }
        if mode != Mode::Rate {
            self.stdout.queue(style::Print(format!(
                "  {drift:>drift_w$}  {ppm:>ppm_w$}  {jit:>jit_w$}  {spk:>spik_w$}  {stat:>stat_w$}",
                drift="drift(s)", ppm="ppm", jit="jitter(ms)", spk="last_spike(ms)", stat="status",
                drift_w=Self::DRIFT_W, ppm_w=Self::PPM_W, jit_w=Self::JIT_W, spik_w=Self::SPIK_W, stat_w=Self::STAT_W
            )))?;
        }
        self.stdout.queue(SetAttribute(Attribute::Reset))?;
        self.stdout.queue(cursor::MoveToNextLine(1))?;

        let mut last_route = String::new();
        for (route, sid, sname, smps_rate, drift_s, ppm, jitter_ms, last_spike_ms, status, stale, bold_ppm) in
            rows
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
                    "WARN" | "RATE↓" => Color::Yellow,
                    "OK" => Color::Green,
                    _ => Color::White,
                }
            };
            self.stdout.queue(SetForegroundColor(color))?;

            match mode {
                Mode::All | Mode::Rate => {
                    self.stdout.queue(style::Print(format!(
                        "{route:<route_w$} {sid:>sid_w$}  {name:<name_w$}  {rsmps:>smps_w$.1}",
                        route = route,
                        sid = sid,
                        name = sname,
                        rsmps = smps_rate,
                        route_w = Self::ROUTE_W,
                        sid_w = Self::SID_W,
                        name_w = Self::NAME_W,
                        smps_w = Self::SMPS_W
                    )))?;
                }
                Mode::Time => {
                    self.stdout.queue(style::Print(format!(
                        "{route:<route_w$} {sid:>sid_w$}  {name:<name_w$}",
                        route = route,
                        sid = sid,
                        name = sname,
                        route_w = Self::ROUTE_W,
                        sid_w = Self::SID_W,
                        name_w = Self::NAME_W
                    )))?;
                }
            }
            if mode != Mode::Rate {
                // Drift
                self.stdout.queue(style::Print(format!(
                    "  {drift:>drift_w$.3}  ",
                    drift = drift_s,
                    drift_w = Self::DRIFT_W
                )))?;

                // PPM (bold only this cell if it's the offending column)
                if *bold_ppm {
                    self.stdout.queue(SetAttribute(Attribute::Bold))?;
                }
                self.stdout.queue(style::Print(format!(
                    "{ppm:>ppm_w$.0}",
                    ppm = ppm,
                    ppm_w = Self::PPM_W
                )))?;
                if *bold_ppm {
                    // reset attributes but keep color
                    self.stdout.queue(SetAttribute(Attribute::Reset))?;
                    self.stdout.queue(SetForegroundColor(color))?;
                }

                // Jitter, spike, status
                self.stdout.queue(style::Print(format!(
                    "  {jit:>jit_w$.2}  {spk:>spik_w$.2}  {stat:>stat_w$}",
                    jit = jitter_ms,
                    spk = last_spike_ms,
                    stat = status,
                    jit_w = Self::JIT_W,
                    spik_w = Self::SPIK_W,
                    stat_w = Self::STAT_W
                )))?;
            }
            self.stdout.queue(ResetColor)?;
            self.stdout.queue(cursor::MoveToNextLine(1))?;
        }

        // ------- Diagnostics sections -------
        self.stdout.queue(cursor::MoveToNextLine(1))?;

        self.render_recent_skips(&diag.recent_skips, diag.skip_total_events)?;
        self.stdout.queue(cursor::MoveToNextLine(1))?;
        self.render_skips_by_route(&diag.by_route_skips)?;
        self.stdout.queue(cursor::MoveToNextLine(1))?;

        self.render_recent_errors(&diag.recent_errors, diag.error_total_events)?;
        self.stdout.queue(cursor::MoveToNextLine(1))?;
        self.render_errors_by_route(&diag.by_route_errors)?;

        if !quiet {
            self.stdout.queue(cursor::MoveToNextLine(2))?;
            self.stdout.queue(style::Print("q/Esc to quit"))?;
        }
        self.stdout.flush()
    }
}

// ---------------- Helpers ----------------
fn drain_tree_once(
    tree: &mut twinleaf::device::DeviceTree<Frozen>,
    streams_filter: &Option<Vec<u8>>,
    jitter_window_s: u64,
    spike_ms: f64,
    time_stats: &mut BTreeMap<StreamKey, TimeStats>,
    smp_stats: &mut BTreeMap<StreamKey, SampleStats>,
    stream_names: &mut BTreeMap<StreamKey, String>,
    stream_cols: &mut BTreeMap<StreamKey, Vec<(String, String)>>,
    // --- skip tracking accumulators ---
    seq_stats: &mut BTreeMap<StreamKey, SeqStats>,
    recent_skips: &mut VecDeque<SkipEvent>,
    route_skip_missing: &mut BTreeMap<String, u64>,
    route_skip_events: &mut BTreeMap<String, u64>,
    skip_occ_total: &mut u64,
) {
    loop {
        match tree.try_next() {
            Ok(Some((sample, route))) => {
                let now = Instant::now();
                let route_str = route.to_string();
                let sid = sample.stream.stream_id as u8;
                if let Some(f) = streams_filter {
                    if !f.contains(&sid) {
                        continue;
                    }
                }
                let key = StreamKey::new(route_str.clone(), sid);

                // Time + jitter
                let ts = time_stats.entry(key.clone()).or_default();
                ts.on_sample(sample.timestamp_end(), now, spike_ms, jitter_window_s);

                // Rate arrivals
                let ss = smp_stats.entry(key.clone()).or_default();
                ss.on_sample(now);

                // Name + columns (memoize)
                stream_names
                    .entry(key.clone())
                    .or_insert_with(|| sample.stream.name.clone());

                if let Some(cols) = {
                    let cols = sample
                        .columns
                        .iter()
                        .map(|c| (c.desc.name.clone(), c.desc.units.clone()))
                        .collect::<Vec<_>>();
                    if cols.is_empty() {
                        None
                    } else {
                        Some(cols)
                    }
                } {
                    stream_cols.insert(key.clone(), cols);
                }

                // --- Sequence gap detection (emulates awk check of non-consecutive sample numbers) ---
                // We assume a per-sample u32 counter `sample.n` that increments by 1 each sample, wrapping at u32::MAX.
                // If your type exposes a different field name, adjust here.
                let n: u32 = sample.n;

                let seq = seq_stats.entry(key).or_default();
                if let Some(last) = seq.last_n {
                    let expected = last.wrapping_add(1);
                    if n != expected {
                        if n > last {
                            let missing = n - last - 1;
                            if missing > 0 {
                                *skip_occ_total += 1;
                                *route_skip_missing
                                    .entry(route_str.clone())
                                    .or_insert(0) += missing as u64;
                                *route_skip_events
                                    .entry(route_str.clone())
                                    .or_insert(0) += 1;

                                recent_skips.push_back(SkipEvent {
                                    when: SystemTime::now(),
                                    route: route_str.clone(),
                                    stream_id: sid,
                                    from: last,
                                    to: n,
                                    missing,
                                });
                                if recent_skips.len() > 5 {
                                    recent_skips.pop_front();
                                }
                            }
                        } else {
                            // Backward/reset: treat as re-baseline without logging.
                        }
                    }
                }
                seq.last_n = Some(n);
            }
            Ok(None) => break,
            Err(_) => break, // Keep UI alive; we can add logging if desired
        }
    }
}

// ---------------- Main ----------------
fn main() {
    let (root, route, cli) = parse_cli();

    // TUI guard for panics
    let mut tui = Tui::setup().expect("TUI setup failed");
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let mut t = Tui {
            stdout: io::stdout(),
        };
        t.teardown();
        original_hook(panic_info);
    }));

    // Build device tree: single proxy, discover briefly, prefetch metadata, freeze, sort.
    let proxy = tio::proxy::Interface::new(&root);
    let mut tree = match DeviceTreeBuilder::new(proxy, route.clone())
        .discover_for(Duration::from_secs(2))
        .prefetch_metadata()
        .freeze()
        .sort_by_route()
        .build()
    {
        Ok(t) => t,
        Err(e) => {
            tui.teardown();
            eprintln!("Failed to build device tree: {:?}", e);
            std::process::exit(1);
        }
    };

    // Per-(route,sid) stats
    let mut time_stats: BTreeMap<StreamKey, TimeStats> = BTreeMap::new();
    let mut smp_stats: BTreeMap<StreamKey, SampleStats> = BTreeMap::new();
    let mut stream_names: BTreeMap<StreamKey, String> = BTreeMap::new();
    let mut stream_cols: BTreeMap<StreamKey, Vec<(String, String)>> = BTreeMap::new();

    // Skip diagnostics
    let mut seq_stats: BTreeMap<StreamKey, SeqStats> = BTreeMap::new();
    let mut recent_skips: VecDeque<SkipEvent> = VecDeque::new();
    let mut route_skip_missing: BTreeMap<String, u64> = BTreeMap::new(); // sum of missing samples
    let mut route_skip_events: BTreeMap<String, u64> = BTreeMap::new(); // number of skip events
    let mut skip_occ_total: u64 = 0; // global tally of skip occurrences

    // Error diagnostics
    let mut recent_errors: VecDeque<ErrorEvent> = VecDeque::new();
    let mut route_error_events: BTreeMap<String, u64> = BTreeMap::new();
    let mut error_occ_total: u64 = 0;
    let mut stream_in_error: BTreeMap<StreamKey, bool> = BTreeMap::new(); // edge-trigger on transitions

    // Tunables
    let rate_window = Duration::from_secs(cli.rate_window_s.max(1));
    let stale_dur = Duration::from_millis(cli.stale_ms.max(1));

    // Redraw cadence
    let frame = Duration::from_millis((1000 / cli.fps.max(1)) as u64);
    let tick = channel::tick(frame);

    // Keyboard reader thread -> channel
    let (key_tx, key_rx) = channel::unbounded();
    std::thread::spawn({
        let key_tx = key_tx.clone();
        move || loop {
            if let Ok(ev) = event::read() {
                let _ = key_tx.send(ev);
            } else {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    });

    'main: loop {
        crossbeam::select! {
            recv(key_rx) -> ev => {
                if let Ok(crossterm::event::Event::Key(k)) = ev {
                    use crossterm::event::{KeyCode, KeyModifiers};
                    let quit = k.code == KeyCode::Char('q')
                             || k.code == KeyCode::Esc
                             || (k.code == KeyCode::Char('c') && k.modifiers == KeyModifiers::CONTROL);
                    if quit { break 'main; }
                }
            }

            recv(tick) -> _ => {
                // Pull samples from the tree (also does skip detection)
                drain_tree_once(
                    &mut tree, &cli.streams_filter,
                    cli.jitter_window_s, cli.spike_ms,
                    &mut time_stats, &mut smp_stats, &mut stream_names, &mut stream_cols,
                    &mut seq_stats, &mut recent_skips, &mut route_skip_missing, &mut route_skip_events, &mut skip_occ_total
                );

                let now = Instant::now();
                let mut keys: BTreeSet<StreamKey> = BTreeSet::new();
                keys.extend(time_stats.keys().cloned());

                // Build table rows + detect ERROR transitions
                let mut rows = Vec::new();
                for k in &keys {
                    let ts = match time_stats.get(k) { Some(v) => v, None => continue };
                    let sname = stream_names.get(k).cloned().unwrap_or_else(|| format!("stream_{}", k.stream_id));

                    // Live rate and baseline (avoid double-borrow/move)
                    let mut smps_rate = 0.0;
                    let mut baseline_smps = 0.0;
                    if let Some(ss) = smp_stats.get_mut(k) {
                        smps_rate = ss.rate_living(now, rate_window);
                        baseline_smps = ss.baseline_smps;
                    }

                    // Compute status with clear priority:
                    //   STALLED > ERROR > WARN > RATE↓ > OK
                    let abs_ppm = ts.ppm.abs();
                    let stale = ts.last_seen.map(|t| now.duration_since(t) > stale_dur).unwrap_or(true);

                    let mut status: &'static str = "OK";
                    let bold_ppm;

                    if stale {
                        status = "STALLED";
                        bold_ppm = false;
                    } else if abs_ppm >= cli.ppm_err {
                        status = "ERROR";
                        bold_ppm = true;
                    } else if abs_ppm >= cli.ppm_warn {
                        status = "WARN";
                        bold_ppm = false;
                    } else if baseline_smps > 0.0 &&
                        smps_rate < baseline_smps * (1.0 - 0.01 * cli.rate_warn_pct) {
                        status = "RATE↓";
                        bold_ppm = false;
                    } else {
                        bold_ppm = false;
                    }

                    // ERROR transition logging (edge-trigger)
                    let now_error = status == "ERROR";
                    let was_error = *stream_in_error.get(k).unwrap_or(&false);
                    if now_error && !was_error {
                        error_occ_total += 1;
                        *route_error_events.entry(k.route.clone()).or_insert(0) += 1;
                        recent_errors.push_back(ErrorEvent {
                            when: SystemTime::now(),
                            route: k.route.clone(),
                            stream_id: k.stream_id,
                            ppm: ts.ppm,
                            drift_s: ts.drift_s,
                        });
                        if recent_errors.len() > 5 {
                            recent_errors.pop_front();
                        }
                    }
                    stream_in_error.insert(k.clone(), now_error);

                    rows.push((
                        k.route.clone(),
                        k.stream_id,
                        sname,
                        smps_rate,
                        ts.drift_s,
                        ts.ppm,
                        ts.jitter_ms,
                        ts.last_spike_ms,
                        status,
                        stale,
                        bold_ppm,
                    ));
                }
                rows.sort_by(|a,b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));

                // Optional meta rows (columns)
                let mut meta_rows: Vec<(String, u8, String, Vec<(String,String)>)> = Vec::new();
                if cli.show_columns {
                    for k in &keys {
                        if let Some(cols) = stream_cols.get(k) {
                            let sname = stream_names.get(k).cloned().unwrap_or_else(|| format!("stream_{}", k.stream_id));
                            meta_rows.push((k.route.clone(), k.stream_id, sname, cols.clone()));
                        }
                    }
                    meta_rows.sort_by(|a,b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
                }

                // --- Build diagnostic snapshot for TUI ---
                // Skips by route rows sorted by most missing, then route
                let mut by_route_skips: Vec<(String, u64, u64)> = route_skip_missing
                    .iter()
                    .map(|(r, miss)| (r.clone(), *miss, *route_skip_events.get(r).unwrap_or(&0)))
                    .collect();
                by_route_skips.sort_by(|a,b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));

                // Errors by route rows sorted by most events, then route
                let mut by_route_errors: Vec<(String, u64)> = route_error_events
                    .iter()
                    .map(|(r, ev)| (r.clone(), *ev))
                    .collect();
                by_route_errors.sort_by(|a,b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));

                let diag = DiagSnapshot {
                    recent_skips: recent_skips.iter().cloned().collect(),
                    by_route_skips,
                    skip_total_events: skip_occ_total,

                    recent_errors: recent_errors.iter().cloned().collect(),
                    by_route_errors,
                    error_total_events: error_occ_total,
                };

                let header = format!(
                    "tio-health  mode={:?}  discover=2s  prefetch=on  rate_window={}s(slide)  jitter_window={}s  warn/err={}ppm/{}ppm  spike={}ms  rate_warn={}%  fps={}  stale={}ms",
                    cli.mode, cli.rate_window_s, cli.jitter_window_s, cli.ppm_warn, cli.ppm_err, cli.spike_ms, cli.rate_warn_pct, cli.fps, cli.stale_ms
                );

                if let Err(_) = tui.draw(&header, &rows, &meta_rows, cli.show_columns, cli.quiet, cli.mode, &diag) {
                    break 'main;
                }
            }
        }
    }
    tui.teardown();
}
