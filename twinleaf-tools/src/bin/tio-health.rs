// tio-health (live diagnostics)
// Live-only TUI showing packet continuity and time/rate health, grouped by device route.
//
// Deps: crossterm = "0.27", getopts = "0.2", crossbeam = "0.8", twinleaf, twinleaf-tools, tio

use crossterm::{cursor, event, style, terminal, ExecutableCommand, QueueableCommand};
use crossterm::style::{Color, SetForegroundColor, ResetColor, SetAttribute, Attribute};
use crossbeam::channel; // select!, tick, key channel
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::{self, Write};
use std::num::Wrapping;
use std::time::{Duration, Instant};
use twinleaf::tio;
use twinleaf_tools::{tio_opts, tio_parseopts};

// ---------------- CLI ----------------
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode { All, Pkt, Time }

#[derive(Debug)]
struct Cli {
    mode: Mode,
    rate_window_s: u64,     // window for pkts/s and smps/s
    jitter_window_s: u64,   // window for jitter stddev
    ppm_warn: f64,
    ppm_err: f64,
    spike_ms: f64,
    rate_warn_pct: f64,     // RATE↓ when smps/s below (1 - pct%) * baseline
    streams_filter: Option<Vec<u8>>,
    quiet: bool,
    fps: u64,
    show_columns: bool,
    stale_ms: u64,          // stale timeout to grey rows
}

fn print_help_and_exit(opts: &getopts::Options, program: &str, code: i32) -> ! {
    let brief = format!("Usage: {program} [options] <tio-url> [route]\n\nLive diagnostics grouped by device route. Only streams receiving packets are shown; stale streams are greyed.");
    let usage = opts.usage(&brief);
    let glossary = r#"
Terms:
  drift(s)         Data clock minus host clock since start (seconds).
  ppm              Long-term frequency error: 1e6 * drift / host_elapsed.
  jitter(ms)       Short-term timing noise (stddev of (Δdata - Δhost) over window).
  last_spike(ms)   Most recent single-window timing error whose |error| exceeded --spike-ms.
  skipped_pkts     Estimated missing packet count from sample-number continuity.
  skipped_smps     Estimated missing samples (same basis).
  last_gap         Size (samples) of the most recent continuity gap.
Statuses:
  OK     Healthy.
  WARN   |ppm| >= --ppm-warn.
  ERROR  |ppm| >= --ppm-err.
  RATE↓  smps/s below (1 - --rate-warn-pct%) of learned baseline.
  GAP    last_gap > 0 (recent continuity break).
  STALLED No packets within --stale-ms.
Notes:
  • Packets: continuity uses the min positive Δ(first_sample_n) as baseline samples/packet (robust).
  • Packet-loss accounting uses ceil(missing / baseline) for skipped_pkts so partial gaps count.
  • Stream names are fetched from device metadata; columns can be shown with --columns.
"#;
    eprintln!("{usage}\n{glossary}");
    std::process::exit(code)
}

fn parse_cli() -> (String, tio::proto::DeviceRoute, Cli) {
    let mut opts = tio_opts();
    opts.optflag("h", "help", "Show help & glossary.");
    opts.optopt("", "mode", "Diagnostic mode: all|pkt|time (default all)", "m");
    opts.optopt("", "rate-window", "Seconds for rate windows (default 5)", "sec");
    opts.optopt("", "jitter-window", "Seconds for jitter window (default 10)", "sec");
    opts.optopt("", "ppm-warn", "Warn threshold in ppm (default 100)", "ppm");
    opts.optopt("", "ppm-err", "Error threshold in ppm (default 200)", "ppm");
    opts.optopt("", "spike-ms", "Spike threshold in ms (default 20)", "ms");
    opts.optopt("", "rate-warn-pct", "Warn if smps/s dips by this % from baseline (default 0.5)", "%");
    opts.optopt("", "streams", "Comma-separated list of stream IDs to include", "list");
    opts.optflag("", "quiet", "Suppress footer hint");
    opts.optopt("", "fps", "UI refresh frames per second (default 10)", "n");
    opts.optflag("", "columns", "Show stream->column breakdown header");
    opts.optopt("", "stale-ms", "Grey rows after this many ms without packets (default 2000)", "ms");

    let (matches, root, route) = tio_parseopts(&opts, &std::env::args().collect::<Vec<_>>());
    if matches.opt_present("help") { print_help_and_exit(&opts, &std::env::args().next().unwrap_or_else(|| "tio-health".into()), 0); }

    let mode = match matches.opt_str("mode").as_deref() {
        Some("pkt")  => Mode::Pkt,
        Some("time") => Mode::Time,
        _ => Mode::All,
    };
    let rate_window_s   = matches.opt_str("rate-window").as_deref().unwrap_or("5").parse().unwrap_or(5);
    let jitter_window_s = matches.opt_str("jitter-window").as_deref().unwrap_or("10").parse().unwrap_or(10);
    let ppm_warn        = matches.opt_str("ppm-warn").as_deref().unwrap_or("100").parse().unwrap_or(100.0);
    let ppm_err         = matches.opt_str("ppm-err").as_deref().unwrap_or("200").parse().unwrap_or(200.0);
    let spike_ms        = matches.opt_str("spike-ms").as_deref().unwrap_or("20").parse().unwrap_or(20.0);
    let rate_warn_pct   = matches.opt_str("rate-warn-pct").as_deref().unwrap_or("0.5").parse().unwrap_or(0.5);
    let streams_filter  = matches.opt_str("streams").map(|s| s.split(',')
        .filter_map(|x| x.trim().parse::<u8>().ok()).collect::<Vec<_>>());
    let quiet           = matches.opt_present("quiet");
    let fps             = matches.opt_str("fps").as_deref().unwrap_or("10").parse().unwrap_or(10);
    let show_columns    = matches.opt_present("columns");
    let stale_ms        = matches.opt_str("stale-ms").as_deref().unwrap_or("2000").parse().unwrap_or(2000);

    (root, route, Cli {
        mode, rate_window_s, jitter_window_s, ppm_warn, ppm_err,
        spike_ms, rate_warn_pct, streams_filter, quiet, fps, show_columns, stale_ms
    })
}

// ---------------- Keys ----------------
#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct StreamKey { route: String, stream_id: u8 }
impl StreamKey { fn new(route: String, stream_id: u8) -> Self { Self { route, stream_id } } }

// ---------------- Time + Sample Stats ----------------
#[derive(Clone)]
struct TimeWindow { buf: Vec<f64>, cap: usize, idx: usize, filled: bool }
impl TimeWindow {
    fn new(seconds: u64, hz_guess: f64) -> Self {
        let cap = ((seconds as f64 * hz_guess).round() as usize).max(16);
        Self { buf: vec![0.0; cap], cap, idx: 0, filled: false }
    }
    fn push(&mut self, v: f64) { self.buf[self.idx] = v; self.idx = (self.idx + 1) % self.cap; if self.idx==0 { self.filled = true; } }
    fn std_ms(&self) -> f64 {
        let n = if self.filled { self.cap } else { self.idx };
        if n==0 {return 0.0;}
        let mean: f64 = self.buf[..n].iter().sum::<f64>()/(n as f64);
        let var: f64  = self.buf[..n].iter().map(|x| (x-mean)*(x-mean)).sum::<f64>()/(n as f64);
        var.sqrt()
    }
}

#[derive(Default, Clone)]
struct TimeStats {
    t0_host: Option<Instant>, t0_data: Option<f64>,
    last_host: Option<Instant>, last_data: Option<f64>,
    host_elapsed: f64, data_elapsed: f64, drift_s: f64, ppm: f64,
    jitter_ms: f64, last_spike_ms: f64, win: Option<TimeWindow>,
    last_seen: Option<Instant>,
}
impl TimeStats {
    fn ensure_window(&mut self, seconds: u64) { if self.win.is_none() { self.win = Some(TimeWindow::new(seconds, 100.0)); } }
    fn on_sample(&mut self, t_data: f64, now: Instant, spike_ms: f64, window_s: u64) {
        self.ensure_window(window_s);
        if self.t0_host.is_none() { self.t0_host = Some(now); }
        if self.t0_data.is_none() { self.t0_data = Some(t_data); }
        if let (Some(lh), Some(ld)) = (self.last_host, self.last_data) {
            let dh = now.duration_since(lh).as_secs_f64();
            let dd = t_data - ld;
            let delta_ms = (dd - dh) * 1000.0;
            if let Some(w) = &mut self.win { w.push(delta_ms); self.jitter_ms = w.std_ms(); }
            if delta_ms.abs() > spike_ms { self.last_spike_ms = delta_ms; }
        }
        self.last_host = Some(now);
        self.last_data = Some(t_data);
        let h0 = self.t0_host.unwrap(); let d0 = self.t0_data.unwrap();
        self.host_elapsed = now.duration_since(h0).as_secs_f64();
        self.data_elapsed = t_data - d0;
        self.drift_s = self.data_elapsed - self.host_elapsed;
        if self.host_elapsed.abs() > 1e-9 { self.ppm = 1e6 * self.drift_s / self.host_elapsed; }
        self.last_seen = Some(now);
    }
}

#[derive(Clone)]
struct SampleStats {
    smps_in_win: u64,
    win_start: Instant,
    pub rate_smps: f64,
    pub baseline_smps: f64,
}
impl Default for SampleStats {
    fn default() -> Self { Self { smps_in_win: 0, win_start: Instant::now(), rate_smps: 0.0, baseline_smps: 0.0 } }
}
impl SampleStats {
    fn on_sample(&mut self, now: Instant, window: Duration) {
        self.smps_in_win += 1;
        let dt = now.duration_since(self.win_start);
        if dt >= window {
            let secs = dt.as_secs_f64().max(1e-6);
            self.rate_smps = (self.smps_in_win as f64) / secs;
            if self.rate_smps > self.baseline_smps { self.baseline_smps = self.rate_smps; }
            self.smps_in_win = 0;
            self.win_start = now;
        }
    }
}

// ---------------- Packet Continuity ----------------
#[derive(Clone)]
struct PktStats {
    last_first:   Option<Wrapping<u32>>,
    last_segment: Option<u32>,
    diffs:        [u32; 8], // rolling min-of-positives baseline
    diffs_len:    usize,
    diffs_idx:    usize,
    skipped_packets: u64,
    skipped_samples: u64,
    last_gap_samples: u32,
    pkts_in_win: u64,
    win_start: Instant,
    rate_pkts: f64,
    last_seen: Option<Instant>,
}
impl Default for PktStats {
    fn default() -> Self {
        Self {
            last_first: None, last_segment: None,
            diffs: [0; 8], diffs_len: 0, diffs_idx: 0,
            skipped_packets: 0, skipped_samples: 0, last_gap_samples: 0,
            pkts_in_win: 0, win_start: Instant::now(), rate_pkts: 0.0,
            last_seen: None,
        }
    }
}
impl PktStats {
    fn baseline_nsamp(&self) -> Option<u32> {
        if self.diffs_len == 0 { return None; }
        let mut minv = u32::MAX;
        for i in 0..self.diffs_len {
            let v = self.diffs[i];
            if v > 0 && v < minv { minv = v; }
        }
        if minv == u32::MAX { None } else { Some(minv.max(1)) }
    }
    fn push_diff(&mut self, diff: u32) {
        if diff == 0 { return; }
        self.diffs[self.diffs_idx] = diff;
        if self.diffs_len < self.diffs.len() { self.diffs_len += 1; }
        self.diffs_idx = (self.diffs_idx + 1) % self.diffs.len();
    }
    fn on_streamdata(&mut self, first_sample_n: u32, segment_id: u32, now: Instant, window: Duration) {
        // rate
        self.pkts_in_win += 1;
        let dt = now.duration_since(self.win_start);
        if dt >= window {
            let secs = dt.as_secs_f64().max(1e-6);
            self.rate_pkts = (self.pkts_in_win as f64) / secs;
            self.pkts_in_win = 0;
            self.win_start = now;
        }

        let first_w = Wrapping(first_sample_n);
        if let (Some(prev_first), Some(prev_seg)) = (self.last_first, self.last_segment) {
            if segment_id == prev_seg {
                // diff with wrap
                let diff = first_w.0.wrapping_sub(prev_first.0);
                // learn/update baseline robustly: only accept "small" diffs or when baseline missing
                match self.baseline_nsamp() {
                    None => { if diff > 0 { self.push_diff(diff); } }
                    Some(base) => {
                        if diff > 0 && diff <= base.saturating_mul(2) { self.push_diff(diff); }
                    }
                }
                let expected = self.baseline_nsamp().unwrap_or(diff.max(1));
                if diff > expected {
                    let missing = diff - expected;
                    self.last_gap_samples = missing;
                    // ceil division for packet count
                    let skipped_pkts_est = (missing + expected - 1) / expected;
                    self.skipped_packets = self.skipped_packets.saturating_add(skipped_pkts_est as u64);
                    self.skipped_samples = self.skipped_samples.saturating_add(missing as u64);
                } else {
                    self.last_gap_samples = 0;
                }
            } else {
                // new segment -> reset per-segment continuity gap and baseline window
                self.last_gap_samples = 0;
                self.diffs_len = 0; self.diffs_idx = 0;
            }
        }
        self.last_first = Some(first_w);
        self.last_segment = Some(segment_id);
        self.last_seen = Some(now);
    }
}

#[derive(Clone)]
struct ErrEntry { at: std::time::SystemTime, msg: String }
const ERR_CAP: usize = 200;

fn push_err(
    err_log: &mut std::collections::VecDeque<ErrEntry>,
    err_counts: &mut std::collections::BTreeMap<String, u64>,
    msg: String,
    count_key: Option<String>,
) {
    if err_log.len() >= ERR_CAP { err_log.pop_front(); }
    err_log.push_back(ErrEntry { at: std::time::SystemTime::now(), msg });
    if let Some(k) = count_key {
        *err_counts.entry(k).or_insert(0) += 1;
    }
}

fn fmt_local_pretty(t: std::time::SystemTime) -> String {
    use time::{OffsetDateTime, UtcOffset};
    use time::macros::format_description;

    let odt_utc = OffsetDateTime::from(t);
    let local_off = UtcOffset::current_local_offset().unwrap_or(UtcOffset::UTC);
    let odt_local = odt_utc.to_offset(local_off);

    let fmt = format_description!(
        "[weekday repr:short] [month repr:short] [day padding:space] \
         [hour]:[minute]:[second].[subsecond digits:3] \
         [year]"
    );
    odt_local.format(&fmt).unwrap_or_else(|_| "<bad time>".to_string())
}

// ---------------- TUI ----------------
struct Tui { stdout: io::Stdout }
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

    // fixed widths
    const ROUTE_W: usize = 10;
    const SID_W:   usize = 3;
    const NAME_W:  usize = 22;
    const SKP_W:   usize = 12;
    const SMS_W:   usize = 12;
    const GAP_W:   usize = 9;
    const PKTS_W:  usize = 9;
    const SMPS_W:  usize = 9;

    const DRIFT_W: usize = 9;
    const PPM_W:   usize = 8;
    const JIT_W:   usize = 11;
    const SPIK_W:  usize = 13;
    const STAT_W:  usize = 8;

    fn section_header(&mut self, title: &str) -> io::Result<()> {
        self.stdout.queue(SetAttribute(Attribute::Bold))?;
        self.stdout.queue(style::Print(title))?;
        self.stdout.queue(SetAttribute(Attribute::Reset))?;
        self.stdout.queue(cursor::MoveToNextLine(1))?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn draw(
        &mut self,
        header: &str,
        pkt_rows: &[(String, u8, String, &PktStats, f64 /*smps/s*/, bool /*stale*/)],
        time_rows: &[(String, u8, String, &TimeStats, &'static str, bool /*stale*/)],
        meta_rows: &[(String, u8, String, Vec<(String,String)>)],
        quiet: bool,
        show_columns: bool,
        mode: Mode,
        err_recent: &[(String, String)],
        err_top: &[(u64, String)], 
    ) -> io::Result<()> {
        self.stdout.queue(cursor::MoveTo(0,0))?;
        self.stdout.queue(terminal::Clear(terminal::ClearType::All))?;
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
                self.stdout.queue(style::Print(format!("{:>sid_w$}  {:<name_w$}",
                    sid, sname, sid_w=Self::SID_W, name_w=Self::NAME_W)))?;
                if !cols.is_empty() {
                    let colnames: Vec<String> = cols.iter().map(|(n,u)| if u.is_empty(){n.clone()} else {format!("{} [{}]", n,u)}).collect();
                    self.stdout.queue(style::Print(format!("  -> {}", colnames.join(", "))))?;
                }
                self.stdout.queue(cursor::MoveToNextLine(1))?;
            }
        }

        // ---- Packets ----
        if mode != Mode::Time {
            self.section_header("Packets:")?;
            self.stdout.queue(SetAttribute(Attribute::Bold))?;
            self.stdout.queue(style::Print(format!(
                "{route:<route_w$} {sid:>sid_w$}  {name:<name_w$}  {skp:>skp_w$}  {sms:>sms_w$}  {gap:>gap_w$}  {pkts:>pkts_w$}  {smps:>smps_w$}",
                route="route", sid="sid", name="stream_name",
                skp="skipped_pkts", sms="skipped_smps", gap="last_gap", pkts="pkts/s", smps="smps/s",
                route_w=Self::ROUTE_W, sid_w=Self::SID_W, name_w=Self::NAME_W,
                skp_w=Self::SKP_W, sms_w=Self::SMS_W, gap_w=Self::GAP_W, pkts_w=Self::PKTS_W, smps_w=Self::SMPS_W
            )))?;
            self.stdout.queue(SetAttribute(Attribute::Reset))?;
            self.stdout.queue(cursor::MoveToNextLine(1))?;

            let mut last_route = String::new();
            for (route, sid, sname, ps, smps_rate, stale) in pkt_rows {
                if *route != last_route {
                    if !last_route.is_empty() {
                        self.stdout.queue(cursor::MoveToNextLine(1))?;
                    }
                    last_route = route.clone();
                }
                if *stale {
                    self.stdout.queue(SetForegroundColor(Color::DarkGrey))?;
                } else if ps.last_gap_samples > 0 {
                    self.stdout.queue(SetForegroundColor(Color::Red))?;
                } else {
                    self.stdout.queue(ResetColor)?;
                }
                self.stdout.queue(style::Print(format!(
                    "{route:<route_w$} {sid:>sid_w$}  {name:<name_w$}  {skp:>skp_w$}  {sms:>sms_w$}  {gap:>gap_w$}  {rpkts:>pkts_w$.2}  {rsmps:>smps_w$.1}",
                    route=route, sid=sid, name=sname,
                    skp=ps.skipped_packets, sms=ps.skipped_samples, gap=ps.last_gap_samples,
                    rpkts=ps.rate_pkts, rsmps=smps_rate,
                    route_w=Self::ROUTE_W, sid_w=Self::SID_W, name_w=Self::NAME_W,
                    skp_w=Self::SKP_W, sms_w=Self::SMS_W, gap_w=Self::GAP_W, pkts_w=Self::PKTS_W, smps_w=Self::SMPS_W
                )))?;
                self.stdout.queue(ResetColor)?;
                self.stdout.queue(cursor::MoveToNextLine(1))?;
            }
            self.stdout.queue(cursor::MoveToNextLine(1))?;
        }

        // ---- Time ----
        if mode != Mode::Pkt {
            self.section_header("Time:")?;
            self.stdout.queue(SetAttribute(Attribute::Bold))?;
            self.stdout.queue(style::Print(format!(
                "{route:<route_w$} {sid:>sid_w$}  {name:<name_w$}  {drift:>drift_w$}  {ppm:>ppm_w$}  {jit:>jit_w$}  {spk:>spik_w$}  {stat:>stat_w$}",
                route="route", sid="sid", name="stream_name",
                drift="drift(s)", ppm="ppm", jit="jitter(ms)", spk="last_spike(ms)", stat="status",
                route_w=Self::ROUTE_W, sid_w=Self::SID_W, name_w=Self::NAME_W,
                drift_w=Self::DRIFT_W, ppm_w=Self::PPM_W, jit_w=Self::JIT_W, spik_w=Self::SPIK_W, stat_w=Self::STAT_W
            )))?;
            self.stdout.queue(SetAttribute(Attribute::Reset))?;
            self.stdout.queue(cursor::MoveToNextLine(1))?;

            let mut last_route = String::new();
            for (route, sid, sname, ts, status, stale) in time_rows {
                if *route != last_route {
                    if !last_route.is_empty() {
                        self.stdout.queue(cursor::MoveToNextLine(1))?;
                    }
                    last_route = route.clone();
                }
                let color = if *stale { Color::DarkGrey } else {
                    match *status {
                        "ERROR" => Color::Red,
                        "WARN" | "RATE↓" => Color::Yellow,
                        "GAP" => Color::Red,
                        "OK" => Color::Green,
                        _ => Color::White,
                    }
                };
                self.stdout.queue(SetForegroundColor(color))?;
                self.stdout.queue(style::Print(format!(
                    "{route:<route_w$} {sid:>sid_w$}  {name:<name_w$}  {drift:>drift_w$.3}  {ppm:>ppm_w$.0}  {jit:>jit_w$.2}  {spk:>spik_w$.2}  {stat:>stat_w$}",
                    route=route, sid=sid, name=sname,
                    drift=ts.drift_s, ppm=ts.ppm, jit=ts.jitter_ms, spk=ts.last_spike_ms, stat=status,
                    route_w=Self::ROUTE_W, sid_w=Self::SID_W, name_w=Self::NAME_W,
                    drift_w=Self::DRIFT_W, ppm_w=Self::PPM_W, jit_w=Self::JIT_W, spik_w=Self::SPIK_W, stat_w=Self::STAT_W
                )))?;
                self.stdout.queue(ResetColor)?;
                self.stdout.queue(cursor::MoveToNextLine(1))?;
            }
        }
        self.stdout.queue(cursor::MoveToNextLine(2))?;
        self.section_header("Errors:")?;
        if err_recent.is_empty() && err_top.is_empty() {
            self.stdout.queue(style::Print("— none so far —"))?;
            self.stdout.queue(cursor::MoveToNextLine(1))?;
        } else {
            self.stdout.queue(SetAttribute(Attribute::Bold))?;
            self.stdout.queue(style::Print("top by count"))?;
            self.stdout.queue(SetAttribute(Attribute::Reset))?;
            self.stdout.queue(cursor::MoveToNextLine(1))?;
            for (count, kind) in err_top.iter().take(5) {
                self.stdout.queue(style::Print(format!("{:>6}  {}", count, kind)))?;
                self.stdout.queue(cursor::MoveToNextLine(1))?;
            }
            if !err_recent.is_empty() {
                self.stdout.queue(cursor::MoveToNextLine(1))?;
                self.stdout.queue(SetAttribute(Attribute::Bold))?;
                self.stdout.queue(style::Print("recent"))?;
                self.stdout.queue(SetAttribute(Attribute::Reset))?;
                self.stdout.queue(cursor::MoveToNextLine(1))?;
                for (ts, msg) in err_recent.iter().take(10) {
                    self.stdout.queue(style::Print(format!("[{}]  {}", ts, msg)))?;
                    self.stdout.queue(cursor::MoveToNextLine(1))?;
                }
            }
        }

        if !quiet {
            self.stdout.queue(cursor::MoveToNextLine(2))?;
            self.stdout.queue(style::Print("q/Esc to quit"))?;
        }
        self.stdout.flush()
    }
}

// ---------------- Helpers to avoid long-lived borrows ----------------

fn handle_packet(
    pkt: tio::Packet,
    streams_filter: &Option<Vec<u8>>,
    rate_window: Duration,
    devices: &mut BTreeMap<String, twinleaf::Device>,
    proxy: &tio::proxy::Interface,
    pkt_stats: &mut BTreeMap<StreamKey, PktStats>,
    err_log: &mut VecDeque<ErrEntry>,
    err_counts: &mut BTreeMap<String, u64>,
) {
    let route_str = pkt.routing.to_string();
    if let tio::proto::Payload::StreamData(sd) = &pkt.payload {
        let now = Instant::now();
        let sid = sd.stream_id as u8;
        if let Some(f) = streams_filter { if !f.contains(&sid) { return; } }
        let key = StreamKey::new(route_str.clone(), sid);

        if !devices.contains_key(&route_str) {
            match twinleaf::Device::open(proxy, pkt.routing.clone()) {
                Ok(dev) => { devices.insert(route_str.clone(), dev); }
                Err(e) => {
                    let kind = format!("open device: {:?}", e);
                    push_err(err_log, err_counts, kind.clone(), Some(kind));
                }
            }
        }

        let ps = pkt_stats.entry(key).or_default();
        ps.on_streamdata(sd.first_sample_n as u32, sd.segment_id as u32, now, rate_window);
    }
}

fn drain_devices_once(
    devices: &mut BTreeMap<String, twinleaf::Device>,
    streams_filter: &Option<Vec<u8>>,
    jitter_window_s: u64,
    rate_window: Duration,
    spike_ms: f64,
    time_stats: &mut BTreeMap<StreamKey, TimeStats>,
    smp_stats: &mut BTreeMap<StreamKey, SampleStats>,
    stream_names: &mut BTreeMap<StreamKey, String>,
    stream_cols: &mut BTreeMap<StreamKey, Vec<(String,String)>>,
    err_log: &mut VecDeque<ErrEntry>,
    err_counts: &mut BTreeMap<String, u64>,
) {
    // Iterate by key list to avoid borrow/iteration pitfalls
    let keys: Vec<String> = devices.keys().cloned().collect();
    for route_str in keys {
        if let Some(dev) = devices.get_mut(&route_str) {
            loop {
                match dev.try_next() {
                    Ok(Some(sample)) => {
                        let now = Instant::now();
                        let sid = sample.stream.stream_id as u8;
                        if let Some(f) = streams_filter { if !f.contains(&sid) { continue; } }
                        let key = StreamKey::new(route_str.clone(), sid);

                        let ts = time_stats.entry(key.clone()).or_default();
                        ts.on_sample(sample.timestamp_end(), now, spike_ms, jitter_window_s);

                        let ss = smp_stats.entry(key.clone()).or_default();
                        ss.on_sample(now, rate_window);

                        stream_names.entry(key.clone()).or_insert_with(|| sample.stream.name.clone());
                        let cols = sample.columns.iter().map(|c| (c.desc.name.clone(), c.desc.units.clone())).collect::<Vec<_>>();
                        if !cols.is_empty() { stream_cols.insert(key, cols); }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        let kind = format!("dev[{}] try_next: {:?}", route_str, e);
                        push_err(err_log, err_counts, kind.clone(), Some(kind));
                        break;
                    }
                }
            }
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
        let mut t = Tui { stdout: io::stdout() };
        t.teardown();
        original_hook(panic_info);
    }));

    // Open proxy: raw subtree for packets; per-route Device handles for samples/metadata
    let proxy = tio::proxy::Interface::new(&root);
    let mut port = match proxy.subtree_full(route.clone()) {
        Ok(p) => p,
        Err(_) => { tui.teardown(); eprintln!("Failed to open subtree port. Check connection / route."); std::process::exit(1); }
    };

    // live Device handles keyed by route str (opened on-demand when packets show up)
    let mut devices: BTreeMap<String, twinleaf::Device> = BTreeMap::new();

    // Stats per (route, stream)
    let mut pkt_stats:    BTreeMap<StreamKey, PktStats>    = BTreeMap::new();
    let mut time_stats:   BTreeMap<StreamKey, TimeStats>   = BTreeMap::new();
    let mut smp_stats:    BTreeMap<StreamKey, SampleStats> = BTreeMap::new();
    let mut stream_names: BTreeMap<StreamKey, String>      = BTreeMap::new();
    let mut stream_cols:  BTreeMap<StreamKey, Vec<(String,String)>> = BTreeMap::new();

    let mut err_log: VecDeque<ErrEntry> = VecDeque::with_capacity(ERR_CAP);
    let mut err_counts: BTreeMap<String, u64> = BTreeMap::new();

    // Tuning
    let rate_window   = Duration::from_secs(cli.rate_window_s.max(1));
    let jitter_window = cli.jitter_window_s;
    let stale_dur     = Duration::from_millis(cli.stale_ms.max(1));

    // Redraw cadence
    let frame = Duration::from_millis((1000 / cli.fps.max(1)) as u64);
    let tick  = channel::tick(frame);

    // Keyboard: dedicated blocking thread -> channel
    let (key_tx, key_rx) = channel::unbounded();
    std::thread::spawn({
        let key_tx = key_tx.clone();
        move || {
            loop {
                if let Ok(ev) = event::read() {
                    let _ = key_tx.send(ev);
                } else {
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }
    });

    // Track port liveness so we don't select on a closed receiver
    let mut port_alive = true;

    // Main event loop
    'main: loop {
        crossbeam::select! {
            // Packets
            recv(port.receiver()) -> msg => {
                match msg {
                    Ok(pkt) => {
                        handle_packet(
                            pkt, &cli.streams_filter, rate_window,
                            &mut devices, &proxy, &mut pkt_stats, &mut err_log, &mut err_counts
                        );

                        {
                            let mut more = Vec::new();
                            for p in port.try_iter() { more.push(p); }
                            for p in more {
                                handle_packet(
                                    p, &cli.streams_filter, rate_window,
                                    &mut devices, &proxy, &mut pkt_stats, &mut err_log, &mut err_counts
                                );
                            }
                        }

                        drain_devices_once(
                            &mut devices, &cli.streams_filter, jitter_window, rate_window, cli.spike_ms,
                            &mut time_stats, &mut smp_stats, &mut stream_names, &mut stream_cols,
                            &mut err_log, &mut err_counts
                        );
                    }
                    Err(_) => {
                        let kind = "proxy recv: disconnected".to_string();
                        push_err(&mut err_log, &mut err_counts, kind.clone(), Some(kind));
                        port_alive = false;
                    }
                }
            }

            // Keyboard
            recv(key_rx) -> ev => {
                if let Ok(event::Event::Key(k)) = ev {
                    use crossterm::event::{KeyCode, KeyModifiers};
                    let quit = k.code == KeyCode::Char('q')
                            || k.code == KeyCode::Esc
                            || (k.code == KeyCode::Char('c') && k.modifiers == KeyModifiers::CONTROL);
                    if quit { break 'main; }
                }
            }

            recv(tick) -> _ => {
                if !port_alive {
                    match proxy.subtree_full(route.clone()) {
                        Ok(p) => {
                            port = p;
                            devices.clear();
                            port_alive = true;
                            push_err(&mut err_log, &mut err_counts, "recovered: reopened subtree port".to_string(), None);
                        }
                        Err(_) => {}
                    }
                }

                let now = Instant::now();

                let mut keys: BTreeSet<StreamKey> = BTreeSet::new();
                keys.extend(pkt_stats.keys().cloned());

                let mut pkt_rows = Vec::new();
                if cli.mode != Mode::Time {
                    for k in &keys {
                        if let Some(ps) = pkt_stats.get(k) {
                            let sname = stream_names.get(k).cloned().unwrap_or_else(|| format!("stream_{}", k.stream_id));
                            let smps_rate = smp_stats.get(k).map(|s| s.rate_smps).unwrap_or(0.0);
                            let stale = ps.last_seen.map(|t| now.duration_since(t) > stale_dur).unwrap_or(true);
                            pkt_rows.push((k.route.clone(), k.stream_id, sname, ps, smps_rate, stale));
                        }
                    }
                    pkt_rows.sort_by(|a,b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
                }

                let mut time_rows = Vec::new();
                if cli.mode != Mode::Pkt {
                    for k in &keys {
                        if let Some(ts) = time_stats.get(k) {
                            let mut status = if ts.ppm.abs() >= cli.ppm_err { "ERROR" }
                                             else if ts.ppm.abs() >= cli.ppm_warn { "WARN" }
                                             else { "OK" };
                            if let Some(ps) = pkt_stats.get(k) {
                                if ps.last_gap_samples > 0 { status = "GAP"; }
                            }
                            if let Some(ss) = smp_stats.get(k) {
                                if ss.baseline_smps > 0.0 &&
                                   ss.rate_smps < ss.baseline_smps * (1.0 - 0.01 * cli.rate_warn_pct) {
                                    status = "RATE↓";
                                }
                            }
                            let sname = stream_names.get(k).cloned().unwrap_or_else(|| format!("stream_{}", k.stream_id));
                            let stale = pkt_stats.get(k).and_then(|ps| ps.last_seen).map(|t| now.duration_since(t) > stale_dur).unwrap_or(true);
                            let status = if stale { "STALLED" } else { status };
                            time_rows.push((k.route.clone(), k.stream_id, sname, ts, status, stale));
                        }
                    }
                    time_rows.sort_by(|a,b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
                }

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

                let header = format!(
                    "tio-health  mode={:?}  rate_window={}s  jitter_window={}s  warn/err={}ppm/{}ppm  spike={}ms  rate_warn={}%  fps={}  stale={}ms",
                    cli.mode, cli.rate_window_s, cli.jitter_window_s, cli.ppm_warn, cli.ppm_err, cli.spike_ms, cli.rate_warn_pct, cli.fps, cli.stale_ms
                );
                let err_recent: Vec<(String, String)> = err_log.iter()
                    .rev()
                    .take(12)
                    .map(|e| (fmt_local_pretty(e.at), e.msg.clone()))
                    .collect();

                let mut err_top: Vec<(u64, String)> = err_counts.iter()
                    .map(|(k,v)| (*v, k.clone()))
                    .collect();
                err_top.sort_by(|a,b| b.0.cmp(&a.0));

                if let Err(_) = tui.draw(
                    &header, &pkt_rows, &time_rows, &meta_rows,
                    cli.quiet, cli.show_columns, cli.mode,
                    &err_recent, &err_top,
                ) {
                    break 'main;
                }
            }
        }
    }
    tui.teardown();
}
