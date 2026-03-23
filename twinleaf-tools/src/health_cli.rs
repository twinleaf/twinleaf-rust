use std::num::ParseFloatError;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "tio-health",
    version,
    about = "Live timing & rate diagnostics for TIO (Twinleaf) devices"
)]
pub struct HealthCli {
    #[command(flatten)]
    pub tio: TioOpts,

    /// Time window in seconds for calculating sample rate
    #[arg(
        long = "rate-window",
        default_value = "5",
        value_name = "SECONDS",
        value_parser = clap::value_parser!(u64).range(1..),
    )]
    pub rate_window: u64,

    /// Time window in seconds for calculating jitter statistics
    #[arg(
        long = "jitter-window",
        default_value = "10",
        value_name = "SECONDS",
        value_parser = clap::value_parser!(u64).range(1..),
        help = "Seconds for jitter calculation window (>= 1)"
    )]
    pub jitter_window: u64,

    /// PPM threshold for yellow warning indicators
    #[arg(
        long = "ppm-warn",
        default_value = "100",
        value_name = "PPM",
        value_parser = nonneg_f64,
        help = "Warning threshold in parts per million (>= 0)"
    )]
    pub ppm_warn: f64,

    /// PPM threshold for red error indicators
    #[arg(
        long = "ppm-err",
        default_value = "200",
        value_name = "PPM",
        value_parser = nonneg_f64,
        help = "Error threshold in parts per million (>= 0)"
    )]
    pub ppm_err: f64,

    /// Filter to only show specific stream IDs (comma-separated)
    #[arg(
        long = "streams",
        value_delimiter = ',',
        value_name = "IDS",
        value_parser = clap::value_parser!(u8),
        help = "Comma-separated stream IDs to monitor (e.g., 0,1,5)"
    )]
    pub streams: Option<Vec<u8>>,

    /// Suppress the footer help text
    #[arg(short = 'q', long = "quiet")]
    pub quiet: bool,

    /// UI refresh rate for animations and stale detection (data updates are immediate)
    #[arg(
        long = "fps",
        default_value = "30",
        value_name = "FPS",
        value_parser = clap::value_parser!(u64).range(1..=60),
        help = "UI refresh rate for heartbeat animation and stale detection (1–60)"
    )]
    pub fps: u64,

    /// Time in milliseconds before marking a stream as stale
    #[arg(
        long = "stale-ms",
        default_value = "2000",
        value_name = "MS",
        value_parser = clap::value_parser!(u64).range(1..),
        help = "Mark streams as stale after this many milliseconds without data (>= 1)"
    )]
    pub stale_ms: u64,

    /// Maximum number of events to keep in the event log
    #[arg(
        short = 'n',
        long = "event-log-size",
        default_value = "100",
        value_name = "N",
        value_parser = clap::value_parser!(u64).range(1..),
        help = "Maximum number of events to keep in history (>= 1)"
    )]
    pub event_log_size: u64,

    /// Number of event lines to display on screen
    #[arg(
        long = "event-display-lines",
        default_value = "8",
        value_name = "LINES",
        value_parser = clap::value_parser!(u16).range(3..),
        help = "Number of event lines to show (>= 3)"
    )]
    pub event_display_lines: u16,

    /// Only show warning and error events in the log
    #[arg(short = 'w', long = "warnings-only")]
    pub warnings_only: bool,
}

impl HealthCli {
    pub fn rate_window_dur(&self) -> Duration {
        Duration::from_secs(self.rate_window)
    }
    pub fn stale_dur(&self) -> Duration {
        Duration::from_millis(self.stale_ms)
    }
}

fn nonneg_f64(s: &str) -> Result<f64, String> {
    let v: f64 = s.parse().map_err(|e: ParseFloatError| e.to_string())?;
    if v < 0.0 {
        Err("must be ≥ 0".into())
    } else {
        Ok(v)
    }
}
