use clap::Parser;

use super::{finite_f64, nonneg_f64};

#[derive(Parser, Debug)]
#[command(version, about = "Simulate a Twinleaf device over UDP")]
pub struct SimulateCli {
    /// Sample rate in Hz
    #[arg(
        long = "samplerate",
        alias = "sample-rate",
        default_value = "1000",
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    pub(crate) samplerate: u32,

    /// Initial sine wave frequency in Hz
    #[arg(long = "frequency", default_value = "10", value_parser = nonneg_f64)]
    pub(crate) frequency: f64,

    /// Initial sine wave amplitude in V
    #[arg(long = "amplitude", default_value = "1", value_parser = nonneg_f64)]
    pub(crate) amplitude: f64,

    /// Initial white noise level in V/sqrt(Hz)
    #[arg(long = "noise", default_value = ".01", value_parser = nonneg_f64)]
    pub(crate) noise: f64,

    /// Segment duration in seconds
    #[arg(
        long = "segment-seconds",
        default_value = "10",
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    pub(crate) segment_seconds: u32,

    /// UDP port to listen on
    #[arg(long = "port", default_value = "7855")]
    pub(crate) port: u16,

    /// Never randomly drop samples (the 'd' key still drops one manually)
    #[arg(long = "no-drop")]
    pub(crate) no_drop: bool,

    /// Never feed the device a simulated PPS (the 'p' key still toggles it)
    #[arg(long = "no-pps")]
    pub(crate) no_pps: bool,

    /// Hub the root device over this many simulated children, at /1../N
    #[arg(
        long = "children",
        default_value = "0",
        value_parser = clap::value_parser!(u8).range(0..=9)
    )]
    pub(crate) children: u8,

    /// Never give the hub a simulated GPS second, so its children inherit its
    /// free-running time
    #[arg(long = "no-gps")]
    pub(crate) no_gps: bool,

    /// Milliseconds from the hub's edge until a child processes its time reference
    #[arg(
        long = "sync-latency",
        default_value = "100",
        value_parser = clap::value_parser!(u64).range(0..=10_000)
    )]
    pub(crate) sync_latency: u64,

    /// Percentage of the hub's time references each cable loses
    #[arg(
        long = "sync-drop",
        default_value = "0",
        value_parser = clap::value_parser!(u8).range(0..=100)
    )]
    pub(crate) sync_drop: u8,

    /// Parts per million each child's counter runs away from the hub's
    #[arg(long = "drift", default_value = "0", value_parser = finite_f64)]
    pub(crate) drift: f64,

    /// Password `dev.priv` takes to unlock a simulated device's developer
    /// entries; empty leaves it with none to unlock
    #[arg(long = "password", default_value = "895895")]
    pub(crate) password: String,
}
