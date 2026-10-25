//! The arguments of `tio simulate`.

use clap::Parser;
use twinleaf::DeviceRoute;

use super::{nonneg_f64, parse_device_route};

#[derive(Parser, Debug)]
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

    /// Disable random sample drops (the 'd' key still drops one manually)
    #[arg(long = "no-drop")]
    pub(crate) no_drop: bool,

    /// Routes of the simulated devices, comma-separated (e.g. `/,/0,/1,/1/0`). A route left out has no device and answers nothing, like a hub port with nothing plugged in
    #[arg(
        long = "layout",
        value_delimiter = ',',
        default_value = "/",
        value_parser = parse_device_route,
    )]
    pub(crate) layout: Vec<DeviceRoute>,
}
