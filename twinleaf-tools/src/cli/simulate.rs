//! The arguments of `tio simulate`.

use clap::{Parser, ValueEnum};
use twinleaf::DeviceRoute;

use super::{nonneg_f64, parse_device_route, positive_f64, ListenOn};

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

    /// Initial precession frequency of the FID capture in Hz
    #[arg(long = "fid-frequency", default_value = "175000", value_parser = nonneg_f64)]
    pub(crate) fid_frequency: f64,

    /// Initial decay time T2 of the FID capture in seconds
    #[arg(long = "fid-t2", default_value = "250e-6", value_parser = positive_f64)]
    pub(crate) fid_t2: f64,

    /// Initial amplitude of the FID capture in V
    #[arg(long = "fid-amplitude", default_value = "1", value_parser = nonneg_f64)]
    pub(crate) fid_amplitude: f64,

    /// Initial noise in each sample of the FID capture in V
    #[arg(long = "fid-noise", default_value = ".01", value_parser = nonneg_f64)]
    pub(crate) fid_noise: f64,

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

    /// Interfaces to listen on
    #[arg(long = "listen", value_enum, default_value_t = ListenOn::All)]
    pub(crate) listen: ListenOn,

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

    /// The kind of firmware the devices take, as a real device of that kind does
    #[arg(long = "upload", value_enum, default_value_t = UploadKind::Image)]
    pub(crate) upload: UploadKind,
}

/// What kind of firmware image a simulated device takes.
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadKind {
    /// Images in the chunked format tl-chibi firmware takes, whose chunks carry their own envelope
    Image,
    /// Signed packages for board `tio-test`, as a device built on twinleaf-device takes
    Signed,
}
