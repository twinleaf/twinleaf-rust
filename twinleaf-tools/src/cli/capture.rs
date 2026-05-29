use clap::{Args, ValueHint};
use std::time::Duration;

use crate::TioOpts;

#[derive(Args, Debug)]
pub struct CaptureCli {
    #[command(flatten)]
    pub tio: TioOpts,

    /// Capture RPC name to execute
    #[arg(default_value = "test.capture", value_hint = ValueHint::Other)]
    pub rpc_name: String,

    /// Maximum time to wait for capture data
    #[arg(long, default_value = "5s", value_parser = humantime::parse_duration)]
    pub timeout: Duration,
}
