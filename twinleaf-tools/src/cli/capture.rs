use clap::{Args, ValueHint};
use clap_complete::engine::ArgValueCandidates;
use std::time::Duration;

use crate::tools::rpc::rpc_name_candidates;
use crate::TioOpts;

#[derive(Args, Debug)]
pub struct CaptureCli {
    #[command(flatten)]
    pub tio: TioOpts,

    /// Capture RPC name to execute
    #[arg(
        default_value = "test.capture",
        value_hint = ValueHint::Other,
        add = ArgValueCandidates::new(|| rpc_name_candidates(true)),
    )]
    pub rpc_name: String,

    /// Maximum time to wait for capture data
    #[arg(long, default_value = "5s", value_parser = humantime::parse_duration)]
    pub timeout: Duration,
}
