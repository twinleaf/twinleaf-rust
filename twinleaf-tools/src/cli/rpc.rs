//! The arguments of `tio rpc` and its subcommands.

use clap::{
    builder::{PossibleValuesParser, TypedValueParser, ValueHint},
    Args, Subcommand,
};
use twinleaf::device::rpc::{RpcValueType, RpcValueTypeExt};

use super::TioOpts;

#[derive(Args, Debug)]
#[command(args_conflicts_with_subcommands = true, arg_required_else_help = true)]
pub struct RpcCli {
    #[command(flatten)]
    pub tio: TioOpts,

    #[command(subcommand)]
    pub subcommands: Option<RPCSubcommands>,

    /// RPC name to execute
    #[arg(value_hint = ValueHint::Other)]
    pub rpc_name: Option<String>,

    /// RPC argument value
    #[arg(
        allow_negative_numbers = true,
        value_name = "ARG",
        value_hint = ValueHint::Other,
        help_heading = "RPC Arguments"
    )]
    pub rpc_arg: Option<String>,

    /// RPC request type
    #[arg(
        short = 't',
        long = "req-type",
        value_parser = PossibleValuesParser::new(RpcValueType::NAMES)
            .map(|s: String| RpcValueType::from_name(&s).expect("validated by possible-values")),
        help_heading = "Type Options",
    )]
    pub req_type: Option<RpcValueType>,

    /// RPC reply type
    #[arg(
        short = 'T',
        long = "rep-type",
        value_parser = PossibleValuesParser::new(RpcValueType::NAMES)
            .map(|s: String| RpcValueType::from_name(&s).expect("validated by possible-values")),
        help_heading = "Type Options",
    )]
    pub rep_type: Option<RpcValueType>,

    /// Enable debug output
    #[arg(short = 'd', long)]
    pub debug: bool,
}

const PATTERNS: &str = "\
A PATTERN is a dotted prefix or a glob over the dotted name, as in sysctl.
`therm` selects `therm` and everything under it, `*.enable` an `enable` one
level down, `**.enable` one at any depth, and `cell.*.setpoint` each cell's.

Protocol RPCs (the standard table's `rpc.*`, clocks, upload, privilege) and
privileged diagnostics are hidden unless --all is given or a pattern selects
nothing else, as `rpc.*` or a diagnostics prefix does.";

const LIST_ABOUT: &str = "\
List available RPCs on the device.

With --json, prints an array of
{name, type, access, persistent, announced, privileged, channel}, where access
is a subset of [\"read\", \"write\"] and channel is setting, report, protocol,
action, diagnostic, or null where the metadata does not say.";

const GET_ABOUT: &str = "\
Read every readable, argument-free RPC that matches, pipelined in one batch.
Prints `name = value` lines, and with --json an array of {name, type, value}.
A read that fails is reported in its place, as `error` in JSON, and the rest
go on. The command fails at the end if any read did, or a pattern matched
nothing readable.

In JSON, bytes are a lowercase hex string, and a float that is NaN or infinite
is the string \"NaN\", \"inf\" or \"-inf\", since JSON has no such numbers. An
f32 is written in the shortest form that reads back as it.";

#[derive(Subcommand, Debug)]
pub enum RPCSubcommands {
    /// List available RPCs on the device
    #[command(long_about = LIST_ABOUT, after_long_help = PATTERNS)]
    List {
        #[command(flatten)]
        tio: TioOpts,

        /// Dotted prefixes or globs of the RPCs to list
        #[arg(value_name = "PATTERN", value_hint = ValueHint::Other)]
        patterns: Vec<String>,

        /// Include protocol and privileged RPCs
        #[arg(short = 'a', long)]
        all: bool,

        /// Print a JSON array of the RPCs
        #[arg(long)]
        json: bool,
    },
    /// Read every readable RPC that matches, in one batch
    #[command(long_about = GET_ABOUT, after_long_help = PATTERNS)]
    Get {
        #[command(flatten)]
        tio: TioOpts,

        /// Dotted prefixes or globs of the RPCs to read
        #[arg(value_name = "PATTERN", required = true, value_hint = ValueHint::Other)]
        patterns: Vec<String>,

        /// Include protocol and privileged RPCs
        #[arg(short = 'a', long)]
        all: bool,

        /// Print a JSON array of the values
        #[arg(long)]
        json: bool,
    },
    /// Dump RPC data from the device
    Dump {
        #[command(flatten)]
        tio: TioOpts,

        /// RPC name to dump
        #[arg(value_hint = ValueHint::Other)]
        rpc_name: String,
    },
}
