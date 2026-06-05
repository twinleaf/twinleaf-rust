use clap::{
    builder::{PossibleValuesParser, TypedValueParser, ValueHint},
    Args, Subcommand,
};
use twinleaf::device::{util, RpcValueType};

use crate::TioOpts;

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
        value_parser = PossibleValuesParser::new(util::RPC_TYPE_NAMES)
            .map(|s: String| util::parse_rpc_type(&s).expect("validated by possible-values")),
        help_heading = "Type Options",
    )]
    pub req_type: Option<RpcValueType>,

    /// RPC reply type
    #[arg(
        short = 'T',
        long = "rep-type",
        value_parser = PossibleValuesParser::new(util::RPC_TYPE_NAMES)
            .map(|s: String| util::parse_rpc_type(&s).expect("validated by possible-values")),
        help_heading = "Type Options",
    )]
    pub rep_type: Option<RpcValueType>,

    /// Enable debug output
    #[arg(short = 'd', long)]
    pub debug: bool,
}

#[derive(Subcommand, Debug)]
pub enum RPCSubcommands {
    /// List available RPCs on the device
    List {
        #[command(flatten)]
        tio: TioOpts,
    },
    /// Dump RPC data from the device
    Dump {
        #[command(flatten)]
        tio: TioOpts,

        /// RPC name to dump
        #[arg(value_hint = ValueHint::Other)]
        rpc_name: String,

        /// Trigger a capture before dumping
        #[arg(long)]
        capture: bool,
    },
}
