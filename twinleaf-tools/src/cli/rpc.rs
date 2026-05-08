use clap::{
    builder::{PossibleValuesParser, TypedValueParser, ValueHint},
    Args, Subcommand,
};
use twinleaf::device::RpcValueType;

use crate::TioOpts;

const RPC_TYPE_NAMES: &[&str] = &[
    "u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64", "f32", "f64", "string",
];

fn parse_rpc_type(s: &str) -> RpcValueType {
    match s {
        "u8" => RpcValueType::Int {
            signed: false,
            size: 1,
        },
        "u16" => RpcValueType::Int {
            signed: false,
            size: 2,
        },
        "u32" => RpcValueType::Int {
            signed: false,
            size: 4,
        },
        "u64" => RpcValueType::Int {
            signed: false,
            size: 8,
        },
        "i8" => RpcValueType::Int {
            signed: true,
            size: 1,
        },
        "i16" => RpcValueType::Int {
            signed: true,
            size: 2,
        },
        "i32" => RpcValueType::Int {
            signed: true,
            size: 4,
        },
        "i64" => RpcValueType::Int {
            signed: true,
            size: 8,
        },
        "f32" => RpcValueType::Float { size: 4 },
        "f64" => RpcValueType::Float { size: 8 },
        "string" => RpcValueType::String { max_len: None },
        // PossibleValuesParser validates against RPC_TYPE_NAMES first.
        _ => unreachable!("possible values already validated"),
    }
}

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
        value_parser = PossibleValuesParser::new(RPC_TYPE_NAMES).map(|s: String| parse_rpc_type(&s)),
        help_heading = "Type Options",
    )]
    pub req_type: Option<RpcValueType>,

    /// RPC reply type
    #[arg(
        short = 'T',
        long = "rep-type",
        value_parser = PossibleValuesParser::new(RPC_TYPE_NAMES).map(|s: String| parse_rpc_type(&s)),
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
