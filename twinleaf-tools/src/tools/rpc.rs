use std::io::Write;

use crate::{ProxyHelp, RPCSubcommands, RpcCli, TioOpts};
use tio::proxy;
use twinleaf::device::rpc::{
    CallError, RpcMeta, RpcMetaExt, RpcValue, RpcValueType, RpcValueTypeExt,
};
use twinleaf::device::Device;
use twinleaf::tio;
use twinleaf::Connection;

pub(crate) fn resolve_rpc_type(metadata: Option<u16>) -> RpcValueType {
    let kind = metadata
        .map(|metadata| RpcMeta::from_bits(metadata).kind())
        .unwrap_or(RpcValueType::String { max_len: None });
    match kind {
        RpcValueType::Raw { .. } => RpcValueType::String { max_len: None },
        other => other,
    }
}

fn parse_rpc_value(input: &str, kind: RpcValueType) -> eyre::Result<RpcValue> {
    Ok(match kind {
        RpcValueType::Unit => {
            if !input.is_empty() {
                eyre::bail!("unit RPC arguments must be empty");
            }
            RpcValue::Unit
        }
        RpcValueType::String { .. } => RpcValue::Str(input.to_owned()),
        RpcValueType::Int { signed: false, .. } => RpcValue::U64(input.parse()?),
        RpcValueType::Int { signed: true, .. } => RpcValue::I64(input.parse()?),
        RpcValueType::Float { size: 4 } => RpcValue::F64(f64::from(input.parse::<f32>()?)),
        RpcValueType::Float { .. } => RpcValue::F64(input.parse()?),
        RpcValueType::Raw { .. } => RpcValue::Bytes(input.as_bytes().to_vec()),
    })
}

pub(crate) fn encode_rpc_argument(input: &str, kind: RpcValueType) -> eyre::Result<Vec<u8>> {
    let value = parse_rpc_value(input, kind)?;
    kind.encode(&value).map_err(eyre::Report::new)
}

pub(crate) fn format_rpc_value(value: &RpcValue) -> String {
    match value {
        RpcValue::Str(value) => format!("\"{}\" {:?}", value, value.as_bytes()),
        RpcValue::Bytes(value) => format!("{:?}", value),
        other => other.to_string(),
    }
}

pub fn run_rpc(rpc_cli: RpcCli) -> eyre::Result<()> {
    match rpc_cli.subcommands {
        Some(RPCSubcommands::List { tio }) => list_rpcs(&tio),
        Some(RPCSubcommands::Dump {
            tio,
            rpc_name,
            capture,
        }) => rpc_dump(&tio, rpc_name, capture),
        None => rpc(
            &rpc_cli.tio,
            rpc_cli.rpc_name.unwrap_or("".to_string()),
            rpc_cli.rpc_arg,
            rpc_cli.req_type,
            rpc_cli.rep_type,
            rpc_cli.debug,
        ),
    }
}

pub fn list_rpcs(tio: &TioOpts) -> eyre::Result<()> {
    use eyre::WrapErr;

    let connection = Connection::open(&tio.root);
    let device = connection.device(tio.route);
    let registry = device
        .rpc_registry()
        .wrap_err("failed to query RPC registry")
        .with_proxy_help()?;

    for desc in registry.iter() {
        println!(
            "{} {}({})",
            desc.meta.perm_str(),
            desc.full_name,
            desc.meta.type_str()
        );
    }

    Ok(())
}

fn infer_rpc_type(name: &str, device: &Device, kind: &str) -> RpcValueType {
    let meta: Option<u16> = device.rpc("rpc.info", name.to_string()).ok();
    if meta.is_none() {
        println!("Unknown RPC {kind} type, assuming 'string'. Use -t/-T to override.");
    }
    resolve_rpc_type(meta)
}

pub fn rpc(
    tio: &TioOpts,
    rpc_name: String,
    rpc_arg: Option<String>,
    req_type: Option<RpcValueType>,
    rep_type: Option<RpcValueType>,
    debug: bool,
) -> eyre::Result<()> {
    let (status_send, proxy_status) = crossbeam::channel::bounded::<proxy::Event>(100);
    let connection = Connection::open_with(&tio.root, None, Some(status_send));
    let device = connection.device(tio.route);

    let outcome = call_and_print(&device, &rpc_name, rpc_arg, req_type, rep_type);

    // The device holds the worker too, so both must go before its status ends.
    drop(device);
    drop(connection);
    if debug {
        for s in proxy_status.iter() {
            println!("{:?}", s);
        }
    }
    outcome
}

fn call_and_print(
    device: &Device,
    rpc_name: &str,
    rpc_arg: Option<String>,
    req_type: Option<RpcValueType>,
    rep_type: Option<RpcValueType>,
) -> eyre::Result<()> {
    use eyre::WrapErr;

    let req_type = req_type.or_else(|| {
        rpc_arg
            .is_some()
            .then(|| infer_rpc_type(rpc_name, device, "arg"))
    });

    let arg_bytes = match (rpc_arg.as_deref(), req_type.as_ref()) {
        (None, _) => Vec::new(),
        (Some(s), Some(t)) => encode_rpc_argument(s, *t)
            .wrap_err_with(|| format!("could not encode argument for RPC {}", rpc_name))?,
        (Some(_), None) => unreachable!("req_type is set whenever rpc_arg is present"),
    };

    let reply = device
        .raw_rpc(rpc_name, &arg_bytes)
        .wrap_err_with(|| format!("RPC {} failed", rpc_name))
        .with_proxy_help()?;

    if !reply.is_empty() {
        let rep_type = rep_type
            .or(req_type)
            .unwrap_or_else(|| infer_rpc_type(rpc_name, device, "ret"));
        let value = rep_type
            .decode(&reply)
            .wrap_err_with(|| format!("could not decode reply from RPC {}", rpc_name))?;
        println!("Reply: {}", format_rpc_value(&value));
    }
    println!("OK");
    Ok(())
}

pub fn rpc_dump(tio: &TioOpts, rpc_name: String, is_capture: bool) -> eyre::Result<()> {
    use eyre::WrapErr;

    let rpc_name = if is_capture {
        rpc_name.clone() + ".block"
    } else {
        rpc_name.clone()
    };

    let connection = Connection::open(&tio.root);
    let route = tio.route;
    let device = connection.device(route);

    if is_capture {
        let trigger_rpc_name = rpc_name[..rpc_name.len() - 6].to_string() + ".trigger";
        device
            .action(&trigger_rpc_name)
            .wrap_err_with(|| format!("failed to trigger {}", trigger_rpc_name))?;
    }

    let mut full_reply = vec![];

    for i in 0u16..=65535u16 {
        match device.raw_rpc(&rpc_name, i.to_le_bytes().as_ref()) {
            Ok(mut rep) => full_reply.append(&mut rep),
            Err(CallError::DeviceError(err)) => {
                if let twinleaf::proto::rpc::RpcError::Invalid = err.error {
                    break;
                } else {
                    return Err(eyre::Report::new(CallError::DeviceError(err))
                        .wrap_err(format!("RPC {} failed at chunk {}", rpc_name, i)));
                }
            }
            Err(e) => {
                return Err(eyre::Report::new(e)
                    .wrap_err(format!("RPC {} failed at chunk {}", rpc_name, i)));
            }
        }
    }

    if let Ok(s) = std::str::from_utf8(&full_reply) {
        println!("{}", s);
    } else {
        std::io::stdout()
            .write(&full_reply)
            .wrap_err("failed to write dump to stdout")?;
    }
    Ok(())
}
