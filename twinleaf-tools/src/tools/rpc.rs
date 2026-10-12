//! The `tio rpc` subcommand and its `list` and `dump` subcommands, which call
//! device RPCs from [`run_rpc`].

use std::io::Write;

use crate::{ProxyHelp, RPCSubcommands, RpcCli, TioOpts};
use twinleaf::device::rpc::{CallError, RpcMetaExt, RpcValueType, RpcValueTypeExt};
use twinleaf::device::Device;
use twinleaf::{Connection, Event};

pub fn run_rpc(rpc_cli: RpcCli) -> eyre::Result<()> {
    match rpc_cli.subcommands {
        Some(RPCSubcommands::List { tio }) => list_rpcs(&tio),
        Some(RPCSubcommands::Dump { tio, rpc_name }) => rpc_dump(&tio, &rpc_name),
        None => rpc(rpc_cli),
    }
}

fn list_rpcs(tio: &TioOpts) -> eyre::Result<()> {
    use eyre::WrapErr;

    let connection = Connection::open(&tio.root)?;
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
    match device.rpc_meta(name) {
        Ok(meta) => meta.text_type(),
        Err(_) => {
            println!("Unknown RPC {kind} type, assuming 'string'. Use -t/-T to override.");
            RpcValueType::String { max_len: None }
        }
    }
}

fn rpc(cli: RpcCli) -> eyre::Result<()> {
    let connection = Connection::open(&cli.tio.root)?;
    // The link's statuses arrive among the connection's events.
    let events = cli.debug.then(|| connection.events());
    let device = connection.device(cli.tio.route);

    let outcome = call_and_print(&device, &cli);

    if let Some(events) = events {
        while let Ok(Some(event)) = events.try_recv() {
            if let Event::Link { subtree, event } = event {
                println!("{subtree}: {event:?}");
            }
        }
    }
    outcome
}

fn call_and_print(device: &Device, cli: &RpcCli) -> eyre::Result<()> {
    use eyre::WrapErr;

    let rpc_name = cli.rpc_name.as_deref().unwrap_or_default();
    let req_type = cli.req_type.or_else(|| {
        cli.rpc_arg
            .is_some()
            .then(|| infer_rpc_type(rpc_name, device, "arg"))
    });

    let arg_bytes = match (cli.rpc_arg.as_deref(), req_type) {
        (None, _) => Vec::new(),
        (Some(s), Some(t)) => t
            .encode_text(s)
            .wrap_err_with(|| format!("could not encode argument for RPC {}", rpc_name))?,
        (Some(_), None) => unreachable!("req_type is set whenever rpc_arg is present"),
    };

    let reply = device
        .raw_rpc(rpc_name, &arg_bytes)
        .wrap_err_with(|| format!("RPC {} failed", rpc_name))
        .with_proxy_help()?;

    if !reply.is_empty() {
        let rep_type = cli
            .rep_type
            .or(req_type)
            .unwrap_or_else(|| infer_rpc_type(rpc_name, device, "ret"));
        let value = rep_type
            .decode(&reply)
            .wrap_err_with(|| format!("could not decode reply from RPC {}", rpc_name))?;
        println!("Reply: {}", value.to_text());
    }
    println!("OK");
    Ok(())
}

fn rpc_dump(tio: &TioOpts, rpc_name: &str) -> eyre::Result<()> {
    use eyre::WrapErr;

    let connection = Connection::open(&tio.root)?;
    let device = connection.device(tio.route);

    let mut full_reply = vec![];

    for i in 0u16..=65535u16 {
        match device.raw_rpc(rpc_name, i.to_le_bytes().as_ref()) {
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
