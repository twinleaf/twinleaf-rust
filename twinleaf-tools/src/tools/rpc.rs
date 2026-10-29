//! The `tio rpc` subcommand and its `list`, `get`, and `dump` subcommands,
//! which call device RPCs from [`run_rpc`].

use std::io::Write;

use crate::{ProxyHelp, RPCSubcommands, RpcCli, TioOpts};
use eyre::WrapErr;
use serde_json::{json, Value};
use twinleaf::device::rpc::{
    pipelined, CallError, Channel, RpcAccess, RpcDescriptor, RpcMetaExt, RpcMetaFlags, RpcRegistry,
    RpcValue, RpcValueType, RpcValueTypeExt, Std, STANDARD,
};
use twinleaf::device::Device;
use twinleaf::{Connection, Event};

/// The most reads `tio rpc get` keeps in flight at once.
const READ_WINDOW: usize = 16;

pub fn run_rpc(rpc_cli: RpcCli) -> eyre::Result<()> {
    match rpc_cli.subcommands {
        Some(RPCSubcommands::List {
            tio,
            patterns,
            all,
            json,
        }) => list_rpcs(&tio, &patterns, all, json),
        Some(RPCSubcommands::Get {
            tio,
            patterns,
            all,
            json,
        }) => get_rpcs(&tio, &patterns, all, json),
        Some(RPCSubcommands::Dump { tio, rpc_name }) => rpc_dump(&tio, &rpc_name),
        None => rpc(rpc_cli),
    }
}

/// A dotted prefix or glob over RPC names. It selects each name it matches
/// and every name under one, as `therm` selects `therm.enable`.
struct NamePattern {
    text: String,
    node: glob::Pattern,
    subtree: glob::Pattern,
}

impl NamePattern {
    fn new(text: &str) -> eyre::Result<NamePattern> {
        let path = text.replace('.', "/");
        let compile = |glob: &str| {
            glob::Pattern::new(glob).wrap_err_with(|| format!("invalid pattern {text:?}"))
        };
        Ok(NamePattern {
            text: text.to_string(),
            node: compile(&path)?,
            subtree: compile(&format!("{path}/**"))?,
        })
    }

    fn matches(&self, name: &str) -> bool {
        let path = name.replace('.', "/");
        let options = glob::MatchOptions {
            require_literal_separator: true,
            ..Default::default()
        };
        self.node.matches_with(&path, options) || self.subtree.matches_with(&path, options)
    }
}

/// Returns the entries of `registry` that `patterns` select, in name order.
/// A hidden entry is selected under `all`, or by a pattern that selects
/// nothing else.
fn select<'a>(
    registry: &'a RpcRegistry,
    patterns: &[NamePattern],
    all: bool,
) -> Vec<&'a RpcDescriptor> {
    let explicit: Vec<bool> = patterns
        .iter()
        .map(|pattern| {
            registry
                .iter()
                .filter(|desc| pattern.matches(&desc.full_name))
                .all(is_hidden)
        })
        .collect();
    registry
        .iter()
        .filter(|desc| {
            patterns.iter().zip(&explicit).any(|(pattern, &explicit)| {
                pattern.matches(&desc.full_name) && (all || explicit || !is_hidden(desc))
            })
        })
        .collect()
}

/// Whether listing and reading leave `desc` out unless asked, as a protocol
/// entry or a privileged one is.
fn is_hidden(desc: &RpcDescriptor) -> bool {
    desc.meta.flags().contains(RpcMetaFlags::PRIVILEGED) || channel(desc) == Some(Channel::Protocol)
}

/// What `desc` is for, from the standard table when it is a standard entry
/// and from its metadata otherwise, or `None` where neither says.
fn channel(desc: &RpcDescriptor) -> Option<Channel> {
    if let Some(index) = STANDARD.iter().position(|spec| spec.name == desc.full_name) {
        return Std::at(index).map(Std::channel);
    }
    if desc.meta.is_unknown() {
        return None;
    }
    let flags = desc.meta.flags();
    match (
        flags.contains(RpcMetaFlags::PRIVILEGED),
        flags.contains(RpcMetaFlags::ANNOUNCED),
        desc.meta.access(),
    ) {
        (true, _, _) => Some(Channel::Diagnostic),
        (false, _, RpcAccess::Action) => Some(Channel::Action),
        (false, true, RpcAccess::ReadWrite) => Some(Channel::Setting),
        (false, true, RpcAccess::ReadOnly) => Some(Channel::Report),
        (false, true, RpcAccess::WriteOnly) => None,
        (false, false, RpcAccess::ReadWrite | RpcAccess::ReadOnly | RpcAccess::WriteOnly) => None,
    }
}

fn channel_name(channel: Channel) -> &'static str {
    match channel {
        Channel::Setting => "setting",
        Channel::Report => "report",
        Channel::Protocol => "protocol",
        Channel::Action => "action",
        Channel::Diagnostic => "diagnostic",
    }
}

/// The type `desc` declares as `tio rpc list` names it, or `None` for none.
fn type_name(desc: &RpcDescriptor) -> Option<String> {
    Some(desc.meta.type_str()).filter(|name| !name.is_empty())
}

/// The JSON object `tio rpc list --json` prints for `desc`.
fn describe(desc: &RpcDescriptor) -> Value {
    let flags = desc.meta.flags();
    let access: &[&str] = match desc.meta.access() {
        RpcAccess::ReadWrite => &["read", "write"],
        RpcAccess::ReadOnly => &["read"],
        RpcAccess::WriteOnly => &["write"],
        RpcAccess::Action => &[],
    };
    json!({
        "name": desc.full_name,
        "type": type_name(desc),
        "access": access,
        "persistent": desc.meta.is_persistent(),
        "announced": flags.contains(RpcMetaFlags::ANNOUNCED),
        "privileged": flags.contains(RpcMetaFlags::PRIVILEGED),
        "channel": channel(desc).map(channel_name),
    })
}

/// Compiles `texts`, or `**` for every name when there are none.
fn compile(texts: &[String]) -> eyre::Result<Vec<NamePattern>> {
    match texts {
        [] => Ok(vec![NamePattern::new("**")?]),
        texts => texts.iter().map(|text| NamePattern::new(text)).collect(),
    }
}

fn load_registry(device: &Device) -> eyre::Result<RpcRegistry> {
    device
        .rpc_registry()
        .wrap_err("failed to query RPC registry")
        .with_proxy_help()
}

fn list_rpcs(tio: &TioOpts, patterns: &[String], all: bool, json: bool) -> eyre::Result<()> {
    let patterns = compile(patterns)?;
    let connection = Connection::open(&tio.root)?;
    let registry = load_registry(&connection.device(tio.route))?;
    let selected = select(&registry, &patterns, all);

    if json {
        let rows: Vec<Value> = selected.into_iter().map(describe).collect();
        println!("{}", serde_json::to_string_pretty(&rows)?);
        return Ok(());
    }
    for desc in selected {
        println!(
            "{} {}({})",
            desc.meta.perm_str(),
            desc.full_name,
            desc.meta.type_str()
        );
    }
    Ok(())
}

/// One value `tio rpc get` read, or why it has none.
struct Reading {
    name: String,
    kind: Option<String>,
    value: Result<RpcValue, String>,
}

/// Whether `desc` reads with no argument as a value of the type it declares,
/// which a capture or an untyped entry does not.
fn is_readable(desc: &RpcDescriptor) -> bool {
    matches!(
        desc.meta.access(),
        RpcAccess::ReadOnly | RpcAccess::ReadWrite
    ) && !matches!(desc.meta.kind(), RpcValueType::Raw { .. })
}

/// Returns `value` with an f32 widened to the f64 of its shortest decimal,
/// which prints as the f32 does.
fn shortest(kind: RpcValueType, value: RpcValue) -> RpcValue {
    match value {
        RpcValue::F64(x) if kind == (RpcValueType::Float { size: 4 }) => RpcValue::F64(
            (x as f32)
                .to_string()
                .parse()
                .expect("an f32 prints as a float"),
        ),
        value => value,
    }
}

/// Reads every readable entry `patterns` select through `device`, pipelined,
/// followed by a failed reading for each pattern that selected none.
fn read_selected(
    device: &Device,
    registry: &RpcRegistry,
    patterns: &[NamePattern],
    all: bool,
) -> Vec<Reading> {
    let readable: Vec<&RpcDescriptor> = select(registry, patterns, all)
        .into_iter()
        .filter(|desc| is_readable(desc))
        .collect();
    let replies = pipelined(
        readable
            .iter()
            .map(|desc| device.submit(&desc.full_name, &[])),
        READ_WINDOW,
    );
    let reads = readable.iter().zip(replies).map(|(desc, reply)| Reading {
        name: desc.full_name.clone(),
        kind: type_name(desc),
        value: reply.map_err(|error| error.to_string()).and_then(|bytes| {
            let kind = desc.meta.kind();
            kind.decode(&bytes)
                .map(|value| shortest(kind, value))
                .map_err(|error| error.to_string())
        }),
    });
    let unmatched = patterns
        .iter()
        .filter(|pattern| !readable.iter().any(|desc| pattern.matches(&desc.full_name)))
        .map(|pattern| Reading {
            name: pattern.text.clone(),
            kind: None,
            value: Err("matches no readable RPC".to_string()),
        });
    reads.chain(unmatched).collect()
}

/// The JSON a value is written as. Bytes are hex, and a float JSON cannot
/// hold is the text Rust writes it as.
fn json_value(value: &RpcValue) -> Value {
    match value {
        RpcValue::Unit => Value::Null,
        RpcValue::U64(n) => json!(n),
        RpcValue::I64(n) => json!(n),
        RpcValue::F64(x) => serde_json::Number::from_f64(*x)
            .map_or_else(|| Value::String(x.to_string()), Value::Number),
        RpcValue::Bool(b) => json!(b),
        RpcValue::Str(s) => json!(s),
        RpcValue::Bytes(_) => json!(value.to_string()),
    }
}

/// The JSON object `tio rpc get --json` prints for `reading`.
fn reading_json(reading: &Reading) -> Value {
    match &reading.value {
        Ok(value) => {
            json!({"name": reading.name, "type": reading.kind, "value": json_value(value)})
        }
        Err(error) => json!({"name": reading.name, "type": reading.kind, "error": error}),
    }
}

fn get_rpcs(tio: &TioOpts, patterns: &[String], all: bool, json: bool) -> eyre::Result<()> {
    let patterns = compile(patterns)?;
    let connection = Connection::open(&tio.root)?;
    let device = connection.device(tio.route);
    let registry = load_registry(&device)?;
    let readings = read_selected(&device, &registry, &patterns, all);

    if json {
        let rows: Vec<Value> = readings.iter().map(reading_json).collect();
        println!("{}", serde_json::to_string_pretty(&rows)?);
    } else {
        for reading in &readings {
            match &reading.value {
                Ok(value) => println!("{} = {}", reading.name, value),
                Err(error) => println!("{} = <error: {}>", reading.name, error),
            }
        }
    }
    match readings
        .iter()
        .filter(|reading| reading.value.is_err())
        .count()
    {
        0 => Ok(()),
        failed => Err(eyre::eyre!("{failed} of {} reads failed", readings.len())),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use twinleaf::device::rpc::RpcMeta;
    use twinleaf::proxy::{HostOptions, Layout, Mount, Proxy};
    use twinleaf::DeviceRoute;

    fn entry(name: &str, kind: RpcValueType, flags: &[RpcMetaFlags]) -> RpcDescriptor {
        let flags = flags
            .iter()
            .fold(RpcMetaFlags::default(), |all, flag| all | *flag);
        RpcDescriptor::from_meta(RpcMeta::typed(kind, flags).bits(), name.to_string())
    }

    /// The standard table, as a device lists it, and a board's own entries.
    fn registry() -> RpcRegistry {
        use RpcMetaFlags as F;
        let f32 = RpcValueType::Float { size: 4 };
        let setting = [F::READABLE, F::WRITABLE, F::ANNOUNCED];
        let standard = STANDARD
            .iter()
            .map(|spec| RpcDescriptor::from_meta(spec.metadata(), spec.name.to_string()));
        let board = [
            entry("therm.enable", RpcValueType::Bool, &setting),
            entry("therm.temp", f32, &[F::READABLE, F::ANNOUNCED]),
            entry("thermal.limit", f32, &setting),
            entry("cell.0.setpoint", f32, &setting),
            entry("cell.1.setpoint", f32, &setting),
            entry("cell.1.gain", f32, &setting),
            entry("cell.1.lock.enable", RpcValueType::Bool, &setting),
            entry("diag.adc.raw", f32, &[F::READABLE, F::PRIVILEGED]),
            entry("legacy.value", f32, &[F::READABLE, F::WRITABLE]),
        ];
        RpcRegistry::new(standard.chain(board).collect())
    }

    fn selected(patterns: &[&str], all: bool) -> Vec<String> {
        let texts: Vec<String> = patterns.iter().map(|text| text.to_string()).collect();
        select(&registry(), &compile(&texts).unwrap(), all)
            .into_iter()
            .map(|desc| desc.full_name.clone())
            .collect()
    }

    #[test]
    fn a_prefix_selects_its_name_and_everything_under_it() {
        assert_eq!(selected(&["therm"], false), ["therm.enable", "therm.temp"]);
        assert_eq!(selected(&["therm.enable"], false), ["therm.enable"]);
        assert_eq!(selected(&["cell.1"], false).len(), 3);
    }

    #[test]
    fn a_star_matches_one_level_and_a_double_star_any() {
        assert_eq!(selected(&["*.enable"], false), ["therm.enable"]);
        assert_eq!(
            selected(&["**.enable"], false),
            ["cell.1.lock.enable", "therm.enable"]
        );
        assert_eq!(
            selected(&["cell.*.setpoint"], false),
            ["cell.0.setpoint", "cell.1.setpoint"]
        );
    }

    #[test]
    fn protocol_and_privileged_entries_are_hidden_unless_asked_for() {
        let everything = selected(&[], false);
        assert!(everything.contains(&"dev.name".to_string()));
        for hidden in [
            "rpc.listinfo",
            "dev.systime",
            "dev.priv.password",
            "diag.adc.raw",
        ] {
            assert!(!everything.contains(&hidden.to_string()), "{hidden} shown");
        }
        assert!(selected(&[], true).contains(&"diag.adc.raw".to_string()));
        assert!(!selected(&["dev"], false).contains(&"dev.systime".to_string()));
        assert_eq!(selected(&["rpc.*"], false).len(), 7);
        assert_eq!(selected(&["diag"], false), ["diag.adc.raw"]);
        assert_eq!(selected(&["dev.systime"], false), ["dev.systime"]);
    }

    #[test]
    fn a_pattern_that_is_not_a_glob_is_refused() {
        assert!(compile(&["cell**".to_string()]).is_err());
    }

    #[test]
    fn a_board_entry_takes_the_channel_its_metadata_says() {
        let channels: Vec<(String, Option<Channel>)> = registry()
            .iter()
            .map(|desc| (desc.full_name.clone(), channel(desc)))
            .collect();
        for (name, expected) in [
            ("therm.enable", Some(Channel::Setting)),
            ("therm.temp", Some(Channel::Report)),
            ("diag.adc.raw", Some(Channel::Diagnostic)),
            ("legacy.value", None),
            ("dev.systime", Some(Channel::Protocol)),
            ("dev.reboot", Some(Channel::Action)),
        ] {
            assert!(
                channels.contains(&(name.to_string(), expected)),
                "{name} is not {expected:?}"
            );
        }
    }

    #[test]
    fn a_value_json_cannot_hold_is_a_string() {
        assert_eq!(json_value(&RpcValue::F64(f64::NAN)), json!("NaN"));
        assert_eq!(json_value(&RpcValue::F64(f64::INFINITY)), json!("inf"));
        assert_eq!(json_value(&RpcValue::F64(f64::NEG_INFINITY)), json!("-inf"));
        assert_eq!(
            json_value(&RpcValue::Bytes(vec![0x0a, 0xff])),
            json!("0aff")
        );
        let tenth = shortest(
            RpcValueType::Float { size: 4 },
            RpcValue::F64(0.1f32.into()),
        );
        assert_eq!(json_value(&tenth), json!(0.1));
    }

    /// The simulated device, served from a thread and opened through a proxy
    /// of the test's own.
    #[test]
    fn list_and_get_describe_the_simulated_device_in_json() {
        let stop = Arc::new(AtomicBool::new(false));
        let url = crate::tools::simulate::serve(&[], stop.clone());
        let proxy = Proxy::host(HostOptions {
            port: 0,
            name: "a test".to_string(),
            default: Some(Layout {
                mounts: vec![Mount {
                    url,
                    source: DeviceRoute::root(),
                    prefix: DeviceRoute::root(),
                }],
            }),
            ..HostOptions::default()
        })
        .unwrap();
        let connection = Connection::open(&proxy.endpoint()).unwrap();
        let device = connection.device(DeviceRoute::root());
        let registry = device.rpc_registry().unwrap();
        let patterns = |texts: &[&str]| {
            compile(
                &texts
                    .iter()
                    .map(|text| text.to_string())
                    .collect::<Vec<_>>(),
            )
            .unwrap()
        };

        let listed: Vec<Value> = select(&registry, &patterns(&["test.enable", "sync"]), false)
            .into_iter()
            .map(describe)
            .collect();
        let read: Vec<Value> = read_selected(
            &device,
            &registry,
            &patterns(&["test.enable", "dev.running", "dev.nothing"]),
            false,
        )
        .iter()
        .map(reading_json)
        .collect();
        stop.store(true, Ordering::Relaxed);
        proxy.shutdown();

        assert_eq!(
            listed,
            [
                json!({"name": "sync.delay", "type": "f32", "access": ["read", "write"],
                    "persistent": true, "announced": true, "privileged": false,
                    "channel": "setting"}),
                json!({"name": "sync.status", "type": "u8", "access": ["read"],
                    "persistent": false, "announced": true, "privileged": false,
                    "channel": "report"}),
                json!({"name": "test.enable", "type": "bool", "access": ["read", "write"],
                    "persistent": false, "announced": true, "privileged": false,
                    "channel": "setting"}),
            ]
        );
        assert_eq!(
            read,
            [
                json!({"name": "dev.running", "type": "bool", "value": true}),
                json!({"name": "test.enable", "type": "bool", "value": true}),
                json!({"name": "dev.nothing", "type": null, "error": "matches no readable RPC"}),
            ]
        );
    }
}
