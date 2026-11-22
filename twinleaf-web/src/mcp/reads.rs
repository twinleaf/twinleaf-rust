//! What each tool reads of the hub and says of it. Each reads through what
//! the hub offers its readers, `Live`, its devices, its log and its counts
//! of what was lost, and says what it read as short text.

use super::tools::{Call, Door, Export, ListRpcs, Log, ReadRows, Span};
use crate::devices::Kind;
use crate::hub::{Live, Spanned};
use crate::protocol::{Access, CallReply, Message, Outcome};
use serde_json::Value;
use std::sync::Arc;
use twinleaf::data::csv::MergedColumn;

pub(super) use super::glance::glance;

/// A failed tool call's plain sentence.
pub(super) type Failed = String;

/// The RPCs `list_rpcs` lists at most.
const RPCS: usize = 200;
/// The rows `read_rows` gives when asked for no number, and at most.
const ROWS: (usize, usize) = (50, 200);
/// The lines `log` gives when asked for no number, and at most.
const LINES: (usize, usize) = (30, 200);

/// A number in few characters: four significant figures, in exponent form
/// when far from one.
pub(super) fn num(value: f64) -> String {
    let size = value.abs();
    if value == 0.0 || (1e-3..1e5).contains(&size) {
        let decimals = (3 - size.log10().floor() as i32).clamp(0, 6) as usize;
        format!("{value:.decimals$}")
    } else {
        format!("{value:.3e}")
    }
}

/// A time in seconds, to the millisecond.
pub(super) fn time(t: f64) -> String {
    format!("{t:.3}")
}

/// The word the page's messages use for `value`, as `alive` or `warn`.
fn word(value: impl serde::Serialize) -> String {
    match serde_json::to_value(value) {
        Ok(Value::String(word)) => word,
        _ => String::new(),
    }
}

/// `value`, a value a device sent, as a result writes it: a string quoted
/// and escaped, so no line break in it starts a line of the result, and
/// any other value as JSON.
fn said(value: &Value) -> String {
    match value {
        Value::String(text) => format!("{text:?}"),
        value => value.to_string(),
    }
}

/// What a tool says when the hub retired as it was asked.
pub(super) fn retired<T>(_: T) -> Failed {
    "the hub retired as it was asked, so call again".into()
}

/// The hub, started when none lives.
pub(super) async fn live(door: &Door) -> Result<Arc<Live>, Failed> {
    (door.hubs.live().await)
        .map_err(|refused| format!("the hub has nothing mounted, as {}", refused.error))
}

/// The trace ids of the signals `names`, and the span `span` names of them,
/// as the hub reads them.
pub(super) async fn resolve(live: &Live, names: &[String], span: Span) -> Result<Spanned, Failed> {
    let spanned = live.span(names.to_vec(), span.seconds, span.t0, span.t1);
    spanned.await.map_err(retired)?
}

/// Every row of traces `ids` within [t0, t1], of which there is one at
/// least.
pub(super) async fn span_rows(
    live: &Live,
    ids: Vec<u16>,
    t0: f64,
    t1: f64,
) -> Result<Vec<MergedColumn>, Failed> {
    let columns = live.export(ids, t0, t1).await.map_err(retired)?;
    if columns.iter().all(|column| column.rows() == 0) {
        let names: Vec<String> = columns.iter().map(|c| format!("{:?}", c.name)).collect();
        return Err(format!(
            "there are no rows of {} from {} to {} s",
            names.join(", "),
            time(t0),
            time(t1)
        ));
    }
    Ok(columns)
}

pub(super) async fn devices(live: &Live) -> Result<String, Failed> {
    let routes = live.devices.listed();
    if routes.is_empty() {
        return Err("the hub has no device mounted yet".into());
    }
    let signals = live.signals().await.map_err(retired)?;
    let mut text = String::new();
    for &route in &routes {
        let at = route.to_string();
        text += &at;
        match live.devices.newest(route, Kind::Device) {
            Some(Message::Device {
                name,
                serial,
                firmware,
                presence,
                ..
            }) => {
                if let Some(name) = name {
                    text += &format!(" {name:?}");
                }
                if let Some(serial) = serial {
                    text += &format!(", serial {serial:?}");
                }
                if let Some(firmware) = firmware {
                    text += &format!(", firmware {firmware:?}");
                }
                text += &format!(", {}\n", word(presence));
            }
            _ => text += ", unknown\n",
        }
        let counts = live.losses.counts(route);
        if let Some((link, _)) = &counts {
            if link.times > 0 {
                let (times, seconds) = (link.times, num(link.seconds));
                text += &format!("  link dropped {times} times, {seconds} s down\n");
            }
        }
        let mine = (signals.iter()).filter(|((device, _), _)| *device == at);
        for ((_, stream), columns) in mine {
            let named: Vec<String> = (columns.iter())
                .map(|signal| match signal.units.as_str() {
                    "" => format!("{:?}", signal.name),
                    units => format!("{:?} ({units:?})", signal.name),
                })
                .collect();
            let rate = num(columns[0].rate);
            text += &format!("  {stream:?} at {rate} Hz: {}\n", named.join(", "));
            let lost = counts
                .as_ref()
                .and_then(|(_, streams)| streams.get(stream.as_str()))
                .map(|stream| stream.lost);
            if let Some(lost) = lost {
                let counts: Vec<String> = [
                    ("skipped by the device or lost on its link", lost.skipped),
                    ("lost with the link", lost.link),
                    ("dropped by the proxy", lost.proxy),
                    ("dropped by the hub", lost.hub),
                ]
                .iter()
                .filter(|(_, n)| *n > 0)
                .map(|(said, n)| format!("{n} {said}"))
                .collect();
                if !counts.is_empty() {
                    text += &format!("    rows lost: {}\n", counts.join(", "));
                }
            }
        }
    }
    Ok(text.trim_end().to_string())
}

pub(super) async fn rpcs(live: &Live, args: ListRpcs) -> Result<String, Failed> {
    let (route, table) = (crate::devices::table(&live.tree, &args.route).await)
        .map_err(|(_, why)| format!("the RPC table of {} is not to be had, {why:?}", args.route))?;
    let rpcs: Vec<_> = (table.iter())
        .filter(|desc| !desc.is_protocol())
        .map(crate::devices::describe)
        .collect();
    let settings = match live.devices.newest(route, Kind::Settings) {
        Some(Message::Settings { values, .. }) => values,
        _ => Default::default(),
    };
    let filter = args.filter.unwrap_or_default();
    let matching: Vec<_> = (rpcs.iter())
        .filter(|rpc| rpc.name.contains(&filter))
        .collect();
    if matching.is_empty() {
        return Ok(format!("{route} has no RPC whose name holds {filter:?}"));
    }
    let mut text = String::new();
    for rpc in matching.iter().take(RPCS) {
        let kind = rpc.kind.as_deref().unwrap_or("none");
        let mut access: String = (rpc.access.iter())
            .map(|access| match access {
                Access::Read => 'r',
                Access::Write => 'w',
            })
            .collect();
        if access.is_empty() {
            access = "action".into();
        }
        text += &format!("{:?} {kind} {access}", rpc.name);
        if let Some(value) = settings.get(&rpc.name) {
            text += &format!(" = {}", said(value));
        }
        text.push('\n');
    }
    if matching.len() > RPCS {
        let more = matching.len() - RPCS;
        text += &format!("and {more} more, which a filter narrows\n");
    }
    Ok(text.trim_end().to_string())
}

pub(super) async fn call(live: &Live, args: Call) -> Result<String, Failed> {
    if args.routes.is_empty() {
        return Err("name at least one route".into());
    }
    let value = args.value.as_deref().filter(|value| !value.is_empty());
    if value.is_none() && args.run != Some(true) {
        let mut actions = Vec::new();
        for route in &args.routes {
            let Ok((_, table)) = crate::devices::table(&live.tree, route).await else {
                continue;
            };
            if (table.find(&args.name)).is_some_and(|desc| !desc.is_readable()) {
                actions.push(route.as_str());
            }
        }
        if !actions.is_empty() {
            return Err(format!(
                "{} cannot be read on {}, so a call of it with no value runs it as an action, \
                 which takes run true. Nothing was sent to any device",
                args.name,
                actions.join(", ")
            ));
        }
    }
    let CallReply { sent, results } =
        crate::devices::call(&live.tree, &args.name, value, &args.routes).await;
    let mut text: Vec<String> = results.into_iter().map(outcome).collect();
    if !sent {
        text.push("nothing was sent to any device".into());
    }
    Ok(text.join("\n"))
}

/// What the device made of a call, on a line.
fn outcome(outcome: Outcome) -> String {
    match outcome {
        Outcome::Ok { route, value } => match value {
            Value::Null => format!("{route}: ok"),
            Value::String(text) if text.is_empty() => format!("{route}: ok"),
            value => format!("{route}: ok {}", said(&value)),
        },
        Outcome::Error { route, error } => format!("{route}: error, {error:?}"),
        Outcome::Unknown { route, error } => {
            format!("{route}: unknown, it may or may not have taken effect, {error}")
        }
        Outcome::Refused { route, error } => format!("{route}: refused, {error:?}"),
        Outcome::Skipped { route } => format!("{route}: skipped"),
    }
}

pub(super) async fn rows(live: &Live, args: ReadRows) -> Result<String, Failed> {
    let (ids, t0, t1) = resolve(live, &[args.signal], args.span).await?;
    let columns = span_rows(live, ids, t0, t1).await?;
    let column = &columns[0];
    let limit = args.limit.unwrap_or(ROWS.0).min(ROWS.1);
    let count = column.rows();
    let units = if column.units.is_empty() {
        String::new()
    } else {
        format!(" in {:?}", column.units)
    };
    let (from, to) = (time(t0), time(t1));
    let mut text = format!(
        "{:?}{units}, {count} rows from {from} to {to} s",
        column.name
    );
    if count > limit {
        text += &format!(", the first {limit}");
    }
    text += ", time and value\n";
    let mut left = limit;
    for batch in &column.batches {
        if left == 0 {
            break;
        }
        let taken = batch.len().min(left);
        let Some(values) = batch.column(column.column) else {
            continue;
        };
        for (row, t) in batch.timestamps()[..taken].iter().enumerate() {
            text += &format!("{t} {}\n", values.get(row));
        }
        left -= taken;
    }
    Ok(text.trim_end().to_string())
}

pub(super) async fn export(live: &Live, args: Export) -> Result<String, Failed> {
    let path = crate::export::path_of(args.name.as_deref(), args.format)?;
    let (ids, t0, t1) = resolve(live, &args.signals, args.span).await?;
    let columns = span_rows(live, ids, t0, t1).await?;
    let counts: Vec<String> = (columns.iter())
        .map(|column| format!("{:?} {}", column.name, column.rows()))
        .collect();
    let format = args.format;
    let bytes = {
        let path = path.clone();
        tokio::task::spawn_blocking(move || crate::export::write_new(columns, format, &path))
            .await
            .map_err(|error| error.to_string())??
    };
    Ok(format!(
        "wrote {}\n{bytes} bytes of {}, {} to {} s\nrows {}",
        path.display(),
        format.extension(),
        time(t0),
        time(t1),
        counts.join(", ")
    ))
}

pub(super) async fn log(live: &Live, args: Log) -> Result<String, Failed> {
    let wanted = args.lines.unwrap_or(LINES.0).min(LINES.1);
    let lines = live.log.newest(wanted);
    if lines.is_empty() {
        return Ok("the log is empty".into());
    }
    let text: Vec<String> = (lines.into_iter())
        .filter_map(|line| match line {
            Message::Log {
                n,
                time: at,
                level,
                part,
                route,
                text,
                ..
            } => {
                let who = match (part, route) {
                    (Some(part), Some(route)) => format!("{part} of {route}"),
                    (Some(part), None) => part,
                    (None, Some(route)) => format!("device {route}"),
                    (None, None) => "proxy".to_string(),
                };
                let at = at.map(|t| format!(" at {} s", time(t))).unwrap_or_default();
                Some(format!("{n} {} {who}{at}: {text:?}", word(level)))
            }
            _ => None,
        })
        .collect();
    Ok(text.join("\n"))
}
