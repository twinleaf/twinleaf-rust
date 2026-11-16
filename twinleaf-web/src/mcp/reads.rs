//! What each tool reads of the hub and says of it. Each reads through what
//! the hub offers its pages, `Live` and its devices, and says what it read
//! as short text.

use super::tools::{Call, Door, Export, ListRpcs, Log, ReadRows, Span};
use crate::hub::Live;
use crate::protocol::{CallReply, Frame, Outcome};
use serde_json::Value;
use std::sync::Arc;
use twinleaf::data::csv::MergedColumn;
use twinleaf::DeviceRoute;

pub(super) use super::glance::glance;

/// A failed tool call's plain sentence.
pub(super) type Failed = String;

/// The span a call takes when it names none, in seconds before the newest
/// row.
const SECONDS: f64 = 10.0;
/// The RPCs `list_rpcs` lists at most.
const RPCS: usize = 200;
/// The rows `read_rows` gives when asked for no number, and at most.
const ROWS: (usize, usize) = (50, 200);
/// The lines `log` gives when asked for no number, and at most.
const LINES: (usize, usize) = (30, 200);

/// The messages among `frames` of the type `kind`.
fn messages<'a>(frames: &'a [Frame], kind: &'a str) -> impl Iterator<Item = Value> + 'a {
    frames.iter().filter_map(move |frame| match frame {
        Frame::Text(text) => serde_json::from_str::<Value>(text)
            .ok()
            .filter(|message| message["type"] == kind),
        Frame::Binary(_) => None,
    })
}

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

/// The rows of `column`.
pub(super) fn rows_of(column: &MergedColumn) -> usize {
    column.batches.iter().map(|batch| batch.len()).sum()
}

/// The span `span` names: `t0` to `t1`, from `t0` to the newest row, the
/// `seconds` before `t1`, or the newest `seconds`.
fn bounds(newest: f64, span: Span) -> Result<(f64, f64), Failed> {
    let seconds = span.seconds.unwrap_or(SECONDS);
    if !(seconds.is_finite() && seconds > 0.0) {
        return Err(format!("seconds must be a positive number, not {seconds}"));
    }
    let (t0, t1) = match (span.t0, span.t1) {
        (Some(t0), Some(t1)) => (t0, t1),
        (Some(t0), None) => (t0, newest),
        (None, Some(t1)) => (t1 - seconds, t1),
        (None, None) => (newest - seconds, newest),
    };
    if !(t0.is_finite() && t1.is_finite() && t0 < t1) {
        return Err(format!(
            "the span from {t0} to {t1} holds no time, as t0 must come before t1"
        ));
    }
    Ok((t0, t1))
}

/// The hub, started when none lives.
pub(super) async fn live(door: &Door) -> Result<Arc<Live>, Failed> {
    (door.hubs.live().await)
        .map_err(|refused| format!("the hub has nothing mounted, as {}", refused.error))
}

/// Every signal of the hub, by trace id, each with its rows in [t0, t1].
/// The rows share the history's chunks.
async fn every_signal(live: &Live, t0: f64, t1: f64) -> Result<Vec<MergedColumn>, Failed> {
    (live.export((0..=u16::MAX).collect(), t0, t1).await).map_err(retired)
}

/// The trace ids of the signals `names`, and the span `span` names of them,
/// whose newest row is the newest of any of them.
pub(super) async fn resolve(
    live: &Live,
    names: &[String],
    span: Span,
) -> Result<(Vec<u16>, f64, f64), Failed> {
    if names.is_empty() {
        return Err("name at least one signal".into());
    }
    let all = every_signal(live, f64::NEG_INFINITY, f64::INFINITY).await?;
    if all.is_empty() {
        return Err("the hub has no signals yet".into());
    }
    let mut ids = Vec::new();
    let mut newest = f64::NEG_INFINITY;
    for name in names {
        let id = (all.iter().position(|signal| signal.name == *name)).ok_or_else(|| {
            format!("there is no signal {name}, and list_devices names those there are")
        })?;
        let last = all[id].batches.last().and_then(|b| b.timestamps().last());
        newest = newest.max(last.copied().unwrap_or(f64::NEG_INFINITY));
        ids.push(id as u16);
    }
    if !newest.is_finite() {
        return Err(format!("the hub holds no rows of {}", names.join(", ")));
    }
    let (t0, t1) = bounds(newest, span)?;
    Ok((ids, t0, t1))
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
    if columns.iter().all(|column| rows_of(column) == 0) {
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

pub(super) async fn devices(door: &Door) -> Result<String, Failed> {
    let live = live(door).await?;
    let (frames, _) = live.snapshot(0).await.map_err(retired)?;
    let routes: Vec<String> = messages(&frames, "routes")
        .flat_map(|message| {
            let routes = message["routes"].as_array().cloned().unwrap_or_default();
            (routes.into_iter()).filter_map(|r| r.as_str().map(str::to_string))
        })
        .collect();
    if routes.is_empty() {
        return Err("the hub has no device mounted yet".into());
    }
    let names: Vec<String> = every_signal(&live, f64::INFINITY, f64::INFINITY)
        .await?
        .into_iter()
        .map(|signal| signal.name)
        .collect();
    let traces: Vec<Value> = messages(&frames, "trace")
        .map(|message| message["trace"].clone())
        .collect();
    let mut text = String::new();
    for route in &routes {
        let of = |message: &Value| message["route"] == route.as_str();
        let device = messages(&frames, "device").find(of);
        let loss = messages(&frames, "loss").find(of);
        let field = |name: &str| {
            let value = device.as_ref().and_then(|d| d[name].as_str());
            value.map(str::to_string)
        };
        text += route;
        if let Some(name) = field("name") {
            text += &format!(" {name}");
        }
        if let Some(serial) = field("serial") {
            text += &format!(", serial {serial}");
        }
        if let Some(firmware) = field("firmware") {
            text += &format!(", firmware {firmware}");
        }
        let presence = field("presence").unwrap_or_else(|| "unknown".into());
        text += &format!(", {presence}\n");
        if let Some(link) = loss.as_ref().map(|l| &l["link"]) {
            let times = link["times"].as_u64().unwrap_or(0);
            if times > 0 {
                let seconds = link["seconds"].as_f64().unwrap_or(0.0);
                text += &format!("  link dropped {times} times, {} s down\n", num(seconds));
            }
        }
        let mine: Vec<&Value> = traces.iter().filter(|t| of(t)).collect();
        let mut streams: Vec<&str> = mine.iter().filter_map(|t| t["stream"].as_str()).collect();
        streams.dedup();
        for stream in streams {
            let columns: Vec<&Value> = (mine.iter().copied())
                .filter(|t| t["stream"] == stream)
                .collect();
            let rate = columns[0]["rate"].as_f64().unwrap_or(0.0);
            let signals: Vec<String> = columns
                .iter()
                .map(|t| {
                    let id = t["id"].as_u64().unwrap_or(u64::MAX) as usize;
                    let name = names.get(id).cloned().unwrap_or_default();
                    match t["units"].as_str().filter(|u| !u.is_empty()) {
                        Some(units) => format!("{name} ({units})"),
                        None => name,
                    }
                })
                .collect();
            text += &format!("  {stream} at {} Hz: {}\n", num(rate), signals.join(", "));
            let lost = loss.as_ref().map(|l| &l["streams"][stream]);
            if let Some(lost) = lost.filter(|lost| lost.is_object()) {
                let counts: Vec<String> = [
                    ("skipped by the device", "skipped"),
                    ("lost with the link", "link"),
                    ("dropped by the proxy", "proxy"),
                    ("dropped by the hub", "hub"),
                ]
                .iter()
                .filter_map(|(said, key)| {
                    let n = lost[key].as_u64().unwrap_or(0);
                    (n > 0).then(|| format!("{n} {said}"))
                })
                .collect();
                if !counts.is_empty() {
                    text += &format!("    rows lost: {}\n", counts.join(", "));
                }
            }
        }
    }
    Ok(text.trim_end().to_string())
}

pub(super) async fn rpcs(door: &Door, args: ListRpcs) -> Result<String, Failed> {
    let route = (args.route.parse::<DeviceRoute>())
        .map_err(|error| format!("{} is no route, as {error}", args.route))?
        .to_string();
    let live = live(door).await?;
    let frames = live.devices.snapshot();
    let mine = |message: &Value| message["route"] == route.as_str();
    let table = (messages(&frames, "table").find(mine))
        .ok_or_else(|| format!("the hub has no RPC table of a device at {route}"))?;
    if let Some(error) = table["error"].as_str() {
        return Err(format!("the RPC table of {route} did not load, as {error}"));
    }
    let settings = (messages(&frames, "settings").find(mine))
        .map(|m| m["values"].clone())
        .unwrap_or(Value::Null);
    let filter = args.filter.unwrap_or_default();
    let rpcs = table["rpcs"].as_array().cloned().unwrap_or_default();
    let matching: Vec<&Value> = (rpcs.iter())
        .filter(|rpc| rpc["name"].as_str().is_some_and(|n| n.contains(&filter)))
        .collect();
    if matching.is_empty() {
        return Ok(format!("{route} has no RPC whose name holds {filter:?}"));
    }
    let mut text = String::new();
    for rpc in matching.iter().take(RPCS) {
        let name = rpc["name"].as_str().unwrap_or_default();
        let kind = rpc["type"].as_str().unwrap_or("none");
        let access = rpc["access"].as_array().cloned().unwrap_or_default();
        let mut access: String = (access.iter())
            .filter_map(|a| match a.as_str() {
                Some("read") => Some('r'),
                Some("write") => Some('w'),
                _ => None,
            })
            .collect();
        if access.is_empty() {
            access = "action".into();
        }
        text += &format!("{name} {kind} {access}");
        if let Some(value) = settings.get(name) {
            text += &format!(" = {value}");
        }
        text.push('\n');
    }
    if matching.len() > RPCS {
        let more = matching.len() - RPCS;
        text += &format!("and {more} more, which a filter narrows\n");
    }
    Ok(text.trim_end().to_string())
}

pub(super) async fn call(door: &Door, args: Call) -> Result<String, Failed> {
    if args.routes.is_empty() {
        return Err("name at least one route".into());
    }
    let live = live(door).await?;
    let (name, routes) = (&args.name, &args.routes);
    let Some(value) = args.value else {
        return read(&live, name, routes).await;
    };
    let CallReply { sent, results } = crate::devices::call(&live.tree, name, &value, routes).await;
    let mut text: Vec<String> = results.into_iter().map(outcome).collect();
    if !sent {
        text.push("nothing was sent to any device".into());
    }
    Ok(text.join("\n"))
}

/// Reads `name` from each device of `routes`, as the page reads what no
/// announcement keeps current. A device whose `name` is no value to read
/// is called with no argument, as an action is.
async fn read(live: &Live, name: &str, routes: &[String]) -> Result<String, Failed> {
    let mut text = Vec::new();
    let names = [name.to_string()];
    for route in routes {
        let read = crate::devices::read(&live.tree, &live.devices, route, &names).await;
        let said = match read.map(|mut reply| reply.values.remove(name)) {
            Ok(Some(read)) => outcome(read),
            Ok(None) => {
                let at = [route.clone()];
                let CallReply { results, .. } =
                    crate::devices::call(&live.tree, name, "", &at).await;
                results.into_iter().map(outcome).collect()
            }
            Err(refused) => format!("{route}: refused, {}", refused.error),
        };
        text.push(said);
    }
    Ok(text.join("\n"))
}

/// What the device made of a call or a read, on a line.
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

pub(super) async fn rows(door: &Door, args: ReadRows) -> Result<String, Failed> {
    let live = live(door).await?;
    let (ids, t0, t1) = resolve(&live, &[args.signal], args.span).await?;
    let columns = span_rows(&live, ids, t0, t1).await?;
    let column = &columns[0];
    let limit = args.limit.unwrap_or(ROWS.0).min(ROWS.1);
    let count = rows_of(column);
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
        let values = values.slice(0..taken).to_f64().into_owned();
        for (t, value) in batch.timestamps().iter().zip(values) {
            text += &format!("{t} {value}\n");
        }
        left -= taken;
    }
    Ok(text.trim_end().to_string())
}

pub(super) async fn export(door: &Door, args: Export) -> Result<String, Failed> {
    let path = crate::export::path_of(args.name.as_deref(), args.format)?;
    let live = live(door).await?;
    let (ids, t0, t1) = resolve(&live, &args.signals, args.span).await?;
    let columns = span_rows(&live, ids, t0, t1).await?;
    let counts: Vec<String> = (columns.iter())
        .map(|column| format!("{:?} {}", column.name, rows_of(column)))
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

pub(super) async fn log(door: &Door, args: Log) -> Result<String, Failed> {
    let live = live(door).await?;
    let (frames, _) = live.snapshot(0).await.map_err(retired)?;
    let lines: Vec<Value> = messages(&frames, "log").collect();
    let wanted = args.lines.unwrap_or(LINES.0).min(LINES.1);
    let newest = &lines[lines.len().saturating_sub(wanted)..];
    if newest.is_empty() {
        return Ok("the log is empty".into());
    }
    let text: Vec<String> = (newest.iter())
        .map(|line| {
            let who = match (line["part"].as_str(), line["route"].as_str()) {
                (Some(part), Some(route)) => format!("{part} of {route}"),
                (Some(part), None) => part.to_string(),
                (None, Some(route)) => format!("device {route}"),
                (None, None) => "proxy".to_string(),
            };
            let at = (line["time"].as_f64())
                .map(|t| format!(" at {} s", time(t)))
                .unwrap_or_default();
            format!(
                "{} {} {who}{at}: {}",
                line["n"],
                line["level"].as_str().unwrap_or("info"),
                line["text"].as_str().unwrap_or_default()
            )
        })
        .collect();
    Ok(text.join("\n"))
}
