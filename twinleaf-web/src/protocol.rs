//! What the page and the server say to each other, in one place. The page's
//! `ui/src/lib/protocol.js` mirrors it.
//!
//! # The socket
//!
//! `/ws` carries [`Message`]s as JSON text frames and points and spectra as
//! the binary frames `wire` lays out. The page holds one hub's state, and it
//! comes to hold it one way, from a snapshot.
//!
//! - A snapshot is the hub's whole state. It starts with [`Message::Hub`],
//!   which names the hub and the number of the last change the snapshot
//!   holds. Then come a [`Message::Trace`] for every trace, one points frame
//!   of every point the hub has sent, a [`Message::Config`], and the newest
//!   [`Message::Routes`], [`Message::Device`], [`Message::Table`] and
//!   [`Message::Settings`] of every device. The page replaces all it holds of
//!   any hub with it.
//! - A change comes after it, numbered one more than the change before. It
//!   is a points frame as each flush has new points, a spectrum frame per
//!   trace as each is estimated, a [`Message::Trace`] as a new column
//!   appears, a device's message as the proxy learns more of it, or
//!   [`Message::Clear`] when the decimation changes, followed by the history
//!   again and a [`Message::Config`]. A message of a snapshot carries no
//!   number and a binary frame of one carries 0.
//!
//! The server sends a snapshot as a page connects, and again in place of the
//! changes a page fell too far behind to be sent. A page that finds a gap, a
//! change whose number is not the next, or a trace id the snapshot did not
//! name, connects again for a snapshot. The socket closes once the hub
//! retires, as a page mounts other devices or the hub's connection ends, and
//! the page connects again to the hub that takes its place. Every way to the
//! page's state is that one path.
//!
//! The page sends one message on the socket, [`PageMessage::Spectra`], which
//! names the traces it shows as spectra, after each snapshot and whenever the
//! set changes. The hub estimates the spectra of only the traces some page
//! names, and sends each to every page.
//!
//! A device's presence, metadata, RPC table and settings are each the newest
//! value, as the proxy keeps them, so the hub keeps the newest of each and a
//! snapshot holds it. Only points and spectra are a stream, and the snapshot
//! holds the decimated history of the points.
//!
//! # The requests
//!
//! Each `/api` route takes one request type here and answers one reply type,
//! as JSON. A request the server cannot serve answers a [`Refusal`], a status
//! other than 2xx with a body of `{"error": "..."}`. What each device made of
//! a call is part of the call's reply, an [`Outcome`] per device, and no
//! refusal.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// One frame of the socket, already encoded, which every page is sent as is.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Frame {
    Text(String),
    Binary(Vec<u8>),
}

/// A text frame of the socket.
#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "lowercase")]
pub(crate) enum Message {
    /// Starts a snapshot. `id` names the hub, which a window request names
    /// back, as trace ids start over in each hub. `seq` is the number of the
    /// last change the snapshot holds.
    Hub { id: String, seq: u64 },
    /// A trace, one float column of a stream, which points and spectra name
    /// by its id.
    Trace { trace: TraceInfo },
    /// Every trace's points are dropped. The history follows, decimated anew.
    Clear,
    /// The decimation, in points per second per trace.
    Config { pps: f64 },
    /// What the hub's buffer of raw rows holds and may hold: `bytes` of the
    /// `budget`, and `seconds` of history in the stream that keeps the
    /// least, of the `max_age` each keeps at most. `rate` is the bytes per
    /// second the streams add, so the budget lasts `budget / rate` seconds.
    /// `held` is the history captured while a page holds a plot paused, and
    /// `oldest` the oldest time the history holds of each trace, by id,
    /// captured or not, where a paused plot's history begins.
    Memory {
        bytes: u64,
        budget: u64,
        seconds: f64,
        max_age: f64,
        rate: f64,
        held: Option<Held>,
        oldest: BTreeMap<u16, f64>,
    },
    /// The devices there are, by route, and the devices the hub mounted, or
    /// `None` for the proxy's default.
    Routes {
        routes: Vec<String>,
        mounts: Option<Vec<MountInfo>>,
    },
    /// What the proxy knows of one device besides its RPCs.
    Device {
        route: String,
        name: Option<String>,
        serial: Option<String>,
        firmware: Option<String>,
        presence: Presence,
    },
    /// One device's RPC table, or why it did not load.
    Table {
        route: String,
        #[serde(flatten)]
        table: TableState,
    },
    /// The values one device last announced, by RPC name.
    Settings {
        route: String,
        values: BTreeMap<String, Value>,
    },
}

impl Message {
    /// The message as a frame of a snapshot.
    pub(crate) fn frame(&self) -> Frame {
        Frame::Text(serde_json::to_string(self).expect("a message is plain data"))
    }

    /// The message as change `seq`.
    pub(crate) fn numbered(&self, seq: u64) -> Frame {
        #[derive(Serialize)]
        struct Numbered<'a> {
            seq: u64,
            #[serde(flatten)]
            message: &'a Message,
        }
        let numbered = Numbered { seq, message: self };
        Frame::Text(serde_json::to_string(&numbered).expect("a message is plain data"))
    }
}

/// The raw history the hub captured while a page holds a plot paused: the
/// bytes it holds that the live buffer no longer does, which is what holding
/// it costs.
#[derive(Serialize, Clone, Copy, Debug, PartialEq)]
pub(crate) struct Held {
    pub(crate) bytes: u64,
}

/// A text frame the page sends on the socket.
#[derive(Deserialize, Debug, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub(crate) enum PageMessage {
    /// The traces the page shows as spectra, by id, in place of those it
    /// named before.
    Spectra { traces: Vec<u16> },
    /// Whether the page holds a plot paused, so the hub keeps the history as
    /// it was at the pause, as a scope's Stop does.
    Hold { on: bool },
}

/// A trace as the page names it.
#[derive(Serialize, Clone, Debug, PartialEq)]
pub(crate) struct TraceInfo {
    pub(crate) id: u16,
    /// The device's route, as `/` or `/0`.
    pub(crate) route: String,
    pub(crate) stream: String,
    pub(crate) column: String,
    pub(crate) units: String,
    /// Samples per second.
    pub(crate) rate: f64,
    /// The timescale of the trace's time, as its segment names it: `unix`
    /// for Unix seconds, `systime` for its source's uptime, `zero` for an
    /// arbitrary zero, or `invalid`.
    pub(crate) epoch: String,
}

/// One device the hub mounted, the subtree `source` of `url` at `prefix`.
#[derive(Serialize, Clone, Debug)]
pub(crate) struct MountInfo {
    pub(crate) url: String,
    pub(crate) source: String,
    pub(crate) prefix: String,
}

#[derive(Serialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Presence {
    Connecting,
    Alive,
    Lost,
}

/// A device's RPC table as it loaded, or why it did not.
#[derive(Serialize, Clone, Debug)]
#[serde(untagged)]
pub(crate) enum TableState {
    Loaded(TableInfo),
    Failed { error: String },
}

/// A loaded RPC table.
#[derive(Serialize, Clone, Debug)]
pub(crate) struct TableInfo {
    pub(crate) rpcs: Vec<RpcInfo>,
}

/// One entry of a table as the page lists it.
#[derive(Serialize, Clone, Debug)]
pub(crate) struct RpcInfo {
    pub(crate) name: String,
    /// The type it takes and returns, or `None` for none.
    #[serde(rename = "type")]
    pub(crate) kind: Option<String>,
    /// Read, write, both, or neither for an action.
    pub(crate) access: Vec<Access>,
    pub(crate) persistent: bool,
    /// Whether a call with no argument reads its value.
    pub(crate) readable: bool,
    /// What it is for, as [`Channel::name`](twinleaf::device::rpc::Channel)
    /// names it, or `None` where nothing says.
    pub(crate) channel: Option<&'static str>,
}

/// What a call of an entry may do.
#[derive(Serialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Access {
    Read,
    Write,
}

/// `POST /api/call`. Calls one RPC on several devices with one argument.
#[derive(Deserialize, Debug)]
pub(crate) struct CallRequest {
    pub(crate) name: String,
    /// The argument as a person typed it, empty for none.
    #[serde(default)]
    pub(crate) value: String,
    pub(crate) routes: Vec<String>,
}

/// The reply to a [`CallRequest`], an outcome for every route it named, in
/// its order. Nothing was sent unless `sent`.
#[derive(Serialize, Debug, PartialEq)]
pub(crate) struct CallReply {
    pub(crate) sent: bool,
    pub(crate) results: Vec<Outcome>,
}

/// What one device made of a call or a read.
#[derive(Serialize, Debug, PartialEq)]
#[serde(tag = "state", rename_all = "lowercase")]
pub(crate) enum Outcome {
    /// It answered `value`, `null` for none.
    Ok { route: String, value: Value },
    /// It answered with an error.
    Error { route: String, error: String },
    /// Its reply timed out or was lost, so the call may or may not have
    /// taken effect.
    Unknown { route: String, error: String },
    /// The call could not be made of it, so nothing was sent to any device.
    Refused { route: String, error: String },
    /// It would have taken the call, but another device refused it, so
    /// nothing was sent.
    Skipped { route: String },
}

/// `POST /api/read`. Reads entries of one device that no announcement keeps
/// current.
#[derive(Deserialize, Debug)]
pub(crate) struct ReadRequest {
    pub(crate) route: String,
    pub(crate) names: Vec<String>,
}

/// The reply to a [`ReadRequest`], an outcome per readable name.
#[derive(Serialize, Debug)]
pub(crate) struct ReadReply {
    pub(crate) values: BTreeMap<String, Outcome>,
}

/// `GET /api/discover`. Looks for devices this host reaches.
#[derive(Deserialize, Debug)]
pub(crate) struct DiscoverRequest {
    /// Whether to browse the network over mDNS as well as the serial ports.
    #[serde(default = "yes")]
    pub(crate) network: bool,
}

fn yes() -> bool {
    true
}

/// The reply to a [`DiscoverRequest`].
#[derive(Serialize, Debug)]
pub(crate) struct DiscoverReply {
    pub(crate) devices: Vec<FoundDevice>,
    /// Why a part of the scan could not run, as a person reads it.
    pub(crate) notices: Vec<String>,
}

/// One device a scan found.
#[derive(Serialize, Debug)]
pub(crate) struct FoundDevice {
    pub(crate) url: String,
    pub(crate) interface: Interface,
    pub(crate) name: Option<String>,
    /// The subdevices routed behind it.
    pub(crate) routes: Vec<FoundRoute>,
}

/// How a found device is reached.
#[derive(Serialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Interface {
    Usb,
    Net,
    Serial,
    /// A way this build names no better, or not yet known.
    #[default]
    Other,
}

#[derive(Serialize, Debug)]
pub(crate) struct FoundRoute {
    pub(crate) route: String,
    pub(crate) name: Option<String>,
}

/// `POST /api/mount`. Opens devices in place of the hub's.
#[derive(Deserialize, Debug)]
pub(crate) struct MountRequest {
    /// The devices to open, or none for the proxy's default.
    #[serde(default)]
    pub(crate) mounts: Vec<MountArg>,
}

/// One device to open, the subtree `source` of `url`, the root for none.
#[derive(Deserialize, Debug, Clone)]
pub(crate) struct MountArg {
    pub(crate) url: String,
    #[serde(default)]
    pub(crate) source: Option<String>,
}

/// `POST /api/pps`. Changes the decimation, which every page shares.
#[derive(Deserialize, Serialize, Debug)]
pub(crate) struct PpsRequest {
    pub(crate) pps: f64,
}

/// `POST /api/history`. Sets how much raw history the hub keeps: at most
/// `seconds` of each stream's own time, and `bytes` across every stream.
#[derive(Deserialize, Serialize, Debug)]
pub(crate) struct HistoryRequest {
    pub(crate) seconds: f64,
    pub(crate) bytes: u64,
}

/// `GET /api/window`. Reads the raw rows of one trace between two times.
#[derive(Deserialize, Debug)]
pub(crate) struct WindowRequest {
    /// The hub the trace id is of, as [`Message::Hub`] named it.
    pub(crate) hub: String,
    pub(crate) id: u16,
    pub(crate) t0: f64,
    pub(crate) t1: f64,
    /// The plot's width in pixels, which the rows are decimated to.
    pub(crate) width: usize,
}

/// A request for every raw row of one trace within [t0, t1], as copying
/// reads them, or only how many there are when more than `limit`.
#[derive(Deserialize, Debug)]
pub(crate) struct RowsRequest {
    /// The hub the trace id is of, as [`Message::Hub`] named it.
    pub(crate) hub: String,
    pub(crate) id: u16,
    pub(crate) t0: f64,
    pub(crate) t1: f64,
    pub(crate) limit: usize,
}

/// The reply to a [`RowsRequest`]: `count` rows, their times `x` and values
/// `y`, empty when there are more than the limit, and the oldest time the
/// history still holds of the trace, from which a reader can tell what of
/// its range is gone.
#[derive(Serialize, Debug, PartialEq)]
pub(crate) struct Rows {
    pub(crate) count: usize,
    pub(crate) oldest: Option<f64>,
    pub(crate) x: Vec<f64>,
    pub(crate) y: Vec<f64>,
}

/// The reply to a [`WindowRequest`].
#[derive(Serialize, Debug)]
pub(crate) struct Window {
    pub(crate) t0: f64,
    pub(crate) t1: f64,
    /// Raw rows per point, 1 for raw rows.
    pub(crate) ratio: usize,
    /// The points, split where the samples have gaps.
    pub(crate) chunks: Vec<WindowChunk>,
}

#[derive(Serialize, Debug)]
pub(crate) struct WindowChunk {
    pub(crate) x: Vec<f64>,
    pub(crate) y: Vec<f64>,
}

/// The reply to a request that changes something and has nothing to say.
#[derive(Serialize, Debug)]
pub(crate) struct Done {}

/// Why a request was not served, as the page hears it.
#[derive(Debug, PartialEq)]
pub(crate) struct Refusal {
    pub(crate) status: StatusCode,
    pub(crate) error: String,
}

impl Refusal {
    pub(crate) fn new(status: StatusCode, error: impl ToString) -> Refusal {
        Refusal {
            status,
            error: error.to_string(),
        }
    }
}

impl IntoResponse for Refusal {
    fn into_response(self) -> Response {
        let body = serde_json::json!({ "error": self.error });
        (self.status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn json_of(message: &Message) -> Value {
        match message.frame() {
            Frame::Text(text) => serde_json::from_str(&text).unwrap(),
            Frame::Binary(_) => unreachable!(),
        }
    }

    #[test]
    fn a_message_names_its_type() {
        assert_eq!(json_of(&Message::Clear), json!({"type": "clear"}));
        assert_eq!(
            json_of(&Message::Hub {
                id: "7".into(),
                seq: 3
            }),
            json!({"type": "hub", "id": "7", "seq": 3})
        );
    }

    #[test]
    fn a_table_is_its_entries_or_its_error() {
        let failed = Message::Table {
            route: "/".into(),
            table: TableState::Failed {
                error: "timed out".into(),
            },
        };
        assert_eq!(
            json_of(&failed),
            json!({"type": "table", "route": "/", "error": "timed out"})
        );
        let loaded = Message::Table {
            route: "/0".into(),
            table: TableState::Loaded(TableInfo { rpcs: vec![] }),
        };
        assert_eq!(
            json_of(&loaded),
            json!({"type": "table", "route": "/0", "rpcs": []})
        );
    }

    #[test]
    fn a_page_names_the_spectra_it_shows() {
        let message: PageMessage =
            serde_json::from_str(r#"{"type": "spectra", "traces": [1, 4]}"#).unwrap();
        assert_eq!(message, PageMessage::Spectra { traces: vec![1, 4] });
    }

    #[test]
    fn an_outcome_names_its_state() {
        let skipped = serde_json::to_value(Outcome::Skipped { route: "/1".into() }).unwrap();
        assert_eq!(skipped, json!({"state": "skipped", "route": "/1"}));
    }
}
