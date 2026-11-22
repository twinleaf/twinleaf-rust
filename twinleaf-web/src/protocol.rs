//! What the page and the server say to each other, in one place. The page
//! reads it in `ui/src/lib/protocol.js`, and its tests read what the hub
//! sends from the files `recorded` writes.
//!
//! # The socket
//!
//! `/ws` carries [`Message`]s as JSON text frames and points and spectra as
//! the binary frames `wire` lays out. The page holds one hub's state, and it
//! comes to hold it one way, from a snapshot.
//!
//! - A snapshot is the hub's whole state. It starts with [`Message::Hub`],
//!   which names the hub and the number of the last change the snapshot
//!   holds. Then come a [`Message::Trace`] for every trace, a
//!   [`Message::Config`], one points frame of every point the hub has sent,
//!   which the page keeps as the config says, and the newest
//!   [`Message::Routes`], [`Message::Device`], [`Message::Table`],
//!   [`Message::Settings`], [`Message::Captures`], [`Message::Capture`],
//!   [`Message::Firmware`] and [`Message::Upgrade`] of every device, the
//!   newest [`Message::Loss`] of every device that lost anything, and the
//!   newest lines of the log, a [`Message::Log`] each. The page replaces
//!   all it holds of any hub with it.
//! - A change comes after it, numbered one more than the change before. It
//!   is a points frame as each flush has new points, a spectrum frame per
//!   trace as each is estimated, a [`Message::Trace`] as a new column
//!   appears, a device's message as the proxy learns more of it, a
//!   [`Message::Log`] as a line of the log is taken, or
//!   [`Message::Clear`] when the decimation or a trace's rate changes,
//!   followed by the history again and a [`Message::Config`]. A message of
//!   a snapshot carries no number and a binary frame of one carries 0.
//!
//! The server sends a snapshot as a page connects, and again in place of the
//! changes a page fell too far behind to be sent. A page that finds a gap, a
//! change whose number is not the next, or a trace id the snapshot did not
//! name, connects again for a snapshot. The socket closes once the hub
//! retires, as a page mounts other devices or the hub's connection ends, and
//! the page connects again to the hub that takes its place. Every way to the
//! page's state is that one path.
//!
//! The page sends [`PageMessage::Spectra`] on the socket, which names the
//! traces it shows as spectra, after each snapshot and whenever the set
//! changes. The hub estimates the spectra of only the traces some page
//! names, and sends each to every page. The page also holds the history,
//! asks for captures and for firmware upgrades on the socket, as below.
//!
//! A device's presence, metadata, RPC table and settings are each the newest
//! value, as the proxy keeps them, so the hub keeps the newest of each and a
//! snapshot holds it. Only points and spectra are a stream, and the snapshot
//! holds the decimated history of the points.
//!
//! # What was lost
//!
//! A sample is lost in one of five places, and the page is told of each
//! apart. Four are before the hub's history, and [`Message::Loss`] counts
//! them by device, since the hub started: the rows of each stream the
//! device skipped, those lost with its link down, those the proxy dropped
//! for the hub's connection and those the hub's own queue dropped
//! ([`Lost`]), and how often and for how long the device's link was down
//! ([`Link`]). Each is the newest value, sent at most once a second while
//! it grows, and a snapshot holds it. The fifth is the page's own, the
//! changes it fell behind the hub, which the snapshot sent in their place
//! says in `behind` of its [`Message::Hub`].
//!
//! # The log
//!
//! [`Message::Log`] is one line of the log, which `logbook` keeps. A device
//! says a line in a log packet, and the proxy says the hub's own, each
//! loss the counts above grow by among them, with what was lost as a
//! [`LossLine`]. A line is a change every page hears, and a snapshot holds
//! the newest 500. The lines are numbered from 1 since the hub started, so
//! a page that hears one again, in a snapshot and as a change after it,
//! knows it by its number.
//!
//! # Captures
//!
//! A capture is a device's one-shot readout of a buffer, such as an ADC's
//! samples of one shot. A device has a capture RPC for each kind it takes,
//! and [`Message::Captures`] lists them. A capture has no time of its own,
//! and none is sent.
//!
//! The page asks for captures on the socket. [`PageMessage::Capture`] asks
//! for one capture of an RPC now, [`PageMessage::Repeat`] for captures of
//! it again and again, and [`PageMessage::Stop`] takes back what the page
//! asked of it. What a page asked goes when its socket does. The hub takes
//! the captures, one RPC's at a time, while some page wants them.
//!
//! Where each capture RPC stands is its [`Message::Capture`], a change
//! every page hears and a snapshot holds the newest of. A capture that
//! cannot be taken is a state of it too, [`CaptureState::Busy`],
//! [`CaptureState::Timeout`] or [`CaptureState::Error`], and no refusal.
//!
//! A capture the hub read goes to the pages that asked for it and to no
//! other, as one capture frame, which `wire` lays out. It holds the
//! device's elements as the device sent them, with their calibration and
//! what they are of. It is no numbered change, so its frame carries 0, and
//! no snapshot holds it. The hub numbers the captures it reads of each RPC
//! from 1, and the frame and the RPC's state carry the number. A page that
//! reads slowly is kept only the newest capture of each RPC, so the
//! numbers it is sent may skip.
//!
//! # Firmware
//!
//! The hub reads what each device runs from its `dev.desc` as its RPC table
//! loads and after an upgrade, and looks it up in the catalog of published
//! firmware only when a page sends [`PageMessage::Check`]. What a device
//! runs and what the catalog holds for it is its [`Message::Firmware`], a
//! newest value every page hears and a snapshot holds. Once a device runs
//! other firmware the hub compares it with the releases it holds, without
//! the catalog.
//!
//! [`PageMessage::Upgrade`] asks for an upgrade of one device from an
//! [`ImageSource`], the newest release the last check listed, one of its
//! releases, or a file, whose bytes follow in binary frames on the same
//! socket. An image is neither empty nor more than 16 MiB. A text message
//! or the socket closing before every byte came drops the image, and more
//! bytes than it named refuse it. [`PageMessage::Cancel`] takes back an
//! upgrade that waits or downloads, and nothing is sent to the device, and
//! clears one that is done or failed. [`PageMessage::Retry`] runs a failed
//! one again from its source. One upgrade runs at a time across the hub, in
//! the order asked, and the others wait.
//!
//! Where a device's upgrade stands is its [`Message::Upgrade`], every page
//! hears it and a snapshot holds the newest, once a page asked an upgrade
//! of the device. An ask the hub cannot take is answered to the page that
//! asked alone, as [`Message::Refused`], which is no numbered change and in
//! no snapshot. A failed upgrade is a state, and no refusal.
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
    /// last change the snapshot holds. `behind` is how many changes the
    /// page fell behind, when the snapshot is sent in their place, and 0 in
    /// the snapshot a socket opens with.
    Hub { id: String, seq: u64, behind: u64 },
    /// A trace, one float column of a stream, which points and spectra name
    /// by its id.
    Trace { trace: TraceInfo },
    /// Every trace's points are dropped. The points of the levels a new
    /// decimation or a trace's new rate names follow.
    Clear,
    /// The decimation: `pps` points per second per trace asked of the finest
    /// level, and the seconds a page keeps each level, finest first.
    Config { pps: f64, levels: Vec<f64> },
    /// What the hub's buffer of raw rows holds and may hold: `bytes` of the
    /// `budget`, and `seconds` of history in the stream that keeps the
    /// least, of the `max_age` each keeps at most. `rate` is the bytes per
    /// second the streams add, so the budget lasts `budget / rate` seconds.
    /// `held` is the history captured while a page holds a plot paused,
    /// which keeps only what of the budget the buffer leaves, so `bytes` and
    /// its bytes come to at most `budget`, and `oldest` the oldest time the
    /// history holds of each trace, by id, captured or not, where a paused
    /// plot's history begins. `paused` is what a plot paused now keeps, or
    /// `None` while nothing streams or no budget is set.
    Memory {
        bytes: u64,
        budget: u64,
        seconds: f64,
        max_age: f64,
        rate: f64,
        held: Option<Held>,
        oldest: BTreeMap<u16, f64>,
        paused: Option<Paused>,
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
    /// What was lost of one device before the hub's history held it, since
    /// the hub started: its `link`'s drops, and the rows lost of each of its
    /// `streams`, by the stream's name, as a trace names it, with the
    /// stream's rate.
    Loss {
        route: String,
        link: Link,
        streams: BTreeMap<String, StreamLoss>,
    },
    /// One line of the log. `n` numbers the lines from 1 since the hub
    /// started. `wall` is the host's clock as the hub took the line, in
    /// Unix seconds, and `time` the time of the newest sample the hub had
    /// taken of the line's device then, on the clock its traces' points
    /// are on, or `null` for a device no sample was taken of and for a line
    /// of no device. `source` says who said it, and `route` names the
    /// device it is of, the device that said it or the one the proxy says
    /// it of, or `null` for none. `part` is the part of the proxy that said
    /// a proxy's line, `hub` for the hub's own and else the source its
    /// console names, and a device's line has none. `loss` is what the line
    /// says was lost, and a line of no loss has none.
    Log {
        n: u64,
        wall: f64,
        time: Option<f64>,
        level: Level,
        source: Source,
        #[serde(skip_serializing_if = "Option::is_none")]
        part: Option<String>,
        route: Option<String>,
        text: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        loss: Option<LossLine>,
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
    /// The capture RPCs of one device, in the order of their names. It
    /// follows the device's table, and lists none for a device with none.
    /// It goes out again when a capture read of one says another label.
    Captures {
        route: String,
        captures: Vec<CaptureInfo>,
    },
    /// Where one capture RPC of one device stands. `number` is that of the
    /// last capture the hub read of it, 0 for none. `repeat` is the seconds
    /// the hub waits after each capture while pages ask for them again and
    /// again, the shortest any page asked, or `null` while none does.
    Capture {
        route: String,
        rpc: String,
        #[serde(flatten)]
        state: CaptureState,
        number: u64,
        repeat: Option<f64>,
    },
    /// The firmware one device runs, `None` until it answered `dev.desc`,
    /// what the catalog holds for it, and the most bytes an image may have.
    Firmware {
        route: String,
        installed: Option<Installed>,
        catalog: CatalogState,
        image_limit: u64,
    },
    /// Where the upgrade of one device stands, once a page asked one of it.
    Upgrade {
        route: String,
        #[serde(flatten)]
        state: UpgradeState,
        cancellable: bool,
    },
    /// The hub refused what the page asked, which only the page that asked
    /// hears. It is no numbered change and no snapshot holds it.
    Refused {
        ask: Ask,
        route: String,
        error: String,
    },
}

/// The firmware a device runs, as its `dev.desc` says. `name` and
/// `revision` are empty for a description of another form, and `date`
/// (`YYYY-MM-DD`), `build` and `serial` are `None` where it gives none.
/// `development` marks a development build, which the catalog offers no
/// release for.
#[derive(Serialize, Clone, Debug, PartialEq)]
pub(crate) struct Installed {
    pub(crate) name: String,
    pub(crate) revision: String,
    pub(crate) date: Option<String>,
    pub(crate) build: Option<String>,
    pub(crate) serial: Option<String>,
    pub(crate) development: bool,
}

/// What the catalog of published firmware holds for a device.
#[derive(Serialize, Clone, Debug, PartialEq)]
#[serde(tag = "state", rename_all = "lowercase")]
pub(crate) enum CatalogState {
    /// No page has asked the hub to look the device up.
    Unchecked,
    /// The hub is looking it up.
    Checking,
    /// The catalog could not be listed, as `error` says.
    Unreachable { error: String },
    /// How the installed firmware compares with the newest release, and
    /// every release for the device, newest first.
    Checked {
        status: UpdateStatus,
        releases: Vec<ReleaseInfo>,
    },
}

/// How a device's firmware compares with the newest release.
#[derive(Serialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum UpdateStatus {
    /// A newer release is published.
    Update,
    /// The device runs the newest release, or a build at least as new.
    Current,
    /// No firmware is published for the device, or its `dev.desc` names no
    /// sensor and revision to look it up by.
    None,
    /// The device runs a development build.
    Development,
    /// The device's build date is unknown, and so is whether the newest
    /// release is newer.
    Unknown,
}

/// One published release, its build `date` and `build` and the `file` an
/// upgrade names it by.
#[derive(Serialize, Clone, Debug, PartialEq)]
pub(crate) struct ReleaseInfo {
    pub(crate) date: String,
    pub(crate) build: String,
    pub(crate) file: String,
    /// Whether it is the build the device runs.
    pub(crate) installed: bool,
}

/// Where an image comes from, the newest release, a release the page
/// chose, or a file the page sent.
#[derive(Serialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum SourceKind {
    Latest,
    Release,
    File,
}

/// The step an upgrade failed at. `check` is an image the device cannot
/// take, or a failure before anything was stopped or sent.
#[derive(Serialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Step {
    Check,
    Download,
    Upload,
    Finalize,
}

/// Where an upgrade stands. Every state but `idle` names its `source` and
/// its `target`, the release as `<date> <build>` or the file's name.
#[derive(Serialize, Clone, Debug, PartialEq)]
#[serde(tag = "state", rename_all = "lowercase")]
pub(crate) enum UpgradeState {
    /// No upgrade is asked of the device.
    Idle,
    /// It waits for the upgrades asked before it.
    Queued { source: SourceKind, target: String },
    /// The hub is downloading the release.
    Downloading { source: SourceKind, target: String },
    /// The hub is uploading the image, `sent` of its `size` bytes so far.
    Uploading {
        source: SourceKind,
        target: String,
        sent: u64,
        size: u64,
    },
    /// The device commits the image and writes it.
    Finalizing { source: SourceKind, target: String },
    /// The device restarts into the image, and the hub reads its `dev.desc`.
    Restarting { source: SourceKind, target: String },
    /// The upgrade is done, and the device runs `installed`, or did not say
    /// what it runs in time.
    Done {
        source: SourceKind,
        target: String,
        installed: Option<Installed>,
    },
    /// The upgrade failed at `step`, as `error` says.
    Failed {
        source: SourceKind,
        target: String,
        step: Step,
        error: String,
    },
}

/// What a page asked that the hub refused.
#[derive(Serialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Ask {
    Upgrade,
    Cancel,
    Retry,
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
/// it costs, and the seconds of each stream's time it holds, up to its
/// newest row, as the live history's `seconds` counts them. It gives up its
/// oldest rows first as the buffer leaves it less room, and those older
/// than the History setting before its newest row.
#[derive(Serialize, Clone, Copy, Debug, PartialEq)]
pub(crate) struct Held {
    pub(crate) bytes: u64,
    pub(crate) seconds: f64,
}

/// What a plot paused keeps once the live history holds all it may.
/// `steady` is the bytes of the History setting at the streams' rate, and
/// `need` those of the live history and a full paused one beside it. The
/// paused history costs the rows the live history drops since the pause, in
/// the room the live history leaves of the budget, so `keeps` is the seconds
/// from the pause it keeps the whole setting before it loses its oldest,
/// all of the setting when `need` fits the budget. `memory_binds` is
/// whether `steady` fills the budget, so memory is the limit that comes first.
#[derive(Serialize, Clone, Copy, Debug, PartialEq)]
pub(crate) struct Paused {
    pub(crate) steady: f64,
    pub(crate) need: f64,
    pub(crate) keeps: f64,
    pub(crate) memory_binds: bool,
}

/// The rows of one stream lost before the hub's history held them, by
/// where: `skipped` the device did not send, or its link lost while it
/// stayed up, `link` were lost while the device's link or the hub's
/// connection was down, `proxy` the proxy dropped from its queue for the
/// hub's connection, and `hub` the hub's connection dropped as the hub fell
/// behind.
#[derive(Serialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Lost {
    pub(crate) skipped: u64,
    pub(crate) link: u64,
    pub(crate) proxy: u64,
    pub(crate) hub: u64,
}

/// The rows one stream lost, and its `rate` in rows a second, which weighs
/// them in seconds.
#[derive(Serialize, Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct StreamLoss {
    pub(crate) rate: f64,
    #[serde(flatten)]
    pub(crate) lost: Lost,
}

/// Each place's count of `other` added to its own.
impl std::ops::AddAssign for Lost {
    fn add_assign(&mut self, other: Lost) {
        self.skipped += other.skipped;
        self.link += other.link;
        self.proxy += other.proxy;
        self.hub += other.hub;
    }
}

/// The counts of a stream's buffer, where the rows its connection dropped
/// are the hub's.
impl From<twinleaf::data::Lost> for Lost {
    fn from(lost: twinleaf::data::Lost) -> Lost {
        Lost {
            skipped: lost.skipped,
            link: lost.link,
            proxy: lost.proxy,
            hub: lost.dropped,
        }
    }
}

/// How often the link to a device dropped, and for how many seconds in all
/// it was down, up to now while it is.
#[derive(Serialize, Clone, Copy, Debug, Default, PartialEq)]
pub(crate) struct Link {
    pub(crate) times: u64,
    pub(crate) seconds: f64,
}

/// How grave a line of the log is. A device's critical line is an error.
#[derive(Serialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Level {
    Error,
    Warn,
    Info,
    Debug,
}

/// Who said a line of the log.
#[derive(Serialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Source {
    /// The device the line names, in a log packet.
    Device,
    /// The proxy, of what its hub sees.
    Proxy,
}

/// What a line of the log says was lost of its device, by `cause`.
#[derive(Serialize, Clone, Debug, PartialEq)]
#[serde(tag = "cause", rename_all = "lowercase")]
pub(crate) enum LossLine {
    /// The device's link dropped, with no `seconds` yet, or is back after
    /// `seconds` down. `why` is why the device was lost, on the line of the
    /// drop, and `url` the address of the link that serves it, each when
    /// the hub knows it.
    Link {
        seconds: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        why: Option<&'static str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        url: Option<String>,
    },
    /// The device skipped `samples` of `stream`, or its link lost them
    /// while it stayed up.
    Skipped { stream: String, samples: u64 },
    /// The proxy dropped `samples` of `stream` from its queue for the hub.
    Proxy { stream: String, samples: u64 },
    /// The hub's connection dropped `samples` of `stream`.
    Hub { stream: String, samples: u64 },
}

/// One capture RPC as the page lists it.
#[derive(Serialize, Clone, Debug, PartialEq)]
pub(crate) struct CaptureInfo {
    pub(crate) rpc: String,
    /// What its metadata says a capture of it is of, as the newest capture
    /// the hub read of it says, or as the device says before one is taken.
    /// `None` for a device that does not say before one is.
    pub(crate) label: Option<CaptureLabel>,
}

/// What a capture is of, as its metadata names it. `name` is what the
/// device calls the capture, `units` those of a calibrated element, and
/// `x_name` and `x_units` what its x measures.
#[derive(Serialize, Clone, Debug, PartialEq)]
pub(crate) struct CaptureLabel {
    pub(crate) name: String,
    pub(crate) units: String,
    pub(crate) x_name: String,
    pub(crate) x_units: String,
}

/// Where a capture RPC stands.
#[derive(Serialize, Clone, Debug, PartialEq)]
#[serde(tag = "state", rename_all = "lowercase")]
pub(crate) enum CaptureState {
    /// The hub is taking no capture of it and waits to take none, and the
    /// last it took did not fail.
    Idle,
    /// The hub triggered a capture, which the device has not finished.
    Capturing,
    /// The hub is reading a capture, `read` of its `size` bytes so far.
    /// Only a read longer than a tenth of a second says so.
    Reading { read: u32, size: u32 },
    /// The hub read a capture and waits to take the next.
    Waiting,
    /// The device refused a step of the capture as busy, the trigger or a
    /// read after it. A capture is in progress on the device.
    Busy,
    /// The device did not finish the capture, or did not answer, in time.
    Timeout,
    /// The capture failed, as `error` says, and `gone` says it ended as its
    /// device is lost.
    Error { error: String, gone: bool },
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
    /// Asks for one capture of the capture RPC `rpc` of the device at
    /// `route`, taken now. A capture the hub is already taking of it
    /// answers the ask.
    Capture { route: String, rpc: String },
    /// Asks for captures of `rpc` again and again, each triggered `delay`
    /// seconds after the last was read, from 0.05 to 10. Asked again, it
    /// changes the page's delay.
    Repeat {
        route: String,
        rpc: String,
        delay: f64,
    },
    /// Takes back what the page asked of `rpc`.
    Stop { route: String, rpc: String },
    /// Looks every device whose firmware the hub knows up in the catalog.
    Check,
    /// Asks for an upgrade of the device at `route` from `source`.
    Upgrade { route: String, source: ImageSource },
    /// Takes back an upgrade that waits or downloads, or clears one that
    /// is done or failed.
    Cancel { route: String },
    /// Runs a failed upgrade again from its source.
    Retry { route: String },
}

/// The image an upgrade asks for. `file` is followed by binary frames whose
/// lengths sum to `size`, the bytes of the file named `name`.
#[derive(Deserialize, Debug, PartialEq)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub(crate) enum ImageSource {
    Latest,
    Release { file: String },
    File { name: String, size: u64 },
}

/// A trace as the page names it.
#[derive(Serialize, Clone, Debug, PartialEq)]
pub(crate) struct TraceInfo {
    pub(crate) id: u16,
    /// The name a page keeps it by, and which saved layouts hold: the
    /// stream and column, as `sine.sine`, with the device's route ahead for
    /// any device but the root, as `0/sine.sine`.
    pub(crate) name: String,
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
    /// The class of value a person types for it: `int`, `float`, `bool`,
    /// `string`, `bytes` as hex, or `none`.
    pub(crate) class: &'static str,
    /// The least and greatest value of an integer, as text.
    pub(crate) min: Option<String>,
    pub(crate) max: Option<String>,
    /// The most bytes a string holds, or the bytes a byte array holds.
    pub(crate) length: Option<u8>,
    /// Whether a call of it runs it, with no value.
    pub(crate) action: bool,
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
    /// It answered with an error, or its reply did not decode.
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
    /// How it is reached, as `PortInterface::label` names it, or `other`.
    pub(crate) interface: &'static str,
    pub(crate) name: Option<String>,
    /// The subdevices routed behind it.
    pub(crate) routes: Vec<FoundRoute>,
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

/// An export of the raw rows of traces `ids`, a comma-separated list,
/// within [t0, t1], as `csv` or `h5`, in a file named `name`.
#[derive(Deserialize, Debug)]
pub(crate) struct ExportRequest {
    /// The hub the trace ids are of, as [`Message::Hub`] named it.
    pub(crate) hub: String,
    pub(crate) ids: String,
    pub(crate) t0: f64,
    pub(crate) t1: f64,
    pub(crate) format: String,
    pub(crate) name: String,
}

/// The reply to a [`RowsRequest`]: `count` rows, their times `x` and values
/// `y`, empty when there are more than the limit, and the oldest time the
/// history still holds of the trace.
#[derive(Serialize, Debug, PartialEq)]
pub(crate) struct Rows {
    pub(crate) count: usize,
    pub(crate) oldest: Option<f64>,
    /// The seconds at the start of the range the history no longer holds.
    pub(crate) missing: f64,
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

/// `GET /api/mcp` replies it, and `POST /api/mcp` sets it: whether the MCP
/// door at `/mcp` answers.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub(crate) struct McpSwitch {
    pub(crate) on: bool,
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

    /// The form of `message`, its type and an upgrade's source.
    fn form(message: &PageMessage) -> &'static str {
        match message {
            PageMessage::Spectra { .. } => "spectra",
            PageMessage::Hold { .. } => "hold",
            PageMessage::Capture { .. } => "capture",
            PageMessage::Repeat { .. } => "repeat",
            PageMessage::Stop { .. } => "stop",
            PageMessage::Check => "check",
            PageMessage::Upgrade { source, .. } => match source {
                ImageSource::Latest => "upgrade latest",
                ImageSource::Release { .. } => "upgrade release",
                ImageSource::File { .. } => "upgrade file",
            },
            PageMessage::Cancel { .. } => "cancel",
            PageMessage::Retry { .. } => "retry",
        }
    }

    /// The hub reads each message the page sends, as the page's tests
    /// record them in `ui/tests/fixtures/page`, and the files hold every
    /// form of [`PageMessage`].
    #[test]
    fn the_hub_reads_what_the_page_sends() {
        const FORMS: [&str; 11] = [
            "spectra",
            "hold",
            "capture",
            "repeat",
            "stop",
            "check",
            "upgrade latest",
            "upgrade release",
            "upgrade file",
            "cancel",
            "retry",
        ];
        let folder = concat!(env!("CARGO_MANIFEST_DIR"), "/ui/tests/fixtures/page");
        let mut sent = std::collections::BTreeSet::new();
        for entry in std::fs::read_dir(folder).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                continue;
            }
            let text = std::fs::read_to_string(&path).unwrap();
            let message: PageMessage = serde_json::from_str(&text)
                .unwrap_or_else(|error| panic!("the hub refuses {}: {error}", path.display()));
            sent.insert(form(&message));
        }
        let unsent: Vec<&str> = FORMS.into_iter().filter(|f| !sent.contains(f)).collect();
        assert!(unsent.is_empty(), "no file sends {unsent:?}");
    }

    #[test]
    fn a_refusal_names_the_ask_and_carries_no_number() {
        let refused = Message::Refused {
            ask: Ask::Cancel,
            route: "/".into(),
            error: "the upgrade of / is uploading and cannot be cancelled".into(),
        };
        assert_eq!(
            json_of(&refused),
            json!({"type": "refused", "ask": "cancel", "route": "/",
                "error": "the upgrade of / is uploading and cannot be cancelled"})
        );
        for (ask, name) in [(Ask::Upgrade, "upgrade"), (Ask::Retry, "retry")] {
            assert_eq!(serde_json::to_value(ask).unwrap(), name);
        }
    }
}
