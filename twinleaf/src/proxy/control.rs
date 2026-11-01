//! The control protocol, which carries a client's requests to the proxy
//! itself. A request travels in a `PROXY_TX` packet and its answer in a
//! `PROXY_RX` packet, its arguments and reply each in CBOR. [`Request`]
//! decodes a request, [`answer`] builds the reply, and [`call`] and
//! [`handshake`] make requests as a client.

use super::Layout;
use crate::data::PacketParser;
use crate::device::rpc::{CallError, RpcErrorPayload, RpcRegistry, RpcRegistryError};
use crate::device::store::{Store, METADATA, PRESENCE, REGISTRY};
use crate::device::{MetadataState, Presence, RegistryState, SettingChange, Settings};
use crate::proto::packet::Packet as WirePacket;
use crate::proto::rpc::RpcError;
use crate::proto::{DeviceRoute, SessionId, TCP_DEFAULT_PORT};
use crate::rt::Supervisor;
use crate::tio::packet::{LinkStatus, Packet, Payload, ProxyRx};
use crate::tio::transport::{self, KeepAlive, ReceiveResult, RecvError, TransportEvent};
use futures_util::StreamExt;
use minicbor::bytes::ByteVec;
use minicbor::{Decode, Encode};
use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{timeout_at, Instant};

/// The method by which a client and the proxy introduce themselves.
pub(crate) const HELLO: &str = "proxy.hello";
/// The method that binds a connection to a device and makes the device a
/// choice for `auto`.
pub(super) const OPEN: &str = "proxy.open";
/// The method that binds a connection to a device to look it over. The
/// device does not become a choice for `auto`.
pub(super) const PROBE: &str = "proxy.probe";
/// The method that binds a connection to a layout of mounts the client
/// names, each of which becomes a choice for `auto`.
pub(crate) const MOUNT: &str = "proxy.mount";
/// The method that makes a layout the default of a background proxy while
/// the connection that names it stays open.
pub(crate) const DEFAULT_LAYOUT: &str = "proxy.default";
/// The method that stops a background proxy.
pub(super) const STOP: &str = "proxy.stop";
/// The method that follows the state of one device a connection opened.
pub(crate) const WATCH: &str = "proxy.watch";
/// The method that stops a watch.
pub(crate) const UNWATCH: &str = "proxy.unwatch";

/// How a client keeps its link to the proxy alive once the hello is done.
pub(crate) const CLIENT_KEEPALIVE: KeepAlive = KeepAlive {
    beat: || Packet::heartbeat(DeviceRoute::root()),
    every: Duration::from_secs(1),
    quiet: Duration::from_secs(5),
};

/// How the proxy keeps its link to a client alive once the client is known
/// to speak the control protocol.
pub(super) const PROXY_KEEPALIVE: KeepAlive = KeepAlive {
    beat: Packet::alive,
    ..CLIENT_KEEPALIVE
};

/// What one side of `proxy.hello` says about itself. A client sends its own,
/// and the proxy replies with its own.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub(crate) struct Introduction {
    /// The process id.
    #[n(0)]
    pub(crate) pid: u32,
    /// The version of the `twinleaf` crate it was built with, as major,
    /// minor and patch.
    #[n(1)]
    pub(crate) version: [u16; 3],
    /// Its name, such as `tio monitor` or `tio proxy`.
    #[n(2)]
    pub(crate) name: String,
}

impl Introduction {
    /// Returns the introduction of this process under `name`.
    pub(crate) fn of_this_process(name: impl Into<String>) -> Introduction {
        let part = |text: &str| text.parse().unwrap_or(0);
        Introduction {
            pid: std::process::id(),
            version: [
                part(env!("CARGO_PKG_VERSION_MAJOR")),
                part(env!("CARGO_PKG_VERSION_MINOR")),
                part(env!("CARGO_PKG_VERSION_PATCH")),
            ],
            name: name.into(),
        }
    }

    /// Returns the `twinleaf` version, such as `2.0.0`.
    pub(crate) fn version_text(&self) -> String {
        let [major, minor, patch] = self.version;
        format!("{major}.{minor}.{patch}")
    }
}

/// Shows the name, the `twinleaf` version and the pid, such as
/// `tio proxy (twinleaf 2.0.0, pid 812)`.
impl fmt::Display for Introduction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let version = self.version_text();
        write!(
            formatter,
            "{} (twinleaf {version}, pid {})",
            self.name, self.pid
        )
    }
}

/// Encodes a control request's arguments or reply.
pub(crate) fn encode<T: Encode<()> + ?Sized>(value: &T) -> Vec<u8> {
    minicbor::to_vec(value).expect("encoding into a Vec cannot fail")
}

/// Decodes a control request's arguments or reply.
pub(crate) fn decode<'b, T: Decode<'b, ()>>(bytes: &'b [u8]) -> Result<T, minicbor::decode::Error> {
    minicbor::decode(bytes)
}

/// Returns `table` as its hash, its session and its entries.
fn entries(table: &RpcRegistry) -> WireEntries {
    let entries = table
        .iter()
        .map(|rpc| (rpc.full_name.clone(), rpc.meta.bits()));
    (
        table.hash,
        table.session.map(SessionId::value),
        entries.collect(),
    )
}

/// Encodes a [`DeviceRoute`] as the byte string of its hops, for
/// `#[cbor(with = "...")]`.
pub(super) mod hops {
    use crate::proto::DeviceRoute;
    use minicbor::{Decoder, Encoder};

    pub(crate) fn encode<C, W: minicbor::encode::Write>(
        route: &DeviceRoute,
        encoder: &mut Encoder<W>,
        _: &mut C,
    ) -> Result<(), minicbor::encode::Error<W::Error>> {
        encoder.bytes(route.as_slice())?;
        Ok(())
    }

    pub(crate) fn decode<C>(
        decoder: &mut Decoder<'_>,
        _: &mut C,
    ) -> Result<DeviceRoute, minicbor::decode::Error> {
        DeviceRoute::from_hops(decoder.bytes()?)
            .map_err(|error| minicbor::decode::Error::message(error.to_string()))
    }
}

/// What `proxy.watch` follows, the device at `route` as the client sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub(crate) struct Watch {
    #[n(0)]
    #[cbor(with = "hops")]
    pub(crate) route: DeviceRoute,
    /// Whether the watch follows the RPC table and the settings, rather
    /// than the presence, the metadata and the announced settings of the
    /// device and the routes heard below it.
    #[n(1)]
    pub(crate) table: bool,
}

/// The code of a table error that says a call timed out, that the table
/// changed during every walk, or that a call got no reply. Any other code
/// is the device's refusal.
const TIMED_OUT: u16 = 0xfffd;
const CHANGED: u16 = 0xfffe;
const UNANSWERED: u16 = 0xffff;

/// An RPC table as its hash, the session it was read in, and the name and
/// metadata word of each entry.
type WireEntries = (Option<u32>, Option<u32>, Vec<(String, u16)>);

/// The RPC table, or why it did not load as an error code and message.
type WireTable = Result<WireEntries, (u16, ByteVec)>;

/// One value of a watched device's state, as a reply to `proxy.watch`
/// carries it in [`Part`]s.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub(crate) enum Update {
    /// The device's presence. `Ok` holds the session its heartbeats report
    /// while it sends. `Err(None)` says it is connecting, and `Err(Some)`
    /// says its link lost it this many milliseconds before the update went
    /// out, as the two ends share no clock.
    #[n(0)]
    Presence(#[n(0)] Result<Option<u32>, Option<u64>>),
    /// The device's complete metadata as its own metadata packets, each
    /// addressed to the root, none while it is collected, or `None` for a
    /// device with no `dev.metadata`.
    #[n(1)]
    Metadata(#[n(0)] Option<Vec<ByteVec>>),
    /// The RPC table, or why it did not load.
    #[n(2)]
    Table(#[n(0)] WireTable),
    /// The values of the announced settings, as the hash of the table they
    /// were read with, their version, and each value by name.
    #[n(3)]
    Settings(
        #[n(0)] Option<u32>,
        #[n(1)] Option<u32>,
        #[n(2)] BTreeMap<String, ByteVec>,
    ),
    /// One setting the device announced, as its name, its
    /// `settings.version` once counted and its value. It was recorded under
    /// the table the last [`Recorded`](Self::Recorded) update named.
    #[n(4)]
    Setting(#[n(0)] String, #[n(1)] Option<u32>, #[n(2)] ByteVec),
    /// Every route heard at or below the watched one, each as its hops.
    #[n(5)]
    Routes(#[n(0)] Vec<ByteVec>),
    /// The watch missed this many announced settings, as the client fell
    /// behind.
    #[n(6)]
    Lagged(#[n(0)] u64),
    /// The RPC table the announced settings after it were recorded under,
    /// up to the next such update, or `None` for settings recorded while no
    /// table was in force. One goes out ahead of the first setting of a
    /// watch, and ahead of each setting recorded under another table than
    /// the setting before.
    #[n(7)]
    Recorded(#[n(0)] Option<WireEntries>),
}

impl Update {
    /// Returns the update of a device's presence.
    pub(crate) fn presence(presence: Presence) -> Update {
        Update::Presence(match presence {
            Presence::Connecting => Err(None),
            Presence::Alive { session } => Ok(session.map(SessionId::value)),
            Presence::Lost { since } => {
                let ago = Instant::now().into_std().saturating_duration_since(since);
                Err(Some(u64::try_from(ago.as_millis()).unwrap_or(u64::MAX)))
            }
        })
    }

    /// Returns the update of a device's metadata, or `None` if a record
    /// does not fit a packet.
    pub(crate) fn metadata(state: &MetadataState) -> Option<Update> {
        let packets = match state {
            MetadataState::Ready(snapshot) => snapshot.metadata_packets().ok()?,
            MetadataState::Pending => Vec::new(),
            MetadataState::Unsupported => return Some(Update::Metadata(None)),
        };
        let root = DeviceRoute::root();
        let records = packets
            .iter()
            .map(|packet| packet.with_route(root).as_bytes().to_vec());
        Some(Update::Metadata(Some(records.map(ByteVec::from).collect())))
    }

    /// Returns the update of an RPC table as loaded, or of why it did not
    /// load.
    pub(crate) fn table(registry: &RegistryState) -> Update {
        let none = || ByteVec::from(Vec::new());
        Update::Table(match registry {
            Ok(table) => Ok(entries(table)),
            Err(RpcRegistryError::DeviceRpcError(CallError::DeviceError(refusal))) => {
                Err((refusal.error.value(), refusal.extra.clone().into()))
            }
            Err(RpcRegistryError::DeviceRpcError(CallError::Timeout)) => Err((TIMED_OUT, none())),
            Err(RpcRegistryError::TableChanged) => Err((CHANGED, none())),
            Err(RpcRegistryError::DeviceRpcError(_)) => Err((UNANSWERED, none())),
        })
    }

    /// Returns the update of the values of a device's announced settings.
    pub(crate) fn settings(settings: &Settings) -> Update {
        let values = settings.iter();
        let values = values.map(|(name, value)| (name.to_string(), value.to_vec().into()));
        Update::Settings(
            settings.rpc_registry().hash,
            settings.version(),
            values.collect(),
        )
    }

    /// Returns the update of one announced setting.
    pub(crate) fn setting(change: SettingChange) -> Update {
        Update::Setting(change.name, change.seq, change.reply.into())
    }

    /// Returns the update that names `table` as the one the announced
    /// settings after it were recorded under.
    pub(crate) fn recorded(table: Option<&Arc<RpcRegistry>>) -> Update {
        Update::Recorded(table.map(|table| entries(table)))
    }

    /// Keeps what the update says of the device at `route` in `store`. Its
    /// metadata records reach `parser` as the device's own, and the
    /// metadata the parser then holds is the value kept, and no metadata
    /// resets any route but `route`. Settings are kept only when they were
    /// read with the table the store holds, and a table with the hash and
    /// session of the one held keeps them. A table with the hash and session
    /// of one the store holds already is that one, so the device's table and
    /// the table its changes carry are one value while they are the same.
    pub(crate) fn apply(self, route: DeviceRoute, store: &mut Store, parser: &mut PacketParser) {
        match self {
            Update::Presence(presence) => {
                let now = Instant::now().into_std();
                let presence = match presence.map(|session| session.map(SessionId::new)) {
                    Ok(session) => Presence::Alive { session },
                    Err(None) => Presence::Connecting,
                    Err(Some(ago)) => {
                        let since = now.checked_sub(Duration::from_millis(ago));
                        Presence::Lost {
                            since: since.unwrap_or(now),
                        }
                    }
                };
                store.set(route, PRESENCE, presence);
            }
            Update::Metadata(None) => store.set(route, METADATA, MetadataState::Unsupported),
            Update::Metadata(Some(records)) if records.is_empty() => {
                parser.relearn_route(route);
                store.set(route, METADATA, MetadataState::Pending);
            }
            Update::Metadata(Some(records)) => {
                for record in records {
                    let applied = Packet::from_slice_prefix(&record)
                        .map_err(|error| error.to_string())
                        .and_then(|(packet, _)| {
                            let packet = packet.with_route(route);
                            parser
                                .push_packet(&packet)
                                .map_err(|error| error.to_string())
                        });
                    if let Err(error) = applied {
                        log::warn!("dropping a metadata record of {route}: {error}");
                    }
                }
                if let Some(snapshot) = parser.metadata(route) {
                    store.set(route, METADATA, MetadataState::Ready(snapshot));
                }
            }
            Update::Table(Ok((hash, session, entries))) => {
                let session = session.map(SessionId::new);
                // The same table again keeps the settings read with it.
                let held = store.value(route, REGISTRY);
                if !held.is_some_and(|held| {
                    held.is_ok_and(|held| (held.hash, held.session) == (hash, session))
                }) {
                    let table = store.table_of(route, hash, session).unwrap_or_else(|| {
                        Arc::new(RpcRegistry::from_entries(entries, hash, session))
                    });
                    store.set_registry(route, Ok(table));
                }
            }
            Update::Table(Err((CHANGED, _))) => {
                store.set_registry(route, Err(RpcRegistryError::TableChanged))
            }
            Update::Table(Err((code, message))) => {
                let error = match code {
                    TIMED_OUT => CallError::Timeout,
                    UNANSWERED => CallError::ResponseLost,
                    code => CallError::DeviceError(RpcErrorPayload {
                        error: RpcError::from(code),
                        extra: message.into(),
                    }),
                };
                store.set_registry(route, Err(RpcRegistryError::DeviceRpcError(error)));
            }
            Update::Settings(hash, version, values) => match store.value(route, REGISTRY) {
                Some(Ok(registry)) if registry.hash == hash => {
                    let values = values.into_iter().map(|(name, value)| (name, value.into()));
                    let settings = Settings::new(registry, version, values.collect());
                    store.set_settings(route, Arc::new(settings));
                }
                _ => log::warn!("dropping settings of {route} read with a table it does not hold"),
            },
            Update::Setting(name, seq, reply) => {
                store.record_change(route, name, seq, reply.into());
            }
            Update::Recorded(table) => {
                let table = table.map(|(hash, session, entries)| {
                    RpcRegistry::from_entries(entries, hash, session.map(SessionId::new))
                });
                store.name_table(route, table);
            }
            Update::Routes(routes) => {
                let routes = routes
                    .iter()
                    .filter_map(|hops| DeviceRoute::from_hops(hops).ok());
                store.list(route, routes);
            }
            Update::Lagged(missed) => store.lag(route, missed),
        }
    }
}

/// One part of an encoded [`Update`], as a reply to `proxy.watch` carries
/// it. The parts of one update go out in order, numbered from zero.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub(crate) struct Part {
    #[n(0)]
    pub(crate) index: u16,
    #[n(1)]
    pub(crate) count: u16,
    #[n(2)]
    pub(crate) bytes: ByteVec,
}

/// The most bytes of an update one part carries. The rest of the reply
/// fits in what a packet leaves.
const PART_BYTES: usize = WirePacket::MAX_PAYLOAD - 32;

/// Returns the replies to watch `id` that carry `update`, one per part.
pub(crate) fn parts(id: u16, update: &Update) -> Vec<Packet> {
    let encoded = encode(update);
    let chunks = encoded.chunks(PART_BYTES);
    let count = u16::try_from(chunks.len()).unwrap_or(u16::MAX);
    chunks
        .zip(0..count)
        .map(|(bytes, index)| {
            let part = Part {
                index,
                count,
                bytes: bytes.to_vec().into(),
            };
            Packet::proxy_reply(id, &encode(&part)).expect("a part fits a reply")
        })
        .collect()
}

/// What has arrived of the next update of one watch.
#[derive(Default)]
pub(crate) struct Joined {
    bytes: Vec<u8>,
    /// The index of the part due next, or `None` until the first part of
    /// the next update.
    next: Option<u16>,
}

impl Joined {
    /// Takes the reply `value`, a part of an update. Returns the update once
    /// its last part is in and it decodes. An update starts at its first
    /// part, so the tail of one that lost a part is dropped.
    pub(crate) fn join(&mut self, value: &[u8]) -> Option<Update> {
        let part: Part = decode(value).ok()?;
        if part.index == 0 {
            self.bytes.clear();
            self.next = Some(0);
        }
        if self.next != Some(part.index) {
            self.next = None;
            return None;
        }
        self.bytes.extend_from_slice(&part.bytes);
        self.next = part.index.checked_add(1).filter(|next| *next < part.count);
        if self.next.is_some() {
            return None;
        }
        let decoded = decode(&std::mem::take(&mut self.bytes));
        decoded
            .map_err(|error| log::warn!("dropping a state update that does not decode: {error}"))
            .ok()
    }
}

/// Why a client binds a connection to a device. It names the control method
/// the client calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Purpose {
    /// Using the device, with `proxy.open`. The device becomes a choice for
    /// `auto`.
    Use,
    /// Looking the device over with `proxy.probe`, as discovery does to read
    /// its name. The device does not become a choice for `auto`.
    Probe,
}

impl Purpose {
    /// Returns the control method that binds a connection for this purpose.
    pub(crate) fn method(self) -> &'static str {
        match self {
            Purpose::Use => OPEN,
            Purpose::Probe => PROBE,
        }
    }
}

/// The id a [`handshake`] tags its hello with.
const HELLO_ID: u16 = 1;
/// The id a [`handshake`] tags its request with.
const REQUEST_ID: u16 = 2;
/// How long a proxy may take to accept a connection and answer anything at
/// all. A peer that is silent this long is a proxy from an older build, or
/// no proxy.
pub(super) const HELLO_TIMEOUT: Duration = Duration::from_secs(2);

/// The longest refusal message that fits a `PROXY_RX` error.
const MAX_MESSAGE: usize = WirePacket::MAX_PAYLOAD - 5;

/// A control request decoded from its method name and arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Request {
    /// `proxy.hello`, introducing the client.
    Hello(Introduction),
    /// `proxy.open`, naming what to open.
    Open {
        /// `auto`, `default`, or a device URL.
        target: String,
    },
    /// `proxy.probe`, naming the device to look over.
    Probe {
        /// The device's URL.
        url: String,
    },
    /// `proxy.mount`, naming the layout to open.
    Mount(Layout),
    /// `proxy.default`, naming the layout every client that names none opens.
    Default(Layout),
    /// `proxy.stop`.
    Stop,
    /// `proxy.watch`, naming the device to follow.
    Watch(Watch),
    /// `proxy.unwatch`, naming the id of the watch to stop.
    Unwatch(u16),
    /// `proxy.test.panic`, which makes the task serving the client panic.
    #[cfg(test)]
    Panic,
}

impl Request {
    /// Decodes a request for the method `name` from its encoded `args`.
    ///
    /// Returns a [`Refusal`] if the proxy has no method `name` or `args` do
    /// not decode.
    pub(super) fn decode(name: &[u8], args: &[u8]) -> Result<Request, Refusal> {
        let malformed =
            |error: minicbor::decode::Error| Refusal::new(RpcError::ArgsSize, error.to_string());
        match name {
            b"proxy.hello" => Ok(Request::Hello(decode(args).map_err(malformed)?)),
            b"proxy.open" => Ok(Request::Open {
                target: decode(args).map_err(malformed)?,
            }),
            b"proxy.probe" => Ok(Request::Probe {
                url: decode(args).map_err(malformed)?,
            }),
            b"proxy.mount" => Ok(Request::Mount(checked(decode(args).map_err(malformed)?)?)),
            b"proxy.default" => Ok(Request::Default(checked(decode(args).map_err(malformed)?)?)),
            b"proxy.stop" => Ok(Request::Stop),
            b"proxy.watch" => Ok(Request::Watch(decode(args).map_err(malformed)?)),
            b"proxy.unwatch" => Ok(Request::Unwatch(decode(args).map_err(malformed)?)),
            #[cfg(test)]
            b"proxy.test.panic" => Ok(Request::Panic),
            other => Err(Refusal::new(
                RpcError::NotFound,
                format!("the proxy has no method {}", String::from_utf8_lossy(other)),
            )),
        }
    }

    /// Returns the method name and encoded arguments, as a client sends them.
    pub(super) fn encode(&self) -> (&'static str, Vec<u8>) {
        match self {
            Request::Hello(introduction) => (HELLO, encode(introduction)),
            Request::Open { target } => (OPEN, encode(target)),
            Request::Probe { url } => (PROBE, encode(url)),
            Request::Mount(layout) => (MOUNT, encode(layout)),
            Request::Default(layout) => (DEFAULT_LAYOUT, encode(layout)),
            Request::Stop => (STOP, Vec::new()),
            Request::Watch(watch) => (WATCH, encode(watch)),
            Request::Unwatch(id) => (UNWATCH, encode(id)),
            #[cfg(test)]
            Request::Panic => ("proxy.test.panic", Vec::new()),
        }
    }
}

/// Returns `layout` if it mounts something and no prefix lies within
/// another, as a packet's route would then fall under two mounts, or else a
/// [`Refusal`] saying why.
fn checked(layout: Layout) -> Result<Layout, Refusal> {
    let mounts = &layout.mounts;
    if mounts.is_empty() {
        return Err(Refusal::new(RpcError::Invalid, "the layout mounts nothing"));
    }
    let overlap = mounts
        .iter()
        .enumerate()
        .flat_map(|(i, a)| mounts[i + 1..].iter().map(move |b| (a, b)))
        .find(|(a, b)| a.prefix.starts_with(&b.prefix) || b.prefix.starts_with(&a.prefix));
    if let Some((a, b)) = overlap {
        let message = format!("the mounts at {} and {} overlap", a.prefix, b.prefix);
        return Err(Refusal::new(RpcError::Invalid, message));
    }
    Ok(layout)
}

/// A control request refused, with a code and a message saying why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Refusal {
    /// The error code.
    pub(super) code: RpcError,
    /// Why, in words.
    pub(super) message: String,
}

impl Refusal {
    /// Creates a refusal with `code` and `message`.
    pub(super) fn new(code: RpcError, message: impl Into<String>) -> Refusal {
        Refusal {
            code,
            message: message.into(),
        }
    }
}

/// Returns the `PROXY_RX` packet answering request `id` with `outcome`. A
/// refusal's message too long for a packet is cut to fit.
pub(super) fn answer(id: u16, outcome: Result<Vec<u8>, Refusal>) -> Packet {
    let refusal = match outcome {
        Ok(value) => match Packet::proxy_reply(id, &value) {
            Ok(reply) => return reply,
            Err(error) => Refusal::new(RpcError::Internal, error.to_string()),
        },
        Err(refusal) => refusal,
    };
    let mut end = refusal.message.len().min(MAX_MESSAGE);
    while !refusal.message.is_char_boundary(end) {
        end -= 1;
    }
    Packet::proxy_error(id, refusal.code, &refusal.message[..end])
        .expect("a message cut to fit always fits")
}

/// Makes one control request of the proxy at `endpoint`, such as
/// `tcp://127.0.0.1:7855`, and returns the reply's bytes. Connects, then
/// makes the request as [`handshake`] does, waiting at most `timeout` for
/// the reply. A call of `proxy.hello` returns the proxy's [`Introduction`].
/// Blocks until the reply arrives or the call fails.
///
/// Returns an error saying no proxy is running at `endpoint` if the
/// connection is refused or times out. Also returns any other connection
/// error, and the errors of [`handshake`].
pub(crate) fn call(
    endpoint: &str,
    method: &str,
    args: &[u8],
    timeout: Duration,
) -> io::Result<Vec<u8>> {
    let address = socket_addr(endpoint)?;
    let (endpoint, method, args) = (endpoint.to_string(), method.to_string(), args.to_vec());
    let tasks = Supervisor::new();
    let on = tasks.clone();
    tasks.run("proxy control call", async move {
        call_at(address, &endpoint, &method, &args, timeout, &on).await
    })
}

/// Makes one control request of the proxy at `endpoint`, whose address is
/// `address`, as [`call`] does, without blocking. The connection's task runs
/// on `tasks`.
pub(crate) async fn call_at(
    address: SocketAddr,
    endpoint: &str,
    method: &str,
    args: &[u8],
    timeout: Duration,
    tasks: &Supervisor,
) -> io::Result<Vec<u8>> {
    let connect = timeout.min(HELLO_TIMEOUT);
    let connected = match tokio::time::timeout(connect, transport::connect_socket(address)).await {
        Ok(connected) => connected,
        Err(_elapsed) => Err(io::ErrorKind::TimedOut.into()),
    };
    let stream = connected.map_err(|error| match error.kind() {
        // A firewall can drop a connection to a closed port rather than
        // refuse it.
        io::ErrorKind::ConnectionRefused | io::ErrorKind::TimedOut => {
            io::Error::new(error.kind(), format!("no proxy is running at {endpoint}"))
        }
        _ => error,
    })?;
    let (sender, received) = crate::chan::bounded(transport::DEFAULT_RX_CHANNEL_SIZE);
    let port =
        transport::Port::over_tcp(stream, sender, transport::DEFAULT_TX_CHANNEL_SIZE, tasks)?;
    let goal = format!("answer {method}");
    let (_, reply) = handshake(&port, &received, endpoint, method, args, &goal, timeout).await?;
    Ok(reply)
}

/// Introduces this process to the proxy at `endpoint` with `proxy.hello`,
/// then makes the control request `method` with `args`. Sends on `port` and
/// reads its results from `received`. Waits at most `timeout` for the reply.
/// Returns the proxy's [`Introduction`], if it gave one before the reply, and
/// the reply's bytes.
///
/// Returns the [`older_build`] error if the peer sends no control reply
/// within [`HELLO_TIMEOUT`], as a proxy from an older build does, or answers
/// the hello with no introduction. Returns an error with the proxy's message
/// if the proxy refuses the hello or the request. Returns an error saying it
/// could not do `goal`, such as `open auto`, if the proxy stops, closes the
/// connection, or does not answer in time.
pub(crate) async fn handshake(
    port: &transport::Port,
    received: &crate::chan::Receiver<ReceiveResult>,
    endpoint: &str,
    method: &str,
    args: &[u8],
    goal: &str,
    timeout: Duration,
) -> io::Result<(Option<Introduction>, Vec<u8>)> {
    let (name, hello) = Request::Hello(Introduction::of_this_process(client_name())).encode();
    for request in [
        Packet::proxy_request(name, &hello, HELLO_ID),
        Packet::proxy_request(method, args, REQUEST_ID),
    ] {
        let request = request.map_err(invalid_input)?;
        port.send_async(request).await.map_err(io::Error::other)?;
    }
    // The proxy hangs up on a client that is quiet too long, and an open may
    // wait longer than that for its device. A peer that is no proxy of this
    // build hears a heartbeat or two.
    port.keep_alive(CLIENT_KEEPALIVE).await;
    let started = Instant::now();
    let deadline = started + timeout;
    let hello_deadline = deadline.min(started + HELLO_TIMEOUT);
    // Whether the peer has sent any PROXY_RX, which only a proxy of this
    // build does.
    let mut answered = false;
    let mut host = None;
    let mut incoming = received.stream();
    loop {
        let until = if answered { deadline } else { hello_deadline };
        let timed_out = || {
            if answered {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("the proxy at {endpoint} did not {goal} within {timeout:?}"),
                )
            } else {
                older_build(endpoint)
            }
        };
        if Instant::now() >= until {
            return Err(timed_out());
        }
        let event = timeout_at(until, incoming.next())
            .await
            .map_err(|_| timed_out())?
            .unwrap_or(Err(RecvError::Disconnected))
            .map_err(|error| io::Error::new(
                io::ErrorKind::ConnectionAborted,
                format!("the proxy at {endpoint} closed the connection before it could {goal}: {error}"),
            ))?;
        let packet = match event {
            TransportEvent::Packet(packet) => packet,
            TransportEvent::Text(text) => {
                log::debug!("the proxy sent text: {text}");
                continue;
            }
            TransportEvent::RejectedFrame(error) => {
                log::debug!("the proxy sent a rejected frame: {error}");
                continue;
            }
        };
        let Payload::ProxyRx(rx) = packet.payload() else {
            continue;
        };
        answered = true;
        match rx {
            ProxyRx::Reply(reply) if reply.req_id.value() == HELLO_ID => {
                let introduction: Introduction =
                    decode(reply.value).map_err(|_| older_build(endpoint))?;
                log::debug!("the proxy at {endpoint} is {introduction}");
                host = Some(introduction);
            }
            ProxyRx::Reply(reply) if reply.req_id.value() == REQUEST_ID => {
                return Ok((host, reply.value.to_vec()))
            }
            ProxyRx::Error(error) if matches!(error.req_id.value(), HELLO_ID | REQUEST_ID) => {
                return Err(io::Error::other(
                    String::from_utf8_lossy(error.message).into_owned(),
                ))
            }
            ProxyRx::Link(LinkStatus::Stopped) if packet.route().is_empty() => {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    format!("the proxy at {endpoint} stopped before it could {goal}"),
                ))
            }
            _ => {}
        }
    }
}

/// Returns the error for a peer at `endpoint` that does not answer
/// `proxy.hello` with an introduction, as a proxy from an older build of
/// twinleaf does not. Such a proxy forwards the hello to its device as a
/// packet of a type it does not know, and sends no control reply.
pub(super) fn older_build(endpoint: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "{endpoint} is a proxy from an older build of twinleaf, or no proxy; \
             update tio, or stop it"
        ),
    )
}

/// Wraps `error` in an [`io::ErrorKind::InvalidInput`] error.
fn invalid_input(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error)
}

/// Returns the socket address of a `tcp://` endpoint, on the default port if
/// it names none. Returns an error if the endpoint does not resolve.
fn socket_addr(endpoint: &str) -> io::Result<SocketAddr> {
    let address = endpoint.strip_prefix("tcp://").unwrap_or(endpoint);
    let mut addresses = match address.to_socket_addrs() {
        Ok(addresses) => addresses,
        Err(_) => (address, TCP_DEFAULT_PORT).to_socket_addrs()?,
    };
    addresses.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{endpoint} names no address"),
        )
    })
}

/// Returns the name of this process's executable without its extension, or
/// `twinleaf` if it cannot be found.
pub(crate) fn executable_name() -> String {
    std::env::current_exe()
        .ok()
        .and_then(|exe| Some(exe.file_stem()?.to_string_lossy().into_owned()))
        .unwrap_or_else(|| "twinleaf".to_string())
}

/// Returns the name this process gives in `proxy.hello`. It is the
/// executable's name, followed for `tio` by its subcommand, such as
/// `tio monitor`.
pub(crate) fn client_name() -> String {
    let executable = executable_name();
    if executable != "tio" {
        return executable;
    }
    match std::env::args().nth(1).filter(|arg| !arg.starts_with('-')) {
        Some(subcommand) => format!("{executable} {subcommand}"),
        None => executable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy::Mount;

    /// Events skipped during the handshake do not restart its absolute deadline.
    #[tokio::test(start_paused = true)]
    async fn handshake_noise_does_not_extend_the_deadline() {
        let (send, received) = crate::chan::unbounded();
        let (port, _peer) =
            transport::Port::fake(transport::TransportKind::Tcp, None, send.clone());
        let started = Instant::now();
        let within = Duration::from_millis(10);
        let greeting = handshake(
            &port,
            &received,
            "tcp://127.0.0.1:7855",
            OPEN,
            &[],
            "open the device",
            within,
        );
        let noise = async move {
            loop {
                tokio::time::sleep(Duration::from_millis(2)).await;
                send.send(Ok(TransportEvent::Text("boot".into()))).unwrap();
                send.send(Ok(TransportEvent::RejectedFrame(
                    crate::tio::packet::DecodeError::CRC32,
                )))
                .unwrap();
            }
        };
        tokio::select! {
            outcome = greeting => assert!(outcome.is_err()),
            () = noise => unreachable!(),
        }
        assert_eq!(started.elapsed(), within);
    }

    /// A handshake skips recoverable events and accepts the following reply
    /// through the channel's ordinary stream adapter.
    #[tokio::test]
    async fn handshake_accepts_a_reply_after_rejected_frames() {
        let (send, received) = crate::chan::unbounded();
        let (port, _peer) =
            transport::Port::fake(transport::TransportKind::Tcp, None, send.clone());
        send.send(Ok(TransportEvent::Text("boot".into()))).unwrap();
        send.send(Ok(TransportEvent::RejectedFrame(
            crate::tio::packet::DecodeError::CRC32,
        )))
        .unwrap();
        send.send(Ok(TransportEvent::Packet(
            Packet::proxy_reply(REQUEST_ID, b"opened").unwrap(),
        )))
        .unwrap();
        let (_, reply) = handshake(
            &port,
            &received,
            "tcp://127.0.0.1:7855",
            OPEN,
            &[],
            "open the device",
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(reply, b"opened");
    }

    /// A request as the proxy decodes it from the packet a client sends.
    fn sent(request: &Request) -> Result<Request, Refusal> {
        let (name, args) = request.encode();
        let packet = Packet::proxy_request(name, &args, 9).unwrap();
        let packet = Packet::from_wire(packet.into_bytes()).unwrap();
        let Payload::ProxyTx(wire) = packet.payload() else {
            panic!("expected a PROXY_TX payload, got {:?}", packet.payload());
        };
        let crate::proto::rpc::Method::ByName(name) = wire.method else {
            panic!("a control request names its method");
        };
        Request::decode(name, wire.args)
    }

    #[test]
    fn every_request_decodes_as_it_was_sent() {
        for request in [
            Request::Hello(Introduction::of_this_process("tio monitor")),
            Request::Hello(Introduction {
                pid: u32::MAX,
                version: [u16::MAX; 3],
                name: String::new(),
            }),
            Request::Open {
                target: "serial:///dev/ttyACM0".to_string(),
            },
            Request::Probe {
                url: "tcp://192.0.2.1".to_string(),
            },
            Request::Mount(Layout {
                mounts: vec![
                    mount("serial:///dev/ttyACM0", "/", "/1"),
                    mount("tcp://192.0.2.1", "/2", "/3/4"),
                ],
            }),
            Request::Default(Layout {
                mounts: vec![mount("tcp://192.0.2.1", "/", "/")],
            }),
            Request::Stop,
        ] {
            assert_eq!(sent(&request), Ok(request));
        }
    }

    /// An introduction is a CBOR array of the pid, the version as an array
    /// of three, and the name.
    #[test]
    fn a_hello_matches_the_wire_layout() {
        let (name, args) = Request::Hello(Introduction {
            pid: 0x0102_0304,
            version: [2, 0x0304, 5],
            name: "ab".to_string(),
        })
        .encode();

        assert_eq!(name, "proxy.hello");
        let pid = [0x1a, 1, 2, 3, 4];
        let version = [0x83, 2, 0x19, 3, 4, 5];
        let expected = [&[0x83][..], &pid, &version, &[0x62, b'a', b'b']].concat();
        assert_eq!(args, expected);
    }

    /// A peer that sends no control reply to the hello, as a proxy from an
    /// older build does, is reported as one once [`HELLO_TIMEOUT`] passes,
    /// though the call may wait longer.
    #[tokio::test(start_paused = true)]
    async fn a_hello_with_no_control_reply_is_a_proxy_from_an_older_build() {
        let (send, received) = crate::chan::unbounded();
        let (port, _peer) =
            transport::Port::fake(transport::TransportKind::Tcp, None, send.clone());
        let started = Instant::now();

        let error = handshake(
            &port,
            &received,
            "tcp://127.0.0.1:7855",
            OPEN,
            &[],
            "open the device",
            Duration::from_secs(15),
        )
        .await
        .unwrap_err();
        assert_eq!(started.elapsed(), HELLO_TIMEOUT);
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            error.to_string(),
            "tcp://127.0.0.1:7855 is a proxy from an older build of twinleaf, or no proxy; \
             update tio, or stop it"
        );
    }

    /// Returns the mount of `source` of the device at `url`, seen at `prefix`.
    fn mount(url: &str, source: &str, prefix: &str) -> Mount {
        Mount {
            url: url.to_string(),
            source: source.parse().unwrap(),
            prefix: prefix.parse().unwrap(),
        }
    }

    /// A layout is a CBOR array holding the array of its mounts, and a mount
    /// the array of its URL and the hops of its source and prefix as bytes.
    #[test]
    fn a_layout_matches_the_wire_layout() {
        let layout = Layout {
            mounts: vec![mount("ab", "/", "/7")],
        };

        let expected = [0x81, 0x81, 0x83, 0x62, b'a', b'b', 0x40, 0x41, 7];
        assert_eq!(encode(&layout), expected);
    }

    /// A layout that mounts nothing, or two prefixes of which one lies within
    /// the other, is refused, as is one cut short.
    #[test]
    fn a_layout_whose_mounts_are_missing_or_overlap_is_refused() {
        let decode_layout = |args: &[u8]| Request::decode(b"proxy.mount", args);
        let refused = |mounts: Vec<Mount>| decode_layout(&encode(&Layout { mounts })).unwrap_err();

        assert_eq!(refused(Vec::new()).code, RpcError::Invalid);
        let overlapping = vec![mount("a", "/", "/1"), mount("b", "/", "/1/2")];
        assert_eq!(
            refused(overlapping).message,
            "the mounts at /1 and /1/2 overlap"
        );
        let at_the_root = vec![mount("a", "/", "/"), mount("b", "/", "/2")];
        assert_eq!(refused(at_the_root).code, RpcError::Invalid);
        let whole = Layout {
            mounts: vec![mount("a", "/", "/1")],
        };
        let cut = &encode(&whole)[..3];
        assert_eq!(decode_layout(cut).unwrap_err().code, RpcError::ArgsSize);
    }

    #[test]
    fn replies_decode_as_they_were_encoded() {
        let host = Introduction::of_this_process("tio proxy");
        assert_eq!(decode::<Introduction>(&encode(&host)).unwrap(), host);
        let target = "tcp://127.0.0.1:7856".to_string();
        assert_eq!(decode::<String>(&encode(&target)).unwrap(), target);
    }

    /// This build introduces itself with its own pid and crate version.
    #[test]
    fn this_process_gives_its_pid_and_version() {
        let introduction = Introduction::of_this_process("tio monitor");

        assert_eq!(introduction.pid, std::process::id());
        assert_eq!(introduction.version_text(), env!("CARGO_PKG_VERSION"));
        assert_eq!(
            introduction.to_string(),
            format!(
                "tio monitor (twinleaf {}, pid {})",
                env!("CARGO_PKG_VERSION"),
                std::process::id()
            )
        );
    }

    #[test]
    fn a_hello_too_short_for_its_introduction_is_refused() {
        let refusal = Request::decode(b"proxy.hello", &[1, 2, 3, 4, 1, 2]).unwrap_err();

        assert_eq!(refusal.code, RpcError::ArgsSize);
    }

    #[test]
    fn an_unknown_method_is_not_found() {
        let refusal = Request::decode(b"proxy.reboot", &[]).unwrap_err();

        assert_eq!(refusal.code, RpcError::NotFound);
        assert!(refusal.message.contains("proxy.reboot"));
    }

    /// A refusal always reaches the client, cut to fit when it is too long.
    #[test]
    fn a_long_refusal_is_cut_to_fit() {
        let message = "é".repeat(400);
        let packet = answer(3, Err(Refusal::new(RpcError::Invalid, message)));

        let Payload::ProxyRx(ProxyRx::Error(error)) = packet.payload() else {
            panic!("expected a PROXY_RX error, got {:?}", packet.payload());
        };
        assert_eq!(error.req_id.value(), 3);
        assert_eq!(RpcError::from(error.code), RpcError::Invalid);
        assert!(error.message.len() <= MAX_MESSAGE);
        assert!(std::str::from_utf8(error.message).is_ok());
    }

    #[test]
    fn endpoints_resolve_with_or_without_a_port() {
        assert_eq!(
            socket_addr("tcp://127.0.0.1:7856").unwrap(),
            SocketAddr::from(([127, 0, 0, 1], 7856))
        );
        assert_eq!(
            socket_addr("tcp://127.0.0.1").unwrap(),
            SocketAddr::from(([127, 0, 0, 1], TCP_DEFAULT_PORT))
        );
    }

    /// A client's store records each change under the table the proxy
    /// named for it, which is the client's own copy of the device's table
    /// while the two have one hash and session. A change decodes with its
    /// own table after the device's table has moved on to one that types
    /// its entry otherwise, and one named with no table does not decode.
    #[test]
    fn a_client_records_each_change_under_the_table_named_for_it() {
        use crate::device::rpc::RpcValue;
        use crate::device::store::{CHANGES, REGISTRY};
        use crate::device::SettingValueError;

        let (mut store, mut parser) = (Store::new(false), PacketParser::new(false));
        let route = DeviceRoute::root();
        let changes = store.follow(route, CHANGES);
        let table = |hash, meta| {
            let entries = vec![("test.amplitude".to_string(), meta)];
            Arc::new(RpcRegistry::from_entries(entries, Some(hash), None))
        };
        let (old, new) = (table(1, 0xA310), table(2, 0xA321));
        let reply = 0x1234i16.to_le_bytes();
        let setting = || Update::Setting("test.amplitude".to_string(), None, reply.to_vec().into());
        let mut apply = |update: Update| update.apply(route, &mut store, &mut parser);

        apply(Update::table(&Ok(Arc::clone(&old))));
        apply(Update::recorded(Some(&old)));
        apply(setting());
        apply(Update::table(&Ok(Arc::clone(&new))));
        apply(setting());
        apply(Update::recorded(Some(&new)));
        apply(setting());
        apply(Update::recorded(None));
        apply(setting());

        let held = store.value(route, REGISTRY).unwrap().unwrap();
        let received: Vec<_> = std::iter::from_fn(|| changes.try_recv().unwrap()).collect();
        let values: Vec<_> = received.iter().map(|change| change.value().ok()).collect();
        let byte = Some(RpcValue::U64(0x34));
        assert_eq!(
            values,
            [byte.clone(), byte, Some(RpcValue::I64(0x1234)), None]
        );
        assert!(Arc::ptr_eq(received[2].rpc_registry().unwrap(), &held));
        assert!(Arc::ptr_eq(
            received[0].rpc_registry().unwrap(),
            received[1].rpc_registry().unwrap()
        ));
        assert!(matches!(
            received[3].value(),
            Err(SettingValueError::NoTable)
        ));
    }

    /// A client counts back how long ago the proxy says a device was lost
    /// from the paused clock, and a presence update made of that moment says
    /// the same again.
    #[tokio::test(start_paused = true)]
    async fn a_lost_presence_counts_back_from_the_paused_clock() {
        let (mut store, mut parser) = (Store::new(false), PacketParser::new(false));
        let route = DeviceRoute::root();
        tokio::time::sleep(Duration::from_secs(60)).await;

        Update::Presence(Err(Some(1500))).apply(route, &mut store, &mut parser);
        let since = (Instant::now() - Duration::from_millis(1500)).into_std();
        assert_eq!(store.value(route, PRESENCE), Some(Presence::Lost { since }));
        let again = Update::presence(Presence::Lost { since });
        assert_eq!(again, Update::Presence(Err(Some(1500))));
    }
}
