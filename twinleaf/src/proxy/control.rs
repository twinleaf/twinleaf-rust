//! The control protocol, which carries a client's requests to the proxy
//! itself. A request travels in a `PROXY_TX` packet and its answer in a
//! `PROXY_RX` packet, its arguments and reply each in CBOR. [`Request`]
//! decodes a request, [`answer`] builds the reply, and [`call`] and
//! [`handshake`] make requests as a client.

use super::Layout;
use crate::data::DeviceMetadataSnapshot;
use crate::device::rpc::{CallError, RpcErrorPayload, RpcRegistry, RpcRegistryError};
use crate::device::{RegistryState, Settings};
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

/// The version of the control protocol this build speaks. Version 2 encodes
/// every argument and reply in CBOR.
pub(super) const VERSION: u8 = 2;

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
    /// The version of the control protocol it speaks.
    #[n(1)]
    pub(crate) protocol: u8,
    /// The version of the `twinleaf` crate it was built with, as major,
    /// minor and patch.
    #[n(2)]
    pub(crate) version: [u16; 3],
    /// Its name, such as `tio monitor` or `tio proxy`.
    #[n(3)]
    pub(crate) name: String,
}

impl Introduction {
    /// Returns the introduction of this process under `name`.
    pub(crate) fn of_this_process(name: impl Into<String>) -> Introduction {
        let part = |text: &str| text.parse().unwrap_or(0);
        Introduction {
            pid: std::process::id(),
            protocol: VERSION,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub(crate) struct Watch {
    #[n(0)]
    #[cbor(with = "hops")]
    pub(crate) route: DeviceRoute,
    /// Whether the watch follows the RPC table and the settings, rather
    /// than the metadata.
    #[n(1)]
    pub(crate) table: bool,
}

/// One value of a watched device's state, as a reply to `proxy.watch`
/// carries it in [`Part`]s.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub(crate) enum Update {
    /// The device's complete metadata, as its own metadata packets, each
    /// addressed to the root.
    #[n(0)]
    Metadata(#[n(0)] Vec<ByteVec>),
    /// The device has no `dev.metadata`.
    #[n(1)]
    Unsupported,
    /// The RPC table, as its hash, the session it was read in, and the
    /// name and metadata word of each entry.
    #[n(2)]
    Table {
        #[n(0)]
        hash: Option<u32>,
        #[n(1)]
        session: Option<u32>,
        #[n(2)]
        entries: Vec<(String, u16)>,
    },
    /// The RPC table did not load, as the device refused a call with this
    /// code and message.
    #[n(3)]
    Refused(#[n(0)] u16, #[n(1)] ByteVec),
    /// The RPC table did not load, as it changed during every walk.
    #[n(4)]
    Changed,
    /// The RPC table did not load, as a call timed out.
    #[n(5)]
    TimedOut,
    /// The RPC table did not load, as a call got no reply.
    #[n(6)]
    Lost,
    /// The values of the announced settings, as the hash of the table they
    /// were read with, their session and version, and each value by name.
    #[n(7)]
    Settings {
        #[n(0)]
        hash: Option<u32>,
        #[n(1)]
        session: Option<u32>,
        #[n(2)]
        version: Option<u32>,
        #[n(3)]
        values: BTreeMap<String, ByteVec>,
    },
}

impl Update {
    /// Returns the update of a device's complete metadata, or `None` if a
    /// record does not fit a packet.
    pub(crate) fn metadata(snapshot: &DeviceMetadataSnapshot) -> Option<Update> {
        let packets = snapshot.metadata_packets().ok()?;
        let records = packets.iter().map(|packet| {
            packet
                .with_route(DeviceRoute::root())
                .as_bytes()
                .to_vec()
                .into()
        });
        Some(Update::Metadata(records.collect()))
    }

    /// Returns the update of an RPC table as loaded.
    pub(crate) fn table(registry: &RpcRegistry) -> Update {
        Update::Table {
            hash: registry.hash,
            session: registry.session.map(SessionId::value),
            entries: registry
                .iter()
                .map(|rpc| (rpc.full_name.clone(), rpc.meta.bits()))
                .collect(),
        }
    }

    /// Returns the update of why an RPC table did not load.
    pub(crate) fn unloaded(error: &RpcRegistryError) -> Update {
        match error {
            RpcRegistryError::DeviceRpcError(CallError::DeviceError(refusal)) => {
                Update::Refused(refusal.error.value(), refusal.extra.clone().into())
            }
            RpcRegistryError::DeviceRpcError(CallError::Timeout) => Update::TimedOut,
            RpcRegistryError::TableChanged => Update::Changed,
            RpcRegistryError::DeviceRpcError(_) => Update::Lost,
        }
    }

    /// Returns the update of the values of a device's announced settings.
    pub(crate) fn settings(settings: &Settings) -> Update {
        Update::Settings {
            hash: settings.rpc_registry().hash,
            session: settings.session().map(SessionId::value),
            version: settings.version(),
            values: settings
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_vec().into()))
                .collect(),
        }
    }

    /// Returns the RPC table, or why it did not load, that the update
    /// carries, or `None` for an update of something else.
    pub(crate) fn registry(self) -> Option<RegistryState> {
        let failed = |error: CallError| Some(Err(RpcRegistryError::DeviceRpcError(error)));
        match self {
            Update::Table {
                hash,
                session,
                entries,
            } => {
                let session = session.map(SessionId::new);
                Some(Ok(Arc::new(RpcRegistry::from_entries(
                    entries, hash, session,
                ))))
            }
            Update::Refused(code, message) => failed(CallError::DeviceError(RpcErrorPayload {
                error: RpcError::from(code),
                extra: message.into(),
            })),
            Update::TimedOut => failed(CallError::Timeout),
            Update::Lost => failed(CallError::ResponseLost),
            Update::Changed => Some(Err(RpcRegistryError::TableChanged)),
            Update::Metadata(_) | Update::Unsupported | Update::Settings { .. } => None,
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
/// all. A peer that is silent this long is not a 2.0 proxy.
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
    let connect = timeout.min(HELLO_TIMEOUT);
    let connected = transport::connect_blocking(&socket_addr(endpoint)?, connect);
    let stream = connected.map_err(|error| match error.kind() {
        // A firewall can drop a connection to a closed port rather than
        // refuse it.
        io::ErrorKind::ConnectionRefused | io::ErrorKind::TimedOut => {
            io::Error::new(error.kind(), format!("no proxy is running at {endpoint}"))
        }
        _ => error,
    })?;
    stream.set_nodelay(true)?;
    let (sender, received) = flume::bounded(transport::DEFAULT_RX_CHANNEL_SIZE);
    let port =
        transport::Port::from_tcp_stream(stream, sender, transport::DEFAULT_TX_CHANNEL_SIZE)?;
    let (endpoint, method, args) = (endpoint.to_string(), method.to_string(), args.to_vec());
    Supervisor::new().run("proxy control call", async move {
        let goal = format!("answer {method}");
        let (_, reply) =
            handshake(&port, &received, &endpoint, &method, &args, &goal, timeout).await?;
        Ok(reply)
    })
}

/// Introduces this process to the proxy at `endpoint` with `proxy.hello`,
/// then makes the control request `method` with `args`. Sends on `port` and
/// reads its results from `received`. Waits at most `timeout` for the reply.
/// Returns the proxy's [`Introduction`], if it gave one before the reply, and
/// the reply's bytes.
///
/// Returns an [`io::ErrorKind::Unsupported`] error naming the proxy if it
/// speaks an older control protocol than this build. Returns an error with
/// the proxy's message if the proxy refuses the hello or the request. Returns an error saying it could not do `goal`, such as
/// `open auto`, if the proxy stops, closes the connection, or does not answer
/// in time. Returns an [`io::ErrorKind::InvalidData`] error saying how to
/// reach the peer if it sends no control reply within [`HELLO_TIMEOUT`]. A
/// proxy older than tio 2.0 is such a peer.
pub(crate) async fn handshake(
    port: &transport::Port,
    received: &flume::Receiver<ReceiveResult>,
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
    // wait longer than that for its device. A peer that is no proxy hears a
    // heartbeat or two, as it would from a client older than tio 2.0.
    port.keep_alive(CLIENT_KEEPALIVE).await;
    let started = Instant::now();
    let deadline = started + timeout;
    let hello_deadline = deadline.min(started + HELLO_TIMEOUT);
    // Whether the peer has sent any PROXY_RX, which only a 2.0 proxy does.
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
                not_a_proxy(endpoint)
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
            // A proxy from before protocol 2 can neither read this hello nor
            // write one this build reads.
            ProxyRx::Reply(reply) if reply.req_id.value() == HELLO_ID => {
                let introduction: Introduction =
                    decode(reply.value).map_err(|_| older_build(endpoint))?;
                if introduction.protocol < VERSION {
                    return Err(older_protocol(endpoint, &introduction));
                }
                log::debug!("the proxy at {endpoint} is {introduction}");
                host = Some(introduction);
            }
            ProxyRx::Reply(reply) if reply.req_id.value() == REQUEST_ID => {
                return Ok((host, reply.value.to_vec()))
            }
            ProxyRx::Error(error)
                if error.req_id.value() == HELLO_ID
                    && RpcError::from(error.code) == RpcError::ArgsSize =>
            {
                return Err(older_build(endpoint))
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

/// Returns the error for the proxy at `endpoint`, introduced as `host`, that
/// speaks an older control protocol than this build.
fn older_protocol(endpoint: &str, host: &Introduction) -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        format!(
            "the proxy at {endpoint} is {host}, which speaks proxy protocol {}, older than {VERSION}; \
             update it, or stop it and let this program start its own",
            host.protocol
        ),
    )
}

/// Returns the error for the proxy at `endpoint` from a build of twinleaf
/// before protocol 2, which cannot read this build's hello.
fn older_build(endpoint: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        format!(
            "the proxy at {endpoint} is from an older build of twinleaf and speaks another \
             proxy protocol; update tio, or stop it and let this program start its own"
        ),
    )
}

/// Returns the error for a peer at `endpoint` that is not a 2.0 proxy, such
/// as a proxy older than tio 2.0. Its message says how to reach the peer.
pub(super) fn not_a_proxy(endpoint: &str) -> io::Error {
    let port = endpoint
        .rsplit(':')
        .next()
        .and_then(|port| port.parse().ok());
    let port = port.unwrap_or(TCP_DEFAULT_PORT);
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("{endpoint} is not a Twinleaf 2.0 proxy; reach it with -r tcp://localhost:{port}"),
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
        let (send, received) = flume::unbounded();
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
        let (send, received) = flume::unbounded();
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
                protocol: u8::MAX,
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

    /// An introduction is a CBOR array of the pid, the protocol, the version
    /// as an array of three, and the name.
    #[test]
    fn a_hello_matches_the_wire_layout() {
        let (name, args) = Request::Hello(Introduction {
            pid: 0x0102_0304,
            protocol: 1,
            version: [2, 0x0304, 5],
            name: "ab".to_string(),
        })
        .encode();

        assert_eq!(name, "proxy.hello");
        let pid = [0x1a, 1, 2, 3, 4];
        let version = [0x83, 2, 0x19, 3, 4, 5];
        let expected = [&[0x84][..], &pid, &[1], &version, &[0x62, b'a', b'b']].concat();
        assert_eq!(args, expected);
    }

    /// A proxy from before protocol 2 refuses this build's hello as cut
    /// short, which the client reports as a proxy to update.
    #[tokio::test]
    async fn a_hello_refused_as_cut_short_is_a_proxy_from_an_older_build() {
        let (send, received) = flume::unbounded();
        let (port, _peer) =
            transport::Port::fake(transport::TransportKind::Tcp, None, send.clone());
        let refusal = Packet::proxy_error(HELLO_ID, RpcError::ArgsSize, "short").unwrap();
        send.send(Ok(TransportEvent::Packet(refusal))).unwrap();

        let error = handshake(
            &port,
            &received,
            "tcp://127.0.0.1:7855",
            OPEN,
            &[],
            "open the device",
            Duration::from_secs(1),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert!(error.to_string().contains("older build"), "{error}");
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

    /// This build introduces itself with its own protocol and crate version.
    #[test]
    fn this_process_gives_its_protocol_and_version() {
        let introduction = Introduction::of_this_process("tio monitor");

        assert_eq!(introduction.pid, std::process::id());
        assert_eq!(introduction.protocol, VERSION);
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
}
