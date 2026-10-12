//! The control protocol, which carries a client's requests to the proxy
//! itself. A request travels in a `PROXY_TX` packet and its answer in a
//! `PROXY_RX` packet. [`Request`] decodes a request, [`answer`] builds the
//! reply, and [`call`] and [`handshake`] make requests as a client.

use crate::proto::packet::Packet as WirePacket;
use crate::proto::rpc::RpcError;
use crate::proto::TCP_DEFAULT_PORT;
use crate::rt::Supervisor;
use crate::tio::packet::{LinkStatus, Packet, Payload, ProxyRx};
use crate::tio::rpc::{RpcArgs, RpcDecodeError, RpcReply};
use crate::tio::transport::{self, ReceiveResult, RecvError};
use std::fmt;
use std::io;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
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
/// The method that stops a background proxy.
pub(super) const STOP: &str = "proxy.stop";

/// The version of the control protocol this build speaks.
pub(super) const VERSION: u8 = 1;

/// What one side of `proxy.hello` says about itself. A client sends its own,
/// and the proxy replies with its own.
///
/// On the wire it is the `u32` pid, the `u8` protocol, the `twinleaf`
/// version as three `u16`s for major, minor and patch, and then the name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Introduction {
    /// The process id.
    pub(crate) pid: u32,
    /// The version of the control protocol it speaks.
    pub(crate) protocol: u8,
    /// The version of the `twinleaf` crate it was built with.
    pub(crate) version: [u16; 3],
    /// Its name, such as `tio monitor` or `tio proxy`.
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

impl RpcArgs for Introduction {
    fn encode_into(&self, output: &mut Vec<u8>) {
        let [major, minor, patch] = self.version;
        (
            self.pid,
            (self.protocol, (major, (minor, (patch, self.name.as_str())))),
        )
            .encode_into(output);
    }
}

impl RpcReply for Introduction {
    fn decode_from(input: &mut &[u8]) -> Result<Self, RpcDecodeError> {
        let (pid, (protocol, (major, (minor, (patch, name))))) =
            <(u32, (u8, (u16, (u16, (u16, String)))))>::decode_from(input)?;
        Ok(Introduction {
            pid,
            protocol,
            version: [major, minor, patch],
            name,
        })
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
    /// `proxy.stop`.
    Stop,
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
        let malformed = |error: RpcDecodeError| Refusal::new(RpcError::ArgsSize, error.to_string());
        match name {
            b"proxy.hello" => Ok(Request::Hello(
                Introduction::decode_reply(args).map_err(malformed)?,
            )),
            b"proxy.open" => Ok(Request::Open {
                target: String::decode_reply(args).map_err(malformed)?,
            }),
            b"proxy.probe" => Ok(Request::Probe {
                url: String::decode_reply(args).map_err(malformed)?,
            }),
            b"proxy.stop" => Ok(Request::Stop),
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
            Request::Hello(introduction) => (HELLO, introduction.encode_args()),
            Request::Open { target } => (OPEN, target.encode_args()),
            Request::Probe { url } => (PROBE, url.encode_args()),
            Request::Stop => (STOP, Vec::new()),
            #[cfg(test)]
            Request::Panic => ("proxy.test.panic", Vec::new()),
        }
    }
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
    let connected = TcpStream::connect_timeout(&socket_addr(endpoint)?, connect);
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
    let started = Instant::now();
    let deadline = started + timeout;
    let hello_deadline = deadline.min(started + HELLO_TIMEOUT);
    // Whether the peer has sent any PROXY_RX, which only a 2.0 proxy does.
    let mut answered = false;
    let mut host = None;
    loop {
        let until = if answered { deadline } else { hello_deadline };
        let packet = match timeout_at(until, received.recv_async()).await {
            Ok(Ok(Ok(packet))) => packet,
            Ok(Ok(Err(RecvError::Text(_) | RecvError::Protocol(_)))) => continue,
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    format!("the proxy at {endpoint} closed the connection before it could {goal}"),
                ))
            }
            Err(_) if answered => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("the proxy at {endpoint} did not {goal} within {timeout:?}"),
                ))
            }
            Err(_) => return Err(not_a_proxy(endpoint)),
        };
        let Payload::ProxyRx(rx) = packet.payload() else {
            continue;
        };
        answered = true;
        match rx {
            ProxyRx::Reply(reply) if reply.req_id.value() == HELLO_ID => {
                let introduction = Introduction::decode_reply(reply.value).map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("the proxy at {endpoint} answered proxy.hello with {error}"),
                    )
                })?;
                if introduction.protocol < VERSION {
                    return Err(older_protocol(endpoint, &introduction));
                }
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
            Request::Stop,
        ] {
            assert_eq!(sent(&request), Ok(request));
        }
    }

    /// An introduction is the pid, the protocol and the version, each little
    /// endian, then the name raw.
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
        assert_eq!(args, [4, 3, 2, 1, 1, 2, 0, 4, 3, 5, 0, b'a', b'b']);
    }

    #[test]
    fn replies_decode_as_they_were_encoded() {
        let host = Introduction::of_this_process("tio proxy");
        assert_eq!(Introduction::decode_reply(&host.encode_args()), Ok(host));
        let target = "tcp://127.0.0.1:7856".to_string();
        assert_eq!(String::decode_reply(&target.encode_args()), Ok(target));
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
