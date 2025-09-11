//! Proxy
//!
//! A proxy allows to multiplex access to a hardware port, by one or more
//! `proxy::Port`s. These allow sending and receiving `proto::Packet`s
//! to/from a single sensor, or an arbitrary device tree, and restrcting the
//! type of traffic. Also, the proxy implements serial port rate negotiation
//! for higher data rates.
//!
//! Note: the proxy runs in a dedicated thread.

use super::port;
use super::proto::{self, DeviceRoute, Packet};
use super::proxy_core::{ProxyClient, ProxyCore};
use super::util;
use super::util::{TioRpcReplyable, TioRpcRequestable};

use std::env;
use std::thread;
use std::time::Duration;

use crossbeam::channel;

/// Status event that ProxyCore sent back to an optional user specified channel
#[derive(Debug)]
pub enum Event {
    SensorConnected,
    SensorDisconnected,
    SensorReconnected,
    FailedToConnect,
    FailedToReconnect,
    Exiting,
    ProtocolError(proto::Error),
    FatalError(port::RecvError),
    NewClient(u64),
    RpcRemap((u64, u16), u16),
    RpcRestore(u16, (u64, u16)),
    RpcRestoreNotFound(u16),
    RpcClientNotFound(u64),
    RpcTimeout(u16),
    RpcCancel(u16),
    ClientSendFailed(u64),
    ClientTerminated(u64),
    RootDeviceRestarted,
    AutoRateGaveUp,
    AutoRateQueried(u32),
    AutoRateRpcError(proto::RpcErrorCode),
    AutoRateRpcInvalid,
    AutoRateIncompatible(u32),
    AutoRateCompatible(u32),
    AutoRateWait,
    AutoRateSet(u32),
    SetRate(u32),
    SetRateFailed,
    NoData,
}

/// A port which communicates with a proxy via `crossbeam::channel`s
pub struct Port {
    tx: channel::Sender<Packet>,
    rx: channel::Receiver<Packet>,
    depth: usize,
}

#[derive(Debug, Clone)]
pub enum SendError {
    WouldBlock(Packet),
    ProxyDisconnected(Packet),
    InvalidRoute(Packet),
}

#[derive(Debug, Clone)]
pub enum RecvError {
    WouldBlock,
    ProxyDisconnected,
}

#[derive(Debug, Clone)]
pub enum RpcError {
    SendFailed(SendError),
    ExecError(proto::RpcErrorPayload),
    RecvFailed(RecvError),
    TypeError,
}

impl Port {
    /// Sends a TIO packet to this port synchronously. This call will
    /// block if the port is backed up.
    pub fn send(&self, packet: Packet) -> Result<(), SendError> {
        if packet.routing.len() > self.depth {
            return Err(SendError::InvalidRoute(packet));
        }
        match self.tx.send(packet) {
            Ok(()) => Ok(()),
            Err(se) => Err(SendError::ProxyDisconnected(se.into_inner())),
        }
    }

    /// Attempts to send a TIO packet to this port without blocking.
    pub fn try_send(&self, packet: Packet) -> Result<(), SendError> {
        if packet.routing.len() > self.depth {
            return Err(SendError::InvalidRoute(packet));
        }
        match self.tx.try_send(packet) {
            Ok(()) => Ok(()),
            Err(crossbeam::channel::TrySendError::Full(pkt)) => Err(SendError::WouldBlock(pkt)),
            Err(crossbeam::channel::TrySendError::Disconnected(pkt)) => {
                Err(SendError::ProxyDisconnected(pkt))
            }
        }
    }

    /// `Select` the tx channel
    pub fn select_send<'a>(&'a self, sel: &mut crossbeam::channel::Select<'a>) -> usize {
        sel.send(&self.tx)
    }

    /// Waits for a packet to be available, and returns it.
    pub fn recv(&self) -> Result<Packet, RecvError> {
        match self.rx.recv() {
            Ok(pkt) => Ok(pkt),
            Err(crossbeam::channel::RecvError) => Err(RecvError::ProxyDisconnected),
        }
    }

    /// Returns a packet if available, otherwise it doesn't stop.
    pub fn try_recv(&self) -> Result<Packet, RecvError> {
        match self.rx.try_recv() {
            Ok(pkt) => Ok(pkt),
            Err(crossbeam::channel::TryRecvError::Empty) => Err(RecvError::WouldBlock),
            Err(crossbeam::channel::TryRecvError::Disconnected) => {
                Err(RecvError::ProxyDisconnected)
            }
        }
    }

    /// `Select` the rx channel
    pub fn select_recv<'a>(&'a self, sel: &mut crossbeam::channel::Select<'a>) -> usize {
        sel.recv(&self.rx)
    }

    /// To use `crossbeam::channel::select!`.
    pub fn receiver<'a>(&'a self) -> &'a crossbeam::channel::Receiver<Packet> {
        &self.rx
    }

    /// Iterate over packets (until disconnect or break out).
    pub fn iter(&self) -> crossbeam::channel::Iter<'_, Packet> {
        self.rx.iter()
    }

    /// Iterate over packets (until disconnect, break out, or empty channel).
    pub fn try_iter(&self) -> crossbeam::channel::TryIter<'_, Packet> {
        self.rx.try_iter()
    }

    /// Generic any sized input/output RPC, blocking
    pub fn raw_rpc(&self, name: &str, arg: &[u8]) -> Result<Vec<u8>, RpcError> {
        if let Err(err) = self.send(util::PacketBuilder::make_rpc_request(
            name,
            arg,
            0,
            DeviceRoute::root(),
        )) {
            return Err(RpcError::SendFailed(err));
        }
        loop {
            match self.recv() {
                Ok(pkt) => match pkt.payload {
                    proto::Payload::RpcReply(rep) => return Ok(rep.reply),
                    proto::Payload::RpcError(err) => return Err(RpcError::ExecError(err)),
                    _ => continue,
                },
                Err(err) => {
                    return Err(RpcError::RecvFailed(err));
                }
            }
        }
    }

    pub fn rpc<ReqT: TioRpcRequestable<ReqT>, RepT: TioRpcReplyable<RepT>>(
        &self,
        name: &str,
        arg: ReqT,
    ) -> Result<RepT, RpcError> {
        let ret = self.raw_rpc(name, &arg.to_request())?;
        if let Ok(val) = RepT::from_reply(&ret) {
            Ok(val)
        } else {
            Err(RpcError::TypeError)
        }
    }

    /// Action: rpc with no argument which returns nothing
    pub fn action(&self, name: &str) -> Result<(), RpcError> {
        self.rpc(name, ())
    }

    pub fn get<T: TioRpcReplyable<T>>(&self, name: &str) -> Result<T, RpcError> {
        self.rpc(name, ())
    }
}

#[derive(Debug, Clone)]
pub enum PortError {
    RpcTimeoutTooShort,
    RpcTimeoutTooLong,
    FailedNewClientSetup,
}

/// Interface to a port proxy. Can create new ports.
pub struct Interface {
    new_client_queue: channel::Sender<ProxyClient>,
    new_client_confirm: Option<channel::Receiver<Event>>,
    client_rx_channel_size: usize,
    client_tx_channel_size: usize,
}

impl Interface {
    /// Create a new Interface, and a new ProxyCore running in a separate thread.
    pub fn new_proxy(
        url: &str,
        reconnect_timeout: Option<Duration>,
        status_queue: Option<channel::Sender<Event>>,
    ) -> Interface {
        let (client_sender, client_receiver) = channel::bounded::<ProxyClient>(5);
        let (status_sender, status_receiver, only_clients) = {
            if let Some(status_sender) = status_queue {
                (status_sender, None, false)
            } else {
                let (s, r) = channel::bounded::<Event>(50);
                (s, Some(r), true)
            }
        };
        let url_string = url.to_string();
        thread::spawn(move || {
            #[cfg(target_os = "windows")]
            let _priority = super::os::windows_helpers::ActivityGuard::latency_critical()
                .map_err(|e| eprintln!("proxy core: failed to raise thread priority: {e}"))
                .ok();

            #[cfg(target_os = "macos")]
            let _activity = super::os::macos_helpers::ActivityGuard::latency_critical(
                "Twinleaf proxy core"
            );
            
            let mut proxy = ProxyCore::new(
                url_string,
                reconnect_timeout,
                client_receiver,
                status_sender,
                only_clients,
            );
            proxy.run();
        });
        Interface {
            new_client_queue: client_sender,
            new_client_confirm: status_receiver,
            client_rx_channel_size: Self::get_client_rx_channel_size(),
            client_tx_channel_size: Self::get_client_tx_channel_size(),
        }
    }

    /// Create a new proxy which connects to a url with default parameters.
    pub fn new(url: &str) -> Interface {
        Self::new_proxy(url, None, None)
    }

    /// Create a new proxy which connects to a standalone tio-proxy process at
    /// the default address.
    pub fn default() -> Interface {
        Self::new(util::default_proxy_url())
    }

    pub fn get_client_rx_channel_size() -> usize {
        let min_size = port::DEFAULT_RX_CHANNEL_SIZE;
        if let Ok(req) = env::var("TWINLEAF_PROXY_INTERFACE_RX_BUFSIZE") {
            std::cmp::max(req.parse().unwrap_or(0), min_size)
        } else {
            min_size
        }
    }

    pub fn get_client_tx_channel_size() -> usize {
        let min_size = port::DEFAULT_TX_CHANNEL_SIZE;
        if let Ok(req) = env::var("TWINLEAF_PROXY_INTERFACE_TX_BUFSIZE") {
            std::cmp::max(req.parse().unwrap_or(0), min_size)
        } else {
            min_size
        }
    }

    /// Create a new port
    pub fn new_port(
        &self,
        rpc_timeout: Option<Duration>,
        scope: DeviceRoute,
        depth: usize,
        forward_data: bool,
        forward_nonrpc: bool,
    ) -> Result<Port, PortError> {
        let default_rpc_timeout = Duration::from_millis(3000);
        let rpc_timeout = rpc_timeout.unwrap_or(default_rpc_timeout);
        if rpc_timeout < Duration::from_millis(100) {
            return Err(PortError::RpcTimeoutTooShort);
        }
        if rpc_timeout > Duration::from_secs(60) {
            return Err(PortError::RpcTimeoutTooLong);
        }

        let (client_to_proxy_sender, proxy_from_client_receiver) =
            channel::bounded::<Packet>(self.client_tx_channel_size);
        let (proxy_to_client_sender, client_from_proxy_receiver) =
            channel::bounded::<Packet>(self.client_rx_channel_size);
        if let Err(_) = self.new_client_queue.send(ProxyClient::new(
            proxy_to_client_sender,
            proxy_from_client_receiver,
            rpc_timeout,
            scope,
            depth,
            forward_data,
            forward_nonrpc,
        )) {
            return Err(PortError::FailedNewClientSetup);
        }
        if let Some(confirm) = &self.new_client_confirm {
            if let Err(_) = confirm.recv() {
                return Err(PortError::FailedNewClientSetup);
            }
        }
        Ok(Port {
            tx: client_to_proxy_sender,
            rx: client_from_proxy_receiver,
            depth: depth,
        })
    }

    /// New port with default parameters for a subtree, receiving all packets.
    pub fn subtree_full(&self, subtree_root: DeviceRoute) -> Result<Port, PortError> {
        self.new_port(None, subtree_root, usize::MAX, true, true)
    }

    /// New port with default parameters for a subtree, receiving only RPCs.
    pub fn subtree_rpc(&self, subtree_root: DeviceRoute) -> Result<Port, PortError> {
        self.new_port(None, subtree_root, usize::MAX, false, false)
    }

    /// New port with default parameters for the full device tree, receiving all packets.
    pub fn tree_full(&self) -> Result<Port, PortError> {
        self.subtree_full(DeviceRoute::root())
    }

    /// New port for the full device tree, useful to probe connected devices.
    pub fn tree_probe(&self) -> Result<Port, PortError> {
        self.new_port(None, DeviceRoute::root(), usize::MAX, false, true)
    }

    /// New port with default parameters for the full device tree, receiving only RPCs.
    pub fn tree_rpc(&self) -> Result<Port, PortError> {
        self.subtree_rpc(DeviceRoute::root())
    }

    /// New port with default parameters for a specific device, receiving all packets.
    pub fn device_full(&self, address: DeviceRoute) -> Result<Port, PortError> {
        self.new_port(None, address, 0, true, true)
    }

    /// New port with default parameters for a specific device, receiving only RPCs.
    pub fn device_rpc(&self, address: DeviceRoute) -> Result<Port, PortError> {
        self.new_port(None, address, 0, false, false)
    }

    /// New port with default parameters for the root device, receiving all packets.
    pub fn root_full(&self) -> Result<Port, PortError> {
        self.device_full(DeviceRoute::root())
    }

    /// New port with default parameters for the root device, receiving only RPCs.
    pub fn root_rpc(&self) -> Result<Port, PortError> {
        self.device_rpc(DeviceRoute::root())
    }
}
