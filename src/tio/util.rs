use crate::tio::proto::{self, DeviceRoute, Packet, Payload};
use crate::tio::proxy;
use crossbeam::channel;

pub fn default_proxy_url() -> &'static str {
    "tcp://localhost"
}

pub struct PacketBuilder {
    routing: DeviceRoute,
}

impl PacketBuilder {
    pub fn new(routing: DeviceRoute) -> PacketBuilder {
        PacketBuilder { routing }
    }

    pub fn make_rpc_request(name: &str, arg: &[u8]) -> Packet {
        Packet {
            payload: Payload::RpcRequest(proto::RpcRequestPayload {
                id: 0,
                method: proto::RpcMethod::Name(name.into()),
                arg: arg.to_vec(),
            }),
            routing: DeviceRoute::root(),
            ttl: 0,
        }
    }

    pub fn rpc_request(&self, name: &str, arg: &[u8]) -> Packet {
        let mut ret = Self::make_rpc_request(name, arg);
        ret.routing = self.routing.clone();
        ret
    }

    pub fn make_rpc_error(id: u16, error: proto::RpcErrorCodeRaw) -> Packet {
        Packet {
            payload: Payload::RpcError(proto::RpcErrorPayload {
                id: id,
                error: proto::RpcErrorCode::Known(error),
                extra: vec![],
            }),
            routing: DeviceRoute::root(),
            ttl: 0,
        }
    }

    pub fn rpc_error(&self, id: u16, error: proto::RpcErrorCodeRaw) -> Packet {
        let mut ret = Self::make_rpc_error(id, error);
        ret.routing = self.routing.clone();
        ret
    }

    pub fn make_heartbeat(payload: Vec<u8>) -> Packet {
        Packet {
            payload: Payload::Heartbeat(proto::HeartbeatPayload::Any(payload)),
            routing: DeviceRoute::root(),
            ttl: 0,
        }
    }

    pub fn heartbeat(&self, payload: Vec<u8>) -> Packet {
        let mut ret = Self::make_heartbeat(payload);
        ret.routing = self.routing.clone();
        ret
    }

    pub fn make_empty_heartbeat() -> Packet {
        PacketBuilder::make_heartbeat(vec![])
    }

    pub fn empty_heartbeat(&self) -> Packet {
        let mut ret = Self::make_empty_heartbeat();
        ret.routing = self.routing.clone();
        ret
    }
}

pub trait TioRpcRequestable<T> {
    fn to_request(&self) -> Vec<u8>;
}

pub trait TioRpcReplyable<T> {
    fn from_reply_prefix(reply: &[u8]) -> Result<(T, &[u8]), ()>;

    fn from_reply(reply: &[u8]) -> Result<T, ()> {
        let (ret, rest) = Self::from_reply_prefix(reply)?;
        if rest.len() == 0 {
            Ok(ret)
        } else {
            Err(())
        }
    }
}

pub trait TioRpcReplyableFixedSize {}

impl TioRpcRequestable<()> for () {
    fn to_request(&self) -> Vec<u8> {
        vec![]
    }
}

impl TioRpcReplyable<()> for () {
    fn from_reply_prefix(reply: &[u8]) -> Result<((), &[u8]), ()> {
        Ok(((), reply))
    }
}

impl TioRpcReplyableFixedSize for () {}

macro_rules! make_tio_rpc_traits {
    ($primitive: ident) => {
        impl TioRpcRequestable<$primitive> for $primitive {
            fn to_request(&self) -> Vec<u8> {
                self.to_le_bytes().to_vec()
            }
        }

        impl TioRpcReplyable<$primitive> for $primitive {
            fn from_reply_prefix(reply: &[u8]) -> Result<($primitive, &[u8]), ()> {
                let psize = std::mem::size_of::<$primitive>();
                if reply.len() < psize {
                    return Err(());
                }
                let array = if let Ok(array) = reply[0..psize].try_into() {
                    array
                } else {
                    return Err(());
                };
                Ok(($primitive::from_le_bytes(array), &reply[psize..]))
            }
        }
        impl TioRpcReplyableFixedSize for $primitive {}
    };
}

make_tio_rpc_traits!(u8);
make_tio_rpc_traits!(i8);
make_tio_rpc_traits!(u16);
make_tio_rpc_traits!(i16);
make_tio_rpc_traits!(u32);
make_tio_rpc_traits!(i32);
make_tio_rpc_traits!(u64);
make_tio_rpc_traits!(i64);
make_tio_rpc_traits!(f32);
make_tio_rpc_traits!(f64);

// &str only for requests
impl TioRpcRequestable<&str> for &str {
    fn to_request(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

// String for requests and replies, but not fixed size
impl TioRpcRequestable<String> for String {
    fn to_request(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

impl TioRpcRequestable<&String> for &String {
    fn to_request(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

impl TioRpcReplyable<String> for String {
    fn from_reply_prefix(reply: &[u8]) -> Result<(String, &[u8]), ()> {
        Ok((String::from_utf8_lossy(reply).to_string(), &[]))
    }
}

impl<A: TioRpcRequestable<A>, B: TioRpcRequestable<B>> TioRpcRequestable<(A, B)> for (A, B) {
    fn to_request(&self) -> Vec<u8> {
        let mut ret = self.0.to_request();
        ret.extend(self.1.to_request());
        ret
    }
}

impl<A: TioRpcReplyable<A> + TioRpcReplyableFixedSize, B: TioRpcReplyable<B>>
    TioRpcReplyable<(A, B)> for (A, B)
{
    fn from_reply_prefix(reply: &[u8]) -> Result<((A, B), &[u8]), ()> {
        let (first, rest) = A::from_reply_prefix(reply)?;
        let (second, rest) = B::from_reply_prefix(rest)?;
        Ok(((first, second), rest))
    }
}

impl<
        A: TioRpcReplyable<A> + TioRpcReplyableFixedSize,
        B: TioRpcReplyable<B> + TioRpcReplyableFixedSize,
    > TioRpcReplyableFixedSize for (A, B)
{
}

pub struct DeviceRpc {
    tx: channel::Sender<Packet>,
    rx: channel::Receiver<Packet>,
}

impl DeviceRpc {
    pub fn new(proxy: &proxy::Port, device: Option<DeviceRoute>) -> DeviceRpc {
        let route = match device {
            Some(route) => route,
            None => DeviceRoute::root(),
        };
        let (tx, rx) = proxy.port(None, route, false, false).unwrap();
        DeviceRpc { tx, rx }
    }

    // TODO: error type

    /// Generic any sized input/output RPC
    pub fn raw_rpc(&self, name: &str, arg: &[u8]) -> Result<Vec<u8>, ()> {
        // TODO: error handling
        self.tx
            .send(PacketBuilder::make_rpc_request(name, arg))
            .unwrap();
        let rep = self.rx.recv().unwrap();
        if let Payload::RpcReply(rep) = rep.payload {
            Ok(rep.reply)
        } else {
            Err(())
        }
    }

    pub fn rpc<ReqT: TioRpcRequestable<ReqT>, RepT: TioRpcReplyable<RepT>>(
        &self,
        name: &str,
        arg: ReqT,
    ) -> Result<RepT, ()> {
        let ret = self.raw_rpc(name, &arg.to_request())?;
        RepT::from_reply(&ret)
    }

    /// Action: rpc with no argument which returns nothing
    pub fn action(&self, name: &str) -> Result<(), ()> {
        self.rpc(name, ())
    }

    pub fn get<T: TioRpcReplyable<T>>(&self, name: &str) -> Result<T, ()> {
        self.rpc(name, ())
    }
}
