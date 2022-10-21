use crate::tio::proto::{self, DeviceRoute, Packet, Payload};
use crate::tio::proxy;
use crossbeam::channel;

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

impl TioRpcRequestable<u16> for u16 {
    fn to_request(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

impl TioRpcReplyable<u16> for u16 {
    fn from_reply_prefix(reply: &[u8]) -> Result<(u16, &[u8]), ()> {
        let array = if let Ok(array) = reply[0..2].try_into() {
            array
        } else {
            return Err(());
        };
        Ok((u16::from_le_bytes(array), &reply[2..]))
    }
}

impl TioRpcReplyableFixedSize for u16 {}

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

pub struct Rpc {
    tx: channel::Sender<Packet>,
    rx: channel::Receiver<Packet>,
}

impl Rpc {
    pub fn new(proxy: &proxy::Port, device: Option<DeviceRoute>) -> Rpc {
        let route = match device {
            Some(route) => route,
            None => DeviceRoute::root(),
        };
        let (tx, rx) = proxy.port(None, route, false, false).unwrap();
        Rpc { tx, rx }
    }

    // TODO: error handling?. this can fail if arg is too big. But we do fail on serialization so see what happens
    pub fn make_request(name: &str, arg: &[u8]) -> Packet {
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

    // TODO: error type

    /// Generic any sized input/output RPC
    pub fn raw_rpc(&self, name: &str, arg: &[u8]) -> Result<Vec<u8>, ()> {
        // TODO: error handling
        self.tx.send(Self::make_request(name, arg)).unwrap();
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
        arg: &ReqT,
    ) -> Result<RepT, ()> {
        let ret = self.raw_rpc(name, &arg.to_request())?;
        RepT::from_reply(&ret)
    }
    /// Action: rpc with no argument which returns nothing
    pub fn action(&self, name: &str) -> Result<(), ()> {
        self.rpc::<(), ()>(name, &())
    }

    pub fn get<T: TioRpcReplyable<T>>(&self, name: &str) -> Result<T, ()> {
        self.rpc(name, &())
    }
    // TODO: error type
    //fn get<T>() -> Result<T,()>;
    //fn set<T>() -> Result<T,()>;

    /*
    fn rpc<T>(name: String, arg: &T) -> Packet {
        panic!("NEED TO IMPLEMENT");
    }
    */
    /*
    fn rpc<u8>(name: String, arg: u8) -> Packet {
        Self::mkrpc(name, arg.to_le_bytes())
    }
    fn rpc<u16>(name: String, arg: u16) -> Packet {
        Self::mkrpc(name, arg.to_le_bytes())
    }
    */
    /*
    fn mkrpc<T>(name: String, arg: T) -> Packet {
        let bytes = arg.to_le_bytes();
        Packet {
            payload: Payload::RpcRequest(proto::RpcRequestPayload {
                id: 0,
                method: proto::RpcMethod::Name(name),
                arg: bytes.to_vec(),
            }),
            routing: DeviceRoute::root(),
            ttl: 0,
        }
    }*/
}

// TODO: legacy make methods for now

/*
pub fn rpc(name: String, arg: &[u8]) -> Packet {
    Packet {
        payload: Payload::RpcRequest(proto::RpcRequestPayload {
            id: 0,
            method: proto::RpcMethod::Name(name),
            arg: arg.to_vec(),
        }),
        routing: DeviceRoute::root(),
        ttl: 0,
    }
}
*/

pub fn make_hb(payload: Option<Vec<u8>>) -> Packet {
    Packet {
        payload: Payload::Heartbeat(proto::HeartbeatPayload::Any(match payload {
            Some(v) => v,
            None => {
                vec![]
            }
        })),
        routing: DeviceRoute::root(),
        ttl: 0,
    }
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
