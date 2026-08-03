use bytes::{Buf, BufMut};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RpcDecodeError {
    #[error("RPC reply is too short: expected at least {expected} bytes, got {actual}")]
    TooShort { expected: usize, actual: usize },
    #[error("RPC reply has {remaining} trailing bytes")]
    TrailingBytes { remaining: usize },
}

pub trait RpcArgs {
    fn encode_into(&self, output: &mut Vec<u8>);

    fn encode_args(&self) -> Vec<u8> {
        let mut output = Vec::new();
        self.encode_into(&mut output);
        output
    }
}

pub trait RpcReply: Sized {
    fn decode_from(input: &mut &[u8]) -> Result<Self, RpcDecodeError>;

    fn decode_reply(mut input: &[u8]) -> Result<Self, RpcDecodeError> {
        let value = Self::decode_from(&mut input)?;
        if input.is_empty() {
            Ok(value)
        } else {
            Err(RpcDecodeError::TrailingBytes {
                remaining: input.len(),
            })
        }
    }
}

/// Reply types with a fixed wire size. Only such a type can precede another
/// value in a tuple reply, since variable-length values consume the remainder
/// of the reply.
pub trait RpcReplyFixedSize: RpcReply {}

fn require_bytes(input: &[u8], expected: usize) -> Result<(), RpcDecodeError> {
    if input.len() < expected {
        Err(RpcDecodeError::TooShort {
            expected,
            actual: input.len(),
        })
    } else {
        Ok(())
    }
}

impl RpcArgs for () {
    fn encode_into(&self, _output: &mut Vec<u8>) {}
}

impl RpcReply for () {
    fn decode_from(_input: &mut &[u8]) -> Result<Self, RpcDecodeError> {
        Ok(())
    }
}

impl RpcReplyFixedSize for () {}

macro_rules! impl_rpc_scalar {
    ($type:ty, $size:expr, $put:ident, $get:ident) => {
        impl RpcArgs for $type {
            fn encode_into(&self, output: &mut Vec<u8>) {
                output.$put(*self);
            }
        }

        impl RpcReply for $type {
            fn decode_from(input: &mut &[u8]) -> Result<Self, RpcDecodeError> {
                require_bytes(input, $size)?;
                Ok(input.$get())
            }
        }

        impl RpcReplyFixedSize for $type {}
    };
}

impl_rpc_scalar!(u8, 1, put_u8, get_u8);
impl_rpc_scalar!(i8, 1, put_i8, get_i8);
impl_rpc_scalar!(u16, 2, put_u16_le, get_u16_le);
impl_rpc_scalar!(i16, 2, put_i16_le, get_i16_le);
impl_rpc_scalar!(u32, 4, put_u32_le, get_u32_le);
impl_rpc_scalar!(i32, 4, put_i32_le, get_i32_le);
impl_rpc_scalar!(u64, 8, put_u64_le, get_u64_le);
impl_rpc_scalar!(i64, 8, put_i64_le, get_i64_le);
impl_rpc_scalar!(f32, 4, put_f32_le, get_f32_le);
impl_rpc_scalar!(f64, 8, put_f64_le, get_f64_le);

impl RpcArgs for str {
    fn encode_into(&self, output: &mut Vec<u8>) {
        output.extend_from_slice(self.as_bytes());
    }
}

impl RpcArgs for String {
    fn encode_into(&self, output: &mut Vec<u8>) {
        self.as_str().encode_into(output);
    }
}

impl<T: RpcArgs + ?Sized> RpcArgs for &T {
    fn encode_into(&self, output: &mut Vec<u8>) {
        (*self).encode_into(output);
    }
}

impl RpcReply for String {
    fn decode_from(input: &mut &[u8]) -> Result<Self, RpcDecodeError> {
        let value = String::from_utf8_lossy(input).into_owned();
        *input = &[];
        Ok(value)
    }
}

impl<A: RpcArgs, B: RpcArgs> RpcArgs for (A, B) {
    fn encode_into(&self, output: &mut Vec<u8>) {
        self.0.encode_into(output);
        self.1.encode_into(output);
    }
}

impl<A: RpcReplyFixedSize, B: RpcReply> RpcReply for (A, B) {
    fn decode_from(input: &mut &[u8]) -> Result<Self, RpcDecodeError> {
        Ok((A::decode_from(input)?, B::decode_from(input)?))
    }
}

impl<A: RpcReplyFixedSize, B: RpcReplyFixedSize> RpcReplyFixedSize for (A, B) {}
