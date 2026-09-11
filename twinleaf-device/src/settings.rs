//! A setting: the value cell behind an RPC property, and what a write to it
//! announces.
//!
//! An application declares one per property and answers its RPC with
//! [`crate::device::Device::apply`], which broadcasts a SETTING packet on
//! every write and counts it in `settings.version`.

use twinleaf_proto::rpc::RpcError;

use crate::rpc::{self, put, Reply, RpcSpec};

/// A value a setting holds, encoded as its RPC reads and writes it.
pub trait Scalar: Copy {
    /// The kind of value the table declares for it.
    const KIND: rpc::Kind;

    /// Decode the argument of a write.
    fn decode(args: &[u8]) -> Result<Self, RpcError>;

    /// Append the value as the RPC replies it.
    fn encode(self, out: &mut Reply) -> Result<(), RpcError>;
}

macro_rules! number {
    ($($type:ty => $variant:ident,)*) => {$(
        impl Scalar for $type {
            const KIND: rpc::Kind = rpc::Kind::$variant(core::mem::size_of::<$type>() as u16);

            fn decode(args: &[u8]) -> Result<Self, RpcError> {
                let bytes = args.try_into().map_err(|_| RpcError::ArgsSize)?;
                Ok(Self::from_le_bytes(bytes))
            }

            fn encode(self, out: &mut Reply) -> Result<(), RpcError> {
                put(out, &self.to_le_bytes())
            }
        }
    )*};
}

number! {
    u8 => Uint,
    u16 => Uint,
    u32 => Uint,
    i32 => Int,
    f32 => Float,
    f64 => Float,
}

/// A flag, one byte on the wire like every other libtio bool.
impl Scalar for bool {
    const KIND: rpc::Kind = rpc::Kind::Bool;

    fn decode(args: &[u8]) -> Result<Self, RpcError> {
        Ok(u8::decode(args)? != 0)
    }

    fn encode(self, out: &mut Reply) -> Result<(), RpcError> {
        u8::from(self).encode(out)
    }
}

/// Whether an RPC wrote the setting. Every write is announced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Changed {
    /// The RPC read the value.
    Unchanged,
    /// The RPC wrote the value.
    Changed,
}

/// One value an RPC property reads and writes.
pub struct Setting<T: Scalar> {
    name: &'static str,
    value: T,
    initial: T,
    check: fn(T) -> Result<T, RpcError>,
}

impl<T: Scalar> Setting<T> {
    /// A setting booting at `initial`, accepting every write.
    pub const fn new(name: &'static str, initial: T) -> Self {
        Self {
            name,
            value: initial,
            initial,
            check: Ok,
        }
    }

    /// The same setting, taking only the values `check` passes.
    pub const fn checked(self, check: fn(T) -> Result<T, RpcError>) -> Self {
        Self { check, ..self }
    }

    /// The name its RPC and its announcements carry.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// The value it holds.
    pub fn get(&self) -> T {
        self.value
    }

    /// Go back to the value a boot starts from.
    pub fn reset(&mut self) {
        self.value = self.initial;
    }

    /// Answer its RPC: no argument reads, a value of its size writes.
    pub fn rpc(&mut self, args: &[u8], out: &mut Reply) -> Result<Changed, RpcError> {
        let changed = match args {
            [] => Changed::Unchanged,
            value => {
                self.value = (self.check)(T::decode(value)?)?;
                Changed::Changed
            }
        };
        self.value.encode(out)?;
        Ok(changed)
    }

    /// The table entry it answers.
    pub fn spec(&self) -> RpcSpec {
        RpcSpec::prop(self.name, T::KIND, rpc::Access::RW)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::{Access, Kind, Method};

    fn positive(value: f64) -> Result<f64, RpcError> {
        (value.is_finite() && value > 0.0)
            .then_some(value)
            .ok_or(RpcError::Invalid)
    }

    fn answer<T: Scalar>(
        setting: &mut Setting<T>,
        args: &[u8],
    ) -> Result<(Changed, Vec<u8>), RpcError> {
        let mut out = Reply::new();
        setting
            .rpc(args, &mut out)
            .map(|changed| (changed, out.to_vec()))
    }

    #[test]
    fn no_argument_reads_and_a_value_writes() {
        let mut gain = Setting::new("app.gain", 3u16);
        assert_eq!(answer(&mut gain, &[]), Ok((Changed::Unchanged, vec![3, 0])));
        assert_eq!(
            answer(&mut gain, &7u16.to_le_bytes()),
            Ok((Changed::Changed, vec![7, 0]))
        );
        assert_eq!(gain.get(), 7);
        assert_eq!(answer(&mut gain, &[1]), Err(RpcError::ArgsSize));
        assert_eq!(answer(&mut gain, &[1, 0, 0]), Err(RpcError::ArgsSize));
        assert_eq!(gain.get(), 7);
    }

    #[test]
    fn a_check_refuses_a_value_and_keeps_the_one_held() {
        let mut rate = Setting::new("app.rate", 1.0f64).checked(positive);
        assert_eq!(
            answer(&mut rate, &(-1.0f64).to_le_bytes()),
            Err(RpcError::Invalid)
        );
        assert_eq!(
            answer(&mut rate, &f64::NAN.to_le_bytes()),
            Err(RpcError::Invalid)
        );
        assert_eq!(rate.get(), 1.0);
    }

    #[test]
    fn a_reset_restores_the_value_a_boot_starts_from() {
        let mut enable = Setting::new("app.enable", true);
        assert_eq!(answer(&mut enable, &[0]), Ok((Changed::Changed, vec![0])));
        enable.reset();
        assert!(enable.get());
        assert_eq!(enable.name(), "app.enable");
    }

    #[test]
    fn a_flag_takes_any_nonzero_byte_and_replies_with_one() {
        let mut flag = Setting::new("app.enable", false);
        assert_eq!(answer(&mut flag, &[2]), Ok((Changed::Changed, vec![1])));
        assert!(flag.get());
        assert_eq!(answer(&mut flag, &[0]), Ok((Changed::Changed, vec![0])));
        assert!(!flag.get());
    }

    #[test]
    fn a_spec_describes_the_property_the_setting_answers() {
        let spec = Setting::new("app.rate", 1.0f64).spec();
        assert_eq!(spec.method, Method::Prop);
        assert_eq!(spec.kind, Kind::Float(8));
        assert_eq!(spec.access, Access::RW);
        assert_eq!(Setting::new("app.enable", true).spec().kind, Kind::Bool);

        let kinds = [
            Setting::new("u8", 0u8).spec().kind,
            Setting::new("u32", 0u32).spec().kind,
            Setting::new("i32", 0i32).spec().kind,
            Setting::new("f32", 0.0f32).spec().kind,
        ];
        assert_eq!(
            kinds,
            [Kind::Uint(1), Kind::Uint(4), Kind::Int(4), Kind::Float(4)]
        );
    }
}
