//! A setting: the value cell behind an RPC property, and what a write to it
//! announces.
//!
//! An application declares one per property and answers its RPC with
//! [`crate::device::Device::apply`], which broadcasts a SETTING packet on
//! every write and counts it in `settings.version`.

use twinleaf_proto::rpc::RpcError;

use crate::rpc::{self, put, Access, Reply, RpcSpec};

/// A value a setting holds, encoded as its RPC reads and writes it.
pub trait Scalar: Copy + PartialEq {
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

/// A short string a setting holds, bounded as tl-chibi's `password[16]` is:
/// `N - 1` bytes, since the NUL that ends them is one of them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Text<const N: usize> {
    bytes: [u8; N],
    len: usize,
}

impl<const N: usize> Default for Text<N> {
    fn default() -> Self {
        Self {
            bytes: [0; N],
            len: 0,
        }
    }
}

impl<const N: usize> Text<N> {
    /// The bytes it holds, which a write cut at the first NUL.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

/// A string too long for the cell leaves it empty, which no write can match.
impl<const N: usize> From<&str> for Text<N> {
    fn from(value: &str) -> Self {
        Self::decode(value.as_bytes()).unwrap_or_default()
    }
}

impl<const N: usize> Scalar for Text<N> {
    const KIND: rpc::Kind = rpc::Kind::String;

    fn decode(args: &[u8]) -> Result<Self, RpcError> {
        let len = args
            .iter()
            .position(|&byte| byte == 0)
            .unwrap_or(args.len());
        if len >= N {
            return Err(RpcError::ArgsSize);
        }
        let mut text = Self::default();
        text.bytes[..len].copy_from_slice(&args[..len]);
        text.len = len;
        Ok(text)
    }

    fn encode(self, out: &mut Reply) -> Result<(), RpcError> {
        put(out, self.as_bytes())
    }
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

/// Whether a call left the setting holding a value to announce.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Changed {
    /// Nothing to announce: the RPC read, or a load found the value held.
    Unchanged,
    /// A value to announce.
    Changed,
}

/// A setting a device keeps across a reboot, stored under its own name.
pub trait Persisted {
    /// The name its RPC and its stored entry carry.
    fn name(&self) -> &'static str;

    /// Encode the value as its RPC replies it, and take it as stored.
    fn save(&mut self, out: &mut Reply) -> Result<(), RpcError>;

    /// Take a stored value through the setting's check, and encode what it
    /// now holds.
    fn load(&mut self, value: &[u8], out: &mut Reply) -> Result<Changed, RpcError>;
}

/// One value an RPC property reads and writes.
pub struct Setting<T: Scalar> {
    name: &'static str,
    value: T,
    initial: T,
    access: Access,
    check: fn(T) -> Result<T, RpcError>,
}

impl<T: Scalar> Setting<T> {
    /// A setting booting at `initial`, accepting every write.
    pub const fn new(name: &'static str, initial: T) -> Self {
        Self {
            name,
            value: initial,
            initial,
            access: Access::RW,
            check: Ok,
        }
    }

    /// The same setting, taking only the values `check` passes.
    pub const fn checked(self, check: fn(T) -> Result<T, RpcError>) -> Self {
        Self { check, ..self }
    }

    /// The same setting, saved to flash and taken back from it at a boot.
    pub const fn persistent(self) -> Self {
        Self {
            access: self.access.union(Access::PERSISTENT),
            ..self
        }
    }

    /// The same setting, callable but left out of what a locked session lists.
    pub const fn hidden(self) -> Self {
        Self {
            access: self.access.union(Access::HIDDEN),
            ..self
        }
    }

    /// The same setting, reached only by a session `dev.priv` has unlocked.
    pub const fn privileged(self) -> Self {
        Self {
            access: self.access.privileged(),
            ..self
        }
    }

    /// The name its RPC and its announcements carry.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// The value it holds.
    pub fn get(&self) -> T {
        self.value
    }

    /// Go back to the value a boot starts from, stored or not.
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
        RpcSpec::prop(self.name, T::KIND, self.access)
    }
}

impl<T: Scalar> Persisted for Setting<T> {
    fn name(&self) -> &'static str {
        self.name
    }

    fn save(&mut self, out: &mut Reply) -> Result<(), RpcError> {
        self.value.encode(out)
    }

    fn load(&mut self, value: &[u8], out: &mut Reply) -> Result<Changed, RpcError> {
        let value = (self.check)(T::decode(value)?)?;
        let changed = match value == self.value {
            true => Changed::Unchanged,
            false => Changed::Changed,
        };
        self.value = value;
        self.value.encode(out)?;
        Ok(changed)
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
    fn a_setting_saves_what_it_holds_and_loads_what_was_stored() {
        let mut gain = Setting::new("app.gain", 3u16).persistent();
        answer(&mut gain, &7u16.to_le_bytes()).unwrap();

        let mut out = Reply::new();
        gain.save(&mut out).unwrap();
        assert_eq!(out.to_vec(), vec![7, 0]);

        let mut out = Reply::new();
        assert_eq!(
            gain.load(&9u16.to_le_bytes(), &mut out),
            Ok(Changed::Changed)
        );
        assert_eq!(out.to_vec(), vec![9, 0]);
        assert_eq!(
            gain.load(&9u16.to_le_bytes(), &mut Reply::new()),
            Ok(Changed::Unchanged)
        );
        assert_eq!(gain.load(&[9], &mut Reply::new()), Err(RpcError::ArgsSize));

        gain.reset();
        assert_eq!(gain.get(), 3);
    }

    #[test]
    fn a_stored_value_a_check_refuses_leaves_the_one_held() {
        let mut rate = Setting::new("app.rate", 1.0f64)
            .checked(positive)
            .persistent();
        assert_eq!(
            rate.load(&(-1.0f64).to_le_bytes(), &mut Reply::new()),
            Err(RpcError::Invalid)
        );
        assert_eq!(rate.get(), 1.0);
    }

    #[test]
    fn a_developer_setting_declares_the_bits_that_hide_it() {
        let cal = Setting::new("imu.cal.x", 0.0f64).persistent().privileged();
        assert!(cal.spec().access.is_privileged());
        assert!(cal.spec().access.contains(Access::PERSISTENT));
        assert!(!cal.spec().access.visible(false));
        assert!(Setting::new("dev.secret", 0u8)
            .hidden()
            .spec()
            .access
            .visible(true));
        assert!(!Setting::new("dev.secret", 0u8)
            .hidden()
            .spec()
            .access
            .visible(false));
    }

    #[test]
    fn a_text_cell_holds_as_much_as_it_is_given() {
        let mut password = Setting::new("dev.priv.password", Text::<16>::from("895895"));
        assert_eq!(password.get().as_bytes(), b"895895");
        assert_eq!(
            answer(&mut password, b"hunter2"),
            Ok((Changed::Changed, b"hunter2".to_vec()))
        );
        assert_eq!(
            answer(&mut password, b"opensesame\0\0"),
            Ok((Changed::Changed, b"opensesame".to_vec())),
            "a write is cut at the first NUL, as tl-chibi cuts it"
        );
        assert_eq!(
            answer(&mut password, &[b'x'; 15]),
            Ok((Changed::Changed, vec![b'x'; 15]))
        );
        assert_eq!(
            answer(&mut password, &[b'x'; 16]),
            Err(RpcError::ArgsSize),
            "a cell of sixteen holds fifteen bytes and the NUL that ends them"
        );
        assert_eq!(password.get().as_bytes(), &[b'x'; 15]);
        assert_eq!(
            Text::<16>::from("a string too long for the cell").as_bytes(),
            b""
        );
    }

    #[test]
    fn a_spec_describes_the_property_the_setting_answers() {
        let spec = Setting::new("app.rate", 1.0f64).spec();
        assert_eq!(spec.method, Method::Prop);
        assert_eq!(spec.kind, Kind::Float(8));
        assert_eq!(spec.access, Access::RW);
        assert_eq!(Setting::new("app.enable", true).spec().kind, Kind::Bool);
        assert_eq!(
            Setting::new("app.rate", 1.0f64).persistent().spec().access,
            Access::RW | Access::PERSISTENT
        );

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
