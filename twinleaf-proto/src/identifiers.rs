//! Typed identifiers shared by Twinleaf hosts and devices.

/// RPC transaction id, matching a reply or error to its request. Every `u16`
/// value is valid; hosts may wrap the counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct RpcRequestId(u16);

impl RpcRequestId {
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u16 {
        self.0
    }

    pub const fn from_le_bytes(bytes: [u8; 2]) -> Self {
        Self(u16::from_le_bytes(bytes))
    }

    pub const fn to_le_bytes(self) -> [u8; 2] {
        self.0.to_le_bytes()
    }
}

/// Numeric RPC method id. Bit 15 is not part of the id space: on the wire it
/// marks a request whose method is encoded by name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct RpcMethodId(u16);

impl RpcMethodId {
    pub const MAX: u16 = 0x7fff;

    /// Panics for a value with the by-name bit set; in a `const` context this
    /// makes an invalid table id a compile error.
    pub const fn new(value: u16) -> Self {
        assert!(value <= Self::MAX, "RPC method id has the by-name bit set");
        Self(value)
    }

    pub const fn try_new(value: u16) -> Option<Self> {
        if value <= Self::MAX {
            Some(Self(value))
        } else {
            None
        }
    }

    pub const fn value(self) -> u16 {
        self.0
    }

    pub const fn from_le_bytes(bytes: [u8; 2]) -> Option<Self> {
        Self::try_new(u16::from_le_bytes(bytes))
    }

    pub const fn to_le_bytes(self) -> [u8; 2] {
        self.0.to_le_bytes()
    }
}

/// Per-boot random session id, reported by `dev.session` and every heartbeat so
/// hosts detect device restarts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct SessionId(u32);

impl SessionId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u32 {
        self.0
    }

    pub const fn from_le_bytes(bytes: [u8; 4]) -> Self {
        Self(u32::from_le_bytes(bytes))
    }

    /// Wire form used by the heartbeat payload and the dev.session reply.
    pub const fn to_le_bytes(self) -> [u8; 4] {
        self.0.to_le_bytes()
    }
}

/// Hardware revision used by `dev.revision` and firmware packages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct HwRev(u16);

impl HwRev {
    pub const MIN: u16 = 1;
    pub const MAX: u16 = 99;

    /// Panics when out of range; in a `const` context that is a compile error.
    pub const fn new(value: u16) -> Self {
        assert!(
            value >= Self::MIN && value <= Self::MAX,
            "hardware revision out of range"
        );
        Self(value)
    }

    pub const fn try_new(value: u16) -> Option<Self> {
        if value >= Self::MIN && value <= Self::MAX {
            Some(Self(value))
        } else {
            None
        }
    }

    /// Parse a decimal string such as `env!("ETHAN_HW_REV")`; in a `const`
    /// context a malformed or out-of-range value fails the build.
    pub const fn parse(s: &str) -> Self {
        let bytes = s.as_bytes();
        assert!(!bytes.is_empty(), "hardware revision is empty");
        let mut value: u16 = 0;
        let mut i = 0;
        while i < bytes.len() {
            assert!(
                bytes[i].is_ascii_digit(),
                "hardware revision must be decimal digits"
            );
            value = value * 10 + (bytes[i] - b'0') as u16;
            assert!(value <= Self::MAX, "hardware revision out of range");
            i += 1;
        }
        Self::new(value)
    }

    pub const fn value(self) -> u16 {
        self.0
    }

    /// Wire form used by the dev.revision reply and the package header.
    pub const fn to_le_bytes(self) -> [u8; 2] {
        self.0.to_le_bytes()
    }
}

/// Prints the bare number, so `write!("R{rev}")` forms "R1".
impl core::fmt::Display for HwRev {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Opaque `dev.serial` text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeviceSerial<T>(T);

impl<T> DeviceSerial<T> {
    pub const fn new(value: T) -> Self {
        Self(value)
    }
}

impl<T: AsRef<str>> DeviceSerial<T> {
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

impl<T: AsRef<str>> core::fmt::Display for DeviceSerial<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Firmware build serial as exposed by `dev.firmware.serial` and the final
/// `[...]` section of `dev.desc`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FirmwareSerial<T>(T);

impl<T> FirmwareSerial<T> {
    pub const fn new(value: T) -> Self {
        Self(value)
    }
}

impl<T: AsRef<str>> FirmwareSerial<T> {
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

impl<T: AsRef<str>> core::fmt::Display for FirmwareSerial<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Eight-byte board discriminator in a signed firmware package.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BoardId([u8; 8]);

impl BoardId {
    pub const fn from_bytes(bytes: [u8; 8]) -> Self {
        Self(bytes)
    }

    /// Build a NUL-padded id from 1..=8 printable ASCII bytes.
    pub const fn from_ascii(value: &str) -> Self {
        let input = value.as_bytes();
        assert!(!input.is_empty(), "board id is empty");
        assert!(input.len() <= 8, "board id is longer than 8 bytes");
        let mut bytes = [0; 8];
        let mut i = 0;
        while i < input.len() {
            assert!(
                input[i] >= 0x21 && input[i] <= 0x7e,
                "board id must be printable ASCII"
            );
            bytes[i] = input[i];
            i += 1;
        }
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 8] {
        &self.0
    }

    pub const fn to_bytes(self) -> [u8; 8] {
        self.0
    }
}

/// Eight-byte format discriminator at the start of a firmware package.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FirmwareMagic([u8; 8]);

impl FirmwareMagic {
    pub const fn new(bytes: [u8; 8]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 8] {
        &self.0
    }

    pub const fn to_bytes(self) -> [u8; 8] {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_decimal_revisions() {
        assert_eq!(HwRev::parse("1"), HwRev::new(1));
        assert_eq!(HwRev::parse("99"), HwRev::new(99));
    }

    #[test]
    fn rejects_out_of_range_and_garbage() {
        assert!(HwRev::try_new(0).is_none());
        assert!(HwRev::try_new(100).is_none());
        for s in ["", "0", "100", "R1", "1.5", "-1"] {
            assert!(std::panic::catch_unwind(|| HwRev::parse(s)).is_err());
        }
    }

    #[test]
    fn wire_forms_are_little_endian() {
        assert_eq!(HwRev::new(2).to_le_bytes(), [2, 0]);
        assert_eq!(
            SessionId::new(0x1234_5678).to_le_bytes(),
            [0x78, 0x56, 0x34, 0x12]
        );
        assert_eq!(RpcRequestId::new(0x1234).to_le_bytes(), [0x34, 0x12]);
        assert_eq!(RpcMethodId::new(0x1234).to_le_bytes(), [0x34, 0x12]);
    }

    #[test]
    fn method_id_excludes_the_by_name_bit() {
        assert_eq!(RpcMethodId::try_new(0x7fff), Some(RpcMethodId::new(0x7fff)));
        assert!(RpcMethodId::try_new(0x8000).is_none());
    }

    #[test]
    fn board_id_is_nul_padded() {
        assert_eq!(BoardId::from_ascii("ETHAN").to_bytes(), *b"ETHAN\0\0\0");
        assert_eq!(BoardId::from_ascii("12345678").to_bytes(), *b"12345678");
    }

    #[test]
    fn serial_types_do_not_invent_a_wire_format() {
        assert_eq!(
            DeviceSerial::new("COMM-ETH.00123").as_str(),
            "COMM-ETH.00123"
        );
        assert_eq!(DeviceSerial::new("37383731").as_str(), "37383731");
        assert_eq!(
            FirmwareSerial::new("2026-07-22/abc123-DEV").as_str(),
            "2026-07-22/abc123-DEV"
        );
    }
}
