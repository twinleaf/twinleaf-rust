//! Ids carried in packets and RPC replies.

/// RPC transaction id, matching a reply or error to its request. Every `u16`
/// value is valid; hosts may wrap the counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct RpcRequestId(u16);

impl RpcRequestId {
    /// Request id from its value.
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    /// The id as a `u16`.
    pub const fn value(self) -> u16 {
        self.0
    }

    /// Request id from its two wire bytes.
    pub const fn from_le_bytes(bytes: [u8; 2]) -> Self {
        Self(u16::from_le_bytes(bytes))
    }

    /// The two wire bytes, little endian.
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
    /// Largest method id, 0x7FFF.
    pub const MAX: u16 = 0x7fff;

    /// Panics for a value with the by-name bit set; in a `const` context this
    /// makes an invalid table id a compile error.
    pub const fn new(value: u16) -> Self {
        assert!(value <= Self::MAX, "RPC method id has the by-name bit set");
        Self(value)
    }

    /// Method id from its value. None above [`MAX`](Self::MAX).
    pub const fn try_new(value: u16) -> Option<Self> {
        if value <= Self::MAX {
            Some(Self(value))
        } else {
            None
        }
    }

    /// The id as a `u16`.
    pub const fn value(self) -> u16 {
        self.0
    }

    /// Method id from its two wire bytes. None if the by-name bit is set.
    pub const fn from_le_bytes(bytes: [u8; 2]) -> Option<Self> {
        Self::try_new(u16::from_le_bytes(bytes))
    }

    /// The two wire bytes, little endian.
    pub const fn to_le_bytes(self) -> [u8; 2] {
        self.0.to_le_bytes()
    }
}

/// Session id, chosen at boot and reported by `dev.session` and every heartbeat.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct SessionId(u32);

impl SessionId {
    /// Session id from its value.
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// The id as a `u32`.
    pub const fn value(self) -> u32 {
        self.0
    }

    /// Session id from its four wire bytes.
    pub const fn from_le_bytes(bytes: [u8; 4]) -> Self {
        Self(u32::from_le_bytes(bytes))
    }

    /// Wire form used by the heartbeat payload and the dev.session reply.
    pub const fn to_le_bytes(self) -> [u8; 4] {
        self.0.to_le_bytes()
    }
}

impl core::fmt::Display for SessionId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// TIO data-stream id. Stream 0 is reserved for the legacy 32-bit sample
/// format; current metadata and sample packets use ids 1 through 127.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct StreamId(u8);

impl StreamId {
    /// First stream id, 1.
    pub const MIN: u8 = 1;
    /// Last stream id, 127.
    pub const MAX: u8 = 127;

    /// Stream id from its value. Panics outside 1 to 127.
    pub const fn new(value: u8) -> Self {
        assert!(
            value >= Self::MIN && value <= Self::MAX,
            "stream id out of range"
        );
        Self(value)
    }

    /// Stream id from its value. None outside 1 to 127.
    pub const fn try_new(value: u8) -> Option<Self> {
        if value >= Self::MIN && value <= Self::MAX {
            Some(Self(value))
        } else {
            None
        }
    }

    /// The id as a `u8`.
    pub const fn value(self) -> u8 {
        self.0
    }
}

impl core::fmt::Display for StreamId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Id of a segment within a stream's segment ring.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct SegmentId(u8);

impl SegmentId {
    /// Segment id from its value.
    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    /// The id as a `u8`.
    pub const fn value(self) -> u8 {
        self.0
    }
}

impl core::fmt::Display for SegmentId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Id of a column within one stream schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct ColumnId(u8);

impl ColumnId {
    /// Column id from its value.
    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    /// The id as a `u8`.
    pub const fn value(self) -> u8 {
        self.0
    }

    /// The column position in the stream schema.
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}

impl core::fmt::Display for ColumnId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Sample counter within one segment. Current stream packets encode 24 bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct SampleNumber(u32);

impl SampleNumber {
    /// Largest sample number in a stream packet, 2^24 - 1.
    pub const MAX: u32 = (1 << 24) - 1;

    /// Sample number from its value.
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// The number as a `u32`.
    pub const fn value(self) -> u32 {
        self.0
    }

    /// Whether the number fits the 24-bit stream packet field.
    pub const fn fits_stream_packet(self) -> bool {
        self.0 <= Self::MAX
    }

    /// Distance from `other`, wrapping at 2^32.
    pub const fn wrapping_sub(self, other: Self) -> u32 {
        self.0.wrapping_sub(other.0)
    }

    /// Four little endian bytes.
    pub const fn to_le_bytes(self) -> [u8; 4] {
        self.0.to_le_bytes()
    }
}

impl core::fmt::Display for SampleNumber {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Numeric comparisons stay convenient without making sample numbers
/// implicitly interchangeable with arbitrary `u32` function arguments.
impl PartialEq<u32> for SampleNumber {
    fn eq(&self, other: &u32) -> bool {
        self.0 == *other
    }
}

/// Hardware revision used by `dev.revision` and firmware packages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct HwRev(u16);

impl HwRev {
    /// Lowest revision, 1.
    pub const MIN: u16 = 1;
    /// Highest revision, 99.
    pub const MAX: u16 = 99;

    /// Panics when out of range; in a `const` context that is a compile error.
    pub const fn new(value: u16) -> Self {
        assert!(
            value >= Self::MIN && value <= Self::MAX,
            "hardware revision out of range"
        );
        Self(value)
    }

    /// Revision from its value. None outside 1 to 99.
    pub const fn try_new(value: u16) -> Option<Self> {
        if value >= Self::MIN && value <= Self::MAX {
            Some(Self(value))
        } else {
            None
        }
    }

    /// Revision from decimal text. Panics unless it is a number from 1 to 99.
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

    /// The revision as a `u16`.
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
    /// Serial from its text.
    pub const fn new(value: T) -> Self {
        Self(value)
    }
}

impl<T: AsRef<str>> DeviceSerial<T> {
    /// The serial text.
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

impl<T: AsRef<str>> core::fmt::Display for DeviceSerial<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Firmware build serial, reported by `dev.firmware.serial`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FirmwareSerial<T>(T);

impl<T> FirmwareSerial<T> {
    /// Serial from its text.
    pub const fn new(value: T) -> Self {
        Self(value)
    }
}

impl<T: AsRef<str>> FirmwareSerial<T> {
    /// The serial text.
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
    /// Board id from its eight bytes.
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

    /// The eight bytes.
    pub const fn as_bytes(&self) -> &[u8; 8] {
        &self.0
    }

    /// The eight bytes by value.
    pub const fn to_bytes(self) -> [u8; 8] {
        self.0
    }
}

/// Eight-byte format discriminator at the start of a firmware package.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FirmwareMagic([u8; 8]);

impl FirmwareMagic {
    /// Magic from its eight bytes.
    pub const fn new(bytes: [u8; 8]) -> Self {
        Self(bytes)
    }

    /// The eight bytes.
    pub const fn as_bytes(&self) -> &[u8; 8] {
        &self.0
    }

    /// The eight bytes by value.
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
