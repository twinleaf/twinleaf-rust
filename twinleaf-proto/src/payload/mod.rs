//! The payload codecs, one module for each kind of payload a packet carries.

pub mod capture;
pub mod data;
pub mod heartbeat;
pub mod log;
pub mod proxy;
pub mod rpc;
pub mod settings;
pub mod sync;
pub mod update;

/// Returns the `N` bytes of `bytes` from offset `at`, or `None` if they run
/// past the end.
pub(crate) fn array_at<const N: usize>(bytes: &[u8], at: usize) -> Option<[u8; N]> {
    bytes.get(at..)?.first_chunk().copied()
}

/// Lengths, words, text and buffer sizes the payload properties draw from,
/// most of them at the edges of their range, and the check every write goes
/// through.
#[cfg(test)]
pub(crate) mod strategy {
    use proptest::prelude::*;
    use proptest::test_runner::TestCaseError;

    use crate::packet::WriteError;

    /// A length from 0 to `max`. Four in five are 0, 1, `max - 1` or `max`.
    pub(crate) fn len_to(max: usize) -> impl Strategy<Value = usize> {
        prop_oneof![
            Just(0),
            Just(1.min(max)),
            Just(max.saturating_sub(1)),
            Just(max),
            0..=max,
        ]
    }

    /// Bytes of any value, `len` of them.
    pub(crate) fn bytes(len: usize) -> impl Strategy<Value = Vec<u8>> {
        prop::collection::vec(any::<u8>(), len)
    }

    /// Bytes of any value, as many as [`len_to`] draws.
    pub(crate) fn bytes_to(max: usize) -> impl Strategy<Value = Vec<u8>> {
        len_to(max).prop_flat_map(bytes)
    }

    /// A byte of any value. Two in three are 0 or 0xFF.
    pub(crate) fn octet() -> impl Strategy<Value = u8> {
        prop_oneof![Just(0), Just(u8::MAX), any::<u8>()]
    }

    /// A word of any value. Two in three are 0 or all ones.
    pub(crate) fn word() -> impl Strategy<Value = u32> {
        prop_oneof![Just(0), Just(u32::MAX), any::<u32>()]
    }

    /// A half word of any value. Two in three are 0 or all ones.
    pub(crate) fn half() -> impl Strategy<Value = u16> {
        prop_oneof![Just(0), Just(u16::MAX), any::<u16>()]
    }

    /// An `f32` of any bit pattern, compared by its bits. Half are positive
    /// or negative zero, an infinity, a quiet or signalling NaN, or the NaN
    /// of all ones.
    pub(crate) fn float() -> impl Strategy<Value = f32> {
        prop_oneof![
            1 => Just(0),
            1 => Just(0x8000_0000),
            1 => Just(0x7F80_0000),
            1 => Just(0xFF80_0000),
            1 => Just(0x7FC0_0000),
            1 => Just(0x7F80_0001),
            1 => Just(u32::MAX),
            7 => any::<u32>(),
        ]
        .prop_map(f32::from_bits)
    }

    /// The longest prefix of `text` that is at most `max` bytes and ends on
    /// a character boundary.
    pub(crate) fn cut(text: &str, max: usize) -> &str {
        let mut end = text.len().min(max);
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        &text[..end]
    }

    /// The prefixes a record writer keeps of `strings` given `room` bytes
    /// after its head. Each string in turn is cut to what is left of the room
    /// and to the 255 bytes its length byte measures, on a character boundary.
    pub(crate) fn cut_in_turn<'a>(strings: &[&'a str], room: usize) -> Vec<&'a str> {
        let mut left = room;
        strings
            .iter()
            .map(|text| {
                let kept = cut(text, left.min(u8::MAX as usize));
                left -= kept.len();
                kept
            })
            .collect()
    }

    /// The strings of a record, read from its bytes alone, or why the record
    /// is refused. The first byte is the head length, which must be at least
    /// `min_head` and within the record. `offsets` are where the head holds
    /// the length of each string, in the order the strings follow the head,
    /// and a length the head does not reach is zero. Each string must fit the
    /// bytes left in turn and be UTF-8.
    pub(crate) fn strings_of<'a>(
        record: &'a [u8],
        min_head: usize,
        offsets: &[usize],
    ) -> Result<Vec<&'a str>, &'static str> {
        let head = record.first().map_or(0, |&head| usize::from(head));
        if head < min_head || head > record.len() {
            return Err("refused, head length");
        }
        let mut at = head;
        let mut strings = Vec::new();
        for &offset in offsets {
            let len = record.get(offset).filter(|_| offset < head);
            let len = len.map_or(0, |&len| usize::from(len));
            let Some(bytes) = record.get(at..at + len) else {
                return Err("refused, string past the end");
            };
            let Ok(text) = core::str::from_utf8(bytes) else {
                return Err("refused, string not UTF-8");
            };
            strings.push(text);
            at += len;
        }
        Ok(strings)
    }

    /// Text of up to `max` bytes. Half are ASCII of a length [`len_to`]
    /// draws. A quarter are any characters cut to `max` bytes on a character
    /// boundary. A quarter are up to three ASCII bytes and then one character
    /// of two, three or four bytes repeated as often as `max` allows, so a
    /// limit near `max` falls inside a character.
    pub(crate) fn text(max: usize) -> BoxedStrategy<String> {
        prop_oneof![
            2 => len_to(max)
                .prop_flat_map(|len| prop::collection::vec(0..=0x7Fu8, len))
                .prop_map(|ascii| String::from_utf8(ascii).unwrap()),
            1 => "\\PC{0,128}".prop_map(move |text| cut(&text, max).to_owned()),
            1 => (0..=3usize, prop::sample::select(vec!['é', '€', '𝄞'])).prop_map(
                move |(ascii, wide)| {
                    let mut text = "a".repeat(ascii.min(max));
                    while text.len() + wide.len_utf8() <= max {
                        text.push(wide);
                    }
                    text
                }
            ),
        ]
        .boxed()
    }

    /// Runs `write` on a buffer of `len` zero bytes and on one of `len` 0xFF
    /// bytes, and fails the case unless both return the same length, hold
    /// the same bytes up to it, and still hold their fill past it. A byte the
    /// write leaves alone differs between the two fills, so this shows that
    /// the write sets every byte up to its length and none past it. Two fills
    /// cannot show every way a write might read the old bytes. A refused
    /// write is not checked. Returns the length and the zeroed buffer.
    pub(crate) fn write_over_any(
        len: usize,
        write: impl Fn(&mut [u8]) -> Result<usize, WriteError>,
    ) -> Result<(Result<usize, WriteError>, Vec<u8>), TestCaseError> {
        let mut zeroes = vec![0; len];
        let mut ones = vec![u8::MAX; len];
        let written = write(&mut zeroes);
        prop_assert_eq!(write(&mut ones), written);
        let Ok(end) = written else {
            return Ok((written, zeroes));
        };
        prop_assert_eq!(
            &zeroes[..end],
            &ones[..end],
            "a byte up to the length was left as the buffer held it"
        );
        prop_assert!(
            zeroes[end..].iter().all(|&byte| byte == 0)
                && ones[end..].iter().all(|&byte| byte == u8::MAX),
            "a byte past the length was written"
        );
        Ok((written, zeroes))
    }

    /// How far a buffer is from the length a write needs, up to eight bytes
    /// either way. Three in four are one short, exact, or one over.
    pub(crate) fn slack() -> impl Strategy<Value = isize> {
        prop_oneof![Just(-1), Just(0), Just(1), -8..=8isize]
    }

    /// A zeroed buffer `slack` bytes longer than `needed`, or empty if that
    /// is below zero.
    pub(crate) fn buffer(needed: usize, slack: isize) -> Vec<u8> {
        vec![0; needed.saturating_add_signed(slack)]
    }
}
