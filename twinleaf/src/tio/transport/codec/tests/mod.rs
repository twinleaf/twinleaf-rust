/// The packets, streams and cuts the codec properties draw from.
///
/// [`packet`] draws a packet of any kind a link carries, half from the typed
/// constructors and half as wire bytes from [`raw`], each with a TTL of 0 to
/// 15. [`invalid_body`] draws a packet whose header decodes and whose payload
/// is not valid for its type, [`invalid_header`] a header that does not
/// decode, and [`not_one_packet`] the bytes of a frame or datagram that are
/// not exactly one packet, each with the error it gives. Every draw of these
/// names its class, and [`tally`] prints how often a run draws each class.
pub(crate) mod strategy;

/// The TTL, routing and payload size classes of the packet `raw`.
fn header(raw: &[u8]) -> Vec<&'static str> {
    let ttl = match raw[1] >> 4 {
        0..=1 => "TTL 0 or 1",
        2..=7 => "TTL 2 to 7",
        _ => "TTL 8 to 15",
    };
    let hops = match raw[1] & 0x0f {
        0 => "no hops",
        8 => "8 hops",
        _ => "1 to 7 hops",
    };
    let payload = match u16::from_le_bytes([raw[2], raw[3]]) {
        0 => "empty payload",
        500 => "500 byte payload",
        _ => "payload of 1 to 499 bytes",
    };
    vec![ttl, hops, payload]
}

/// Prints the share of each class the shared strategies draw, when
/// `TWINLEAF_CODEC_CLASSES` is set.
#[test]
fn class_counts() {
    strategy::tally("raw packets by type", strategy::raw(), |raw| {
        vec![raw.class]
    });
    strategy::tally("packets by header", strategy::packet(), |packet| {
        header(packet.as_bytes())
    });
    strategy::tally("invalid payloads", strategy::invalid_body(), |invalid| {
        let mut classes = vec![invalid.class];
        classes.extend(header(&invalid.bytes));
        classes
    });
    strategy::tally(
        "invalid headers",
        strategy::invalid_header(),
        |(bytes, _, class)| {
            vec![
                *class,
                ["1 byte", "2 bytes", "3 bytes", "4 bytes"][bytes.len() - 1],
            ]
        },
    );
    strategy::tally(
        "not one packet",
        strategy::not_one_packet(),
        |(_, _, class)| vec![*class],
    );
}
