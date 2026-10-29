//! One-shot mDNS queries, as RFC 6762 section 5.1 has them. A query sent
//! from a port other than 5353 asks for a unicast answer to that port, which
//! a responder sends at once, as section 6.7 has it. It is not held to the
//! second a responder waits before it multicasts the same answer again, and
//! the answer reaches this socket alone. On Windows, other mDNS stacks that
//! also hold port 5353 would take a unicast answer sent there.

use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::{timeout_at, Instant};

/// How long [`resolve`] waits for an answer to its query.
const WAIT: Duration = Duration::from_millis(50);

/// The IPv4 address and port of mDNS.
const GROUP: (Ipv4Addr, u16) = (Ipv4Addr::new(224, 0, 0, 251), 5353);

/// The record type and class of an IPv4 address.
const TYPE_A: u16 = 1;
const CLASS_IN: u16 = 1;

/// Asks for the IPv4 addresses of `name`, a `.local` name with its trailing
/// dot, with one query from a port of this socket's own on each IPv4
/// interface. Returns the addresses of the first answer within [`WAIT`],
/// each with `port`, or none.
pub(super) async fn resolve(name: &str, port: u16) -> Vec<SocketAddr> {
    match ask(name).await {
        Ok(found) => found
            .into_iter()
            .map(|ip| SocketAddr::from((ip, port)))
            .collect(),
        Err(error) => {
            log::debug!("no one-shot mDNS query for {name}: {error}");
            Vec::new()
        }
    }
}

/// Sends the query for `name` and returns the addresses of the first answer
/// within [`WAIT`].
async fn ask(name: &str) -> io::Result<Vec<Ipv4Addr>> {
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
    let id = query_id();
    let query = query(name, id)?;
    let mut sent = false;
    for interface in if_addrs::get_if_addrs()? {
        let if_addrs::IfAddr::V4(v4) = interface.addr else {
            continue;
        };
        let socket_ref = socket2::SockRef::from(&socket);
        // An interface that is down or takes no multicast is skipped.
        if socket_ref.set_multicast_if_v4(&v4.ip).is_ok()
            && socket.send_to(&query, GROUP).await.is_ok()
        {
            sent = true;
        }
    }
    if !sent {
        return Ok(Vec::new());
    }
    let deadline = Instant::now() + WAIT;
    let mut buffer = vec![0; 9000];
    loop {
        let Ok(received) = timeout_at(deadline, socket.recv_from(&mut buffer)).await else {
            return Ok(Vec::new());
        };
        let Ok((len, _)) = received else {
            // Windows reports a port unreachable reply to an earlier send on
            // the next receive.
            continue;
        };
        let found = addresses(&buffer[..len], name);
        if !found.is_empty() {
            return Ok(found);
        }
    }
}

/// Returns an id for a query, which a responder repeats in its answer.
fn query_id() -> u16 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.subsec_nanos());
    (nanos as u16) ^ (std::process::id() as u16)
}

/// Returns the query for the IPv4 addresses of `name`, with `id`.
fn query(name: &str, id: u16) -> io::Result<Vec<u8>> {
    let mut query = Vec::with_capacity(name.len() + 18);
    query.extend(id.to_be_bytes());
    // No flags, one question, and no records.
    query.extend([0, 0, 0, 1, 0, 0, 0, 0, 0, 0]);
    for label in name.trim_end_matches('.').split('.') {
        let len = u8::try_from(label.len())
            .ok()
            .filter(|len| (1..64).contains(len))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{name} has a label no query can carry"),
                )
            })?;
        query.push(len);
        query.extend(label.as_bytes());
    }
    query.push(0);
    query.extend(TYPE_A.to_be_bytes());
    query.extend(CLASS_IN.to_be_bytes());
    Ok(query)
}

/// Returns the IPv4 addresses that the records of `reply` give `name`. A
/// reply that does not parse gives none. The id is not checked, as some
/// responders do not repeat it, and only an answer to this socket's own
/// query reaches it.
fn addresses(reply: &[u8], name: &str) -> Vec<Ipv4Addr> {
    let wanted = name.trim_end_matches('.');
    let mut found = Vec::new();
    let Some(header) = reply.get(..12) else {
        return found;
    };
    // An answer has the QR bit set.
    if header[2] & 0x80 == 0 {
        return found;
    }
    let count = |at: usize| usize::from(u16::from_be_bytes([header[at], header[at + 1]]));
    let mut at = 12;
    for _ in 0..count(4) {
        let Some((_, next)) = read_name(reply, at) else {
            return found;
        };
        at = next + 4;
    }
    for _ in 0..count(6) + count(8) + count(10) {
        let Some((record, next)) = read_name(reply, at) else {
            return found;
        };
        let Some(fixed) = reply.get(next..next + 10) else {
            return found;
        };
        let rtype = u16::from_be_bytes([fixed[0], fixed[1]]);
        let length = usize::from(u16::from_be_bytes([fixed[8], fixed[9]]));
        let data = next + 10;
        let Some(rdata) = reply.get(data..data + length) else {
            return found;
        };
        if rtype == TYPE_A && length == 4 && record.eq_ignore_ascii_case(wanted) {
            found.push(Ipv4Addr::new(rdata[0], rdata[1], rdata[2], rdata[3]));
        }
        at = data + length;
    }
    found
}

/// Reads the name at `at` of `packet`, following compression pointers.
/// Returns the name and where the record goes on, or `None` if the name runs
/// past the packet or points in a loop.
fn read_name(packet: &[u8], mut at: usize) -> Option<(String, usize)> {
    let mut labels = Vec::new();
    let mut end = None;
    // A name of the most labels a packet holds needs fewer steps than this.
    for _ in 0..128 {
        let len = usize::from(*packet.get(at)?);
        if len == 0 {
            return Some((labels.join("."), end.unwrap_or(at + 1)));
        }
        if len & 0xC0 == 0xC0 {
            let low = usize::from(*packet.get(at + 1)?);
            end.get_or_insert(at + 2);
            at = ((len & 0x3F) << 8) | low;
            continue;
        }
        let label = packet.get(at + 1..at + 1 + len)?;
        labels.push(String::from_utf8_lossy(label).into_owned());
        at += 1 + len;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reply a responder gives the query for `name`, with one address in
    /// its answer, the name compressed to point back at the question.
    fn reply(name: &str, ip: [u8; 4]) -> Vec<u8> {
        let mut reply = query(name, 7).unwrap();
        reply[2] = 0x84;
        reply[7] = 1;
        reply.extend([0xC0, 12]);
        reply.extend(TYPE_A.to_be_bytes());
        reply.extend(CLASS_IN.to_be_bytes());
        reply.extend(10u32.to_be_bytes());
        reply.extend(4u16.to_be_bytes());
        reply.extend(ip);
        reply
    }

    #[test]
    fn an_answer_gives_the_address_of_its_name() {
        let answer = reply("fakehub.local.", [192, 168, 1, 20]);

        assert_eq!(
            addresses(&answer, "fakehub.local."),
            [Ipv4Addr::new(192, 168, 1, 20)]
        );
        assert_eq!(
            addresses(&answer, "FAKEHUB.local."),
            addresses(&answer, "fakehub.local.")
        );
        assert!(addresses(&answer, "other.local.").is_empty());
    }

    /// A query, a reply cut short, and a name that points at itself give no
    /// address, and nothing panics.
    #[test]
    fn what_is_no_answer_gives_no_address() {
        let answer = reply("fakehub.local.", [192, 168, 1, 20]);
        assert!(addresses(&query("fakehub.local.", 7).unwrap(), "fakehub.local.").is_empty());
        for cut in 0..answer.len() {
            assert!(addresses(&answer[..cut], "fakehub.local.").is_empty());
        }
        let mut looping = answer.clone();
        let at = looping.len() - 16;
        looping[at..at + 2].copy_from_slice(&[0xC0, at as u8]);
        assert!(addresses(&looping, "fakehub.local.").is_empty());
    }

    #[test]
    fn a_name_whose_label_no_query_carries_is_refused() {
        assert!(query("a..local.", 1).is_err());
        assert!(query(&format!("{}.local.", "x".repeat(64)), 1).is_err());
    }
}
