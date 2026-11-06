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
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{timeout_at, Instant};

/// How long [`resolve`] waits for an answer to its query.
const WAIT: Duration = Duration::from_millis(50);

/// How long [`browse`] waits for the answers to its queries and those it asks after.
const BROWSE_WAIT: Duration = Duration::from_millis(400);

/// The IPv4 address and port of mDNS.
const GROUP: (Ipv4Addr, u16) = (Ipv4Addr::new(224, 0, 0, 251), 5353);

/// The record types of an IPv4 address, a pointer and a service, and the class of each.
const TYPE_A: u16 = 1;
const TYPE_PTR: u16 = 12;
const TYPE_SRV: u16 = 33;
const CLASS_IN: u16 = 1;

/// A service instance that answers advertise, with the IPv4 addresses of its host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Advertised {
    /// The service type asked for, such as `_twinleaf._tcp.local.`.
    pub(crate) service: String,
    /// The instance's name with its service type and trailing dot.
    pub(crate) fullname: String,
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) v4: Vec<Ipv4Addr>,
}

/// A record of an answer that [`browse`] or [`resolve`] reads, its names without a trailing dot.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Record {
    Ptr {
        name: String,
        target: String,
    },
    Srv {
        name: String,
        port: u16,
        target: String,
    },
    A {
        name: String,
        ip: Ipv4Addr,
    },
}

impl Record {
    /// Returns the instance this record points `service` at.
    fn instance_of(&self, service: &str) -> Option<&str> {
        match self {
            Record::Ptr { name, target } if same(name, service) => Some(target),
            Record::Ptr { .. } | Record::Srv { .. } | Record::A { .. } => None,
        }
    }

    /// Returns the port and host this record gives `instance`.
    fn service_of(&self, instance: &str) -> Option<(u16, &str)> {
        match self {
            Record::Srv { name, port, target } if same(name, instance) => Some((*port, target)),
            Record::Ptr { .. } | Record::Srv { .. } | Record::A { .. } => None,
        }
    }

    /// Returns the IPv4 address this record gives `host`.
    fn address_of(&self, host: &str) -> Option<Ipv4Addr> {
        match self {
            Record::A { name, ip } if same(name, host) => Some(*ip),
            Record::Ptr { .. } | Record::Srv { .. } | Record::A { .. } => None,
        }
    }
}

/// Returns `true` if `a` and `b` are one name, in any case and with or without a trailing dot.
fn same(a: &str, b: &str) -> bool {
    a.trim_end_matches('.')
        .eq_ignore_ascii_case(b.trim_end_matches('.'))
}

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
    if !send(&socket, &query(name, TYPE_A, query_id())?).await {
        return Ok(Vec::new());
    }
    let deadline = Instant::now() + WAIT;
    let mut buffer = vec![0; 9000];
    while let Some(reply) = receive(&socket, deadline, &mut buffer).await {
        let found: Vec<_> = records(reply)
            .iter()
            .filter_map(|record| record.address_of(name))
            .collect();
        if !found.is_empty() {
            return Ok(found);
        }
    }
    Ok(Vec::new())
}

/// Asks for the instances of each of `services`, each with its trailing dot, with one query
/// from a port of this socket's own on each IPv4 interface, as [`resolve`] asks. Sends `heard`
/// each instance once the answers give its host and addresses, asking in turn for a host or
/// address they leave out, and stops after [`BROWSE_WAIT`].
pub(crate) async fn browse(services: &[&str], heard: UnboundedSender<Advertised>) {
    if let Err(error) = listen(services, &heard).await {
        log::debug!("no one-shot mDNS browse: {error}");
    }
}

/// Sends the queries of [`browse`] and sends `heard` what their answers advertise.
async fn listen(services: &[&str], heard: &UnboundedSender<Advertised>) -> io::Result<()> {
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
    let id = query_id();
    for service in services {
        send(&socket, &query(service, TYPE_PTR, id)?).await;
    }
    let deadline = Instant::now() + BROWSE_WAIT;
    let mut buffer = vec![0; 9000];
    let (mut known, mut asked, mut told) = (Vec::new(), Vec::new(), Vec::new());
    while let Some(reply) = receive(&socket, deadline, &mut buffer).await {
        let fresh: Vec<_> = records(reply)
            .into_iter()
            .filter(|record| !known.contains(record))
            .collect();
        known.extend(fresh);
        for found in instances(services, &known) {
            match found {
                Ok(advertised) if told.contains(&advertised) => {}
                Ok(advertised) => {
                    if heard.send(advertised.clone()).is_err() {
                        return Ok(());
                    }
                    told.push(advertised);
                }
                Err(missing) if asked.contains(&missing) => {}
                Err(missing) => {
                    send(&socket, &query(&missing.0, missing.1, id)?).await;
                    asked.push(missing);
                }
            }
        }
    }
    Ok(())
}

/// Returns each instance that `known` points one of `services` at, or the name and record
/// type to ask for next where `known` leaves out its host or its addresses.
fn instances(services: &[&str], known: &[Record]) -> Vec<Result<Advertised, (String, u16)>> {
    services
        .iter()
        .flat_map(|service| {
            known
                .iter()
                .filter_map(move |record| Some((*service, record.instance_of(service)?)))
        })
        .map(|(service, instance)| {
            let Some((port, host)) = known.iter().find_map(|record| record.service_of(instance))
            else {
                return Err((format!("{instance}."), TYPE_SRV));
            };
            let mut v4: Vec<_> = known
                .iter()
                .filter_map(|record| record.address_of(host))
                .collect();
            v4.sort_unstable();
            match v4.is_empty() {
                true => Err((format!("{host}."), TYPE_A)),
                false => Ok(Advertised {
                    service: service.to_string(),
                    fullname: format!("{instance}."),
                    host: format!("{host}."),
                    port,
                    v4,
                }),
            }
        })
        .collect()
}

/// Sends `query` from `socket` on each IPv4 interface. Returns `true` if it went out on one.
async fn send(socket: &UdpSocket, query: &[u8]) -> bool {
    let mut sent = false;
    for interface in if_addrs::get_if_addrs().unwrap_or_default() {
        let if_addrs::IfAddr::V4(v4) = interface.addr else {
            continue;
        };
        let socket_ref = socket2::SockRef::from(socket);
        // An interface that is down or takes no multicast is skipped.
        if socket_ref.set_multicast_if_v4(&v4.ip).is_ok()
            && socket.send_to(query, GROUP).await.is_ok()
        {
            sent = true;
        }
    }
    sent
}

/// Returns the next datagram `socket` receives into `buffer` before `deadline`, or `None`.
async fn receive<'a>(
    socket: &UdpSocket,
    deadline: Instant,
    buffer: &'a mut [u8],
) -> Option<&'a [u8]> {
    loop {
        let received = timeout_at(deadline, socket.recv_from(buffer)).await.ok()?;
        let Ok((len, _)) = received else {
            // Windows reports a port unreachable reply to an earlier send on
            // the next receive.
            continue;
        };
        return Some(&buffer[..len]);
    }
}

/// Returns an id for a query, which a responder repeats in its answer.
fn query_id() -> u16 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.subsec_nanos());
    (nanos as u16) ^ (std::process::id() as u16)
}

/// Returns the query for the records of type `qtype` of `name`, with `id`.
fn query(name: &str, qtype: u16, id: u16) -> io::Result<Vec<u8>> {
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
    query.extend(qtype.to_be_bytes());
    query.extend(CLASS_IN.to_be_bytes());
    Ok(query)
}

/// Returns the pointer, service and IPv4 address records of `reply`, from its answers and its
/// additional records alike, up to the first that does not parse. The id is not checked, as
/// some responders do not repeat it, and only an answer to this socket's own query reaches it.
fn records(reply: &[u8]) -> Vec<Record> {
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
        let Some((name, next)) = read_name(reply, at) else {
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
        let record = match rtype {
            TYPE_A => <[u8; 4]>::try_from(rdata).ok().map(|ip| Record::A {
                name,
                ip: Ipv4Addr::from(ip),
            }),
            TYPE_PTR => read_name(reply, data).map(|(target, _)| Record::Ptr { name, target }),
            TYPE_SRV => {
                rdata
                    .get(4..6)
                    .zip(read_name(reply, data + 6))
                    .map(|(port, (target, _))| Record::Srv {
                        name,
                        port: u16::from_be_bytes([port[0], port[1]]),
                        target,
                    })
            }
            _ => None,
        };
        found.extend(record);
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

    /// The answer Twinleaf Ethan gave a one-shot query for `_twinleaf._tcp.local`, with its
    /// pointer as the answer and its address, service and text as additional records.
    const ETHAN: &str = concat!(
        "123484000001000100000003095f7477696e6c656166045f746370056c6f63616c00000c0001095f7477",
        "696e6c656166045f746370056c6f63616c00000c00010000003c0030195477696e6c6561662045746861",
        "6e2028304135343745433329095f7477696e6c656166045f746370056c6f63616c00117477696e6c6561",
        "662d3041353437454333056c6f63616c00000100010000003c00040a000ba4195477696e6c6561662045",
        "7468616e2028304135343745433329095f7477696e6c656166045f746370056c6f63616c000021000100",
        "00003c001f000000001eaf117477696e6c6561662d3041353437454333056c6f63616c00195477696e6c",
        "65616620457468616e2028304135343745433329095f7477696e6c656166045f746370056c6f63616c00",
        "001000010000003c004f09747874766572733d310b6d6f64656c3d455448414e0a6e616d653d45746861",
        "6e0f73657269616c3d30413534374543330468773d311866773d323032362d31312d30342f3930333736",
        "632d444556",
    );

    /// The answer `tio proxy --mdns` gave the same query on loopback, its names compressed.
    const PROXY: &str = concat!(
        "123484000001000100000003095f7477696e6c656166045f746370056c6f63616c00000c0001c00c000c",
        "00010000000a00100d74696f2d746573742028783229c00cc032002100010000000a001c00000000ba60",
        "0667617264656e087477696e6c65616603636f6dc01bc032001000010000000a000100c0540001000100",
        "00000a00047f000001",
    );

    fn bytes(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).unwrap())
            .collect()
    }

    fn ethan() -> Vec<u8> {
        bytes(ETHAN)
    }

    /// The reply a responder gives the query for `name`, with one address in
    /// its answer, the name compressed to point back at the question.
    fn reply(name: &str, ip: [u8; 4]) -> Vec<u8> {
        let mut reply = query(name, TYPE_A, 7).unwrap();
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

    fn addresses(reply: &[u8], name: &str) -> Vec<Ipv4Addr> {
        records(reply)
            .iter()
            .filter_map(|record| record.address_of(name))
            .collect()
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
        assert!(addresses(
            &query("fakehub.local.", TYPE_A, 7).unwrap(),
            "fakehub.local."
        )
        .is_empty());
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
        assert!(query("a..local.", TYPE_A, 1).is_err());
        assert!(query(&format!("{}.local.", "x".repeat(64)), TYPE_A, 1).is_err());
    }

    /// A query for a service carries its name and the pointer type, as Ethan's answer repeats.
    #[test]
    fn a_service_query_is_the_question_ethan_answered() {
        let question = query("_twinleaf._tcp.local.", TYPE_PTR, 0x1234).unwrap();
        let mut asked = ethan()[..question.len()].to_vec();
        asked[2..12].copy_from_slice(&[0, 0, 0, 1, 0, 0, 0, 0, 0, 0]);

        assert_eq!(question, asked);
    }

    /// Ethan's one answer gives the instance, its host, its port and its address.
    #[test]
    fn one_answer_advertises_an_instance_with_its_host_and_address() {
        let known = records(&ethan());

        assert_eq!(
            instances(&["_twinleaf._tcp.local.", "_tio._tcp.local."], &known),
            [Ok(Advertised {
                service: "_twinleaf._tcp.local.".into(),
                fullname: "Twinleaf Ethan (0A547EC3)._twinleaf._tcp.local.".into(),
                host: "twinleaf-0A547EC3.local.".into(),
                port: 7855,
                v4: vec![Ipv4Addr::new(10, 0, 11, 164)],
            })]
        );
    }

    /// The proxy's answer, its names pointing back into the packet, gives its instance alike.
    #[test]
    fn a_compressed_answer_advertises_its_instance() {
        assert_eq!(
            instances(&["_twinleaf._tcp.local."], &records(&bytes(PROXY))),
            [Ok(Advertised {
                service: "_twinleaf._tcp.local.".into(),
                fullname: "tio-test (x2)._twinleaf._tcp.local.".into(),
                host: "garden.twinleaf.com.local.".into(),
                port: 47712,
                v4: vec![Ipv4Addr::LOCALHOST],
            })]
        );
    }

    /// An answer that leaves out the service or the address of its instance asks for it next.
    #[test]
    fn an_answer_without_a_host_or_address_asks_for_it() {
        let known = records(&ethan());
        let without = |kept: fn(&Record) -> bool| -> Vec<Record> {
            known
                .iter()
                .filter(|record| kept(record))
                .cloned()
                .collect()
        };
        let services = ["_twinleaf._tcp.local."];

        assert_eq!(
            instances(
                &services,
                &without(|record| !matches!(record, Record::Srv { .. }))
            ),
            [Err((
                "Twinleaf Ethan (0A547EC3)._twinleaf._tcp.local.".into(),
                TYPE_SRV
            ))]
        );
        assert_eq!(
            instances(
                &services,
                &without(|record| !matches!(record, Record::A { .. }))
            ),
            [Err(("twinleaf-0A547EC3.local.".into(), TYPE_A))]
        );
        assert!(instances(&["_tio._tcp.local."], &known).is_empty());
    }

    /// Ethan's answer cut short anywhere gives a prefix of its records, and never an instance
    /// with a part from past the cut.
    #[test]
    fn an_answer_cut_short_gives_what_came_before_the_cut() {
        let answer = ethan();
        let whole = records(&answer);
        for cut in 0..answer.len() {
            let some = records(&answer[..cut]);
            assert_eq!(some, whole[..some.len()], "cut at {cut}");
        }
    }
}
