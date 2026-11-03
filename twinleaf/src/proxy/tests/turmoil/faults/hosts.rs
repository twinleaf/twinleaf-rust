//! The log the hosts and the driver write, and the hosts of the proxy, the
//! observer and the apps.

use super::schedule::Action;
use super::{APPS, CALL_TIMEOUT, MOST_APPS, OPEN_TIMEOUT};
use crate::data::{Change, SampleBatch};
use crate::device::runtime::ProxyAddress;
use crate::device::{Connection, Presence, Receiver, RecvError};
use crate::proto::{ColumnId, DeviceRoute};
use crate::proxy::control::Purpose;
use crate::proxy::tests::turmoil::{child as child_route, World, DEVICE_PORT, PROXY_PORT};
use crate::proxy::{server, HostOptions, Listen};
use crate::tio::packet::LinkStatus;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

/// One event of a case's log, at the virtual time in milliseconds that it
/// happened.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::proxy::tests::turmoil) struct Logged {
    pub(in crate::proxy::tests::turmoil) at: u64,
    pub(in crate::proxy::tests::turmoil) event: Event,
}

/// What a host or the driver saw.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::proxy::tests::turmoil) enum Event {
    /// The case's seed and swarm parameters.
    Began(String),
    /// An action of the driver.
    Did(Action),
    WarmedUp,
    Healed,
    /// A boot of the device.
    Boot {
        index: u32,
        session: u32,
    },
    /// A connection the device accepted. The device numbers its connections
    /// in the order it accepts them, from 0.
    Accepted,
    /// The end of a connection of the device, at the end of what it read or
    /// at a write that failed.
    Ended {
        connection: u32,
    },
    /// An RPC the device was asked in the boot of `index` on its connection
    /// `connection`, and for `rpc.hash` the hash it answered.
    Asked {
        index: u32,
        connection: u32,
        child: bool,
        name: String,
        args: Vec<u8>,
        hash: Option<u32>,
    },
    /// What an app saw.
    App(u8, Seen),
    /// What the proxy host counted.
    Proxy(Stats),
    /// turmoil's own count of the proxy host's streams.
    Streams(usize),
}

/// What an app saw.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::proxy::tests::turmoil) enum Seen {
    Opening,
    Opened,
    OpenFailed(String),
    Closing,
    Presence {
        child: bool,
        presence: Shown,
    },
    Batch(Batch),
    Lagged(Feed, u64),
    Link {
        status: LinkStatus,
        root: bool,
    },
    Table {
        child: bool,
        table: Result<(Option<u32>, Option<u32>), String>,
    },
    /// A `test.echo` call, by its number.
    Sent(u32),
    /// A call's outcome, the session, app and number of the reply.
    Done(u32, Result<(u32, u32, u32), String>),
    NameSent(u32),
    Named(u32, Result<String, String>),
    /// A receiver that ended.
    Stopped(Feed, RecvError),
}

/// A presence, with no real time in it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::proxy::tests::turmoil) enum Shown {
    Connecting,
    Alive(Option<u32>),
    Lost,
}

/// One of an app's receivers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub(in crate::proxy::tests::turmoil) enum Feed {
    Presence,
    ChildPresence,
    Samples,
    Packets,
    Table,
    ChildTable,
}

/// A batch an app received.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::proxy::tests::turmoil) struct Batch {
    pub(super) session: u32,
    pub(super) first: u32,
    pub(super) last: u32,
    /// Whether its numbers step by one.
    pub(super) steps: bool,
    pub(super) boundary: Option<Bound>,
    /// A value that is not the session, if any is.
    pub(super) odd: Option<String>,
}

/// A batch's boundary.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::proxy::tests::turmoil) struct Bound {
    pub(super) change: Option<Changed>,
    pub(super) lost: Option<u64>,
}

/// A boundary's change.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::proxy::tests::turmoil) enum Changed {
    Initial,
    Session { old: u32, new: u32 },
    Other(String),
}

/// What the proxy host counted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::proxy::tests::turmoil) struct Stats {
    /// Each client's id and app.
    pub(super) clients: Vec<(u64, Option<u8>)>,
    pub(super) users: usize,
    pub(super) streams: usize,
    pub(super) tasks: usize,
}

impl Stats {
    /// The ids of app `app`'s clients.
    pub(super) fn clients_of(&self, app: u8) -> impl Iterator<Item = u64> + '_ {
        self.clients
            .iter()
            .filter(move |client| client.1 == Some(app))
            .map(|client| client.0)
    }
}

impl Batch {
    fn of(batch: &SampleBatch) -> Batch {
        let session = batch.metadata().device().session().value();
        let numbers: Vec<u32> = batch.sample_numbers().iter().map(|n| n.value()).collect();
        let steps = numbers
            .windows(2)
            .all(|pair| pair[1] == pair[0].wrapping_add(1));
        let odd = batch.iter().find_map(|row| {
            let value = row
                .value(ColumnId::new(0))
                .and_then(|value| value.try_as_f64());
            (value != Some(f64::from(session as f32))).then(|| format!("{value:?}"))
        });
        let boundary = batch.boundary().map(|boundary| Bound {
            change: boundary.change.as_ref().map(|change| match change {
                Change::Initial => Changed::Initial,
                Change::SessionChanged { old, new } => Changed::Session {
                    old: old.value(),
                    new: new.value(),
                },
                other => Changed::Other(format!("{other:?}")),
            }),
            lost: boundary.loss.as_ref().map(|loss| loss.samples),
        });
        Batch {
            session,
            first: numbers[0],
            last: numbers[numbers.len() - 1],
            steps,
            boundary,
            odd,
        }
    }
}

/// The arguments of app `app`'s `test.echo` call `number`, its nonce.
pub(super) fn nonce(app: u8, number: u32) -> Vec<u8> {
    [u32::from(app).to_le_bytes(), number.to_le_bytes()].concat()
}

/// What the driver tells the apps, and what an app keeps across a restart
/// of its host.
#[derive(Default)]
pub(super) struct Control {
    /// Set while an app stays away with its connection closed.
    pub(super) away: [AtomicBool; MOST_APPS],
    /// Set once an app has opened, heard `Alive` and taken a batch.
    pub(super) warm: [AtomicBool; MOST_APPS],
    /// Each app's count of its calls.
    calls: [AtomicU32; MOST_APPS],
    /// Each app's count of its `dev.name` calls.
    names: [AtomicU32; MOST_APPS],
}

/// Hosts the proxy, and writes what it counts every 100 ms when it
/// changes.
pub(super) async fn proxy(
    world: Arc<World>,
    reconnect_timeout: Option<Duration>,
) -> ::turmoil::Result {
    let options = HostOptions {
        listen: Listen::AllInterfaces,
        port: PROXY_PORT,
        name: "simulated".to_string(),
        reconnect_timeout,
        ..HostOptions::default()
    };
    let proxy = server::host(options).await?;
    world.proxy_listens.store(true, Ordering::SeqCst);
    let me = ::turmoil::lookup("proxy");
    let mut last = None;
    loop {
        let clients = proxy
            .clients()
            .iter()
            .map(|client| (client.id, app_at(client.peer.ip())))
            .collect();
        let stats = Stats {
            clients,
            users: proxy.links.users_async().await,
            streams: crate::seam::turmoil::live_streams_on(me),
            tasks: crate::seam::turmoil::live_tasks(),
        };
        if last.as_ref() != Some(&stats) {
            world.write(Event::Proxy(stats.clone()));
            last = Some(stats);
        }
        sleep(Duration::from_millis(100)).await;
    }
}

/// The app whose host has `address`.
fn app_at(address: std::net::IpAddr) -> Option<u8> {
    let name = ::turmoil::reverse_lookup(address)?;
    APPS.iter()
        .position(|app| *app == name)
        .map(|app| app as u8)
}

/// Writes turmoil's own count of the proxy host's streams every 100 ms when
/// it changes. No fault touches its host.
pub(super) async fn observer(world: Arc<World>) -> ::turmoil::Result {
    let mut last = None;
    loop {
        let streams = ::turmoil::established_tcp_stream_count_on("proxy");
        if last != Some(streams) {
            world.write(Event::Streams(streams));
            last = Some(streams);
        }
        sleep(Duration::from_millis(100)).await;
    }
}

/// Runs app `app`, which opens the device and follows it, and opens it
/// again whenever its connection ends.
pub(super) async fn app(
    world: Arc<World>,
    control: Arc<Control>,
    app: u8,
    child: bool,
) -> ::turmoil::Result {
    while !world.proxy_listens.load(Ordering::SeqCst) {
        sleep(Duration::from_millis(10)).await;
    }
    let proxy = ProxyAddress {
        url: format!("tcp://proxy:{PROXY_PORT}"),
        ensure: None,
    };
    let device = format!("tcp://device:{DEVICE_PORT}");
    loop {
        while control.away[usize::from(app)].load(Ordering::SeqCst) {
            sleep(Duration::from_millis(100)).await;
        }
        world.write(Event::App(app, Seen::Opening));
        match Connection::open_at_async(&proxy, &device, Purpose::Use, OPEN_TIMEOUT).await {
            Ok(connection) => {
                world.write(Event::App(app, Seen::Opened));
                follow(&world, &control, app, child, connection).await;
            }
            Err(error) => {
                world.write(Event::App(app, Seen::OpenFailed(error.to_string())));
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

/// Receives the next item of `receiver`, or waits forever without one.
async fn next<T>(receiver: Option<&Receiver<T>>) -> Result<T, RecvError> {
    match receiver {
        Some(receiver) => receiver.recv_async().await,
        None => std::future::pending().await,
    }
}

/// Follows `connection` until one of its receivers ends.
async fn follow(
    world: &Arc<World>,
    control: &Control,
    app: u8,
    child: bool,
    connection: Connection,
) {
    let say = |seen| world.write(Event::App(app, seen));
    let root = connection.device(DeviceRoute::root());
    let hub = connection.device(child_route());
    let presence = root.presence_updates();
    let table = root.rpc_registry_updates();
    let samples = connection.samples();
    let packets = connection.packets();
    let child_presence = child.then(|| hub.presence_updates());
    let child_table = child.then(|| hub.rpc_registry_updates());
    let calling = root.with_timeout(Duration::from_millis(CALL_TIMEOUT));
    let mut second = tokio::time::interval(Duration::from_secs(1));
    let mut looking = tokio::time::interval(Duration::from_millis(100));
    let (mut alive, mut sampled, mut closing) = (false, false, false);
    let mut seconds = 0u32;
    let presence_of = |presence: Presence| match presence {
        Presence::Connecting => Shown::Connecting,
        Presence::Alive { session } => Shown::Alive(session.map(|s| s.value())),
        Presence::Lost { .. } => Shown::Lost,
    };
    let table_of = |table: crate::device::RegistryState| match table {
        Ok(table) => Ok((table.hash, table.session.map(|s| s.value()))),
        Err(error) => Err(error.to_string()),
    };
    // Which receivers have ended, in the order of `Feed`. Once one has, the
    // app takes what the others still hold until each has ended too.
    let mut over = [false, !child, false, false, false, !child];
    let mut ending = false;
    let mut grace = std::pin::pin!(sleep(Duration::from_secs(1_000_000)));
    loop {
        let ended = tokio::select! {
            got = presence.recv_async(), if !over[0] => match got {
                Ok(seen) => {
                    alive |= matches!(seen, Presence::Alive { .. });
                    say(Seen::Presence { child: false, presence: presence_of(seen) });
                    None
                }
                Err(error) => Some((Feed::Presence, error)),
            },
            got = next(child_presence.as_ref()), if !over[1] => match got {
                Ok(seen) => {
                    say(Seen::Presence { child: true, presence: presence_of(seen) });
                    None
                }
                Err(error) => Some((Feed::ChildPresence, error)),
            },
            got = samples.recv_async(), if !over[2] => match got {
                Ok(batch) => {
                    sampled = true;
                    say(Seen::Batch(Batch::of(&batch)));
                    None
                }
                Err(error) => Some((Feed::Samples, error)),
            },
            got = packets.recv_async(), if !over[3] => match got {
                Ok(packet) => {
                    if let Some(status) = packet.link_status() {
                        let root = packet.route() == DeviceRoute::root();
                        say(Seen::Link { status, root });
                    }
                    None
                }
                Err(error) => Some((Feed::Packets, error)),
            },
            got = table.recv_async(), if !over[4] => match got {
                Ok(table) => {
                    say(Seen::Table { child: false, table: table_of(table) });
                    None
                }
                Err(error) => Some((Feed::Table, error)),
            },
            got = next(child_table.as_ref()), if !over[5] => match got {
                Ok(table) => {
                    say(Seen::Table { child: true, table: table_of(table) });
                    None
                }
                Err(error) => Some((Feed::ChildTable, error)),
            },
            _ = second.tick(), if !ending => {
                call(world, control, app, &calling, seconds);
                seconds += 1;
                None
            }
            _ = looking.tick(), if !closing && !ending => {
                if control.away[usize::from(app)].load(Ordering::SeqCst) {
                    closing = true;
                    say(Seen::Closing);
                    connection.close();
                }
                None
            }
            _ = &mut grace => return,
        };
        match ended {
            Some((feed, RecvError::Lagged(missed))) => say(Seen::Lagged(feed, missed)),
            Some((feed, error)) => {
                say(Seen::Stopped(feed, error));
                over[feed as usize] = true;
                if !ending {
                    ending = true;
                    grace
                        .as_mut()
                        .reset(tokio::time::Instant::now() + Duration::from_secs(1));
                }
                if over.iter().all(|over| *over) {
                    return;
                }
            }
            None => {}
        }
        if alive && (sampled || world.withholds_metadata.load(Ordering::SeqCst)) {
            control.warm[usize::from(app)].store(true, Ordering::SeqCst);
        }
    }
}

/// Makes the app's call of this second, a `test.echo` whose arguments are
/// its nonce, and every fifth second a `dev.name` too.
fn call(
    world: &Arc<World>,
    control: &Control,
    app: u8,
    calling: &crate::device::Device,
    second: u32,
) {
    let number = control.calls[usize::from(app)].fetch_add(1, Ordering::SeqCst);
    world.write(Event::App(app, Seen::Sent(number)));
    let echo = calling.submit("test.echo", &nonce(app, number));
    let writing = world.clone();
    crate::chan::spawn(async move {
        let outcome = match echo.await {
            Ok(reply) if reply.len() == 12 => {
                let word = |at: usize| {
                    u32::from_le_bytes(reply[at..at + 4].try_into().expect("four bytes"))
                };
                Ok((word(0), word(4), word(8)))
            }
            Ok(reply) => Err(format!("the reply {reply:?}")),
            Err(error) => Err(error.to_string()),
        };
        writing.write(Event::App(app, Seen::Done(number, outcome)));
    });
    if second.is_multiple_of(5) {
        let asked = control.names[usize::from(app)].fetch_add(1, Ordering::SeqCst);
        world.write(Event::App(app, Seen::NameSent(asked)));
        let name = calling.submit("dev.name", &[]);
        let writing = world.clone();
        crate::chan::spawn(async move {
            let named = name.await;
            let named = named.map(|name| String::from_utf8_lossy(&name).into_owned());
            writing.write(Event::App(
                app,
                Seen::Named(asked, named.map_err(|e| e.to_string())),
            ));
        });
    }
}
