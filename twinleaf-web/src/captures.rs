//! The captures of the hub's devices, taken for the pages that ask.
//!
//! A capture RPC is one its device flags as a capture, or names with
//! `.capture` at its end. A page asks for one capture of it, or for captures
//! again and again with a delay between them, and takes its asks back. The
//! hub takes a capture a step at a time, as `twinleaf::device::capture`
//! lays the steps out. It triggers, asks the status until the device is
//! done, at once and then after waits that double from 2 ms to 100 ms,
//! reads the metadata, and reads the blocks one after another. One
//! call is in flight at a time, so a capture shares the device with the
//! stream and with every other call.
//!
//! One task runs each capture RPC some page wants, and ends once none does
//! or its device is lost. While pages want it again and again it waits the
//! shortest delay any of them asked after each capture it read, and at least
//! [`RETRY`] after one that failed. Between two steps it gives up a capture
//! no page wants any more.
//!
//! Where a capture stands is a state every page hears, as [`Message::Capture`]
//! says, and a snapshot holds the newest. The capture itself goes only to
//! the pages that asked, as the capture frame `wire` lays out. Each is
//! numbered per RPC from 1, in the order the hub read them. A page's
//! [`Outbox`] keeps the newest capture of each RPC the page has not been
//! sent and drops the one it replaces, so a page that reads slowly is sent
//! fewer captures and holds up no other.

use crate::devices::{Devices, Kind};
use crate::protocol::{CaptureLabel, CaptureState, Frame, Message};
use crate::wire;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;
use twinleaf::device::capture::{
    Capture, CaptureError, CaptureMetadata, CaptureReadout, CaptureStatus,
};
use twinleaf::device::rpc::{CallError, RpcDescriptor, RpcMetaFlags};
use twinleaf::device::{Presence, Receiver};
use twinleaf::proto::data::DataType;
use twinleaf::{DeviceRoute, DeviceTree};

/// The delays between two captures a page may ask for, in seconds.
const DELAY: (f64, f64) = (0.05, 10.0);
/// The delay of a page that asks for one that is no number.
const DEFAULT_DELAY: f64 = 0.5;
/// How long a repeating capture waits after one that failed, at least.
const RETRY: Duration = Duration::from_secs(1);
/// How long a device may take to finish a capture after its trigger.
const PATIENCE: Duration = Duration::from_secs(10);
/// How long each call of a capture waits for its reply.
const STEP_TIMEOUT: Duration = Duration::from_secs(3);
/// How long the hub waits before it asks again whether a capture is done,
/// after the first ask, which follows the trigger at once. Each wait after
/// is twice the last, up to [`POLL_MAX`].
const POLL_FIRST: Duration = Duration::from_millis(2);
const POLL_MAX: Duration = Duration::from_millis(100);
/// How often a capture being read says how far it is.
const PROGRESS_EVERY: Duration = Duration::from_millis(100);

/// The element types a capture frame carries, those a page reads as an
/// array of numbers.
const ELEMENTS: [DataType; 10] = [
    DataType::U8,
    DataType::I8,
    DataType::U16,
    DataType::I16,
    DataType::U32,
    DataType::I32,
    DataType::U64,
    DataType::I64,
    DataType::F32,
    DataType::F64,
];

/// Whether `desc` is a capture RPC, by its flag or by its name.
pub(crate) fn is_capture(desc: &RpcDescriptor) -> bool {
    desc.meta.flags().contains(RpcMetaFlags::CAPTURE) || desc.full_name.ends_with(".capture")
}

/// One capture RPC of one device.
type Key = (DeviceRoute, String);

/// What a page asks of a capture RPC.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Want {
    /// One capture, now.
    Once,
    /// Captures again and again, this many seconds after each.
    Repeat(f64),
    /// Nothing more.
    Stop,
}

/// The captures a page has not been sent yet, the newest of each RPC.
#[derive(Default)]
pub(crate) struct Outbox {
    newest: Mutex<BTreeMap<Key, Arc<Frame>>>,
    ready: Notify,
}

impl Outbox {
    /// Keeps `frame` as the newest capture of `key`, in place of one the
    /// page has not taken.
    fn put(&self, key: Key, frame: Arc<Frame>) {
        self.newest.lock().unwrap().insert(key, frame);
        self.ready.notify_one();
    }

    /// Waits until there is a capture, and takes them all.
    pub(crate) async fn take(&self) -> Vec<Arc<Frame>> {
        loop {
            let taken = std::mem::take(&mut *self.newest.lock().unwrap());
            if !taken.is_empty() {
                return taken.into_values().collect();
            }
            self.ready.notified().await;
        }
    }
}

/// The pages that want one capture RPC, and what the hub last said of it.
#[derive(Default)]
struct Slot {
    /// The pages that asked for one capture, which the next one read
    /// answers.
    once: BTreeMap<u64, Arc<Outbox>>,
    /// The pages that asked for captures again and again, each with its
    /// delay.
    repeat: BTreeMap<u64, (Duration, Arc<Outbox>)>,
    /// The number of the last capture read.
    number: u64,
    /// Whether a task runs the captures. It ends only as it finds no page
    /// wants one, under the lock an ask starts another under.
    running: bool,
    /// Wakes the task as what the pages want changes.
    wake: Arc<Notify>,
    /// The state, delay and number last told to the pages.
    told: Option<(CaptureState, Option<Duration>, u64)>,
}

impl Slot {
    /// The shortest delay a page asked for, or `None` when no page wants
    /// captures again and again.
    fn delay(&self) -> Option<Duration> {
        self.repeat.values().map(|(delay, _)| *delay).min()
    }

    fn wanted(&self) -> bool {
        !self.once.is_empty() || !self.repeat.is_empty()
    }
}

/// Why a capture was not read.
#[derive(Debug, PartialEq)]
enum Failure {
    /// No page wants it any more.
    Abandoned,
    /// Its device is lost, or the hub's connection ended.
    Gone,
    /// It failed, and stands as this state says.
    Stands(CaptureState),
}

impl Failure {
    fn error(text: impl ToString) -> Failure {
        Failure::Stands(CaptureState::Error {
            error: text.to_string(),
        })
    }
}

impl From<CaptureError> for Failure {
    fn from(error: CaptureError) -> Failure {
        match error {
            busy if busy.is_busy() => Failure::Stands(CaptureState::Busy),
            CaptureError::Rpc(CallError::Timeout) => Failure::Stands(CaptureState::Timeout),
            other => Failure::error(other),
        }
    }
}

/// Whether a capture whose device reports `status` is done, `waited` after
/// its trigger, or why it never will be.
fn done(status: CaptureStatus, waited: Duration) -> Result<bool, Failure> {
    match status {
        CaptureStatus::Done => Ok(true),
        CaptureStatus::Capturing if waited < PATIENCE => Ok(false),
        CaptureStatus::Capturing => Err(Failure::Stands(CaptureState::Timeout)),
        CaptureStatus::Idle => Err(Failure::error("the device did not start the capture")),
        CaptureStatus::Error => Err(Failure::error("the device failed the capture")),
        CaptureStatus::Unknown(status) => Err(Failure::error(format!(
            "the device reported the status {status}, which this proxy does not know"
        ))),
    }
}

/// The capture `readout` holds as a page is sent it, cut to its elements,
/// or why a page cannot read it.
fn checked(mut readout: CaptureReadout) -> Result<CaptureReadout, Failure> {
    let meta = &readout.metadata;
    if !ELEMENTS.contains(&meta.data_type) {
        return Err(Failure::error(format!(
            "its elements are of type 0x{:02x}, which this proxy does not send",
            meta.data_type.value()
        )));
    }
    let bytes = meta.length as usize * meta.data_type.size();
    if readout.data.len() < bytes {
        return Err(Failure::error(format!(
            "it holds {} bytes, fewer than the {bytes} of its {} elements",
            readout.data.len(),
            meta.length
        )));
    }
    readout.data.truncate(bytes);
    Ok(readout)
}

/// Resolves once the device `presence` follows is lost, or the hub's
/// connection ends.
async fn lost(presence: &Receiver<Presence>) {
    loop {
        match presence.recv_async().await {
            Ok(Presence::Lost { .. }) => return,
            Ok(_) | Err(twinleaf::RecvError::Lagged(_)) => {}
            Err(_) => return,
        }
    }
}

/// What the task of a capture RPC does next.
enum Next {
    /// Takes a capture now.
    Now,
    /// Takes one at this time, unless what the pages want changes first.
    At(Instant),
    /// Ends, as no page wants one.
    Stop,
}

/// Every capture RPC a page has asked for, and the tasks that take them.
pub(crate) struct Captures {
    devices: Arc<Devices>,
    tree: DeviceTree,
    runtime: tokio::runtime::Handle,
    slots: Mutex<HashMap<Key, Slot>>,
}

impl Captures {
    /// The captures of the devices `tree` sees, whose tasks run on
    /// `runtime` and tell `devices` how each stands.
    pub(crate) fn new(
        devices: Arc<Devices>,
        tree: &DeviceTree,
        runtime: &tokio::runtime::Handle,
    ) -> Arc<Captures> {
        Arc::new(Captures {
            devices,
            tree: tree.with_timeout(STEP_TIMEOUT),
            runtime: runtime.clone(),
            slots: Mutex::new(HashMap::new()),
        })
    }

    /// Takes what page `page` asks of the capture `rpc` of the device at
    /// `route`, whose captures go to `outbox`. An ask of what the device's
    /// table does not list as a capture is dropped.
    pub(crate) fn ask(
        self: &Arc<Self>,
        page: u64,
        outbox: &Arc<Outbox>,
        route: &str,
        rpc: &str,
        want: Want,
    ) {
        let Some(key) = self.listed(route, rpc) else {
            log::debug!("A page asked for {rpc} of {route}, which is no capture the hub lists");
            return;
        };
        let mut slots = self.slots.lock().unwrap();
        let slot = slots.entry(key.clone()).or_default();
        match want {
            Want::Once => {
                slot.once.insert(page, outbox.clone());
            }
            Want::Repeat(delay) => {
                let delay = match delay.is_finite() {
                    true => delay.clamp(DELAY.0, DELAY.1),
                    false => DEFAULT_DELAY,
                };
                let delay = Duration::from_secs_f64(delay);
                slot.repeat.insert(page, (delay, outbox.clone()));
            }
            Want::Stop => {
                slot.once.remove(&page);
                slot.repeat.remove(&page);
            }
        }
        slot.wake.notify_one();
        if slot.wanted() && !slot.running {
            slot.running = true;
            self.runtime.spawn(self.clone().run(key));
        }
    }

    /// Takes back all that page `page` asked, as the page goes.
    pub(crate) fn forget(&self, page: u64) {
        for slot in self.slots.lock().unwrap().values_mut() {
            let asked = slot.once.remove(&page).is_some();
            if slot.repeat.remove(&page).is_some() || asked {
                slot.wake.notify_one();
            }
        }
    }

    /// The capture `rpc` of the device at `route`, when its table lists it.
    fn listed(&self, route: &str, rpc: &str) -> Option<Key> {
        let route = route.parse::<DeviceRoute>().ok()?;
        let table = self.devices.table(route)?;
        let desc = table.find(rpc).filter(|desc| is_capture(desc))?;
        Some((route, desc.full_name.clone()))
    }

    /// Tells every page that the capture `key` stands as `state`, unless
    /// that is what they were last told.
    fn tell(&self, key: &Key, slot: &mut Slot, state: CaptureState) {
        let delay = slot.delay();
        let told = Some((state.clone(), delay, slot.number));
        if slot.told == told {
            return;
        }
        slot.told = told;
        let message = Message::Capture {
            route: key.0.to_string(),
            rpc: key.1.clone(),
            state,
            number: slot.number,
            repeat: delay.map(|delay| delay.as_secs_f64()),
        };
        self.devices
            .publish(key.0, Kind::Capture(key.1.clone()), message);
    }

    fn say(&self, key: &Key, state: CaptureState) {
        let mut slots = self.slots.lock().unwrap();
        let slot = slots.get_mut(key).expect("a running capture has a slot");
        self.tell(key, slot, state);
    }

    /// Whether any page still wants the capture `key`.
    fn wanted(&self, key: &Key) -> bool {
        self.slots.lock().unwrap()[key].wanted()
    }

    /// What the task of `key` does next, after an attempt that ended at
    /// `ended` and left the capture as `last`, [`CaptureState::Idle`] for
    /// one that was read. It tells the pages how the capture stands
    /// meanwhile.
    fn next(&self, key: &Key, last: &CaptureState, ended: Option<Instant>) -> Next {
        let mut slots = self.slots.lock().unwrap();
        let slot = slots.get_mut(key).expect("a running capture has a slot");
        if !slot.once.is_empty() {
            return Next::Now;
        }
        match (slot.delay(), ended) {
            (None, _) => {
                slot.running = false;
                self.tell(key, slot, last.clone());
                Next::Stop
            }
            (Some(_), None) => Next::Now,
            (Some(delay), Some(ended)) if *last == CaptureState::Idle => {
                self.tell(key, slot, CaptureState::Waiting);
                Next::At(ended + delay)
            }
            (Some(delay), Some(ended)) => {
                self.tell(key, slot, last.clone());
                Next::At(ended + delay.max(RETRY))
            }
        }
    }

    /// Ends the capture `key` as its device is lost. No page wants it any
    /// more, and it stands as an error that says so.
    fn gone(&self, key: &Key) {
        let mut slots = self.slots.lock().unwrap();
        let slot = slots.get_mut(key).expect("a running capture has a slot");
        slot.once.clear();
        slot.repeat.clear();
        slot.running = false;
        let state = CaptureState::Error {
            error: "the device is gone".to_string(),
        };
        self.tell(key, slot, state);
    }

    /// Numbers the capture `readout` holds and gives it to every page that
    /// asked for `key`. The pages that asked for one have theirs. What the
    /// capture says it is of labels its RPC in the device's list.
    fn deliver(&self, key: &Key, readout: &CaptureReadout) {
        let meta = &readout.metadata;
        let label = CaptureLabel {
            name: meta.name.clone(),
            units: meta.units.clone(),
            x_name: meta.x_name.clone(),
            x_units: meta.x_units.clone(),
        };
        self.devices.label_capture(key.0, &key.1, label);
        let mut slots = self.slots.lock().unwrap();
        let slot = slots.get_mut(key).expect("a running capture has a slot");
        slot.number += 1;
        let route = key.0.to_string();
        let bytes = wire::encode_capture(&route, &key.1, slot.number, readout);
        let frame = Arc::new(Frame::Binary(bytes));
        let once = std::mem::take(&mut slot.once);
        let asked = once.iter().chain(
            slot.repeat
                .iter()
                .filter(|(page, _)| !once.contains_key(page))
                .map(|(page, (_, outbox))| (page, outbox)),
        );
        for (_, outbox) in asked {
            outbox.put(key.clone(), frame.clone());
        }
    }

    /// Takes the captures of `key` while a page wants them.
    async fn run(self: Arc<Self>, key: Key) {
        let Ok(device) = self.tree.device(key.0) else {
            return self.gone(&key);
        };
        let capture = Capture::new(&device, &key.1);
        let presence = device.presence_updates();
        let wake = self.slots.lock().unwrap()[&key].wake.clone();
        let mut last = CaptureState::Idle;
        let mut ended = None;
        loop {
            loop {
                match self.next(&key, &last, ended) {
                    Next::Now => break,
                    Next::Stop => return,
                    Next::At(due) => tokio::select! {
                        _ = tokio::time::sleep_until(due) => break,
                        _ = wake.notified() => {}
                        _ = lost(&presence) => return self.gone(&key),
                    },
                }
            }
            // The attempt is polled first, so the pages hear of it even when
            // the device is found lost at once.
            let attempt = tokio::select! {
                biased;
                attempt = self.take(&key, &capture) => attempt,
                _ = lost(&presence) => Err(Failure::Gone),
            };
            ended = Some(Instant::now());
            last = match attempt {
                Ok(readout) => {
                    self.deliver(&key, &readout);
                    CaptureState::Idle
                }
                Err(Failure::Abandoned) => CaptureState::Idle,
                Err(Failure::Gone) => return self.gone(&key),
                Err(Failure::Stands(state)) => {
                    // The pages that asked for one capture have their answer.
                    let mut slots = self.slots.lock().unwrap();
                    slots.get_mut(&key).expect("it is running").once.clear();
                    state
                }
            };
        }
    }

    /// Takes one capture of `key`, a step at a time, and says how it
    /// stands as it goes.
    async fn take(&self, key: &Key, capture: &Capture) -> Result<CaptureReadout, Failure> {
        self.say(key, CaptureState::Capturing);
        capture.trigger().await?;
        let triggered = Instant::now();
        let mut poll = POLL_FIRST;
        loop {
            let status = capture.status().await?;
            if done(status, triggered.elapsed())? {
                break;
            }
            if !self.wanted(key) {
                return Err(Failure::Abandoned);
            }
            tokio::time::sleep(poll).await;
            poll = (poll * 2).min(POLL_MAX);
        }
        let metadata: CaptureMetadata = capture.metadata().await?;
        let mut data = Vec::with_capacity(metadata.size as usize);
        let mut told = Instant::now();
        for index in 0..metadata.block_count()? {
            if !self.wanted(key) {
                return Err(Failure::Abandoned);
            }
            data.extend(capture.block(index).await?);
            if told.elapsed() >= PROGRESS_EVERY {
                told = Instant::now();
                let read = data.len().min(metadata.size as usize) as u32;
                let size = metadata.size;
                self.say(key, CaptureState::Reading { read, size });
            }
        }
        checked(CaptureReadout::new(metadata, data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hub::{Live, Page};
    use crate::wire::{decode_capture, Captured};
    use serde_json::{json, Value};
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::broadcast;
    use twinleaf::device::rpc::RpcMeta;
    use twinleaf::proxy::{HostOptions, Layout, Mount, Proxy};
    use twinleaf::Connection;

    /// How long a test waits for what must come, on a machine however busy.
    const WAIT: Duration = Duration::from_secs(30);
    /// How long a test waits to see that nothing comes.
    const QUIET: Duration = Duration::from_millis(400);

    /// The simulated device behind a proxy of the test's own, and a hub
    /// over the proxy's connection to it.
    struct Bench {
        stop: Arc<AtomicBool>,
        proxy: Proxy,
        connection: Connection,
        live: Arc<Live>,
        /// Every change the hub sent since it started.
        changes: broadcast::Receiver<Arc<Frame>>,
    }

    impl Drop for Bench {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            self.live.retire();
            self.proxy.shutdown();
        }
    }

    impl Bench {
        /// Starts the bench and waits until the hub lists the device's
        /// captures, from when a page may ask for them.
        async fn start() -> Bench {
            let stop = Arc::new(AtomicBool::new(false));
            let url = twinleaf_tools::tools::simulate::serve(&["--no-drop"], stop.clone());
            let root = DeviceRoute::root();
            let proxy = Proxy::host(HostOptions {
                port: 0,
                name: "a test".to_string(),
                default: Some(Layout {
                    mounts: vec![Mount {
                        url,
                        source: root,
                        prefix: root,
                    }],
                }),
                ..HostOptions::default()
            })
            .unwrap();
            let connection = Connection::open(&proxy.endpoint()).unwrap();
            let runtime = tokio::runtime::Handle::current();
            let live = Live::over(connection.clone(), None, &runtime).unwrap();
            let live = Arc::new(live);
            let (snapshot, changes) = live.snapshot(0).await.unwrap();
            let mut bench = Bench {
                stop,
                proxy,
                connection,
                live,
                changes,
            };
            // The list is in the snapshot when the table loaded before it.
            let listed = snapshot.iter().any(|frame| match frame {
                Frame::Text(text) => text.starts_with(r#"{"type":"captures""#),
                Frame::Binary(_) => false,
            });
            if !listed {
                bench.next("captures").await;
            }
            bench
        }

        /// The next change that is a message of type `kind`.
        async fn next(&mut self, kind: &str) -> Value {
            let found = async {
                loop {
                    match self.changes.recv().await {
                        Ok(frame) => {
                            let Frame::Text(text) = &*frame else { continue };
                            let message: Value = serde_json::from_str(text).unwrap();
                            if message["type"] == kind {
                                return message;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {}
                        Err(broadcast::error::RecvError::Closed) => panic!("the hub ended"),
                    }
                }
            };
            match tokio::time::timeout(WAIT, found).await {
                Ok(message) => message,
                Err(_) => panic!("no {kind} message in {WAIT:?}"),
            }
        }

        /// Every state the hub tells of a capture, up to the first that
        /// `last` holds of.
        async fn until(&mut self, last: impl Fn(&Value) -> bool) -> Vec<Value> {
            let mut states = Vec::new();
            loop {
                states.push(self.next("capture").await);
                if last(&states[states.len() - 1]) {
                    return states;
                }
            }
        }

        /// The capture states the hub tells in the next [`QUIET`].
        async fn told_in_quiet(&mut self) -> Vec<Value> {
            let mut states = Vec::new();
            let end = Instant::now() + QUIET;
            loop {
                let next = tokio::time::timeout_at(end, self.changes.recv()).await;
                match next {
                    Ok(Ok(frame)) => {
                        let Frame::Text(text) = &*frame else { continue };
                        let message: Value = serde_json::from_str(text).unwrap();
                        if message["type"] == "capture" {
                            states.push(message);
                        }
                    }
                    Ok(Err(_)) => {}
                    Err(_) => return states,
                }
            }
        }
    }

    /// The names of the states in `states`, in order.
    fn names(states: &[Value]) -> Vec<&str> {
        states
            .iter()
            .map(|state| state["state"].as_str().unwrap())
            .collect()
    }

    /// The captures `page` is sent next.
    async fn sent(page: &Page) -> Vec<Captured> {
        match tokio::time::timeout(WAIT, page.captures()).await {
            Ok(frames) => frames
                .iter()
                .map(|frame| match &**frame {
                    Frame::Binary(bytes) => decode_capture(bytes),
                    Frame::Text(_) => panic!("a capture is a binary frame"),
                })
                .collect(),
            Err(_) => panic!("no capture in {WAIT:?}"),
        }
    }

    /// The one capture `page` is sent next.
    async fn one(page: &Page) -> Captured {
        let mut captures = sent(page).await;
        assert_eq!(captures.len(), 1, "one capture");
        captures.remove(0)
    }

    /// Whether `page` is sent no capture in the next [`QUIET`].
    async fn nothing(page: &Page) -> bool {
        tokio::time::timeout(QUIET, page.captures()).await.is_err()
    }

    /// The elements of a capture of a 16 bit ADC, calibrated.
    fn volts(capture: &Captured) -> Vec<f64> {
        assert_eq!(capture.data_type, DataType::I16.value());
        capture
            .data
            .as_chunks::<2>()
            .0
            .iter()
            .map(|bytes| f64::from(i16::from_le_bytes(*bytes)) * f64::from(capture.y_calibration))
            .collect()
    }

    fn rms(values: &[f64]) -> f64 {
        (values.iter().map(|v| v * v).sum::<f64>() / values.len() as f64).sqrt()
    }

    /// The hub lists each device's capture RPCs once its table loads, each
    /// with what its metadata names before a capture is taken, and a page
    /// that connects later finds the list in its snapshot.
    #[tokio::test]
    async fn a_device_lists_its_captures_with_what_each_is_of() {
        let bench = Bench::start().await;
        let (snapshot, _) = bench.live.snapshot(0).await.unwrap();
        let listed: Vec<Value> = snapshot
            .iter()
            .filter_map(|frame| match frame {
                Frame::Text(text) => serde_json::from_str::<Value>(text).ok(),
                Frame::Binary(_) => None,
            })
            .filter(|message| message["type"] == "captures")
            .collect();
        let label = |name: &str, units: &str| json!({"name": name, "units": units, "x_name": "Time", "x_units": "s"});
        assert_eq!(
            listed,
            [json!({"type": "captures", "route": "/", "captures": [
                {"rpc": "fid.capture", "label": label("FID", "V")},
                {"rpc": "phase.capture", "label": label("Unwrapped phase", "rad")},
                {"rpc": "test.capture", "label": label("Test Signal", "V")},
            ]})]
        );
    }

    /// One capture goes to the page that asked and to no other, as the
    /// device's elements with what they are of, numbered from 1. Every page
    /// hears it was being taken and then that it was read.
    #[tokio::test]
    async fn one_capture_goes_to_the_page_that_asked() {
        let mut bench = Bench::start().await;
        let (asker, other) = (bench.live.page(), bench.live.page());

        asker.capture("/", "fid.capture", Want::Once);
        let states = bench.until(|state| state["state"] == "idle").await;
        // A read that a busy machine stretches past 0.1 s says how far it is.
        let (told, last) = (names(&states), states.len() - 1);
        assert_eq!((told[0], told[last]), ("capturing", "idle"), "{told:?}");
        assert!(
            told[1..last].iter().all(|name| *name == "reading"),
            "{told:?}"
        );
        let rest = |state: &Value| {
            let mut rest = state.clone();
            let fields = rest.as_object_mut().unwrap();
            fields.remove("seq");
            fields.remove("state");
            rest
        };
        let of = |number: u64| json!({"type": "capture", "route": "/", "rpc": "fid.capture", "number": number, "repeat": null});
        assert_eq!(rest(&states[0]), of(0));
        assert_eq!(rest(&states[last]), of(1));

        let first = one(&asker).await;
        assert_eq!(
            (first.route.as_str(), first.rpc.as_str()),
            ("/", "fid.capture")
        );
        assert_eq!(first.number, 1);
        assert_eq!((first.name.as_str(), first.units.as_str()), ("FID", "V"));
        assert_eq!(
            (first.x_name.as_str(), first.x_units.as_str()),
            ("Time", "s")
        );
        assert_eq!((first.x_offset, first.x_stride), (0.0, 0.5e-6));
        assert_eq!(first.y_calibration, (2.5f64 / 32768.0) as f32);
        let shot = volts(&first);
        assert_eq!(shot.len(), 1500);
        assert!(rms(&shot[..150]) > 10.0 * rms(&shot[1350..]), "it decays");
        assert!(nothing(&other).await);

        asker.capture("/", "fid.capture", Want::Once);
        let second = one(&asker).await;
        assert_eq!(second.number, 2);
        assert_ne!(second.data, first.data, "a new shot");
        let phase = bench.live.page();
        phase.capture("/", "phase.capture", Want::Once);
        let phase = one(&phase).await;
        assert_eq!(
            (phase.name.as_str(), phase.units.as_str()),
            ("Unwrapped phase", "rad")
        );
        assert_eq!((phase.number, phase.data_type), (1, DataType::F32.value()));
        assert_eq!(phase.data.len(), 1500 * 4);
    }

    /// A capture two pages ask for again and again is taken at the shorter
    /// of their delays and sent to both. A page that stops is sent no more,
    /// and the hub takes no more once the last page stops.
    #[tokio::test]
    async fn a_repeating_capture_runs_while_a_page_wants_it() {
        let mut bench = Bench::start().await;
        let (fast, slow) = (bench.live.page(), bench.live.page());

        slow.capture("/", "fid.capture", Want::Repeat(2.0));
        fast.capture("/", "fid.capture", Want::Repeat(0.05));
        let waiting = bench
            .until(|state| state["state"] == "waiting" && state["repeat"] == 0.05)
            .await;
        assert!(waiting.iter().all(|state| state["rpc"] == "fid.capture"));
        let started = Instant::now();
        let mut numbers = Vec::new();
        while numbers.len() < 5 {
            numbers.extend(sent(&fast).await.iter().map(|capture| capture.number));
        }
        assert!(
            numbers.windows(2).all(|pair| pair[0] < pair[1]),
            "{numbers:?}"
        );
        // Five captures 2 s apart take 8 s.
        assert!(
            started.elapsed() < Duration::from_secs(6),
            "{:?}",
            started.elapsed()
        );
        let also = one(&slow).await;
        assert!(also.number >= numbers[0], "{} of {numbers:?}", also.number);

        fast.capture("/", "fid.capture", Want::Stop);
        bench.until(|state| state["repeat"] == 2.0).await;
        // A capture read as the page stopped may wait for it yet.
        let _ = tokio::time::timeout(QUIET, fast.captures()).await;
        assert!(nothing(&fast).await, "a page that stopped is sent no more");
        let later = one(&slow).await;
        assert!(later.number > also.number);

        slow.capture("/", "fid.capture", Want::Stop);
        let stopped = bench.until(|state| state["repeat"].is_null()).await;
        let last = &stopped[stopped.len() - 1];
        assert_eq!(last["state"], "idle");
        assert_eq!(bench.told_in_quiet().await, Vec::<Value>::new());
        let _ = tokio::time::timeout(QUIET, slow.captures()).await;
        assert!(nothing(&slow).await);
    }

    /// A page that goes takes what it asked with it. Its repeating capture
    /// ends, and a capture the device has not finished is given up.
    #[tokio::test]
    async fn a_page_that_goes_takes_what_it_asked_with_it() {
        let mut bench = Bench::start().await;

        let page = bench.live.page();
        page.capture("/", "fid.capture", Want::Repeat(0.05));
        bench.until(|state| state["state"] == "waiting").await;
        drop(page);
        let ended = bench.until(|state| state["repeat"].is_null()).await;
        assert_eq!(ended[ended.len() - 1]["state"], "idle");
        assert_eq!(bench.told_in_quiet().await, Vec::<Value>::new());

        // The device takes half a second over `test.capture`. A machine
        // too busy to drop the page within it reads the capture, and the
        // test asks again.
        let mut given_up = false;
        for _ in 0..5 {
            let page = bench.live.page();
            page.capture("/", "test.capture", Want::Once);
            let asked = bench
                .until(|state| state["rpc"] == "test.capture" && state["state"] == "capturing")
                .await;
            let before = asked[asked.len() - 1]["number"].clone();
            drop(page);
            let ended = bench.until(|state| state["state"] == "idle").await;
            given_up = ended[ended.len() - 1]["number"] == before;
            if given_up {
                break;
            }
        }
        assert!(given_up, "a capture no page wants is given up unread");
    }

    /// A page that does not read is kept the newest capture of each RPC and
    /// no other, however many the hub read meanwhile.
    #[tokio::test]
    async fn a_slow_page_is_sent_only_the_newest_capture_of_each_rpc() {
        let mut bench = Bench::start().await;
        let page = bench.live.page();
        page.capture("/", "fid.capture", Want::Repeat(0.05));
        page.capture("/", "phase.capture", Want::Repeat(0.05));

        let read = |rpc: &'static str| {
            move |state: &Value| state["rpc"] == rpc && state["number"].as_u64() >= Some(4)
        };
        bench.until(read("fid.capture")).await;
        bench.until(read("phase.capture")).await;
        let kept = sent(&page).await;
        let rpcs: Vec<&str> = kept.iter().map(|capture| capture.rpc.as_str()).collect();
        assert_eq!(rpcs, ["fid.capture", "phase.capture"]);
        assert!(kept.iter().all(|capture| capture.number >= 4));

        let next = sent(&page).await;
        for capture in &next {
            let before = kept.iter().find(|kept| kept.rpc == capture.rpc).unwrap();
            assert!(capture.number > before.number);
        }
    }

    /// A device that goes while it captures ends the capture, repeating
    /// or not, as an error that says the device is gone, and the pages that
    /// wanted it are sent nothing more. A call the proxy refused for the
    /// lost device may stand as an error of its own first.
    #[tokio::test]
    async fn a_device_that_goes_while_it_captures_ends_the_capture() {
        let mut bench = Bench::start().await;
        let page = bench.live.page();
        page.capture("/", "test.capture", Want::Repeat(0.05));
        bench.until(|state| state["state"] == "capturing").await;

        bench.stop.store(true, Ordering::Relaxed);
        let states = bench.until(|state| state["repeat"].is_null()).await;
        let gone = &states[states.len() - 1];
        assert_eq!(gone["state"], "error");
        assert_eq!(gone["error"], "the device is gone");
        // A capture read before the device went may wait for the page yet.
        let _ = tokio::time::timeout(QUIET, page.captures()).await;
        assert!(nothing(&page).await);
        assert_eq!(bench.told_in_quiet().await, Vec::<Value>::new());
    }

    /// A capture another host started makes the device refuse the hub's
    /// trigger, which the pages hear as busy. One capture asked for ends
    /// there, and a repeating one tries again after a second. The other
    /// host's capture takes half a second, and on a machine too busy to
    /// ask within it the test has the other host trigger again.
    #[tokio::test]
    async fn a_capture_another_host_holds_is_busy() {
        let mut bench = Bench::start().await;
        let page = bench.live.page();
        let device = bench.connection.device(DeviceRoute::root());
        let settled =
            |state: &Value| !["capturing", "reading"].contains(&state["state"].as_str().unwrap());
        let mut found_busy = false;
        for _ in 0..5 {
            // Refused while the other host's last capture is in progress.
            let _ = device.submit("test.capture", &(-1i16).to_le_bytes()).await;
            page.capture("/", "test.capture", Want::Once);
            let once = bench.until(settled).await;
            let once = &once[once.len() - 1];
            if once["state"] != "busy" {
                continue;
            }
            assert_eq!(once["repeat"], Value::Null, "one capture ends as busy");

            let asked = Instant::now();
            page.capture("/", "test.capture", Want::Repeat(0.05));
            let again = bench.until(settled).await;
            let again = &again[again.len() - 1];
            if again["state"] != "busy" {
                page.capture("/", "test.capture", Want::Stop);
                bench.until(|state| state["repeat"].is_null()).await;
                continue;
            }
            assert_eq!(again["repeat"], 0.05, "a repeating one goes on");
            let _ = tokio::time::timeout(Duration::ZERO, page.captures()).await;
            one(&page).await;
            assert!(asked.elapsed() >= RETRY, "{:?}", asked.elapsed());
            found_busy = true;
            break;
        }
        assert!(found_busy, "the hub found the device busy");
    }

    /// An ask for what the device does not list as a capture, or of a
    /// device there is not, is dropped. Nothing is called and no page hears
    /// of it.
    #[tokio::test]
    async fn an_ask_for_what_is_no_capture_is_dropped() {
        let mut bench = Bench::start().await;
        let page = bench.live.page();
        page.capture("/", "dev.name", Want::Once);
        page.capture("/", "nothing.capture", Want::Repeat(0.05));
        page.capture("/7", "fid.capture", Want::Once);
        page.capture("nowhere", "fid.capture", Want::Once);
        page.capture("/", "fid.capture", Want::Stop);
        assert_eq!(bench.told_in_quiet().await, Vec::<Value>::new());
        assert!(nothing(&page).await);
    }

    /// An RPC is a capture by its flag, or by a name that ends `.capture`
    /// on a device that does not flag it.
    #[test]
    fn a_capture_is_flagged_or_named_one() {
        let unit = twinleaf::device::rpc::RpcValueType::Unit;
        let entry = |name: &str, flags: RpcMetaFlags| {
            RpcDescriptor::from_meta(RpcMeta::typed(unit, flags).bits(), name.to_string())
        };
        let readable = RpcMetaFlags::READABLE;
        assert!(is_capture(&entry(
            "adc.dump",
            readable | RpcMetaFlags::CAPTURE
        )));
        assert!(is_capture(&entry("fid.capture", readable)));
        assert!(!is_capture(&entry("fid.capture.rate", readable)));
        assert!(!is_capture(&entry("capture", readable)));
    }

    /// What the device's status makes of a capture, which is done, still
    /// coming while the hub has patience, timed out once it has none, or
    /// failed as its state says.
    #[test]
    fn a_status_is_done_still_coming_or_a_failure() {
        let (soon, late) = (PATIENCE - Duration::from_millis(1), PATIENCE);
        assert_eq!(done(CaptureStatus::Done, late), Ok(true));
        assert_eq!(done(CaptureStatus::Capturing, soon), Ok(false));
        assert_eq!(
            done(CaptureStatus::Capturing, late),
            Err(Failure::Stands(CaptureState::Timeout))
        );
        for status in [
            CaptureStatus::Idle,
            CaptureStatus::Error,
            CaptureStatus::Unknown(9),
        ] {
            assert!(matches!(
                done(status, soon),
                Err(Failure::Stands(CaptureState::Error { .. }))
            ));
        }
    }

    /// A call that timed out stands as a timeout, and any other failure as
    /// an error that says what failed.
    #[test]
    fn a_failed_step_stands_as_what_failed() {
        let timed_out = CaptureError::Rpc(CallError::Timeout);
        assert_eq!(
            Failure::from(timed_out),
            Failure::Stands(CaptureState::Timeout)
        );
        assert_eq!(
            Failure::from(CaptureError::ZeroBlocksize),
            Failure::error("capture metadata reported blocksize 0")
        );
    }

    fn readout(data_type: DataType, length: u32, bytes: usize) -> CaptureReadout {
        CaptureReadout {
            metadata: CaptureMetadata {
                size: bytes as u32,
                blocksize: 64,
                data_type,
                length,
                y_calibration: 1.0,
                x_offset: 0.0,
                x_stride: 1.0,
                name: String::new(),
                units: String::new(),
                x_name: String::new(),
                x_units: String::new(),
            },
            data: vec![7; bytes],
        }
    }

    /// A capture is sent as its elements and no byte more. One of a type a
    /// page reads no array of, or with fewer bytes than its elements, is an
    /// error and is not sent.
    #[test]
    fn a_capture_is_sent_as_its_elements_or_not_at_all() {
        let cut = checked(readout(DataType::I16, 3, 8)).unwrap();
        assert_eq!(cut.data.len(), 6);
        for (data_type, length, bytes) in [
            (DataType::I24, 2, 6),
            (DataType::new(0x00), 2, 6),
            (DataType::F32, 3, 11),
        ] {
            assert!(matches!(
                checked(readout(data_type, length, bytes)),
                Err(Failure::Stands(CaptureState::Error { .. }))
            ));
        }
    }

    /// An outbox keeps one capture of each RPC, the newest put in it.
    #[tokio::test]
    async fn an_outbox_keeps_the_newest_capture_of_each_rpc() {
        let outbox = Outbox::default();
        let key = |rpc: &str| (DeviceRoute::root(), rpc.to_string());
        let frame = |byte: u8| Arc::new(Frame::Binary(vec![byte]));
        for byte in 0..100 {
            outbox.put(key("fid.capture"), frame(byte));
        }
        outbox.put(key("phase.capture"), frame(200));
        let taken: Vec<Frame> = outbox.take().await.iter().map(|f| (**f).clone()).collect();
        assert_eq!(taken, [Frame::Binary(vec![99]), Frame::Binary(vec![200])]);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), outbox.take())
                .await
                .is_err()
        );
    }

    /// Prints how long a capture of the simulated device takes, and what a
    /// capture repeated at the shortest delay costs the stream and the
    /// other calls beside none. A thread of its own calls `dev.name` every
    /// 20 ms and counts the rows of the 1 kHz stream, as another client of
    /// the hub's connection would. Run with `--ignored --nocapture`.
    #[tokio::test]
    #[ignore = "a measurement"]
    async fn what_a_repeating_capture_costs_the_stream_and_other_calls() {
        const SPAN: Duration = Duration::from_secs(20);
        for repeating in [false, true] {
            let mut bench = Bench::start().await;
            let page = bench.live.page();
            let samples = bench.connection.samples();
            let device = bench.connection.device(DeviceRoute::root());
            let beside = std::thread::spawn(move || {
                let started = std::time::Instant::now();
                let (mut rows, mut lost, mut lagged) = (0usize, 0u64, 0u64);
                let mut latencies = Vec::new();
                while started.elapsed() < SPAN {
                    let asked = std::time::Instant::now();
                    device.raw_rpc("dev.name", &[]).unwrap();
                    latencies.push(asked.elapsed());
                    loop {
                        match samples.try_recv() {
                            Ok(Some(batch)) => {
                                if batch.stream_key().stream_id.value() == 1 {
                                    rows += batch.timestamps().len();
                                    let loss = batch.boundary().and_then(|b| b.loss.clone());
                                    lost += loss.map_or(0, |loss| loss.samples);
                                }
                            }
                            Ok(None) => break,
                            Err(twinleaf::RecvError::Lagged(batches)) => lagged += batches,
                            Err(error) => panic!("{error}"),
                        }
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
                latencies.sort();
                (rows, lost, lagged, latencies, started.elapsed())
            });
            if repeating {
                page.capture("/", "fid.capture", Want::Repeat(0.0));
            }
            let end = Instant::now() + SPAN;
            let mut captures = Vec::new();
            let (mut took, mut apart) = (Vec::new(), Vec::new());
            let (mut capturing, mut last) = (None, None::<Instant>);
            loop {
                tokio::select! {
                    _ = tokio::time::sleep_until(end) => break,
                    frames = page.captures() => {
                        captures.extend(frames.iter().map(|frame| match &**frame {
                            Frame::Binary(bytes) => decode_capture(bytes).number,
                            Frame::Text(_) => unreachable!(),
                        }));
                    }
                    change = bench.changes.recv() => {
                        let Ok(frame) = change else { continue };
                        let Frame::Text(text) = &*frame else { continue };
                        let message: Value = serde_json::from_str(text).unwrap();
                        match message["state"].as_str() {
                            Some("capturing") => {
                                apart.extend(last.replace(Instant::now()).map(|at| at.elapsed()));
                                capturing = Some(Instant::now());
                            }
                            Some("waiting") => took.extend(capturing.take().map(|at| at.elapsed())),
                            _ => {}
                        }
                    }
                }
            }
            let (rows, lost, lagged, latencies, span) = beside.join().unwrap();
            took.sort();
            apart.sort();
            let at = |sorted: &[Duration], part: f64| {
                sorted
                    .get(((sorted.len() as f64 - 1.0) * part) as usize)
                    .copied()
                    .unwrap_or_default()
            };
            println!(
                "repeating {repeating}: {rows} rows of the 1 kHz stream in {span:?}, {lost} lost, {lagged} batches lagged"
            );
            println!(
                "  {} calls of dev.name: median {:?}, 95% {:?}, 99% {:?}, worst {:?}",
                latencies.len(),
                at(&latencies, 0.5),
                at(&latencies, 0.95),
                at(&latencies, 0.99),
                at(&latencies, 1.0),
            );
            println!(
                "  {} captures sent, the last number {:?}; trigger to read: median {:?}, 95% {:?}, worst {:?}; \
                 trigger to trigger: median {:?}, 95% {:?}",
                captures.len(),
                captures.last(),
                at(&took, 0.5),
                at(&took, 0.95),
                at(&took, 1.0),
                at(&apart, 0.5),
                at(&apart, 0.95),
            );
        }
    }
}
