//! What the hub sends a page, recorded as the files the page's tests read,
//! under `ui/tests/fixtures/hub`. Each file is one frame of the socket, made
//! by the code that makes it for a page. A message is as a snapshot holds
//! it, or as the change it goes out as when no snapshot holds it. A file is
//! named by the message's type and its state, as `upgrade-done.json`, or by
//! the kind of binary frame, as `points.bin`. A further form of a type and
//! state adds a word that names it, as `upgrade-done-no-build.json`. The folder `snapshot` holds a
//! page's first snapshot of a hub with one simulated device, a file per
//! frame, in order. The folder `http` holds what the routes answer, a file
//! per reply in each form worth holding, named by its route and its form,
//! as `call-error.json`. A file holds the body of a success, and the status
//! and the body of a refusal, or the bytes of a reply that is no JSON, as
//! `export.csv`. Every number in a file comes from exactly representable
//! values through exact arithmetic, so the files are the same on every
//! platform. The spectra frame is encoded from bins the test gives, so it
//! pins the frame's layout and not the numbers of an estimate.
//!
//! The test fails on a file that differs from what the hub makes now, or
//! that the hub no longer makes, and names it. With `TWINLEAF_WEB_RECORD`
//! set it writes the files instead.

use crate::protocol::Frame;
use axum::response::IntoResponse;
use futures_util::FutureExt;
use std::collections::BTreeMap;
use std::path::Path;

/// The folder of the recorded files.
const FOLDER: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/ui/tests/fixtures/hub");

/// The files the hub's code made, by their paths in [`FOLDER`].
#[derive(Default)]
pub(crate) struct Recorded(BTreeMap<String, Vec<u8>>);

impl Recorded {
    /// Keeps `frame` as the file of its kind.
    pub(crate) fn frame(&mut self, frame: &Frame) {
        self.file(name(frame), frame);
    }

    /// Keeps `frame` as the file of its kind in the form `form`.
    pub(crate) fn form(&mut self, frame: &Frame, form: &str) {
        let name = name(frame);
        let stem = name.strip_suffix(".json").unwrap();
        self.file(format!("{stem}-{form}.json"), frame);
    }

    /// Keeps `frames`, a snapshot, in order in the folder `snapshot`.
    pub(crate) fn snapshot(&mut self, frames: &[Frame]) {
        for (index, frame) in frames.iter().enumerate() {
            self.file(format!("snapshot/{index:02}-{}", name(frame)), frame);
        }
    }

    /// Keeps `frame` as the file `path`, a text frame as its text and a
    /// newline.
    fn file(&mut self, path: String, frame: &Frame) {
        let bytes = match frame {
            Frame::Text(text) => format!("{text}\n").into_bytes(),
            Frame::Binary(bytes) => bytes.clone(),
        };
        self.keep(path, bytes);
    }

    /// Keeps `reply`, as a route answers it, as the file `http/{name}.json`:
    /// the body of a success, and of a refusal its status and its body.
    pub(crate) fn reply(&mut self, name: &str, reply: impl IntoResponse) {
        let response = reply.into_response();
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX);
        let body = body.now_or_never().expect("a whole body").unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        let text = match status.is_success() {
            true => format!("{body}\n"),
            false => format!("{{\"status\":{},\"body\":{body}}}\n", status.as_u16()),
        };
        self.keep(format!("http/{name}.json"), text.into_bytes());
    }

    /// Keeps `bytes`, a reply that is no JSON, as the file `http/{name}`.
    pub(crate) fn bytes(&mut self, name: &str, bytes: Vec<u8>) {
        self.keep(format!("http/{name}"), bytes);
    }

    fn keep(&mut self, path: String, bytes: Vec<u8>) {
        assert!(self.0.insert(path.clone(), bytes).is_none(), "{path} twice");
    }
}

/// The name of the file of `frame`: its type, and the state it stands in
/// for a message that has one, or the kind of a binary frame.
fn name(frame: &Frame) -> String {
    let text = match frame {
        Frame::Text(text) => text,
        Frame::Binary(bytes) => {
            let kind = ["points", "spectra", "capture"][usize::from(bytes[0]) - 1];
            return format!("{kind}.bin");
        }
    };
    let message: serde_json::Value = serde_json::from_str(text).unwrap();
    let state = match message["type"].as_str().unwrap() {
        "firmware" => message["catalog"]["state"].as_str(),
        "table" => message.get("error").map(|_| "failed"),
        "log" => message["source"].as_str(),
        _ => message["state"].as_str(),
    };
    match state {
        Some(state) => format!("{}-{state}.json", message["type"].as_str().unwrap()),
        None => format!("{}.json", message["type"].as_str().unwrap()),
    }
}

/// The paths of the files under `dir`, as `under` and their names.
fn kept(dir: &Path, under: &str) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut paths = Vec::new();
    for entry in entries.map(Result::unwrap) {
        let path = format!("{under}{}", entry.file_name().to_str().unwrap());
        if entry.file_type().unwrap().is_dir() {
            paths.extend(kept(&entry.path(), &format!("{path}/")));
        } else {
            paths.push(path);
        }
    }
    paths
}

/// The files the page's tests read are what the hub sends now.
#[tokio::test(flavor = "multi_thread")]
async fn the_page_tests_read_what_the_hub_sends() {
    use crate::devices::Kind;
    use crate::firmware::tests::{Bench, Fake, OLD_BUILD};
    use crate::protocol::{Message, Presence};
    use twinleaf::DeviceRoute;

    let args = ["--build", OLD_BUILD];
    let bench = Bench::start(&args, Fake::listing(Vec::new()), None, &["/"]).await;
    // The hub knows what the device runs. The test waits until it also
    // knows the device is alive and what its captures are of.
    let devices = &bench.live.devices;
    let root = DeviceRoute::root();
    let started = std::time::Instant::now();
    while !matches!(
        devices.newest(root, Kind::Device),
        Some(Message::Device {
            presence: Presence::Alive,
            ..
        })
    ) || devices.newest(root, Kind::Captures).is_none()
    {
        assert!(
            started.elapsed().as_secs() < 30,
            "the device is not all known"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }

    let mut recorded = Recorded::default();
    let settings = crate::devices::tests::settings(&bench.live.tree).await;
    crate::hub::tests::record(&mut recorded, devices, settings);
    crate::logbook::tests::record(&mut recorded);
    crate::devices::tests::record(&mut recorded);
    crate::captures::tests::record(&mut recorded);
    crate::firmware::tests::record(&mut recorded);
    crate::wire::tests::record(&mut recorded);
    crate::discover::tests::record(&mut recorded);
    crate::routes::tests::record(&mut recorded, &bench.live).await;

    let folder = Path::new(FOLDER);
    let stale: Vec<String> = (kept(folder, "").into_iter())
        .filter(|path| !recorded.0.contains_key(path))
        .collect();
    if std::env::var_os("TWINLEAF_WEB_RECORD").is_some() {
        for path in stale {
            std::fs::remove_file(folder.join(path)).unwrap();
        }
        for (path, bytes) in &recorded.0 {
            let path = folder.join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, bytes).unwrap();
        }
        return;
    }
    let differ: Vec<&String> = (recorded.0.iter())
        .filter(|(path, bytes)| std::fs::read(folder.join(path)).ok().as_ref() != Some(bytes))
        .map(|(path, _)| path)
        .collect();
    assert!(
        differ.is_empty() && stale.is_empty(),
        "the hub sends other than {differ:?}, and no longer sends {stale:?}. \
         TWINLEAF_WEB_RECORD set records what it sends."
    );
}
