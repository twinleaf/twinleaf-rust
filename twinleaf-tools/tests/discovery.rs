//! `tio` end to end on loopback: a simulated tree served by `tio proxy`, found by `tio list`,
//! answering `tio rpc` through the machine's shared proxy, and held as its default by a pick.
#![cfg(unix)]

use std::io::{Read, Write};
use std::net::{TcpListener, UdpSocket};
use std::process::{Child, Command, Output, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const WAIT: Duration = Duration::from_secs(20);

/// A process the test started, killed as the test ends.
struct Running(Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The tree each test serves: `tio simulate` with a device at `/` and `/1`, and `tio proxy`
/// serving it on a port of its own and advertising it, with the port of the shared proxy.
struct Served {
    _simulator: Running,
    _proxy: Running,
    serving: u16,
    shared: u16,
}

impl Served {
    fn start() -> Self {
        let device = UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let simulator = spawn(&[
            "simulate",
            "--port",
            &device.to_string(),
            "--listen",
            "loopback",
            "--no-drop",
            "--layout",
            "/,/1",
        ]);
        let serving = free_tcp();
        let proxy = spawn(&[
            "proxy",
            "-p",
            &serving.to_string(),
            "--listen",
            "loopback",
            "--mdns",
            &format!("udp://127.0.0.1:{device}"),
        ]);
        let served = Self {
            _simulator: simulator,
            _proxy: proxy,
            serving,
            shared: free_tcp(),
        };
        let deadline = Instant::now() + WAIT;
        while !served
            .tio(&["rpc", "-r", &served.url(), "dev.name"])
            .status
            .success()
        {
            assert!(
                Instant::now() < deadline,
                "tio proxy never served the simulator"
            );
            thread::sleep(Duration::from_millis(200));
        }
        served
    }

    /// The URL of `tio proxy`.
    fn url(&self) -> String {
        format!("tcp://127.0.0.1:{}", self.serving)
    }

    /// Runs `tio args` to its end with the shared proxy as the machine's.
    fn tio(&self, args: &[&str]) -> Output {
        tio()
            .args(args)
            .env("TWINLEAF_PROXY", self.shared_url())
            .output()
            .unwrap()
    }

    fn shared_url(&self) -> String {
        format!("tcp://127.0.0.1:{}", self.shared)
    }
}

fn tio() -> Command {
    Command::new(env!("CARGO_BIN_EXE_tio"))
}

fn spawn(args: &[&str]) -> Running {
    Running(
        tio()
            .args(args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    )
}

fn free_tcp() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// How long the picker has to show the tree.
const BROWSE: Duration = Duration::from_secs(8);

/// `tio list` finds the tree `tio proxy` advertises on this machine at loopback, with the name
/// and route of each device, and `tio rpc` reaches each there. A pick in the picker then holds
/// the tree as the shared proxy's default, which `tio rpc` naming no device reaches, and the hold
/// stays up while the device serves and after it goes away. One test runs it all, as a second
/// would advertise the same name.
#[test]
fn a_served_tree_is_listed_answers_and_is_held_by_a_pick() {
    let served = Served::start();
    let listed = served.tio(&["list", "--json", "--duration", "4s"]);
    let devices: serde_json::Value = serde_json::from_slice(&listed.stdout).unwrap();
    let ours = devices
        .as_array()
        .and_then(|devices| devices.iter().find(|device| device["url"] == served.url()))
        .expect("tio list finds the tree");
    assert_eq!(ours["name"], "tio-test");
    assert_eq!(
        ours["routes"],
        serde_json::json!([{"route": "/1", "name": "tio-test"}])
    );
    for route in ["/", "/1"] {
        let answered = served.tio(&["rpc", "-r", &served.url(), "-s", route, "dev.name"]);
        assert!(
            stdout(&answered).contains("\"tio-test\""),
            "{route}: {answered:?}"
        );
    }

    let mut picker = Picker::open(&served.shared_url());
    assert!(
        picker.shows("tio-test", BROWSE),
        "the picker shows the tree"
    );
    let mut shared = tio();
    shared.args(["proxy", "--background", "-p", &served.shared.to_string()]);
    let shared = Running(
        shared
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + WAIT;
    while std::net::TcpStream::connect(("127.0.0.1", served.shared)).is_err() {
        assert!(Instant::now() < deadline, "the shared proxy never listened");
        thread::sleep(Duration::from_millis(50));
    }
    let deadline = Instant::now() + WAIT;
    while !picker.highlights("tio-test", Duration::from_millis(500)) {
        assert!(
            Instant::now() < deadline,
            "the picker never highlights the tree"
        );
        picker.press(b"\t");
    }
    picker.press(b"\r");
    assert!(
        picker.shows("as the default while this runs", WAIT),
        "the pick is held"
    );

    let answered = served.tio(&["rpc", "dev.name"]);
    assert!(stdout(&answered).contains("\"tio-test\""), "{answered:?}");
    thread::sleep(Duration::from_secs(2));
    assert!(
        picker.running.0.try_wait().unwrap().is_none(),
        "the hold let go"
    );
    drop(served);
    thread::sleep(Duration::from_secs(2));
    assert!(
        picker.running.0.try_wait().unwrap().is_none(),
        "the hold let go with its device"
    );
    drop(picker);
    drop(shared);
}

/// `tio list` on a terminal of 120 by 40, and everything it has drawn.
struct Picker {
    running: Running,
    screen: Arc<Mutex<Vec<u8>>>,
}

impl Picker {
    /// Opens the picker with `proxy` as the machine's proxy.
    fn open(proxy: &str) -> Self {
        let command = format!(
            "stty rows 40 cols 120; exec {} list",
            env!("CARGO_BIN_EXE_tio")
        );
        let mut script = Command::new("script");
        match cfg!(target_os = "macos") {
            true => script.args(["-q", "/dev/null", "sh", "-c", &command]),
            false => script.args(["-q", "-e", "-c", &command, "/dev/null"]),
        };
        let mut running = Running(
            script
                .env("TWINLEAF_PROXY", proxy)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let screen = Arc::new(Mutex::new(Vec::new()));
        let (mut out, shown) = (running.0.stdout.take().unwrap(), Arc::clone(&screen));
        thread::spawn(move || {
            let mut buf = [0u8; 4096];
            while let Ok(read @ 1..) = out.read(&mut buf) {
                shown.lock().unwrap().extend_from_slice(&buf[..read]);
            }
        });
        Self { running, screen }
    }

    /// Whether it draws `text` within `within`.
    fn shows(&self, text: &str, within: Duration) -> bool {
        self.draws(within, |screen| screen.contains(text))
    }

    /// Whether the row it highlights last holds `text` within `within`, as another device on
    /// the network may be the one it highlights.
    fn highlights(&self, text: &str, within: Duration) -> bool {
        self.draws(within, |screen| {
            screen
                .rsplit('❯')
                .next()
                .and_then(|row| row.split('\x1b').next())
                .is_some_and(|row| row.contains(text))
        })
    }

    /// Whether what it has drawn passes `drawn` within `within`.
    fn draws(&self, within: Duration, drawn: impl Fn(&str) -> bool) -> bool {
        let deadline = Instant::now() + within;
        while !drawn(&String::from_utf8_lossy(&self.screen.lock().unwrap())) {
            if Instant::now() >= deadline {
                return false;
            }
            thread::sleep(Duration::from_millis(100));
        }
        true
    }

    /// Types `keys` into it.
    fn press(&mut self, keys: &[u8]) {
        self.running
            .0
            .stdin
            .as_mut()
            .unwrap()
            .write_all(keys)
            .unwrap();
    }
}
