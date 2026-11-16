//! The MCP endpoint of a proxy's web page, here a proxy of the test's own
//! on a loopback port over the simulated device. Every tool an agent calls
//! is a line of the log, and `tio mcp` bridges a host's stdio to it.

#![cfg(feature = "web")]

use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::AtomicBool;
use std::sync::{mpsc, Arc, OnceLock};
use std::time::{Duration, Instant};
use twinleaf::proxy::{HostOptions, Layout, Mount, Proxy};
use twinleaf::DeviceRoute;

/// How long a test waits for the hub, on a machine however busy.
const WAIT: Duration = Duration::from_secs(60);

/// The proxy over the simulated device whose page has its MCP endpoint
/// on, which the tests share.
fn proxy() -> &'static Proxy {
    static PROXY: OnceLock<Proxy> = OnceLock::new();
    PROXY.get_or_init(|| {
        let stop = Arc::new(AtomicBool::new(false));
        let url = twinleaf_tools::tools::simulate::serve(&["--no-drop"], stop);
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
            foreign: Some(twinleaf_web::handler(true)),
            ..HostOptions::default()
        })
        .unwrap();
        // The hub the endpoint's first tool call starts finds its proxy by this.
        std::env::set_var("TWINLEAF_PROXY", proxy.endpoint());
        proxy
    })
}

/// A proxy with no device whose page has its MCP endpoint off.
fn closed() -> &'static Proxy {
    static PROXY: OnceLock<Proxy> = OnceLock::new();
    PROXY.get_or_init(|| {
        Proxy::host(HostOptions {
            port: 0,
            name: "a test".to_string(),
            foreign: Some(twinleaf_web::handler(false)),
            ..HostOptions::default()
        })
        .unwrap()
    })
}

fn port(proxy: &Proxy) -> String {
    proxy.endpoint().rsplit(':').next().unwrap().to_string()
}

/// A `tio mcp` the test writes to and reads from as a host does.
struct Bridge {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: mpsc::Receiver<String>,
    next_id: u64,
}

impl Bridge {
    /// Starts `tio mcp` to go through the proxy at `proxy`.
    fn start(proxy: &str) -> Bridge {
        let mut child = Command::new(env!("CARGO_BIN_EXE_tio"))
            .arg("mcp")
            .env("TWINLEAF_PROXY", proxy)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { return };
                if tx.send(line).is_err() {
                    return;
                }
            }
        });
        let stdin = child.stdin.take();
        Bridge {
            child,
            stdin,
            lines,
            next_id: 1,
        }
    }

    fn write(&mut self, message: &Value) {
        let stdin = self.stdin.as_mut().unwrap();
        writeln!(stdin, "{message}").unwrap();
        stdin.flush().unwrap();
    }

    /// Sends a request of `method` with `params`, and returns its reply.
    fn ask(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        self.write(&json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }));
        loop {
            let line = self.lines.recv_timeout(WAIT).expect("a reply in time");
            let reply: Value = serde_json::from_str(&line).unwrap();
            if reply["id"] == id {
                return reply;
            }
        }
    }

    /// Closes stdin, and waits for the bridge to end.
    fn finish(mut self) -> std::process::ExitStatus {
        drop(self.stdin.take());
        self.child.wait().unwrap()
    }
}

#[test]
fn tio_mcp_forwards_initialize_and_the_tool_list() {
    let mut bridge = Bridge::start(&proxy().endpoint());
    let params = json!({
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "clientInfo": { "name": "a test", "version": "0" }
    });
    let reply = bridge.ask("initialize", params);
    assert_eq!(reply["result"]["serverInfo"]["name"], "twinleaf-web");
    bridge.write(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }));
    let reply = bridge.ask("tools/list", json!({}));
    let mut tools: Vec<&str> = (reply["result"]["tools"].as_array().unwrap().iter())
        .map(|tool| tool["name"].as_str().unwrap())
        .collect();
    tools.sort();
    let expected = [
        "call",
        "export",
        "glance",
        "list_devices",
        "list_rpcs",
        "log",
        "read_rows",
    ];
    assert_eq!(tools, expected);
    assert!(bridge.finish().success());
}

#[test]
fn tio_mcp_says_the_endpoint_is_off_and_fails() {
    let output = Command::new(env!("CARGO_BIN_EXE_tio"))
        .arg("mcp")
        .env("TWINLEAF_PROXY", closed().endpoint())
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("is off"), "{stderr}");
    let nothing = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let unused = format!("tcp://{}", nothing.local_addr().unwrap());
    drop(nothing);
    let output = Command::new(env!("CARGO_BIN_EXE_tio"))
        .arg("mcp")
        .env("TWINLEAF_PROXY", &unused)
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("no web page of a proxy answers"),
        "{stderr}"
    );
}

/// Posts the JSON-RPC request of `method` with `params` to the endpoint
/// on `port`, and returns its reply.
fn post(port: &str, method: &str, params: Value) -> Value {
    let body = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
    let body = body.to_string();
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream.set_read_timeout(Some(WAIT)).unwrap();
    write!(
        stream,
        "POST /mcp HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Type: application/json\r\n\
         Accept: application/json, text/event-stream\r\nConnection: close\r\n\
         Content-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut reply = String::new();
    stream.read_to_string(&mut reply).unwrap();
    let (head, body) = reply.split_once("\r\n\r\n").unwrap();
    assert!(head.starts_with("HTTP/1.1 200"), "{head}");
    serde_json::from_str(body).unwrap()
}

/// The text `tool` gives for `args` through the endpoint on `port`, and
/// whether it failed.
fn tool(port: &str, tool: &str, args: Value) -> (bool, String) {
    let reply = post(
        port,
        "tools/call",
        json!({ "name": tool, "arguments": args }),
    );
    let result = &reply["result"];
    let text = result["content"][0]["text"].as_str().unwrap_or_default();
    (result["isError"] == true, text.to_string())
}

/// The text `tool` gives for `args`, which must not fail.
fn text(port: &str, name: &str, args: Value) -> String {
    let (failed, text) = tool(port, name, args);
    assert!(!failed, "{name} failed: {text}");
    text
}

#[test]
fn each_tool_call_is_a_line_of_the_log() {
    let port = port(proxy());
    let start = Instant::now();
    while !tool(&port, "list_devices", json!({}))
        .1
        .contains("sine.sine")
    {
        assert!(start.elapsed() < WAIT, "the hub found no signal");
        std::thread::sleep(Duration::from_millis(100));
    }
    // Rows of a second, so a glance and a read have rows.
    std::thread::sleep(Duration::from_secs(1));
    let signal = json!(["sine.sine"]);
    text(
        &port,
        "list_rpcs",
        json!({ "route": "/", "filter": "dev." }),
    );
    text(
        &port,
        "call",
        json!({ "name": "dev.name", "routes": ["/"] }),
    );
    text(
        &port,
        "glance",
        json!({ "signals": signal, "view": "time" }),
    );
    text(
        &port,
        "read_rows",
        json!({ "signal": "sine.sine", "limit": 2 }),
    );
    let exported = text(
        &port,
        "export",
        json!({ "signals": signal, "format": "csv" }),
    );
    let path = exported.lines().next().unwrap().strip_prefix("wrote ");
    std::fs::remove_file(path.unwrap()).unwrap();
    let log = text(&port, "log", json!({ "lines": 200 }));
    for said in [
        "list_devices",
        "list_rpcs {\"route\":\"/\",\"filter\":\"dev.\"}",
        "call {\"name\":\"dev.name\",\"routes\":[\"/\"]}",
        "glance {\"signals\":[\"sine.sine\"],\"view\":\"time\"}",
        "read_rows {\"signal\":\"sine.sine\",\"limit\":2}",
        "export {\"signals\":[\"sine.sine\"],\"format\":\"csv\"}",
        "log {\"lines\":200}",
    ] {
        let said = format!("agent: {said}");
        assert!(log.contains(&said), "{said} is not in the log\n{log}");
    }
}
