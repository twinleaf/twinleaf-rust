//! The options of a test proxy and fake openers of devices, for the unit
//! tests of `proxy` and of the modules above it, on a paused clock.

use crate::proxy::link::Opens;
use crate::proxy::worker::Opener;
use crate::proxy::HostOptions;
use crate::tio::tests::PAUSED_WAIT;
use crate::tio::transport::tests::fake::{self, FakeEnd};
use crate::tio::transport::{Port, TransportKind};
use std::collections::HashMap;
use std::future;
use std::io;
use std::sync::{Arc, Barrier};
use tokio::sync::mpsc;

/// Returns the options a test hosts a proxy with. The proxy listens on
/// loopback on a free port and has no default.
pub(crate) fn options() -> HostOptions {
    HostOptions {
        port: 0,
        name: "a test".to_string(),
        ..HostOptions::default()
    }
}

/// Returns how many Twinleaf serial devices are attached to this machine.
/// `auto` considers them too.
pub(crate) fn attached() -> usize {
    crate::proxy::auto::attached().len()
}

/// How a fake opener opens the device of the first link to a URL.
#[derive(Clone)]
pub(crate) enum First {
    /// The open blocks a thread until the test meets it at the gate.
    Blocks(Arc<Barrier>),
    /// The open panics.
    Panics,
}

/// A fake device port that a fake opener opened.
pub(crate) struct Opened {
    /// The URL of the link it opened.
    pub(crate) url: String,
    /// The device's end of the port.
    pub(crate) end: FakeEnd,
}

/// Returns what opens each new link's device as a fake TCP port on the
/// test's runtime, and the receiver of each port it opens. The first link to
/// each URL in `first` opens as it says. Every other open succeeds at once.
pub(in crate::proxy) fn fakes(
    first: Vec<(&str, First)>,
) -> (Opens, mpsc::UnboundedReceiver<Opened>) {
    let (sender, opened) = mpsc::unbounded_channel();
    let mut first: HashMap<String, First> = first
        .into_iter()
        .map(|(url, how)| (url.to_string(), how))
        .collect();
    let opens = move |url: &str| -> Opener {
        let how = first.remove(url);
        let (url, sender) = (url.to_string(), sender.clone());
        Opener::new(
            move |results| -> crate::rt::BoxFuture<'static, io::Result<Port>> {
                let open = {
                    let (url, sender) = (url.clone(), sender.clone());
                    move || {
                        let (port, end) = fake::port(TransportKind::Tcp, None, results);
                        let _ = sender.send(Opened { url, end });
                        Ok(port)
                    }
                };
                match how.clone() {
                    None => Box::pin(future::ready(open())),
                    Some(First::Blocks(gate)) => Box::pin(async move {
                        let opening = tokio::task::spawn_blocking(move || {
                            gate.wait();
                            open()
                        });
                        opening.await.map_err(io::Error::other)?
                    }),
                    Some(First::Panics) => panic!("the opener of {url} panics"),
                }
            },
        )
    };
    (Box::new(opens), opened)
}

/// Waits for the next fake device port that `opened` hands over.
pub(crate) async fn opened(opened: &mut mpsc::UnboundedReceiver<Opened>) -> Opened {
    let next = tokio::time::timeout(PAUSED_WAIT, opened.recv()).await;
    next.expect("no device was opened")
        .expect("the opener is gone")
}
