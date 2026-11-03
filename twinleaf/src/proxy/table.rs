//! The server's mailbox and what every client's task reads, which the
//! proxy's server and its clients' tasks share.

use super::link::Links;
use super::{HostOptions, Layout};
use crate::chan::oneshot;
use crate::rt::Supervisor;
use crate::session::Introduction;
use std::sync::Mutex;
use tokio::sync::{mpsc, watch};

/// What the server is told, by the proxy's handle and its clients' tasks.
pub(super) enum Notice {
    /// A change to client `id`'s entry in the table. The server answers on
    /// `done` once the table shows it.
    Update {
        id: u64,
        update: TableChange,
        done: oneshot::Sender<()>,
    },
    /// A stop of the proxy.
    Stop,
}

/// A change to a client's entry in the table.
pub(super) enum TableChange {
    /// The client introduced itself with `proxy.hello`.
    Hello(Introduction),
    /// The client opened this target.
    Target(String),
}

/// What every client's task reads, none of which keeps the proxy running.
pub(super) struct Shared {
    /// How the proxy is hosted. Its default layout is held in `default`.
    pub(super) options: HostOptions,
    pub(super) endpoint: String,
    pub(super) links: Links,
    /// The spawner of the clients' transports and blocking steps.
    pub(super) tasks: Supervisor,
    /// The server's mailbox.
    pub(super) server: mpsc::WeakUnboundedSender<Notice>,
    /// The default layout, which the host sets and a client may hold.
    pub(super) default: watch::Sender<Option<Layout>>,
    /// The id and address of the client holding the default with `proxy.default`.
    pub(super) holder: Mutex<Option<(u64, String)>>,
}

impl Shared {
    /// Returns the layout `default` opens.
    pub(super) fn default(&self) -> Option<Layout> {
        self.default.borrow().clone()
    }
}
