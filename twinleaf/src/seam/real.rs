//! The primitives of a normal build, which are flume's channels, tokio's
//! tasks, timers, sockets and stream sets, and the standard library's locks.
//!
//! The test backends start from these and replace what they model.
// A test backend replaces some of these, which are then unused.
#![cfg_attr(all(test, any(shuttle, turmoil)), allow(dead_code, unused_imports))]

/// Channels, locks, timers and task spawns.
pub(crate) mod chan {
    use std::future::Future;
    use std::hash::Hash;
    use std::sync::{Mutex, MutexGuard};
    use tokio::runtime::Handle;
    use tokio_util::task::{JoinMap, TaskTracker};

    // Aliases and wrappers rather than re-exports, so that clippy, which
    // sees through a re-export, tells their uses from the library's.
    pub(crate) use flume::{RecvError, RecvTimeoutError, TryRecvError, TrySendError};
    pub(crate) type Sender<T> = flume::Sender<T>;
    pub(crate) type Receiver<T> = flume::Receiver<T>;
    pub(crate) type RecvFut<'a, T> = flume::r#async::RecvFut<'a, T>;
    pub(crate) type RecvStream<'a, T> = flume::r#async::RecvStream<'a, T>;
    pub(crate) type Selector<'a, T> = flume::Selector<'a, T>;
    pub(crate) type Notify = tokio::sync::Notify;
    pub(crate) type JoinHandle<T> = tokio::task::JoinHandle<T>;
    pub(crate) type AbortOnDropHandle<T> = tokio_util::task::AbortOnDropHandle<T>;
    pub(crate) type StreamMap<K, V> = tokio_stream::StreamMap<K, V>;

    /// The receiver of an mDNS search's answers, a flume channel that the
    /// mDNS library makes.
    #[cfg(feature = "mdns")]
    pub(crate) type MdnsReceiver<T> = mdns_sd::Receiver<T>;

    /// Returns a channel with room for `capacity` items.
    pub(crate) fn bounded<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
        flume::bounded(capacity)
    }

    /// Returns a channel with room for any number of items.
    pub(crate) fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
        flume::unbounded()
    }

    /// Channels that carry one item.
    pub(crate) mod oneshot {
        pub(crate) use tokio::sync::oneshot::error;

        pub(crate) type Sender<T> = tokio::sync::oneshot::Sender<T>;
        pub(crate) type Receiver<T> = tokio::sync::oneshot::Receiver<T>;

        /// Returns a channel that carries one item.
        pub(crate) fn channel<T>() -> (Sender<T>, Receiver<T>) {
            tokio::sync::oneshot::channel()
        }
    }

    /// Waits until `deadline`.
    pub(crate) fn sleep_until(deadline: tokio::time::Instant) -> tokio::time::Sleep {
        tokio::time::sleep_until(deadline)
    }

    /// Locks `mutex`, past a panic of another holder. It suits a lock that
    /// guards no change a panic could leave half made.
    pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
        mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Spawns `future` as a task of the runtime of `handle`.
    pub(crate) fn spawn_on<F>(handle: &Handle, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        handle.spawn(future)
    }

    /// Spawns `future` into `map` under `key`, as a task of the runtime of
    /// `handle`.
    pub(crate) fn spawn_in_map<K, F>(
        map: &mut JoinMap<K, F::Output>,
        key: K,
        future: F,
        handle: &Handle,
    ) where
        K: Hash + Eq,
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        map.spawn_on(key, future, handle);
    }

    /// Runs `work`, tracked by `tracker`, on a thread of the runtime of
    /// `handle` where it may block.
    pub(crate) fn spawn_blocking_on<R>(
        tracker: &TaskTracker,
        work: impl FnOnce() -> R + Send + 'static,
        handle: &Handle,
    ) -> tokio::task::JoinHandle<R>
    where
        R: Send + 'static,
    {
        tracker.spawn_blocking_on(work, handle)
    }

    /// Spawns `future` as a task of the runtime it is called from.
    ///
    /// # Panics
    ///
    /// Panics if it is called outside a runtime.
    #[cfg(test)]
    pub(crate) fn spawn<F>(future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::spawn(future)
    }
}

/// TCP sockets and name lookup.
pub(crate) mod net {
    use socket2::{Domain, Protocol, SockRef, Socket, Type};
    use std::future::{self, Future};
    use std::io;
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
    use std::task::Poll;

    pub(crate) type TcpStream = tokio::net::TcpStream;
    pub(crate) type TcpListener = tokio::net::TcpListener;

    /// Returns the addresses `address`, a `host:port`, resolves to.
    pub(crate) async fn lookup_host(address: String) -> io::Result<Vec<SocketAddr>> {
        Ok(tokio::net::lookup_host(address).await?.collect())
    }

    /// Connects to `address` as [`TcpStream::connect`] does. A loopback
    /// address that nothing listens on refuses the connection at once, on
    /// Windows too.
    pub(crate) async fn connect(address: SocketAddr) -> io::Result<TcpStream> {
        let socket = match address {
            SocketAddr::V4(_) => tokio::net::TcpSocket::new_v4()?,
            SocketAddr::V6(_) => tokio::net::TcpSocket::new_v6()?,
        };
        if address.ip().is_loopback() {
            refuse_at_once(&socket);
        }
        socket.connect(address).await
    }

    /// Returns the first byte `stream` has received, leaving it to be read,
    /// or `None` once the peer has closed without sending one.
    pub(crate) async fn peek_first(stream: &mut TcpStream) -> io::Result<Option<u8>> {
        let mut first = [0];
        Ok((stream.peek(&mut first).await? == 1).then_some(first[0]))
    }

    /// Returns `stream` as the socket a [`Foreign`](crate::seam::Foreign)
    /// handler takes.
    pub(crate) fn into_foreign(stream: TcpStream) -> Option<crate::seam::ForeignStream> {
        Some(stream)
    }

    /// Makes a connect on `socket` give up after the first refusal. Windows
    /// otherwise sends the handshake twice more, so a closed port takes
    /// about 2 s to refuse.
    #[cfg(windows)]
    fn refuse_at_once(socket: &impl std::os::windows::io::AsRawSocket) {
        use windows_sys::Win32::Networking::WinSock::{
            WSAIoctl, SIO_TCP_INITIAL_RTO, TCP_INITIAL_RTO_PARAMETERS,
        };
        // mstcpip.h gives these as (USHORT)-1 and (UCHAR)-2. windows-sys has
        // no name for the first, and types the second as a u16.
        const UNSPECIFIED_RTT: u16 = u16::MAX;
        const NO_SYN_RETRANSMISSIONS: u8 = u8::MAX - 1;
        let parameters = TCP_INITIAL_RTO_PARAMETERS {
            Rtt: UNSPECIFIED_RTT,
            MaxSynRetransmissions: NO_SYN_RETRANSMISSIONS,
        };
        let mut returned = 0;
        // SAFETY: the socket is open, and the input is a
        // TCP_INITIAL_RTO_PARAMETERS of the size given. The call writes
        // nothing but `returned`.
        let failed = unsafe {
            WSAIoctl(
                socket.as_raw_socket() as _,
                SIO_TCP_INITIAL_RTO,
                std::ptr::from_ref(&parameters).cast(),
                std::mem::size_of_val(&parameters) as u32,
                std::ptr::null_mut(),
                0,
                &mut returned,
                std::ptr::null_mut(),
                None,
            )
        } != 0;
        if failed {
            log::debug!(
                "a loopback connect keeps retrying: {}",
                io::Error::last_os_error()
            );
        }
    }

    #[cfg(not(windows))]
    fn refuse_at_once<T>(_socket: &T) {}

    /// The listening sockets of a proxy. A connection may arrive on any of
    /// them.
    #[derive(Debug, Default)]
    pub(crate) struct Listeners(Vec<TcpListener>);

    impl Listeners {
        /// Listens on `port` of each of the IPv4 addresses `v4`, in order,
        /// and then of the IPv6 address `v6`. The first IPv4 socket takes
        /// the port, which is any free one if `port` is 0, and the rest
        /// share it. The IPv6 socket takes no IPv4. A host without IPv6
        /// gets the IPv4 sockets alone. Must be awaited on a tokio runtime,
        /// which the sockets belong to.
        ///
        /// # Errors
        ///
        /// Returns an error if an IPv4 address cannot be bound, or a socket
        /// cannot join the runtime.
        pub(crate) async fn bind(
            v4: &[Ipv4Addr],
            v6: Ipv6Addr,
            port: u16,
        ) -> io::Result<Listeners> {
            let first = std::net::TcpListener::bind((v4[0], port))?;
            let port = first.local_addr()?.port();
            let rest = v4[1..]
                .iter()
                .map(|v4| std::net::TcpListener::bind((*v4, port)));
            let mut listeners = std::iter::once(Ok(first))
                .chain(rest)
                .collect::<io::Result<Vec<_>>>()?;
            match bind_v6_only((v6, port).into()) {
                Ok(v6) => listeners.push(v6),
                Err(error) => log::debug!("Not listening on [{v6}]:{port}: {error}"),
            }
            Listeners::from_std(listeners)
        }

        /// Takes `listeners`, bound already, onto the runtime it is called
        /// from.
        pub(crate) fn from_std(listeners: Vec<std::net::TcpListener>) -> io::Result<Listeners> {
            let listening = listeners.into_iter().map(|listener| {
                listener.set_nonblocking(true)?;
                TcpListener::from_std(listener)
            });
            Ok(Listeners(listening.collect::<io::Result<_>>()?))
        }

        /// Returns the port the listeners share.
        pub(crate) fn port(&self) -> io::Result<u16> {
            let first = self.0.first().ok_or(io::ErrorKind::NotConnected)?;
            Ok(first.local_addr()?.port())
        }

        /// Returns the listening sockets, in the order they were bound.
        #[cfg(test)]
        pub(crate) fn sockets(&self) -> &[TcpListener] {
            &self.0
        }

        /// Closes every listener.
        pub(crate) fn close(&mut self) {
            self.0.clear();
        }

        /// Waits for a connection on any listener. Never returns while there
        /// is none.
        pub(crate) fn accept(
            &self,
        ) -> impl Future<Output = io::Result<(TcpStream, SocketAddr)>> + '_ {
            future::poll_fn(move |cx| {
                for listener in &self.0 {
                    if let Poll::Ready(accepted) = listener.poll_accept(cx) {
                        return Poll::Ready(accepted);
                    }
                }
                Poll::Pending
            })
        }

        /// Takes each connection already waiting, though the runtime may not
        /// have seen it arrive, with the error of any that cannot be served.
        pub(crate) fn waiting(&self) -> Vec<io::Result<(TcpStream, SocketAddr)>> {
            let mut waiting = Vec::new();
            for listener in &self.0 {
                loop {
                    let socket = match SockRef::from(listener).accept() {
                        Ok((socket, _)) => socket,
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                        Err(error) => {
                            log::warn!("Error accepting client: {error}");
                            break;
                        }
                    };
                    let stream = std::net::TcpStream::from(socket);
                    waiting.push(stream.set_nonblocking(true).and_then(|()| {
                        let peer = stream.peer_addr()?;
                        Ok((TcpStream::from_std(stream)?, peer))
                    }));
                }
            }
            waiting
        }
    }

    /// Listens on the IPv6 `address` like [`std::net::TcpListener::bind`],
    /// with a socket that takes no IPv4.
    fn bind_v6_only(address: SocketAddr) -> io::Result<std::net::TcpListener> {
        let socket = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))?;
        socket.set_only_v6(true)?;
        #[cfg(not(windows))]
        socket.set_reuse_address(true)?;
        socket.bind(&address.into())?;
        socket.listen(128)?;
        Ok(socket.into())
    }
}
