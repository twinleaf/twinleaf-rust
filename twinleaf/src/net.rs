//! The crate's TCP sockets and name lookup.
//!
//! Built with `--cfg turmoil`, the crate's tests run them over turmoil's
//! simulated network, where hosts in one process connect, lose their links
//! and restart on a virtual clock. They are built with
//! `--cfg tokio_unstable` too, under which turmoil seeds each host's tokio
//! runtime, so the order a `select!` tries its branches in comes from the
//! scenario's seed. The command is
//!
//! ```text
//! RUSTFLAGS="--cfg turmoil --cfg tokio_unstable" cargo test -p twinleaf --lib --target-dir target/turmoil turmoil
//! ```

#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::io;
use std::net::SocketAddr;

#[cfg(not(all(test, turmoil)))]
pub(crate) type TcpStream = tokio::net::TcpStream;
#[cfg(not(all(test, turmoil)))]
pub(crate) type TcpListener = tokio::net::TcpListener;
#[cfg(all(test, turmoil))]
pub(crate) use counted::{
    bytes_read, forget_live_streams, live_streams_on, TcpListener, TcpStream,
};

/// Returns the addresses `address`, a `host:port`, resolves to.
#[cfg(not(all(test, turmoil)))]
pub(crate) async fn lookup_host(address: String) -> io::Result<Vec<SocketAddr>> {
    Ok(tokio::net::lookup_host(address).await?.collect())
}

/// Returns the address of the simulated host `address` names.
#[cfg(all(test, turmoil))]
pub(crate) async fn lookup_host(address: String) -> io::Result<Vec<SocketAddr>> {
    if let Ok(found) = address.parse::<SocketAddr>() {
        return Ok(vec![found]);
    }
    let (host, port) = address
        .rsplit_once(':')
        .and_then(|(host, port)| Some((host, port.parse::<u16>().ok()?)))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, address.clone()))?;
    Ok(vec![SocketAddr::new(turmoil::lookup(host), port)])
}

/// turmoil's sockets, each stream counted on its host while it lives.
/// turmoil's own count takes a stream before its handshake and keeps one
/// whose connect was refused or timed out, so it does not say how many a
/// host holds.
#[cfg(all(test, turmoil))]
mod counted {
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::io;
    use std::net::{IpAddr, SocketAddr};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    thread_local! {
        /// The count of live streams of each host, by its address, on this
        /// thread, which runs one simulation at a time.
        static LIVE: RefCell<BTreeMap<IpAddr, Arc<AtomicUsize>>> = RefCell::default();
        /// How many bytes each stream has read, by its local and peer
        /// address, on this thread.
        static READ: RefCell<BTreeMap<(SocketAddr, SocketAddr), Arc<AtomicU64>>> = RefCell::default();
    }

    /// Returns how many bytes the stream from `local` to `peer` has read, as
    /// its peer would see them acknowledged. A stream that was dropped keeps
    /// its count.
    pub(crate) fn bytes_read(local: SocketAddr, peer: SocketAddr) -> u64 {
        READ.with(|read| {
            let read = read.borrow();
            read.get(&(local, peer))
                .map_or(0, |count| count.load(Ordering::SeqCst))
        })
    }

    /// A new counter of the bytes the stream from `local` to `peer` reads,
    /// which replaces that of an earlier stream between the two addresses.
    fn reading(local: SocketAddr, peer: SocketAddr) -> Arc<AtomicU64> {
        let count = Arc::new(AtomicU64::new(0));
        READ.with(|read| read.borrow_mut().insert((local, peer), count.clone()));
        count
    }

    /// Returns how many streams the host at `host` holds now, each counted
    /// from the connect or accept that made it to its drop.
    pub(crate) fn live_streams_on(host: IpAddr) -> usize {
        LIVE.with(|live| {
            let live = live.borrow();
            live.get(&host)
                .map_or(0, |count| count.load(Ordering::SeqCst))
        })
    }

    /// Forgets every count, as a new simulation starts on this thread. A
    /// stream of an earlier one that is dropped later counts against that
    /// simulation's host.
    pub(crate) fn forget_live_streams() {
        LIVE.with(|live| live.borrow_mut().clear());
        READ.with(|read| read.borrow_mut().clear());
    }

    /// One live stream of a host, counted until it is dropped.
    #[derive(Debug)]
    struct Live(Arc<AtomicUsize>);

    impl Live {
        fn on(host: IpAddr) -> Live {
            let count = LIVE.with(|live| live.borrow_mut().entry(host).or_default().clone());
            count.fetch_add(1, Ordering::SeqCst);
            Live(count)
        }
    }

    impl Drop for Live {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// A turmoil stream or one of its halves, counted on its host while it
    /// lives. The two halves of a stream share its count until both are
    /// dropped.
    #[derive(Debug)]
    pub(crate) struct Counted<T> {
        inner: T,
        live: Arc<Live>,
        /// The count of the bytes the stream has read.
        read: Arc<AtomicU64>,
    }

    /// A turmoil stream, counted on its host while it lives.
    pub(crate) type TcpStream = Counted<turmoil::net::TcpStream>;

    /// The read half of a counted stream.
    pub(crate) type ReadHalf = Counted<turmoil::net::tcp::OwnedReadHalf>;

    /// The write half of a counted stream.
    pub(crate) type WriteHalf = Counted<turmoil::net::tcp::OwnedWriteHalf>;

    impl TcpStream {
        /// Counts `inner` as live on its host until it and its halves are
        /// dropped.
        fn on_its_host(inner: turmoil::net::TcpStream) -> io::Result<TcpStream> {
            let local = inner.local_addr()?;
            let live = Arc::new(Live::on(local.ip()));
            let read = reading(local, inner.peer_addr()?);
            Ok(Counted { inner, live, read })
        }

        pub(crate) fn local_addr(&self) -> io::Result<SocketAddr> {
            self.inner.local_addr()
        }

        /// Connects to `address`, as turmoil's stream does.
        pub(crate) async fn connect(address: SocketAddr) -> io::Result<TcpStream> {
            TcpStream::on_its_host(turmoil::net::TcpStream::connect(address).await?)
        }

        pub(crate) fn peer_addr(&self) -> io::Result<SocketAddr> {
            self.inner.peer_addr()
        }

        pub(crate) fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
            self.inner.set_nodelay(nodelay)
        }

        /// Splits the stream into its halves.
        pub(crate) fn into_split(self) -> (ReadHalf, WriteHalf) {
            let (reading, writing) = self.inner.into_split();
            let read = Counted {
                inner: reading,
                live: self.live.clone(),
                read: self.read.clone(),
            };
            let write = Counted {
                inner: writing,
                live: self.live,
                read: self.read,
            };
            (read, write)
        }
    }

    impl<T: AsyncRead + Unpin> AsyncRead for Counted<T> {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let before = buf.filled().len();
            let polled = Pin::new(&mut self.inner).poll_read(cx, buf);
            let read = (buf.filled().len() - before) as u64;
            self.read.fetch_add(read, Ordering::SeqCst);
            polled
        }
    }

    impl<T: AsyncWrite + Unpin> AsyncWrite for Counted<T> {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.inner).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    /// A turmoil listener whose streams are counted.
    pub(crate) struct TcpListener(turmoil::net::TcpListener);

    impl TcpListener {
        /// Listens on `address`, as turmoil's listener does.
        pub(crate) async fn bind(address: impl Into<SocketAddr>) -> io::Result<TcpListener> {
            let listener = turmoil::net::TcpListener::bind(address.into()).await?;
            Ok(TcpListener(listener))
        }

        /// Accepts a connection, as turmoil's listener does.
        pub(crate) async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
            let (stream, peer) = self.0.accept().await?;
            Ok((TcpStream::on_its_host(stream)?, peer))
        }

        pub(crate) fn local_addr(&self) -> io::Result<SocketAddr> {
            self.0.local_addr()
        }
    }
}
