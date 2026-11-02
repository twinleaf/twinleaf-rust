//! Generated fault schedules over turmoil. Each case starts the fake device,
//! the proxy and one to three apps on hosts of their own, applies a generated
//! schedule of faults to the links and hosts between them, heals everything,
//! and then waits for the system to come back. A checker judges the log the
//! hosts write against the laws below.
//!
//! Paths are relative to `twinleaf/src`, and turmoil's paths point into the
//! source of turmoil 0.7.2. Each law names the doc it comes from. None comes
//! from the code that implements it.
//!
//! The files of this module follow a case. `schedule` draws and lays out its
//! faults, `hosts` runs the apps and the proxy and writes what they see,
//! `driver` runs the case through its phases, `model` reads the log into
//! spans and connections, `laws` judges it, `classes` counts what it reached,
//! `report` runs and reports the generated cases, and `examples` holds the
//! fixed ones.
//!
//! # 1. Observations
//!
//! Every event carries the virtual time in milliseconds, as
//! `turmoil::sim_elapsed()` gives it, and the host that saw it. Hosts append
//! their events to the shared `World` in the order they happen.
//!
//! The driver writes
//! - each fault as the interpreter applies it at `sim.elapsed()`, and its
//!   end, with the pair or host it touches,
//! - the heal, at the time called `H` below,
//! - the case's seed and swarm parameters.
//!
//! The device writes
//! - each boot and its session, 7 on the first boot and one more on each
//!   later one,
//! - each connection it accepts, and its end, by end of file, a failed
//!   write or a write that waited 20 s, at which it stops serving the
//!   connection,
//! - each RPC request it receives, with the boot, the connection, the route,
//!   the name, the arguments and, for `rpc.hash`, the hash it answered.
//!
//! The fake device of `proxy/turmoil.rs` behaves so that its log can serve as
//! the oracle.
//! - A new connection's first sample number is the larger of the whole
//!   seconds since the boot and one more than the highest number the boot has
//!   sent on any connection. After that each live connection counts on its
//!   own, as `serve` in `proxy/turmoil.rs` does, so a stale connection and a
//!   new one can send the same numbers. A proxy reads one connection at a
//!   time, and S1 judges what reaches the apps.
//! - The sample's one `F32` column holds the boot's session, which S4
//!   compares with the metadata the sample was decoded under.
//! - It answers `test.echo` with its session as four little endian bytes
//!   followed by the call's arguments. Each app numbers its calls with a
//!   counter of its own, and sends the app's index and that number as the
//!   arguments, the call's nonce, so nonces replay with the seed.
//! - Its `rpc.hash` at the root is `0x5157_0001` plus the boot's index, and
//!   at the hub child that plus `0x100` times the number of the child's
//!   reboots so far. A silence of the child stands for its reboot, which
//!   ends with the silence. A device's table changes only with new firmware,
//!   which always comes with a new session, so the child's session grows by
//!   `0x100` at each reboot too, from the boot's. So a table read before a
//!   reboot of the device or of the child never matches the device after
//!   it, and the engine, which loads a table again once a device is heard in
//!   another session or after a loss, is held to load it.
//! - The hub child's heartbeat carries the child's session, as the root's
//!   carries the boot's.
//!
//! The fake device also does what the firmware does over TCP, as far as
//! turmoil lets it.
//! - It serves one connection at a time. It listens only while it serves
//!   none, so a connect that comes while a connection lives is refused.
//! - It drops a peer that has left what it wrote unacknowledged for 20 s.
//!   turmoil has no acknowledgements, so the device takes what its peer has
//!   read as acknowledged, from a count of the bytes each stream reads that
//!   `net.rs` keeps under turmoil, and counts the 20 s from the last moment
//!   its peer read while some of what it wrote was unread. So it also drops
//!   a peer that acknowledges and stops reading, which the firmware never
//!   does. The proxy reads its connection for as long as it holds it, so no
//!   case reaches that. The firmware's keepalive of 5 s probes only an idle
//!   connection, and the device sends a heartbeat every 300 ms.
//! - A connection the proxy has let go therefore lives on at the device
//!   until the device reads the proxy's close, until a write fails at the
//!   proxy host's reset, or for 20 s after its peer last read, and until
//!   then the device refuses the proxy's next connection. turmoil never
//!   sends a segment again, so a device whose segments were lost in an
//!   outage, and whose writes then wait for credit, learns nothing when the
//!   outage ends, and waits out the 20 s where a real one would hear the
//!   reset of its next retransmission.
//!
//! Five things outside the fake device behave differently in a turmoil
//! build. Each sits under the turmoil cfg of the code beside it, which is
//! `cfg(all(test, turmoil, not(shuttle)))` in `chan.rs` and `rt.rs`, whose
//! shuttle build replaces the same items, and `cfg(all(test, turmoil))`
//! everywhere else.
//! - `Supervisor::spawn_blocking` runs its work as a task of the host. A
//!   blocking thread ends in real time, so the step that hears it would
//!   vary with the machine's load, and M2 would fail on a busy machine.
//! - `path` in `device/rpc/cache.rs` finds no cache file, so no case reads or
//!   writes the user's real RPC cache, which parallel tests share, and every
//!   load asks the device.
//! - The `TcpStream` of `net.rs` is a wrapper that counts its live streams
//!   per host, incremented on a connect or accept that succeeds and
//!   decremented on drop. turmoil's own count cannot serve, as
//!   `TcpStream::connect` in turmoil's `net/tcp/stream.rs` adds its socket to
//!   the host's table before the handshake and never removes it when the
//!   connect is refused or times out. The wrapper also counts the bytes each
//!   stream reads, which the fake device takes as its peer's
//!   acknowledgements.
//! - Each task the crate spawns, through `chan::spawn_on` or
//!   `Supervisor::spawn_in_map`, counts as live on its runtime until it is
//!   dropped, which `chan::live_tasks` reads.
//!   `Handle::current().metrics().num_alive_tasks()` cannot serve, as it
//!   leaves out the tasks of a `LocalSet`, where every task of a turmoil host
//!   runs.
//! - `Links::users_async` in `proxy/link.rs` counts the leases on the open
//!   links without blocking, as the proxy host's loop runs on its runtime.
//!
//! Each app is a host that runs one loop. It opens the device with
//! `Connection::open_at_async`, `Purpose::Use` and a 10 s timeout, and writes
//! `Opening`, then `Opened` or `OpenFailed` with the error. After a failure
//! it waits 1 s and opens again. Once open, it follows the root's presence,
//! the connection's samples, the connection's packets and the RPC table of
//! the root and of the child, and the child's presence, awaiting all of them
//! at once so that an event's time is the time it arrived. It writes
//! - each presence, with the session of `Alive`, and only the kind of
//!   `Lost`, as `Lost::since` is a real `Instant`,
//! - each batch, with its metadata session, its sample numbers, its boundary
//!   and its column values, and each `Lagged` with its count,
//! - each link status, with its route,
//! - each RPC table update, with its hash and session or its error,
//! - one `test.echo` call each second, with a timeout of 20 s, as
//!   `Sent(number)` and later `Done(number, outcome)`, the outcome being the
//!   reply's session and nonce or the `CallError`, and every fifth second a
//!   `dev.name` call, numbered with a count of its own,
//! - the end of each receiver with its `RecvError`. After the first, it takes
//!   what the others still yield until each has ended, for at most 1 s, then
//!   drops the connection and opens again.
//!
//! The proxy host writes, every 100 ms when they change, the id and the app
//! of each client `Proxy::clients()` lists, the count `links.users()`, its
//! live stream count and its live task count. An observer host that no
//! fault touches writes `established_tcp_stream_count_on("proxy")` every
//! 100 ms when it changes.
//!
//! The test cannot observe these, and no law names them.
//! - Anything inside the proxy, such as its outboxes, its RPC map, which
//!   link serves a client, and why it let a client go.
//! - Whether a `SensorDisconnected` an app hears came from the proxy or from
//!   the app's own pump, which makes one when its socket is lost
//!   (`Pump::lose` in `device/stream/mod.rs`). The checker attributes it with
//!   the driver's fault log, and calls the pump's one the app's socket loss.
//!   It is the pump's when the app's own link was cut, failing or held, or
//!   the proxy down, within 6 s and `d` before it, the proxy's when the
//!   device link was, or the device was down, or a tainted connection ended,
//!   within 4 s and `d` before it, and either when both or neither were. A
//!   law that needs the proxy's or the pump's takes no one that may be
//!   either.
//! - The status a mount hears as it is added, when the link has lost its
//!   device (`Mounts::add` in `proxy/engine.rs`). It reaches the session
//!   before the open returns, ahead of any receiver the app can make.
//! - Which segments turmoil dropped. The model's taint covers that.
//! - Real time, other than how long a whole case takes.
//! - The order of two events that reach one app through different receivers
//!   in one step.
//!
//! # 2. The model
//!
//! The checker holds no copy of the proxy. It keeps this state, in `Model`
//! and `Judge`.
//! - From the driver's log, for each link and each direction, the intervals
//!   in which it was cut, held, slowed or failing, and for each host the
//!   intervals in which it was down and how it went down. A direction is
//!   unusable while it is cut or failing, or while a host at either end is
//!   down. A link is unusable while either direction is. An app's path is
//!   unusable while its own link, the proxy or the device link is. A failing
//!   link lets connections through between its random partitions, so a law
//!   that needs a link to stay unusable counts only cuts and hosts down, and
//!   a timing law drops an obligation whose window meets a failing link, as
//!   section 5 says.
//! - From the device's log, each boot's session, start and order, its
//!   connections with their spans, and the nonces and hash answers each boot
//!   gave. The current boot at a time is the last one started by then.
//! - The taint. A device connection that was live at any moment the
//!   direction from the proxy to the device was cut or failing is tainted
//!   from then until it ends. turmoil never sends a segment again
//!   (`Link::rand_partition_or_repair`, `Link::explicit_partition` and
//!   `Link::partition_oneway` in turmoil's `top.rs` clear what is in flight),
//!   so a segment lost toward the device leaves a hole the device never reads
//!   past. The device still sends, so the proxy's quiet time never runs out,
//!   and the connection ends only once the proxy's writes use up
//!   `tcp_capacity` credits and the 3 s drain timeout passes. A request the
//!   device reads that the proxy sent after the cut or failing span ended,
//!   one read later than `d` after it, shows no segment was lost, and ends
//!   the taint. A connection ends at the earliest of the device logging its
//!   end, the device accepting the proxy's next connection, and the device
//!   going down.
//! - The taint of an app's connection, alike. A connection live while the
//!   direction from the proxy to the app was cut or failing is tainted from
//!   then until the app loses its socket, a `SensorDisconnected` it hears
//!   later than `d` after, or until it hears something the proxy sent after
//!   the span ended, a batch with a sample the device sent after it or the
//!   reply to a request the device read after it. While a connection is
//!   tainted the app may hear nothing the proxy sends, so the timing laws
//!   L6 (a) and (e), L8, L10 and L11 drop their obligations on it.
//! - The connections the device serves after the proxy let them go, from a
//!   kill of the proxy, or from a cut of the device link toward the proxy
//!   that lasts longer than the quiet time and `d` while the connection
//!   lives, to their end. The device refuses the proxy's connects
//!   meanwhile, as section 1 says.
//! - For each app, its connections, each with what the app saw on it, its
//!   end and the error it ended with. From these the laws read the app's
//!   last presence, the latest boot it heard `Alive` for, the highest sample
//!   number of each session, the last batch of a connection, the calls sent
//!   and not yet done, and the statuses it heard.
//! - The proxy's losses of the device, each from the earliest
//!   `SensorDisconnected` from the proxy that any app heard to the next
//!   `SensorReconnected` or `FailedToReconnect` any app heard, to the first
//!   `Alive` with a session an app heard on a connection it opened since,
//!   once the device has accepted a connection since, or to the moment no
//!   app held the link or the proxy went down. Once the link has ended, at a
//!   kill of the proxy or a `FailedToReconnect` any app heard, no loss starts
//!   until the device accepts a connection again, as what an app hears until
//!   then comes from the link that ended, from a link that has not reached
//!   the device, or from its own socket. Each of these moments is in the
//!   log, so none rests on the proxy host's counts, taken every 100 ms.
//! - The device's sessions at the root and at the hub child, each with its
//!   start. One starts at each boot, and at the child at the end of each of
//!   its silences.
//! - Each boot's and each device connection's end, at the first kill of the
//!   device after it in the log. A bounce logs its kill before the boot it
//!   starts, in the same millisecond.
//! - Obligations, each an event some app must see by a due time. A law that
//!   predicts an event adds one, with the spans in which its preconditions
//!   stop holding. One whose window meets such a span, or whose connection
//!   ends first, is dropped, and one whose event never comes by its due time
//!   fails the case.
//!
//! The slack `d` of an obligation is twice the largest message latency in
//! force on the links involved, plus 200 ms, plus the time those links were
//! held within the obligation's window.
//!
//! The liveness laws start at `H'`, the later of `H`, the end of the last
//! tainted connection of the device, and the end of the last connection the
//! device serves after the proxy let it go. A case whose `H'` comes more
//! than 70 s after `H` is a harness error. The laws are judged at `H' + B` and over the window from
//! `H' + B` to `H' + B + W`, with the bound `B` of 30 s and `W` of 10 s.
//!
//! # 3. Laws
//!
//! Each law gives its kind, its source, a wrong implementation it rules out,
//! and when it is checked. The doc of
//! `generated_fault_schedules_keep_the_laws` gives its mutants, and the doc
//! of `laws::OPEN` the open questions the laws name.
//!
//! S1. No sample is delivered twice. For each app and each session, the
//! sample numbers the app receives strictly increase across all its
//! connections, and step by one within a batch. The checker compares the
//! numbers themselves, since a repeat of the last sample makes no boundary
//! at all (the gap and step back checks of `StreamTracker::detect_boundary`
//! in `data/state.rs` see neither a gap nor a step back).
//! - Safety invariant, checked at every batch.
//! - Source the module doc of `proxy`, where a client that falls behind
//!   loses samples and nothing else, and its section How a packet reaches a
//!   client, which promise loss of samples and nothing else, in order. That
//!   a sample never comes twice is the user's decision, as no doc says it.
//! - It rules out a link that replays what it held after a reconnect, a
//!   proxy that reads two connections to one device, which the module doc of
//!   `proxy::link` forbids, and a fan out that queues a packet twice.
//!
//! S2. Continuity is told exactly. Between two batches of one connection in
//! one session, when the second's first number is one more than the first's
//! last, the second has no boundary at all. When it is not, the boundary's
//! change is none and its `Loss` counts exactly the missing numbers. The fake
//! device sends one segment, so no rollover or other change is due.
//! - Safety postcondition, checked at every batch.
//! - Source the doc of `Boundary` in `data/continuity.rs` (no boundary when
//!   nothing changed or was lost), the doc of `Loss::samples`, and the
//!   section How a packet reaches a client in the module doc of `proxy`,
//!   where each place a sample can be lost counts what it lost.
//! - It rules out a count off by one, a phantom loss, a boundary with nothing
//!   behind it, and a gap after a link loss told as `Restarted`, which counts
//!   nothing (the doc of `Change::Restarted`).
//!
//! S3. A new session is announced. Between two batches of one connection, a
//! change of metadata session comes with `SessionChanged` naming both, and no
//! batch after a connection's first has the change `Initial`.
//! - Safety invariant, checked at every batch.
//! - Source the docs of `Change::Initial` and `Change::SessionChanged`. The
//!   doc of `Initial` lists when a stream starts over, and none of those
//!   happens here.
//! - It rules out a reboot passed off as a step back in time or a fresh
//!   start.
//!
//! S4. Each sample is decoded under its own metadata. Every value in a
//! batch's column equals the batch's metadata session.
//! - Safety invariant, checked at every batch. Reaching it needs the step
//!   `RebootInBlip`.
//! - Source the section How a packet reaches a client in the module doc of
//!   `proxy`, where a metadata update reaches the client ahead of the
//!   samples after it, the module doc of `proxy::outbox`, and the module doc
//!   of `proxy::engine`, where the engine feeds the link's `Mounts`.
//! - It rules out a metadata update that overtakes samples sent before it.
//!
//! S5. Presence names the device as it is. Order here is the order in which
//! the device's sessions at a route started, never the order of session
//! ids, which a real device draws at random.
//! - An app's `Alive { session: Some(s) }` at the root or at the hub child
//!   names a session the device had started at that route, and never one
//!   that started before one the app already heard `Alive` for there.
//! - At its time, that session is the current one, or the current one
//!   started within 3 s and `d` before.
//! - `Alive` with no session is allowed at either route. At the root a new
//!   connection's records reach the proxy before its first heartbeat, and at
//!   the child the replies to the table requests the proxy sends when a link
//!   reconnects may too.
//! - Safety invariant, checked at every presence.
//! - Source the docs of `Presence::Alive` and `Presence::Lost`.
//! - It rules out a presence that invents a session, and one that keeps or
//!   returns to a session the device has left. A hub child that reboots in
//!   a silence too short to be a loss comes back in a new session on the
//!   same connection. A presence that prefers the session it held to the
//!   heartbeat's keeps the old one, and sends no presence again, which L1
//!   rules out.
//!
//! S6. A reply is the device's answer, from one delivery.
//! - (a) A successful `test.echo` carries its own nonce, and the boot it
//!   names logged that nonce after the app sent it.
//! - (b) No nonce appears twice in the device's log, across boots and
//!   connections.
//! - (c) A reply never crosses a device reboot. A call the boot that received
//!   it did not answer ends in an error, never in a later boot's reply. With
//!   the fake device, (c) follows from (a) and (b).
//! - Safety invariant, checked at every `Done` and every device log entry.
//! - Source the module doc of `proxy::worker`, which answers a request a
//!   lost or restarted device leaves with an RPC error, read as never
//!   sending it again, and the module doc of `device::session`, on a socket
//!   that closes without a stop. Parts (b) and (c) are an assumption to
//!   confirm, open question 1.
//! - It rules out a misrouted reply, a cached reply, and a retry of pending
//!   requests on the reconnected device.
//!
//! S7. That settings are as the device announced them (the doc of
//! `SettingsCache` in `proxy/settings.rs`) is not checked, as it needs a
//! device built on `twinleaf_device`.
//!
//! S8. A connection ends only for a reason.
//! - An app's receivers end with `Disconnected` only if its connection heard
//!   `FailedToReconnect`, up to 1 s after the end as another receiver may
//!   yield it later, or 30 s less `d` have passed since the app's socket
//!   loss with no `Alive` heard since, and the app's path was unusable for
//!   at least 23 s of the 30 s before. The path counts the device link, as a
//!   session whose proxy restarted or closed its idle link opens again
//!   through a new link, whose first open fails while the device is out. It
//!   counts too the time the device refuses the proxy's connects, as section
//!   2 says.
//! - They end with `Closed` only after the app closed the connection.
//! - No app hears `FailedToReconnect` unless `reconnect_timeout` is some T
//!   and the proxy lost the device at least T less `d` before, with no
//!   `SensorReconnected` since. The loss time is the first
//!   `SensorDisconnected` any app heard, when that app heard it from the
//!   proxy. When the app may have heard its own socket's loss, or heard the
//!   proxy's late over a link that was unusable, or when no app heard one,
//!   it is the start of the device link's outage less 4 s, a connect timeout
//!   and the interval after it, or the first one heard if that is earlier.
//! - Safety invariant, checked at every end and every status.
//! - Source the last two paragraphs of the module doc of `device::session`,
//!   `REOPEN_FOR` in `device/session.rs`, the module doc of `proxy` on
//!   `HostOptions::reconnect_timeout` and `FailedToReconnect`, the doc of
//!   `HostOptions::reconnect_timeout`, and the module doc of `proxy::worker`
//!   on when the worker stops.
//! - It rules out a session that gives up early, and a link that gives up a
//!   device that came back or counts from the wrong moment.
//!
//! S9. No app hears `Stopped`. Nothing in the suite stops a proxy, and a
//! crash cancels every task of its host before any runs again.
//! - Safety invariant, checked at every status.
//! - Source the module doc of `proxy` on the `Stopped` status, which says
//!   the status can fail to arrive when the proxy crashes. Open question 3
//!   asks whether that reading is meant.
//! - It rules out a link's end told as a stop, which would end a session
//!   that must open again (the module doc of `device::session`, where the
//!   session ends when the proxy stops).
//!
//! L1. Every app is back. At `H' + B` every app has an open connection, and
//! its last root presence is `Alive` in the current boot's session. With a hub
//! child, its last presence of the child is `Alive` in the child's session.
//! - Liveness, checked after the heal. A warm up in which some app does not
//!   reach `Alive` and a batch within 15 s fails this law too.
//! - Source the module doc of `proxy`, where a device that drops out is
//!   reconnected and its clients stay connected, the module doc of
//!   `device::session` on a socket that closes without a stop, and the doc
//!   of `Presence::Alive`.
//! - It rules out a half open link never noticed, a link or session that
//!   stops trying, and a stale session.
//!
//! L2. Samples flow. Unless metadata is withheld, every app has a batch of
//! the current boot by `H' + B`, and then one at least every 2 s and `d`
//! through `W`, the slack covering the latencies of two samples.
//! - Liveness, checked after the heal.
//! - Source the paragraph of the module doc of `proxy` on which devices the
//!   proxy keeps open and how it reconnects them, and the module doc of
//!   `proxy::engine`, where the engine feeds the link's `Mounts`.
//! - It rules out a fan out that skips a client, and an outbox that drops
//!   every sample.
//!
//! L3. The device answers. A `test.echo` sent after `H' + B` succeeds from
//! the current boot, and `dev.name` returns `FAKE`.
//! - Liveness, checked after the heal.
//! - Source the module doc of `proxy::worker`, on how the worker hands
//!   packets to the engine and answers the ports' requests.
//! - It rules out answers handed to the wrong id or to no one.
//!
//! L4. The table is the device's. At `H' + B`, each app's table of the root
//! and of the child is `Ok`, its hash is the last the device answered at that
//! route and the one the device answers there at `H' + B`, and its session is
//! the device's session there at `H' + B`. A child that rebooted and was
//! asked nothing since still answers the old hash last, so the hash it would
//! answer is checked too. A table is read once per session, so one read
//! before the device's last reboot at that route is out of force. The
//! settings half is not checked, as with S7.
//! - Liveness, checked after the heal.
//! - Source the module doc of `proxy::engine`, on presence and the table
//!   loaded again, the doc of `RpcRegistry` in `device/rpc/registry.rs`, a
//!   table read once per session that only a new session can change, and
//!   the doc of `CacheAction::Reload` in `proxy/settings.rs`, which reads
//!   the registry again when the device starts a new session.
//! - It rules out a reconnected or rebooted device whose old table stays in
//!   force.
//!
//! L5. Nothing leaks. At `H' + B` and through `W`
//! - the proxy host's live stream count is one per app with an open
//!   connection, plus one to the device,
//! - `clients()` and `links.users()` each equal the number of apps with an
//!   open connection,
//! - turmoil's own count on the proxy host does not grow across `W`,
//! - the proxy host's count of live tasks equals its count at the end of the
//!   warm up, when the same apps were open. A difference found legitimate is
//!   recorded here with its reason. The proxy starts tasks while the apps
//!   open and while it loads their tables, so the warm up ends with a count
//!   taken once every app has its tables and the count has held for 1 s.
//! - The device's own count is excluded, as section 5 says.
//! - Liveness, checked after the heal.
//! - Source the opening sentence of the module doc of `proxy`, its section
//!   When the proxy lets a client go, the module doc of `proxy::link`, where
//!   a new link to a device opens only once every earlier link to it has
//!   ended, and the module doc of `proxy::worker` on reopening a device
//!   whose port closes, fails or goes quiet.
//! - It rules out a worker that keeps a lost device's port, a client or lease
//!   the proxy never lets go, and tasks that pile up with faults.
//!
//! L6. Losses are seen in time. Each part adds obligations during the chaos,
//! for apps that are open, `Alive` and on an untainted, usable link at the
//! fault, and drops them if that stops holding.
//! - (a) The device link cut both ways or from the device to the proxy, or a
//!   device power cut, for longer than 3 s and `d`, has every such app see
//!   `Lost` within 3 s and `d` of the cut. Source the doc of
//!   `Presence::Alive` and the module doc of `proxy::worker` on the quiet
//!   times of 1 s and of 3 s over TCP.
//! - (b) An app's link cut for longer than 6 s and `d`, either way, has that
//!   app see its socket loss within 6 s and `d` of the cut, and the proxy
//!   drop it from `clients()` within 5 s and `d`. The extra second is the
//!   close timeout, when the cut is from the app to the proxy and the app
//!   learns of it from the proxy's close. When the cut is from the proxy to
//!   the app and the direction toward the proxy is cut too within those
//!   5 s, the app's close never reaches the proxy, so the 5 s count from
//!   that second cut. Source the keepalive item of the
//!   section When the proxy lets a client go in the module doc of `proxy`,
//!   the module doc of `device::session` on the other reasons the proxy
//!   closes the socket, `CLIENT_KEEPALIVE` and `PROXY_KEEPALIVE` in
//!   `proxy/control.rs`, and `CLOSE_TIMEOUT` in `tio/transport.rs`.
//! - (c) An app killed while the proxy runs is gone from `clients()` within
//!   1 s and `d`, also while its open waits for an unreachable device.
//!   Source the hung up item of the section When the proxy lets a client go
//!   in the module doc of `proxy`, with the promptness taken from the doc of
//!   the test `a_client_that_hangs_up_mid_wait_is_let_go` in `proxy/mod.rs`,
//!   open question 4.
//! - (d) Once no app has held the device open for 5 s, on an untainted and
//!   usable device link, the device logs the end of the proxy's connection
//!   within 6 s and `d`. Source the module doc of `proxy`, where it closes a
//!   device no client has used for five seconds, and the module doc of
//!   `proxy::link`, where the registry closes a link with no users for
//!   `IDLE`.
//! - (e) A hub child silent for longer than 3 s and `d` is `Lost` at every
//!   such app within 3 s and `d` of its last heartbeat. Source the doc of
//!   `Presence::Lost` and the module doc of `proxy::engine`, where the
//!   engine loses a device silent for `QUIET`.
//! - Timeliness, checked at every event against the obligations. A cut from
//!   the proxy to the device alone has no time, as section 5 says.
//! - It rules out a quiet time longer than documented, and a client let go
//!   only once its open ends.
//!
//! L7. Endings are predicted. These are the halves of S8 that say an ending
//! must come, each counted from the loss the apps observed.
//! - (a) While the proxy stays up and an app's link stays usable, with
//!   `reconnect_timeout` some T, a device that stays unreachable for T and
//!   `d` after the proxy lost it has that app hear `FailedToReconnect` and
//!   see its receivers end with `Disconnected` within T and `d` of the loss.
//!   Source the module doc of `proxy` on `HostOptions::reconnect_timeout`
//!   and `FailedToReconnect`, the doc of `HostOptions::reconnect_timeout`,
//!   and the module doc of `device::session`, where calls made while the
//!   device is out fail and `FailedToReconnect` ends the session.
//! - (b) An app whose link or proxy stays unusable for 30 s and `d` after its
//!   socket loss sees its connection end within 30 s and `d` of the loss.
//!   Source the module doc of `device::session`, where the session ends once
//!   `REOPEN_FOR` has passed, and `REOPEN_FOR` in `device/session.rs`.
//! - Timeliness, checked at every event.
//! - It rules out a session or link that never gives up, or gives up late.
//!
//! L8. The device's return is heard. On a connection whose own link stays
//! usable, a `SensorDisconnected` from the proxy at the root is followed by
//! `SensorReconnected`, unless `FailedToReconnect` comes first or the
//! connection ends. It comes within 1 s and `d` of the device listening
//! again after a kill, within 4 s and `d` of the device link being usable
//! again after a cut or a power cut, the 4 s being a connect timeout and the
//! interval after it, within 1 s and `d` of the end of the device's
//! connection the proxy lost, which the device serves before it takes
//! another, within 4 s and `d` of the end of any span in which the device
//! refuses the proxy, as section 2 says, and by `H' + B` at the latest. Its order against the
//! `Alive` presence is not checked, as presence may overtake the device's
//! packets (the `proxy.watch` item of the section Control requests in the
//! module doc of `proxy`).
//! - Timeliness, checked at every event and after the heal.
//! - Source the module doc of `proxy` on reconnecting a device and the
//!   `SensorDisconnected` and `SensorReconnected` statuses, and the module
//!   doc of `proxy::worker` on the statuses the engine hears and attempts
//!   tried each second.
//! - It rules out a reconnect the clients never hear of, and a reconnect
//!   tried less often than documented.
//!
//! L9. Calls end. Every call ends by its timeout and `d` (the doc of
//! `Session::submit_with` in `device/session.rs`). Unless the device
//! connection is tainted while it waits, it ends within 10 s of being sent.
//! The device answers at once, the proxy fails what a lost device leaves
//! within its 3 s quiet time, and a lost socket fails every call in flight
//! within the 6 s of L6 (b).
//! - Timeliness, checked at every event.
//! - Source the module doc of `proxy::worker`, where a request a lost or
//!   restarted device leaves is answered with an RPC error, and the module
//!   doc of `device::session`, on a socket that closes without a stop and
//!   where calls made while the device is out fail. The docs name no time,
//!   so the 10 s is an inference, open question 5.
//! - It rules out a proxy that leaves a lost device's requests to time out,
//!   and a session that keeps calls across a lost socket.
//!
//! L10. Every app hears the loss. Each app open with a usable link when the
//! proxy loses the device hears `SensorDisconnected` at the root within `d`
//! of the first app that hears it, and so within 3 s and `d` of a cut as in
//! L6 (a). An app that opens while the device is out hears it, or sees the
//! device `Lost`, within `d` of its `Opened`. The status itself reaches the
//! session before the open returns, as section 1 says, so the presence the
//! proxy sends at the open stands for it.
//! - Timeliness, checked at every event.
//! - Source the module doc of `proxy`, on reconnecting a device and a client
//!   that opens it while it is out hearing `SensorDisconnected` at once.
//! - It rules out a status that reaches some clients only, and a client that
//!   opens during an outage and is not told.
//!
//! L11. Samples follow a return. Unless metadata is withheld, an app that
//! hears `Alive` after a `Lost` has a batch of that boot within 2 s and `d`,
//! while its link and the device link stay usable and untainted and the
//! boot lives. A new connection sends the device's records first, so no
//! metadata wait adds to this. An `Alive` for a boot that has ended, which
//! S5 allows for a while, promises nothing.
//! - Timeliness, checked at every event.
//! - Source the doc of `Presence::Alive` (alive means the device is
//!   sending) and the module doc of `proxy`, where a device that drops out
//!   is reconnected and its clients stay connected.
//! - It rules out a link that comes back without its samples, or with them
//!   held back.
//!
//! L12. An app tries again promptly. An app whose socket was lost to a proxy
//! kill opens again within 1.25 s and `d` of the proxy listening again, while
//! its own link and the device link stay usable, the proxy is not killed
//! again and the connection has not ended. The proxy listens again at the
//! first bounce of the proxy after the kill in the log, as the driver may
//! bounce the proxy and kill it again within one millisecond.
//! - Timeliness, checked at every event.
//! - Source the doc of `open_again` in `device/session.rs`, which says a
//!   proxy that is back is tried within 1.25 s.
//! - It rules out a backoff that grows past its cap.
//!
//! L13. The proxy lets go of a lost device. Once the proxy has lost the
//! device, as the first app hears, or gone down, the device logs the end of
//! the connection the proxy held within 1 s and `d`, while the direction
//! toward the device can carry the proxy's close and the connection is
//! untainted. The connection is the last the device accepted more than the
//! slack of the app that heard the loss first before it heard it.
//! - Timeliness, checked at every loss and every kill of the proxy. It is the
//!   law of open question 7, counted and failing no case, as no doc says the
//!   port a device was lost on is closed when it is lost.
//! - Source the module doc of `proxy::worker`, which reopens a device whose
//!   port closes, fails or goes quiet, and `CLOSE_TIMEOUT` in
//!   `tio/transport.rs`, the longest a closed port takes to let go.
//! - It rules out a worker that keeps a lost device's port, which a device
//!   that serves one connection at a time takes as the proxy still there,
//!   so it refuses the proxy's next connect. The rules that count the time
//!   the device refuses the proxy, in S8, L8 and `H'`, count from that
//!   connection's end, which the proxy controls, so they rely on this law.
//!
//! M1. A blip changes nothing but timing. In a case whose only faults are
//! holds and slow links within their margins, each app opens once, as one
//! client of the proxy, its presence stays `Alive` in the first boot's
//! session after the warm up, no batch after its first has a boundary, every
//! call succeeds, and the device accepts one connection. A hold of the
//! device link is within its margin when the hold, 300 ms and the largest
//! latency add up to under 3 s. A hold of an app link is within its margin
//! when the hold and the largest latency add up to under 3 s, as with a
//! window of 4 the proxy's writes stall through the hold and the drain
//! timeout is 3 s.
//! - Metamorphic, the fault free outcome being the other side, checked at
//!   the end of the case.
//! - Source the doc of `Presence::Alive`, `LIVENESS_TIMEOUT_TCP` in
//!   `proxy/worker.rs`, the hung up and keepalive items of the section When
//!   the proxy lets a client go in the module doc of `proxy`, and
//!   `DRAIN_TIMEOUT` in `tio/transport.rs`.
//! - It rules out a TCP link judged by the 1 s quiet time of serial.
//!
//! M2. A seed replays. A case run twice with one seed gives the same turmoil
//! trace and the same `World` log.
//! - Metamorphic, run on `REPLAYED` cases (`proxy/turmoil.rs`), drawn from
//!   the first generated cases in which some app opens again or comes back
//!   to the proxy as a new client, so that the backoff's jitter is on the
//!   trace.
//! - Source the module doc of `proxy/turmoil.rs`. With the RPC cache off, no
//!   state outside the case reaches it.
//! - It rules out real randomness and unordered iteration in the proxy.
//!
//! M3. One app's faults touch no other. In a case of two or more apps whose
//! faults touch only the link or host of one, every other app sees what M1
//! promises.
//! - Metamorphic, checked at the end of the case.
//! - Source the module doc of `proxy`, where a device that drops out is
//!   reconnected and its clients stay connected, where a client that falls
//!   behind loses samples and nothing else, and its paragraph on a client's
//!   outbox, each client with an outbox of its own.
//! - It rules out a client's stall or end that reaches the link's other
//!   clients.
//!
//! I1. No host panics and no step fails. Debug assertions such as the
//! `debug_assert!` in `Outbox::urgent` stay on. A listener's backlog holds
//! `tcp_capacity` connects (`Tcp::new` in turmoil's `host.rs` sets
//! `server_socket_capacity`, and `Tcp::receive_from_network` panics on a
//! full server socket buffer), 4 in half the cases. Its panic, a case whose
//! `H'` comes late, and a case that reaches `simulation_duration` are
//! harness errors, reported apart from failures. Implicit oracle.
//!
//! I2. No case spins. Each case ends within `SPIN_LIMIT` of real time
//! (`proxy/turmoil.rs`). In the regression
//! `a_hub_child_lost_while_its_metadata_is_retried_leaves_the_proxy_idle`,
//! 60 s of an idle proxy take under 10 s of real time. Implicit oracle.
//!
//! # 4. The fault alphabet and the generator
//!
//! A case has four phases.
//! - Warm up, at most 15 s, until each app has written `Opened`, `Alive` and
//!   a batch, or `Alive` alone when metadata is withheld, and then until
//!   each app's last connection has its tables `Ok` and the proxy host's
//!   counts have held for 1 s.
//! - Chaos, at most 60 s, the schedule's steps.
//! - The heal at `H`. It releases every link, which delivers what was held
//!   and ends every cut (`Link::release` in turmoil's `top.rs`), then
//!   repairs every link, sets fail rates to 0 and latency back to the case's
//!   own, unsilences the child, and bounces every host that is down.
//! - Liveness, from `H'` through `B` and `W`. The case runs up to 200 s of
//!   `simulation_duration`.
//!
//! The steps, simplest first, as proptest shrinks toward earlier variants.
//! Each step carries its own end, so any subsequence a shrink leaves is a
//! valid schedule. A pair is the device and the proxy, or an app and the
//! proxy.
//! 1. `Wait`.
//! 2. `SlowLink { pair, max_ms }`, 100 to 1000 ms, until the heal or the
//!    pair's next `SlowLink`.
//! 3. `Blip { pair, ms }`, a hold released after 100 to 2500 ms. A blip
//!    stands for a short loss real TCP survives.
//! 4. `Cut { pair, ms }`, a partition repaired after `ms`. A cut stands for a
//!    real outage.
//! 5. `CutOneWay { way, ms }`, a partition of one direction of a pair.
//! 6. `AppAway { app, style, ms }`, where the style closes the connection,
//!    kills the host, or cuts the app's link and then kills it. The host is
//!    bounced after `ms` or at the heal.
//! 7. `ChildSilent { ms }`, 1 to 20 s.
//! 8. `RebootDevice { style, down_ms }`, a kill or a power cut, which is a
//!    cut of the device's link and then a kill. A kill with no down time is
//!    `Sim::bounce`.
//! 9. `RebootInBlip { app, hold_ms, at_ms }`, a blip of an app's link with a
//!    device kill `at_ms` into it, drawn only in cases with `tcp_capacity`
//!    of 4. It reaches S4, which needs packets waiting in that app's outbox
//!    when the new boot's metadata arrives, and which random steps reach in
//!    about one case in three hundred.
//! 10. `CrashProxy { style, down_ms }`, with the same two styles as the
//!     device.
//! 11. `FailRate { pair, permille, ms }`, a fail rate of 5 to 50 percent,
//!     repaired after `ms`.
//!
//! A schedule is 0 to 12 steps, each a gap and a step. A third of the gaps
//! are 0 to 500 ms, so steps land inside opens, and the rest 0 to 10 s. A
//! step starts after its gap, and once every earlier step that sets the
//! same link, crashes the same host or silences the child has ended. So V1
//! holds for any subsequence, and a step that would start after the chaos
//! is left out. The chaos ends when the last step does, or after 60 s.
//!
//! Durations keep out of the bands where a law could go either way. As the
//! timing laws count from the loss an app observes, these bands are wider
//! than the laws need.
//! - An app's outage, from `Cut`, `CutOneWay`, `FailRate`, `AppAway` or a
//!   proxy down time, is at most 20 s or at least 45 s.
//! - A device outage with `reconnect_timeout` of 10 s is at most 3 s or at
//!   least 16 s. Without it, any length from 300 ms to 60 s. Its kill may
//!   also have no down time.
//!
//! The generator must pass a validity property over generated and shrunk
//! schedules.
//! - V1. No two steps that set a pair's link state overlap on that pair.
//!   These are `Blip`, `Cut`, `CutOneWay`, `FailRate`, `RebootInBlip`, and
//!   the cuts of a power cut, which touches every pair of its host. In
//!   turmoil a release ends a cut, a cut discards what is held, a repair
//!   ends a hold and strands what it held, and a one way repair reopens one
//!   direction of a cut (the methods of `Link` in turmoil's `top.rs` from
//!   `hold` through `explicit_repair`).
//! - V2. Only hosts are crashed or bounced.
//! - V3. Durations lie in their bands. The heal may cut a step short, and
//!   the checker judges the intervals applied, so a clipped step changes its
//!   class and never its validity.
//! - V4. The schedule ends by the end of the chaos.
//!
//! Each case draws its swarm parameters.
//! - One to three apps, two or three in two cases of three, for M3 and the
//!   fan out of L2.
//! - `reconnect_timeout` of none or 10 s.
//! - A hub child or none.
//! - Withheld metadata in one case of eight, as it silences every sample
//!   law.
//! - `enable_random_order` or not.
//! - A largest latency of 100 ms or 500 ms, which the heal restores.
//! - `tcp_capacity` of 64 or 4. The small window lets a short hold fill an
//!   app's socket, so the outbox holds packets, and ends a tainted
//!   connection sooner.
//! - Each step kind enabled or not, even odds, so that no kind crowds out
//!   another.
//!
//! The run prints how many cases reach each class, with the real time a
//! case takes and how often each law of an open question broke. With
//! `TWINLEAF_TURMOIL_SHARES` set, as for a nightly run, it asserts each class
//! was reached at least once. C5, C12, C15, C16 and C17 count each of their
//! parts apart.
//! - C1. A device cut longer than 3 s and `d`.
//! - C2. `FailedToReconnect` heard.
//! - C3. An app cut between 6 s and 20 s, which the app reopens through.
//! - C4. An app outage of 45 s or more, which ends its connection.
//! - C5. A proxy kill, short and long.
//! - C6. A proxy power cut.
//! - C7. A device kill.
//! - C8. A device power cut.
//! - C9. A crash of the device or the proxy while an app is between
//!   `Opening` and `Opened`.
//! - C10. An app that leaves while its open waits for an unreachable device.
//! - C11. A `RebootInBlip` whose new metadata found packets of the old boot
//!   still unsent, seen as old samples after the reboot reaching the app
//!   after the hold.
//! - C12. A call pending when a device cut begins, and a call whose nonce
//!   reached a boot that died before its reply reached the app.
//! - C13. Two or more apps, with faults that touch one app only.
//! - C14. A child silent longer than 3 s and then heard again.
//! - C15. A heal with an app still cut off, one with the proxy down, one
//!   with the device down, and one with a tainted connection.
//! - C16. A blip within its margin on each kind of link.
//! - C17. A one way cut in each direction on each kind of link.
//! - C18. A session that opens again through a new link while the device is
//!   out.
//! - C19. A `Lagged` receiver, expected rare, counted for information.
//!
//! C11 as counted shows old samples after the hold, which the transport's
//! queue of 64 packets carries, and not that the outbox held any. At about
//! four packets a second a hold of 2.5 s never fills that queue, and the
//! keepalive ends a longer one, so S4 is out of the generator's reach and
//! its mutant survives.
//!
//! C9 and C10 need a few milliseconds' timing or three conditions at once,
//! and C4 needs a long outage early in the chaos, and C13 needs a case with
//! no device or proxy fault. No class is weighted, and the counts show how
//! often each is reached.
//!
//! # 5. Exclusions
//!
//! - turmoil's TCP never retransmits, so a dropped segment leaves a hole
//!   that stalls the connection for good. A cut therefore never stands for a
//!   blip, and blips are holds.
//! - A cut from the proxy to the device alone, or the hole a failing or cut
//!   link leaves in that direction, has no timing law. The proxy keeps
//!   hearing the device, and nothing documented ends the connection. The
//!   taint marks it, L9 allows its calls their full timeout, and liveness
//!   waits for it to end. Open question 6. The other one way cuts have
//!   documented timers and are covered by L6.
//! - turmoil models no timeout or keepalive of TCP itself. It sends a reset
//!   only when a segment reaches a host with no socket for it (the
//!   `Segment::Data` and `Segment::Fin` arms of `Tcp::receive_from_network`
//!   in turmoil's `host.rs`), and nothing tells a peer whose segments are
//!   dropped. A device whose window filled during a cut keeps its stale
//!   socket for the 20 s of section 1, so L5 counts the proxy's streams
//!   alone.
//! - turmoil cannot lose, repeat or corrupt a single segment, skew a clock,
//!   or crash a client. Serial, mDNS, UDP and rate negotiation are out of
//!   reach. The restart check in `Device::received` in `proxy/worker.rs`
//!   runs only while a rate is negotiated, so it never runs here.
//! - No timing law covers a failing link, since the driver does not know
//!   when `FailRate` repairs it.
//! - Settings are not checked, as S7 and L4 say.
//! - A proxy behind a proxy, a stopping proxy, probes and mounts are not
//!   generated.
//! - The outbox's sample budget, its hard limit and the pump's inlet are
//!   unreachable at one sample a second.
//! - `Alive` with no session, before a connection's first heartbeat, is
//!   allowed, as S5 says.
//! - The value of `Lost::since` is not compared with virtual time.
//!
//! # 6. Bounds
//!
//! The checker's constants for these timers sit in this module, named in
//! the last column.
//!
//! | Timer | Value | Where | Checker |
//! |---|---|---|---|
//! | Device quiet time over TCP | 3 s | `LIVENESS_TIMEOUT_TCP` in `proxy/worker.rs` | `QUIET` |
//! | Engine quiet time | 3 s | `QUIET` in `proxy/engine.rs` | `QUIET` |
//! | Reconnect interval | 1 s | `RECONNECT_INTERVAL` in `proxy/worker.rs` | `RECONNECT_INTERVAL` |
//! | TCP connect timeout | 3 s | `CONNECT_TIMEOUT` in `tio/transport/tcp.rs` | `RECONNECT_TRY` |
//! | Hello timeout | 2 s | `HELLO_TIMEOUT` in `proxy/control.rs` | |
//! | Keepalive beat and quiet time | 1 s and 5 s | `CLIENT_KEEPALIVE` in `proxy/control.rs` | `KEEPALIVE_QUIET`, `SOCKET_LOSS` |
//! | Reopen backoff | 250 ms doubling to 1 s, up to a quarter more | `open_again` and `jittered` in `device/session.rs` | `REOPEN_WAIT` |
//! | Reopen limit | 30 s | `REOPEN_FOR` in `device/session.rs` | `REOPEN_FOR` |
//! | First packet of a new link | 10 s | `READY_TIMEOUT` in `proxy/link.rs` | |
//! | Table reload after a failed load | 5 s | `RELOAD_DELAY` in `proxy/engine.rs` | |
//! | Drain and close | 3 s and 1 s | `DRAIN_TIMEOUT` and `CLOSE_TIMEOUT` in `tio/transport.rs` | `SOCKET_LOSS`, `CLOSE_TIMEOUT` |
//! | Idle link | 5 s | `IDLE` in `proxy/mod.rs` | `IDLE_CLOSE` |
//! | Fake heartbeat and sample | 300 ms and 1 s | `serve` in `proxy/turmoil.rs` | `SAMPLE_GAP` |
//! | Fake device drops a peer that leaves what it wrote unread | 20 s | `UNACKNOWLEDGED` in `proxy/turmoil.rs` | |
//! | App open timeout and retry | 10 s and 1 s | `app` in `hosts.rs` | `OPEN_TIMEOUT` |
//! | Largest latency after the heal | 100 ms or 500 ms | the case's own | |
//!
//! The bound `B` comes from the longest chain after `H'`.
//! - An app whose handshake was in flight at `H'` and lost a segment stalls
//!   for the keepalive's quiet time, 5 s. Its next try waits at most 1.25 s
//!   and then opens at once. That is 6.25 s. An app whose connection ended
//!   just before `H'` opens again within 10 s and the 1 s retry.
//! - The device chain runs beside it. A worker whose connection lost a
//!   segment loses the device by `H' + 3 s`. One whose connect was cut fails
//!   at `H' + 3 s` and tries again at `H' + 4 s`. A new connection sends the
//!   records first, so `Alive` waits for one heartbeat, 300 ms, and a batch
//!   for one sample, 1 s.
//! - A table whose load failed during the chaos loads again 5 s after the
//!   device is heard.
//!
//! The longest chain is near 13 s with latencies of 500 ms, and `B` of 30 s
//! is over twice that. A tainted connection ends once the proxy has written
//! `tcp_capacity` packets to it and the drain timeout has passed. The apps'
//! calls, one a second each, and the engine's own requests make that under
//! 70 s at a window of 64 with one app, which bounds `H'`. A connection the
//! device serves after the proxy let it go ends within 20 s of its peer's
//! last read, well within that bound. Warm up, chaos,
//! the 70 s, `B` and `W` take 185 s, so a case runs with a
//! `simulation_duration` of 200 s.
//!
//! # 7. Examples that stay
//!
//! The scenarios of `proxy/turmoil.rs` stay as named regressions, keeping
//! their own assertions, and
//! `the_scenarios_as_fixed_schedules_keep_the_laws` runs each as a fixed
//! schedule the interpreter runs and the checker judges.
//! - `a_device_cut_off_without_a_close_is_lost_and_comes_back`. `Cut` of the
//!   device link for 6 s.
//! - `a_device_that_restarts_comes_back_in_a_new_session`. `RebootDevice`
//!   with a kill and no down time.
//! - `an_app_opens_its_device_again_once_a_crashed_proxy_is_back`.
//!   `CrashProxy` with a kill for 2 s.
//! - `an_app_cut_off_from_its_proxy_opens_its_device_again`. `Cut` of the app
//!   link for 8 s.
//! - `a_device_on_a_slow_link_stays_alive_and_answers`. `SlowLink` of
//!   1000 ms, under M1.
//! - `a_hub_child_lost_while_its_metadata_is_retried_leaves_the_proxy_idle`.
//!   `ChildSilent` for 60 s, under I2.
//! - `a_hub_child_heard_again_after_going_quiet_loads_its_table_again`.
//!   `ChildSilent` for 5 s, under L4.
//! - `a_scenario_run_twice_with_one_seed_runs_the_same_way`, the replay
//!   check, which M2 extends to generated cases.
//!
//! Four fixed examples sit on each side of the two limits, counted from the
//! loss, which a kill makes immediate.
//! - A proxy killed for 27 s. Each app opens again, and no connection ends.
//! - A proxy killed for 33 s. Each app's connection ends with
//!   `Disconnected` 30 s after the kill, and the app opens a new one.
//! - With `reconnect_timeout` of 10 s, a device killed for 8 s. No
//!   `FailedToReconnect`.
//! - With `reconnect_timeout` of 10 s, a device killed for 12 s.
//!   `FailedToReconnect` 10 s after the kill.
//!
//! A hub child that reboots in a silence of 1.5 s, too short to be a loss,
//! is heard in its new session and its new table is loaded. Generated cases
//! reach this too seldom to hold the engine to the heartbeat's session, and
//! this example does, through L1.
//!
//! One fixed example holds a regression of the proxy, and is ignored until
//! the proxy is fixed. With `reconnect_timeout` of 10 s and three apps, a
//! device killed for 20 s, the kill placed 860 to 960 ms into the chaos.
//! Every app hears `FailedToReconnect` 10 s to 10.6 s after the kill and the
//! laws hold. An app whose request reaches a port the worker has closed,
//! before the engine hears the worker gave up, has its connection closed
//! with `ForwardFailed` in `proxy/client.rs`, without the status, and opens
//! again and again.
//!
//! The suite covers two real socket tests, which stay beside it.
//! - The test `a_client_that_hangs_up_mid_wait_is_let_go` in
//!   `proxy/mod.rs`, by L6 (c) and class C10. Its probe half is covered only
//!   if an app also probes.
//! - The test `a_slow_reply_within_the_calls_timeout_arrives` in
//!   `device/connection.rs`, by L9 and S6 under `SlowLink`.
//!
//! The suite does not reach the tests of threads, panics, ports and
//! loopback in `proxy/mod.rs`, the task leak test
//! `a_dropped_connection_and_proxy_leave_no_task` and the handles a close
//! ends in `a_close_ends_every_handle_on_the_connection`, both in
//! `device/connection.rs`, and the proxy behind a proxy in
//! `a_device_lost_behind_a_mounted_proxy_is_a_status_at_its_mount` in
//! `proxy/mod.rs` and `clients_of_a_proxy_behind_a_proxy_share_its_state`
//! in `device/connection.rs`.
//!
//! # Running
//!
//! The command of `crate::net` runs the suite with the other turmoil tests.
//! A run takes 24 cases, or as many as `TWINLEAF_TURMOIL_CASES` says, from
//! seed 1 up. `TWINLEAF_TURMOIL_CASE` runs the cases of the seeds it lists,
//! with commas between, alone. A failing case is shrunk up to 64 times, or
//! as many as `TWINLEAF_TURMOIL_SHRINKS` says, and its message names the law,
//! the seed and the first event that broke it. A seed always draws the case
//! it drew before shrinking, so `TWINLEAF_TURMOIL_CASE` runs that case and
//! shrinks it again, and with `TWINLEAF_TURMOIL_SHRINKS=0` it runs it as
//! drawn. Cases run on one thread per core, or as many as
//! `TWINLEAF_TURMOIL_THREADS` says, each with a simulation of its own.
//! `TWINLEAF_TURMOIL_ALL` runs every case and lists each that failed, and
//! `TWINLEAF_TURMOIL_DUMP` prints each case's log. A fixed example runs
//! with the seeds of the scenarios, so `TWINLEAF_TURMOIL_SEED` runs it with
//! one seed alone.

mod classes;
mod driver;
mod examples;
mod hosts;
mod laws;
mod model;
mod report;
mod schedule;

pub(super) use hosts::{Event, Logged};

use super::{traced, REPLAYED};
use classes::CLASSES;
use proptest::strategy::{Strategy, ValueTree};
use report::{case_seeds, exercise, threads, Totals};
use schedule::{drawn, drawn_case, runner, valid, Case};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

// The phases of a case.

/// The longest warm up, in milliseconds.
const WARM_UP: u64 = 15_000;
/// The longest chaos, in milliseconds.
const CHAOS: u64 = 60_000;
/// The latest `H'` may come after the heal, in milliseconds.
const SETTLE: u64 = 70_000;
/// The bound `B`, in milliseconds.
const BOUND: u64 = 30_000;
/// The window `W`, in milliseconds.
const WINDOW: u64 = 10_000;
/// How long a case may run in virtual time.
const DURATION: Duration = Duration::from_secs(200);

// The hosts.

/// The most apps a case has.
const MOST_APPS: usize = 3;
/// The hosts of the apps.
const APPS: [&str; MOST_APPS] = ["app0", "app1", "app2"];
/// How long an app's open waits.
const OPEN_TIMEOUT: Duration = Duration::from_secs(10);
/// How long an app's call waits for its reply, in milliseconds.
const CALL_TIMEOUT: u64 = 20_000;
/// The `reconnect_timeout` a case may draw, in milliseconds.
const RECONNECT: u64 = 10_000;

// The timers of section 6, in milliseconds, as the laws count them.

/// The device's quiet time over TCP and the engine's, after which the proxy
/// loses a silent device or hub child.
const QUIET: u64 = 3_000;
/// How long the proxy's worker waits between attempts to reach the device.
const RECONNECT_INTERVAL: u64 = 1_000;
/// A connect timeout and the reconnect interval after it, the longest the
/// proxy takes to reach a device whose link is usable again.
const RECONNECT_TRY: u64 = 4_000;
/// The keepalive's quiet time, after which the proxy lets a silent client go.
const KEEPALIVE_QUIET: u64 = 5_000;
/// How long an app takes to see its socket lost, the keepalive's quiet time
/// and the close timeout after it.
const SOCKET_LOSS: u64 = 6_000;
/// How long a closed port takes at most to write what was queued and let go
/// of its link.
const CLOSE_TIMEOUT: u64 = 1_000;
/// How long the device takes to see its connection end once no app holds
/// the link, the idle time and a second for the close.
const IDLE_CLOSE: u64 = 6_000;
/// The longest an app waits before it opens again once the proxy is back,
/// the backoff's cap with its jitter.
const REOPEN_WAIT: u64 = 1_250;
/// The reopen limit, after which a session whose socket was lost ends.
const REOPEN_FOR: u64 = 30_000;
/// How much of the reopen limit an app's path must have been unusable for
/// before its connection may end with `Disconnected`, as S8 says.
const MOSTLY_UNUSABLE: u64 = 23_000;
/// The longest gap between two batches an app takes, two samples.
const SAMPLE_GAP: u64 = 2_000;
/// How long a call may take unless the device connection is tainted, as L9
/// says.
const CALL_BOUND: u64 = 10_000;
/// The longest blip, in milliseconds.
const LONGEST_BLIP: u64 = 2_500;

/// Generated schedules of faults keep every law whose source is settled.
/// The laws of open questions are counted, and a run prints them with the
/// classes its cases reached and the real time they took. A failing case
/// is shrunk, and its seed reruns it alone. Cases run on several threads,
/// and a failure stops the run once the cases under way end.
///
/// Each law has mutants the suite must catch, applied by hand and run with
/// this test. Where a constant changes, the mutant uses a value a person
/// might plausibly write, as well as an extreme one. A survivor gets one
/// answer. A law is missing, the generator cannot reach the case, or the
/// mutant is equivalent, with the reason.
/// - S1. In `Mounts::feed` in `proxy/engine.rs`, queue each packet twice,
///   `mount.outbox.stream(shown.clone())` before `mount.outbox.stream(shown)`.
/// - S2. In `StreamTracker::detect_boundary` in `data/state.rs`, count one
///   more, `first_sample_n.value() - expected.value() + 1`. In the same
///   function, drop `opened &&` from the condition `opened && self.relearned`
///   of the `Restarted` branch, so a gap after a link loss is told as
///   `Restarted`.
/// - S3. In `StreamTracker::detect_boundary`, delete the `SessionChanged`
///   branch, `if device.session() != self.last_session_id`.
/// - S4. In `Outbox::update` in `proxy/outbox.rs`, put a metadata update on
///   `Queue::Urgent`, in the arm `Update::Metadata(_) => Queue::Stream`.
/// - S5. In `Engine::take` in `proxy/engine.rs`, store the session plus one
///   in `store.set(route, PRESENCE, Presence::Alive { session })`. In the
///   heartbeat arm `beat.session().or(held.flatten())` of the same function,
///   prefer the held session to the heartbeat's,
///   `held.flatten().or(beat.session())`.
/// - S6. In `Worker::dispatch_device_packet` in `proxy/worker.rs`, answer
///   with `original_id ^ 1` in `self.answer(port, pkt.with_rpc_id(original_id))`.
///   For the retry, a small patch that keeps each request packet in
///   `RpcMapEntry`, deletes the `self.cancel_active_rpcs()` under
///   `if self.device.is_none()` in `Worker::turn`, and sends every request
///   still in `rpc_map` in the arm `Ok(device) => self.device = Some(device)`
///   of `Worker::turn`, where it adopts the device.
/// - S8. Make `REOPEN_FOR` in `device/session.rs` 25 s and 3 s. In
///   `Worker::device_received` in `proxy/worker.rs`, delete
///   `self.reconnect_deadline = None`. In `Worker::lose_device`, set
///   `self.reconnect_deadline` to `now + timeout / 2` for `now + timeout`.
/// - S9. In `Outbox::end` in `proxy/outbox.rs`, also set the stop due, so a
///   client let go hears `Stopped`.
/// - L1. In `Worker::turn` in `proxy/worker.rs`, delete the liveness check
///   `let liveness = self.device.as_ref().and_then(Device::liveness_deadline)`.
///   In the same function, wait 100 s in
///   `self.next_attempt = Instant::now() + RECONNECT_INTERVAL`.
/// - L2. In `Mounts::feed` in `proxy/engine.rs`, feed only the first mount,
///   `list.mounts.iter().take(1)` for the loop `for mount in &list.mounts`.
///   In `Outbox::queue` in `proxy/outbox.rs`, compare with 0 in the loop
///   `while queues.sample_bytes > SAMPLE_BUDGET`.
/// - L3. The first S6 mutant, with one call in flight.
/// - L4. Delete `self.reconnect(&mut store, route)` in `Engine::apply_status`
///   in `proxy/engine.rs` for the root, and in the `Presence::Lost` arm of
///   `Engine::take` for the child.
/// - L5. At the end of `Worker::lose_device` in `proxy/worker.rs`, add
///   `std::mem::forget(device)`, so the lost port's socket and task stay.
/// - L6. (a) Make `LIVENESS_TIMEOUT_TCP` in `proxy/worker.rs` 4000 ms and
///   30000 ms. (b) Make the `quiet` of `CLIENT_KEEPALIVE` in
///   `proxy/control.rs` 7 s. (c) In `Session::serve` in `proxy/client.rs`,
///   guard the arm `received = self.incoming.next()` of the `select!` with
///   `if self.opening.is_none()`. (d) Make `IDLE` in `proxy/mod.rs` 8 s.
///   (e) Make `QUIET` in `proxy/engine.rs` 4 s.
/// - L7. (a) In `Worker::lose_device` in `proxy/worker.rs`, set
///   `self.reconnect_deadline` to `now + timeout * 3 / 2`. In `Worker::packet`
///   in `device/session.rs`, delete `self.reopen = None` on
///   `FailedToReconnect`. (b) Make `REOPEN_FOR` in `device/session.rs` 35 s.
/// - L8. In `Worker::device_received` in `proxy/worker.rs`, delete
///   `self.status_to_engine(packet::LinkStatus::SensorReconnected)`. In
///   `Worker::turn`, wait 5 s in
///   `self.next_attempt = Instant::now() + RECONNECT_INTERVAL`.
/// - L9. Empty `Worker::cancel_active_rpcs` in `proxy/worker.rs`. Delete the
///   loop `for pending in std::mem::take(&mut self.pending).into_values()`
///   in `Worker::lose` in `device/session.rs`, which fails pending calls.
/// - L10. In `Mounts::feed` in `proxy/engine.rs`, put link statuses on the
///   first mount only.
/// - L11. In `Engine::apply_status` in `proxy/engine.rs`, never forget the
///   subtree on `SensorDisconnected`, at `let forget = matches!(status, ...)`,
///   so samples of a new boot wait on metadata the parser thinks it has. If
///   it survives, record whether the records sent first make it equivalent.
/// - L12. In `open_again` in `device/session.rs`, cap the backoff at 5 s in
///   `wait = (wait * 2).min(Duration::from_secs(1))`.
/// - M1. In `Device::liveness_timeout` in `proxy/worker.rs`, use
///   `LIVENESS_TIMEOUT` for TCP in the arm
///   `transport::TransportKind::Tcp => LIVENESS_TIMEOUT_TCP`.
/// - M2. In `jittered` in `device/session.rs`, draw from the nanoseconds of
///   `SystemTime::now()` for `let random = crate::rand::next_u64()`.
/// - M3. In `Mounts::feed` in `proxy/engine.rs`, feed only the last mount
///   added, so an app that opens again starves the others.
/// - I2. In `Engine::next_wake` in `proxy/engine.rs`, make a pending retry
///   due now, at
///   `let retry = self.schedule.next_retry(asked(&self.heard, &self.demand))`.
///
/// The restart check `std::mem::take(&mut device.restarted)` in
/// `Worker::turn` in `proxy/worker.rs`, and the `self.cancel_active_rpcs()`
/// it calls, are equivalent here, as section 5 says.
#[test]
fn generated_fault_schedules_keep_the_laws() {
    let seeds = case_seeds();
    let all = std::env::var("TWINLEAF_TURMOIL_ALL").is_ok();
    let totals = std::sync::Mutex::new(Totals::default());
    let failed = std::sync::Mutex::new(Vec::<(u64, String)>::new());
    let next = AtomicUsize::new(0);
    let stop = AtomicBool::new(false);
    let started = std::time::Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..threads().min(seeds.len()) {
            scope.spawn(|| {
                while !stop.load(Ordering::SeqCst) {
                    let Some(&seed) = seeds.get(next.fetch_add(1, Ordering::SeqCst)) else {
                        break;
                    };
                    if let Some(failure) = exercise(seed, &totals) {
                        crate::chan::lock(&failed).push((seed, failure));
                        stop.store(!all, Ordering::SeqCst);
                    }
                }
            });
        }
    });
    let totals = totals.into_inner().expect("the totals");
    totals.print();
    eprintln!("  the run took {:?} of real time", started.elapsed());
    let mut failed = failed.into_inner().expect("the failures");
    failed.sort_by_key(|(seed, _)| *seed);
    if let [(_, failure)] = failed.as_slice() {
        panic!("{failure}");
    }
    let shown = |failure: &String| {
        failure
            .split(".\nminimal failing input")
            .next()
            .unwrap_or_default()
            .to_string()
    };
    let firsts: Vec<String> = failed.iter().map(|(_, failure)| shown(failure)).collect();
    assert!(
        failed.is_empty(),
        "{} cases failed\n{}",
        failed.len(),
        firsts.join("\n")
    );
    if std::env::var("TWINLEAF_TURMOIL_SHARES").is_ok() {
        let missed: Vec<&str> = CLASSES
            .iter()
            .copied()
            .filter(|class| !totals.classes.contains_key(class))
            .collect();
        assert!(missed.is_empty(), "no case reached {missed:?}");
    }
    assert!(
        totals.unjudged.is_empty(),
        "the harness could not judge {:?}",
        totals.unjudged
    );
}

/// The schedule steps of `Step` are valid in every generated case, and in
/// every case shrinking leaves.
#[test]
fn generated_and_shrunk_schedules_are_valid() {
    for seed in 1..=256 {
        let mut runner = runner(seed, 0);
        let mut tree = drawn().new_tree(&mut runner).expect("a case");
        for _ in 0..64 {
            let (swarm, schedule) = tree.current();
            let case = Case {
                seed,
                swarm,
                schedule,
            };
            if let Err(why) = valid(&case) {
                panic!("{why} in {case:#?}");
            }
            if !tree.simplify() {
                break;
            }
        }
    }
}

/// How many cases the replay check looks through for ones in which an app
/// opens again.
const LOOKED_THROUGH: u64 = 64;

/// A generated case in which an app opens again, run twice with one seed,
/// gives the same turmoil trace and the same log.
#[test]
fn a_generated_case_run_twice_with_one_seed_runs_the_same_way() {
    let mut replayed = 0;
    for seed in 1..=LOOKED_THROUGH {
        if replayed == REPLAYED {
            break;
        }
        let case = drawn_case(seed);
        let mut first_run = None;
        let first = traced(|| first_run = Some(driver::simulate(&case)));
        let Some(Ok(first_run)) = first_run else {
            continue;
        };
        if !first_run.reopened(case.swarm.apps) {
            continue;
        }
        let mut second_run = None;
        let second = traced(|| second_run = Some(driver::simulate(&case)));
        let Some(Ok(second_run)) = second_run else {
            panic!("the second run of the case of seed {seed} was not judged");
        };
        let longest = first.len().max(second.len());
        if let Some(at) = (0..longest).find(|&at| first.get(at) != second.get(at)) {
            panic!(
                "the runs of seed {seed} differ at trace event {at}\nfirst  {:?}\nsecond {:?}",
                first.get(at),
                second.get(at)
            );
        }
        let longest = first_run.log.len().max(second_run.log.len());
        if let Some(at) = (0..longest).find(|&at| first_run.log.get(at) != second_run.log.get(at)) {
            panic!(
                "the runs of seed {seed} differ at log event {at}\nfirst  {:?}\nsecond {:?}",
                first_run.log.get(at),
                second_run.log.get(at)
            );
        }
        replayed += 1;
    }
    assert_eq!(
        replayed, REPLAYED,
        "too few of {LOOKED_THROUGH} cases have an app open again"
    );
}
