# twinleaf-web: the plot page, served by the proxy

`tio proxy` serves a browser page at `http://localhost:7855`, on the port it
already listens on: live plots of every device the proxy reaches, and a
panel of their RPCs. A build of `twinleaf-tools` without its `web` feature,
and every other user of the `twinleaf` library, pulls in no web dependency.

```
twinleaf-proto ← twinleaf ← twinleaf-web ← twinleaf-tools [feature "web"]
```

- `twinleaf` has one hook for it: `HostOptions::foreign`, which serves the
  connections that do not speak the packet protocol.
- `twinleaf-web` is the web layer: the HTTP routes, the WebSocket, the
  server-side trace processing and the built page (`ui/`). It uses
  `twinleaf`'s public API only, with its `json` and `processing` features.
- `twinleaf-tools` installs the handler in the foreground `tio proxy`, on
  the default port only (`serves_page`): the hub reaches the proxy through
  `Connection::connect()`, which finds the default port or
  `TWINLEAF_PROXY`. A background proxy idles out when its last client goes,
  and a page is no client.

## One port, two protocols

The proxy tells the two apart by the first byte a connection sends
(`twinleaf/src/proxy/server.rs`). A byte that begins an HTTP method (`C D G
H O P T`) goes to the handler, and every other byte to a client task. A
client may send a packet before `proxy.hello`, so the rule is HTTP's, not
"only `PROXY_TX` is a client". A connection that sends nothing within 2 s
closes. The peek runs in a task per connection, so a silent connection
holds up no accept.

## Security (`guard.rs`)

A page on any site can make the browser send requests to `localhost:7855`,
and with DNS rebinding read the answers. This page changes device settings,
so every request passes two checks, or gets a 403 `{"error": ...}`:

- `Host` must name loopback: `localhost`, `*.localhost`, `127.0.0.1` or
  `[::1]`, with any port.
- `Origin`, when present (a WebSocket upgrade, any request but GET), must be
  this server's own origin.

The server sends no CORS headers, and hyper's `header_read_timeout` is set.

## The server (`twinleaf-web/src`)

| Module | What it holds |
|---|---|
| `protocol` | every message and request type, and the contract below |
| `wire` | the binary frames: points, spectra and captures |
| `feed` | the channel of changes, which numbers each one |
| `hub` | `Live` (one connection to the proxy, its `Hub` of traces, its devices) and how it ends |
| `devices` | each device's newest identity, RPC table, settings and captures; calls and reads |
| `loss` | what was lost of each device and stream before the hub's history held it, by where |
| `captures` | the captures pages ask for: the task that takes each capture RPC's, and each page's outbox |
| `firmware` | what each device runs, the catalog of published firmware, and the upgrades pages ask for, one at a time |
| `routes` | the routes, which hub is current, mount order |
| `discover` | the device picker's scan |
| `guard`, `assets` | the checks above, and the built page |

One hub (`Live`) serves every page. It opens with the first request that
needs it, of the devices a page last mounted or the proxy's default. It
ends one way, `End::retire`, when a page mounts other devices or its
connection's samples end; its sockets close and the next request opens
the hub that takes its place.

## The protocol (`protocol.rs`, mirrored by `ui/src/lib/protocol.js`)

The page holds one hub's state and comes to hold it one way, from a
snapshot:

1. **Snapshot**, as a socket opens and whenever a page fell more than 1024
   changes behind (`Lagged`): `hub {id, seq, behind}`, a `trace` per trace, one
   points frame of all history sent so far, `config`, `memory`, `routes`, then each
   listed device's newest `device`, `table`, `settings`, `captures`, a
   `capture` per capture RPC the hub has taken, `firmware` once the hub
   read what it runs and `upgrade` once a page asked an upgrade of it, a
   `loss` per device that lost anything, and the newest 500 lines of the
   log, a `log` each.
   The page drops
   everything of any hub and builds a new `Session` from it. `behind` is
   how many changes the page fell behind, 0 as a socket opens.
2. **Changes**, each numbered `seq` one more than the last (`feed.rs`):
   points frames (every 16 ms), one spectra frame per round (every 100 ms),
   `trace` for a new column, device messages (`firmware` and `upgrade`
   among them), `memory` when what the buffer
   holds or may hold changed (checked each second: `bytes` of `budget`, the
   `seconds` held of `max_age`, and the bytes per second `rate`), `loss`
   when a device lost more (checked each second), `log` as a line of
   the log is taken, and on a
   decimation change `clear`, the history again and `config`.
3. **Gaps** go back through 1: a change numbered other than the next, or a
   frame naming a trace the session does not have, makes the page close its
   socket and open another (`Hub.resync`). A retired hub closes the socket.

Devices are latest-value state, as the proxy keeps them: the hub keeps the
newest of each kind, sends a message only when it changes (a heartbeat
sends nothing), and sends a device's messages only while `routes` lists it
(a device listed again sends its newest again). Only points and spectra
stream.

The page sends `spectra {traces}`, naming the trace ids its
open tab shows as spectra, after each snapshot and when that set changes.
The hub estimates and sends only the spectra some page names; a page's
names go when its socket does. It sends `hold {on}` while it holds a plot
paused, the three capture messages below, and the four firmware messages
under Firmware.

### What was lost

A sample can be lost in five places between a device and a plot. Each
has its own count, and the page is told of each apart.

| Where | Counted by | Reaches the page as |
|---|---|---|
| The device skipped it, or its link lost it while it stayed up | the parser, from the sample numbers (`Loss::skipped_samples`) | `loss.streams[stream].skipped` |
| The link to the device was down, or the hub's connection to the proxy was | the proxy's link, which says so in order with the samples, and the device's presence | `loss.link {times, seconds}`, `loss.streams[stream].link` |
| The proxy dropped it from its queue for the hub's connection (1 MiB of samples) | the proxy's outbox, which tells the client in a `PROXY_RX` packet of kind 4, `Dropped {stream_id, packets}`, ahead of the samples that follow | `loss.streams[stream].proxy` |
| The hub's connection dropped it: its queue of 1024 batches was full, or its inlet of 32768 packets was | the connection's queue (`Loss::dropped_samples`) | `loss.streams[stream].hub` |
| The page fell more than 1024 changes behind the hub | the page's socket task | `hub.behind` of the snapshot sent in their place |

`loss {route, link: {times, seconds}, streams: {name: {skipped, link,
proxy, hub}}}` is one device's counts since the hub started, the newest
value as a device's other messages are: `streams` by the stream's name,
each count in rows, `link.times` how often the device's link dropped after
the hub first heard it, and `link.seconds` how long it was down in all, to
a tenth, up to now while it is down. The hub's thread sends it at most once
a second, when it changed. A row is in one count only. The rows lost
before a stream's next rows are counted where the parser was told they
went since the stream's last rows: with the link, if it was lost; else as
the hub's, if its connection had no room for a packet; else as the
proxy's, if the proxy said it dropped packets of that stream; else as
skipped. A device that restarts while its link is down sends rows that
say no count, so `link.times` says what `streams` cannot.

The page keeps its own counts from these (`lib/loss.svelte.js`), by
device, since it opened: what each `loss` adds to the one before, so what
a hub had lost before the page first heard it is not the page's. It counts
four: a link's drops and their seconds, the samples skipped, the samples
dropped (the proxy's and the hub's together) and the times the page fell
behind.

The status bar has a loss mark only while something is missing, after
History, so nothing moves as it comes and goes:

- While a device's link is down: `link down 3 s`, in the error's colour,
  for the device longest down, counted from when the page heard it lost.
- Else, while the last minute lost anything: `18% missing · link`, in the
  warning's colour. The share is that of the device that lost the most:
  the seconds its link was down and, of its stream that lost the most
  samples, the seconds they stand for at the stream's rate, with the
  seconds the page fell behind (each change a 16 ms flush), over the 60 s.
  The word is the cause that stands for the most seconds of data: `link`,
  `skipped`, `dropped` or `page`.
- Its tooltip is one line of the page's counts since it opened, and a
  press opens the log drawer on the lines of lost data.

A plot marks each place its trace is missing samples
with a dashed line, and a gap wide enough to see, as a link's outage is,
with a band from the last point before it to the first after: it tells
the two apart by the gap's width, which is all a points frame says of a
break.

### The log

`log {n, wall, time, level, source, part?, route, text, loss?}` is one line of the
log (`logbook.rs`):

| Field | What it is |
|---|---|
| `n` | The line's number, from 1 since the hub started, in the order the hub took the lines. |
| `wall` | The host's clock as the hub took the line, in Unix seconds, to a millisecond. |
| `time` | The time of the newest sample the hub had taken of the line's device as it took the line, on the clock that device's points are on; `null` for a device no sample was taken of yet, and for a line of no device. |
| `level` | `error`, `warn`, `info` or `debug`. |
| `source` | `device`, a line the device said in a log packet, or `proxy`. |
| `part` | Only on a proxy's line: the part of the proxy that said it, `hub` for the hub's own lines, and for a line of the proxy's console the source the console names (`server`, `link`, `client`, a link's URL). |
| `route` | The device the line is of, as `/` or `/0`: the one that said it, or the one the proxy says it of; `null` for a line of no device. The device's name is its `device` message's. |
| `text` | The line's words. |
| `loss` | Only on a line of a loss: what was lost, as data (below). |

Where the lines come from:

- **A device's.** The hub reads every packet of its connection
  (`Connection::packets`) and takes each log packet as a line of the device
  at the packet's route, its text as sent. The device's level 0 and 1
  (critical, error) are `error`, 2 `warn`, 4 `debug`, and 3 or any other
  `info`. A device sends only what its `dev.loglevel` lets through.
- **The proxy's.** The hub's own: a device as its metadata first names it
  or names it anew (`info`), a device's RPC table that did not load
  (`warn`), the device lines missed when the hub's reader of packets fell
  behind its queue of 8192 (`warn`, of no device), and each loss as
  `loss.rs` counts it.
- **The proxy's console.** Every record `tio proxy` prints on its console
  is a proxy line of no device too, when the proxy serves the page: its
  logger (`twinleaf-tools/src/logging.rs`) hands each record it writes to
  `twinleaf_web::console(level, source, text)`, after the console's own
  filter, so what the terminal shows is what the log holds. `text` is the
  record's text and `part` the last part of the record's target as the
  console prints it (`server`, `mdns`, `client`). Error,
  warn and info
  keep their level, and debug and trace are `debug`. Every record of a
  link has the one part `link`: the link's own (`Opening`, `Closing`), its
  worker's (target `twinleaf::proxy::worker`), and those the worker logs
  of its device under `proxy::<url>`, whose text then starts with the URL.
  Four things are left out: a record of target `device::<route>`, which is
  a device's log packet the proxy echoes and the hub already takes from the
  packet itself, a record of target `status::<url>`, which says a link
  status the proxy also tells its clients ("Sensor disconnected", "Sensor
  reconnected", "Stopping reconnection attempts due to timeout") and the
  hub says as the link's own line, a
  record the log's own owner writes of a line it says in the log (`loss.rs`
  "fell behind", inside `logbook::unheard`), and what the proxy prints
  without the `log` crate, as its "Plots:" line. A record that comes with
  no hub alive, as at start-up, waits, the newest 200 (`logbook::HELD`),
  and is in the next hub's log with the time it was said.

A line of a loss has `loss`, by `cause`, so nothing reads a loss out of
`text`:

| `loss` | Said when | Level |
|---|---|---|
| `{cause: "link", seconds: null, why?, url?}` | the device's link drops (`loss.link.times` grows) | `error` |
| `{cause: "link", seconds, url?}` | the link is back, after `seconds` down, to a tenth | `info` |
| `{cause: "skipped", stream, samples}` | the counts are sent (each second they grew): what `loss.streams[stream].skipped` grew by | `warn` |
| `{cause: "proxy", stream, samples}` | the same, of `.proxy` | `warn` |
| `{cause: "hub", stream, samples}` | the same, of `.hub` | `warn` |

The rows lost with a link down (`loss.streams[stream].link`) are no line
of their own: the link's two lines stand for them.

A link's two lines are the hub's, one for the drop and one for the return.
`why` is why the proxy lost the device, `disconnected` (the device went
quiet on its link or the transport closed), `stopped` (the proxy stopped),
`failed_to_connect`, `failed_to_reconnect` or `quiet` (silent for 3 s while
its link stayed up), and `url` the address of the link that serves the
device. The hub has both from the proxy, through its connection
(`Device::link_updates`): the proxy's watch of a device says the link's
URL, and the cause of a loss ahead of the lost presence it explains. A hub
that lost its own connection to the proxy has neither to say.

**The two clocks.** A plot's x is its device's own time, the timestamps of
its samples. A log packet carries no time on that clock the hub can place
(its one number is the sender's own, the uptime in ms on Twinleaf
devices), so the hub stamps each line as it takes it: `time` is the
timestamp of the newest sample it has taken of that device, whichever of
its streams it is of. The line was said no later than that sample by more
than what waited in the hub's two queues, of packets and of batches, and
one sample period: a few milliseconds while the hub keeps up. A proxy's
line of a device is stamped the same way, so a link's drop carries the
time of the last sample before it, and its return the first after. `wall`
is the host's clock, which the page shows as the line's time of day;
`time` is what places the line on a plot.

**The bound.** The hub keeps the newest 500 lines (`logbook::KEPT`) for a
snapshot, oldest first; older ones are gone from the hub. A line is a
numbered change as it is taken, and a snapshot's lines carry no `seq`. A
line may reach a page in a snapshot and again as a change after it, as the
snapshot's lines are read after the page subscribes: the page keeps a line
only when its `n` is past the last it holds of that hub.

**On the page** (`lib/log.svelte.js`, `lib/LogDrawer.svelte`). The status
bar's Log item counts the warnings and errors that arrived while the
drawer was closed; those of a hub's first snapshot were never unseen.
The drawer opens under the docks and the plots, by the item, by `` ` ``
(`showLog` in the command table) or by the loss mark, in a pane of its
own whose height is dragged at its edge and kept, with whether it is
open, under `tl-log-v1`. It keeps the newest 1000 rows, a row being one
line or several in a row that say the same (losses of one cause of one
stream of one device sum their samples). Its bar chooses the rows shown:
a toggle per level with its count of lines (Debug off to start), one for
the lines of a loss, the source (every line of one device, whoever said
it, or the lines of one part of the proxy, the list made of the devices
and of the parts the rows were said by, each under its heading) and a
filter of words. A row's source column names its device, or the part of
the proxy that said it. Follow keeps the
newest row in view until the list is scrolled up. The row of a link's drop
is followed by a divider, the outage, which says how long it lasted once
the `link` line with `seconds` comes. A row's button runs `x1At` with the
line's `time`: X1 at that moment on every time plot of the open tab, each
paused and moved to show it, as one edit and one view step. The page
writes a loss row from `loss`, in its own words, and never reads `text`
for it.

### Captures

A capture is a device's one-shot readout of a buffer, such as the ADC
samples of one FID shot. A device has one capture RPC per kind of capture
(flagged CAPTURE, or named `*.capture` on a device that does not flag
it). A capture has no absolute time, and the hub sends none. The page
lists them in its Signals list and draws them in capture plots, as "A
capture, to a pixel" says below.

**Listing.** As a device's table loads, the hub lists its capture RPCs and
reads each one's metadata once, without triggering, for what the device
says before a capture is taken:

```json
{"type": "captures", "seq": 41, "route": "/", "captures": [
  {"rpc": "fid.capture",
   "label": {"name": "FID", "units": "V", "x_name": "Time", "x_units": "s"}},
  {"rpc": "old.capture", "label": null}]}
```

`label` is `null` for a device that does not answer its metadata before a
capture. The first capture the hub reads of that RPC fills it in, and the
message goes out again whenever a capture's metadata names it otherwise.
`name` is the capture's identity for a person. A device with no
capture RPC lists `[]`.

**Asking.** Three messages from the page, each naming one capture RPC of
one device. Only an RPC that device's `captures` lists is taken; any other
ask is dropped and logged.

```json
{"type": "capture", "route": "/", "rpc": "fid.capture"}
{"type": "repeat", "route": "/", "rpc": "fid.capture", "delay": 0.05}
{"type": "stop", "route": "/", "rpc": "fid.capture"}
```

- `capture` asks for one capture now. If the hub is already taking one of
  that RPC, that one answers the ask.
- `repeat` asks for captures again and again: trigger, read, wait `delay`
  seconds (0.05 to 10, clamped), repeat. Several pages may ask; the hub
  runs one loop per RPC at the shortest delay asked. Sent again, it changes
  the page's delay.
- `stop` takes back both. A page's asks also go when its socket closes.
  The loop ends when no page wants it, or when the device is lost; a
  capture in flight that no page wants any more is given up between two
  calls.

**State.** Every page hears where each capture RPC stands, as a numbered
change, and a snapshot holds the newest:

```json
{"type": "capture", "seq": 57, "route": "/", "rpc": "fid.capture",
 "state": "waiting", "number": 12, "repeat": 0.05}
```

| `state` | Means |
|---|---|
| `idle` | nothing in flight or awaited, and the last capture taken did not fail |
| `capturing` | triggered; the device has not finished |
| `reading` | the blocks are being read, with `read` and `size` in bytes; only sent for a read longer than 0.1 s, then every 0.1 s |
| `waiting` | a repeating capture was read and waits its delay |
| `busy` | the device refused the trigger: a capture is already in progress on it (another host's, or one the hub gave up) |
| `timeout` | the device did not finish within 10 s of the trigger, or a call got no reply in 3 s |
| `error` | anything else, with `error` as text: the device's error status, a refused call, metadata this build cannot read, an element type the frame does not carry, or `the device is gone` |

`number` is the number of the last capture the hub read of that RPC (0
for none). `repeat` is the delay in effect, or `null` when no page asks
for it to repeat. A failure is a state, not a refused request: one
capture asked for ends in `busy`, `timeout` or `error`; a repeating one
shows the failure, waits its delay but at least 1 s, and tries again. It
stops only for `the device is gone`, which also clears every page's ask.

**The capture.** Each capture the hub reads goes, as one binary frame of
kind 3 (`wire.rs`), to the pages that asked for that RPC and to no other:

```text
u8 kind = 3, 7 × u8 pad, u64 seq = 0
u64 number
u32 n, u8 element type, u8 element bytes, u16 pad
f32 y calibration, f32 x offset, f32 x stride, u32 pad
6 × u16 byte lengths of route, rpc, name, units, x name, x units, u32 pad
the six strings (UTF-8), zero padded to a multiple of 8 bytes
n × element, little endian, exactly as the device sent them,
  zero padded to a multiple of 8 bytes
u32 m, u32 pad
m × f32 frequency, m × f32 density
```

The elements are in the device's own type and width (`0x10` u8, `0x11`
i8, `0x20` u16, `0x21` i16, `0x40` u32, `0x41` i32, `0x42` f32, `0x80`
u64, `0x81` i64, `0x82` f64; a capture of any other type is an `error`).
Element `i` is `element × y calibration` in `units` at `x offset + i × x
stride` in the x units. They start on a multiple of 8 bytes, so the page
can view them in place as a typed array. The `m` bins after them are the
capture's amplitude spectral density, estimated as a signal's spectrum is
(`Welch`: four Hann windowed segments, half overlapped, over all its
values), along the reciprocal of its x: frequency in cycles per x unit,
density in its units per root cycle per x unit; none (`m` 0) for a
capture too short for an estimate or of no positive x stride. The frame is not a numbered
change (`seq` 0) and is in no snapshot: a page is sent the captures read
after it asked.

`number` counts the captures the hub read of that RPC, from 1, in a hub's
lifetime, and is the same number the `capture` state carries, so a page
can tell a new capture from one it has. A page that reads slowly is not
queued for: each page has one slot per capture RPC, a newer capture
replaces an unsent older one, and the numbers that page sees skip.

The hub takes a capture one call at a time (trigger, status until done,
at once and then after waits that double from 2 ms to 100 ms, metadata,
then each block in turn), so the stream and other RPCs share the link with
at most one capture call in flight per capture RPC.

### Firmware

The hub reads what each device runs, and installs firmware the page asks
for, as `tio upgrade` does: it calls `twinleaf::firmware`
(`query_installed`, `check_for_update`, `download_cached`, `flash`,
`reread_installed`) and copies none of it. Each call blocks, so each runs
on tokio's blocking threads, on the device `DeviceTree::device` gives.

**What a device runs.** The hub reads a device's `dev.desc` once each
time its RPC table loads (beside listing its captures) and again after an
upgrade. A read that fails leaves what the hub knew. Once a read succeeds
the device has a `firmware` message, latest-value as a device's other
messages are:

```json
{"type": "firmware", "seq": 41, "route": "/",
 "installed": {"name": "tio-test", "revision": "R1", "date": "2026-06-08",
               "build": "000002", "serial": null, "development": false},
 "catalog": {"state": "unchecked"}}
```

`installed` is `twinleaf::firmware::InstalledFirmware` field for field
(`build_date` as `date`, `YYYY-MM-DD`, `hash` as `build`, `is_development`
as `development`). `name` and `revision` are `""` and `date`, `build` and
`serial` are `null` where `dev.desc` gives none (the simulator's `(null)`
serial is `null`). It is `null` only before the device answered.

**The catalog** is consulted only when a page asks, never on a timer or
as the hub starts: `tio` has no background policy, and `tio upgrade` looks
the catalog up when it is run. The hub keeps what it found per device.

| `catalog.state` | Means |
|---|---|
| `unchecked` | no page has asked since the hub started, or the device now runs a build the last check listed nothing for |
| `checking` | the hub is looking it up |
| `unreachable` | the catalog could not be listed; `error` is the `FirmwareError` text |
| `checked` | `status` and `releases`, every release for the device's name and revision, newest first (`[{date, build, file}]`) |

`status` is `update` (a newer release), `current` (the newest, or a build
at least as new, or the same build), `none` (nothing published, or a
`dev.desc` that names no sensor and revision), `development` (a
development build: `releases` is `[]` and the catalog was not asked) or
`unknown` (no build date to compare). Once a device runs other firmware,
after an upgrade or a restart, the hub finds its status again from the
releases it holds, without the network (`check_for_update` over a
catalog of those releases).

The catalog is the published repository on GitHub (`GithubCatalog::twinleaf`),
its downloads cached where `tio upgrade` caches them
(`default_cache_dir`), unless `TWINLEAF_FIRMWARE_DIR` names a directory.
That directory is then the catalog, read from disk with no cache, laid out
as the repository is:
`{name}/{revision}/{name}-{revision}-firmware-{YYYY-MM-DD}-{hash}.bin`
(for the simulator, `tio-test/R1/tio-test-R1-firmware-2026-07-01-5d1494.bin`).
A name and revision with no folder have no release; a root that is no
directory is `unreachable`.

**Asking.** Four messages from the page:

```json
{"type": "check"}
{"type": "upgrade", "route": "/", "source": {"kind": "latest"}}
{"type": "upgrade", "route": "/", "source": {"kind": "release", "file": "tio-test-R1-firmware-2026-03-17-5d1494.bin"}}
{"type": "upgrade", "route": "/", "source": {"kind": "file", "name": "image.bin", "size": 412000}}
{"type": "cancel", "route": "/"}
{"type": "retry", "route": "/"}
```

- `check` looks every listed device whose `installed` the hub knows up in
  the catalog: `checking`, then the result. A check while one runs does
  nothing more.
- `upgrade` with `latest` installs the first release the device's last
  check listed, and `release` the one of that `file`. A `file` is followed
  by binary frames on the same socket whose lengths sum to `size`, in any
  chunking; the upgrade starts once `size` bytes came. Any text message,
  or the socket closing, before then drops the partial image and starts
  nothing. Binary frames with no image awaited are ignored. An image is at
  most 16 MiB (`MAX_IMAGE`, 16 777 216 bytes) and not empty.
- `cancel` takes back an upgrade that is `queued` or `downloading` (it
  stands `idle` at once; a download in flight is discarded when it returns
  and nothing reaches the device), and clears one that is `done` or
  `failed` to `idle`. It is refused while `uploading`, `finalizing` or
  `restarting`, and does nothing while idle.
- `retry` runs a `failed` upgrade again from the same source: the same
  release, or the file's bytes, which the hub keeps until the upgrade is
  done, so any page can retry.

**State.** Every page hears where each device's upgrade stands, and a
snapshot holds the newest. A device nobody asked an upgrade of has none.

```json
{"type": "upgrade", "seq": 57, "route": "/", "state": "uploading",
 "source": "file", "target": "image.bin", "sent": 28800, "size": 412000}
```

Every state but `idle` carries `source` (`latest`, `release`, `file`) and
`target`, the release as `"<date> <build>"` or the file's name.

| `state` | Means |
|---|---|
| `idle` | no upgrade asked, or one taken back or cleared |
| `queued` | waits for the upgrades asked before it |
| `downloading` | the release is being downloaded (`download_cached`, or the catalog's `download` with no cache) |
| `uploading` | the image is on its way: `sent` of `size` bytes (`size × chunk / chunks`), from 0 as `flash` starts, at most every 100 ms and the last |
| `finalizing` | from `FlashEvent::Committing`, through the 5 s settle |
| `restarting` | `flash` returned; the hub re-reads `dev.desc` (`reread_installed`, 6 reads 1.5 s apart) |
| `done` | with `installed`, as in `firmware`, or `null` if the device did not answer in time, which is no failure |
| `failed` | with `step` and `error`, the error's text |

`step` is `check` for an image the device cannot take (`FirmwareError::Refused`,
a signed package to a device that takes images or the other way round) or
any failure before `flash` stopped the device, `download`, `upload` (after
`dev.stop`), or `finalize` (from the commit on). After `done` the device's
`firmware` goes out anew, with the new `installed` and its status found
again.

**Refusals.** An ask the hub cannot take is answered to the asking page
alone, as an unnumbered text frame in no snapshot:

```json
{"type": "refused", "ask": "upgrade", "route": "/7", "error": "there is no device at /7"}
```

| `ask` | `error` |
|---|---|
| any | `there is no device at <route>` (no such device listed) |
| `upgrade` | `an upgrade of <route> is already running` (queued or running) |
| `upgrade` | `the catalog lists no such firmware for <route>` (`latest` or `release` without a checked catalog that lists it) |
| `upgrade` | `the image <name> is empty`, `the image <name> is <size> bytes, more than the 16777216 an upgrade takes`, `more than the <size> bytes the upgrade of <route> named came` |
| `cancel` | `the upgrade of <route> is uploading and cannot be cancelled` (likewise `finalizing`, `restarting`) |
| `retry` | `no upgrade of <route> has failed` |

**One at a time.** One upgrade runs at a time across the hub, in the order
asked; the others wait `queued`. This is how the page's "Upgrade all" runs
devices one after another, as `tio upgrade --all` does: one `upgrade` with
`latest` per device. One device's failure does not stop the rest.
`POST /api/mount` is refused with 409 `{"error": "a firmware upgrade is
running"}` while any upgrade waits or runs, as mounting retires the hub
and its connection. The hub logs each upgrade's start, each step and its
result with `log::info!` and `log::warn!`, naming the route and target.

Every `/api` route takes the request type `protocol.rs` declares and
answers its reply type. A request the server cannot serve answers a non-2xx
status with `{"error": "..."}` (`Refusal`), which `protocol.js` throws as
an Error. What a device made of a call is data in the reply, not a refusal.

| Route | Does |
|---|---|
| `GET /ws` | the socket above |
| `POST /api/call` | one RPC, one argument, several devices: an `Outcome` per route |
| `POST /api/read` | entries of one device no announcement keeps |
| `GET /api/discover` | a 3 s scan for devices, mDNS optional |
| `POST /api/mount` | opens devices in place of the hub's; 409 while a firmware upgrade waits or runs |
| `POST /api/pps` | the decimation every page shares |
| `POST /api/history` | the raw history the hub keeps: `{seconds, bytes}`, a span of each stream's time and a budget across every stream |
| `GET /api/window` | one trace of a named hub between two times, at about two points a pixel of `width`, for a paused plot |

## The page (`twinleaf-web/ui/src`)

Svelte 5 and shadcn-svelte components (`lib/components/ui`, generated by
the shadcn-svelte CLI, style "nova"). Custom drawing is only the plot
canvas and its WebGL layer.

| File | What it is |
|---|---|
| `App.svelte` | the shell: toolbar, the resizable left and right docks, tab strip, tiles, dialogs |
| `lib/docks.svelte.js`, `Dock.svelte` | the docks' panels: where each is, folded or open, dragged by its header or moved by its menu |
| `lib/hub.svelte.js` | the socket, the current `Session`, sequence checks, what the page asks of the hub: spectra, the hold, captures |
| `lib/session.svelte.js` | one hub's state: `Trace`s, `Detail` windows, `Capture`s, `Device`s, the clock |
| `lib/loss.svelte.js` | what the page lost and where, since it opened and in the last minute, across hubs |
| `lib/protocol.js` | the protocol mirror, `signalKey`, `captureKey`, `api` |
| `lib/firmware.js`, `FirmwareDialog.svelte` | what the page says of a device's firmware and of an upgrade, and the dialog that asks the hub for one |
| `lib/shots.js` | a capture's shots: calibration, the newest kept, and what a plot shows of them, the newest, an overlay or a mean |
| `lib/tabs.js` | what a tab is, the default tab, saving, export and import |
| `lib/workspace.svelte.js` | the tabs as the page edits them, focus, selection |
| `lib/views.js` | each time plot's `View`, pruned with its plot |
| `lib/plot.js` | `PlotView` (one plot's drawing and gestures), `View`, `lanesOf`, `monitorOf` |
| `lib/gl.js` | the WebGL renderer behind every plot |
| `lib/layout.js` | the tiling tree |
| `lib/drag.svelte.js` | moving plots, dropping signals, the selection box |
| `lib/keymap.js` | every key, which drives the handlers and the keys dialog |
| `lib/properties.js`, `Properties.svelte` | the Properties data table (TanStack table-core) |
| `StatusBar.svelte`, `LossCounts.svelte` | the page's foot: connection, devices and lost ones, data rate, what was lost in the last minute with a popover of the counts since the page opened, the hub's raw history (`memory`), frame rate, the keys |
| `PlotCard`, `Devices`, `DevicePicker`, `SignalList`, `TabStrip`, `CommandPalette`, `KeysDialog`, `DragOverlay`, `Tile` | the components |

### A sample, to a pixel

1. The proxy delivers a `SampleBatch` to the hub's connection, whose queue
   of 1024 batches is the one place samples wait for the hub. The hub's
   thread, raised to the priority of the threads that carry packets
   (`twinleaf::device::raise_thread_priority`), takes it from that queue. A
   queue that fills drops the batches that come, and the next batch of
   each stream says how many rows went (`Loss::dropped_samples`), which the
   hub counts by device and stream with what was lost before it
   (`loss.rs`). The hub's
   thread puts the batch in the `Buffer` of raw rows (10 min of each stream in at
   most 1 GB at first, as `/api/history` sets; past the budget the chunk
   that arrived first goes) and makes a trace of each new float column
   (`Hub::register`, id = its index).
2. Every 16 ms, `Hub::flush` catches each trace's `Fpcs` decimator up (the
   decimation is `pps` points per second per trace, 500 at first), splits
   the new points where samples were lost, and sends one points frame.
3. The page's `Hub.binary` checks its `seq` and hands it to
   `Session.apply`, which appends to each `Trace` (absolute indices, a pen
   lift at each break, 120 s kept, or more while a paused view holds it).
4. Each animation frame, `App` ticks the session's clock (device time =
   page time + a smoothed offset to the newest point) and draws every
   `PlotView`. A time plot asks its `View` for its x range, uploads the new
   points of each trace to that trace's GPU ring (`gl.js`, x relative to a
   base near the newest point), and draws one instanced quad per segment,
   split at breaks. Axes, cursors and readouts go on a 2D canvas above, the
   time axis counting from the session's first point (`t0`).
5. Paused, a plot asks `/api/window` (with the hub id) for its view and a
   span either side at every move, and once for its whole captured history.
   The session reads one window at a time for each, one a frame at most, and
   aborts a read the view has left or wants finer than
   (`Session.fetchWindow`). Each frame the plot draws from whichever of the
   two windows and its live points spans the most of the view, and of those
   the one with the most points across it (`pickSource` in `plot.js`), so a
   move is drawn from the best the page holds until its reply lands. The
   reply is the raw rows, decimated
   when they are more than two a pixel, and from 128 rows a point on the
   points of a level of the trace's `Pyramid` (`pyramid.rs`): FPCS
   decimations of every row the history holds, at ratios that double, kept
   as the rows come. So a window costs by its width, whatever its span
   (`Hub::window`). A spectrum plot draws the newest spectrum, or the one it
   showed when it paused.

### A capture, to a pixel

1. Each device's `captures` makes a `Capture` in the `Session` for every
   capture RPC it lists, keyed `capture:<rpc>` after the device's route
   (`captureKey`), a key no signal has. The Signals list shows them under
   that device's signals, headed Captures, each by the name its metadata
   gives, and they are picked, dragged and put on plots as signals are.
2. A plot shows captures or the signals that stream, never both, as they
   share no x axis (`tabs.js` `kindWith`): a capture given to an empty
   plot makes it a plot of kind `capture`, a plot that has signals of the
   other sort takes none and the page says so, and a capture plot can be
   switched to no other kind.
3. The page asks the hub for what its plots show (`Hub.showCaptures`, from
   `Workspace.captureAsks`): `repeat` for a capture some plot of the open
   tab shows on Auto, at the shortest delay of those plots, `stop` once
   none does or none shows it, and `capture` for Trigger. Auto and its
   delay are the plot's, kept with its tab, so a page loaded again asks at
   once. The hub drops every ask of a device it loses, so the page forgets
   what it asked and asks again when the device is alive again.
4. A capture frame is decoded to its elements as a typed array over the
   frame's bytes, calibrated once to 32-bit floats (`shots.js`; 32-bit
   floats of calibration 1 are used as they are), and kept as the newest
   shot of its `Capture`, 64 at most.
5. A capture plot is a slice, as a spectrum is, drawn by the same path
   (`PlotView.drawSlice`): its x axis is the capture's own, `x offset + i ×
   x stride` in the capture's x units, written with an SI prefix (0 to 750
   µs), its x axis linear and its y axis linear until set log
   (`captureYScale`). What it shows of the shots is the plot's
   setting (`shots`, `shotCount`): the newest, the newest over the ones
   before it, the older the fainter, or the mean of the newest alike, value
   by value. Cursors, peaks, the axis popovers, the legend, Copy data and
   Copy image are a slice's, so a capture plot has them as a spectrum has.
   Where its capture stands, and how many shots it shows, is written in the
   plot's lower left corner.
6. In its Spectrum view (`captureView`, set by the `captureView` command
   from its menu or its x axis popover) a capture plot is drawn as a
   spectrum (`constants.js` `drawnKind`): the hub's spectrum of each shot,
   in Hz and per √Hz where the capture's x is seconds, with a spectrum's
   log axes, scales, cursors and peaks. Of the newest, of each shot of an
   overlay, or the power average of the spectra of the newest alike, bin
   by bin, not the spectrum of their mean. A capture whose metadata names
   x units that are no time has no Spectrum view.
7. Export writes a capture plot's data as a CSV the page makes of the
   shots it held as the dialog opened, as the plot shows them, a header
   naming what they are (`shots.js` `shotsCsv`); no HDF5 holds a capture.

A spectrum's processing settings, when it has them, belong beside a
capture's: plot settings set by commands (`captureView`, `captureShots`,
`captureCount`), listed under Show in the plot's menu.

### A firmware upgrade

1. Each `Device` of the `Session` keeps its newest `firmware` (`fw`) and
   `upgrade` messages. Its row in the Devices panel has one line under its
   name, in every state, so a row is one height and nothing under it
   moves: the build date of what it runs, and beside it what
   `firmware.js` `rowState` makes of the two messages, nothing before the
   catalog was checked or when it was not reached, a newer release's date
   to press, Up to date, None published or Development build. An upgrade
   takes the line in the version's place: its step, the percent uploaded
   and a meter. The line never wraps. What it has no room for is cut with
   an ellipsis, the version first, and says itself whole in its tooltip,
   and an upgrade's number keeps its width.
2. The page asks the catalog only when a person does: the panel's Check
   for firmware updates, or opening the dialog on a device whose catalog
   is unchecked or was not reached. `tio` has no background check, and
   the catalog is on the network.
3. Upgrade firmware… is a command (`commands.js` `upgradeFirmware`, a row
   per device), in a device row's menu, the Devices panel's menu and the
   Commands box, and what a row offers runs it. The dialog shows the
   device, takes the source as `tio upgrade` has it (the newest release,
   another from the list, or a file from this computer, sent as the
   protocol says in frames of 64 KiB), warns that streaming stops and the
   device restarts, and sends `upgrade`.
4. From then on the dialog shows the device's `upgrade` state, the steps
   Download (none for a file), Upload with the bytes sent, Finalize and
   Restart, and at the end the version the device reports. The state is
   the hub's, so the dialog shows the same on any page whenever it is
   opened, and closing it stops nothing. Cancel upgrade sends `cancel`
   until the upload begins. A failure names its step and the reason, with
   Retry (`retry`), or, for an image the device cannot take, a way back to
   the sources. Closing the dialog on an upgrade that is done sends
   `cancel`, which clears it at the hub. An ask the hub refuses shows as
   a notice.
5. Upgrade all (N), offered once two devices have a newer release, lists
   them and sends one `upgrade` of the newest release for each. The hub
   runs them one at a time, the others waiting.
6. While a device restarts its samples stop, which its plots draw as the
   band of any gap that wide.

### A write to two devices

Each Properties panel (Settings, Reports, Actions, Diagnostics, Other) is
one RPC channel of the checked devices. Its table has a row per RPC name
(`mergeRows`) and a column per checked device, up to eight; Actions is a
row of buttons instead. More devices share one column, which shows
the value they agree on or how many values they hold. Enter or the edit bar calls `api.call(name, text,
routes)` for the checked devices whose entry is writable, or an action
(`writersOf`). The server encodes the text by each device's own table
(`devices::plan`). If any device lacks the RPC or cannot take the text,
nothing is sent: those are `refused` and the rest `skipped`. Otherwise all
are sent at once and each answers `ok`, `error`, or `unknown` (timed out or
lost: it may have taken effect). The written value shows in each cell from
the call until the device announces a value again, and each cell shows its
outcome.

### What a remount resets

Mounting other devices, a lag resync, or a reconnect gives a new snapshot,
and with it a new `Session`: traces and their ids, points, spectra,
windows, devices, and every GPU buffer keyed by a trace (WeakMaps). The
set of unchecked devices is one per session, and the Properties panels are
keyed by it, so the checkboxes, reads and outcomes go too. What survives is the page's: tabs, plots, each plot's
`View` (span, pause, cursors), y ranges (keyed by signal key) and colours
(by signal key). Paused plots read their windows again from the new hub.
The page reconnects 1 s after a socket closes on its own, at once after a
gap.

### Where layouts live, and how signals are named

Tabs are kept in this browser's localStorage under `tl-plot-tabs-v2`, as
`{tabs: [{id, name, plots: {id: {id, kind, mode, items: [{id, key}],
legend?}}, layout}], activeId}`, `layout` the split tree of `layout.js`
(`tabs.js`, `tabFromJson` reads it). File, Export writes the tabs to a
`twinleaf-plot-layouts` JSON file, and Import reads one. The docks'
widths are `tl-side-size` and `tl-right-size`, percentages, and their
panels are `tl-docks-v1`, `{left: [{id, open}], right: [...]}`. A plot names its signals by
`signalKey`: `stream.column` for the root device, and `route/stream.column`
for another, as `0/sine.sine`. Several devices mount at `/0`, `/1` and on
in the order of their URL and subtree (`routes.rs`, `layout_of`), so the
same devices give the same names whatever order they were picked in.

### How a plot becomes a monitor

`monitorOf(size, mode, n)` in `plot.js`: a plot narrower than 360 px,
shorter than 160 px, or whose lanes are shorter than 100 px is a monitor.
Its lanes are `lanesOf(mode, n)`, one per trace stacked and one otherwise,
`n` the plot's traces the hub has. A monitor shows only its lines, and its
header over them while hovered or focused.

## Building and checking

- `npm run build` in `twinleaf-web/ui` writes `ui/dist`, which a release
  build embeds and a debug build reads from disk on each request.
- A release build of a checkout without `ui/dist` builds it
  (`twinleaf-web/build.rs` runs `npm ci` and `npm run build`), so `cargo
  install --git` serves the page where Node and npm are installed. Without
  them the build warns and goes on, and `/` says how to build the page, as
  it does for a debug build without `ui/dist`. `TWINLEAF_WEB_NO_UI_BUILD`
  skips the page's build.
- `npm test` runs the page's vitest suite (`ui/test`).
- `cargo test -p twinleaf-web` and `cargo clippy -p twinleaf-web
  --all-targets`.

## Open questions

- Does `--listen all` serve the page, refuse it, or ask for a flag?
- Serving the page on `-p` needs a `Connection` constructor that takes the
  proxy's address.
- Decimation is per hub, shared by every page.
