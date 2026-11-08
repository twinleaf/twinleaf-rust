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
| `wire` | the binary frames: points and spectra |
| `feed` | the channel of changes, which numbers each one |
| `hub` | `Live` (one connection to the proxy, its `Hub` of traces, its devices) and how it ends |
| `devices` | each device's newest identity, RPC table and settings; calls and reads |
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
   changes behind (`Lagged`): `hub {id, seq}`, a `trace` per trace, one
   points frame of all history sent so far, `config`, `memory`, `routes`, then each
   listed device's newest `device`, `table` and `settings`. The page drops
   everything of any hub and builds a new `Session` from it.
2. **Changes**, each numbered `seq` one more than the last (`feed.rs`):
   points frames (every 16 ms), one spectra frame per round (every 100 ms),
   `trace` for a new column, device messages, `memory` when what the buffer
   holds or may hold changed (checked each second: `bytes` of `budget`, the
   `seconds` held of `max_age`, and the bytes per second `rate`), and on a
   decimation change `clear`, the history again and `config`.
3. **Gaps** go back through 1: a change numbered other than the next, or a
   frame naming a trace the session does not have, makes the page close its
   socket and open another (`Hub.resync`). A retired hub closes the socket.

Devices are latest-value state, as the proxy keeps them: the hub keeps the
newest of each kind, sends a message only when it changes (a heartbeat
sends nothing), and sends a device's messages only while `routes` lists it
(a device listed again sends its newest again). Only points and spectra
stream.

The page sends one message, `spectra {traces}`, naming the trace ids its
open tab shows as spectra, after each snapshot and when that set changes.
The hub estimates and sends only the spectra some page names; a page's
names go when its socket does.

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
| `POST /api/mount` | opens devices in place of the hub's |
| `POST /api/pps` | the decimation every page shares |
| `POST /api/history` | the raw history the hub keeps: `{seconds, bytes}`, a span of each stream's time and a budget across every stream |
| `GET /api/window` | raw rows of one trace of a named hub, for a paused plot |

## The page (`twinleaf-web/ui/src`)

Svelte 5 and shadcn-svelte components (`lib/components/ui`, generated by
the shadcn-svelte CLI, style "nova"). Custom drawing is only the plot
canvas and its WebGL layer.

| File | What it is |
|---|---|
| `App.svelte` | the shell: toolbar, the resizable left and right docks, tab strip, tiles, dialogs |
| `lib/docks.svelte.js`, `Dock.svelte` | the docks' panels: where each is, folded or open, dragged by its header or moved by its menu |
| `lib/hub.svelte.js` | the socket, the current `Session`, sequence checks, spectra subscription |
| `lib/session.svelte.js` | one hub's state: `Trace`s, `Detail` windows, `Device`s, the clock |
| `lib/protocol.js` | the protocol mirror, `signalKey`, `api` |
| `lib/tabs.js` | what a tab is, the default tab, saving, export and import |
| `lib/workspace.svelte.js` | the tabs as the page edits them, focus, selection |
| `lib/views.js` | each time plot's `View`, pruned with its plot |
| `lib/plot.js` | `PlotView` (one plot's drawing and gestures), `View`, `lanesOf`, `monitorOf` |
| `lib/gl.js` | the WebGL renderer behind every plot |
| `lib/layout.js` | the tiling tree |
| `lib/drag.svelte.js` | moving plots, dropping signals, the selection box |
| `lib/keymap.js` | every key, which drives the handlers and the keys dialog |
| `lib/properties.js`, `Properties.svelte` | the Properties data table (TanStack table-core) |
| `StatusBar.svelte` | the page's foot: connection, devices and lost ones, data rate, the hub's raw history (`memory`), frame rate, the keys |
| `PlotCard`, `Devices`, `DevicePicker`, `SignalList`, `TabStrip`, `CommandPalette`, `KeysDialog`, `DragOverlay`, `Tile` | the components |

### A sample, to a pixel

1. The proxy delivers a `SampleBatch` to the hub's connection. The hub's
   thread puts it in the `Buffer` of raw rows (10 min of each stream in at
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
5. Paused, a plot asks `/api/window` (with the hub id) for the raw rows of
   its view, and draws those where they cover it. A spectrum plot draws the
   newest spectrum, or the one it showed when it paused.

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
  build embeds and a debug build reads from disk on each request. A
  checkout without `ui/dist` still builds; `/` then says how to build it.
- `npm test` runs the page's vitest suite (`ui/test`).
- `cargo test -p twinleaf-web` and `cargo clippy -p twinleaf-web
  --all-targets`.

## Open questions

- Does `--listen all` serve the page, refuse it, or ask for a flag?
- Serving the page on `-p` needs a `Connection` constructor that takes the
  proxy's address.
- Decimation is per hub, shared by every page.
