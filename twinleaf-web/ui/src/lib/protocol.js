// What the page and the server say to each other. It mirrors
// `twinleaf-web/src/protocol.rs`, whose module doc is the definition, and
// the binary frames of `twinleaf-web/src/wire.rs`.
//
// The page holds one hub's state and comes to hold it one way, from a
// snapshot. The socket, /ws, sends a snapshot as it opens and whenever the
// page fell too far behind: `hub` {id, seq, behind}, `behind` the changes
// it stands in place of, then `trace` for each trace,
// one points frame of history, `config`, and each device's newest `routes`,
// `device`, `table`, `settings` and `loss`, and the newest lines of the log,
// a `log` each. The page replaces everything it held
// with it (`Hub.text`). Then come changes, each numbered `seq` one more than
// the last: points and spectrum frames, `trace` for a new column, device
// messages, `log` for a line, and on a decimation change `clear`, the
// history and `config`.
// A message of a snapshot has no `seq`, and a binary frame of one has 0.
//
// `loss` {route, link: {times, seconds}, streams: {name: {skipped, link,
// proxy, hub}}} counts what was lost of one device before the hub held it,
// since the hub started: how often its link dropped and for how long, and
// the samples of each stream its device skipped, its link's outages lost,
// the proxy dropped and the hub dropped. The page's own counts are made of
// what each adds to the last (loss.svelte.js).
//
// `log` {n, wall, time, level, source, route, text, loss} is one line of
// the hub's log: `n` its number, from 1 since the hub started; `wall` the
// host's time of day as the hub took it, in Unix seconds; `time` the time
// of the newest sample the hub had taken of its device then, on the clock
// that device's points are on, or null; `level` error, warn, info or debug;
// `source` device, for a line the device at `route` said, or proxy, for
// one the proxy says of the device at `route`, or of none with null, with
// `part` the part of the proxy that said it, `hub` for the hub's own and
// else the source the proxy's console names; and
// `loss`, on a line of a loss alone, what was lost: {cause: 'link',
// seconds, why, url}, `seconds` null as the link drops and how long it was
// down as it is back, `why` on the drop the reason the proxy lost the
// device (disconnected, stopped, failed_to_connect, failed_to_reconnect or
// quiet) and `url` the link's address, each where the proxy said it,
// or {cause, stream, samples} with `cause` skipped, proxy or hub. A line
// may come in a snapshot and again as a change; the page keeps it once, by
// `n` (log.svelte.js).
//
// A gap, a change numbered other than the next or a trace id the snapshot
// did not name, sends the page back for a snapshot: it closes the socket
// and connects again. So does the hub's retiring, which closes the socket.
//
// The page sends `spectra`, naming the trace ids it shows as spectra, after
// each snapshot and whenever the set changes: the hub estimates only those.
// It sends `hold` while it holds a plot paused, and of each capture RPC its
// plots show, `capture` for one capture now, `repeat` for captures again
// and again, and `stop` to take both back. Each device's `captures` lists
// its capture RPCs, each one's `capture` says where it stands, and a
// capture the hub read for this page comes as a capture frame, which is no
// numbered change.
//
// Each device's `firmware` {route, installed, catalog} says what it runs,
// {name, revision, date, build, serial, development}, and what the catalog
// of published firmware offers it: {state} unchecked, checking, unreachable
// (with `error`) or checked, with `status` (update, current, none,
// development, unknown) and `releases` [{date, build, file}], newest first.
// The hub asks the catalog only when a page sends `check`. A device's
// `upgrade` {route, state} says where an upgrade of it stands: idle, or
// with `source` and `target`, queued, downloading, uploading (`sent` of
// `size` bytes), finalizing, restarting, done (with `installed`) or failed
// (with `step` and `error`). The page sends `upgrade` {route, source} to
// start one, the newest release, a release by its file's name, or a file,
// whose bytes follow in binary frames; `cancel` to take one back before it
// uploads, or clear one that ended; and `retry` for one that failed. An
// ask the hub refuses is answered `refused` {ask, route, error}, to this
// page alone, which is no numbered change.
//
// Every /api request answers JSON. One the server cannot serve answers a
// status other than 2xx with {error}, which `request` throws as an Error.

/** The bytes before a binary frame's body: u8 kind, 7 pad, u64 seq. */
const HEADER = 16;

/**
 * The name a plot keeps a signal by, and which saved layouts hold: the
 * stream and column, as `sine.sine`, with the device's route ahead for any
 * device but the root, as `0/sine.sine`. Routes follow the mount order the
 * server documents in `routes.rs` (`layout_of`), so the same devices give
 * the same names.
 */
export function signalKey(trace) {
  const at = trace.route && trace.route !== '/' ? `${trace.route.slice(1)}/` : '';
  return `${at}${trace.stream}.${trace.column}`;
}

/** What marks a capture's key among signal keys, ahead of its RPC's name. */
const CAPTURE = 'capture:';

/**
 * The name a plot keeps a capture by: its RPC's name after `capture:`, with
 * the device's route ahead as `signalKey` puts it, as `capture:fid.capture`
 * and `0/capture:fid.capture`. No signal's key reads so.
 */
export function captureKey({ route, rpc }) {
  const at = route && route !== '/' ? `${route.slice(1)}/` : '';
  return `${at}${CAPTURE}${rpc}`;
}

/** Whether `key` is a capture's (`captureKey`) and no signal's. */
export const isCaptureKey = (key) => key.slice(key.lastIndexOf('/') + 1).startsWith(CAPTURE);

/** What a capture's `error` says once the hub has lost its device, as it drops every page's ask of it (twinleaf-web/src/captures.rs `gone`). */
export const DEVICE_GONE = 'the device is gone';

/** The typed array of each element type a capture frame carries, by the protocol's code. */
const ELEMENTS = { 0x10: Uint8Array, 0x11: Int8Array, 0x20: Uint16Array, 0x21: Int16Array, 0x40: Uint32Array, 0x41: Int32Array, 0x42: Float32Array, 0x80: BigUint64Array, 0x81: BigInt64Array, 0x82: Float64Array };
/** The name of each element type a capture frame carries, by the protocol's code, as the hub's wire format names them (twinleaf-web/src/wire.rs). */
export const ELEMENT_NAMES = { 0x10: 'u8', 0x11: 'i8', 0x20: 'u16', 0x21: 'i16', 0x40: 'u32', 0x41: 'i32', 0x42: 'f32', 0x80: 'u64', 0x81: 'i64', 0x82: 'f64' };
/** The bytes of a capture frame before its strings. */
const CAPTURE_HEAD = HEADER + 48;

/**
 * Decodes one binary frame, change `seq` or 0 in a snapshot: points, as
 * {kind: 'points', seq, chunks: [{id, level, brk, xs, ys}]}, where `level` is
 * the decimation the points are of, 0 the finest, and `brk` lifts the
 * pen before the chunk's first point; the spectra of a round, as
 * {kind: 'spectra', seq, spectra: [{id, f, a, floor}]}, a floor null when
 * there is none; or a capture, as {kind: 'capture', seq: 0, route, rpc,
 * number, type, calibration, x0, dx, name, units, xName, xUnits, elements}:
 * `elements` the device's own, a typed array of their type over the
 * frame's bytes, element i being `elements[i] * calibration` in `units` at
 * `x0 + i * dx` in `xUnits`. A frame of another kind, or a capture of an
 * element type the page does not read, is null.
 */
export function decodeFrame(buf) {
  const dv = new DataView(buf);
  const kind = dv.getUint8(0);
  const seq = Number(dv.getBigUint64(8, true));
  if (kind === 1) {
    const count = dv.getUint16(HEADER, true);
    const chunks = [];
    let off = HEADER + 2;
    for (let c = 0; c < count; c++) {
      const id = dv.getUint16(off, true);
      const brk = dv.getUint8(off + 2) === 1;
      const level = dv.getUint8(off + 3);
      const n = dv.getUint32(off + 4, true);
      off += 8;
      const xs = new Float64Array(n), ys = new Float32Array(n);
      for (let i = 0; i < n; i++) xs[i] = dv.getFloat64(off + i * 8, true);
      off += n * 8;
      for (let i = 0; i < n; i++) ys[i] = dv.getFloat32(off + i * 4, true);
      off += n * 4;
      chunks.push({ id, level, brk, xs, ys });
    }
    return { kind: 'points', seq, chunks };
  }
  if (kind === 2) {
    const count = dv.getUint16(HEADER, true);
    const spectra = [];
    let off = HEADER + 8;
    for (let s = 0; s < count; s++) {
      const id = dv.getUint16(off, true), n = dv.getUint32(off + 4, true), floor = dv.getFloat64(off + 8, true);
      off += 16;
      const f = new Float32Array(n), a = new Float32Array(n);
      for (let i = 0; i < n; i++) f[i] = dv.getFloat32(off + i * 4, true);
      for (let i = 0; i < n; i++) a[i] = dv.getFloat32(off + (n + i) * 4, true);
      off += n * 8;
      spectra.push({ id, f, a, floor: isNaN(floor) ? null : floor });
    }
    return { kind: 'spectra', seq, spectra };
  }
  if (kind === 3) {
    const type = dv.getUint8(28), Elements = ELEMENTS[type];
    if (!Elements) return null;
    const utf8 = new TextDecoder();
    let off = CAPTURE_HEAD;
    const [route, rpc, name, units, xName, xUnits] = Array.from({ length: 6 }, (_, i) => {
      const length = dv.getUint16(48 + 2 * i, true), text = utf8.decode(new Uint8Array(buf, off, length));
      off += length;
      return text;
    });
    const elements = new Elements(buf, Math.ceil(off / 8) * 8, dv.getUint32(24, true));
    return { kind: 'capture', seq, route, rpc, number: Number(dv.getBigUint64(16, true)), type, calibration: dv.getFloat32(32, true), x0: dv.getFloat32(36, true), dx: dv.getFloat32(40, true), name, units, xName, xUnits, elements };
  }
  return null;
}

/** The page's message naming the trace ids it shows as spectra. */
export const spectraMessage = (ids) => JSON.stringify({ type: 'spectra', traces: ids });
/** Whether the page holds a plot paused, so the hub holds its history as it was then. */
export const holdMessage = (on) => JSON.stringify({ type: 'hold', on });
/**
 * The page's ask of capture RPC `rpc` of the device at `route`: `type`
 * 'capture' for one capture now, 'repeat' for captures again and again
 * `delay` seconds after each, or 'stop' to take both back.
 */
export const captureMessage = (type, route, rpc, delay) => JSON.stringify({ type, route, rpc, ...(type === 'repeat' && { delay }) });

/** The page's ask that the hub look each device up in the catalog of published firmware. */
export const checkMessage = () => JSON.stringify({ type: 'check' });
/**
 * The page's ask to upgrade the device at `route` from `source`: {kind:
 * 'latest'}, {kind: 'release', file}, or {kind: 'file', name, size}, whose
 * `size` bytes follow in binary frames with no other message between.
 */
export const upgradeMessage = (route, source) => JSON.stringify({ type: 'upgrade', route, source });
/** The page's ask of the upgrade of the device at `route`: `type` 'cancel' to take it back or clear it, 'retry' to run a failed one again. */
export const upgradeAsk = (type, route) => JSON.stringify({ type, route });

/**
 * Sends one /api request and returns its reply, or throws the server's
 * refusal as an Error. Aborting `signal` ends the request, its reply
 * unread, and throws the signal's reason.
 */
async function request(path, { query, body, signal } = {}) {
  const url = query ? `${path}?${new URLSearchParams(query)}` : path;
  const init = body === undefined ? { signal } : { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body), signal };
  const r = await fetch(url, init);
  const reply = await r.json().catch(() => ({ error: `${r.status} ${r.statusText}` }));
  if (!r.ok) throw new Error(reply.error ?? `${r.status} ${r.statusText}`);
  return reply;
}

/**
 * The /api routes. Each takes the request `protocol.rs` declares and
 * returns its reply.
 */
export const api = {
  /**
   * Calls RPC `name` with `value`, as typed, on every device of `routes`.
   * Replies {sent, results}, one outcome per route in order: {state, route}
   * with `state` ok (and `value`), error, unknown or refused (and `error`),
   * or skipped, which another device's refusal left unsent.
   */
  call: (name, value, routes) => request('/api/call', { body: { name, value, routes } }),
  /** Reads `names` of the device at `route`. Replies {values: {name: outcome}}. */
  read: (route, names) => request('/api/read', { body: { route, names } }),
  /** Looks for devices for a few seconds. Replies {devices: [{url, interface, name, routes}], notices}. */
  discover: (network) => request('/api/discover', { query: { network } }),
  /** Opens the devices [{url, source}] in place of the hub's, or the proxy's default for none. */
  mount: (mounts) => request('/api/mount', { body: { mounts } }),
  /** Sets the decimation every page shares. Replies {pps} as the server clamped it. */
  setPps: (pps) => request('/api/pps', { body: { pps } }),
  /** Keeps `seconds` of each stream's raw history, in at most `bytes` across every stream. */
  setHistory: (seconds, bytes) => request('/api/history', { body: { seconds, bytes: Math.round(bytes) } }),
  /**
   * Reads trace `id` of hub `hub` between t0 and t1, decimated to about
   * `width` px, until `signal` aborts the read. Replies {t0, t1, ratio,
   * chunks: [{x, y}]}, `ratio` raw rows per point.
   */
  window: ({ hub, id, t0, t1, width, signal }) => request('/api/window', { query: { hub, id, t0, t1, width: Math.round(width) }, signal }),
  /**
   * Reads every raw row of trace `id` of hub `hub` between t0 and t1, or,
   * past `limit` rows, only how many. Replies {count, oldest, x, y}, `oldest`
   * the oldest time the history still holds of the trace.
   */
  rows: ({ hub, id, t0, t1, limit }) => request('/api/rows', { query: { hub, id, t0, t1, limit } }),
};
