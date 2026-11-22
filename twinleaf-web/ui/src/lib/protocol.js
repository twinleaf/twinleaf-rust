// What the page and the hub say to each other. The messages and the /api
// routes are defined in twinleaf-web/src/protocol.rs and the binary frames
// in twinleaf-web/src/wire.rs. What the hub sends is recorded in
// tests/fixtures/hub, which the page's tests read.

/** The bytes before a binary frame's body: u8 kind, 7 pad, u64 seq. */
const HEADER = 16;

/** What marks a capture's key among signal keys, ahead of its RPC's name. */
const CAPTURE = 'capture:';

/**
 * The name a plot keeps a capture by: its RPC's name after `capture:`, with
 * the device's route ahead as a trace's `name` has it, as `capture:fid.capture`
 * and `0/capture:fid.capture`. No signal's key reads so.
 */
export function captureKey({ route, rpc }) {
  const at = route && route !== '/' ? `${route.slice(1)}/` : '';
  return `${at}${CAPTURE}${rpc}`;
}

/** Whether `key` is a capture's (`captureKey`) and no signal's. */
export const isCaptureKey = (key) => key.slice(key.lastIndexOf('/') + 1).startsWith(CAPTURE);


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
 * number, type, calibration, x0, dx, name, units, xName, xUnits, elements,
 * spectrum}: `elements` the device's own, a typed array of their type over
 * the frame's bytes, element i being `elements[i] * calibration` in `units`
 * at `x0 + i * dx` in `xUnits`, and `spectrum` the hub's estimate of their
 * amplitude spectral density, {f, a}, f in cycles per x unit and a in
 * `units` per root cycle per x unit, empty where it has none. A frame of another kind, or a capture of an
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
    const start = Math.ceil(off / 8) * 8, elements = new Elements(buf, start, dv.getUint32(24, true));
    const bins = Math.ceil((start + elements.byteLength) / 8) * 8, m = dv.getUint32(bins, true);
    const spectrum = { f: new Float32Array(buf, bins + 8, m), a: new Float32Array(buf, bins + 8 + 4 * m, m) };
    return { kind: 'capture', seq, route, rpc, number: Number(dv.getBigUint64(16, true)), type, calibration: dv.getFloat32(32, true), x0: dv.getFloat32(36, true), dx: dv.getFloat32(40, true), name, units, xName, xUnits, elements, spectrum };
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
/** The source of an upgrade: the newest release, the release in `file`, or the file `name` of `size` bytes. */
export const imageSource = { latest: () => ({ kind: 'latest' }), release: (file) => ({ kind: 'release', file }), file: (name, size) => ({ kind: 'file', name, size }) };
/** The page's ask of the upgrade of the device at `route`: `type` 'cancel' to take it back or clear it, 'retry' to run a failed one again. */
export const upgradeAsk = (type, route) => JSON.stringify({ type, route });

/** The /api request of `path`: a GET with `query`, or a POST of `body` as JSON. */
const request = (path, { query, body } = {}) => ({
  method: body === undefined ? 'GET' : 'POST',
  url: query ? `${path}?${new URLSearchParams(query)}` : path,
  ...(body !== undefined && { body: JSON.stringify(body) }),
});

/**
 * Sends `request` and returns its reply, or throws the server's refusal as
 * an Error. Aborting `signal` ends the request, its reply unread, and
 * throws the signal's reason.
 */
async function send({ method, url, body }, signal) {
  const init = method === 'GET' ? { signal } : { method, headers: { 'content-type': 'application/json' }, body, signal };
  const r = await fetch(url, init);
  const reply = await r.json().catch(() => ({ error: `${r.status} ${r.statusText}` }));
  if (!r.ok) throw new Error(reply.error ?? `${r.status} ${r.statusText}`);
  return reply;
}

/** The request of each /api route, the one `protocol.rs` declares, as `api` sends it. */
export const requests = {
  /** Calls RPC `name` with `value`, as typed, on every device of `routes`. */
  call: (name, value, routes) => request('/api/call', { body: { name, value, routes } }),
  /** Reads `names` of the device at `route`. */
  read: (route, names) => request('/api/read', { body: { route, names } }),
  /** Looks for devices, on the network too when `network`. */
  discover: (network) => request('/api/discover', { query: { network } }),
  /** Opens the devices [{url, source}] in place of the hub's, or the proxy's default for none. */
  mount: (mounts) => request('/api/mount', { body: { mounts } }),
  /** Sets the decimation every page shares. */
  setPps: (pps) => request('/api/pps', { body: { pps } }),
  /** Keeps `seconds` of each stream's raw history, in at most `bytes` across every stream. */
  setHistory: (seconds, bytes) => request('/api/history', { body: { seconds, bytes: Math.round(bytes) } }),
  /** Reads trace `id` of hub `hub` between t0 and t1 for a plot `width` px wide. */
  window: ({ hub, id, t0, t1, width }) => request('/api/window', { query: { hub, id, t0, t1, width: Math.round(width) } }),
  /** Reads every raw row of trace `id` of hub `hub` between t0 and t1, or, past `limit` rows, only how many. */
  rows: ({ hub, id, t0, t1, limit }) => request('/api/rows', { query: { hub, id, t0, t1, limit } }),
};

/**
 * The /api routes. Each sends the request of `requests` its arguments
 * build and returns its reply. A window read ends as `signal` among its
 * arguments aborts it.
 */
export const api = Object.fromEntries(Object.entries(requests).map(([name, build]) => [name, (...args) => send(build(...args), args[0]?.signal)]));
