// What the page and the server say to each other. It mirrors
// `twinleaf-web/src/protocol.rs`, whose module doc is the definition, and
// the binary frames of `twinleaf-web/src/wire.rs`.
//
// The page holds one hub's state and comes to hold it one way, from a
// snapshot. The socket, /ws, sends a snapshot as it opens and whenever the
// page fell too far behind: `hub` {id, seq}, then `trace` for each trace,
// one points frame of history, `config`, and each device's newest `routes`,
// `device`, `table` and `settings`. The page replaces everything it held
// with it (`Hub.text`). Then come changes, each numbered `seq` one more than
// the last: points and spectrum frames, `trace` for a new column, device
// messages, and on a decimation change `clear`, the history and `config`.
// A message of a snapshot has no `seq`, and a binary frame of one has 0.
//
// A gap, a change numbered other than the next or a trace id the snapshot
// did not name, sends the page back for a snapshot: it closes the socket
// and connects again. So does the hub's retiring, which closes the socket.
//
// The page sends one message, `spectra`, naming the trace ids it shows as
// spectra, after each snapshot and whenever the set changes. The hub
// estimates only those.
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

/**
 * Decodes one binary frame, change `seq` or 0 in a snapshot: points, as
 * {kind: 'points', seq, chunks: [{id, level, brk, xs, ys}]}, where `level` is
 * the decimation the points are of, 0 the finest, and `brk` lifts the
 * pen before the chunk's first point, or the spectra of a round, as
 * {kind: 'spectra', seq, spectra: [{id, f, a, floor}]}, a floor null when
 * there is none.
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
  return null;
}

/** The page's message naming the trace ids it shows as spectra. */
export const spectraMessage = (ids) => JSON.stringify({ type: 'spectra', traces: ids });
/** Whether the page holds a plot paused, so the hub holds its history as it was then. */
export const holdMessage = (on) => JSON.stringify({ type: 'hold', on });

/** Sends one /api request and returns its reply, or throws the server's refusal as an Error. */
async function request(path, { query, body } = {}) {
  const url = query ? `${path}?${new URLSearchParams(query)}` : path;
  const init = body === undefined ? {} : { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) };
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
   * `width` px. Replies {t0, t1, ratio, chunks: [{x, y}]}, `ratio` raw rows per point.
   */
  window: ({ hub, id, t0, t1, width }) => request('/api/window', { query: { hub, id, t0, t1, width: Math.round(width) } }),
  /**
   * Reads every raw row of trace `id` of hub `hub` between t0 and t1, or,
   * past `limit` rows, only how many. Replies {count, oldest, x, y}, `oldest`
   * the oldest time the history still holds of the trace.
   */
  rows: ({ hub, id, t0, t1, limit }) => request('/api/rows', { query: { hub, id, t0, t1, limit } }),
};
