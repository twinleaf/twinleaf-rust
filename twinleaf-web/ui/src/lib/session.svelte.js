// One hub's state on the page: its traces and their points, spectra and
// full-resolution windows, its devices, and the clock that maps page time
// onto its device time. A `hub` message starts a new Session and the old one
// is dropped whole, so nothing of one hub, its trace ids above all, outlives
// it. What is keyed by a Trace object (GPU buffers, a plot's paused spectra)
// goes with it.
//
// Points are not reactive: the plots read them every frame. What the page's
// components show is: `signals`, `devices`, `mounts`, `pps` and `detailsDropped`.

import { SvelteMap } from 'svelte/reactivity';
import { api, signalKey } from './protocol.js';

/** How long a trace keeps its points, in seconds of device time. */
const RETAIN_SECONDS = 120;

/**
 * One trace's points, in device seconds. Points have absolute indices that
 * never change, so a GPU ring buffer can find the ones it has not uploaded.
 */
export class Trace {
  constructor(meta) {
    this.meta = meta;
    this.key = signalKey(meta);
    this.cap = 4096;
    this.x = new Float64Array(this.cap);
    this.y = new Float32Array(this.cap);
    this.start = 0; // local indices of the retained points
    this.end = 0;
    this.shift = 0; // absolute index of local index 0
    this.breaks = []; // absolute indices the pen lifts before, ascending
    this.spectrum = null; // the newest {f, a, floor}
  }

  get length() {
    return this.end - this.start;
  }
  get absStart() {
    return this.start + this.shift;
  }
  get absEnd() {
    return this.end + this.shift;
  }

  reserve(more) {
    if (this.end + more <= this.cap) return;
    const live = this.length;
    let cap = this.cap;
    while (live + more > cap * 0.75) cap *= 2;
    const x = new Float64Array(cap), y = new Float32Array(cap);
    x.set(this.x.subarray(this.start, this.end));
    y.set(this.y.subarray(this.start, this.end));
    this.shift += this.start;
    Object.assign(this, { x, y, cap, start: 0, end: live });
  }

  /**
   * Appends points, the pen lifted before the first with `breakFirst`, and
   * drops those older than RETAIN_SECONDS before the newest, but none from
   * `heldFrom` on.
   */
  push(xs, ys, breakFirst, heldFrom = Infinity) {
    if (!xs.length) return;
    this.reserve(xs.length);
    for (let i = 0; i < xs.length; i++) {
      // A point earlier than the last one also lifts the pen, so a line
      // never runs backwards in time.
      const back = this.end > this.start && xs[i] < this.x[this.end - 1];
      if (((i === 0 && breakFirst) || back) && this.end > this.start) this.breaks.push(this.absEnd);
      this.x[this.end] = xs[i];
      this.y[this.end] = ys[i];
      this.end++;
    }
    const keepFrom = Math.min(this.x[this.end - 1] - RETAIN_SECONDS, heldFrom);
    while (this.start < this.end && this.x[this.start] < keepFrom) this.start++;
    while (this.breaks.length && this.breaks[0] <= this.absStart) this.breaks.shift();
  }

  /** Drops every point; the next one starts a new line. */
  clear() {
    this.start = this.end;
    this.breaks = [];
  }

  /** First absolute index whose x is >= t. */
  lowerBound(t) {
    let lo = this.start, hi = this.end;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this.x[mid] < t) lo = mid + 1;
      else hi = mid;
    }
    return lo + this.shift;
  }

  xAt(abs) {
    return this.x[abs - this.shift];
  }
  yAt(abs) {
    return this.y[abs - this.shift];
  }

  /** The y extent of absolute points [a0, a1). */
  extent(a0, a1) {
    let lo = Infinity, hi = -Infinity;
    for (let i = a0 - this.shift, e = a1 - this.shift; i < e; i++) {
      const v = this.y[i];
      if (v < lo) lo = v;
      if (v > hi) hi = v;
    }
    return [lo, hi];
  }
}

/**
 * A window of one trace at full resolution, or lightly decimated, read from
 * the server's raw buffer. It reads as a Trace does, so plots draw and read
 * cursors from either.
 */
export class Detail {
  constructor(trace, reply) {
    this.trace = trace;
    this.meta = trace.meta;
    this.key = trace.key;
    this.t0 = reply.t0;
    this.t1 = reply.t1;
    this.ratio = reply.ratio;
    const n = reply.chunks.reduce((a, c) => a + c.x.length, 0);
    this.x = new Float64Array(n);
    this.y = new Float32Array(n);
    this.breaks = [];
    let k = 0;
    reply.chunks.forEach((c, i) => {
      if (i > 0) this.breaks.push(k);
      this.x.set(c.x, k);
      this.y.set(c.y, k);
      k += c.x.length;
    });
    this.shift = 0;
    this.start = 0;
    this.end = n;
  }
  get length() {
    return this.end;
  }
  get absStart() {
    return 0;
  }
  get absEnd() {
    return this.end;
  }
  /**
   * Whether the window holds raw rows across the whole view. It may not
   * start at the view's left edge when the view reaches back past the oldest
   * row the server's Buffer keeps, or past the start of the stream's run (a
   * reboot, a new session). It does not reach the right edge of a view past
   * the newest samples it was read at, where the live stream goes on.
   */
  covers(x0, x1) {
    if (!(this.t0 <= x0 && this.t1 >= x1) || this.end === 0) return false;
    const period = 1 / this.meta.rate;
    return this.x[0] <= x0 + 2 * period && this.x[this.end - 1] >= x1 - 2 * period;
  }
}
Detail.prototype.lowerBound = Trace.prototype.lowerBound;
Detail.prototype.xAt = Trace.prototype.xAt;
Detail.prototype.yAt = Trace.prototype.yAt;
Detail.prototype.extent = Trace.prototype.extent;

/**
 * One device as the hub's connection sees it. Each field is its own state,
 * and each announced value its own entry of `values`, so a change to one
 * device, or to one of its values, redraws only what shows it.
 */
export class Device {
  name = $state(null);
  serial = $state(null);
  firmware = $state(null);
  /** connecting, alive or lost. */
  presence = $state('connecting');
  /** {rpcs} once its RPC table loaded, {error, rpcs: []} if it did not, or null before. */
  table = $state.raw(null);
  /** The values it last announced, by RPC name. */
  values = new SvelteMap();

  constructor(route) {
    this.route = route;
  }

  /** Takes the values of a `settings` message, changing only those that differ. */
  announce(values) {
    for (const name of [...this.values.keys()]) if (!(name in values)) this.values.delete(name);
    for (const [name, value] of Object.entries(values)) {
      if (!this.values.has(name) || !sameValue(this.values.get(name), value)) this.values.set(name, value);
    }
  }
}

/** Whether two values of a device are the same. */
export const sameValue = (a, b) => a === b || JSON.stringify(a) === JSON.stringify(b);

export class Session {
  /** The traces, sorted by signal key: what the signal lists show. */
  signals = $state.raw([]);
  /** Each Device by route, in the order the hub listed them. */
  devices = new SvelteMap();
  /** The devices the hub mounted, [{url, source, prefix}], or null for the proxy's default. */
  mounts = $state.raw(null);
  /** The decimation, in points per second per trace. */
  pps = $state(0);
  /** What the hub's buffer holds, {bytes, capacity, seconds}, or null before it says. */
  memory = $state.raw(null);

  traces = new Map(); // trace id -> Trace
  byKey = new Map(); // signal key -> Trace
  /** Each paused plot's full-resolution windows: plot id -> (Trace -> Detail). */
  details = new Map();
  /** How many times every window was dropped, for the page to read them again. */
  detailsDropped = $state(0);
  detailTokens = new Map(); // plot id -> its newest window request
  t0 = null; // device time of the first point, where the time axis counts from
  latest = -Infinity; // device time of the newest point
  offset = null; // device time minus page time, in seconds
  /** The device time from which no trace drops a point, for paused views. */
  heldFrom = Infinity;

  /** `id` names the hub, null before the socket reached one. */
  constructor(id = null, { heldFrom = Infinity } = {}) {
    this.id = id;
    this.heldFrom = heldFrom;
  }

  /** The trace of signal `key`, read reactively: a component asks again as traces arrive. */
  trace(key) {
    this.signals;
    return this.byKey.get(key);
  }

  /** Device time that page time `perfMs` corresponds to, smoothed. */
  now(perfMs) {
    return this.offset === null ? this.latest : perfMs / 1000 + this.offset;
  }

  /** Moves the clock toward the newest point, once per frame. */
  tick(perfMs) {
    if (!isFinite(this.latest)) return;
    const target = this.latest - perfMs / 1000;
    if (this.offset === null || Math.abs(target - this.offset) > 0.5) this.offset = target;
    else this.offset += (target - this.offset) * (target > this.offset ? 0.05 : 0.01);
  }

  /** The device at `route`, made the first time the routes list it. */
  device(route) {
    let d = this.devices.get(route);
    if (!d) this.devices.set(route, (d = new Device(route)));
    return d;
  }

  /** Applies one text message other than `hub`. */
  text(msg) {
    switch (msg.type) {
      case 'trace': {
        if (this.traces.has(msg.trace.id)) return;
        const t = new Trace(msg.trace);
        this.traces.set(t.meta.id, t);
        this.byKey.set(t.key, t);
        this.signals = [...this.byKey.values()].sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
        return;
      }
      case 'clear':
        // The server re-decimated its history; a fresh copy follows.
        for (const t of this.traces.values()) t.clear();
        this.details.clear();
        this.detailsDropped++;
        return;
      case 'config':
        this.pps = msg.pps;
        return;
      case 'memory':
        this.memory = { bytes: msg.bytes, capacity: msg.capacity, seconds: msg.seconds };
        return;
      case 'routes':
        this.mounts = msg.mounts;
        for (const route of [...this.devices.keys()]) if (!msg.routes.includes(route)) this.devices.delete(route);
        for (const route of msg.routes) this.device(route);
        return;
      // The server sends a device's messages only while the routes list it.
      case 'device':
        return void Object.assign(this.devices.get(msg.route) ?? {}, { name: msg.name, serial: msg.serial, firmware: msg.firmware, presence: msg.presence });
      case 'table': {
        const d = this.devices.get(msg.route);
        if (d) d.table = msg.error ? { error: msg.error, rpcs: [] } : { rpcs: msg.rpcs };
        return;
      }
      case 'settings':
        return this.devices.get(msg.route)?.announce(msg.values);
    }
  }

  /**
   * Applies one decoded binary frame. Returns false when it names a trace
   * this session never heard of, a gap the page fills with a new snapshot.
   */
  apply(frame) {
    if (frame.kind === 'spectra') {
      for (const { id, f, a, floor } of frame.spectra) {
        const t = this.traces.get(id);
        if (!t) return false;
        t.spectrum = { f, a, floor };
      }
      return true;
    }
    for (const { id, brk, xs, ys } of frame.chunks) {
      const trace = this.traces.get(id);
      if (!trace) return false;
      if (xs.length === 0) continue;
      if (this.t0 === null) this.t0 = xs[0];
      trace.push(xs, ys, brk, this.heldFrom);
      if (xs[xs.length - 1] > this.latest) this.latest = xs[xs.length - 1];
    }
    return true;
  }

  /** The window of `trace` plot `plotId` last read, if any. */
  detailOf(plotId, trace) {
    return this.details.get(plotId)?.get(trace);
  }

  /** Reads [t0, t1] of `traces` at full resolution, about `width` px wide, for the paused plot `plotId`. */
  async fetchDetail(plotId, traces, t0, t1, width) {
    const token = (this.detailTokens.get(plotId) ?? 0) + 1;
    this.detailTokens.set(plotId, token);
    const replies = await Promise.all(
      traces.map((t) => api.window({ hub: this.id, id: t.meta.id, t0, t1, width }).then((reply) => [t, reply], () => null)),
    );
    if (token !== this.detailTokens.get(plotId)) return; // a newer request superseded this one
    let windows = this.details.get(plotId);
    if (!windows) this.details.set(plotId, (windows = new Map()));
    for (const got of replies) if (got) windows.set(got[0], new Detail(got[0], got[1]));
  }

  /** Drops the plot's windows, as its view streams again. */
  clearDetail(plotId) {
    this.details.delete(plotId);
    this.detailTokens.set(plotId, (this.detailTokens.get(plotId) ?? 0) + 1);
  }
}
