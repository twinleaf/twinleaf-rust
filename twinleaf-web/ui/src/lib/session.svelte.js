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
import { colors } from './colors.svelte.js';

/** How long a trace keeps its finest points, in seconds of device time, until the hub says. */
const RETAIN_SECONDS = 120;
/** How far back, in ms of page time, the clock looks for the batch that arrived least delayed. */
const CLOCK_WINDOW_MS = 2000;
/**
 * How fast the clock moves toward its goal, in seconds a second: the plots
 * scroll at most this share faster or slower than real time.
 */
const CLOCK_SLEW = 0.005;
/** How far, in seconds, the clock may be from its goal before it is set there at once. */
const CLOCK_SNAP = 0.5;

/**
 * One line of points, in device seconds, kept for `keep` seconds before the
 * newest. Points have absolute indices that never change, so a GPU ring
 * buffer can find the ones it has not uploaded.
 */
export class Points {
  constructor(keep = RETAIN_SECONDS) {
    this.keep = keep;
    this.cap = 4096;
    this.x = new Float64Array(this.cap);
    this.y = new Float32Array(this.cap);
    this.start = 0; // local indices of the retained points
    this.end = 0;
    this.shift = 0; // absolute index of local index 0
    this.breaks = []; // absolute indices the pen lifts before, ascending
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
   * drops those older than `keep` before the newest. A paused plot reads
   * older ones from the hub's history.
   */
  push(xs, ys, breakFirst) {
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
    const keepFrom = this.x[this.end - 1] - this.keep;
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
 * One trace: its points at each level of the hub's decimation, finest
 * first, and its newest spectrum. The trace is its own finest level, so what
 * reads only the newest points reads the trace.
 */
export class Trace extends Points {
  constructor(meta) {
    super();
    this.meta = meta;
    this.key = signalKey(meta);
    this.levels = [this];
    this.spectrum = null; // the newest {f, a, floor}
  }

  /** Level `k`, keeping `keep` seconds, with every finer level made first. */
  level(k, keep = RETAIN_SECONDS * 4 ** k) {
    while (this.levels.length <= k) this.levels.push(new Points(RETAIN_SECONDS * 4 ** this.levels.length));
    this.levels[k].keep = keep;
    return this.levels[k];
  }

  /** Drops every point of every level; the next one starts a new line. */
  clear() {
    super.clear();
    for (const level of this.levels.slice(1)) level.clear();
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
  /** How many times its presence turned alive: one more each time it reconnects. */
  connections = $state(0);
  /** {rpcs} once its RPC table loaded, {error, rpcs: []} if it did not, or null before. */
  table = $state.raw(null);
  /** The values it last announced, by RPC name. */
  values = new SvelteMap();

  constructor(route) {
    this.route = route;
  }

  /** How many times each announced value changed, by RPC name, so a value set back to what it was is still a change. */
  #changes = new Map();

  /** Takes the values of a `settings` message, changing only those that differ. */
  announce(values) {
    for (const name of [...this.values.keys()]) if (!(name in values)) this.#change(name, () => this.values.delete(name));
    for (const [name, value] of Object.entries(values)) {
      if (!this.values.has(name) || !sameValue(this.values.get(name), value)) this.#change(name, () => this.values.set(name, value));
    }
  }

  /** How many times the announced value of `name` changed: it read with the value, which changes with it. */
  changes(name) {
    return this.#changes.get(name) ?? 0;
  }

  #change(name, apply) {
    this.#changes.set(name, this.changes(name) + 1);
    apply();
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
  /** The decimation, in points per second per trace of the finest level. */
  pps = $state(0);
  /** The seconds each level of the decimation keeps, finest first. */
  levels = $state.raw([]);
  /** The hub's raw history, {bytes, budget, seconds, maxAge, rate}, or null before it says. */
  memory = $state.raw(null);
  /** The oldest time the hub held of each trace when the session first heard, by trace id: an oldest later than it since is a drop. */
  firstHeld = {};

  traces = new Map(); // trace id -> Trace
  byKey = new Map(); // signal key -> Trace
  /** Each paused plot's full-resolution windows: plot id -> (Trace -> Detail). */
  windows = new Map(); // plot id -> {detail, overview}: each a Map, trace -> Detail
  /** How many times every window was dropped, for the page to read them again. */
  detailsDropped = $state(0);
  windowTokens = new Map(); // plot id -> {asked, shown}: the newest request of each layer, and the one drawn
  latest = -Infinity; // device time of the newest point
  offset = null; // device time minus page time, in seconds
  arrivals = []; // [page ms, device time minus page time in s] of each batch that moved `latest`, the last CLOCK_WINDOW_MS
  tickedAt = null; // page ms of the last tick
  /** `id` names the hub, null before the socket reached one. */
  constructor(id = null) {
    this.id = id;
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

  /**
   * Moves the clock toward its goal, once per frame. The goal is the most
   * device time any batch of the last CLOCK_WINDOW_MS was ahead of page
   * time as it arrived: a batch can arrive late but never early, so that
   * one was least delayed. The clock runs at the page's rate and moves
   * toward the goal no faster than CLOCK_SLEW, whatever the frame rate, so
   * the plots scroll evenly however the batches arrive.
   */
  tick(perfMs) {
    if (!this.arrivals.length) return;
    this.forgetArrivals(perfMs);
    const goal = Math.max(...this.arrivals.map((a) => a[1]));
    const dt = this.tickedAt === null ? 0 : Math.max(0, perfMs - this.tickedAt) / 1000;
    this.tickedAt = perfMs;
    if (this.offset === null || Math.abs(goal - this.offset) > CLOCK_SNAP) this.offset = goal;
    else this.offset += Math.max(-CLOCK_SLEW * dt, Math.min(CLOCK_SLEW * dt, goal - this.offset));
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
        colors.give(this.signals.map((s) => s.key));
        return;
      }
      case 'clear':
        // The server re-decimated its history; a fresh copy follows.
        for (const t of this.traces.values()) t.clear();
        this.windows.clear();
        this.detailsDropped++;
        return;
      case 'config':
        this.pps = msg.pps;
        this.levels = msg.levels ?? [];
        return;
      case 'memory':
        this.memory = { bytes: msg.bytes, budget: msg.budget, seconds: msg.seconds, maxAge: msg.max_age, rate: msg.rate, held: msg.held ?? null, oldest: msg.oldest ?? {} };
        for (const [id, t] of Object.entries(this.memory.oldest)) this.firstHeld[id] ??= t;
        return;
      case 'routes':
        this.mounts = msg.mounts;
        for (const route of [...this.devices.keys()]) if (!msg.routes.includes(route)) this.devices.delete(route);
        for (const route of msg.routes) this.device(route);
        return;
      // The server sends a device's messages only while the routes list it.
      case 'device': {
        const d = this.devices.get(msg.route);
        if (!d) return;
        if (msg.presence === 'alive' && d.presence !== 'alive') d.connections++;
        return void Object.assign(d, { name: msg.name, serial: msg.serial, firmware: msg.firmware, presence: msg.presence });
      }
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
   * `perfMs` is the page time it arrived.
   */
  apply(frame, perfMs = performance.now()) {
    if (frame.kind === 'spectra') {
      for (const { id, f, a, floor } of frame.spectra) {
        const t = this.traces.get(id);
        if (!t) return false;
        t.spectrum = { f, a, floor };
      }
      return true;
    }
    const was = this.latest;
    for (const { id, level, brk, xs, ys } of frame.chunks) {
      const trace = this.traces.get(id);
      if (!trace) return false;
      if (xs.length === 0) continue;
      trace.level(level, this.levels[level]).push(xs, ys, brk);
      if (xs[xs.length - 1] > this.latest) this.latest = xs[xs.length - 1];
    }
    if (this.latest > was) {
      this.arrivals.push([perfMs, this.latest - perfMs / 1000]);
      this.forgetArrivals(perfMs);
    }
    return true;
  }

  /** Drops the arrivals older than CLOCK_WINDOW_MS before `perfMs`, keeping the newest, also while no frame ticks. */
  forgetArrivals(perfMs) {
    while (this.arrivals.length > 1 && this.arrivals[0][0] < perfMs - CLOCK_WINDOW_MS) this.arrivals.shift();
  }

  // A paused plot draws from windows of the hub's history, in two layers:
  // the overview, its whole captured history at the plot's width, and the
  // detail, the view and a span either side at full resolution. The overview
  // covers every time the plot can be moved to, so nothing the hub holds is
  // ever drawn blank while a detail window is on its way.

  /** The `layer` window, 'detail' or 'overview', of `trace` that plot `plotId` last read, if any. */
  /**
   * The time before which the hub has dropped samples it held this session
   * of traces `ids`, the latest of theirs, or null when it has dropped none:
   * before its oldest time, a trace that has dropped nothing never had any.
   */
  /**
   * The oldest time the hub holds of any of traces `ids`, or null while it
   * has said of none. The page's own points can be older: a coarse level
   * keeps what the hub has dropped.
   */
  oldestHeld(ids) {
    let at = null;
    for (const id of ids) {
      const oldest = this.memory?.oldest?.[id];
      if (oldest !== undefined) at = Math.min(at ?? Infinity, oldest);
    }
    return at;
  }

  droppedBefore(ids) {
    let at = null;
    for (const id of ids) {
      const oldest = this.memory?.oldest?.[id], first = this.firstHeld[id];
      if (oldest !== undefined && first !== undefined && oldest > first) at = Math.max(at ?? -Infinity, oldest);
    }
    return at;
  }

  windowOf(plotId, layer, trace) {
    return this.windows.get(plotId)?.[layer].get(trace);
  }

  /**
   * Reads [t0, t1] of `traces`, about `width` px wide, into plot `plotId`'s
   * `layer`. A reply replaces what the layer shows unless a later request's
   * reply already did: none is thrown away for being overtaken by a request
   * still in flight.
   */
  async fetchWindow(plotId, layer, traces, t0, t1, width) {
    const tokens = this.tokensOf(plotId);
    const token = ++tokens.asked;
    const replies = await Promise.all(
      traces.map((t) => api.window({ hub: this.id, id: t.meta.id, t0, t1, width }).then((reply) => [t, reply], () => null)),
    );
    if (token <= tokens.shown[layer]) return;
    tokens.shown[layer] = token;
    let windows = this.windows.get(plotId);
    if (!windows) this.windows.set(plotId, (windows = { detail: new Map(), overview: new Map() }));
    for (const got of replies) if (got) windows[layer].set(got[0], new Detail(got[0], got[1]));
  }

  tokensOf(plotId) {
    let tokens = this.windowTokens.get(plotId);
    if (!tokens) this.windowTokens.set(plotId, (tokens = { asked: 0, shown: { detail: 0, overview: 0 } }));
    return tokens;
  }

  /** Drops the plot's windows, as its view streams again; replies on their way are dropped too. */
  clearWindows(plotId) {
    this.windows.delete(plotId);
    const tokens = this.tokensOf(plotId);
    tokens.shown = { detail: tokens.asked, overview: tokens.asked };
  }
}
