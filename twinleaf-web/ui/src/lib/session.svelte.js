// One hub's state on the page: its traces and their points, spectra and
// full-resolution windows, its devices, and the clock that maps page time
// onto its device time. A `hub` message starts a new Session and the old one
// is dropped whole, so nothing of one hub, its trace ids above all, outlives
// it. What is keyed by a Trace object (GPU buffers, a plot's paused spectra)
// goes with it.
//
// Points and a capture's shots are not reactive: the plots read them every
// frame. What the page's components show is: `signals`, `captures`, each
// capture's `label` and `status`, `devices`, `mounts`, `pps` and
// `detailsDropped`.

import { SvelteMap } from 'svelte/reactivity';
import { api, captureKey, signalKey } from './protocol.js';
import { keep, shotOf } from './shots.js';
import { colors } from './colors.svelte.js';
import * as clock from './clock.js';

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
/** How much longer one window may be than another and count as the same length, as a pan's are but for rounding. */
const SAME_SPAN = 1 + 1e-9;

/**
 * The seconds of history a History setting of `maxAge` seconds and
 * `budget` bytes drops of what the hub holds, by its `memory` (Session
 * `memory`), as {live, paused}: the oldest seconds of the live history, and
 * of the history paused plots hold. Raising a limit drops nothing.
 *
 * The live history keeps the least of what it holds, the new time limit,
 * and the share of what it holds the new budget leaves, the oldest bytes
 * going first. The paused copy runs from `held.seconds` before the pause
 * to the pause, and holds `held.bytes` of rows the live history no longer
 * does, its oldest; the bytes of a second are the live history's. It
 * keeps the new time limit before the pause, and of the rows the live
 * history then no longer holds what fits in the budget the live history
 * leaves, its oldest going first. So both parts are what the hub's own
 * rules (twinleaf-web/src/hub.rs `set_history`) drop, to within a second
 * of what the streams' rates and the hub's chunks round to, as of the
 * hub's last `memory` message, which it sends once a second while what it
 * holds changes.
 */
export function dropping(memory, maxAge, budget) {
  const { seconds: live, bytes } = memory;
  const perSecond = live > 0 ? bytes / live : 0;
  const kept = Math.min(live, maxAge, budget < bytes ? live * (budget / bytes) : live);
  const held = memory.held;
  if (!held || !(held.seconds > 0)) return { live: Math.max(0, live - kept), paused: 0 };
  // Seconds back from the newest live row: the live history begins at
  // `live`, the paused copy's newest row is where it meets it.
  const beyond = perSecond > 0 ? held.bytes / perSecond : 0;
  const pause = live - Math.max(0, held.seconds - beyond);
  const from = pause + Math.min(held.seconds, maxAge);
  const alone = Math.max(0, from - Math.max(pause, kept));
  const room = perSecond > 0 ? Math.max(0, budget - kept * perSecond) / perSecond : Infinity;
  const heldKept = Math.min(held.seconds, maxAge) - Math.max(0, alone - room);
  return { live: Math.max(0, live - kept), paused: Math.max(0, held.seconds - heldKept) };
}

/**
 * What a plot paused keeps at the rate and limits of the hub's `memory`,
 * once the live history holds all it may: {steady, need, keeps}, or null
 * while nothing streams or no budget is set. `steady` is the bytes of the
 * time limit at the rate, `need` those of the live history and a full
 * paused one beside it, and `keeps` the seconds from the pause that the
 * paused history keeps the whole time limit before it loses its oldest:
 * all of `maxAge` when `need` fits the budget, and then it loses none.
 *
 * The paused history shares the live history's rows, and costs the rows
 * the live history drops since the pause, `rate` bytes a second, in the
 * room the live history leaves of the budget, its oldest going first
 * (twinleaf-web/src/hub.rs `trim_held`). When memory comes first the live
 * history fills the budget and leaves no room, so `keeps` is 0.
 */
export function pausing({ rate, maxAge, budget }) {
  if (!(rate > 0) || !(budget > 0)) return null;
  const steady = rate * maxAge;
  return { steady, need: 2 * steady, keeps: Math.min(maxAge, Math.max(0, (budget - steady) / rate)) };
}

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
}
Detail.prototype.lowerBound = Trace.prototype.lowerBound;
Detail.prototype.xAt = Trace.prototype.xAt;
Detail.prototype.yAt = Trace.prototype.yAt;
Detail.prototype.extent = Trace.prototype.extent;

/**
 * One capture RPC of one device: what its metadata names it, where it
 * stands, and the shots the hub sent this page of it, newest last
 * (shots.js). A plot names it as it names a Trace, by `key` and `meta`.
 */
export class Capture {
  /** What its metadata says a capture of it is of, {name, units, x_name, x_units}, or null while the device has not said. */
  label = $state.raw(null);
  /**
   * Where it stands, as the hub's `capture` says: {state, number, repeat,
   * read, size, error, since}. `state` is idle, capturing, reading (`read` of
   * `size` bytes), waiting, busy, timeout or error (with `error`); `number`
   * that of the last capture the hub read of it; `repeat` the seconds
   * between captures while a page asks for them again and again, or null;
   * `since` the page time, in ms, the hub said so.
   */
  status = $state.raw({ state: 'idle', number: 0, repeat: null, since: 0 });
  /** Its shots, which only the session adds to. */
  shots = [];
  /** What a person reads it as: the name its metadata gives, or its RPC's, after its device's route as a signal key has it. */
  title = $derived(`${this.key.slice(0, this.key.lastIndexOf('/') + 1)}${this.label?.name || this.rpc}`);
  /** Its units and those of its x axis, with the x axis's name, as a plot reads a signal's `meta`. */
  meta = $derived({ units: this.label?.units ?? '', xName: this.label?.x_name ?? '', xUnits: this.label?.x_units ?? '' });

  constructor(route, rpc) {
    this.route = route;
    this.rpc = rpc;
    this.key = captureKey({ route, rpc });
  }
}

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
  /** The page time its presence turned lost, in ms, or null while it is not. */
  lostAt = $state(null);
  /** How many times its presence turned alive: one more each time it reconnects. */
  connections = $state(0);
  /** {rpcs} once its RPC table loaded, {error, rpcs: []} if it did not, or null before. */
  table = $state.raw(null);
  /** The values it last announced, by RPC name. */
  values = new SvelteMap();
  /** Its firmware, {installed, catalog} as the hub's `firmware` message has them, or null before the hub read it. */
  fw = $state.raw(null);
  /** Where an upgrade of it stands, the hub's `upgrade` message, or null while none was asked for or it was cleared. */
  upgrade = $state.raw(null);

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
  /** The capture RPCs the devices list, each a Capture, sorted by key. */
  captures = $state.raw([]);
  /** Each Device by route, in the order the hub listed them. */
  devices = new SvelteMap();
  /** The devices the hub mounted, [{url, source, prefix}], or null for the proxy's default. */
  mounts = $state.raw(null);
  /** The decimation, in points per second per trace of the finest level. */
  pps = $state(0);
  /** The seconds each level of the decimation keeps, finest first. */
  levels = $state.raw([]);
  /**
   * The hub's raw history, {bytes, budget, seconds, maxAge, rate, held,
   * oldest}, or null before it says. `held` is the history paused plots
   * hold, {bytes, seconds}: the bytes it holds beyond the live history and
   * the seconds of each stream it holds up to the pause, or null while no
   * plot is paused.
   */
  memory = $state.raw(null);
  /** The oldest time the hub held of each trace when the session first heard, by trace id: an oldest later than it since is a drop. */
  firstHeld = {};

  traces = new Map(); // trace id -> Trace
  byKey = new Map(); // signal key -> Trace, and capture key -> Capture
  /** Each paused plot's windows of the hub's history, by layer. */
  windows = new Map(); // plot id -> {detail, overview}: each a Map, trace -> Detail
  /** How many times every window was dropped, for the page to read them again. */
  detailsDropped = $state(0);
  reads = new Map(); // plot id -> {detail, overview}: each layer's {flying, waiting, sent}, see `fetchWindow`
  latest = -Infinity; // device time of the newest point
  offset = null; // device time minus page time, in seconds
  arrivals = []; // [page ms, device time minus page time in s] of each batch that moved `latest`, the last CLOCK_WINDOW_MS
  tickedAt = null; // page ms of the last tick
  /** `id` names the hub, null before the socket reached one. */
  constructor(id = null) {
    this.id = id;
  }

  /** The samples a second of stream `stream` of the device at `route`, as a trace of it says, or undefined when the hub sent none of it. */
  rateOf(route, stream) {
    for (const t of this.traces.values()) if (t.meta.route === route && t.meta.stream === stream) return t.meta.rate;
    return undefined;
  }

  /** The trace of signal `key`, or the Capture of a capture's, read reactively: a component asks again as traces arrive and captures are listed. */
  trace(key) {
    this.signals;
    this.captures;
    return this.byKey.get(key);
  }

  /** Lists the captures `keep(capture)` holds of those listed, and gives each listed a colour. */
  #listCaptures(keep) {
    for (const c of this.captures) if (!keep(c)) this.byKey.delete(c.key);
    this.captures = [...this.byKey.values()].filter((c) => c instanceof Capture).sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
    colors.give(this.captures.map((c) => c.key));
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
        this.signals = [...this.traces.values()].sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
        colors.give(this.signals.map((s) => s.key));
        return;
      }
      case 'clear':
        // The server re-decimated its history; a fresh copy follows.
        for (const t of this.traces.values()) t.clear();
        this.clearAllWindows();
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
        this.#listCaptures((c) => msg.routes.includes(c.route));
        return;
      case 'captures':
        for (const { rpc, label } of msg.captures) {
          const key = captureKey({ route: msg.route, rpc });
          if (!this.byKey.has(key)) this.byKey.set(key, new Capture(msg.route, rpc));
          this.byKey.get(key).label = label ?? null;
        }
        this.#listCaptures((c) => c.route !== msg.route || msg.captures.some((listed) => listed.rpc === c.rpc));
        return;
      case 'capture': {
        const c = this.byKey.get(captureKey(msg));
        if (c) c.status = { state: msg.state, number: msg.number, repeat: msg.repeat ?? null, read: msg.read, size: msg.size, error: msg.error, since: clock.now() };
        return;
      }
      // The server sends a device's messages only while the routes list it.
      case 'device': {
        const d = this.devices.get(msg.route);
        if (!d) return;
        if (msg.presence === 'alive' && d.presence !== 'alive') d.connections++;
        if (msg.presence !== 'lost') d.lostAt = null;
        else if (d.presence !== 'lost') d.lostAt = clock.now();
        return void Object.assign(d, { name: msg.name, serial: msg.serial, firmware: msg.firmware, presence: msg.presence });
      }
      case 'table': {
        const d = this.devices.get(msg.route);
        if (d) d.table = msg.error ? { error: msg.error, rpcs: [] } : { rpcs: msg.rpcs };
        return;
      }
      case 'settings':
        return this.devices.get(msg.route)?.announce(msg.values);
      case 'firmware': {
        const d = this.devices.get(msg.route);
        if (d) d.fw = { installed: msg.installed ?? null, catalog: msg.catalog ?? { state: 'unchecked' } };
        return;
      }
      case 'upgrade': {
        const d = this.devices.get(msg.route);
        if (d) d.upgrade = msg.state === 'idle' ? null : { state: msg.state, source: msg.source, target: msg.target, sent: msg.sent, size: msg.size, step: msg.step, error: msg.error, installed: msg.installed ?? null };
        return;
      }
    }
  }

  /**
   * Applies one decoded binary frame. Returns false when it names a trace
   * this session never heard of, a gap the page fills with a new snapshot.
   * `perfMs` is the page time it arrived. A capture is the newest shot of
   * its Capture, and one of a capture no device lists is dropped.
   */
  apply(frame, perfMs = clock.now()) {
    if (frame.kind === 'capture') {
      const c = this.byKey.get(captureKey(frame));
      if (c) keep(c.shots, shotOf(frame));
      return true;
    }
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

  /**
   * What the hub holds of `traces`, as {oldest, dropped, history}.
   * `oldest` is the oldest time it holds of any of them: where it holds
   * more of one than of another, the longer history's. While it has said of
   * none, it is the oldest point the page has of them, or null with none.
   * The page's own points can be older than the hub's: a coarse level keeps
   * what the hub has dropped. `dropped` is whether the hub has dropped
   * samples it held this session of any of them: before its oldest time, a
   * trace that has dropped nothing never had any. `history` is the hub's
   * History setting, the seconds it keeps of each stream at most, or null
   * while it has not said.
   */
  held(traces) {
    let oldest = null, dropped = false;
    for (const t of traces) {
      const at = this.memory?.oldest?.[t.meta.id];
      if (at === undefined) continue;
      oldest = Math.min(oldest ?? Infinity, at);
      if (at > this.firstHeld[t.meta.id]) dropped = true;
    }
    if (oldest === null) {
      for (const t of traces) for (const l of t.levels) if (l.length) oldest = Math.min(oldest ?? Infinity, l.xAt(l.absStart));
    }
    return { oldest, dropped, history: this.memory?.maxAge || null };
  }

  /** The `layer` window, 'detail' or 'overview', of `trace` that plot `plotId` last read, if any. */
  windowOf(plotId, layer, trace) {
    return this.windows.get(plotId)?.[layer].get(trace);
  }

  /** Whether plot `plotId` has a window read in flight or still to send, of either layer. */
  reading(plotId) {
    return Object.values(this.reads.get(plotId) ?? {}).some((r) => r.flying || r.waiting || r.sent);
  }

  // Each layer of each plot reads one window at a time, and sends at most
  // one read a frame, so a move asks at once and a run of moves asks once a
  // frame. An ask made while a read is in flight waits, in place of any ask
  // already waiting, and is sent as the reply lands, when the window being
  // read holds the view it asks for and is no longer than the window it
  // asks for: that reply shows the view as finely as a read begun over
  // would, and sooner. Any other read in flight is of a stretch the plot
  // has left, or coarser than the view wants: it is aborted, its reply
  // never parsed, and the ask is sent in its place. So the view a move ends
  // at is drawn from the first reply to land after it, replies land in the
  // order asked, and none replaces a later one's.

  /**
   * Asks for [x0, x1] of `traces` and `pad` seconds either side, about
   * `width` px wide, for plot `plotId`'s `layer`: read at once, or once the
   * layer's read in flight has landed or the frame has passed.
   */
  fetchWindow(plotId, layer, traces, x0, x1, pad, width) {
    let reads = this.reads.get(plotId);
    if (!reads) this.reads.set(plotId, (reads = { detail: { flying: null, waiting: null, sent: null }, overview: { flying: null, waiting: null, sent: null } }));
    const read = reads[layer];
    const ask = { traces, t0: x0 - pad, t1: x1 + pad, width }, flying = read.flying;
    if (flying && !(flying.t0 <= x0 && flying.t1 >= x1 && flying.t1 - flying.t0 <= (ask.t1 - ask.t0) * SAME_SPAN)) this.#abort(read);
    read.waiting = ask;
    this.#send(plotId, layer, read);
  }

  /** Sends the ask `read` has waiting, unless it has a read in flight or sent one this frame. */
  #send(plotId, layer, read) {
    const ask = read.waiting;
    if (!ask || read.flying || read.sent) return;
    read.waiting = null;
    read.sent = clock.frame(() => {
      read.sent = null;
      this.#send(plotId, layer, read);
    });
    const { traces, t0, t1, width } = ask, abort = new AbortController();
    const flying = (read.flying = { t0, t1, abort });
    const one = (t) => api.window({ hub: this.id, id: t.meta.id, t0, t1, width, signal: abort.signal }).then((reply) => [t, reply], () => null);
    Promise.all(traces.map(one)).then((replies) => {
      // Aborted, it is no longer the layer's read.
      if (read.flying !== flying) return;
      read.flying = null;
      let windows = this.windows.get(plotId);
      if (!windows) this.windows.set(plotId, (windows = { detail: new Map(), overview: new Map() }));
      for (const got of replies) if (got) windows[layer].set(got[0], new Detail(got[0], got[1]));
      this.#send(plotId, layer, read);
    });
  }

  /** Aborts the read `read` has in flight, and drops the ask it has waiting. */
  #abort(read) {
    read.flying?.abort.abort();
    read.flying = null;
    read.waiting = null;
  }

  /** Drops the plot's windows, as its view streams again, with the reads on their way and still to send. */
  clearWindows(plotId) {
    this.windows.delete(plotId);
    for (const read of Object.values(this.reads.get(plotId) ?? {})) this.#abort(read);
    this.reads.delete(plotId);
  }

  /** Drops every plot's windows and reads, as the hub's history is replaced or the page leaves the hub. */
  clearAllWindows() {
    for (const plotId of new Set([...this.windows.keys(), ...this.reads.keys()])) this.clearWindows(plotId);
  }
}
