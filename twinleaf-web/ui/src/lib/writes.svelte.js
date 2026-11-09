// What the page writes to devices and runs on them, and what it shows of
// that. One DeviceWrites serves the page, so every panel that shows a row
// shows the same. It holds the Cells of the hub's session: what each cell
// shows beside what its device announces, and each device's outcome of its
// last call; a new session starts them empty. And it holds the one notice
// of device writes, which belongs to the write or Revert started last: one
// started earlier that answers later changes its cells, never the notice.
//
// A write offers Revert for REVERT_MS, kept while the pointer is on the
// notice. Revert writes each device's old value back where the device is
// checked and still shows the value written, and leaves any other device
// as it is, saying so. A device whose old value was unknown, as no value
// showed or a call of it was waiting for its answer, has no Revert, and
// the notice says so. Revert goes when its notice closes, when another
// write or Revert starts, and when a device written disconnects or
// reconnects, or the hub does. A run of actions reports with a notice of
// its own, with no Revert. Nothing here is an undo step: Ctrl+Z never
// writes to a device. What the notices say is words.js's.

import { SvelteMap } from 'svelte/reactivity';
import { sameShown } from './properties.js';
import * as words from './words.js';

/** How long a write's notice offers Revert, in ms, the pointer off it. */
export const REVERT_MS = 10_000;
/** How long a notice with no Revert stays, in ms, the pointer off it. */
export const NOTICE_MS = 5_000;
/** How long a cell shows that a call took, in ms. */
export const OK_MS = 1500;

const key = (route, name) => `${route}\u0000${name}`;
/** Value `v` as a write sends it. */
const asText = (v) => (typeof v === 'string' ? v : JSON.stringify(v));
/** Whether a value is known: an old value that is not has no Revert. */
const known = (v) => v !== undefined && v !== null;
const unique = (items) => [...new Set(items)];

/**
 * What the cells of one hub's session show beside what the devices
 * announce. For each device and row: the calls of it waiting for an
 * answer, each showing the value it sends until it is answered, the
 * newest over the rest; the value the last that took came back with,
 * shown until the device announces anew; a value read on request; and the
 * device's outcome of its last call.
 */
class Cells {
  /** By key: {pending: [{id, text, seen}], settled: {value, seen} or null}, `seen` how many times the device's announced value had changed as the call was sent (Device `changes`). */
  written = new SvelteMap();
  reads = new SvelteMap();
  /** By key: {state, error, id}, the outcome of call `id`, busy until it is answered. */
  outcomes = new SvelteMap();
  #ids = 0;

  constructor(session) {
    this.session = session;
  }

  /** The value shown for `name` on device `d`, or undefined when none is known. */
  valueOf(d, name) {
    const k = key(d.route, name), w = this.written.get(k), announced = d.values.get(name);
    if (w?.pending.length) return w.pending.at(-1).text;
    if (w?.settled && d.changes(name) === w.settled.seen) return w.settled.value;
    return announced !== undefined ? announced : this.reads.get(k);
  }

  /** The value shown for `name` on device `d` when no call of it waits for its answer; else undefined, as nobody knows it yet. */
  settled(d, name) {
    return this.written.get(key(d.route, name))?.pending.length ? undefined : this.valueOf(d, name);
  }

  /**
   * Calls `name` with `text` on `devices` through `hub`, showing the value
   * sent while it waits when `writes`, and each device's outcome, and
   * returns the results, each {route, name, state, value, error}. A value
   * that took shows until the device announces anew; one that may have, as
   * no answer came, shows what the device announces; one that did not take
   * leaves the cell as it was.
   */
  async call(hub, name, text, devices, writes) {
    const id = ++this.#ids;
    for (const d of devices) {
      const k = key(d.route, name);
      this.outcomes.set(k, { state: 'busy', id });
      if (writes) {
        const w = this.written.get(k) ?? { pending: [], settled: null };
        this.written.set(k, { ...w, pending: [...w.pending, { id, text, seen: d.changes(name) }] });
      }
    }
    let results;
    try {
      results = (await hub.call(name, text, devices.map((d) => d.route))).results;
    } catch (error) {
      results = devices.map((d) => ({ state: 'unknown', route: d.route, error: error.message }));
    }
    for (const r of results) {
      const k = key(r.route, name);
      if (writes) {
        const w = this.written.get(k);
        const sent = w.pending.find((p) => p.id === id);
        const settled = r.state === 'ok' ? { value: r.value ?? text, seen: sent.seen } : r.state === 'unknown' ? null : w.settled;
        this.written.set(k, { pending: w.pending.filter((p) => p !== sent), settled });
      } else if (r.state === 'ok' && known(r.value)) this.reads.set(k, r.value);
      // A cell shows the outcome of its newest call.
      if (this.outcomes.get(k)?.id !== id) continue;
      this.outcomes.set(k, { state: r.state, error: r.error, id });
      if (r.state === 'ok') setTimeout(() => this.outcomes.get(k)?.id === id && this.outcomes.delete(k), OK_MS);
    }
    return results.map((r) => ({ ...r, name }));
  }
}

/**
 * A notice open for a time, kept while the pointer is on it. `present`
 * opens it and closes it, and `ended` runs as it goes, by its time or by
 * the person.
 */
class TimedNotice {
  /** What it says, {level, text, detail, revert}, or null while closed. */
  value = $state.raw(null);
  #timer = null;
  #left = 0;
  #since = 0;
  #hovered = false;

  constructor(present, ended) {
    this.present = present;
    this.ended = ended;
  }

  /** Shows `value` in place of the one before, for `ms` the pointer is off it. */
  show(value, ms) {
    this.value = value;
    this.present.open();
    this.#left = ms;
    this.#start();
  }
  /** The pointer came onto the notice, `on`, or left it. */
  hover(on) {
    if (on === this.#hovered) return;
    this.#hovered = on;
    if (on) {
      if (this.#timer !== null) this.#left -= Date.now() - this.#since;
      this.#stop();
    } else this.#start();
  }
  /** Takes the notice away, closing it when `close`, as the person closed it otherwise. */
  end(close) {
    this.#stop();
    this.#hovered = false;
    this.ended();
    if (!this.value) return;
    this.value = null;
    if (close) this.present.close();
  }
  #start() {
    this.#stop();
    if (this.#hovered || !this.value) return;
    this.#since = Date.now();
    this.#timer = setTimeout(() => this.end(true), this.#left);
  }
  #stop() {
    clearTimeout(this.#timer);
    this.#timer = null;
  }
}

export class DeviceWrites {
  /** The cells of the hub's session, or null before any. */
  #cells = $state.raw(null);
  /** What Revert puts back: {session, rows, toggle, entries}, each entry {device, connections, name, old, wrote}. */
  #offer = $state.raw(null);
  /** The number of the write or Revert started last, whose notice shows. */
  #latest = 0;
  #notice;

  /**
   * `hub` is the page's Hub, whose `call` and `read` reach the devices.
   * `present` shows the notices: `open()` shows `notice`, or shows it
   * anew, `close()` takes it away, and `report(level, text, detail)` shows
   * a run's report as a notice of its own. `checked(route)` says whether
   * the panels show the device at `route`, as Revert writes to no other.
   */
  constructor(hub, present, { checked = () => true } = {}) {
    this.hub = hub;
    this.present = present;
    this.checked = checked;
    this.#notice = new TimedNotice(present, () => (this.#offer = null));
  }

  /**
   * The notice of the last write or Revert, or null: {level, text,
   * detail, revert}, `level` success, warning or error, `detail` lines
   * under the text, and `revert` whether it was made offering Revert.
   */
  get notice() {
    return this.#notice.value;
  }

  /** The cells of the hub's session, or null while none were made of it. */
  #shown() {
    const c = this.#cells;
    return c && c.session === this.hub.session ? c : null;
  }
  /** The cells of the hub's session, made when none are. */
  #now() {
    return this.#shown() ?? (this.#cells = new Cells(this.hub.session));
  }

  /** The value shown for `name` on device `d`: a value sent and not yet answered, else the one that took while the device has not announced anew, else the one announced or read, or undefined. */
  valueOf(d, name) {
    return this.#shown()?.valueOf(d, name) ?? d.values.get(name);
  }

  /** The outcome of the last call of `name` on the device at `route`, {state, error}, or undefined: busy, then ok for OK_MS, or error, refused, unknown or skipped. */
  outcomeOf(route, name) {
    return this.#shown()?.outcomes.get(key(route, name));
  }

  /** Reads `name` from each of `devices` that shows no value of it, as announcements keep only some entries. */
  readMissing(name, devices) {
    for (const d of devices) {
      if (this.valueOf(d, name) !== undefined) continue;
      const c = this.#now();
      this.hub
        .read(d.route, [name])
        .then((reply) => {
          const got = reply.values[name];
          if (got?.state === 'ok') c.reads.set(key(d.route, name), got.value);
        })
        .catch(() => {});
    }
  }

  /** Starts a write or Revert: the one before it loses its Revert and its claim on the notice. */
  #begin() {
    this.#offer = null;
    return ++this.#latest;
  }

  /**
   * Writes `text` to each row of `edits`, {name, devices}, the devices the
   * write reaches, a call per row at once, and shows a notice of it, which
   * offers Revert of every device whose old value was known. `toggle` says
   * the rows are switches.
   */
  async write(edits, text, toggle = false) {
    edits = edits.filter((e) => e.devices.length);
    if (!edits.length) return;
    const session = this.hub.session, cells = this.#now(), mine = this.#begin();
    const before = edits.flatMap((e) => e.devices.map((d) => ({ device: d, connections: d.connections, name: e.name, old: cells.settled(d, e.name) })));
    const at = (r) => before.find((b) => b.device.route === r.route && b.name === r.name);
    const results = (await Promise.all(edits.map((e) => cells.call(this.hub, e.name, text, e.devices, true)))).flat().map((r) => ({ ...r, label: words.device(at(r).device) }));
    if (mine !== this.#latest) return;
    const ok = results.filter((r) => r.state === 'ok');
    const entries = ok.map((r) => ({ ...at(r), wrote: r.value ?? text })).filter((e) => known(e.old));
    const unknownOld = unique(ok.filter((r) => !known(at(r).old)).map((r) => r.label));
    const olds = unique(entries.map((e) => asText(e.old)));
    const was = edits.length === 1 && entries.length === ok.length && olds.length === 1 ? entries[0].old : undefined;
    const rows = edits.map((e) => e.name);
    this.#offer = entries.length && session === this.hub.session ? { session, rows, toggle, entries } : null;
    const revert = !!this.#offer;
    this.#notice.show({ ...words.wrote({ rows, toggle, value: text, results, was, unknownOld, revert }), revert }, revert ? REVERT_MS : NOTICE_MS);
  }

  /** Whether Revert is offered and can put the last write back: every device it wrote is connected as it was then. */
  get canRevert() {
    return !!this.#offer && !this.revertOff;
  }

  /** Why the last write's Revert went away while its notice shows, {label, why}, or null: `label` the device's, or null for the hub. */
  get revertOff() {
    const o = this.#offer;
    if (!o) return null;
    if (o.session !== this.hub.session) return { label: null, why: 'reconnected' };
    for (const e of o.entries) {
      const d = e.device, label = words.device(d);
      if (o.session.devices.get(d.route) !== d || d.presence !== 'alive') return { label, why: 'disconnected' };
      if (d.connections !== e.connections) return { label, why: 'reconnected' };
    }
    return null;
  }

  /**
   * Puts the last write back: each checked device that still shows the
   * value written gets its old value, and the rest are left as they are,
   * which its notice says. It offers no Revert of its own.
   */
  async revert() {
    if (!this.canRevert) return;
    const o = this.#offer, cells = this.#now(), mine = this.#begin();
    const back = [], left = [];
    for (const e of o.entries) {
      const label = words.device(e.device);
      if (!this.checked(e.device.route)) left.push({ label, name: e.name, why: 'unchecked' });
      else {
        const now = cells.valueOf(e.device, e.name);
        if (sameShown(now, e.wrote)) back.push(e);
        else left.push({ label, name: e.name, now, why: 'changed' });
      }
    }
    // A call per row and old value, all at once.
    const groups = new Map();
    for (const e of back) {
      const k = key(e.name, asText(e.old));
      if (!groups.has(k)) groups.set(k, { name: e.name, text: asText(e.old), devices: [] });
      groups.get(k).devices.push(e.device);
    }
    const results = (await Promise.all([...groups.values()].map((g) => cells.call(this.hub, g.name, g.text, g.devices, true)))).flat().map((r) => ({ ...r, label: words.device(back.find((e) => e.device.route === r.route).device) }));
    if (mine !== this.#latest) return;
    const put = unique(results.filter((r) => r.state === 'ok').map((r) => r.name));
    const olds = unique(back.filter((e) => put.includes(e.name)).map((e) => asText(e.old)));
    const old = put.length === 1 && olds.length === 1 ? back.find((e) => e.name === put[0]).old : undefined;
    const rows = unique(o.entries.map((e) => e.name));
    this.#notice.show({ ...words.reverted({ rows, toggle: o.toggle, results, old, left }), revert: false }, NOTICE_MS);
  }

  /**
   * Runs actions `names` in order, each on the devices `devicesOf(name)`
   * gives, stopping at the first that does not run on every device, and
   * reports what ran with a notice of its own. It asks nothing and offers
   * no Revert.
   */
  async run(names, devicesOf) {
    const cells = this.#now(), done = [], reached = new Set();
    for (const name of names) {
      const to = devicesOf(name);
      if (!to.length) continue;
      for (const d of to) reached.add(d.route);
      const results = (await cells.call(this.hub, name, '', to, false)).map((r) => ({ ...r, label: words.device(to.find((d) => d.route === r.route)) }));
      if (results.every((r) => r.state === 'ok')) {
        done.push(name);
        continue;
      }
      const n = words.ran({ names, done, failed: { name, results }, devices: reached.size });
      return this.present.report(n.level, n.text, n.detail);
    }
    if (!done.length) return;
    const n = words.ran({ names, done, devices: reached.size });
    this.present.report(n.level, n.text, n.detail);
  }

  /** The pointer came onto the notice, `on`, or left it: while it is on, the notice stays. */
  hover(on) {
    this.#notice.hover(on);
  }

  /** The person closed the notice: Revert goes with it. */
  closed() {
    this.#notice.end(false);
  }
}
