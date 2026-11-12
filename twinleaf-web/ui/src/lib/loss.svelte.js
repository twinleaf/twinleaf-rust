// What the page lost of the data it shows, and where, since it opened or
// its counts were last reset, and what of it was lost in the last minute.
//
// A sample is lost in one of four places the page tells apart (see "What
// was lost" in docs/twinleaf-web.md):
// - `link`: the link to a device went down, counted in drops and seconds;
// - `skipped`: the device sent no sample, or its link lost it while it
//   stayed up, counted in samples;
// - `dropped`: the proxy or its hub dropped samples because the hub fell
//   behind, counted in samples;
// - `behind`: the page fell too far behind the hub and was sent a snapshot
//   again, counted in times.
//
// The hub's `loss` message holds a device's counts since the hub started.
// The page counts what each adds to the one before it. What a hub had lost
// before the page first heard it is not the page's: the `loss` of a
// snapshot of a hub new to the page is where its counts start. The Hub
// (hub.svelte.js) hands every snapshot and every `loss` here, and these
// counts outlive the hub's sessions.
//
// The causes of the last minute are listed largest first, by the seconds
// of data each stands for: a link's outage as it is, samples by their
// stream's rate, and a snapshot by the changes it stood for, each a flush
// of the hub's (16 ms).

import { SvelteMap } from 'svelte/reactivity';

/** How long a loss stays in the status bar, in ms. */
export const RECENT_MS = 60_000;
/** The seconds of data one change the page fell behind stands for: the hub's flush. */
const CHANGE_SECONDS = 0.016;
/** The causes, in the order equal ones are listed. */
export const CAUSES = ['link', 'dropped', 'skipped', 'behind'];

/** A device's counts: the drops of its link and their seconds, and the samples skipped and dropped. */
const none = () => ({ drops: 0, seconds: 0, skipped: 0, dropped: 0 });

export class Losses {
  /** Each device's counts since the page opened or `reset`, by route: {drops, seconds, skipped, dropped}. */
  devices = new SvelteMap();
  /** How many times the page fell behind the hub, since the same. */
  behind = $state(0);
  /** How many times the counts were reset. */
  resets = $state(0);
  /**
   * What was lost in the last RECENT_MS, largest first: [{kind, n,
   * seconds}], `kind` one of CAUSES, `n` its count, and `seconds` how long
   * a link was down.
   */
  recent = $state.raw([]);

  /** The id of the hub the counts last heard are of. */
  #hub = null;
  /** Whether the hub is new to the page since its last snapshot, so that snapshot's counts are where the page's start. */
  #fresh = false;
  /** The counts last heard of each device of the hub, by route: {drops, seconds, streams: {name: {skipped, dropped}}}. */
  #heard = new Map();
  /** Each loss of the last RECENT_MS: {at, kind, n, seconds, time}, `time` the seconds of data it stands for. */
  #events = [];

  /**
   * Notes a snapshot of hub `id` at page time `now`, sent in place of
   * `behind` changes the page fell behind, 0 for one a socket opened with.
   */
  snapshot(id, behind, now) {
    this.#fresh = id !== this.#hub;
    if (this.#fresh) this.#heard.clear();
    this.#hub = id;
    if (!(behind > 0)) return;
    this.behind++;
    this.#note(now, 'behind', 1, 0, behind * CHANGE_SECONDS);
  }

  /**
   * Counts what `loss` message `msg` adds to the one before it of its
   * device, at page time `now`. `inSnapshot` is whether a snapshot holds
   * it, and `rateOf(stream)` the samples a second of its device's stream
   * of that name, or undefined when the page shows none of it.
   */
  apply(msg, inSnapshot, rateOf, now) {
    const before = this.#heard.get(msg.route);
    const streams = Object.fromEntries(Object.entries(msg.streams).map(([name, s]) => [name, { skipped: s.skipped, dropped: s.proxy + s.hub }]));
    this.#heard.set(msg.route, { drops: msg.link.times, seconds: msg.link.seconds, streams });
    if (!before && inSnapshot && this.#fresh) return;
    const add = none();
    add.drops = Math.max(0, msg.link.times - (before?.drops ?? 0));
    add.seconds = Math.max(0, msg.link.seconds - (before?.seconds ?? 0));
    if (add.drops || add.seconds) this.#note(now, 'link', add.drops, add.seconds, add.seconds);
    for (const [name, s] of Object.entries(streams)) {
      const was = before?.streams[name];
      for (const kind of ['skipped', 'dropped']) {
        const n = Math.max(0, s[kind] - (was?.[kind] ?? 0));
        if (!n) continue;
        add[kind] += n;
        this.#note(now, kind, n, 0, n / (rateOf(name) || Infinity));
      }
    }
    if (!add.drops && !add.seconds && !add.skipped && !add.dropped) return;
    const had = this.devices.get(msg.route) ?? none();
    this.devices.set(msg.route, { drops: had.drops + add.drops, seconds: had.seconds + add.seconds, skipped: had.skipped + add.skipped, dropped: had.dropped + add.dropped });
  }

  /** Whether the page counts anything lost. */
  get any() {
    return this.behind > 0 || this.devices.size > 0;
  }

  /** Counts from nothing again, as from the page's opening. */
  reset() {
    this.devices.clear();
    this.behind = 0;
    this.resets++;
    this.#events = [];
    this.recent = [];
  }

  /** Lets the losses older than RECENT_MS at page time `now` leave `recent`. */
  tick(now) {
    if (!this.#events.length || this.#events[0].at > now - RECENT_MS) return;
    this.#events = this.#events.filter((e) => e.at > now - RECENT_MS);
    this.#list();
  }

  #note(at, kind, n, seconds, time) {
    this.#events.push({ at, kind, n, seconds, time });
    this.#list();
  }

  /** Lists the causes of the losses kept, each summed, the one that stands for the most data first. */
  #list() {
    const sums = new Map();
    for (const e of this.#events) {
      const s = sums.get(e.kind) ?? { kind: e.kind, n: 0, seconds: 0, time: 0 };
      sums.set(e.kind, { kind: e.kind, n: s.n + e.n, seconds: s.seconds + e.seconds, time: s.time + e.time });
    }
    this.recent = [...sums.values()]
      .sort((a, b) => b.time - a.time || CAUSES.indexOf(a.kind) - CAUSES.indexOf(b.kind))
      .map(({ kind, n, seconds }) => ({ kind, n, seconds }));
  }
}
