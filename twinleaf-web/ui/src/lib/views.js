// Each plot's View, by plot id. A View outlives the plot's component,
// which a tab switch or a layout change remounts, and outlives the plot
// while an undo or a redo can bring it back, so the plot comes back as it
// showed; it goes once none can.

import { View } from './plot.js';

export class Views {
  map = new Map(); // plot id -> View
  /** How far a live view trails the newest samples, in seconds. */
  delay = 0.15;
  /** Runs with plot `id` whenever its view moves: the page's, which main.js makes the Views before. */
  onChange = () => {};
  /** The traces of plot `id`'s signals the hub has: the page's, which main.js makes the Views before. */
  tracesOf = () => [];

  /** `hub` gives the views the time and what it holds; `onChange`, if given, is `onChange`. */
  constructor(hub, onChange) {
    this.hub = hub;
    if (onChange) this.onChange = onChange;
  }

  /** What the hub holds of plot `id`'s signals, as Session `held` gives it: what each View reads its plot's oldest time from. */
  held(id) {
    return this.hub.session?.held(this.tracesOf(id)) ?? { oldest: null, dropped: false, history: null };
  }

  /** The view of plot `id`, made the first time it is asked for. */
  of(id) {
    let v = this.map.get(id);
    if (!v) {
      v = new View(this.hub);
      v.delay = this.delay;
      v.held = () => this.held(id);
      v.onChange(() => this.onChange(id));
      this.map.set(id, v);
    }
    return v;
  }

  /** The view of plot `id`, if it has one. */
  get(id) {
    return this.map.get(id);
  }

  /** The plots loaded paused whose hub has not yet said what it holds, by id. */
  #loaded = new Set();

  /**
   * Shows each plot of `saved`, a Map of plot id to the paused View state
   * the tabs were kept with (tabs.js `loadTabs`), paused as it was, at page
   * time `perf`, until `settle`.
   */
  load(saved, perf) {
    for (const [id, s] of saved) {
      this.of(id).restore(s, perf);
      this.#loaded.add(id);
    }
  }

  /**
   * Once the hub has said what it holds: the plots `load` showed, each
   * once, that no longer hold what they show at page time `perf` (View
   * `holds`). Those that do stay as they were loaded.
   */
  settle(perf) {
    const gone = [...this.#loaded].filter((id) => this.map.has(id) && !this.map.get(id).holds(perf));
    this.#loaded.clear();
    return gone;
  }

  setDelay(seconds) {
    this.delay = seconds;
    for (const v of this.map.values()) v.delay = seconds;
  }

  /** Drops the views of plots not in `ids`, a Set of plot ids: History `held`'s plots, those the tabs or an undo or redo hold. */
  prune(ids) {
    for (const id of [...this.map.keys()]) if (!ids.has(id)) this.map.delete(id);
  }

  /** Whether a view of a plot `readsHistory` says reads the hub's history is paused. */
  anyPaused(readsHistory) {
    for (const [id, v] of this.map) if (!v.live && readsHistory(id)) return true;
    return false;
  }
}
