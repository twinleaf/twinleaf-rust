// Each time plot's View, by plot id. A View outlives the plot's component,
// which a tab switch or a layout change remounts, and goes when its plot
// leaves every tab.

import { View } from './plot.js';

export class Views {
  map = new Map(); // plot id -> View
  /** How far a live view trails the newest samples, in seconds. */
  delay = 0.15;

  /** `onChange(id)` runs when plot `id`'s view moves. */
  constructor(hub, onChange) {
    this.hub = hub;
    this.onChange = onChange;
  }

  /** The view of plot `id`, made the first time it is asked for. */
  of(id) {
    let v = this.map.get(id);
    if (!v) {
      v = new View(this.hub);
      v.delay = this.delay;
      v.onChange(() => this.onChange(id));
      this.map.set(id, v);
    }
    return v;
  }

  /** The view of plot `id`, if it has one. */
  get(id) {
    return this.map.get(id);
  }

  setDelay(seconds) {
    this.delay = seconds;
    for (const v of this.map.values()) v.delay = seconds;
  }

  /** Drops the views of plots not in `ids`, a Set of every tab's plot ids. */
  prune(ids) {
    for (const id of [...this.map.keys()]) if (!ids.has(id)) this.map.delete(id);
  }

  /**
   * The device time from which the traces must keep every point: the left
   * edge of the earliest paused view of a plot `keepsPoints` says holds
   * points, or null when none is paused.
   */
  heldFrom(keepsPoints) {
    let from = null;
    for (const [id, v] of this.map) if (!v.live && keepsPoints(id)) from = Math.min(from ?? Infinity, v.range(0)[0]);
    return from;
  }
}
