// Which plots are selected, the plot clicked last, and which property rows
// are picked, and every rule of how a press, a click, a key or an undo
// changes them. This class holds them and is their only writer: the page,
// the drags, the Workspace (as plots go) and History (as it restores a step)
// all go through its methods. Plots and rows are never chosen together:
// choosing either lets go of the other. A row pick spans every panel, each
// row named by its panel and its name.
//
// A press anywhere clears them, but in what works on the plots: the plots
// themselves, a menu or popover, and what is marked data-keeps-selection
// (the window slider, the eraser, File, Commands and the Signals panel). A
// press with Shift keeps them, to add to them; the wheel is no press; a
// press in a table of rows lets go of the plots only. A press or right-click
// on a plot's own header or controls selects it first, unless it is in the
// selection already, so what it starts acts on what is selected. The
// keyboard moving to a plot (Tab, 1 to 9, F6) selects it alone, as a click
// on it does; focus that arrives otherwise, as when a menu gives it back,
// only makes the plot the one clicked last.

import { SvelteSet } from 'svelte/reactivity';

/** What a press keeps the selection in. */
export const KEEPS = '.leaf, [data-keeps-selection], [data-slot$=-content], [role=menu], [role=dialog], [role=listbox]';
/** A table of property rows, a press in which picks rows, so lets go of the plots only. */
const ROWS = '[role=grid]';
/** How long a click on one of several selected plots waits before it narrows the selection to that plot, so a double-click acts on them all. */
export const DOUBLE_CLICK_MS = 300;

export class Selection {
  /** The selected plots, by id. */
  plots = new SvelteSet();
  /** The plot clicked or focused last, or null. */
  last = $state(null);
  /** The property rows picked, as `${panel}:${name}` keys. */
  rows = $state.raw(new Set());
  #narrowing;
  /** Runs whenever the plots selected change: the page's, to show them. */
  changed = () => {};

  /** `exists(id)` tells whether plot `id` is in the open tab. */
  constructor(exists) {
    this.exists = exists;
  }

  /** Whether a press on `target`, an element, keeps the selection. */
  keeps(target) {
    return !!target?.closest?.(KEEPS);
  }

  /** A press anywhere on the page, `e` its pointerdown, seen before anything under it. */
  press(e) {
    if (e.button !== 0 || e.shiftKey || !(e.target instanceof Element) || this.keeps(e.target)) return;
    if (e.target.closest(ROWS)) this.#setPlots([], null);
    else this.clear();
  }

  /** Clears the selection, the plot clicked last and the rows picked. */
  clear() {
    this.#setPlots([], null);
    this.#setRows([]);
  }

  /** Makes plots `ids` the selection, and `last` the plot clicked last: the first of them unless named. */
  only(ids, last = ids[0] ?? null) {
    if (ids.length) this.#setRows([]);
    this.#setPlots(ids, last);
  }

  /** Selects plot `id` alone, unless it is selected already, and makes it the plot clicked last. */
  pressOn(id) {
    this.#setRows([]);
    this.#setPlots(this.plots.has(id) ? [...this.plots] : [id], id);
  }

  /** The keyboard moved focus to plot `id`: it is selected alone. */
  arrived(id) {
    this.only([id]);
  }

  /** Makes plot `id`, or none, the plot clicked last, the selection as it is. */
  point(id) {
    clearTimeout(this.#narrowing);
    this.last = id;
  }

  /**
   * A click on plot `id`'s body, `clicks` its count: it selects the plot
   * alone; with Shift it adds the plot or takes it out. A click on one of
   * several selected plots narrows the selection to it once the double-click
   * time has passed, so a double-click still acts on them all; anything that
   * changes the selection meanwhile, or the plot going, stops that.
   */
  click(id, shift, clicks) {
    this.#setRows([]);
    if (shift) {
      const next = new Set(this.plots);
      if (!next.delete(id)) next.add(id);
      return this.#setPlots([...next], id);
    }
    if (!this.plots.has(id) || this.plots.size === 1) return this.#setPlots([id], id);
    this.point(id);
    if (clicks === 1) this.#narrowing = setTimeout(() => this.exists(id) && this.#setPlots([id], id), DOUBLE_CLICK_MS);
  }

  /** Makes `keys` (`${panel}:${name}`) the rows picked, letting go of the plots. */
  pickRows(keys) {
    this.#setPlots([], null);
    this.#setRows(keys);
  }

  /** The plots a command on plot `id` acts on: every selected plot when `id` is one of them, or from the keys whenever any is selected; else `id` alone. */
  targets(id, fromKeys = false) {
    return (this.plots.has(id) || (fromKeys && this.plots.size) ? [...this.plots] : [id]).filter((i) => this.exists(i));
  }

  /** What an undo step keeps of the selection. */
  snapshot() {
    return { selected: [...this.plots], focusedId: this.last };
  }

  /** Puts back what `snapshot` gave, less the plots that are gone. */
  restore(s) {
    const ids = s.selected.filter((id) => this.exists(id));
    if (ids.length) this.#setRows([]);
    this.#setPlots(ids, this.exists(s.focusedId) ? s.focusedId : null);
  }

  /** Lets go of the plots that are gone: the Workspace's, whenever its tabs change. */
  prune() {
    if ([...this.plots].some((id) => !this.exists(id)) || (this.last !== null && !this.exists(this.last))) {
      this.#setPlots([...this.plots].filter((id) => this.exists(id)), this.exists(this.last) ? this.last : null);
    }
  }

  #setPlots(ids, last) {
    clearTimeout(this.#narrowing);
    const same = ids.length === this.plots.size && ids.every((id) => this.plots.has(id));
    if (!same) {
      this.plots.clear();
      for (const id of ids) this.plots.add(id);
    }
    this.last = last;
    if (!same) this.changed();
  }
  #setRows(keys) {
    if (keys.length || this.rows.size) this.rows = new Set(keys);
  }
}
