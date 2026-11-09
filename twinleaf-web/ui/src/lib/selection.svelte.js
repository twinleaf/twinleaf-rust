// Which plots are selected, the plot clicked last, and which property rows
// are picked, and every rule of how a press, a click, a key or an undo
// changes them. This class holds them and is their only writer: the page,
// the drags, the Workspace (as plots go) and History (as it restores a step)
// all go through its methods. Plots and rows are never chosen together:
// choosing either lets go of the other. A row pick spans every panel, each
// row named by its panel and its name, and a Shift pick runs along the rows
// in page order. The rows picked are those of the picks the page shows: a
// pick whose panel folds, whose dock hides or that leaves Favorites stops
// being picked while it is not shown, and the next pick starts from those
// that are.
//
// A press anywhere clears them, but in what works on the plots: the plots
// themselves, a menu or popover, and what is marked data-keeps-selection
// (the window slider, the eraser, File, Commands and the Signals panel). A
// press with Shift keeps them, to add to them; the wheel is no press; a
// press among property rows, a table, a panel's action buttons or the
// edit bar that acts on the rows picked, lets go of the plots only. A press
// or right-click on a plot's own header or controls selects it first,
// unless it is in the selection already, so what it starts acts on what is
// selected. The keyboard moving to a plot (Tab, 1 to 9, F6) selects it
// alone, as a click on it does; focus that arrives otherwise, as when a
// menu gives it back, only makes the plot the one clicked last.

import { SvelteSet } from 'svelte/reactivity';

/** What a press keeps the selection in. */
export const KEEPS = '.leaf, [data-keeps-selection], [data-slot$=-content], [role=menu], [role=dialog], [role=listbox]';
/** A table of property rows, a panel's action buttons, or the edit bar under them: a press in one picks rows or acts on those picked, so lets go of the plots only. */
export const ROWS = '[role=grid], [data-rows]';
/** How long a click on one of several selected plots waits before it narrows the selection to that plot, so a double-click acts on them all. */
export const DOUBLE_CLICK_MS = 300;

export class Selection {
  /** The selected plots, by id. */
  plots = new SvelteSet();
  /** The plot clicked or focused last, or null. */
  last = $state(null);
  /** The property rows picked, shown or not, as `${panel}:${name}` keys, in the order picked. */
  #picks = $state.raw([]);
  /** The property row picked or moved to last, shown or not. */
  #at = $state(null);
  /** The row a Shift pick runs from: the row last picked without Shift, while any row is picked. */
  #anchor = null;
  /**
   * Every property row the page shows, as keys, in page order: the page's
   * to set, a function whose reads are tracked. With none, as with no page,
   * every row picked shows.
   */
  rowOrder = $state.raw(null);
  /** The rows the page shows, or null for every row. */
  #shown = $derived(this.rowOrder ? new Set(this.rowOrder()) : null);
  #rows = $derived(new Set(this.#shown ? this.#picks.filter((k) => this.#shown.has(k)) : this.#picks));
  /** The property rows picked that the page shows, in the order picked: what every act on the rows picked acts on. */
  get rows() {
    return this.#rows;
  }
  /** The property row the arrows are on, picked or moved to last, while the page shows it, or null: where the page edits the rows picked. */
  get rowAt() {
    return this.#shown && !this.#shown.has(this.#at) ? null : this.#at;
  }
  #narrowing;
  /** Runs whenever the plots selected change: the page's, to show them. */
  changed = () => {};

  /** `exists(id)` tells whether plot `id` is in the open tab, and `all()` gives every plot it shows, as Ctrl+A selects them. */
  constructor(exists, all) {
    this.exists = exists;
    this.all = all;
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

  /** Makes `keys` (`${panel}:${name}`) the rows picked, letting go of the plots, and `at` the row the arrows are on. */
  pickRows(keys, at = this.#at) {
    this.#setPlots([], null);
    this.#setRows(keys);
    this.#at = at;
  }

  /**
   * A click on property row `key` with `mods`' keys, or an arrow moving to
   * it: it picks the row alone; with Shift, the run to it from the row a
   * pick started at, in page order (`rowOrder`), or the row alone when that
   * row is not shown; with Ctrl or ⌘, it adds the row or drops it.
   */
  pickRow(key, { shiftKey = false, ctrlKey = false, metaKey = false } = {}) {
    const order = this.rowOrder?.() ?? [];
    if (shiftKey && order.includes(this.#anchor) && order.includes(key)) {
      const [a, b] = [order.indexOf(this.#anchor), order.indexOf(key)].sort((x, y) => x - y);
      this.pickRows(order.slice(a, b + 1));
    } else {
      if (ctrlKey || metaKey) {
        const next = new Set(this.rows);
        if (!next.delete(key)) next.add(key);
        this.pickRows([...next]);
      } else this.pickRows([key]);
      this.#anchor = key;
    }
    this.#at = key;
  }

  /** A right-click on property row `key`: it picks the row alone, unless it is picked already. */
  rowMenu(key) {
    if (!this.rows.has(key)) this.pickRow(key);
  }

  /** The plots a command made on plot `id`, from its menu or its header, acts on: every selected plot when `id` is one of them, else `id` alone. */
  targets(id) {
    return (this.plots.has(id) ? [...this.plots] : [id]).filter((i) => this.exists(i));
  }

  /**
   * The plots a command made from the page or the keys acts on, `scope` its
   * kind (keymap.js `scopeOf`): the selection; with none, for a 'view'
   * command every plot shown, as after Ctrl+A, and for a 'plot' command none.
   */
  pageTargets(scope) {
    if (this.plots.size) return [...this.plots].filter((i) => this.exists(i));
    return scope === 'view' ? this.all() : [];
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
    if (!keys.length) this.#anchor = null;
    if (keys.length || this.#picks.length) this.#picks = [...new Set(keys)];
  }
}
