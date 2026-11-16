// Which plots are selected, the plot clicked last, and which rows of the
// panels' lists are picked, and every rule of how a press, a click, a key
// or an undo changes them. This class holds them and is their only writer:
// the page, the drags, the Workspace (as plots go) and History (as it
// restores a step) all go through its methods.
//
// The rows picked are one pick, made one way for every list: each row is
// named by its panel and its name, a click picks it alone, Ctrl adds it or
// drops it, and a Shift pick runs along the rows in page order. They are of
// one kind at a time: property rows, of every panel of properties together,
// or signals, the rows of the Signals list (`rows`, `signals`). Picking a
// row of one kind lets go of the rows of the other. Plots and property rows
// are never chosen together: choosing either lets go of the other. Plots
// and signals are, as the signals picked are what goes onto the plots
// selected. The rows picked are those of the picks the page shows: a pick
// whose panel folds, whose dock hides, that leaves Favorites or that the
// Signals list's search leaves out stops being picked while it is not
// shown, and the next pick starts from those that are. A click on one of
// several signals picked narrows the pick to it once the double-click time
// has passed, so a double-click acts on them all, as a click on one of
// several selected plots does. The arrows move the row they are on without
// picking it.
//
// A press anywhere clears them, but in what works on the plots: the plots
// themselves, a menu or popover, and what is marked data-keeps-selection
// (the window field, the eraser, File, Commands and the Signals panel). A
// press with Shift keeps them, to add to them; the wheel is no press; a
// press among property rows, a table, a panel's action buttons or the
// edit bar that acts on the rows picked, lets go of the plots only. A press
// or right-click on a plot's own header or controls selects it first,
// unless it is in the selection already, so what it starts acts on what is
// selected. The keyboard moving to a plot (Tab, 1 to 9, F6) selects it
// alone, as a click on it does; focus that arrives otherwise, as when a
// menu gives it back, only makes the plot the one clicked last. An undo
// puts back the plots selected and the property rows picked, never the
// signals picked.

import { SvelteSet } from 'svelte/reactivity';
import * as clock from './clock.js';

/** What a press keeps the selection in. */
export const KEEPS = '.leaf, [data-keeps-selection], [data-slot$=-content], [role=menu], [role=dialog], [role=listbox]';
/** A table of property rows, a panel's action buttons, or the edit bar under them: a press in one picks rows or acts on those picked, so lets go of the plots only. */
export const ROWS = '[role=grid], [data-rows]';
/** What a signal's row is named by, before its key: the Signals panel. */
export const SIGNAL = 'signals:';
/** How long a click on one of several selected plots waits before it narrows the selection to that plot, so a double-click acts on them all. */
export const DOUBLE_CLICK_MS = 300;

const isSignal = (key) => key.startsWith(SIGNAL);

export class Selection {
  /** The selected plots, by id. */
  plots = new SvelteSet();
  /** The plot clicked or focused last, or null. */
  last = $state(null);
  /** The rows picked, shown or not, as `${panel}:${name}` keys, in the order picked: property rows, or signals. */
  #picks = $state.raw([]);
  /** The row picked or moved to last, shown or not. */
  #at = $state(null);
  /** The row a Shift pick runs from: the row last picked without Shift, while any row is picked. */
  #anchor = null;
  /**
   * Every row the page shows, of properties and of signals, as keys, in
   * page order: the page's to set, a function whose reads are tracked. With
   * none, as with no page, every row picked shows.
   */
  rowOrder = $state.raw(null);
  /** The rows the page shows, or null for every row. */
  #shown = $derived(this.rowOrder ? new Set(this.rowOrder()) : null);
  /** The rows picked that the page shows, in the order picked. */
  #picked = $derived(this.#shown ? this.#picks.filter((k) => this.#shown.has(k)) : this.#picks);
  #rows = $derived(new Set(this.#picked.filter((k) => !isSignal(k))));
  #signals = $derived(new Set(this.#picked.filter(isSignal).map((k) => k.slice(SIGNAL.length))));
  /** The property rows picked that the page shows, in the order picked: what every act on the rows picked acts on. */
  get rows() {
    return this.#rows;
  }
  /** The signals picked that the Signals list shows, by key, in the order picked: what goes onto the plots selected. */
  get signals() {
    return this.#signals;
  }
  /** The row the arrows are on, picked or moved to last, while the page shows it, or null: where the page edits the rows picked. */
  get rowAt() {
    return this.#shown && !this.#shown.has(this.#at) ? null : this.#at;
  }
  #narrowing;
  /** The narrowing of the signals picked that a click on one of several of them waits to make. */
  #rowNarrowing;
  /** Runs whenever the plots selected change: the page's, to show them. */
  changed = () => {};

  /** `exists(id)` tells whether plot `id` is one the open tab shows, and `all()` gives every plot it shows, as Ctrl+A selects them. */
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

  /** Clears the selection, the plot clicked last and the rows picked, signals among them. */
  clear() {
    this.#setPlots([], null);
    this.#setRows([]);
  }

  /** Makes plots `ids` the selection, and `last` the plot clicked last: the first of them unless named. */
  only(ids, last = ids[0] ?? null) {
    if (ids.length) this.#dropRows();
    this.#setPlots(ids, last);
  }

  /** Selects plot `id` alone, unless it is selected already, and makes it the plot clicked last. */
  pressOn(id) {
    this.#dropRows();
    this.#setPlots(this.plots.has(id) ? [...this.plots] : [id], id);
  }

  /** The keyboard moved focus to plot `id`: it is selected alone. */
  arrived(id) {
    this.only([id]);
  }

  /** Makes plot `id`, or none, the plot clicked last, the selection as it is. */
  point(id) {
    clock.cancel(this.#narrowing);
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
    this.#dropRows();
    if (shift) {
      const next = new Set(this.plots);
      if (!next.delete(id)) next.add(id);
      return this.#setPlots([...next], id);
    }
    if (!this.plots.has(id) || this.plots.size === 1) return this.#setPlots([id], id);
    this.point(id);
    if (clicks === 1) this.#narrowing = clock.after(DOUBLE_CLICK_MS, () => this.exists(id) && this.#setPlots([id], id));
  }

  /**
   * Makes `keys` (`${panel}:${name}`), rows of one kind, the rows picked,
   * and `at` the row the arrows are on. It lets go of the plots, unless
   * the rows are signals.
   */
  pickRows(keys, at = this.#at) {
    if (!keys.length || !keys.every(isSignal)) this.#setPlots([], null);
    this.#setRows(keys);
    this.#at = at;
  }

  /**
   * A click on row `key` with `mods`' keys, or a key picking it: it picks
   * the row alone; with Shift, the run to it from the row a pick started
   * at, in page order (`rowOrder`), or the row alone when that row is not
   * shown; with Ctrl or ⌘, it adds the row or drops it. The rows of the
   * other kind, property rows or signals, are no part of it: a run passes
   * over them, and none of them stays picked. `clicks` is a click's count
   * on a signal: a click on one of several signals picked narrows the pick
   * to it once the double-click time has passed, and a second click stops
   * that.
   */
  pickRow(key, { shiftKey = false, ctrlKey = false, metaKey = false } = {}, clicks = 0) {
    clock.cancel(this.#rowNarrowing);
    const like = (k) => isSignal(k) === isSignal(key);
    const order = (this.rowOrder?.() ?? []).filter(like);
    // A signal's click leaves the plots selected, the last signal dropped too.
    if (!isSignal(key)) this.#setPlots([], null);
    if (shiftKey && order.includes(this.#anchor) && order.includes(key)) {
      const [a, b] = [order.indexOf(this.#anchor), order.indexOf(key)].sort((x, y) => x - y);
      this.#setRows(order.slice(a, b + 1));
    } else {
      if (ctrlKey || metaKey) {
        const next = new Set(this.#picked.filter(like));
        if (!next.delete(key)) next.add(key);
        this.#setRows([...next]);
      } else if (clicks && !shiftKey && isSignal(key) && this.#picked.length > 1 && this.#picked.includes(key)) {
        if (clicks === 1) this.#rowNarrowing = clock.after(DOUBLE_CLICK_MS, () => this.#setRows([key]));
      } else this.#setRows([key]);
      this.#anchor = key;
    }
    this.#at = key;
  }

  /** A right-click on row `key`, or a key making a command on it: it picks the row alone, unless it is picked already. */
  rowMenu(key) {
    clock.cancel(this.#rowNarrowing);
    if (!this.#picked.includes(key)) this.pickRow(key);
  }

  /** The arrows moved to row `key`: it is the row they are on, the rows picked as they were. */
  moveTo(key) {
    this.#at = key;
  }

  /**
   * The signals a command acts on, by key: made on signal `key`'s row,
   * every signal picked when it is one of them, else it alone; made from
   * the page, with null, the signals picked.
   */
  signalTargets(key = null) {
    return key === null || this.signals.has(key) ? [...this.signals] : [key];
  }

  /** The plots a command made on plot `id`, from its menu or its header, acts on: every selected plot when `id` is one of them, else `id` alone. */
  targets(id) {
    return (this.plots.has(id) ? [...this.plots] : [id]).filter((i) => this.exists(i));
  }

  /**
   * The plots a command made from the page or the keys acts on, `scope` its
   * kind (commands.js): the selection; with none, for a 'view'
   * command every plot shown, as after Ctrl+A, and for a 'plot' command none.
   */
  pageTargets(scope) {
    if (this.plots.size) return [...this.plots].filter((i) => this.exists(i));
    return scope === 'view' ? this.all() : [];
  }

  /** What an undo step keeps of the selection: the plots, the plot clicked last and the property rows picked; never the signals. */
  snapshot() {
    const rows = this.#picks.filter((k) => !isSignal(k));
    return { selected: [...this.plots], focusedId: this.last, rows, rowAt: rows.length ? this.#at : null };
  }

  /**
   * Puts back what `snapshot` gave, less the plots that do not show: its
   * plots, and with none of them, its property rows, of which those the
   * page shows are the rows picked. The signals picked stay as they are,
   * but where property rows come back.
   */
  restore(s) {
    const ids = s.selected.filter((id) => this.exists(id));
    const rows = ids.length ? [] : (s.rows ?? []);
    if (rows.length) {
      this.#setRows(rows);
      this.#at = s.rowAt;
    } else this.#dropRows();
    this.#setPlots(ids, this.exists(s.focusedId) ? s.focusedId : null);
  }

  /** Lets go of the plots that are gone or no longer show: the Workspace’s, whenever its tabs or its maximized plots change. */
  prune() {
    if ([...this.plots].some((id) => !this.exists(id)) || (this.last !== null && !this.exists(this.last))) {
      this.#setPlots([...this.plots].filter((id) => this.exists(id)), this.exists(this.last) ? this.last : null);
    }
  }

  #setPlots(ids, last) {
    clock.cancel(this.#narrowing);
    const same = ids.length === this.plots.size && ids.every((id) => this.plots.has(id));
    if (!same) {
      this.plots.clear();
      for (const id of ids) this.plots.add(id);
    }
    this.last = last;
    if (!same) this.changed();
  }
  #setRows(keys) {
    clock.cancel(this.#rowNarrowing);
    if (!keys.length) this.#anchor = null;
    if (keys.length || this.#picks.length) this.#picks = [...new Set(keys)];
  }
  /** Lets go of the rows picked where they are property rows: signals stay with the plots. */
  #dropRows() {
    if (!this.#picks.every(isSignal)) this.#setRows([]);
  }
}
