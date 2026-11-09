// Which plots are selected, and the plot clicked last, and which property
// rows are picked, and every rule of how a press, a click, a key or focus
// changes them. The plot sets themselves are the Workspace's (`selected`,
// `focusedId`), which History restores and the Workspace prunes; this is the
// one place that decides what they become. Plots and rows are never chosen
// together: choosing either lets go of the other. A row pick spans every
// panel, each row named by its panel and its name.
//
// A press anywhere clears them, but in what works on the plots: the plots
// themselves, a menu or popover, and what is marked data-keeps-selection
// (the window slider, the eraser, File, Commands and the Signals panel). A
// press with Shift keeps them, to add to them; the wheel is no press. A
// press on a plot's own header or controls selects it first, unless it is in
// the selection already, so what it starts acts on what is selected; so does
// a right-click on it. Focus the keyboard brings to a plot from outside it
// selects it alone, as a click on it does.

import { focusedByKeyboard } from './keys.js';

/** What a press keeps the selection in. */
export const KEEPS = '.leaf, [data-keeps-selection], [data-slot$=-content], [role=menu], [role=dialog], [role=listbox]';
/** A table of property rows, a press in which picks rows, so lets go of the plots only. */
const ROWS = '[role=grid]';
/** How long a click on one of several selected plots waits before it narrows the selection to that plot, so a double-click acts on them all. */
export const DOUBLE_CLICK_MS = 300;

export class Selection {
  /** The property rows picked, as `${panel}:${name}` keys. */
  rows = $state.raw(new Set());
  #narrowing;

  /** `ws` is the Workspace whose plots these are; `changed()` runs whenever the selection changes. */
  constructor(ws, changed = () => {}) {
    this.ws = ws;
    this.changed = changed;
  }

  /** Whether a press on `target`, an element, keeps the selection. */
  keeps(target) {
    return !!target?.closest?.(KEEPS);
  }

  /** A press anywhere on the page, `e` its pointerdown, seen before anything under it. */
  press(e) {
    if (e.button !== 0 || e.shiftKey || !(e.target instanceof Element) || this.keeps(e.target)) return;
    if (e.target.closest(ROWS)) this.#clearPlots();
    else this.clear();
  }

  /** Clears the selection, the plot clicked last and the rows picked. */
  clear() {
    this.#clearPlots();
    this.#clearRows();
  }

  #clearPlots() {
    clearTimeout(this.#narrowing);
    if (!this.ws.selected.size && this.ws.focusedId === null) return;
    this.ws.selected.clear();
    this.ws.focusedId = null;
    this.changed();
  }
  #clearRows() {
    if (this.rows.size) this.rows = new Set();
  }

  /** Makes `keys` (`${panel}:${name}`) the rows picked, letting go of the plots. */
  pickRows(keys) {
    this.#clearPlots();
    this.rows = new Set(keys);
  }

  /** Makes plots `ids` the selection, and `last` the plot clicked last: the first of them unless named. */
  only(ids, last = ids[0] ?? null) {
    clearTimeout(this.#narrowing);
    if (ids.length) this.#clearRows();
    this.ws.selected.clear();
    for (const id of ids) this.ws.selected.add(id);
    this.ws.focusedId = last;
    this.changed();
  }

  /** Selects plot `id` alone, unless it is selected already, and makes it the plot clicked last. */
  pressOn(id) {
    clearTimeout(this.#narrowing);
    this.#clearRows();
    if (!this.ws.selected.has(id)) {
      this.ws.selected.clear();
      this.ws.selected.add(id);
    }
    this.ws.focusedId = id;
    this.changed();
  }

  /**
   * A click on plot `id`'s body, `clicks` its count: it selects the plot
   * alone; with Shift it adds the plot or takes it out. A click on one of
   * several selected plots narrows the selection to it once the double-click
   * time has passed, so a double-click still acts on them all.
   */
  click(id, shift, clicks) {
    clearTimeout(this.#narrowing);
    this.#clearRows();
    const selected = this.ws.selected;
    this.ws.focusedId = id;
    if (shift) selected.has(id) ? selected.delete(id) : selected.add(id);
    else if (!selected.has(id)) this.only([id]);
    else if (selected.size > 1 && clicks === 1) this.#narrowing = setTimeout(() => this.only([id]), DOUBLE_CLICK_MS);
    this.changed();
  }

  /**
   * Focus arriving in plot `id`, `e` its focusin. From the keyboard, and from
   * outside the plot, it selects the plot alone, as a click on it does;
   * otherwise the plot is only the plot clicked last.
   */
  focusIn(id, e) {
    const from = e?.relatedTarget;
    const within = from instanceof Element && from.closest('.leaf[data-plot]')?.dataset.plot === id;
    if (e && focusedByKeyboard(e.target) && !within) this.only([id]);
    else this.ws.focusedId = id;
  }
}
