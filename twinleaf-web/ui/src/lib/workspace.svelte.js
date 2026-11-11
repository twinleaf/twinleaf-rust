// The page's layout tabs as it edits them, and which plots are focused,
// selected and maximized. The data of a tab is tabs.js's; this is what the
// page does to it. Every plot id is unique across tabs.
//
// The tabs are one value, frozen, that only this class's methods replace:
// a method builds the tabs it leaves and sets them whole, so an earlier
// value is never changed and History keeps it as it was. `guard`, set by
// History, is told of every edit, so an edit made outside an undo step is
// caught where it is made.
//
// The maximized view is this class's too: `maximize` and `restoreLayout`
// are how the page enters and leaves it, and `#fill` is the one place that
// sets which plots fill the tab. What it keeps true:
// - The maximized plots are plots of the open tab, each once, and never
//   none: with none, every plot shows.
// - A maximized plot that is closed or moved to another tab leaves them, by
//   an edit, an undo or a redo alike, and the rest stay maximized; once the
//   last has gone, every plot shows. An undo or a redo otherwise keeps the
//   view as it is, so a plot it brings back stays hidden behind it.
// - Opening a tab, adding a plot, arranging the tab and resetting it show
//   every plot.
// - The plots selected and the plot clicked last are plots that show: one
//   the view hides is let go of, whether maximizing hid it or an undo or a
//   redo would select it.
// Keyboard focus is the page's: the view never moves it.

import { Selection } from './selection.svelte.js';
import { append, equal, plotIds, presets, removePlot, setRatio, splitBeside } from './layout.js';
import { ARRANGEMENTS } from './constants.js';
import { VIEW_FIELDS, defaultContent, emptyTab, newPlot, remapIds } from './tabs.js';

/** `v` with every object in it frozen, in development; as it is otherwise. */
function frozen(v) {
  if (!import.meta.env?.DEV || v === null || typeof v !== 'object' || Object.isFrozen(v)) return v;
  for (const x of Object.values(v)) frozen(x);
  return Object.freeze(v);
}

/** Tab `t` with its plot `id` given the fields of `patch`. */
const withPlot = (t, id, patch) => ({ ...t, plots: { ...t.plots, [id]: { ...t.plots[id], ...patch } } });

export class Workspace {
  #tabs = $state.raw([]);
  activeId = $state(null);
  /** The open tab. */
  tab = $derived(this.#tabs.find((t) => t.id === this.activeId) ?? this.#tabs[0]);
  /** The open tab's plots by id, and its layout: each changes only when it does, not with the rest of the tab. */
  plots = $derived(this.tab.plots);
  layout = $derived(this.tab.layout);
  /** The maximized plots, as `maximized` gives them: `#fill` alone sets them. */
  #maximized = $state.raw(null);
  /** The plots selected in the open tab, the plot clicked last and the rows picked, and their rules: their only writer. */
  selection = new Selection(
    (id) => this.shows(id),
    () => this.shownIds(),
  );
  /** The plot last focused in each tab, for coming back to it. */
  lastFocus = new Map();
  /** Told of every edit before it is made: History's check that an undo step holds it. */
  guard = null;

  /** `initial` is {tabs, activeId}, as tabs.js `loadTabs` returns it. */
  constructor(initial) {
    this.#tabs = frozen(initial.tabs);
    this.activeId = initial.activeId;
  }

  /** The plots selected in the open tab, by id: the Selection's, which alone changes them. */
  get selected() {
    return this.selection.plots;
  }
  /** The plot clicked or focused last, in the open tab, or null. */
  get focusedId() {
    return this.selection.last;
  }
  /** The tabs, frozen. */
  get tabs() {
    return this.#tabs;
  }
  /** The plots that fill the open tab, the rest hidden, as an array of ids, or null while every plot shows. */
  get maximized() {
    return this.#maximized;
  }

  /** Shows plots `ids` of the open tab alone, filling it; those selected that it hides are let go of. With none of the tab's among them, every plot shows. */
  maximize(ids) {
    this.#fill(ids);
  }
  /** Leaves the maximized view: every plot of the open tab shows again, the selection as it is. */
  restoreLayout() {
    this.#fill(null);
  }
  /**
   * Makes the plots of `ids` that the open tab has the maximized plots, or
   * with none of them, or null, shows every plot; then lets go of every
   * plot the selection and the plot clicked last name that does not show.
   * The only writer of the maximized plots, run on every change of what
   * the open tab holds or which tab is open.
   */
  #fill(ids) {
    const plots = this.tab?.plots ?? {};
    const kept = [...new Set(ids ?? [])].filter((id) => plots[id]);
    const now = this.#maximized ?? [];
    if (kept.length !== now.length || kept.some((id, i) => id !== now[i])) this.#maximized = kept.length ? kept : null;
    this.selection.prune();
  }

  /** Makes `tabs` the tabs, as an edit. */
  #set(tabs) {
    this.guard?.();
    this.#tabs = frozen(tabs);
    this.#prune();
  }
  /**
   * Lets go of every plot the maximized plots, the selection and the plot
   * clicked last name that the open tab no longer has: run whenever the
   * tabs change, so none of them ever names a plot that is gone.
   */
  #prune() {
    this.#fill(this.#maximized);
  }
  /** The open tab replaced by `fn(tab)`. */
  #edit(fn) {
    const id = this.tab.id;
    this.#set(this.#tabs.map((t) => (t.id === id ? fn(t) : t)));
  }

  /** Puts back `tabs`, as undo and redo do: not an edit. */
  restore(tabs) {
    this.#tabs = frozen(tabs);
    this.#prune();
  }

  /** Every plot id of every tab. */
  allPlotIds() {
    return this.#tabs.flatMap((t) => plotIds(t.layout));
  }

  /** The plots of the open tab as they show, in layout order: only the maximized ones while some are. */
  shownIds() {
    const ids = plotIds(this.layout);
    return this.#maximized ? ids.filter((id) => this.#maximized.includes(id)) : ids;
  }

  /** Whether plot `id` is one the open tab shows: one of its plots, and while some are maximized, one of those. */
  shows(id) {
    return id != null && !!this.plots?.[id] && (this.#maximized?.includes(id) ?? true);
  }

  /** The time plots of the open tab. */
  timeIds() {
    return plotIds(this.layout).filter((id) => this.plots[id]?.kind === 'time');
  }

  /** The signal keys the open tab shows as spectra. */
  spectrumKeys() {
    const keys = new Set();
    for (const id of plotIds(this.layout)) {
      const p = this.plots[id];
      if (p?.kind === 'spectrum') for (const i of p.items) keys.add(i.key);
    }
    return [...keys];
  }

  /** Opens tab `id`, every plot of it showing and none selected. */
  selectTab(id) {
    if (this.focusedId) this.lastFocus.set(this.activeId, this.focusedId);
    this.activeId = id;
    this.#fill(null);
    this.selection.clear();
  }

  addTab() {
    const t = emptyTab(`Layout ${this.#tabs.length + 1}`);
    this.#set([...this.#tabs, t]);
    this.selectTab(t.id);
  }

  duplicateTab(t) {
    const copy = { ...remapIds(t), name: `${t.name} copy` };
    const i = this.#tabs.findIndex((x) => x.id === t.id) + 1;
    this.#set([...this.#tabs.slice(0, i), copy, ...this.#tabs.slice(i)]);
    this.selectTab(copy.id);
  }

  /** Closes tab `t`. The page always has a tab; closing the last leaves an empty one. */
  closeTab(t) {
    const i = this.#tabs.findIndex((x) => x.id === t.id);
    if (i < 0) return;
    const rest = this.#tabs.filter((x) => x.id !== t.id);
    this.#set(rest.length ? rest : [emptyTab('Layout 1')]);
    if (this.activeId === t.id) this.selectTab(this.#tabs[Math.max(0, i - 1)].id);
  }

  /** Puts the tabs in the order of `ids`, each tab's id once. */
  orderTabs(ids) {
    const byId = new Map(this.#tabs.map((t) => [t.id, t]));
    const next = ids.map((id) => byId.get(id)).filter(Boolean);
    if (next.length === this.#tabs.length && next.some((t, i) => t !== this.#tabs[i])) this.#set(next);
  }

  /** Names tab `id` `name`. */
  renameTab(id, name) {
    if (this.#tabs.find((t) => t.id === id)?.name === name) return;
    this.#set(this.#tabs.map((t) => (t.id === id ? { ...t, name } : t)));
  }

  /** Adds `added` tabs after the others and opens the first. */
  addTabs(added) {
    this.#set([...this.#tabs, ...added]);
    this.selectTab(added[0].id);
  }

  /** Moves plot `plotId` of the open tab to the end of tab `tabId`. */
  movePlotToTab(plotId, tabId) {
    const p = this.plots[plotId];
    if (!p || tabId === this.tab.id || !this.#tabs.some((t) => t.id === tabId)) return;
    const from = this.tab.id;
    this.#set(
      this.#tabs.map((t) => {
        if (t.id === from) {
          const { [plotId]: _, ...plots } = t.plots;
          return { ...t, plots, layout: removePlot(t.layout, plotId) };
        }
        return t.id === tabId ? { ...t, plots: { ...t.plots, [plotId]: p }, layout: append(t.layout, plotId) } : t;
      }),
    );
  }

  /** Adds plot `p` to the open tab, beside plot `where` along `dir`, or else at the end, and focuses it. */
  addPlot(p, where = null, dir = 'row') {
    this.placePlot(p, where ? splitBeside(this.layout, where, p.id, dir) : append(this.layout, p.id));
  }

  /** Adds plot `p` to the open tab, the tab then laid out as `layout`, and makes it the plot clicked last; plots maximized show the rest again, so it shows. */
  placePlot(p, layout) {
    this.#edit((t) => ({ ...t, plots: { ...t.plots, [p.id]: p }, layout }));
    this.#fill(null);
    this.selection.point(p.id);
  }

  /** Gives plot `id` of the open tab the fields of `patch`, as an edit; a patch that changes nothing is none. */
  setPlot(id, patch) {
    const p = this.plots[id];
    if (!p || Object.entries(patch).every(([k, v]) => p[k] === v)) return;
    this.#edit((t) => withPlot(t, id, patch));
  }

  /**
   * Gives plot `id` of the open tab the fields of `patch` that are what its
   * view shows, as a spectrum's frequency range: not an edit, so no undo
   * step holds it.
   */
  setView(id, patch) {
    if (import.meta.env?.DEV && !Object.keys(patch).every((k) => VIEW_FIELDS.has(k))) throw new Error(`not what a view shows: ${Object.keys(patch)}`);
    const p = this.plots[id];
    if (!p || Object.entries(patch).every(([k, v]) => p[k] === v)) return;
    const tab = this.tab.id;
    this.#tabs = frozen(this.#tabs.map((t) => (t.id === tab ? withPlot(t, id, patch) : t)));
  }

  /** Lays the open tab out as `layout`. */
  setLayout(layout) {
    if (layout !== this.layout) this.#edit((t) => ({ ...t, layout }));
  }

  /** Sets the open tab's split at `path` (as layout.js `gutters` names it) to `ratio`. */
  setRatio(path, ratio) {
    this.setLayout(setRatio(this.layout, path, ratio));
  }

  closePlot(id) {
    this.#edit((t) => {
      const { [id]: _, ...plots } = t.plots;
      return { ...t, plots, layout: removePlot(t.layout, id) };
    });
  }

  /**
   * The first arrangement of ARRANGEMENTS, by name, that `arrange` would
   * leave the open tab laid out as it is now, or null for none.
   */
  arrangement() {
    const ids = plotIds(this.layout);
    if (!ids.length) return null;
    const as = (name) => (name === 'quadrants' ? ids.length === 4 && equal(this.layout, presets.grid(ids)) : equal(this.layout, presets[name](ids)));
    return ARRANGEMENTS.map(([name]) => name).find(as) ?? null;
  }

  /** Arranges the open tab's plots as layout.js preset `name`; quadrants first fills the tab to four plots. */
  arrange(name) {
    let ids = plotIds(this.layout);
    const added = {};
    if (name === 'quadrants') {
      while (ids.length < 4) {
        const p = newPlot('time', 'overlay', []);
        added[p.id] = p;
        ids = [...ids, p.id];
      }
      name = 'grid';
    }
    if (ids.length) this.#edit((t) => ({ ...t, plots: { ...t.plots, ...added }, layout: presets[name](ids) }));
    this.#fill(null);
  }

  /** Puts the open tab back to the plots a new page starts with. */
  reset() {
    const d = defaultContent();
    this.#edit((t) => ({ ...t, plots: d.plots, layout: d.layout }));
    this.#fill(null);
  }
}
