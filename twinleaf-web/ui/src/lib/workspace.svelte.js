// The page's layout tabs as it edits them, and which plots are focused,
// selected and maximized. The data of a tab is tabs.js's; this is what the
// page does to it. Every plot id is unique across tabs.

import { SvelteSet } from 'svelte/reactivity';
import { append, plotIds, presets, removePlot, setRatio, splitBeside } from './layout.js';
import { chip, defaultContent, emptyTab, newPlot, remapIds } from './tabs.js';

export class Workspace {
  tabs = $state([]);
  activeId = $state(null);
  /** The open tab. */
  tab = $derived(this.tabs.find((t) => t.id === this.activeId) ?? this.tabs[0]);
  /** The plot clicked or focused last, in the open tab. */
  focusedId = $state(null);
  /** The plot that fills the tab, or null. */
  maximized = $state(null);
  /** The plots selected in the open tab, by id. */
  selected = new SvelteSet();
  /** The plot last focused in each tab, for coming back to it. */
  lastFocus = new Map();

  /** `initial` is {tabs, activeId}, as tabs.js `loadTabs` returns it. */
  constructor(initial) {
    this.tabs = initial.tabs;
    this.activeId = initial.activeId;
  }

  get plots() {
    return this.tab.plots;
  }
  get layout() {
    return this.tab.layout;
  }

  /** Every plot id of every tab. */
  allPlotIds() {
    return this.tabs.flatMap((t) => plotIds(t.layout));
  }

  /** The highlighted plots: the selection, or the plot clicked last, or every plot in the tab. */
  highlighted() {
    if (this.selected.size) return [...this.selected];
    return this.plots[this.focusedId] ? [this.focusedId] : plotIds(this.layout);
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

  selectTab(id) {
    if (this.focusedId) this.lastFocus.set(this.activeId, this.focusedId);
    this.activeId = id;
    this.maximized = null;
    this.focusedId = null;
    this.selected.clear();
  }

  addTab() {
    const t = emptyTab(`Layout ${this.tabs.length + 1}`);
    this.tabs.push(t);
    this.selectTab(t.id);
  }

  duplicateTab(t) {
    const copy = remapIds(JSON.parse(JSON.stringify(t)));
    copy.name = `${t.name} copy`;
    this.tabs.splice(this.tabs.findIndex((x) => x.id === t.id) + 1, 0, copy);
    this.selectTab(copy.id);
  }

  /** Closes tab `t`. The page always has a tab; closing the last leaves an empty one. */
  closeTab(t) {
    const i = this.tabs.findIndex((x) => x.id === t.id);
    if (i < 0) return;
    this.tabs.splice(i, 1);
    if (!this.tabs.length) this.tabs.push(emptyTab('Layout 1'));
    if (this.activeId === t.id) this.selectTab(this.tabs[Math.max(0, i - 1)].id);
  }

  /** Puts the tabs in the order of `ids`, each tab's id once. */
  orderTabs(ids) {
    const byId = new Map(this.tabs.map((t) => [t.id, t]));
    const next = ids.map((id) => byId.get(id)).filter(Boolean);
    if (next.length === this.tabs.length && next.some((t, i) => t !== this.tabs[i])) this.tabs = next;
  }

  /** Adds `added` tabs after the others and opens the first. */
  addTabs(added) {
    this.tabs.push(...added);
    this.selectTab(added[0].id);
  }

  /** Moves plot `plotId` of the open tab to the end of tab `tabId`. */
  movePlotToTab(plotId, tabId) {
    const target = this.tabs.find((t) => t.id === tabId);
    const p = this.plots[plotId];
    if (!target || !p) return;
    this.tab.layout = removePlot(this.tab.layout, plotId);
    delete this.tab.plots[plotId];
    this.selected.delete(plotId);
    if (this.maximized === plotId) this.maximized = null;
    target.plots[plotId] = { ...p };
    target.layout = append(target.layout, plotId);
  }

  /** Adds plot `p` to the open tab, beside plot `where` along `dir`, or else at the end, and focuses it. */
  addPlot(p, where = null, dir = 'row') {
    this.tab.plots[p.id] = p;
    this.tab.layout = where ? splitBeside(this.layout, where, p.id, dir) : append(this.layout, p.id);
    this.focusedId = p.id;
  }

  /** Adds signal `key` to the focused plot, or the first, or a new one when there is none. */
  addToFocused(key) {
    const plot = this.plots[this.focusedId] ?? this.plots[plotIds(this.layout)[0]];
    if (!plot) this.addPlot(newPlot('time', 'overlay', [key]));
    else if (!plot.items.some((i) => i.key === key)) plot.items.push(chip(key));
  }

  /** Adds signal `key` to each plot of `ids` that does not show it. */
  addTo(ids, key) {
    for (const id of ids) {
      const plot = this.plots[id];
      if (plot && !plot.items.some((i) => i.key === key)) plot.items.push(chip(key));
    }
  }

  /** Sets the open tab's split at `path` (as layout.js `gutters` names it) to `ratio`. */
  setRatio(path, ratio) {
    this.tab.layout = setRatio(this.layout, path, ratio);
  }

  closePlot(id) {
    this.tab.layout = removePlot(this.layout, id);
    delete this.tab.plots[id];
    if (this.maximized === id) this.maximized = null;
    this.selected.delete(id);
  }

  /** Arranges the open tab's plots as layout.js preset `name`; quadrants first fills the tab to four plots. */
  arrange(name) {
    let ids = plotIds(this.layout);
    if (name === 'quadrants') {
      while (ids.length < 4) {
        const p = newPlot('time', 'overlay', []);
        this.tab.plots[p.id] = p;
        ids = [...ids, p.id];
      }
      name = 'grid';
    }
    if (ids.length) this.tab.layout = presets[name](ids);
    this.maximized = null;
  }

  /** Puts the open tab back to the plots a new page starts with. */
  reset() {
    const d = defaultContent();
    this.tab.plots = d.plots;
    this.tab.layout = d.layout;
    this.maximized = null;
  }
}
