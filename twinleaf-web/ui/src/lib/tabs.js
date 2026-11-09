// Layout tabs as data: what a tab holds, the tab a new page starts with, and
// how tabs are kept in this browser and moved between people as a file.
//
// A tab is {id, name, plots, layout}. `plots` maps a plot id to
// {id, kind, mode, items, legend?, legendAt?}: `kind` is time or spectrum,
// `mode` one of plot.js's modes, `items` the plot's signals as [{id, key}],
// each `key` a signal key (protocol.js `signalKey`), `legend` whether the
// legend shows in the plot, and `legendAt` where it was dragged, {x, y} each
// a share of the room around it, 0 to 1, top right when absent. `axisSides`
// places an independent plot's axes, signal key to 'left', 'right' or
// 'none' (plot.js axisSlots). `dockSize` is {w, h}, the width it last had
// beside another plot and the height it last had above or below one, each
// a share of the layout, which a move docks it at again. `layout` is the split tree of layout.js over the plot ids, or null
// for an empty tab.
//
// The tabs are kept in localStorage under SAVE_KEY as {tabs, activeId}.
// Outside tools read that key and shape, so `tabFromJson` is the one place
// that reads it, and a change to it changes the key.

import { SHADOW_ITEM_MARKER_PROPERTY_NAME as SHADOW } from 'svelte-dnd-action';
import { fromJson, plotIds, presets } from './layout.js';

export const SAVE_KEY = 'tl-plot-tabs-v2';
/** The `format` of an exported file of tabs. */
export const FILE_FORMAT = 'twinleaf-plot-layouts';

let n = 0;
/** An id no other tab, plot or signal item on the page has, starting with `prefix`. */
export const uid = (prefix) => `${prefix}-${++n}-${Math.random().toString(36).slice(2, 6)}`;
/** A plot's item showing signal `key`. */
export const chip = (key) => ({ id: uid('c'), key });
export const newPlot = (kind, mode, keys) => ({ id: uid('p'), kind, mode, items: keys.map(chip) });

/** The plots and layout a new page starts with, of the simulator's signals. */
export function defaultContent() {
  const ps = [
    newPlot('time', 'overlay', ['sine.sine', 'aux.triangle']),
    newPlot('time', 'stacked', ['sine.sine', 'sine.cosine', 'aux.triangle', 'aux.sawtooth']),
    newPlot('time', 'independent', ['sine.cosine', 'aux.sawtooth']),
    newPlot('spectrum', 'overlay', ['sine.sine', 'sine.cosine', 'aux.triangle']),
  ];
  return { plots: Object.fromEntries(ps.map((p) => [p.id, p])), layout: presets.grid(ps.map((p) => p.id)) };
}

export const makeTab = (name, content = defaultContent()) => ({ id: uid('t'), name, ...content });
export const emptyTab = (name) => makeTab(name, { plots: {}, layout: null });

/** A copy of tab `t` whose tab, plots and items all have fresh ids, so it shares none with its source. */
export function remapIds(t) {
  const map = {};
  const plots = {};
  for (const [id, p] of Object.entries(t.plots)) {
    map[id] = uid('p');
    plots[map[id]] = { ...p, id: map[id], items: p.items.map((i) => chip(i.key)) };
  }
  const walk = (node) =>
    !node ? null : node.type === 'leaf' ? { ...node, plotId: map[node.plotId] } : { ...node, a: walk(node.a), b: walk(node.b) };
  return { id: uid('t'), name: t.name, plots, layout: walk(t.layout) };
}

/**
 * The tab saved or imported JSON describes, or null if it does not hold
 * together: a name, plots with items, and a layout over those plots. Its
 * items get fresh ids, as one shared with another plot or the signal list
 * confuses the drag library.
 */
export function tabFromJson(t) {
  if (!t || typeof t.name !== 'string' || typeof t.plots !== 'object' || !t.plots) return null;
  const plots = {};
  for (const [id, p] of Object.entries(t.plots)) {
    if (!p || !Array.isArray(p.items)) return null;
    plots[id] = { ...p, id, items: p.items.filter((i) => i && !i[SHADOW] && typeof i.key === 'string').map((i) => chip(i.key)) };
  }
  const layout = t.layout ? fromJson(t.layout) : null;
  const ids = plotIds(layout);
  if ((t.layout && !layout) || !ids.every((id) => plots[id]) || new Set(ids).size !== ids.length) return null;
  return { id: typeof t.id === 'string' ? t.id : uid('t'), name: t.name, plots, layout };
}

/** The tabs as JSON holds them: without the item the drag library shows where a tab would land. */
const realTabs = (tabs) => tabs.filter((t) => !t[SHADOW]);

/** The tabs kept in `storage`, or one default tab, as {tabs, activeId}. */
export function loadTabs(storage) {
  try {
    const saved = JSON.parse(storage.getItem(SAVE_KEY));
    const tabs = (saved?.tabs ?? []).map(tabFromJson).filter(Boolean);
    if (tabs.length) return { tabs, activeId: tabs.some((t) => t.id === saved.activeId) ? saved.activeId : tabs[0].id };
  } catch {}
  const t = makeTab('Main');
  return { tabs: [t], activeId: t.id };
}

/** Keeps `tabs` and the open one in `storage`. A storage that refuses keeps nothing. */
export function saveTabs(storage, tabs, activeId) {
  try {
    storage.setItem(SAVE_KEY, JSON.stringify({ tabs: realTabs(tabs), activeId }));
  } catch {}
}

/** The text of a file of `tabs`, to share them. */
export function exportText(tabs) {
  const data = { format: FILE_FORMAT, version: 1, tabs: realTabs(tabs).map(({ name, plots, layout }) => ({ name, plots, layout })) };
  return JSON.stringify(data, null, 2);
}

/** The tabs of an exported file's `text`, each with fresh ids. Throws an Error saying why there are none. */
export function importText(text) {
  const data = JSON.parse(text);
  if (data?.format !== FILE_FORMAT) throw new Error('not a Twinleaf layout file');
  const tabs = (data.tabs ?? []).map(tabFromJson).filter(Boolean).map(remapIds);
  if (!tabs.length) throw new Error('no layouts in the file');
  return tabs;
}
