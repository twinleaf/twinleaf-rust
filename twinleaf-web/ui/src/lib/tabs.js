// Layout tabs as data: what a tab holds, the tab a new page starts with, and
// how tabs are kept in this browser and moved between people as a file.
//
// A tab is {id, name, plots, layout}. `plots` maps a plot id to
// {id, kind, mode, items, legend?, legendAt?}: `kind` is time, spectrum or
// capture, `mode` one of plot.js's modes, `items` the plot's signals as
// [{id, key}], each `key` a signal key (protocol.js `signalKey`), or on a
// capture plot a capture's (`captureKey`), never both (`kindWith`). A
// capture plot's `auto` is whether it asks for captures again and again,
// `autoDelay` seconds after each (constants.js CAPTURE_DELAY when absent),
// and `shots` and `shotCount` how it shows them (shots.js), the newest
// alone when absent, and `captureView` whether along their x, 'time', or
// as their spectra, 'spectrum' (constants.js `drawnKind`), along their x
// when absent. `legend` is whether the
// legend shows in the plot, and `legendAt` where it was dragged, {x, y} each
// a share of the room around it, 0 to 1, top right when absent. `axisSides`
// places an independent plot's axes, signal key to 'left', 'right' or
// 'none' (plot.js axisSlots). `cursors` holds the plot's X and Y cursors,
// {x, f, y} as cursors.js lays them out, and is absent while none is
// placed; `readout` is false while the cursors' readout is hidden, and
// `readoutAt` where it was dragged, a share as `legendAt` is, top left when
// absent. `yFormat` holds each y axis's label, digits, notation and unit, by its
// range key ('shared', or the signal key of an axis of its own), and
// `xFormat` the digits and notation of a spectrum's or a capture's x axis,
// each as units.js `axisFormat` reads them, a setting absent at Auto;
// `fRange` is the stretch of that axis shown, absent for all of it.
// `xScale` and `yScale` are a spectrum's frequency and density scales, and
// `captureYScale` a capture plot's y scale, each 'log' or 'linear'
// (`yScaleField`). `layout` is
// the split tree of layout.js over the plot ids, or null for an empty tab.
//
// The tabs are kept in localStorage under SAVE_KEY as {tabs, activeId}.
// Outside tools read that key and shape, so `tabFromJson` is the one place
// that reads it, and a change to it changes the key. A paused time plot is
// kept with its `view` there too, as View `state` gives it without spectra,
// so the page loads it paused where it was (ViewHistory `settle`); the
// tabs the page edits hold no view.

import { cursorsFromJson, withoutX } from './cursors.js';
import { fromJson, plotIds, presets } from './layout.js';
import { isCaptureKey } from './protocol.js';
import { drawnKind } from './constants.js';

export const SAVE_KEY = 'tl-plot-tabs-v2';
/** The `format` of an exported file of tabs. */
export const FILE_FORMAT = 'twinleaf-plot-layouts';

let n = 0;
/** An id no other tab, plot or signal item on the page has, starting with `prefix`. */
export const uid = (prefix) => `${prefix}-${++n}-${Math.random().toString(36).slice(2, 6)}`;
/** A plot's item showing signal `key`. */
export const chip = (key) => ({ id: uid('c'), key });
/**
 * What a plot is until it is set otherwise: log scales and no legend. Every
 * plot holds each, so setting a field to what it already shows changes
 * nothing.
 */
export const PLOT_DEFAULTS = Object.freeze({ xScale: 'log', yScale: 'log', legend: false });
/** Plot fields that are what its view shows, not what was edited: no undo step holds them. */
export const VIEW_FIELDS = new Set(['fRange']);
export const newPlot = (kind, mode, keys) => ({ id: uid('p'), kind, mode, ...PLOT_DEFAULTS, items: keys.map(chip) });

/**
 * The field of `plot` that holds the scale of the y axis it draws, 'log' or
 * 'linear': a capture plot's `captureYScale` along its x, linear until
 * set, as a capture's values are of either sign; any other plot's, and a
 * capture plot's in its Spectrum view, `yScale`, a spectrum's density
 * scale, log until set (PLOT_DEFAULTS).
 */
export const yScaleField = (plot) => (drawnKind(plot) === 'capture' ? 'captureYScale' : 'yScale');
/** Whether the y axis `plot` draws is log (`yScaleField`): a time plot's never is. */
export const yLogOf = (plot) => (drawnKind(plot) === 'time' ? false : yScaleField(plot) === 'yScale' ? plot.yScale !== 'linear' : plot[yScaleField(plot)] === 'log');

/** What the page says where a plot takes none of the signals given it, by `kindWith`. */
export const APART = 'A capture and a signal do not share a plot';

/**
 * The kind `plot` is once it shows the signal of `key` too, or null where
 * it cannot: a plot shows captures or the signals that stream, never both,
 * as they share no x axis. A plot with no signal takes either, and becomes
 * a capture plot with a capture, and a time plot again with a signal.
 */
export function kindWith(plot, key) {
  const capture = isCaptureKey(key);
  if (!plot.items.length) return capture ? 'capture' : plot.kind === 'capture' ? 'time' : plot.kind;
  return capture === (plot.kind === 'capture') ? plot.kind : null;
}

/**
 * `plot` with those of the signals `keys` it lacks and can show
 * (`kindWith`), after the ones it has: {kind, items}, or null where it
 * takes none.
 */
export function joined(plot, keys) {
  let { kind, items } = plot;
  for (const key of keys) {
    const next = items.some((i) => i.key === key) ? null : kindWith({ kind, items }, key);
    if (next) [kind, items] = [next, [...items, chip(key)]];
  }
  return items === plot.items ? null : { kind, items };
}
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

/** The paused view JSON `v` describes, as View `state` gives one without spectra, or null when it is none or does not hold together. */
function pausedFromJson(v) {
  const number = (n) => typeof n === 'number' && isFinite(n);
  const object = (o) => !!o && typeof o === 'object' && !Array.isArray(o);
  if (!object(v) || v.live !== false || !['window', 'span', 'right', 'pausedAt'].every((k) => number(v[k]))) return null;
  if (!(v.pausedHistory === null || number(v.pausedHistory)) || !object(v.yRanges)) return null;
  const yRanges = {};
  for (const [kind, byKey] of Object.entries(v.yRanges))
    for (const [key, r] of Object.entries(object(byKey) ? byKey : {})) if (Array.isArray(r) && r.length === 2 && r.every(number)) (yRanges[kind] ??= {})[key] = [...r];
  const { window, span, right, pausedAt, pausedHistory } = v;
  return { live: false, window, span, right, pausedAt, pausedHistory, yRanges };
}

/**
 * The tab saved or imported JSON describes, or null if it does not hold
 * together: a name, plots with items, and a layout over those plots. A
 * plot's cursors are read as far as they hold together, but for its time X
 * cursors, which only a time plot kept paused keeps: any other view starts
 * live, which has none (cursors.js). Such a plot's view goes into `paused`,
 * a Map by plot id, when given; without it none is kept. Its items get
 * fresh ids, as one shared with another plot or the signal list
 * confuses the drag library.
 */
export function tabFromJson(t, paused = null) {
  if (!t || typeof t.name !== 'string' || typeof t.plots !== 'object' || !t.plots) return null;
  const plots = {}, views = new Map();
  for (const [id, p] of Object.entries(t.plots)) {
    if (!p || !Array.isArray(p.items)) return null;
    const view = paused && p.kind === 'time' ? pausedFromJson(p.view) : null;
    if (view) views.set(id, view);
    const { cursors: _, view: __, ...rest } = p, read = cursorsFromJson(p.cursors), cursors = view ? read : withoutX(read, 'time');
    // A signal its plot's kind does not show is left out (`kindWith`).
    const keys = p.items.filter((i) => i && typeof i.key === 'string').map((i) => i.key).filter((key) => isCaptureKey(key) === (p.kind === 'capture'));
    plots[id] = { ...PLOT_DEFAULTS, ...rest, id, items: keys.map(chip), ...(cursors && { cursors }) };
  }
  const layout = t.layout ? fromJson(t.layout) : null;
  const ids = plotIds(layout);
  if ((t.layout && !layout) || !ids.every((id) => plots[id]) || new Set(ids).size !== ids.length) return null;
  for (const [id, view] of views) paused.set(id, view);
  return { id: typeof t.id === 'string' ? t.id : uid('t'), name: t.name, plots, layout };
}

/** The tabs kept in `storage`, or one default tab, as {tabs, activeId, views}: `views` the paused views kept, by plot id. */
export function loadTabs(storage) {
  try {
    const saved = JSON.parse(storage.getItem(SAVE_KEY)), views = new Map();
    const tabs = (saved?.tabs ?? []).map((t) => tabFromJson(t, views)).filter(Boolean);
    if (tabs.length) return { tabs, activeId: tabs.some((t) => t.id === saved.activeId) ? saved.activeId : tabs[0].id, views };
  } catch {}
  const t = makeTab('Main');
  return { tabs: [t], activeId: t.id, views: new Map() };
}

/**
 * Keeps `tabs` and the open one in `storage`, each time plot whose View
 * `viewOf(id)` gives is paused with that view. A storage that refuses
 * keeps nothing.
 */
export function saveTabs(storage, tabs, activeId, viewOf = () => undefined) {
  const kept = tabs.map((t) => {
    const plots = Object.entries(t.plots).map(([id, p]) => {
      const s = p.kind === 'time' ? viewOf(id)?.state() : null;
      if (!s || s.live) return [id, p];
      const { spectra: _, ...view } = s;
      return [id, { ...p, view }];
    });
    return { ...t, plots: Object.fromEntries(plots) };
  });
  try {
    storage.setItem(SAVE_KEY, JSON.stringify({ tabs: kept, activeId }));
  } catch {}
}

/** The text of a file of `tabs`, to share them. */
export function exportText(tabs) {
  const data = { format: FILE_FORMAT, version: 1, tabs: tabs.map(({ name, plots, layout }) => ({ name, plots, layout })) };
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
