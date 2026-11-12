// The plot commands, each once: what it acts on, the key that runs it, how
// its menu row reads and what it does. A menu row (CommandItem.svelte), a
// key (keymap.js lists the keys from here), the command palette and a
// plot's own buttons all run a command through `run`, so each origin does
// the same thing to the same plots.
//
// An entry is {id, scope, kinds, needs, place, show, keys, ctrl, shift,
// keyless, what, gesture, label, icon, iconOf, active, activeIcon, when,
// value, disabled, destructive, choices, chosen, palette, empty, check,
// run}:
// - `scope` is 'view', 'plot', 'tab' or 'page': made from the page or the keys with no
//   plot selected, a 'view' command acts on every plot shown and a 'plot'
//   command on none (selection.svelte.js `pageTargets`). Made on a plot, from
//   its menu or its buttons, a command acts on the selection the plot is in,
//   or else on the plot alone (`targets`). A 'tab' command acts on no plot
//   but on a tab of the page: the one it is made on, from its menu, its
//   buttons or its keys, or else the open one. A 'page' command acts on
//   the page itself.
// - `kinds` lists the kinds of plot it acts on, or null for every kind; the
//   plots of other kinds are dropped from what it acts on.
// - `needs` lists what it needs beyond plots. Of where its plots are, which
//   not every PlotHost has (plothost.js; `svc.has`): 'workspace', a plot
//   placed in a tab's layout on the page, with the page's clipboard and
//   dialogs; 'live', a plot that can follow the newest samples. Where a
//   host has not what a command needs, the command does not run and has no
//   row. Of its origin: 'axis', an axis it is made on, a y axis by the key
//   its `how` names; 'line', a line of the log it is made on, by the moment
//   its `how` names;
//   'signals', the signals of the Signals list it puts on the plots or
//   takes off them. Made on a signal's row, from its menu, its keys or a
//   double-click, a command acts on the signals picked when the row is one
//   of them, or else on that signal alone (selection.svelte.js
//   `signalTargets`); the palette names the one signal. With no signal it
//   does not run.
// - `show`, `keys`, `ctrl`, `shift` and `what` are its keys as keymap.js
//   takes them; `place` 'page' puts them among the page's keys, 'list'
//   among the Signals list's and 'tabs' among a layout tab's, not a plot's.
//   Its row shows its keys, unless it is `keyless`. `gesture` is what a
//   pointer does that runs it too, as ['Ctrl', 'drag'], which its buttons'
//   tips show in place of keys.
// - `label`, `icon`, `value`, `disabled` and `destructive` are how its row
//   reads, and how it reads wherever it shows: a menu's row, a button,
//   whose tip and name are its label, and the command palette. `label` is
//   text or a function of the context, and a command with no label shows
//   nowhere; the icon is what `iconOf(ctx, how)` gives, where there is
//   one, or `activeIcon` while `active` holds, else `icon`. Neither reads
//   where the command is made from (`ctx.from`). `value` is null, text, or
//   {text, icon, warn, title, label}, `label` what an icon alone says.
//   `when`, where there is one, says whether a menu has the row at all; its
//   key and its buttons run the command either way. `disabled` greys its
//   row and its buttons, and keeps it out of the palette.
// - `choices(ctx, how)`, where there is one, lists the values it sets, as
//   [value, name]: its row opens a submenu of them, and picking one runs it
//   with that value as `how.to`. `chosen(ctx, how)` is the value the plot
//   it is made on has, which the submenu marks; without it the submenu
//   marks none. A row made with a value, `how.to`, is that choice's own:
//   a label of text reads the choice's name after it (`labelOf`).
// - The command palette lists every command that has a label and needs
//   nothing of where it is made (ORIGIN): no axis, no signal, no line. A command on
//   plots is listed while it has plots to act on, the selected ones; one of
//   choices once for each. `palette` is the heading it is listed under
//   (PALETTE), which every listed command has and no other.
// - `empty` true runs it with no plots to act on too.
// - `check(ctx)`, where there is one, asks, as a menu opens, what its row
//   then reads as `how`.
// - `run(ctx, how)` does it, and returns false where it did nothing that a
//   key should be taken for.
//
// Each of `label`, `value`, `disabled` and `run` takes the context `ctx`
// and `how`, what the origin adds: the axis a y axis command is made on,
// or the count a menu holds. `ctx` is what the command acts on, made by
// `context`: {plots, on, lead, signals, tab, from, svc}. `plots` is each
// plot it acts on, as {id, plot, view, pv}: its settings, its view and its
// PlotView, which is all a command reaches of a plot. `on` is the plot it
// was made on, or null; `lead` that plot, or else the one clicked last
// among `plots`, or the first. `signals` is the keys of the signals it acts
// on, none for a command that needs none. `tab` is the tab a 'tab' command
// acts on, null for any other. `from` is the origin: 'menu',
// 'button', 'key', 'palette', 'toolbar' or 'row', a double-click on a
// signal. `svc` is the page's services, as `commands` takes them.
import { common, counted } from './menus.js';
import { axisSlots, cursorText } from './plot.js';
import { placedIn, withX } from './cursors.js';
import { nearestPeak, peaks, stepPeak } from './snap.js';
import { APART, joined, yScaleField } from './tabs.js';
import { ARRANGEMENTS, CAPTURE_DELAY, CAPTURE_DELAYS, KINDS, MODES, isSlice, kindLabel, kindsFor } from './constants.js';
import { SHOT_COUNTS, SHOT_MODES, shotCount, shotMode } from './shots.js';
import { writeTables } from './copy.js';
import { plotIds } from './layout.js';
import { DIGITS, axisFormat, unitChoices } from './units.js';
import * as clock from './clock.js';
import RiAdd from '@lucide/svelte/icons/plus';
import RiTrash from '@lucide/svelte/icons/trash-2';
import RiNewPlot from '@lucide/svelte/icons/square-plus';
import RiSplitH from '@lucide/svelte/icons/columns-2';
import RiSplitV from '@lucide/svelte/icons/rows-2';
import RiFullscreen from '@lucide/svelte/icons/maximize-2';
import RiFullscreenExit from '@lucide/svelte/icons/minimize-2';
import RiClose from '@lucide/svelte/icons/x';
import RiLegend from '@lucide/svelte/icons/list';
import RiCopy from '@lucide/svelte/icons/clipboard-copy';
import RiExport from '@lucide/svelte/icons/file-down';
import RiImage from '@lucide/svelte/icons/image';
import RiReadout from '@lucide/svelte/icons/table-2';
import RiXCursors from '@lucide/svelte/icons/separator-vertical';
import RiYCursors from '@lucide/svelte/icons/separator-horizontal';
import RiEraser from '@lucide/svelte/icons/eraser';
import RiSpan from '@lucide/svelte/icons/ruler-dimension-line';
import RiPlay from '@lucide/svelte/icons/play';
import RiFit from '@lucide/svelte/icons/unfold-vertical';
import RiFitX from '@lucide/svelte/icons/unfold-horizontal';
import RiToLeft from '@lucide/svelte/icons/arrow-left';
import RiToRight from '@lucide/svelte/icons/arrow-right';
import RiPeak from '@lucide/svelte/icons/mountain';
import RiPeakRight from '@lucide/svelte/icons/chevron-right';
import RiPeakLeft from '@lucide/svelte/icons/chevron-left';
import RiLabel from '@lucide/svelte/icons/type';
import RiDigits from '@lucide/svelte/icons/hash';
import RiNotation from '@lucide/svelte/icons/superscript';
import RiUnit from '@lucide/svelte/icons/ruler';
import RiZoomTo from '@lucide/svelte/icons/zoom-in';
import RiAxis from '@lucide/svelte/icons/arrow-right-left';
import RiKind from '@lucide/svelte/icons/chart-line';
import RiLayout from '@lucide/svelte/icons/layers';
import RiTab from '@lucide/svelte/icons/app-window';
import RiRename from '@lucide/svelte/icons/pencil';
import RiDuplicate from '@lucide/svelte/icons/copy';
import RiDock from '@lucide/svelte/icons/panel-left';
import RiLog from '@lucide/svelte/icons/panel-bottom';
import RiLogHide from '@lucide/svelte/icons/panel-bottom-close';
import RiArrange from '@lucide/svelte/icons/layout-grid';
import RiRows from '@lucide/svelte/icons/rows-3';
import RiColumns from '@lucide/svelte/icons/columns-3';
import RiQuadrants from '@lucide/svelte/icons/grid-2x2';
import RiFocus from '@lucide/svelte/icons/layout-panel-left';
import RiCheck from '@lucide/svelte/icons/check';
import RiReset from '@lucide/svelte/icons/rotate-ccw';
import RiDownload from '@lucide/svelte/icons/download';
import RiUpload from '@lucide/svelte/icons/upload';
import RiTrigger from '@lucide/svelte/icons/zap';
import RiAuto from '@lucide/svelte/icons/repeat';
import RiDelay from '@lucide/svelte/icons/timer';
import RiShots from '@lucide/svelte/icons/copy-plus';
import RiCheckUpdates from '@lucide/svelte/icons/refresh-cw';
import RiFirmware from '@lucide/svelte/icons/hard-drive-upload';
import RiUpgradeAll from '@lucide/svelte/icons/circle-arrow-up';
import { deviceText, hasUpdate } from './firmware.js';
import RiCount from '@lucide/svelte/icons/list-ordered';

/** How far an arrow pans, as a share of the span, and zooms, as a factor. */
const PAN = 0.1, ZOOM = 1.25;

const ids = (ctx) => ctx.plots.map((t) => t.id);
const pvs = (ctx) => ctx.plots.map((t) => t.pv).filter(Boolean);
/** How many plots `ctx` acts on, as a row's value, when more than one. */
const plotsValue = (ctx) => (ctx.plots.length > 1 ? counted(ctx.plots.length, 'plot') : null);

/**
 * The PlotViews of `targets` and the key of each one's y axis in the units
 * of y axis `key` of PlotView `on`, as [pv, axis] (PlotView `axisIn`): the
 * axis of the same signal where it is one of them, else the first, as an
 * overlay plot's shared axis is. A plot with no axis in those units is left
 * out, and with no key, or no such axis on `on`, every plot is.
 */
export function axesLike(on, targets, key) {
  const units = key === undefined ? null : on?.axisUnits(key);
  if (units === null || units === undefined) return [];
  return targets.map((p) => [p, p.axisIn(units, key)]).filter(([, axis]) => axis !== null);
}

/**
 * What a cursor dragged on PlotView `on` moves, of the PlotViews `targets`
 * a gesture on it acts on, as [pv, axis]: an X cursor's, with no `key`, on
 * those of `on`'s kind; a Y cursor's, on its y axis `key`, on the axis in
 * its units of each of those that has one (`axesLike`). A plot of another
 * kind is left as it was.
 */
export function cursorPeers(on, targets, key) {
  const same = targets.filter((p) => p.config().kind === on.config().kind);
  return key === undefined ? same.map((p) => [p, undefined]) : axesLike(on, same, key);
}

/**
 * What an axis's settings hold (units.js `axisFormat`), by field: the name
 * of the edit that sets each, and how it names each value. A unit's values
 * are those the axis's units offer (units.js `unitChoices`), so `names` of
 * a unit takes the units and names Auto alone for a unit the page does not
 * know.
 */
export const AXIS_SETTINGS = {
  label: { what: 'Label', names: new Map([['off', 'Off'], ['units', 'Units'], ['name', 'Name and units']]) },
  digits: { what: 'Digits', names: new Map([[null, 'Auto'], ...DIGITS.map((d) => [d, String(d)])]) },
  notation: { what: 'Notation', names: new Map([['auto', 'Auto'], ['plain', 'Plain'], ['exp', 'Exponent']]) },
  unit: { what: 'Unit', names: (units) => new Map([[null, 'Auto'], ...unitChoices(units).map((c) => [c.e, c.text])]) },
};
/** How setting `field` names its values on an axis in `units`. */
const namesOf = (field, units) => (typeof AXIS_SETTINGS[field].names === 'function' ? AXIS_SETTINGS[field].names(units ?? '') : AXIS_SETTINGS[field].names);

/** An axis's settings at Auto. */
const AUTO = axisFormat(undefined);
/** Settings `s` of one axis with `field` set to `to`: those at Auto left out, and undefined when all are. */
function withSetting(s, field, to) {
  const out = { ...s, [field]: to };
  for (const [f, v] of Object.entries(out)) if (v === undefined || v === AUTO[f]) delete out[f];
  return Object.keys(out).length ? out : undefined;
}

/**
 * Sets `field` of the settings of the y axis of each plot of `ctx` in the
 * units of y axis `key` of the plot it is made on (`axesLike`) to `to`, as
 * one edit.
 */
function setYAxis(ctx, field, { key, to }) {
  const axes = new Map(axesLike(ctx.on?.pv, pvs(ctx), key).map(([p, axis]) => [p.plotId, axis]));
  const { what } = AXIS_SETTINGS[field], names = namesOf(field, ctx.on?.pv?.axisUnits(key));
  if (!axes.size || !names.has(to)) return false;
  ctx.svc.edit([...axes.keys()], `${what} ${names.get(to)}`, (plot) => {
    const all = { ...plot.yFormat, [axes.get(plot.id)]: withSetting(plot.yFormat?.[axes.get(plot.id)], field, to) };
    for (const [k, v] of Object.entries(all)) if (!v) delete all[k];
    return { yFormat: Object.keys(all).length ? all : undefined };
  });
}

/** Sets `field` of the x axis settings of each plot of `ctx`, of the lead plot's kind, to `to`, as one edit: a spectrum's frequencies, or a capture plot's x. */
function setXAxis(ctx, field, { to }) {
  const { what, names } = AXIS_SETTINGS[field], kind = ctx.lead?.plot.kind;
  if (!names.has(to)) return false;
  ctx.svc.edit(ids(ctx), `${kind === 'capture' ? 'X' : 'Frequency'} ${what.toLowerCase()} ${names.get(to)}`, (plot) => (plot.kind === kind ? { xFormat: withSetting(plot.xFormat, field, to) } : null));
}

/** How `field` is set on the axes a y axis command of `ctx` made on y axis `key` acts on, as a row's value: the name of the value they share, or Mixed. */
const yAxisValue = (ctx, field, key) => {
  const names = namesOf(field, ctx.on?.pv?.axisUnits(key));
  return common(axesLike(ctx.on?.pv, pvs(ctx), key).map(([p, axis]) => names.get(axisFormat(ctx.plots.find((t) => t.pv === p)?.plot.yFormat?.[axis])[field]) ?? names.get(null)));
};
/** How `field` is set on the x axes of the spectra of `ctx`, as a row's value. */
const xAxisValue = (ctx, field) => common(ctx.plots.map((t) => namesOf(field).get(axisFormat(t.plot.xFormat)[field])));

/** The distance between X1 and X2 on the PlotViews `targets` that have the pair, or null while none has. */
export function xSpan(targets) {
  return common(targets.flatMap((p) => {
    const { a, b } = p.markers();
    return a !== null && b !== null ? [cursorText(p.xKind()).span(b - a)] : [];
  }));
}

/** The distance between X1 and X2 on the plots of `ctx` that have the pair, after a ruler, as a row's value. */
function spanValue(ctx) {
  const d = xSpan(pvs(ctx));
  return d ? { text: d, icon: RiSpan } : null;
}

/** The first X cursor placed on the PlotViews `targets` that have one, named to tell it from its pair, or from the view's width. */
function xFirst(targets) {
  return common(targets.flatMap((p) => {
    const { a, b } = p.markers(), at = a ?? b;
    if (at === null) return [];
    const between = a !== null && b !== null ? Math.abs(b - a) : p.xd ? p.xd[1] - p.xd[0] : 0;
    return [`${a !== null ? 'X1' : 'X2'} ${cursorText(p.xKind()).at(at, between)}`];
  }));
}

/** Gives each PlotView of `targets` the cursors `fn(pv, axis)` returns, as one edit named `what`. */
function editCursors(ctx, what, targets, fn) {
  ctx.svc.cursors.set(what, new Map(targets.map(([p, axis]) => [p.plotId, fn(p, axis)])));
}

/** The cursors' readout of the lead plot of `ctx`, the one it is made on or else the one clicked last (PlotView `legend`), or null where it shows none. */
const readout = (ctx) => ctx.lead?.pv?.legend?.cursors ?? null;
/** Whether the lead plot of `ctx` has X1 and X2 placed, as its readout shows them. */
function xPair(ctx) {
  const x = readout(ctx)?.x;
  return !!x && x.a !== '—' && x.b !== '—';
}

/** Puts the cursors' readout of the lead plot of `ctx` on the clipboard, as its table shows it; with none, does nothing. */
function copyReadout(ctx) {
  const c = readout(ctx);
  if (!c) return false;
  const { plot, pv } = ctx.lead, rows = [];
  if (c.x) {
    rows.push([`X ${c.x.v}`, c.x.a, c.x.b, c.x.d, c.x.rate ? `1/Δt ${c.x.rate}` : '']);
    for (const item of plot.items) {
      const r = pv.legend.rows[item.key];
      rows.push([item.key, r?.a ?? '—', r?.b ?? '—', r?.d ?? '—', '']);
    }
  }
  for (const y of c.ys) rows.push([`Y ${y.name}`, y.a, y.b, y.d, y.ratio]);
  const blocks = [{ title: `Cursors · ${plot.items.map((i) => i.key).join(', ')}`, header: ['', '1', '2', 'Δ = 2 − 1', ''], rows }];
  writeTables(blocks).then(
    () => ctx.svc.toast.success('Copied the cursor readout'),
    (error) => ctx.svc.toast.error(`Could not copy: ${error.message}`),
  );
}

/** The page's tabs, as [id, name]. */
const tabNames = (ctx) => ctx.svc.ws.tabs.map((t) => [t.id, t.name]);
/** The page's docks, and their names. */
const DOCKS = [['left', 'Left'], ['right', 'Right']];
/** Each arrangement's picture. */
const ARRANGE_ICONS = { rows: RiRows, columns: RiColumns, grid: RiArrange, quadrants: RiQuadrants, focus: RiFocus };

/** How many plots tab `t` holds, as a row's value. */
const plotsIn = (t) => counted(plotIds(t.layout).length, 'plot');

/** The tabs of the page but the open one. */
const otherTabs = (ctx) => ctx.svc.ws.tabs.filter((t) => t.id !== ctx.svc.ws.activeId);

/** The name list `list` (KINDS, MODES, SIDES) gives value `v`. */
const nameIn = (list, v) => list.find((x) => x[0] === v)?.[1] ?? v;

/** Where an independent plot draws a signal's axis, and their names. */
const SIDES = [['left', 'Left'], ['right', 'Right'], ['none', 'None']];
/** What moving an axis to each side of SIDES reads as, and its picture. */
const SIDE_LABELS = { left: 'Move this axis to the left', right: 'Move this axis to the right', none: 'Remove this axis' };
const SIDE_ICONS = { left: RiToLeft, right: RiToRight, none: RiTrash };
/** Which side `plot`'s axis of signal `key` is on, of SIDES. */
function sideOf(plot, key) {
  const s = axisSlots(plot.items.filter((i) => !i.hidden).map((i) => i.key), plot.axisSides);
  return s.left.includes(key) ? 'left' : s.right.includes(key) ? 'right' : 'none';
}
/** Whether `plot` is an independent plot that shows signal `key`, which then has an axis to place. */
const sided = (plot, key) => plot.mode === 'independent' && plot.items.some((i) => i.key === key);

// The peak commands move an X cursor to a peak of the signals a plot shows,
// all of them merged, as the cursor reads every one (snap.js `peaks`): on a
// spectrum a peak stands 6 dB over the bins about it, on a time plot a share
// of the y range shown. They read the points the plot drew last.

/** How far inside the view's edge a peak past it comes to rest when the view moves to show it, as a share of the span. */
const PEAK_ROOM = 0.05;

/**
 * The signals PlotView `p` drew last, as snap.js reads them, each with what
 * its peaks stand out by: of a time plot with `whole`, every point the
 * source it drew from holds. None before it has drawn an x axis.
 */
function drawnSignals(p, whole) {
  const plans = p.drawn?.plans;
  if (!Array.isArray(plans) || !Array.isArray(p.xd)) return [];
  return p.snapSignals(whole && !p.slice ? plans.map((q) => ({ ...q, a0: q.src.absStart, a1: q.src.absEnd })) : plans).filter((s) => s.n > 0);
}

/** The peaks of all PlotView `p` holds, in order: a spectrum's among all its bins, a time plot's among all its sources hold. */
function peaksHeld(p) {
  const signals = drawnSignals(p, true);
  if (!signals.length) return [];
  return peaks(signals, [Math.min(...signals.map((s) => s.x(0))), Math.max(...signals.map((s) => s.x(s.n - 1)))]);
}

/**
 * Where X cursor `name` of PlotView `p` goes by a peak command: with `dir`
 * 0, to the peak of the view nearest it, or nearest the view's middle where
 * it is not placed; with 1 or -1, to the next peak right or left of it,
 * or from the view's far edge where it is not placed (snap.js `stepPeak`),
 * past the view's edge where the plot holds one. As {x, show}, `show` the
 * range the view moves to, as wide as it is, to show a peak past its edge,
 * else null; or null for no peak.
 */
export function peakMove(p, name, dir) {
  const view = p.xd, signals = drawnSignals(p, false);
  if (!signals.length) return null;
  const [to, from] = p.logX ? [Math.log10, (v) => 10 ** v] : [(v) => v, (v) => v];
  const at = p.markers()[name], inView = peaks(signals, view);
  if (!dir) {
    const x = nearestPeak(inView, at ?? from((to(view[0]) + to(view[1])) / 2), to);
    return x === null ? null : { x, show: null };
  }
  const x = stepPeak(inView, at ?? view[dir > 0 ? 0 : 1], dir, () => peaksHeld(p));
  if (x === null) return null;
  if (x >= view[0] && x <= view[1]) return { x, show: null };
  // Along the axis's own scale, just far enough to have the peak PEAK_ROOM inside the edge; a spectrum's within its bins.
  const span = to(view[1]) - to(view[0]), v = to(x);
  let lo = v > to(view[1]) ? v + PEAK_ROOM * span - span : v - PEAK_ROOM * span;
  if (p.slice && p.fExtent) lo = Math.max(to(p.fExtent[0]), Math.min(lo, to(p.fExtent[1]) - span));
  return { x, show: [from(lo), from(lo + span)] };
}

/** The X cursor a next-peak command moves on the plots of `ctx`: the one its lead plot placed or moved last (CursorEdits `lastX`). */
const lastX = (ctx) => (ctx.lead ? ctx.svc.cursors.lastX(ctx.lead.id) : 'a');

/** Where X cursor `name` goes by peak command `dir` on each plot of `ctx` of its lead plot's kind that has a peak to go to (`peakMove`), as [t, pv, move]. */
function peakMoves(ctx, name, dir) {
  const lead = ctx.lead?.pv;
  if (!lead) return [];
  return cursorPeers(lead, pvs(ctx)).flatMap(([p]) => {
    const move = peakMove(p, name, dir);
    return move ? [[ctx.plots.find((t) => t.pv === p), p, move]] : [];
  });
}

/** Where X cursor `name` would go on the plots of `ctx` by peak command `dir`, as a row's value, or else why it goes nowhere. */
function peakValue(ctx, name, dir) {
  const moves = peakMoves(ctx, name, dir);
  if (!moves.length) return dir ? `No peak to the ${dir > 0 ? 'right' : 'left'}` : 'No peak in view';
  const at = common(moves.map(([, p, m]) => cursorText(p.xKind()).at(m.x, p.xd[1] - p.xd[0])));
  return dir ? `${name === 'a' ? 'X1' : 'X2'} ${at}` : at;
}

/**
 * Moves X cursor `name` by peak command `dir` on each plot of `ctx` of its
 * lead plot's kind (`cursorPeers`), each to a peak of its own signals
 * (`peakMove`), where a drag would set them all to one value: one edit
 * named `what`, the time plots paused, as they hold the moment it marks, in
 * one view step that moves each view that shows a peak past its edge.
 */
function toPeak(ctx, name, dir, what) {
  const moves = peakMoves(ctx, name, dir);
  if (!moves.length) return false;
  ctx.svc.viewHistory.step(() => {
    const perf = clock.now();
    for (const [t, p, m] of moves) {
      if (p.config().kind === 'time') m.show ? t.view.show(...m.show, perf) : t.view.pause(perf);
      else if (m.show) ctx.svc.ws.setView(t.id, { fRange: m.show });
    }
    ctx.svc.cursors.set(what, new Map(moves.map(([, p, m]) => [p.plotId, withX(p.placed, p.config().kind, name, m.x)])));
  });
}

/** Has the plots of `ctx` follow the newest samples again, back at their windows with `restore` (ViewHistory `follow`). */
const follow = (ctx, restore) => ctx.svc.viewHistory.follow(ids(ctx), restore);

/** Opens the picker of keymap.js plot action `id` on the lead plot, and says whether it has one. */
const picker = (id) => (ctx) => ctx.svc.picker(ctx.lead.id, id);

/** The rows a copy holds, as Copy data says. */
const rowsText = (n) => (n === null ? 'Counting…' : `${n.toLocaleString()} row${n === 1 ? '' : 's'}`);

/** Moves the time views of `ctx` as one burst of view steps of `kind`, `fn(view, perf)` each. */
function burst(ctx, kind, fn) {
  const perf = clock.now(), views = ctx.plots.map((t) => t.view);
  ctx.svc.viewHistory.burst(kind, views, () => views.forEach((v) => fn(v, perf)));
}

const shows = (plot, key) => plot.items.some((i) => i.key === key);
/** The plots of `ctx` that lack any of its signals. */
const lacking = (ctx) => ctx.plots.filter((t) => ctx.signals.some((key) => !shows(t.plot, key)));
/** How many signals `ctx` acts on and how many of `plots`, as a row's value, each when more than one, `word` between them: "4 signals to 2 plots". */
const signalsValue = (ctx, plots, word) => [ctx.signals.length > 1 && counted(ctx.signals.length, 'signal'), plots.length > 1 && counted(plots.length, 'plot')].filter(Boolean).join(` ${word} `) || null;
/** The signals of `ctx` as an edit names them: the one signal, or how many. */
const signalsNamed = (ctx) => (ctx.signals.length > 1 ? counted(ctx.signals.length, 'signal') : (ctx.svc.titleOf?.(ctx.signals[0]) ?? ctx.signals[0]));

/** Whether a command of a capture plot's is made on one, where a menu has its row. */
const onCapture = (ctx) => ctx.on?.plot.kind === 'capture';
/** A delay between two captures as a row names it: "0.5 s". */
const delayName = (s) => `${s} s`;

/** Every plot command, the plot keys first in the order the keys sheet lists them. */
export const COMMANDS = [
  { id: 'addSignals', scope: 'plot', needs: ['workspace'], show: ['A'], keys: ['a', 'A', '+'], what: 'Add signals', run: picker('addSignals') },
  { id: 'enterSignals', scope: 'plot', needs: ['workspace'], show: ['Enter'], keys: ['Enter'], what: 'Into its signals', run: picker('enterSignals') },
  {
    id: 'splitRight', scope: 'plot', palette: 'Plots', needs: ['workspace'], show: ['|'], keys: ['|', '\\'], what: 'Split right',
    icon: RiSplitH, label: 'Split right',
    run: (ctx) => ctx.svc.split(ids(ctx), 'row', ctx.from === 'key'),
  },
  {
    id: 'splitDown', scope: 'plot', palette: 'Plots', needs: ['workspace'], show: ['_'], keys: ['-', '_'], what: 'Split down',
    icon: RiSplitV, label: 'Split down',
    run: (ctx) => ctx.svc.split(ids(ctx), 'col', ctx.from === 'key'),
  },
  {
    // It restores a maximized view whatever it acts on, as Escape does.
    id: 'maximize', scope: 'plot', palette: 'Plots', needs: ['workspace'], show: ['F'], keys: ['f', 'F'], what: 'Maximize, restore', empty: true,
    icon: RiFullscreen, activeIcon: RiFullscreenExit, active: (ctx) => !!ctx.svc.ws.maximized,
    label: (ctx) => (ctx.svc.ws.maximized ? 'Restore' : 'Maximize'),
    run: (ctx) => {
      if (ctx.svc.ws.maximized) ctx.svc.ws.restoreLayout();
      else if (ctx.plots.length) ctx.svc.ws.maximize(ids(ctx));
      else return false;
    },
  },
  { id: 'kind', scope: 'plot', needs: ['workspace'], show: ['K'], keys: ['k', 'K'], what: 'Kind and layout', run: picker('kind') },
  {
    id: 'pan', scope: 'view', kinds: ['time'], show: ['←', '→'], keys: ['ArrowLeft', 'ArrowRight'], what: 'Pan; with Shift, a window',
    run: (ctx, how) => burst(ctx, 'pan', (v, perf) => v.pan((how.shift ? 1 : PAN) * (how.back ? -1 : 1), perf)),
  },
  {
    id: 'zoom', scope: 'view', kinds: ['time'], show: ['↑', '↓'], keys: ['ArrowUp', 'ArrowDown'], what: 'Zoom time',
    run: (ctx, how) => burst(ctx, 'zoom', (v, perf) => v.zoom(how.back ? ZOOM : 1 / ZOOM, v.live ? 1 : 0.5, perf)),
  },
  {
    id: 'follow', scope: 'view', palette: 'Plots', needs: ['live'], show: ['End'], keys: ['End'], what: 'Follow live',
    icon: RiPlay, label: 'Follow live',
    run: (ctx) => follow(ctx, true),
  },
  {
    id: 'fitY', scope: 'view', palette: 'Plots', show: ['Y'], keys: ['y', 'Y'], what: 'Fit y',
    icon: RiFit, label: 'Fit y',
    run: (ctx) => ctx.svc.viewHistory.step(() => pvs(ctx).forEach((p) => p.autoscale())),
  },
  {
    // Shown or hidden on every plot it acts on, as the lead plot's turns.
    id: 'legend', scope: 'plot', palette: 'Plots', show: ['L'], keys: ['l', 'L'], what: 'Legend',
    icon: RiLegend, label: (ctx) => (ctx.lead?.plot.legend ? 'Hide the legend' : 'Show the legend'),
    run: (ctx) => {
      const on = !ctx.lead.plot.legend;
      ctx.svc.edit(ids(ctx), on ? 'Show the legend' : 'Hide the legend', () => ({ legend: on }));
    },
  },
  {
    id: 'close', scope: 'plot', palette: 'Plots', needs: ['workspace'], show: ['Delete'], keys: ['Delete', 'Backspace'], what: 'Close',
    icon: RiClose, label: 'Close plot', destructive: true,
    value: (ctx) => plotsValue(ctx) ?? (ctx.lead ? counted(ctx.lead.plot.items.length, 'signal') : null),
    run: (ctx) => ctx.svc.close(ids(ctx), ctx.from === 'key'),
  },
  {
    // Made on a plot it pauses while that plot follows live; from the page, while any it acts on does. Else they follow live. A capture plot shows a capture until the next it asks for, with its own commands.
    id: 'pause', scope: 'view', kinds: ['time', 'spectrum'], needs: ['live'], place: 'page', show: ['Space'], keys: [' '], what: 'Pause, or follow live', empty: true,
    run: (ctx) => {
      const live = ctx.on ? ctx.on.view.live : ctx.plots.some((t) => t.view.live);
      if (!live) return void follow(ctx, false);
      const perf = clock.now();
      ctx.svc.viewHistory.step(() => ctx.plots.forEach((t) => t.view.pause(perf)));
    },
  },
  {
    // One capture now of every capture the capture plots it acts on show.
    id: 'captureNow', scope: 'view', kinds: ['capture'], palette: 'Plots', show: ['T'], keys: ['t', 'T'], what: 'Trigger a capture',
    icon: RiTrigger, label: 'Trigger',
    when: onCapture,
    run: (ctx) => ctx.svc.trigger(ctx.plots.flatMap((t) => t.plot.items.map((i) => i.key))),
  },
  {
    // Captures again and again, or no more, on every capture plot it acts on, as the lead plot's turns: the page asks the hub for what its plots show (App.svelte).
    id: 'captureAuto', scope: 'view', kinds: ['capture'], palette: 'Plots', show: ['R'], keys: ['r', 'R'], what: 'Auto: capture again and again',
    icon: RiAuto, label: (ctx) => (ctx.lead?.plot.auto ? 'Stop Auto' : 'Auto'),
    when: onCapture,
    value: (ctx) => (ctx.lead ? delayName(ctx.lead.plot.autoDelay ?? CAPTURE_DELAY) : null),
    run: (ctx) => {
      const on = !ctx.lead.plot.auto;
      ctx.svc.edit(ids(ctx), on ? 'Auto capture' : 'Stop Auto capture', () => ({ auto: on }));
    },
  },
  {
    // Its row says first when a copy would hold too much or come back short, from the hub's count of the rows (`how`), and cannot be chosen while the count is 'counting'. Its key leaves text that is selected to the browser, to copy.
    id: 'copyData', scope: 'view', palette: 'Plots', needs: ['workspace'], place: 'page', show: ['Ctrl', 'C'], keys: ['c'], ctrl: true, shift: false, what: 'Copy data', empty: true,
    icon: RiCopy, label: 'Copy data',
    check: (ctx) => ctx.svc.copyCheck(ctx.plots),
    value: (ctx, check) => (check ? { text: rowsText(check.rows), warn: check.state === 'too-many' || check.state === 'partial', title: check.reason } : null),
    disabled: (ctx, check) => check?.state === 'too-many' || check?.state === 'counting',
    run: (ctx) => (ctx.from === 'key' && String(globalThis.getSelection?.() ?? '') ? false : void ctx.svc.copy(ctx.plots).catch((error) => ctx.svc.toast.error(`Could not copy: ${error.message}`))),
  },
  {
    // The picture of the plot it is made on, as the page shows it, with its legend: a PNG on the clipboard.
    id: 'copyImage', scope: 'plot', palette: 'Plots', needs: ['workspace'],
    icon: RiImage, label: 'Copy image',
    run: (ctx) => void ctx.svc.copyImage(ctx.lead.id).catch((error) => ctx.svc.toast.error(`Could not copy the image: ${error.message}`)),
  },
  {
    // Its data or its images, in the export dialog.
    id: 'exportData', scope: 'view', palette: 'Plots', needs: ['workspace'], empty: true,
    icon: RiExport, label: 'Export…',
    value: (ctx) => (ctx.on ? plotsValue(ctx) : ctx.svc.selection.plots.size ? `${ctx.plots.length} selected` : counted(ctx.plots.length, 'plot')),
    run: (ctx) => ctx.svc.openExport(ctx.plots),
  },
  {
    // A pair in view on each plot, the time plots paused, as they hold the moment it marks: one view step that holds the cursors it placed.
    id: 'addXCursors', scope: 'plot', palette: 'Plots',
    icon: RiXCursors, label: 'Add X cursors',
    value: spanValue,
    run: (ctx) =>
      ctx.svc.viewHistory.step(() => {
        pvs(ctx).forEach((p) => p.config().kind === 'time' && p.view.pause(clock.now()));
        editCursors(ctx, 'Add X cursors', pvs(ctx).map((p) => [p]), (p) => p.xCursorsAdded());
      }),
  },
  {
    // The nearest peak in view to X1, or to the view's middle, placing it (`toPeak`).
    id: 'x1ToPeak', scope: 'plot', palette: 'Plots', show: ['P'], keys: ['p', 'P'], shift: false, what: 'X1 to peak',
    icon: RiPeak, label: 'X1 to peak',
    value: (ctx) => peakValue(ctx, 'a', 0),
    disabled: (ctx) => !peakMoves(ctx, 'a', 0).length,
    run: (ctx) => toPeak(ctx, 'a', 0, 'X1 to peak'),
  },
  {
    id: 'x2ToPeak', scope: 'plot', palette: 'Plots', show: ['Shift', 'P'], keys: ['p', 'P'], shift: true, what: 'X2 to peak',
    icon: RiPeak, label: 'X2 to peak',
    value: (ctx) => peakValue(ctx, 'b', 0),
    disabled: (ctx) => !peakMoves(ctx, 'b', 0).length,
    run: (ctx) => toPeak(ctx, 'b', 0, 'X2 to peak'),
  },
  {
    // The X cursor placed or moved last to the next peak right, past the view's edge where the plot holds one (`toPeak`).
    id: 'peakRight', scope: 'plot', palette: 'Plots', show: ['.'], keys: ['.'], what: 'Next peak right',
    icon: RiPeakRight, label: 'Next peak right',
    value: (ctx) => peakValue(ctx, lastX(ctx), 1),
    disabled: (ctx) => !peakMoves(ctx, lastX(ctx), 1).length,
    run: (ctx) => toPeak(ctx, lastX(ctx), 1, 'Next peak right'),
  },
  {
    id: 'peakLeft', scope: 'plot', palette: 'Plots', show: [','], keys: [','], what: 'Next peak left',
    icon: RiPeakLeft, label: 'Next peak left',
    value: (ctx) => peakValue(ctx, lastX(ctx), -1),
    disabled: (ctx) => !peakMoves(ctx, lastX(ctx), -1).length,
    run: (ctx) => toPeak(ctx, lastX(ctx), -1, 'Next peak left'),
  },
  {
    // On the axis of each plot in the units of y axis `how` of the plot it is made on.
    id: 'addYCursors', scope: 'plot', needs: ['axis'],
    icon: RiYCursors, label: 'Add Y cursors',
    value: (ctx, key) => {
      const names = axesLike(ctx.on?.pv, pvs(ctx), key).map(([p, axis]) => (p.yAxes.length > 1 ? p.axisName(axis) : null));
      return names.some(Boolean) ? common(names) : null;
    },
    run: (ctx, key) => editCursors(ctx, 'Add Y cursors', axesLike(ctx.on?.pv, pvs(ctx), key), (p, axis) => p.yCursorsAdded(axis)),
  },
  // An axis's label, digits, notation and unit (units.js `axisFormat`): made on y
  // axis `how.key`, on the axis of each plot in its units (`axesLike`); made
  // on a spectrum's x axis, on each spectrum. `how.to` is the value each sets.
  {
    id: 'yLabel', scope: 'plot', needs: ['axis'],
    icon: RiLabel, label: 'Label',
    value: (ctx, how) => yAxisValue(ctx, 'label', how?.key),
    run: (ctx, how) => setYAxis(ctx, 'label', how ?? {}),
  },
  {
    id: 'yDigits', scope: 'plot', needs: ['axis'],
    icon: RiDigits, label: 'Digits',
    value: (ctx, how) => yAxisValue(ctx, 'digits', how?.key),
    run: (ctx, how) => setYAxis(ctx, 'digits', how ?? {}),
  },
  {
    id: 'yNotation', scope: 'plot', needs: ['axis'],
    icon: RiNotation, label: 'Notation',
    value: (ctx, how) => yAxisValue(ctx, 'notation', how?.key),
    run: (ctx, how) => setYAxis(ctx, 'notation', how ?? {}),
  },
  {
    // `how.to` the power of ten of the prefix, or null for Auto; on an axis in a unit the page does not know it sets nothing.
    id: 'yUnit', scope: 'plot', needs: ['axis'],
    icon: RiUnit, label: 'Unit',
    value: (ctx, how) => yAxisValue(ctx, 'unit', how?.key),
    run: (ctx, how) => setYAxis(ctx, 'unit', how ?? {}),
  },
  {
    id: 'xDigits', scope: 'plot', kinds: ['spectrum', 'capture'], needs: ['axis'],
    icon: RiDigits, label: 'Digits',
    value: (ctx) => xAxisValue(ctx, 'digits'),
    run: (ctx, how) => setXAxis(ctx, 'digits', how ?? {}),
  },
  {
    id: 'xNotation', scope: 'plot', kinds: ['spectrum', 'capture'], needs: ['axis'],
    icon: RiNotation, label: 'Notation',
    value: (ctx) => xAxisValue(ctx, 'notation'),
    run: (ctx, how) => setXAxis(ctx, 'notation', how ?? {}),
  },
  {
    id: 'clearCursors', scope: 'view', palette: 'Plots',
    icon: RiEraser, label: 'Clear cursors',
    value: (ctx) => {
      const n = pvs(ctx).reduce((n, p) => n + p.cursorsPlaced(), 0);
      return n ? `${n} placed` : null;
    },
    run: (ctx) => void ctx.svc.cursors.clear(ids(ctx)),
  },
  {
    // The cursors' readout shown or hidden on every plot it acts on, as the lead plot's turns; a menu has its row while the lead plot has cursors placed.
    id: 'readout', scope: 'plot', palette: 'Plots',
    icon: RiReadout, label: (ctx) => (ctx.lead?.plot.readout === false ? 'Show the readout' : 'Hide the readout'),
    when: (ctx) => !!ctx.lead && placedIn(ctx.lead.plot.cursors, ctx.lead.plot.kind) > 0,
    run: (ctx) => {
      const on = ctx.lead.plot.readout === false;
      ctx.svc.edit(ids(ctx), on ? 'Show the readout' : 'Hide the readout', () => ({ readout: on ? undefined : false }));
    },
  },
  {
    // A menu has its row on a plot with an X cursor placed.
    id: 'clearXCursors', scope: 'plot', palette: 'Plots',
    icon: RiEraser, label: 'Clear X cursors',
    when: (ctx) => !!readout(ctx)?.x,
    value: (ctx) => xFirst(pvs(ctx)),
    run: (ctx) => editCursors(ctx, 'Clear X cursors', pvs(ctx).map((p) => [p]), (p) => p.xCursorsCleared()),
  },
  {
    // Made on y axis `how.key` of a plot, on the axis of each plot in its units (`axesLike`); with no `how`, on every y axis of each plot. A menu has its row on a plot with a Y cursor placed.
    id: 'clearYCursors', scope: 'plot', palette: 'Plots',
    icon: RiEraser, label: 'Clear Y cursors',
    when: (ctx) => !!readout(ctx)?.ys.length,
    value: (ctx) => counted(pvs(ctx).reduce((n, p) => n + p.yCursorAxes(), 0), 'axis', 'axes'),
    run: (ctx, how) => editCursors(ctx, 'Clear Y cursors', how ? axesLike(ctx.on?.pv, pvs(ctx), how.key) : pvs(ctx).map((p) => [p]), (p, axis) => p.yCursorsCleared(axis)),
  },
  {
    // The cursors' readout of its lead plot, the one it is made on, and of no other. It cannot be chosen where that plot shows none.
    id: 'copyReadout', scope: 'plot', palette: 'Plots',
    icon: RiCopy, label: 'Copy table',
    value: (ctx) => {
      const c = readout(ctx);
      return c ? counted((c.x ? 1 + ctx.lead.plot.items.length : 0) + c.ys.length, 'row') : null;
    },
    disabled: (ctx) => !readout(ctx),
    run: copyReadout,
  },
  {
    // To the time or the frequencies between X1 and X2 of its lead plot, the one it is made on, on the plots a gesture on that plot moves, as one view step (PlotView `zoomToCursors`). It cannot be chosen until that plot has both.
    id: 'zoomToCursors', scope: 'plot', palette: 'Plots', gesture: ['Ctrl', 'drag'],
    icon: RiZoomTo, label: 'Zoom to X1–X2',
    value: spanValue,
    disabled: (ctx) => !xPair(ctx),
    run: (ctx) => (ctx.lead?.pv && xPair(ctx) ? void ctx.lead.pv.zoomToCursors() : false),
  },
  // What an axis's popover sets beside its settings. Made on y axis
  // `how.key`, or `how` itself, each acts on the axis of each plot in its
  // units (`axesLike`).
  {
    id: 'fitYAxis', scope: 'plot', needs: ['axis'], gesture: ['double-click'],
    icon: RiFit, label: 'Fit to the data',
    run: (ctx, key) => ctx.svc.viewHistory.step(() => axesLike(ctx.on?.pv, pvs(ctx), key).forEach(([p, axis]) => p.fit([axis]))),
  },
  {
    // From `how.lo` to `how.hi`. A log axis takes no bound at or below zero, and is left as it is.
    id: 'yRange', scope: 'plot', needs: ['axis'],
    run: (ctx, how) => {
      const { key, lo, hi } = how ?? {};
      ctx.svc.viewHistory.step(() => axesLike(ctx.on?.pv, pvs(ctx), key).forEach(([p, axis]) => !(p.logY && lo <= 0) && p.setYBounds([axis], lo, hi)));
    },
  },
  {
    // The y scale of a slice, a spectrum's density or a capture's values, `how.to` 'log' or 'linear', on the plots with an axis in the units of y axis `how.key`, each in the field that holds its own (tabs.js `yScaleField`).
    id: 'yScale', scope: 'plot', needs: ['axis'],
    run: (ctx, how) => {
      const { key, to } = how ?? {}, has = new Set(axesLike(ctx.on?.pv, pvs(ctx), key).map(([p]) => p.plotId));
      if (to !== 'log' && to !== 'linear') return false;
      const what = ctx.on?.plot.kind === 'capture' ? 'y' : 'density';
      ctx.svc.edit(ids(ctx), `${to === 'log' ? 'Log' : 'Linear'} ${what} scale`, (plot) => (has.has(plot.id) && isSlice(plot.kind) ? { [yScaleField(plot)]: to } : null));
    },
  },
  {
    // The frequency scale of the spectra, `how.to` 'log' or 'linear'.
    id: 'xScale', scope: 'plot', needs: ['axis'],
    run: (ctx, how) => ctx.svc.edit(ids(ctx), how?.to === 'log' ? 'Log frequency scale' : 'Linear frequency scale', (plot) => (plot.kind === 'spectrum' ? { xScale: how?.to } : null)),
  },
  {
    // The stretch of x the slices of the lead plot's kind show, a spectrum's frequencies or a capture plot's x, `how.range` as [low, high], or null for all of it, as one view step. Its button shows all of it.
    id: 'fRange', scope: 'plot', kinds: ['spectrum', 'capture'], needs: ['axis'],
    icon: RiFitX, label: (ctx) => (ctx.lead?.plot.kind === 'capture' ? 'Show the whole capture' : 'Show every frequency'),
    run: (ctx, how) => ctx.svc.viewHistory.step(() => ctx.plots.forEach((t) => t.plot.kind === ctx.lead.plot.kind && ctx.svc.ws.setView(t.id, { fRange: how?.range ?? null }))),
  },
  {
    // Signal `how.key`'s axis on side `how.to` of SIDES, hidden with 'none', on each independent plot that shows the signal. A menu has its row on an independent plot. A row or a button made with a side moves the axis there.
    id: 'axisSide', scope: 'plot', needs: ['axis'],
    icon: RiAxis, iconOf: (ctx, how) => SIDE_ICONS[how?.to],
    label: (ctx, how) => SIDE_LABELS[how?.to] ?? 'Axis',
    when: (ctx) => ctx.on?.plot.mode === 'independent',
    value: (ctx, how) => common(ctx.plots.filter((t) => sided(t.plot, how?.key)).map((t) => nameIn(SIDES, sideOf(t.plot, how?.key)))),
    choices: () => SIDES,
    chosen: (ctx, how) => (ctx.on ? sideOf(ctx.on.plot, how?.key) : undefined),
    run: (ctx, how) => {
      const { key, to } = how ?? {};
      if (!SIDES.some(([side]) => side === to)) return false;
      const named = ctx.svc.titleOf?.(key) ?? key;
      ctx.svc.edit(ids(ctx), to === 'none' ? `Hide ${named}'s axis` : `${named}'s axis on the ${to}`, (plot) => (sided(plot, key) ? { axisSides: { ...plot.axisSides, [key]: to } } : null));
    },
  },
  {
    // A window of `how.s` seconds: one typed, with `how.typed`, is a step of its own, the field's arrows and wheel one burst. It sets the view of each plot it acts on of the kind of the plot it is made on, from the page of the time plots.
    id: 'window', scope: 'view',
    run: (ctx, how) => {
      const kind = ctx.on?.plot.kind ?? 'time', views = ctx.plots.filter((t) => t.plot.kind === kind).map((t) => t.view);
      const set = () => views.forEach((v) => v.setSpan(how?.s));
      if (how?.typed) ctx.svc.viewHistory.step(set);
      else ctx.svc.viewHistory.burst('window', views, set);
    },
  },
  {
    id: 'setKind', scope: 'plot', palette: 'Plots',
    icon: RiKind, label: 'Kind',
    value: (ctx) => common(ctx.plots.map((t) => nameIn(KINDS, t.plot.kind))),
    // The kinds the lead plot may be, which each plot it acts on becomes where it may (constants.js `kindsFor`).
    choices: (ctx) => kindsFor(ctx.lead?.plot.kind ?? 'time'),
    chosen: (ctx) => ctx.lead?.plot.kind,
    run: (ctx, how) => (KINDS.some(([kind]) => kind === how?.to) ? ctx.svc.edit(ids(ctx), `Kind ${nameIn(KINDS, how.to)}`, (plot) => (kindsFor(plot.kind).some(([kind]) => kind === how.to) ? { kind: how.to } : null)) : false),
  },
  {
    id: 'setLayout', scope: 'plot', palette: 'Plots',
    icon: RiLayout, label: 'Layout',
    value: (ctx) => common(ctx.plots.map((t) => nameIn(MODES, t.plot.mode))),
    choices: () => MODES,
    chosen: (ctx) => ctx.lead?.plot.mode,
    run: (ctx, how) => (MODES.some(([mode]) => mode === how?.to) ? ctx.svc.edit(ids(ctx), `Layout ${nameIn(MODES, how.to)}`, () => ({ mode: how.to })) : false),
  },
  // A capture plot's settings: how long Auto waits after each capture, and
  // what it shows of the shots it has (shots.js), `how.to` the value each sets.
  {
    id: 'captureDelay', scope: 'plot', kinds: ['capture'], palette: 'Plots',
    icon: RiDelay, label: 'Auto delay',
    when: onCapture,
    value: (ctx) => common(ctx.plots.map((t) => delayName(t.plot.autoDelay ?? CAPTURE_DELAY))),
    choices: () => CAPTURE_DELAYS.map((s) => [s, delayName(s)]),
    chosen: (ctx) => ctx.lead?.plot.autoDelay ?? CAPTURE_DELAY,
    run: (ctx, how) => (CAPTURE_DELAYS.includes(how?.to) ? ctx.svc.edit(ids(ctx), `Auto delay ${delayName(how.to)}`, () => ({ autoDelay: how.to })) : false),
  },
  {
    id: 'captureShots', scope: 'plot', kinds: ['capture'], palette: 'Plots',
    icon: RiShots, label: 'Shots',
    when: onCapture,
    value: (ctx) => common(ctx.plots.map((t) => nameIn(SHOT_MODES, shotMode(t.plot.shots)))),
    choices: () => SHOT_MODES,
    chosen: (ctx) => shotMode(ctx.lead?.plot.shots),
    run: (ctx, how) => (SHOT_MODES.some(([mode]) => mode === how?.to) ? ctx.svc.edit(ids(ctx), `Shots ${nameIn(SHOT_MODES, how.to)}`, () => ({ shots: how.to })) : false),
  },
  {
    // How many shots an overlay or an average takes. A menu has its row while the lead plot shows either.
    id: 'captureCount', scope: 'plot', kinds: ['capture'], palette: 'Plots',
    icon: RiCount, label: 'Shot count',
    when: (ctx) => onCapture(ctx) && shotMode(ctx.on.plot.shots) !== 'latest',
    value: (ctx) => common(ctx.plots.map((t) => String(shotCount(t.plot.shotCount)))),
    choices: () => SHOT_COUNTS.map((n) => [n, String(n)]),
    chosen: (ctx) => shotCount(ctx.lead?.plot.shotCount),
    run: (ctx, how) => (SHOT_COUNTS.includes(how?.to) ? ctx.svc.edit(ids(ctx), `Shot count ${how.to}`, () => ({ shotCount: how.to })) : false),
  },
  {
    // To the end of tab `how.to`, as one edit. Its row names the open tab, and cannot be chosen while that is the only one.
    id: 'moveToTab', scope: 'plot', palette: 'Plots', needs: ['workspace'],
    icon: RiTab, label: 'Move to tab',
    value: (ctx) => ctx.svc.ws.tab.name || null,
    disabled: (ctx) => !otherTabs(ctx).length,
    choices: (ctx) => otherTabs(ctx).map((t) => [t.id, t.name]),
    run: (ctx, how) => {
      const to = otherTabs(ctx).find((t) => t.id === how?.to);
      if (!to) return false;
      ctx.svc.group(`Move ${counted(ctx.plots.length, 'plot')} to ${to.name}`, () => ctx.plots.forEach((t) => ctx.svc.ws.movePlotToTab(t.id, to.id)));
    },
  },
  {
    id: 'clearSignals', scope: 'plot', palette: 'Plots',
    icon: RiTrash, label: 'Remove every signal', destructive: true,
    value: (ctx) => counted(ctx.plots.reduce((n, t) => n + t.plot.items.length, 0), 'signal'),
    run: (ctx) => ctx.svc.edit(ids(ctx), 'Remove every signal', () => ({ items: [] })),
  },
  {
    // Each selected plot gets those of the signals it lacks and can show (tabs.js `joined`), and where none can show any, a notice says why. A menu has its row unless every selected plot shows them all.
    id: 'addToPlots', scope: 'plot', needs: ['signals'], place: 'list', show: ['Enter'], keys: ['Enter'], shift: false, what: 'Add to the selected plots',
    icon: RiAdd, label: 'Add to plot',
    when: (ctx) => !ctx.plots.length || lacking(ctx).length > 0,
    value: (ctx) => (ctx.plots.length ? signalsValue(ctx, lacking(ctx), 'to') : 'No plot selected'),
    disabled: (ctx) => !ctx.plots.length,
    run: (ctx) => {
      if (lacking(ctx).length && !ctx.plots.some((t) => joined(t.plot, ctx.signals))) return void ctx.svc.toast.warning(APART);
      ctx.svc.edit(ids(ctx), `Add ${signalsNamed(ctx)}`, (plot) => joined(plot, ctx.signals));
    },
  },
  {
    // Each selected plot loses those of the signals it shows. A menu has its row, in place of Add to plot, where every selected plot shows them all.
    id: 'removeFromPlots', scope: 'plot', needs: ['signals'], place: 'list', show: ['Delete'], keys: ['Delete', 'Backspace'], what: 'Remove from the selected plots',
    icon: RiTrash, label: 'Remove from plot', destructive: true,
    when: (ctx) => ctx.plots.length > 0 && !lacking(ctx).length,
    value: (ctx) => signalsValue(ctx, ctx.plots, 'from'),
    run: (ctx) => {
      const having = ctx.plots.filter((t) => ctx.signals.some((key) => shows(t.plot, key)));
      ctx.svc.edit(having.map((t) => t.id), `Remove ${signalsNamed(ctx)}`, (plot) => ({ items: plot.items.filter((i) => !ctx.signals.includes(i.key)) }));
    },
  },
  {
    // A time plot of the signals, whatever is selected.
    id: 'newPlot', scope: 'plot', needs: ['workspace', 'signals'], place: 'list', show: ['Shift', 'Enter'], keys: ['Enter'], shift: true, what: 'New plot with them', empty: true,
    icon: RiNewPlot, label: 'New plot',
    value: (ctx) => (ctx.signals.length > 1 ? counted(ctx.signals.length, 'signal') : kindLabel('time', 'overlay')),
    run: (ctx) => ctx.svc.newPlot(ctx.signals),
  },
  // A layout tab's commands.
  {
    // Opens the tab's name for editing.
    id: 'renameTab', scope: 'tab', needs: ['workspace'], palette: 'Tabs', place: 'tabs', show: ['F2'], keys: ['F2'], what: 'Rename tab', empty: true,
    icon: RiRename, label: 'Rename',
    run: (ctx) => void ctx.svc.renameTab(ctx.tab.id),
  },
  {
    // Opens tab `how.to`: the palette's row for each tab.
    id: 'openTab', scope: 'page', needs: ['workspace'], palette: 'Tabs', empty: true,
    icon: RiTab, label: (ctx, how) => `Go to ${nameIn(tabNames(ctx), how?.to)}`,
    choices: tabNames,
    run: (ctx, how) => void ctx.svc.ws.selectTab(how?.to),
  },
  {
    // An empty tab after the others, which it opens.
    id: 'newTab', scope: 'tab', needs: ['workspace'], palette: 'Tabs', empty: true,
    icon: RiAdd, label: 'New layout',
    run: (ctx) => ctx.svc.group('New tab', () => ctx.svc.ws.addTab()),
  },
  {
    id: 'duplicateTab', scope: 'tab', needs: ['workspace'], palette: 'Tabs', empty: true,
    icon: RiDuplicate, label: 'Duplicate',
    value: (ctx) => plotsIn(ctx.tab),
    run: (ctx) => ctx.svc.group('Duplicate tab', () => ctx.svc.ws.duplicateTab(ctx.tab)),
  },
  {
    // The page asks first when the tab has plots.
    id: 'closeTab', scope: 'tab', needs: ['workspace'], palette: 'Tabs', place: 'tabs', show: ['Delete'], keys: ['Delete'], keyless: true, what: 'close tab', empty: true,
    icon: RiClose, label: 'Close', destructive: true,
    value: (ctx) => plotsIn(ctx.tab),
    run: (ctx) => void ctx.svc.closeTab(ctx.tab),
  },
  // The page's own commands.
  {
    // The log drawer under the plots, opened on every line, or closed while it is open.
    id: 'showLog', scope: 'page', needs: ['workspace'], palette: 'Docks', place: 'page', show: ['`'], keys: ['`'], what: 'Log', empty: true,
    icon: RiLog, active: (ctx) => ctx.svc.logShown(), activeIcon: RiLogHide,
    label: (ctx) => (ctx.svc.logShown() ? 'Hide the log' : 'Show the log'),
    run: (ctx) => void ctx.svc.toggleLog(),
  },
  {
    // X1 at time `how.t`, on its samples' clock, on every time plot of the open tab, as a line of the log places it:
    // one view step, each plot paused, as it holds the moment the cursor marks, and moved, as wide as it is, to centre a moment it does not show.
    id: 'x1At', scope: 'page', needs: ['workspace', 'line'], empty: true,
    icon: RiXCursors, label: 'Show on plots',
    run: (ctx, how) => {
      const plots = ctx.svc.ws.timeIds().map(ctx.svc.plotOf).filter((t) => t.pv?.xd);
      if (!plots.length || !Number.isFinite(how?.t)) return false;
      ctx.svc.viewHistory.step(() => {
        const perf = clock.now();
        for (const { view, pv } of plots) {
          const [lo, hi] = pv.xd;
          if (how.t >= lo && how.t <= hi) view.pause(perf);
          else view.show(how.t - (hi - lo) / 2, how.t + (hi - lo) / 2, perf);
        }
        ctx.svc.cursors.set('X1 at a log line', new Map(plots.map(({ id, pv }) => [id, withX(pv.placed, 'time', 'a', how.t)])));
      });
    },
  },
  {
    // Hides dock `how.to`, 'left' or 'right', or shows it while it is hidden: the palette's row for each.
    id: 'dock', scope: 'page', needs: ['workspace'], palette: 'Docks', empty: true,
    icon: RiDock, label: (ctx, how) => `${ctx.svc.dockShown(how?.to) ? 'Hide' : 'Show'} the ${how?.to} dock`,
    choices: () => DOCKS,
    run: (ctx, how) => void ctx.svc.toggleDock(how?.to),
  },
  {
    // Lays the open tab's plots out as `how.to` of ARRANGEMENTS, as one edit. Its row has that arrangement's picture, and a check while the tab is laid out so.
    id: 'arrange', scope: 'page', needs: ['workspace'], palette: 'Arrange', empty: true,
    icon: RiArrange, iconOf: (ctx, how) => ARRANGE_ICONS[how?.to],
    label: (ctx, how) => nameIn(ARRANGEMENTS, how?.to),
    value: (ctx, how) => (how?.to === ctx.svc.ws.arrangement() ? { text: '', icon: RiCheck, label: 'The layout now' } : null),
    choices: () => ARRANGEMENTS,
    run: (ctx, how) => (ARRANGEMENTS.some(([name]) => name === how?.to) ? ctx.svc.group(`Arrange as ${nameIn(ARRANGEMENTS, how.to)}`, () => ctx.svc.ws.arrange(how.to)) : false),
  },
  {
    id: 'resetLayout', scope: 'page', needs: ['workspace'], palette: 'Arrange', empty: true,
    icon: RiReset, label: 'Reset to default',
    run: (ctx) => ctx.svc.group('Reset', () => ctx.svc.ws.reset()),
  },
  {
    // The tabs as a file of layouts, to share.
    id: 'exportTabs', scope: 'page', needs: ['workspace'], palette: 'View', empty: true,
    icon: RiDownload, label: 'Export every tab…',
    value: (ctx) => counted(ctx.svc.ws.tabs.length, 'tab'),
    run: (ctx) => void ctx.svc.exportTabs(),
  },
  {
    id: 'importTabs', scope: 'page', needs: ['workspace'], palette: 'View', empty: true,
    icon: RiUpload, label: 'Import tabs…',
    run: (ctx) => void ctx.svc.importTabs(),
  },
  // The devices' firmware.
  {
    // Has the hub look every device up in the catalog of published firmware, which it does only when asked.
    id: 'checkFirmware', scope: 'page', needs: ['workspace'], palette: 'Devices', empty: true,
    icon: RiCheckUpdates, label: 'Check for firmware updates',
    disabled: (ctx) => !ctx.svc.devices().some((d) => d.fw?.installed),
    run: (ctx) => void ctx.svc.checkFirmware(),
  },
  {
    // Opens the upgrade dialog of the device at route `how.to`: a row for each device.
    id: 'upgradeFirmware', scope: 'page', needs: ['workspace'], palette: 'Devices', empty: true,
    icon: RiFirmware, label: 'Upgrade firmware…',
    choices: (ctx) => ctx.svc.devices().map((d) => [d.route, d.name ? `${d.route} ${deviceText(d)}` : d.route]),
    disabled: (ctx) => !ctx.svc.devices().length,
    run: (ctx, how) => void ctx.svc.openUpgrade(how?.to),
  },
  {
    // Offers to upgrade every device a newer release is published for, one after another.
    id: 'upgradeAll', scope: 'page', needs: ['workspace'], palette: 'Devices', empty: true,
    icon: RiUpgradeAll, label: (ctx) => `Upgrade all (${ctx.svc.devices().filter(hasUpdate).length})`,
    disabled: (ctx) => !ctx.svc.devices().some(hasUpdate),
    run: (ctx) => void ctx.svc.openUpgrade(null),
  },
];

/** The command `id`. */
export const commandOf = (id) => COMMANDS.find((c) => c.id === id);
/** What `label` field `f` of a command reads in `ctx`. */
const read = (f, ctx, how) => (typeof f === 'function' ? f(ctx, how) : f);
/**
 * What command `c` reads as in `ctx` made with `how`, wherever it shows: its
 * label, and for a row of one of its choices, `how.to`, of a label of text,
 * the choice's name after it.
 */
function labelOf(c, ctx, how) {
  const label = read(c.label, ctx, how);
  if (typeof c.label === 'function' || how?.to === undefined || !c.choices) return label;
  return `${label} › ${nameIn(c.choices(ctx, how), how.to)}`;
}
/** The headings the command palette lists commands under (`palette`), in its order. */
export const PALETTE = ['Tabs', 'Docks', 'Arrange', 'Plots', 'View', 'Devices'];
/** The scopes of the commands that act on no plot. */
export const PLOTLESS = ['tab', 'page'];
/** What a command may need of where its plots are (`needs`). */
export const HOSTED = ['workspace', 'live'];
/** What a command may need of where it is made (`needs`): a command that needs one is not in the palette. */
export const ORIGIN = ['axis', 'signals', 'line'];
/** Whether the command palette lists command `c`: it shows somewhere, by its label, and needs nothing of where it is made. */
export const listed = (c) => !!c.label && !c.needs?.some((n) => ORIGIN.includes(n));

/**
 * The commands of a PlotHost (plothost.js) over `svc`, its services:
 * `has(need)`, whether its plots have `need` of HOSTED; `targets(id)` and
 * `pageTargets(scope)`, the plots a command made on plot `id` or from the
 * page acts on, by id; `plotOf(id)`, plot `id` as {id, plot, view, pv};
 * `signalTargets(key)`, the signals a command made on signal `key`'s row,
 * or from the page with null, acts on, by key; `last()`, the plot clicked
 * last; and what the commands write through: `ws`, `selection`,
 * `viewHistory`, `cursors`, `toast`, `edit(ids, what, fn)`, `group(what,
 * fn)`, which runs `fn` as one edit named `what`, `split(ids, dir,
 * fromKeys)`, `close(ids, fromKeys)`, `newPlot(keys)`, `trigger(keys)`,
 * which asks the hub for one capture of each capture of `keys`,
 * `titleOf(key)`, what the page names the signal of `key`, `copy(plots)`,
 * `copyCheck(plots)`, `copyImage(id)`, `openExport(plots)`, `picker(id,
 * action)`, `renameTab(id)`, `closeTab(tab)`, `dockShown(side)`,
 * `toggleDock(side)`, `logShown()`, `toggleLog()`, `exportTabs()`,
 * `importTabs()`, `devices()`, the hub's devices in the order of their
 * routes, `checkFirmware()` and `openUpgrade(route)`, which opens the
 * upgrade dialog of the device at `route`, or with null of every device
 * that has an update.
 *
 * Where a command is made is {on, signal, signals, tab, from}: `on` the
 * plot it is made on, or null for the page; `signal` the signal whose row
 * it is made on, or null, or else `signals`, the very signals it acts on;
 * `tab` the tab it is made on, by id, or null; and `from`, its origin.
 */
export function commands(svc) {
  /** What command `id`, made at `where`, acts on: its context. */
  function context(id, { on = null, signal = null, signals, tab = null, from = 'menu' } = {}) {
    const c = commandOf(id);
    if (PLOTLESS.includes(c.scope)) return { plots: [], all: [], on: null, lead: null, signals: [], tab: c.scope === 'tab' ? (svc.ws.tabs.find((t) => t.id === tab) ?? svc.ws.tab) : null, from, svc };
    const all = (on !== null ? svc.targets(on) : svc.pageTargets(c.scope)).map(svc.plotOf).filter((t) => t.plot);
    const plots = c.kinds ? all.filter((t) => c.kinds.includes(t.plot.kind)) : all;
    const made = on !== null ? svc.plotOf(on) : null;
    const lead = made ?? plots.find((t) => t.id === svc.last()) ?? plots[0] ?? null;
    return { plots, all, on: made, lead, signals: c.needs?.includes('signals') ? (signals ?? svc.signalTargets(signal)) : [], tab: null, from, svc };
  }
  /** Whether the host has what command `c` needs of it. */
  const hosted = (c) => !c.needs?.some((n) => HOSTED.includes(n) && !svc.has(n));
  return {
    context,
    /**
     * Runs command `id`, made at `where`, with `how`. Says whether it was
     * taken: the host has what it needs, it had plots to act on, of any
     * kind, or runs with none, had the signals it needs, and did not
     * decline. It runs on the plots of its kinds.
     */
    run(id, { how, ...where } = {}) {
      const c = commandOf(id);
      if (!hosted(c)) return false;
      const ctx = context(id, where);
      if (c.needs?.includes('signals') && !ctx.signals.length) return false;
      if (!ctx.all.length && !c.empty) return false;
      if (!ctx.plots.length && !c.empty) return true;
      return c.run(ctx, how) !== false;
    },
    /** How command `id`'s row reads, made at `where` with `how`: {label, icon, value, disabled, destructive, show, gesture, choices, chosen, offered}, `choices` null for a row with no submenu, and `offered` whether a menu has the row. */
    row(id, { how, ...where } = {}) {
      const c = commandOf(id), ctx = context(id, where);
      return {
        label: labelOf(c, ctx, how),
        icon: c.iconOf?.(ctx, how) ?? (c.active?.(ctx, how) ? c.activeIcon : c.icon),
        value: c.value?.(ctx, how) ?? null,
        disabled: !!c.disabled?.(ctx, how),
        destructive: !!c.destructive,
        show: c.keyless ? null : (c.show ?? null),
        gesture: c.gesture ?? null,
        choices: how?.to === undefined ? (c.choices?.(ctx, how) ?? null) : null,
        chosen: c.chosen?.(ctx, how),
        offered: hosted(c) && (!c.when || !!c.when(ctx, how)),
      };
    },
    /** What command `id`'s `check` answers, made on plot `on`, or null for a command with none, or one the host has not what it needs. */
    check(id, { on = null } = {}) {
      const c = commandOf(id);
      return c.check && hosted(c) ? c.check(context(id, { on })) : Promise.resolve(null);
    },
    /**
     * The palette's rows now, in the table's order, as {id, how, label,
     * find, group}: `how` what the row runs its command with, `label` what
     * the command reads as (`labelOf`), `group` its heading, and `find`
     * the words the palette matches what is typed against: the heading, the
     * label and the id. A command it lists (`listed`) has a row for each of
     * its choices, or one, while it can be chosen: a command on plots while
     * it has plots to act on now, or runs with none and is on, as Restore is
     * while plots are maximized; a command of the page or a tab always.
     */
    offered() {
      return COMMANDS.filter((c) => listed(c) && hosted(c)).flatMap((c) => {
        const ctx = context(c.id, { from: 'palette' });
        if (!PLOTLESS.includes(c.scope) && !(ctx.plots.length || (c.empty && c.active?.(ctx)))) return [];
        if (c.disabled?.(ctx)) return [];
        return (c.choices?.(ctx) ?? [[undefined]]).map(([to]) => {
          const how = to === undefined ? undefined : { to }, label = labelOf(c, ctx, how);
          return { id: c.id, how, label, find: `${c.palette.toLowerCase()} ${label} ${c.id}`, group: c.palette };
        });
      });
    },
  };
}
