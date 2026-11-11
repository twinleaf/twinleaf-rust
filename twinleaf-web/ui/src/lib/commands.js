// The plot commands, each once: what it acts on, the key that runs it, how
// its menu row reads and what it does. A menu row (CommandItem.svelte), a
// key (keymap.js lists the keys from here), the command palette and a
// plot's own buttons all run a command through `run`, so each origin does
// the same thing to the same plots.
//
// An entry is {id, scope, kinds, needs, place, show, keys, shift, what,
// label, icon, active, activeIcon, when, value, disabled, destructive,
// empty, check, run}:
// - `scope` is 'view' or 'plot': made from the page or the keys with no
//   plot selected, a 'view' command acts on every plot shown and a 'plot'
//   command on none (selection.svelte.js `pageTargets`). Made on a plot, from
//   its menu or its buttons, a command acts on the selection the plot is in,
//   or else on the plot alone (`targets`).
// - `kinds` lists the kinds of plot it acts on, or null for every kind; the
//   plots of other kinds are dropped from what it acts on.
// - `needs` lists what it needs beyond plots: 'workspace', a plot placed in
//   a tab's layout; 'axis', a y axis it is made on, which its `how` names;
//   'signals', the signals of the Signals list it puts on the plots or
//   takes off them. Made on a signal's row, from its menu, its keys or a
//   double-click, a command acts on the signals picked when the row is one
//   of them, or else on that signal alone (selection.svelte.js
//   `signalTargets`); the palette names the one signal. With no signal it
//   does not run.
// - `show`, `keys`, `shift` and `what` are its keys as keymap.js takes
//   them; `place` 'page' puts them among the page's keys and 'list' among
//   the Signals list's, not a plot's.
// - `label`, `icon`, `value`, `disabled` and `destructive` are how its row
//   reads: `label` is text or a function of the context, and a command with
//   no label has no row and is not in the palette; the icon is
//   `activeIcon` while `active` holds, else `icon`. `value` is null, text,
//   or {text, icon, warn, title}. `when`, where there is one, says whether
//   a menu has the row at all; its key runs the command either way.
// - `empty` true runs it with no plots to act on too.
// - `check(ctx)`, where there is one, asks, as a menu opens, what its row
//   then reads as `how`.
// - `run(ctx, how)` does it, and returns false where it did nothing that a
//   key should be taken for.
//
// Each of `label`, `value`, `disabled` and `run` takes the context `ctx`
// and `how`, what the origin adds: the axis a y axis command is made on,
// or the count a menu holds. `ctx` is what the command acts on, made by
// `context`: {plots, on, lead, signals, from, svc}. `plots` is each plot it
// acts on, as {id, plot, view, pv}: its settings, its view and its
// PlotView, which is all a command reaches of a plot. `on` is the plot it
// was made on, or null; `lead` that plot, or else the one clicked last
// among `plots`, or the first. `signals` is the keys of the signals it acts
// on, none for a command that needs none. `from` is the origin: 'menu',
// 'button', 'key', 'palette', 'toolbar' or 'row', a double-click on a
// signal. `svc` is the page's services, as `commands` takes them.
import { common, counted } from './menus.js';
import { cursorText } from './plot.js';
import { withX } from './cursors.js';
import { nearestPeak, peaks, stepPeak } from './snap.js';
import { chip } from './tabs.js';
import { kindLabel } from './constants.js';
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
import RiXCursors from '@lucide/svelte/icons/separator-vertical';
import RiYCursors from '@lucide/svelte/icons/separator-horizontal';
import RiEraser from '@lucide/svelte/icons/eraser';
import RiSpan from '@lucide/svelte/icons/ruler-dimension-line';
import RiPlay from '@lucide/svelte/icons/play';
import RiFit from '@lucide/svelte/icons/unfold-vertical';
import RiPeak from '@lucide/svelte/icons/mountain';
import RiPeakRight from '@lucide/svelte/icons/chevron-right';
import RiPeakLeft from '@lucide/svelte/icons/chevron-left';

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

/** The distance between X1 and X2 on the PlotViews `targets` that have the pair, or null while none has. */
export function xSpan(targets) {
  return common(targets.flatMap((p) => {
    const { a, b } = p.markers();
    return a !== null && b !== null ? [cursorText(p.xKind()).span(b - a)] : [];
  }));
}

/** Gives each PlotView of `targets` the cursors `fn(pv, axis)` returns, as one edit named `what`. */
function editCursors(ctx, what, targets, fn) {
  ctx.svc.cursors.set(what, new Map(targets.map(([p, axis]) => [p.plotId, fn(p, axis)])));
}

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
  return p.snapSignals(whole && !p.spectrum ? plans.map((q) => ({ ...q, a0: q.src.absStart, a1: q.src.absEnd })) : plans).filter((s) => s.n > 0);
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
  if (p.spectrum && p.fExtent) lo = Math.max(to(p.fExtent[0]), Math.min(lo, to(p.fExtent[1]) - span));
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
const signalsNamed = (ctx) => (ctx.signals.length > 1 ? counted(ctx.signals.length, 'signal') : ctx.signals[0]);

/** Every plot command, the plot keys first in the order the keys sheet lists them. */
export const COMMANDS = [
  { id: 'addSignals', scope: 'plot', needs: ['workspace'], show: ['A'], keys: ['a', 'A', '+'], what: 'Add signals', run: picker('addSignals') },
  { id: 'enterSignals', scope: 'plot', needs: ['workspace'], show: ['Enter'], keys: ['Enter'], what: 'Into its signals', run: picker('enterSignals') },
  {
    id: 'splitRight', scope: 'plot', needs: ['workspace'], show: ['|'], keys: ['|', '\\'], what: 'Split right',
    icon: RiSplitH, label: 'Split right',
    run: (ctx) => ctx.svc.split(ids(ctx), 'row', ctx.from === 'key'),
  },
  {
    id: 'splitDown', scope: 'plot', needs: ['workspace'], show: ['_'], keys: ['-', '_'], what: 'Split down',
    icon: RiSplitV, label: 'Split down',
    run: (ctx) => ctx.svc.split(ids(ctx), 'col', ctx.from === 'key'),
  },
  {
    // It restores a maximized view whatever it acts on, as Escape does.
    id: 'maximize', scope: 'plot', needs: ['workspace'], show: ['F'], keys: ['f', 'F'], what: 'Maximize, restore', empty: true,
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
    id: 'follow', scope: 'view', show: ['End'], keys: ['End'], what: 'Follow live',
    icon: RiPlay, label: 'Follow live',
    run: (ctx) => follow(ctx, true),
  },
  {
    id: 'fitY', scope: 'view', show: ['Y'], keys: ['y', 'Y'], what: 'Fit y',
    icon: RiFit, label: 'Fit y',
    run: (ctx) => ctx.svc.viewHistory.step(() => pvs(ctx).forEach((p) => p.autoscale())),
  },
  {
    // Shown or hidden on every plot it acts on, as the lead plot's turns.
    id: 'legend', scope: 'plot', show: ['L'], keys: ['l', 'L'], what: 'Legend',
    icon: RiLegend, label: (ctx) => (ctx.lead?.plot.legend ? 'Hide the legend' : 'Show the legend'),
    run: (ctx) => {
      const on = !ctx.lead.plot.legend;
      ctx.svc.edit(ids(ctx), on ? 'Show the legend' : 'Hide the legend', () => ({ legend: on }));
    },
  },
  {
    id: 'close', scope: 'plot', needs: ['workspace'], show: ['Delete'], keys: ['Delete', 'Backspace'], what: 'Close',
    icon: RiClose, label: 'Close plot', destructive: true,
    value: (ctx) => plotsValue(ctx) ?? (ctx.lead ? counted(ctx.lead.plot.items.length, 'signal') : null),
    run: (ctx) => ctx.svc.close(ids(ctx), ctx.from === 'key'),
  },
  {
    // Made on a plot it pauses while that plot follows live; from the page, while any it acts on does. Else they follow live.
    id: 'pause', scope: 'view', place: 'page', show: ['Space'], keys: [' '], what: 'Pause, or follow live', empty: true,
    run: (ctx) => {
      const live = ctx.on ? ctx.on.view.live : ctx.plots.some((t) => t.view.live);
      if (!live) return void follow(ctx, false);
      const perf = clock.now();
      ctx.svc.viewHistory.step(() => ctx.plots.forEach((t) => t.view.pause(perf)));
    },
  },
  {
    // Its row says first when a copy would hold too much or come back short, from the hub's count of the rows (`how`).
    id: 'copyData', scope: 'view', place: 'page', show: ['Ctrl', 'C'], what: 'Copy data', empty: true,
    icon: RiCopy, label: 'Copy data',
    check: (ctx) => ctx.svc.copyCheck(ctx.plots),
    value: (ctx, check) => (check ? { text: rowsText(check.rows), warn: check.state !== 'ok', title: check.reason } : null),
    disabled: (ctx, check) => check?.state === 'too-many',
    run: (ctx) => void ctx.svc.copy(ctx.plots).catch((error) => ctx.svc.toast.error(`Could not copy: ${error.message}`)),
  },
  {
    id: 'exportData', scope: 'view', empty: true,
    icon: RiExport, label: 'Export data…',
    value: (ctx) => (ctx.on ? plotsValue(ctx) : ctx.svc.selection.plots.size ? `${ctx.plots.length} selected` : counted(ctx.plots.length, 'plot')),
    run: (ctx) => ctx.svc.openExport(ctx.plots),
  },
  {
    // A pair in view on each plot, the time plots paused, as they hold the moment it marks: one view step that holds the cursors it placed.
    id: 'addXCursors', scope: 'plot',
    icon: RiXCursors, label: 'Add X cursors',
    value: (ctx) => {
      const d = xSpan(pvs(ctx));
      return d ? { text: d, icon: RiSpan } : null;
    },
    run: (ctx) =>
      ctx.svc.viewHistory.step(() => {
        pvs(ctx).forEach((p) => p.config().kind === 'time' && p.view.pause(clock.now()));
        editCursors(ctx, 'Add X cursors', pvs(ctx).map((p) => [p]), (p) => p.xCursorsAdded());
      }),
  },
  {
    // The nearest peak in view to X1, or to the view's middle, placing it (`toPeak`).
    id: 'x1ToPeak', scope: 'plot', show: ['P'], keys: ['p', 'P'], shift: false, what: 'X1 to peak',
    icon: RiPeak, label: 'X1 to peak',
    value: (ctx) => peakValue(ctx, 'a', 0),
    disabled: (ctx) => !peakMoves(ctx, 'a', 0).length,
    run: (ctx) => toPeak(ctx, 'a', 0, 'X1 to peak'),
  },
  {
    id: 'x2ToPeak', scope: 'plot', show: ['Shift', 'P'], keys: ['p', 'P'], shift: true, what: 'X2 to peak',
    icon: RiPeak, label: 'X2 to peak',
    value: (ctx) => peakValue(ctx, 'b', 0),
    disabled: (ctx) => !peakMoves(ctx, 'b', 0).length,
    run: (ctx) => toPeak(ctx, 'b', 0, 'X2 to peak'),
  },
  {
    // The X cursor placed or moved last to the next peak right, past the view's edge where the plot holds one (`toPeak`).
    id: 'peakRight', scope: 'plot', show: ['.'], keys: ['.'], what: 'Next peak right',
    icon: RiPeakRight, label: 'Next peak right',
    value: (ctx) => peakValue(ctx, lastX(ctx), 1),
    disabled: (ctx) => !peakMoves(ctx, lastX(ctx), 1).length,
    run: (ctx) => toPeak(ctx, lastX(ctx), 1, 'Next peak right'),
  },
  {
    id: 'peakLeft', scope: 'plot', show: [','], keys: [','], what: 'Next peak left',
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
  {
    id: 'clearCursors', scope: 'view',
    icon: RiEraser, label: 'Clear cursors',
    value: (ctx) => {
      const n = pvs(ctx).reduce((n, p) => n + p.cursorsPlaced(), 0);
      return n ? `${n} placed` : null;
    },
    run: (ctx) => void ctx.svc.cursors.clear(ids(ctx)),
  },
  {
    // Each selected plot gets those of the signals it lacks. A menu has its row unless every selected plot shows them all.
    id: 'addToPlots', scope: 'plot', needs: ['signals'], place: 'list', show: ['Enter'], keys: ['Enter'], shift: false, what: 'Add to the selected plots',
    icon: RiAdd, label: 'Add to plot',
    when: (ctx) => !ctx.plots.length || lacking(ctx).length > 0,
    value: (ctx) => (ctx.plots.length ? signalsValue(ctx, lacking(ctx), 'to') : 'No plot selected'),
    disabled: (ctx) => !ctx.plots.length,
    run: (ctx) => ctx.svc.edit(lacking(ctx).map((t) => t.id), `Add ${signalsNamed(ctx)}`, (plot) => ({ items: [...plot.items, ...ctx.signals.filter((key) => !shows(plot, key)).map(chip)] })),
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
    id: 'newPlot', scope: 'plot', needs: ['signals'], place: 'list', show: ['Shift', 'Enter'], keys: ['Enter'], shift: true, what: 'New plot with them', empty: true,
    icon: RiNewPlot, label: 'New plot',
    value: (ctx) => (ctx.signals.length > 1 ? counted(ctx.signals.length, 'signal') : kindLabel('time', 'overlay')),
    run: (ctx) => ctx.svc.newPlot(ctx.signals),
  },
];

/** The command `id`. */
export const commandOf = (id) => COMMANDS.find((c) => c.id === id);
/** What `label` field `f` of a command reads in `ctx`. */
const read = (f, ctx, how) => (typeof f === 'function' ? f(ctx, how) : f);

/**
 * The page's commands over `svc`, its services: `targets(id)` and
 * `pageTargets(scope)`, the plots a command made on plot `id` or from the
 * page acts on, by id; `plotOf(id)`, plot `id` as {id, plot, view, pv};
 * `signalTargets(key)`, the signals a command made on signal `key`'s row,
 * or from the page with null, acts on, by key; `last()`, the plot clicked
 * last; and what the commands write through: `ws`, `selection`,
 * `viewHistory`, `cursors`, `toast`, `edit(ids, what, fn)`, `split(ids,
 * dir, fromKeys)`, `close(ids, fromKeys)`, `newPlot(keys)`, `copy(plots)`,
 * `copyCheck(plots)`, `openExport(plots)` and `picker(id, action)`.
 *
 * Where a command is made is {on, signal, signals, from}: `on` the plot it
 * is made on, or null for the page; `signal` the signal whose row it is
 * made on, or null, or else `signals`, the very signals it acts on; and
 * `from`, its origin.
 */
export function commands(svc) {
  /** What command `id`, made at `where`, acts on: its context. */
  function context(id, { on = null, signal = null, signals, from = 'menu' } = {}) {
    const c = commandOf(id);
    const all = (on !== null ? svc.targets(on) : svc.pageTargets(c.scope)).map(svc.plotOf).filter((t) => t.plot);
    const plots = c.kinds ? all.filter((t) => c.kinds.includes(t.plot.kind)) : all;
    const made = on !== null ? svc.plotOf(on) : null;
    const lead = made ?? plots.find((t) => t.id === svc.last()) ?? plots[0] ?? null;
    return { plots, all, on: made, lead, signals: c.needs?.includes('signals') ? (signals ?? svc.signalTargets(signal)) : [], from, svc };
  }
  return {
    context,
    /**
     * Runs command `id`, made at `where`, with `how`. Says whether it was
     * taken: it had plots to act on, of any kind, or runs with none, had
     * the signals it needs, and did not decline. It runs on the plots of
     * its kinds.
     */
    run(id, { how, ...where } = {}) {
      const c = commandOf(id), ctx = context(id, where);
      if (c.needs?.includes('signals') && !ctx.signals.length) return false;
      if (!ctx.all.length && !c.empty) return false;
      if (!ctx.plots.length && !c.empty) return true;
      return c.run(ctx, how) !== false;
    },
    /** How command `id`'s row reads, made at `where` with `how`: {label, icon, value, disabled, destructive, show, offered}, `offered` whether a menu has the row. */
    row(id, { how, ...where } = {}) {
      const c = commandOf(id), ctx = context(id, where);
      return {
        label: read(c.label, ctx, how),
        icon: c.active?.(ctx, how) ? c.activeIcon : c.icon,
        value: c.value?.(ctx, how) ?? null,
        disabled: !!c.disabled?.(ctx, how),
        destructive: !!c.destructive,
        show: c.show ?? null,
        offered: !c.when || !!c.when(ctx, how),
      };
    },
    /** What command `id`'s `check` answers, made on plot `on`, or null for a command with none. */
    check(id, { on = null } = {}) {
      const c = commandOf(id);
      return c.check ? c.check(context(id, { on })) : Promise.resolve(null);
    },
    /** The commands the palette offers now, as {id, label}: each with a row that needs no axis and no signal, and with plots to act on now, or else one that runs with none and is on, as Restore is while plots are maximized. */
    offered() {
      return COMMANDS.filter((c) => c.label && !c.needs?.some((n) => n === 'axis' || n === 'signals')).flatMap((c) => {
        const ctx = context(c.id, { from: 'palette' });
        return (ctx.plots.length || (c.empty && c.active?.(ctx))) && !c.disabled?.(ctx) ? [{ id: c.id, label: read(c.label, ctx) }] : [];
      });
    },
  };
}
