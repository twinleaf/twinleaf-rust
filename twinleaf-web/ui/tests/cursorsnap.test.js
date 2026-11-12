// A cursor dragged with Shift held snaps: its place is that of the axis's
// drawn ticks, the other cursor of its pair or the shown signals' features
// the rule of snap.js picks by where the pointer is, placed as the page
// draws them (tests/harness/snaporacle.js states the rule), or the
// pointer's own with none near. Shift pressed or let go mid-drag places it at
// once. The drag is one undo step and moves the cursor on the same plots
// as a drag without Shift, each to the snapped value. Shift held at the
// press on a cursor's line, tag or tab drags it snapped from its first
// move; off the cursors it starts no drag. A drag whose pointer went off
// its press is no click, with or without Shift; a press let go where it
// was is a click, a Shift+click by Shift as it was at the press. At every
// frame of a drag, with Shift or without, the readout reads the cursor as
// its tag does.
import { describe, expect, it } from 'vitest';
import { commands, page as browser } from 'vitest/browser';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { editCursors, feed, frames, leafOf, mountPage, pressKey, settle, wait, withExamples } from './harness/page.js';
import { oracleSnap, RULE } from './harness/snaporacle.js';
import { asKind } from './harness/captures.js';
import * as clock from '../src/lib/clock.js';
import { withX, withY, xOf, yOf } from '../src/lib/cursors.js';
import { plotIds } from '../src/lib/layout.js';

// Two time plots and two spectra of one signal, every plot selected.
const KEYS = [['sine.sine'], ['sine.sine'], ['sine.sine'], ['sine.sine']];
const KIND = ['time', 'time', 'spectrum', 'spectrum'];
/** The spectrum's one peak, its bin and amplitude, standing out of a 1/f slope between two of its axis's ticks. */
const PEAK = { hz: 23, a: 0.346 };

/** The page as the cases have it: its spectra with a peak, its time plots paused about a peak of the sine, every plot selected. */
async function mounted() {
  const page = await mountPage(KEYS, 'grid', 0, ['sine.sine']);
  feed(page.hub);
  const session = page.hub.session, perf = clock.now(), traces = [...session.traces.values()];
  const f = Float32Array.from({ length: 50 }, (_, i) => i + 1);
  session.apply({ kind: 'spectra', spectra: traces.map((t) => ({ id: t.meta.id, f, a: f.map((v) => (v === PEAK.hz ? PEAK.a : 1 / v)), floor: 0.02 })) }, perf);
  session.tick(perf);
  const ids = plotIds(page.ws.layout);
  page.history.group('Prepare', () => ids.forEach((id, n) => page.ws.setPlot(id, { kind: KIND[n], readoutAt: { x: 1, y: 0 } })));
  // A peak of the sine, a period from the ends of its samples.
  const t = traces[0].levels[0];
  let peak = null;
  for (let k = t.absStart; k < t.absEnd; k++) if (t.xAt(k) >= 5 && t.xAt(k) <= 11.3 && (peak === null || t.yAt(k) > t.yAt(peak))) peak = k;
  for (const n of [0, 1]) {
    const v = page.views.get(ids[n]);
    v.pause(perf);
    v.show(t.xAt(peak) - 3.3, t.xAt(peak) + 4.1, perf);
  }
  page.ws.selection.only(ids);
  flushSync();
  await wait(300);
  await settle(6);
  return { page, ids };
}

/** Cursor `name` of plot `id`'s `axis`, 'x' or 'y' on its shared axis, as the tab keeps it. */
const cursorOf = (page, id, axis, name) => {
  const c = page.ws.plots[id].cursors, kind = page.ws.plots[id].kind;
  return (axis === 'x' ? xOf(c, kind) : yOf(c, kind, 'shared'))[name];
};

/** The points PlotView `pv` draws in view, as [x, y] pairs. */
function drawnPoints(pv) {
  const [x0, x1] = pv.xd, out = [];
  for (const p of pv.drawn.plans) {
    if (p.s) p.s.f.forEach((x, i) => x >= x0 && x <= x1 && out.push([x, p.s.a[i]]));
    else for (let k = p.a0; k < p.a1; k++) if (p.src.xAt(k) >= x0 && p.src.xAt(k) <= x1) out.push([p.src.xAt(k), p.src.yAt(k)]);
  }
  return out;
}

/** The drawn point nearest x. */
const pointAt = (pts, x) => pts.reduce((best, p) => (Math.abs(p[0] - x) < Math.abs(best[0] - x) ? p : best));

/**
 * What PlotView `pv` shows that cursor `name` of `axis` could snap to, as
 * this test finds it, by kind, in the cursor's units: the ticks it draws,
 * the other cursor, and the signal's features: along x the spectrum's peak
 * or the sine's peaks and troughs, each the highest or lowest point within
 * half a second; along y its value at the X cursors, its least and
 * greatest in view, and 0. With `px`, the px of a value along the axis, and
 * `view`, what the axis shows.
 */
function known(pv, axis, name) {
  const other = name === 'a' ? 'b' : 'a', pts = drawnPoints(pv), ys = pts.map((p) => p[1]);
  if (axis === 'x') {
    const [x0, x1] = pv.xd;
    const extreme = ([x, y]) => {
      const about = pts.filter(([u]) => Math.abs(u - x) <= 0.5);
      return about.every(([, v]) => v <= y) || about.every(([, v]) => v >= y);
    };
    const data = pv.spectrum ? [PEAK.hz] : pts.filter(([x]) => x > x0 + 0.5 && x < x1 - 0.5).filter(extreme).map(([x]) => x);
    return { px: (v) => pv.pxOf(v), view: pv.xd, tick: pv.xTicks.filter((v) => v >= x0 && v <= x1), cursor: [pv.markers()[other]].filter((v) => v !== null), data };
  }
  const axisOf = pv.yAxes[0], sy = pv.axisScale(axisOf), view = [sy.invert(axisOf.lane.y + axisOf.lane.h), sy.invert(axisOf.lane.y)];
  const inView = (v) => v >= view[0] && v <= view[1];
  const { a, b } = pv.markers();
  const data = [...[a, b].filter((x) => x !== null).map((x) => pointAt(pts, x)[1]), Math.min(...ys), Math.max(...ys), 0].filter(inView);
  return { px: sy, view, tick: axisOf.ticks.filter(inView), cursor: [pv.yPair('shared')[other]].filter((v) => v !== null && inView(v)), data };
}

/** The px, along the lane `from` to `to`, farthest from every value of `avoid`, and how far that is. */
function clearest(px, avoid, from, to) {
  let best = null;
  for (let at = from; at <= to; at++) {
    const d = Math.min(...avoid.map((v) => Math.abs(px(v) - at)));
    if (!best || d > best.d) best = { at, d };
  }
  return best;
}

/** Where on PlotView `pv` a press takes the line of cursor `name` of `axis`, of y axis `key` if given, as the page draws it now; or null where none does. */
function pressOn(pv, axis, name, key) {
  const p = pv.pressable.find((q) => q.part === 'line' && q.kind === axis && q.name === name && (key === undefined || q.key === key));
  if (!p) return null;
  const points = [0.35, 0.5, 0.65, 0.2, 0.8].map((t) => (axis === 'x' ? [p.at, p.y + t * p.h] : [p.x + t * p.w, p.at]));
  return points.find((at) => document.elementFromPoint(...at) === pv.overlay && pv.pressAt(...at)?.kind === axis && pv.pressAt(...at)?.name === name && (key === undefined || pv.pressAt(...at)?.key === key)) ?? null;
}

/** Gives every plot of `page`, each of `kind`, the cursors `make(pv)` gives: {x: {a, b}, y: {a, b}} in its units, y on its shared axis. */
function place(page, ids, make) {
  ids.forEach((id, n) => {
    const { x, y } = make(page.pv(id), KIND[n]);
    editCursors(page, id, () => withY(withY(withX(withX(undefined, KIND[n], 'a', x.a), KIND[n], 'b', x.b), KIND[n], 'shared', 'a', y.a), KIND[n], 'shared', 'b', y.b));
  });
}

/** At share `f` along what PlotView `pv` shows of `axis`, in its units, a log axis's along its log. */
function along(pv, axis, f) {
  const [lo, hi] = axis === 'x' ? pv.xd : pv.yBounds('shared');
  const log = axis === 'x' ? pv.logX : pv.logY;
  return log ? 10 ** (Math.log10(lo) + f * (Math.log10(hi) - Math.log10(lo))) : lo + f * (hi - lo);
}

/** The cursors every case starts from: X1, X2, Y1 and Y2 at a third and two thirds of what each plot shows. */
const START = (pv) => ({ x: { a: along(pv, 'x', 0.3), b: along(pv, 'x', 0.7) }, y: { a: along(pv, 'y', 0.3), b: along(pv, 'y', 0.7) } });

/** Where in PlotView `pv` the pointer goes to put a cursor of `axis` at px `at` along it: across the plot, clear of the readout in its top right corner. */
const PRESS_AT = (axis, pv) => (at) => {
  const P = pv.geo.plot, lane = pv.yAxes[0].lane;
  return axis === 'x' ? [at, P.y + 0.55 * P.h] : [lane.x + 0.45 * lane.w, at];
};

/** Where VITE_TL_SHOTS names a folder, relative to this file, the screenshots go there. */
const SHOTS = import.meta.env.VITE_TL_SHOTS;

/** The one signal of `oneSine`: a sine of x lifted by LIFT, its peaks at x 1.797 + 2πn and its troughs halfway between. */
const LIFT = 0.07, PHASE = -0.226;

/**
 * The page with one time plot of 'sine.sine', sin(x + PHASE) + LIFT over
 * 20 s, paused on [3.34, 12.74]: a trough near 4.94, a peak at 8.08 near
 * the tick at 8, a trough near 11.22. X1 and X2 placed at the view's ends,
 * Y1 and Y2 at a tenth and nine tenths up the y axis, which shows `range`.
 */
async function oneSine(range) {
  const page = await mountPage([['sine.sine']], 'grid', 0, ['sine.sine']);
  const session = page.hub.session, perf = clock.now(), trace = [...session.traces.values()][0];
  const xs = Float64Array.from({ length: 2000 }, (_, i) => i / 100);
  session.apply({ kind: 'points', chunks: [{ id: trace.meta.id, level: 0, brk: false, xs, ys: Float32Array.from(xs, (x) => Math.sin(x + PHASE) + LIFT) }] }, perf);
  session.tick(perf);
  const [id] = plotIds(page.ws.layout), view = page.views.get(id);
  page.history.group('Prepare', () => page.ws.setPlot(id, { readoutAt: { x: 0, y: 1 } }));
  view.pause(perf);
  view.show(3.34, 12.74, perf);
  page.ws.selection.only([id]);
  flushSync();
  await wait(300);
  await settle(6);
  const pv = page.pv(id);
  pv.y.set('shared', { range: range(pv), auto: false });
  editCursors(page, id, () => withY(withY(withX(withX(undefined, 'time', 'a', 3.5), 'time', 'b', 12.6), 'time', 'shared', 'a', along(pv, 'y', 0.1)), 'time', 'shared', 'b', along(pv, 'y', 0.9)));
  await settle(6);
  return { page, id, pv };
}

/** Of PlotView `pv`'s drawn points, the first of the lowest (`sign` -1) or highest (1) within half a second of x, as [x, y] and where it is drawn, [px, py]. */
function extremeNear(pv, x, sign) {
  const pts = drawnPoints(pv).filter(([u]) => Math.abs(u - x) <= 0.5);
  const [px, py] = pts.reduce((best, p) => (sign * p[1] > sign * best[1] ? p : best));
  return { x: px, y: py, at: [pv.pxOf(px), pv.axisScale(pv.yAxes[0])(py)] };
}

/**
 * Drags cursor `name` of `axis` of PlotView `pv` with Shift held from the
 * press to `to`, the pointer at client [x, y] with the cursor's grab taken
 * off along its axis, so its place without Shift is `to`'s; then saves a
 * screenshot as `shot`, lets go and gives the value the drag left.
 */
async function shiftDrag(page, id, pv, axis, name, to, shot) {
  const from = pressOn(pv, axis, name);
  expect(from, `a press finds ${axis}${name}`).not.toBeNull();
  const grab = (axis === 'x' ? from[0] : from[1]) - pv.pressAt(...from).at;
  const at = axis === 'x' ? [to[0] + grab, to[1]] : [to[0], to[1] + grab];
  await commands.mouse([['move', ...from], ['hold', 'Shift'], ['down'], ['move', ...at]]);
  await settle();
  if (SHOTS) await browser.screenshot({ path: `${SHOTS}/${shot}.png`, element: leafOf(id) });
  const shown = (axis === 'x' ? xOf(pv.placed, 'time') : yOf(pv.placed, 'time', 'shared'))[name];
  await commands.mouse([['up'], ['let go', 'Shift']]);
  await settle();
  return shown;
}

describe('a cursor dragged with Shift held, by where the pointer is', () => {
  it('as a Y cursor at a trough’s height 99 px to its side, with a tick 12 px off along y, takes the trough', async () => {
    // The y axis at 12 px to 0.07: the tick at -1 is 12 px from the trough at -0.93.
    const { page, id, pv } = await oneSine((p) => {
      const span = (p.yAxes[0].lane.h * LIFT) / 12;
      return [-0.93 - 0.4 * span, -0.93 + 0.6 * span];
    });
    try {
      const sy = pv.axisScale(pv.yAxes[0]);
      editCursors(page, id, (c) => withX(c, 'time', 'a', extremeNear(pv, 4.94, -1).x));
      await settle();
      const trough = extremeNear(pv, 4.94, -1);
      const tick = pv.yAxes[0].ticks.reduce((a, b) => (Math.abs(sy(b) - trough.at[1]) < Math.abs(sy(a) - trough.at[1]) ? b : a));
      expect(Math.abs(sy(tick) - trough.at[1]), 'the nearest tick 12 px from the trough').toBeCloseTo(12, 0);
      const got = await shiftDrag(page, id, pv, 'y', 'a', [trough.at[0] + 99, trough.at[1]], 'case1-y-trough-beside');
      expect(got, 'the trough').toBe(trough.y);
    } finally {
      await commands.mouse([['up'], ['let go', 'Shift']]);
      page.dispose();
    }
  }, 120_000);

  it('as an X cursor low by the time axis, 4 px in x from a trough far above, does not jump to it', async () => {
    const { page, id, pv } = await oneSine(() => [-2.5, 1.2]);
    try {
      const trough = extremeNear(pv, 11.22, -1), P = pv.geo.plot, low = P.y + P.h - 8;
      expect(low - trough.at[1], 'the trough far above the pointer').toBeGreaterThan(130);
      const aim = trough.at[0] + 4;
      expect(Math.min(...pv.xTicks.map((t) => Math.abs(pv.pxOf(t) - aim))), 'no tick within 10 px').toBeGreaterThan(10);
      const got = await shiftDrag(page, id, pv, 'x', 'a', [aim, low], 'case2-x-low-under-trough');
      expect(Math.abs(pv.pxOf(got) - aim), 'free at the pointer').toBeLessThan(0.5);
    } finally {
      await commands.mouse([['up'], ['let go', 'Shift']]);
      page.dispose();
    }
  }, 120_000);

  it('as an X cursor on a peak’s height, a tick 1 px nearer along x than the peak, takes the peak', async () => {
    const { page, id, pv } = await oneSine((p) => p.yBounds('shared'));
    try {
      const peak = extremeNear(pv, 8.08, 1), d = peak.at[0] - pv.pxOf(8);
      expect(d, 'the peak a few px right of the tick at 8').toBeGreaterThan(3);
      expect(d).toBeLessThan(11);
      // The pointer between them, (d - 1) / 2 from the tick and (d + 1) / 2 from the peak.
      const aim = pv.pxOf(8) + (d - 1) / 2;
      const got = await shiftDrag(page, id, pv, 'x', 'a', [aim, peak.at[1]], 'case3-x-peak-by-tick');
      expect(got, 'the peak').toBe(peak.x);
    } finally {
      await commands.mouse([['up'], ['let go', 'Shift']]);
      page.dispose();
    }
  }, 120_000);

  it('as an X cursor by the time axis, a peak just above it 1 px nearer along x than a tick, takes the tick at its mark on the axis', async () => {
    // The y axis from just under the peak's 1.07, so that the peak is drawn a little above the time axis.
    const { page, id, pv } = await oneSine(() => [0.95, 2.5]);
    try {
      const peak = extremeNear(pv, 8.08, 1), P = pv.geo.plot, low = P.y + P.h - 2, d = peak.at[0] - pv.pxOf(8);
      expect(low - peak.at[1], 'the peak within 100 px above the pointer').toBeLessThan(90);
      expect(d, 'the peak a few px right of the tick at 8').toBeGreaterThan(2);
      expect(d).toBeLessThan(11);
      // The pointer between them, (d - 1) / 2 from the peak and (d + 1) / 2 from the tick.
      const got = await shiftDrag(page, id, pv, 'x', 'a', [peak.at[0] - (d - 1) / 2, low], 'case4-x-tick-by-axis');
      expect(got, 'the tick').toBe(8);
    } finally {
      await commands.mouse([['up'], ['let go', 'Shift']]);
      page.dispose();
    }
  }, 120_000);

  it('as a Y cursor by the y axis, X1’s point just in from it 1 px nearer along y than a tick, takes the tick at its mark on the gutter’s edge', async () => {
    // The y axis at 5 px to 0.0617, X1's point at 3.5 s, -0.0617 under the tick at 0.
    const value = Math.fround(Math.sin(3.5 + PHASE) + LIFT);
    const { page, id, pv } = await oneSine((p) => {
      const span = (p.yAxes[0].lane.h * Math.abs(value)) / 5;
      return [value - 0.5 * span, value + 0.5 * span];
    });
    try {
      const sy = pv.axisScale(pv.yAxes[0]), x1 = pointAt(drawnPoints(pv), 3.5), d = sy(x1[1]) - sy(0);
      expect(pv.yAxes[0].ticks, 'a tick at 0').toContain(0);
      expect(d, 'X1’s point a few px under the tick at 0').toBeGreaterThan(2);
      expect(d).toBeLessThan(11);
      const edge = pv.yAxes[0].x1;
      expect(pv.pxOf(x1[0]) - edge, 'X1’s point in from the gutter').toBeGreaterThan(6);
      // The pointer 2 px in from the gutter's edge, between them: (d - 1) / 2 from the point and (d + 1) / 2 from the tick.
      const got = await shiftDrag(page, id, pv, 'y', 'a', [edge + 2, sy(x1[1]) - (d - 1) / 2], 'case5-y-tick-by-gutter');
      expect(got, 'the tick').toBe(0);
    } finally {
      await commands.mouse([['up'], ['let go', 'Shift']]);
      page.dispose();
    }
  }, 120_000);
});

/** The plots of `layouts`: an overlay, a stacked and an independent time plot of two sines, the last with an axis on the right, and a spectrum of both. */
const LAYOUTS = [{ kind: 'time', mode: 'overlay' }, { kind: 'time', mode: 'stacked' }, { kind: 'time', mode: 'independent', axisSides: { 'sine.cos': 'right' } }, { kind: 'spectrum', mode: 'overlay' }];
const TWO = ['sine.sine', 'sine.cos'];

/**
 * The page with the plots of LAYOUTS: the sines, sin(x) and sin(x + 1),
 * paused on [2.64, 8.93], whose ends are a second or more from every peak
 * and trough of either; the spectra with the peak PEAK on a 1/f slope.
 * Every y axis holds the range it fitted itself to.
 */
async function layouts() {
  const page = await mountPage(LAYOUTS.map(() => TWO), 'grid', 0, TWO);
  feed(page.hub);
  const session = page.hub.session, perf = clock.now(), traces = [...session.traces.values()];
  const f = Float32Array.from({ length: 50 }, (_, i) => i + 1);
  session.apply({ kind: 'spectra', spectra: traces.map((t) => ({ id: t.meta.id, f, a: f.map((v) => (v === PEAK.hz ? PEAK.a : 1 / v)), floor: 0.02 })) }, perf);
  session.tick(perf);
  const ids = plotIds(page.ws.layout);
  page.history.group('Prepare', () => ids.forEach((id, n) => page.ws.setPlot(id, { ...LAYOUTS[n], readoutAt: { x: 1, y: 0 } })));
  for (const n of [0, 1, 2]) {
    const v = page.views.get(ids[n]);
    v.pause(perf);
    v.show(2.64, 8.93, perf);
  }
  flushSync();
  await wait(300);
  await settle(6);
  // Each y axis held where it fitted itself, so that what the test reads off the page stays put while a drag goes on.
  for (const id of ids) for (const [key, st] of page.pv(id).y) if (st.range) page.pv(id).y.set(key, { range: [...st.range], auto: false });
  await settle(6);
  return { page, ids };
}

/** At share `f` along y axis `key` of PlotView `pv`, in its units, a log axis's along its log. */
function alongY(pv, key, f) {
  const [lo, hi] = pv.yBounds(key);
  return pv.logY ? 10 ** (Math.log10(lo) + f * (Math.log10(hi) - Math.log10(lo))) : lo + f * (hi - lo);
}

/** The points PlotView `pv` draws in view of the signal of `plan`, as [x, y] pairs. */
function planPoints(pv, plan) {
  const [x0, x1] = pv.xd, out = [];
  if (plan.s) plan.s.f.forEach((x, i) => x >= x0 && x <= x1 && out.push([x, plan.s.a[i]]));
  else for (let k = plan.a0; k < plan.a1; k++) if (plan.src.xAt(k) >= x0 && plan.src.xAt(k) <= x1) out.push([plan.src.xAt(k), plan.src.yAt(k)]);
  return out;
}

/**
 * What cursor `name` of `axis` of PlotView `pv`, of y axis `key` for a Y
 * cursor, may snap to, as this test finds it on the page: targets as the
 * oracle takes them, {id, v, along, across, line}, placed in client px as
 * the page draws them. Ticks are points at their mark, an X cursor's on the
 * bottom edge of the plot and a Y cursor's on the edge of its axis's gutter
 * by the plot, and lines; the other cursor and 0 lines. A sine's features
 * are its points highest or lowest, the first of them, within half a second
 * each side, the view's ends a second from each; the spectrum's its peak.
 * Of a Y cursor, each signal's points at the X cursors, nearest each, and
 * its least and greatest, the first of them, all in view.
 */
function onPage(pv, axis, name, key) {
  const P = pv.geo.plot, other = name === 'a' ? 'b' : 'a', out = [];
  const scaleOf = (k) => pv.axisScale(pv.yAxes.find((a) => a.plans.some((p) => p.t.key === k)));
  const point = (k, [x, y], v) => ({ id: `${k} ${x}`, v, line: false, x, y, key: k });
  if (axis === 'x') {
    const [x0, x1] = pv.xd, inView = (v) => v !== null && v >= x0 && v <= x1;
    for (const v of pv.xTicks.filter(inView)) out.push({ id: `tick ${v}`, v, line: true, across: P.y + P.h });
    if (inView(pv.markers()[other])) out.push({ id: 'cursor', v: pv.markers()[other], line: true, across: null });
    for (const plan of pv.drawn.plans) {
      const pts = planPoints(pv, plan), k = plan.t.key;
      const found = plan.s
        ? pts.filter(([x]) => x === PEAK.hz)
        : pts.filter(([x, y], i) => {
            if (x < x0 + 1 || x > x1 - 1) return false;
            const about = pts.filter(([u]) => Math.abs(u - x) <= 0.5), first = (sign) => about.find(([, w]) => sign * w >= Math.max(...about.map(([, u]) => sign * u)));
            return first(1) === pts[i] || first(-1) === pts[i];
          });
      for (const p of found) out.push({ ...point(k, p, p[0]), across: scaleOf(k)(p[1]) });
    }
    return out.map((t) => ({ ...t, along: pv.pxOf(t.v) }));
  }
  const a = pv.yAxes.find((ax) => ax.key === key), sy = pv.axisScale(a);
  const [lo, hi] = [sy.invert(a.lane.y + a.lane.h), sy.invert(a.lane.y)], inView = (v) => v !== null && v >= lo && v <= hi;
  const edge = a.side === 'left' ? a.x1 : a.x0;
  for (const v of a.ticks.filter(inView)) out.push({ id: `tick ${v}`, v, line: true, across: edge });
  if (inView(pv.yPair(key)[other])) out.push({ id: 'cursor', v: pv.yPair(key)[other], line: true, across: null });
  for (const plan of a.plans) {
    const pts = planPoints(pv, plan), k = plan.t.key, { a: xa, b: xb } = pv.markers();
    for (const x of [xa, xb]) if (x !== null && x >= pv.xd[0] && x <= pv.xd[1] && pts.length) out.push(point(k, pointAt(pts, x), pointAt(pts, x)[1]));
    if (!pts.length) continue;
    const least = pts.reduce((m, p) => (p[1] < m[1] ? p : m)), most = pts.reduce((m, p) => (p[1] > m[1] ? p : m));
    out.push(point(k, least, least[1]), point(k, most, most[1]));
  }
  if (inView(0)) out.push({ id: 'zero', v: 0, line: true, across: null });
  return out.filter((t) => inView(t.v)).map((t) => ({ ...t, along: sy(t.v), across: t.x === undefined ? t.across : pv.pxOf(t.x) }));
}

/** A drag: on plot `plot` of LAYOUTS, its cursor X1 or Y1 of its y axis `lane` (of those it has), through `moves`, each near target `pick` of those on the page, by dAlong and dAcross px, or at shares fa and fc of the lane's length and the plot's width or height. */
const DRAG = fc.record({
  plot: fc.nat(3),
  axis: fc.constantFrom('x', 'y'),
  lane: fc.nat(1),
  moves: fc.array(fc.record({ pick: fc.option(fc.nat(40), { freq: 4 }), fa: fc.double({ min: 0, max: 1, noNaN: true }), fc: fc.double({ min: 0, max: 1, noNaN: true }), dAlong: fc.double({ min: -9, max: 9, noNaN: true }), dAcross: fc.double({ min: -140, max: 140, noNaN: true }) }), { minLength: 1, maxLength: 4 }),
});
/** A drag on each plot and axis, near a target, as the generated ones come after. */
const DRAG_EXAMPLES = LAYOUTS.flatMap((_, plot) => ['x', 'y'].map((axis) => [{ plot, axis, lane: 1, moves: [{ pick: 3, fa: 0.5, fc: 0.5, dAlong: 2, dAcross: 30 }, { pick: 5, fa: 0.5, fc: 0.5, dAlong: -4, dAcross: -110 }, { pick: null, fa: 0.4, fc: 0.6, dAlong: 0, dAcross: 0 }] }]));

describe('a cursor dragged with Shift held, over generated drags', () => {
  it('lands where the rule puts it, from the ticks and features on the page, on overlay, stacked, independent and spectrum plots', async () => {
    const { page, ids } = await layouts();
    const seen = { point: 0, line: 0, free: 0 };
    try {
      await fc.assert(
        fc.asyncProperty(DRAG, async ({ plot, axis, lane, moves }) => {
          const id = ids[plot], pv = page.pv(id), kind = LAYOUTS[plot].kind;
          // X1 and X2 at a third and two thirds of the view, Y1 and Y2 so on each y axis, a tenth further up on each after the first.
          editCursors(page, id, () => {
            let c = withX(withX(undefined, kind, 'a', along(pv, 'x', 0.3)), kind, 'b', along(pv, 'x', 0.7));
            pv.yAxes.forEach((a, j) => (c = withY(withY(c, kind, a.key, 'a', alongY(pv, a.key, 0.3 + 0.1 * j)), kind, a.key, 'b', alongY(pv, a.key, 0.7 + 0.1 * j))));
            return c;
          });
          page.ws.selection.only([id]);
          await settle();
          const key = axis === 'y' ? pv.yAxes[lane % pv.yAxes.length].key : undefined;
          const P = pv.geo.plot, ln = axis === 'y' ? pv.yAxes.find((a) => a.key === key).lane : null;
          const [lo, hi] = axis === 'x' ? [P.x + 3, P.x + P.w - 3] : [ln.y + 3, ln.y + ln.h - 3];
          const [clo, chi] = axis === 'x' ? [P.y + 3, P.y + P.h - 3] : [P.x + 3, P.x + P.w - 3];
          const clamp = (v, a, b) => Math.min(b, Math.max(a, v));
          const from = pressOn(pv, axis, 'a', key);
          expect(from, `${kind} ${LAYOUTS[plot].mode} ${axis}: a press finds the line`).not.toBeNull();
          // The page drawn with the pointer over it, as it is when the drag begins.
          await commands.mouse([['move', ...from]]);
          await settle();
          await commands.mouse([['hold', 'Shift'], ['down']]);
          const sy = () => pv.axisScale(pv.yAxes.find((a) => a.key === key));
          const grab = axis === 'x' ? pv.hover.x - pv.pxOf(pv.markers().a) : pv.hover.y - sy()(pv.yPair(key).a);
          let targets = onPage(pv, axis, 'a', key), held = null;
          try {
            for (const m of moves) {
              const base = m.pick !== null && targets.length ? targets[m.pick % targets.length] : null;
              const at = clamp(base ? base.along + m.dAlong : lo + m.fa * (hi - lo), lo, hi);
              const across = clamp(base?.across != null ? base.across + m.dAcross : clo + m.fc * (chi - clo), clo, chi);
              await commands.mouse([['move', ...(axis === 'x' ? [at + grab, across] : [across, at + grab])]]);
              await settle();
              // What the move was placed by: the page as last drawn.
              targets = onPage(pv, axis, 'a', key);
              const pointer = axis === 'x' ? { along: pv.hover.x - grab, across: pv.hover.y } : { along: pv.hover.y - grab, across: pv.hover.x };
              const want = oracleSnap(targets, pointer, held);
              // Within a hair of a threshold or of a tie, either way is the rule's: the drag ends there.
              if (want.close < 1e-6) break;
              const shown = (axis === 'x' ? xOf(pv.placed, kind) : yOf(pv.placed, kind, key)).a;
              const px = axis === 'x' ? pv.pxOf(shown) : sy()(shown), what = `${kind} ${LAYOUTS[plot].mode} ${axis}${key ? ` ${key}` : ''} at ${pointer.along.toFixed(2)}, ${pointer.across.toFixed(2)}`;
              if (want.target) expect(shown, `${what}: ${want.tier} ${want.target.id}`).toBe(want.target.v);
              else expect(Math.abs(px - pointer.along), `${what}: free`).toBeLessThan(0.01);
              expect(Math.abs(px - pointer.along), `${what}: within the jump`).toBeLessThanOrEqual(RULE.jump + 1e-9);
              seen[want.tier ?? 'free']++;
              held = want.target?.id ?? null;
            }
          } finally {
            await commands.mouse([['up'], ['let go', 'Shift']]);
            await settle();
          }
        }),
        withExamples(DRAG_EXAMPLES),
      );
      // Over the drags, each way the rule goes.
      expect(Object.values(seen).every((n) => n > 0), `points, lines and free: ${JSON.stringify(seen)}`).toBe(true);
    } finally {
      await commands.mouse([['up'], ['let go', 'Shift']]);
      page.dispose();
    }
  }, 600_000);

  it('holds to what it snapped to while another is nearer by less than the hold, and gives way past it', async () => {
    const { page, ids } = await layouts();
    try {
      // X1 at 4.26 s, where the two sines are a few px apart on the overlay's y axis: the Y cursor's points A and B, one above the other.
      const id = ids[0], pv = page.pv(id), X1 = 4.26;
      editCursors(page, id, () => withY(withY(withX(withX(undefined, 'time', 'a', X1), 'time', 'b', along(pv, 'x', 0.8)), 'time', 'shared', 'a', alongY(pv, 'shared', 0.6)), 'time', 'shared', 'b', alongY(pv, 'shared', 0.9)));
      page.ws.selection.only([id]);
      await settle();
      const targets = onPage(pv, 'y', 'a', 'shared');
      const at = (key) => targets.find((t) => t.key === key && Math.abs(t.x - X1) < 0.006);
      const [A, B] = [at('sine.sine'), at('sine.cos')], gap = B.along - A.along;
      expect(Math.abs(gap), 'A and B a few px apart').toBeGreaterThan(2);
      expect(Math.abs(gap), 'A and B a few px apart').toBeLessThan(10);
      const from = pressOn(pv, 'y', 'a', 'shared');
      // The page drawn with the pointer over it, as it is when the drag begins.
      await commands.mouse([['move', ...from]]);
      await settle();
      await commands.mouse([['hold', 'Shift'], ['down']]);
      const grab = pv.hover.y - pv.axisScale(pv.yAxes[0])(pv.yPair('shared').a);
      // Past A from B; then 0.55 of the way to B, B nearer but A at 0.7 of its jump nearer still; then 0.75 of the way, past the hold.
      const got = [], rule = [];
      let held = null;
      for (const share of [-1 / Math.abs(gap), 0.55, 0.75]) {
        await commands.mouse([['move', A.across, A.along + share * gap + grab]]);
        await settle();
        // What the move was placed by: the page as last drawn.
        const now = onPage(pv, 'y', 'a', 'shared'), pointer = { along: pv.hover.y - grab, across: pv.hover.x }, want = oracleSnap(now, pointer, held);
        if (share === 0.55) expect(oracleSnap(now, pointer).target?.id, 'without the hold, B').toBe(B.id);
        got.push(yOf(pv.placed, 'time', 'shared').a);
        rule.push(want.target?.v);
        held = want.target?.id ?? null;
      }
      expect(rule, 'the rule: A, A held, B').toEqual([A.v, A.v, B.v]);
      expect(got, 'the page: A, A held, B').toEqual(rule);
    } finally {
      await commands.mouse([['up'], ['let go', 'Shift']]);
      page.dispose();
    }
  }, 120_000);
});

describe('a cursor dragged with Shift held', () => {
  it('snaps to a tick, the other cursor or a feature of the data near the pointer, or follows it, with a mark on what it snapped to, on every plot it moves, as one undo step', async () => {
    const { page, ids } = await mounted();
    const done = [];
    try {
      for (const n of [0, 2])
        for (const axis of ['x', 'y'])
          for (const target of ['tick', 'cursor', 'data', 'none']) {
            const id = ids[n], pv = page.pv(id), what = `${KIND[n]} ${axis} cursor near ${target}`;
            place(page, ids, START);
            await settle();
            let k = known(pv, axis, 'a');
            const [lo, hi] = axis === 'x' ? [pv.geo.plot.x + 25, pv.geo.plot.x + pv.geo.plot.w - 25] : [pv.yAxes[0].lane.y + 20, pv.yAxes[0].lane.y + pv.yAxes[0].lane.h - 20];
            // The other cursor moved somewhere clear of the rest, on the plots of this kind; for a Y cursor near data, X1 put where the signal's value is clear on the y axis.
            if (target === 'cursor') {
              const spot = clearest(k.px, [...k.tick, ...k.data, START(pv)[axis].a], lo, hi);
              const there = axis === 'x' ? pv.dataAt(spot.at) : pv.yValueAt('shared', spot.at);
              place(page, ids, (p, kind) => {
                const s = START(p);
                if (kind === KIND[n]) s[axis].b = there;
                return s;
              });
            }
            if (target === 'data' && axis === 'y') {
              const sy = k.px, P = pv.geo.plot;
              const spot = drawnPoints(pv).find(([x, y]) => pv.pxOf(x) > P.x + 30 && pv.pxOf(x) < P.x + P.w - 30 && [...k.tick, ...k.cursor, ...k.data, START(pv).y.a].every((v) => Math.abs(sy(v) - sy(y)) > 12));
              expect(spot, `${what}: a point of the sine clear of the rest on the y axis`).toBeTruthy();
              place(page, ids, (p, kind) => ({ ...START(p), x: kind === KIND[n] ? { a: spot[0], b: START(p).x.b } : START(p).x }));
            }
            await settle();
            k = known(pv, axis, 'a');
            // The value the case aims at, and where the pointer goes: 4 px off it on the side farther from the rest.
            const all = [...k.tick, ...k.cursor, ...k.data];
            let aim, want;
            if (target === 'none') {
              const spot = clearest(k.px, all, lo, hi);
              expect(spot.d, `${what}: a place more than 10 px from everything`).toBeGreaterThan(12);
              aim = spot.at;
              want = null;
            } else {
              const pool = target === 'data' ? (axis === 'y' ? [k.data[0]] : k.data) : k[target];
              const scored = pool.filter((v) => k.px(v) >= lo && k.px(v) <= hi).map((v) => ({ v, d: Math.min(Infinity, ...all.filter((u) => u !== v).map((u) => Math.abs(k.px(u) - k.px(v)))) }));
              const best = scored.reduce((a, b) => (b.d > a.d ? b : a));
              want = best.v;
              const at = k.px(want), others = all.filter((u) => u !== want).map(k.px);
              const side = Math.min(Infinity, ...others.map((o) => Math.abs(o - (at + 4)))) >= Math.min(Infinity, ...others.map((o) => Math.abs(o - (at - 4)))) ? 4 : -4;
              aim = at + side;
              expect(Math.min(Infinity, ...others.map((o) => Math.abs(o - aim))), `${what}: ${want} the nearest to the pointer, by 2 px or more`).toBeGreaterThan(6);
            }
            const from = pressOn(pv, axis, 'a');
            expect(from, `${what}: a press finds the line`).not.toBeNull();
            // Near data, the pointer level with the point along the other direction.
            const to = PRESS_AT(axis, pv)(aim), pts = drawnPoints(pv);
            if (target === 'data' && axis === 'x') to[1] = pv.axisScale(pv.yAxes[0])(pointAt(pts, want)[1]);
            if (target === 'data' && axis === 'y') to[0] = pv.pxOf(pointAt(pts, pv.markers().a)[0]);
            const mid = [(from[0] + to[0]) / 2, (from[1] + to[1]) / 2];
            const before = ids.map((i) => page.ws.plots[i].cursors), steps = page.history.undos.length;
            const grab = (axis === 'x' ? from[0] : from[1]) - pv.pressAt(...from).at;
            await commands.mouse([['move', ...from], ['down'], ['move', ...mid], ['hold', 'Shift'], ['move', ...to]]);
            await settle();
            // While snapped, a ring on what it snapped to; none while it follows the pointer.
            const ring = pv.pressable.find((p) => p.part === 'snap');
            expect(!!ring, `${what}: the mark shows`).toBe(want !== null);
            // On the point snapped to, the tick or its number, or about the other cursor's name.
            const centre = ring && (axis === 'x' ? (ring.cx ?? ring.x + ring.w / 2) : (ring.cy ?? ring.y + ring.h / 2));
            if (target === 'data') expect([ring.cx !== undefined, Math.abs(centre - k.px(want)) < 1], `${what}: a ring on the point`).toEqual([true, true]);
            if (target === 'tick') expect(Math.abs(centre - k.px(want)), `${what}: the mark on the tick`).toBeLessThan(2);
            if (target === 'cursor') {
              const tag = pv.pressable.find((p) => p.part === 'tag' && p.kind === axis && p.name === 'b' && /^[XY]2$/.test(p.text));
              expect([ring.cx, ring.x < tag.x && ring.y < tag.y && ring.x + ring.w > tag.x + tag.w && ring.y + ring.h > tag.y + tag.h], `${what}: an outline about the other cursor's name`).toEqual([undefined, true]);
            }
            await commands.mouse([['up'], ['let go', 'Shift']]);
            await settle();
            const got = cursorOf(page, id, axis, 'a');
            if (want === null) expect(Math.abs(k.px(got) - (aim - grab)), `${what}: follows the pointer, ${got}`).toBeLessThan(1.5);
            else expect(got, `${what}: snaps to ${want}`).toBe(want);
            expect(pv.pressable.some((p) => p.part === 'snap'), `${what}: no ring once let go`).toBe(false);
            // The plots of its kind move with it, to the same value; the others not.
            ids.forEach((i, m) => expect(cursorOf(page, i, axis, 'a'), `${what}: plot ${m}`).toBe(KIND[m] === KIND[n] ? got : (axis === 'x' ? xOf(before[m], KIND[m]) : yOf(before[m], KIND[m], 'shared')).a));
            expect(page.history.undos.length - steps, `${what}: undo steps`).toBe(1);
            await pressKey('Control+z');
            flushSync();
            expect(ids.map((i) => page.ws.plots[i].cursors), `${what}: undone`).toEqual(before);
            page.ws.selection.only(ids);
            flushSync();
            done.push(what);
          }
      expect(done).toHaveLength(16);
    } finally {
      await commands.mouse([['up'], ['let go', 'Shift']]);
      page.dispose();
    }
  }, 600_000);

  it('snaps at once when Shift goes down mid-drag, and follows the pointer at once when it is let go, with no move between', async () => {
    const { page, ids } = await mounted();
    try {
      for (const n of [0, 2])
        for (const axis of ['x', 'y']) {
          const id = ids[n], pv = page.pv(id), what = `${KIND[n]} ${axis} cursor`;
          place(page, ids, START);
          await settle();
          const k = known(pv, axis, 'a');
          // The tick nearest the middle of the axis, the pointer 4 px off it.
          const mid = k.px((k.view[0] + k.view[1]) / 2);
          const tick = k.tick.reduce((a, b) => (Math.abs(k.px(b) - mid) < Math.abs(k.px(a) - mid) ? b : a));
          const others = [...k.cursor, ...k.data, ...k.tick].map(k.px).filter((o) => Math.abs(o - k.px(tick)) > 0.01);
          const aim = [4, -4].map((s) => k.px(tick) + s).find((at) => others.every((o) => Math.abs(o - at) > 6));
          expect(aim, `${what}: a place 4 px from a tick and clear of the rest`).toBeDefined();
          const from = pressOn(pv, axis, 'a'), to = PRESS_AT(axis, pv)(aim);
          const grab = (axis === 'x' ? from[0] : from[1]) - pv.pressAt(...from).at;
          const steps = page.history.undos.length;
          const now = () => cursorOf(page, id, axis, 'a'), peer = () => cursorOf(page, ids[n + 1], axis, 'a');
          // `pv.placed` is what the plot draws while the drag goes on.
          const shown = () => (axis === 'x' ? xOf(pv.placed, KIND[n]) : yOf(pv.placed, KIND[n], 'shared')).a;
          await commands.mouse([['move', ...from], ['down'], ['move', (from[0] + to[0]) / 2, (from[1] + to[1]) / 2], ['move', ...to]]);
          await settle();
          expect(Math.abs(k.px(shown()) - (aim - grab)), `${what}: free before Shift`).toBeLessThan(1.5);
          await commands.mouse([['hold', 'Shift']]);
          await settle();
          expect(shown(), `${what}: on the tick as Shift goes down`).toBe(tick);
          await commands.mouse([['let go', 'Shift']]);
          await settle();
          expect(Math.abs(k.px(shown()) - (aim - grab)), `${what}: free as Shift is let go`).toBeLessThan(1.5);
          await commands.mouse([['hold', 'Shift']]);
          await settle();
          await commands.mouse([['up'], ['let go', 'Shift']]);
          await settle();
          expect([now(), peer()], `${what}: let go snapped, on its peer too`).toEqual([tick, tick]);
          expect(page.history.undos.length - steps, `${what}: undo steps`).toBe(1);
        }
    } finally {
      await commands.mouse([['up'], ['let go', 'Shift']]);
      page.dispose();
    }
  }, 600_000);

  it('is read in the readout as its tag reads it, at every frame of the drag, with Shift or without, on a time plot and a capture plot', async () => {
    const { page, id, pv } = await oneSine(() => [-2.5, 1.2]);
    let start = page.ws.plots[id].cursors;
    /** The text of cursor `name` of `axis`'s value tag as the frame drew it, or null while it is out of view. */
    const tagOf = (axis, name) => pv.pressable.find((p) => p.part === 'tag' && p.kind === axis && p.name === name && !p.edge && !/^[XY][12]$/.test(p.text))?.text ?? null;
    /** What the readout's row of `axis` shows of cursor `name`. */
    const readOf = (axis, name) => {
      const row = [...pv.body.querySelectorAll('.cursor-readout tr.position')].find((r) => r.querySelector('th').textContent.trim().startsWith(axis.toUpperCase()));
      return row?.querySelectorAll('td')[name === 'a' ? 0 : 1].textContent ?? null;
    };
    const share = fc.double({ min: 0.08, max: 0.92, noNaN: true });
    const drag = fc.record({ axis: fc.constantFrom('x', 'y'), name: fc.constantFrom('a', 'b'), shift: fc.boolean(), path: fc.array(share, { minLength: 1, maxLength: 6 }) });
    try {
      for (const plot of ['time', 'capture']) {
      if (plot === 'capture') {
        // The plot as a capture plot, its cursors at a fifth and four fifths along what it shows.
        asKind(page, id, 'capture');
        await settle(6);
        const kind = page.ws.plots[id].kind;
        editCursors(page, id, () => withY(withY(withX(withX(undefined, kind, 'a', along(pv, 'x', 0.2)), kind, 'b', along(pv, 'x', 0.8)), kind, 'shared', 'a', along(pv, 'y', 0.2)), kind, 'shared', 'b', along(pv, 'y', 0.8)));
        await settle();
        start = page.ws.plots[id].cursors;
      }
      await fc.assert(
        fc.asyncProperty(drag, async ({ axis, name, shift, path }) => {
          // Every case from the cursors the plot started with.
          editCursors(page, id, () => start);
          await settle();
          const from = pressOn(pv, axis, name);
          expect(from, `a press finds ${axis}${name}`).not.toBeNull();
          const P = pv.geo.plot, lane = pv.yAxes[0].lane;
          const at = (f) => (axis === 'x' ? [P.x + f * P.w, from[1]] : [from[0], lane.y + f * lane.h]);
          await commands.mouse([['move', ...from], ...(shift ? [['hold', 'Shift']] : []), ['down']]);
          try {
            for (const [i, f] of path.entries()) {
              await commands.mouse([['move', ...at(f)]]);
              // Each frame the page draws as the pointer rests there.
              for (let k = 0; k < 3; k++) {
                await frames(1);
                const tag = tagOf(axis, name);
                if (tag !== null) expect(readOf(axis, name), `${plot} plot, ${shift ? 'Shift+' : ''}${axis.toUpperCase()}${name === 'a' ? 1 : 2} at move ${i + 1}, frame ${k + 1}: the readout as the tag`).toBe(tag);
              }
            }
          } finally {
            await commands.mouse([['up'], ...(shift ? [['let go', 'Shift']] : [])]);
            await settle();
          }
        }),
        // The case of the report: Y1 dragged with Shift to the trough near the bottom, and X2 dragged without.
        withExamples([[{ axis: 'y', name: 'a', shift: true, path: [0.9, 0.8] }], [{ axis: 'x', name: 'b', shift: false, path: [0.2, 0.6] }]]),
      );
      }
    } finally {
      await commands.mouse([['up'], ['let go', 'Shift']]);
      page.dispose();
    }
  }, 300_000);

  it('held at the press on a cursor drags it snapped from its first move, off the cursors moves nothing; a drag never selects, a Shift+click on a cursor does', async () => {
    const { page, ids } = await mounted();
    try {
      for (const n of [0, 2]) {
        const id = ids[n], pv = page.pv(id), other = ids[(n + 2) % 4], what = KIND[n];
        place(page, ids, START);
        page.ws.selection.only([other]);
        flushSync();
        await settle();
        const before = page.ws.plots[id].cursors, from = pressOn(pv, 'x', 'a'), view = () => [pv.view.state(), page.ws.plots[id].fRange ?? null];
        const k = known(pv, 'x', 'a'), mid = k.px((k.view[0] + k.view[1]) / 2);
        const tick = k.tick.reduce((a, b) => (Math.abs(k.px(b) - mid) < Math.abs(k.px(a) - mid) ? b : a));
        // Shift held from before a press off the cursors: a drag there pans nothing and moves no cursor.
        const P = pv.geo.plot, off = [k.px(tick) + 20, P.y + 0.6 * P.h], was = view();
        expect(pv.pressAt(...off)?.part, `${what}: a press on the plot off the cursors`).toBe('plot');
        await commands.mouse([['move', ...off], ['hold', 'Shift'], ['down'], ['move', off[0] + 15, off[1]], ['move', off[0] + 30, off[1]], ['up'], ['let go', 'Shift']]);
        await settle();
        expect([page.ws.plots[id].cursors, view()], `${what}: Shift+drag off the cursors moves nothing`).toEqual([before, was]);
        expect([...page.ws.selection.plots], `${what}: Shift+drag off the cursors leaves the selection`).toEqual([other]);
        // Shift held from before the press on X1's line: its first move snaps it, near the tick, and the drag leaves the selection.
        await commands.mouse([['move', ...from], ['hold', 'Shift'], ['down'], ['move', k.px(tick) + 4, from[1]]]);
        await settle();
        expect([xOf(pv.placed, KIND[n]).a, pv.pressable.some((p) => p.part === 'snap')], `${what}: the first move snaps`).toEqual([tick, true]);
        await commands.mouse([['up'], ['let go', 'Shift']]);
        await settle();
        expect([cursorOf(page, id, 'x', 'a'), [...page.ws.selection.plots]], `${what}: let go on the tick, the selection as it was`).toEqual([tick, [other]]);
        await pressKey('Control+z');
        flushSync();
        await settle();
        // A drag that went away and came back to its press is no click either.
        await commands.mouse([['move', ...from], ['hold', 'Shift'], ['down'], ['move', from[0] + 30, from[1]], ['move', from[0] + 1, from[1]], ['up'], ['let go', 'Shift']]);
        await settle();
        expect([...page.ws.selection.plots], `${what}: a drag back to its press leaves the selection`).toEqual([other]);
        await pressKey('Control+z');
        flushSync();
        await settle();
        // A Shift+click on the cursor's line, let go where it was pressed, adds the plot and moves no cursor.
        await commands.mouse([['move', ...from], ['hold', 'Shift'], ['down'], ['up'], ['let go', 'Shift']]);
        await settle();
        expect([...page.ws.selection.plots].sort(), `${what}: Shift+click adds the plot`).toEqual([other, id].sort());
        expect(page.ws.plots[id].cursors, `${what}: Shift+click moves no cursor`).toEqual(before);
        // A cursor 3 px from a tick, pressed, nudged 1 px and snapped with Shift: let go there, it is a click without Shift, which keeps the plot selected.
        place(page, ids, (p) => ({ ...START(p), x: { a: p.dataAt(k.px(tick) + 3), b: START(p).x.b } }));
        await settle();
        const at = pressOn(pv, 'x', 'a');
        await commands.mouse([['move', ...at], ['down'], ['move', at[0] + 1, at[1]], ['hold', 'Shift'], ['up'], ['let go', 'Shift']]);
        await settle();
        expect(cursorOf(page, id, 'x', 'a'), `${what}: snapped to the tick`).toBe(tick);
        expect(page.ws.selection.plots.has(id), `${what}: the plot stays selected`).toBe(true);
      }
    } finally {
      await commands.mouse([['up'], ['let go', 'Shift']]);
      page.dispose();
    }
  }, 600_000);
});
