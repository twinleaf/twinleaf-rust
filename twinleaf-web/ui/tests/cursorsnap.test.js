// A cursor dragged with Shift held snaps: its place is the nearest of the
// axis's drawn ticks, the other cursor of its pair and the shown signals'
// features within 10 px of where the pointer holds it, or the pointer's
// own with none that near. Shift pressed or let go mid-drag places it at
// once. The drag is one undo step and moves the cursor on the same plots
// as a drag without Shift, each to the snapped value. Shift held at the
// press on a cursor's line, tag or tab drags it snapped from its first
// move; off the cursors it starts no drag. A drag whose pointer went off
// its press is no click, with or without Shift; a press let go where it
// was is a click, a Shift+click by Shift as it was at the press.
import { describe, expect, it } from 'vitest';
import { commands } from 'vitest/browser';
import { flushSync } from 'svelte';
import { editCursors, feed, mountPage, pressKey, settle, wait } from './harness/page.js';
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

/** Where on PlotView `pv` a press takes the line of cursor `name` of `axis`, as the page draws it now; or null where none does. */
function pressOn(pv, axis, name) {
  const p = pv.pressable.find((q) => q.part === 'line' && q.kind === axis && q.name === name);
  if (!p) return null;
  const points = [0.35, 0.5, 0.65, 0.2, 0.8].map((t) => (axis === 'x' ? [p.at, p.y + t * p.h] : [p.x + t * p.w, p.at]));
  return points.find((at) => document.elementFromPoint(...at) === pv.overlay && pv.pressAt(...at)?.kind === axis && pv.pressAt(...at)?.name === name) ?? null;
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

describe('a cursor dragged with Shift held', () => {
  it('snaps to the nearest of a tick, the other cursor and a feature of the data within 10 px, or follows the pointer, on every plot it moves, as one undo step', async () => {
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
            const to = PRESS_AT(axis, pv)(aim), mid = [(from[0] + to[0]) / 2, (from[1] + to[1]) / 2];
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
