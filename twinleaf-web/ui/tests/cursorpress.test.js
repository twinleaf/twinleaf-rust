// What a press on a plot lands on, read from what its PlotView last drew
// (`pressable`, in the order drawn): for plots of every layout and kind,
// with y axes on either side, and cursors anywhere along their axes, in
// view, at its ends and corners, out of it, or not placed, a press on the
// centre of each cursor's tag and tab drags the cursor whose part lies on
// top there, and nothing else: no other cursor moves, and neither the y
// ranges nor the view do. A tag or a line is held where it was pressed, so
// the cursor moves as far as the pointer does; a tab, or the tag of a
// cursor out of view, brings its cursor under the pointer.
import { describe, expect, it } from 'vitest';
import { commands } from 'vitest/browser';
import fc from 'fast-check';
import { editCursors, feed, frames, leafOf, mountPage, settle, wait } from './harness/page.js';
import * as clock from '../src/lib/clock.js';
import { plotIds } from '../src/lib/layout.js';
import { xOf, yOf } from '../src/lib/cursors.js';

const SIGNALS = ['sine.sine', 'aux.triangle', 'sine.cosine'];
const LAYOUTS = {
  overlay: { mode: 'overlay', axisSides: {} },
  'two left, one right': { mode: 'independent', axisSides: { [SIGNALS[1]]: 'left', [SIGNALS[2]]: 'right' } },
  'one left, two right': { mode: 'independent', axisSides: { [SIGNALS[1]]: 'right', [SIGNALS[2]]: 'right' } },
  stacked: { mode: 'stacked', axisSides: {} },
};
/** Where the cursors' readout sits, which covers what is under it: each corner, so every part of the plot is clear in some case. */
const CORNERS = [{ x: 0, y: 0 }, { x: 1, y: 0 }, { x: 0, y: 1 }, { x: 1, y: 1 }];
/** Where along its axis a cursor is put, as a share of what the axis shows: in view, at an end, out of view, or null for not placed. */
const place = fc.option(fc.oneof(fc.double({ min: 0, max: 1, noNaN: true }), fc.constantFrom(0, 0.004, 0.5, 0.996, 1, -0.3, 1.3)));
const pair = fc.record({ a: place, b: place });
const plotCase = fc.record({
  kind: fc.constantFrom('time', 'spectrum'),
  layout: fc.constantFrom(...Object.keys(LAYOUTS)),
  x: pair,
  y: fc.array(pair, { minLength: SIGNALS.length, maxLength: SIGNALS.length }),
  readoutAt: fc.constantFrom(...CORNERS),
});
const all = (x, y) => ({ x, y: SIGNALS.map(() => y) });
/** Cursors at the ends of every axis, so their tags meet in the corners; none placed, so every tab rests; all out of view; and all well in view. */
const PLACED = {
  ends: all({ a: 0.004, b: 0.996 }, { a: 0.01, b: 0.99 }),
  corner: all({ a: 0, b: 0.004 }, { a: 0, b: 0.02 }),
  none: all({ a: null, b: null }, { a: null, b: null }),
  out: all({ a: -0.3, b: 1.3 }, { a: -0.3, b: 1.3 }),
  mid: all({ a: 0.3, b: 0.6 }, { a: 0.3, b: 0.7 }),
};
/** Every layout of each kind with cursors at the ends of its axes, and the other placements on some of each. */
const EXAMPLES = [
  ...['time', 'spectrum'].flatMap((kind) => Object.keys(LAYOUTS).map((layout, i) => [{ kind, layout, ...PLACED.ends, readoutAt: CORNERS[(i + (kind === 'time')) % 4] }])),
  ...Object.keys(LAYOUTS).flatMap((layout, i) => ['corner', 'none', 'out', 'mid'].map((how, k) => [{ kind: (i + k) % 3 ? 'time' : 'spectrum', layout, ...PLACED[how], readoutAt: CORNERS[(i + k) % 4] }])),
];

const inside = (p, [x, y]) => x >= p.x && x < p.x + p.w && y >= p.y && y < p.y + p.h;
const centreOf = (p) => [p.x + p.w / 2, p.y + p.h / 2];
const within = (v, lo, hi) => Math.min(hi, Math.max(lo, v));
/** A part of a cursor as one name, the same from frame to frame while the cursors stay. */
const nameOf = (p) => [p.part, p.kind, p.name, p.key ?? '', p.edge ? 'edge' : '', p.text ?? ''].join(' ');
/** The tags and tabs plot view `pv` drew. */
const tagsAndTabs = (pv) => pv.pressable.filter((p) => p.part === 'tag' || p.part === 'tab');

/** The page with one plot of SIGNALS, fed and paused, and what a case reads of it. */
async function mounted() {
  const page = await mountPage([SIGNALS], 'grid', 0, SIGNALS);
  feed(page.hub);
  const id = plotIds(page.ws.layout)[0], view = page.views.get(id);
  view.pause(clock.now());
  await wait(300);
  await settle();
  return { page, id, view, base: view.state(), canvas: leafOf(id).querySelector('.plot-body canvas') };
}

/** Shows the plot as case `c` has it, its view as at first, and gives the cursors it placed. */
async function arrange({ page, id, view, base }, c) {
  view.restore(base, clock.now());
  page.history.group('Prepare', () => page.ws.setPlot(id, { kind: c.kind, ...LAYOUTS[c.layout], readoutAt: c.readoutAt, fRange: null, cursors: undefined }));
  await commands.mouse([['move', 4, 4]]);
  // The axes are as wide as their labels, which the first frames measure.
  await settle(6);
  const pv = page.pv(id), log = (v) => (pv.logX ? Math.log10(v) : v), unlog = (v) => (pv.logX ? 10 ** v : v);
  const along = ([lo, hi], f, to = (v) => v, from = (v) => v) => (f === null ? null : from(to(lo) + f * (to(hi) - to(lo))));
  const ylog = pv.logY ? [Math.log10, (v) => 10 ** v] : [];
  const x = { a: along(pv.xd, c.x.a, log, unlog), b: along(pv.xd, c.x.b, log, unlog) };
  const y = Object.fromEntries(pv.yAxes.map((axis, i) => [axis.key, { a: along(pv.yBounds(axis.key), c.y[i].a, ...ylog), b: along(pv.yBounds(axis.key), c.y[i].b, ...ylog) }]));
  const cursors = { [c.kind === 'spectrum' ? 'f' : 'x']: x, y: { [c.kind]: y } };
  await put(page, id, cursors);
  return cursors;
}

/** Gives the plot `cursors`, and waits for the page to show their readout, which it reads a few times a second. */
async function put(page, id, cursors) {
  editCursors(page, id, () => cursors);
  await wait(100);
  await settle();
}

describe('a press on a plot', () => {
  it('on a cursor’s tag or tab drags the cursor drawn on top there, over an axis as within the plot, and moves nothing else', async () => {
    const m = await mounted(), { page, id, canvas } = m;
    const count = { pressed: 0, tabs: 0, held: 0, overAxis: 0, underAnother: 0, covered: 0 };
    try {
      await fc.assert(
        fc.asyncProperty(plotCase, async (c) => {
          const cursors = await arrange(m, c);
          const pv = page.pv(id), kind = c.kind;
          /** The plot's cursors, each by name, and what else a drag could move: the view and the frequency range. */
          const read = () => {
            const held = page.ws.plots[id].cursors, { live, window, span, right, pausedAt, yRanges } = pv.view.state();
            const each = { 'x a': xOf(held, kind).a, 'x b': xOf(held, kind).b };
            for (const axis of pv.yAxes) for (const n of ['a', 'b']) each[`y ${n} ${axis.key}`] = yOf(held, kind, axis.key)[n];
            return { each, rest: { live, window, span, right, pausedAt, yRanges, fRange: page.ws.plots[id].fRange ?? null } };
          };
          for (const name of tagsAndTabs(pv).map(nameOf)) {
            const part = tagsAndTabs(pv).find((p) => nameOf(p) === name);
            expect(part, `${name} is drawn again`).toBeTruthy();
            const at = centreOf(part);
            await commands.mouse([['move', ...at]]);
            await frames(2);
            // What the page lays over the plot, as the readout, takes its own presses.
            if (document.elementFromPoint(...at) !== canvas) {
              count.covered++;
              continue;
            }
            // The part drawn last at the pointer is on top there.
            const top = pv.pressable.findLast((p) => inside(p, at)), P = pv.geo.plot;
            expect(top.kind, `${name}: a cursor's part is on top at its centre`).toBeTruthy();
            if (top.part === 'tab') expect(top.alpha, `${name}: the tab under the pointer shows`).toBe(1);
            const lane = top.kind === 'y' ? top.axis.lane : P;
            // Let go within the plot, and a Y cursor within its lane, where a cursor stays.
            const to = [within(at[0] + 30 * Math.sign(P.x + P.w / 2 - at[0]), P.x + 20, P.x + P.w - 20), within(at[1] + 30 * Math.sign(lane.y + lane.h / 2 - at[1]), lane.y + 15, lane.y + lane.h - 15)];
            const before = read();
            await commands.mouse([['down'], ['move', (at[0] + to[0]) / 2, (at[1] + to[1]) / 2], ['move', ...to], ['up']]);
            await settle();
            const after = read(), own = top.kind === 'x' ? `x ${top.name}` : `y ${top.name} ${top.key}`;
            const where = `${c.kind} ${c.layout}: a press on ${name} at ${at.map(Math.round)}, under ${nameOf(top)}`;
            expect(after.rest, `${where}, leaves the view and the y ranges`).toEqual(before.rest);
            expect({ ...after.each, [own]: null }, `${where}, moves no other cursor`).toEqual({ ...before.each, [own]: null });
            // Held where it was pressed, the cursor moves as far as the pointer did; a tab's comes under the pointer.
            const k = top.kind === 'x' ? 0 : 1, wanted = top.at === undefined ? to[k] : top.at + to[k] - at[k];
            const px = after.each[own] === null ? null : top.kind === 'x' ? pv.pxOf(after.each[own]) : pv.axisScale(pv.yAxes.find((a) => a.key === top.key))(after.each[own]);
            expect(px, `${where}, moves ${own} to ${wanted}`).not.toBeNull();
            expect(Math.abs(px - wanted), `${where}, moves ${own} to ${wanted}, not ${px}`).toBeLessThan(1.5);
            count.pressed++;
            if (top.part === 'tab') count.tabs++;
            if (top.at !== undefined) count.held++;
            if (nameOf(top) !== name) count.underAnother++;
            if (pv.pressAt(...at, ['yaxis', 'xaxis'])) count.overAxis++;
            await put(page, id, cursors);
          }
        }),
        { examples: EXAMPLES, numRuns: EXAMPLES.length + Math.ceil(fc.readConfigureGlobal().numRuns / 4) },
      );
      // Tags are laid clear of one another, so few lie under another; some are under what the page lays over the plot.
      for (const [what, n] of Object.entries(count)) expect(n, what).toBeGreaterThan(what === 'covered' || what === 'underAnother' ? -1 : 10);
    } finally {
      await commands.mouse([['up']]);
      page.dispose();
    }
  }, 600_000);
});
