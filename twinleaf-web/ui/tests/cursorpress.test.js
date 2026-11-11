// What a press on a plot lands on, read from what its PlotView last drew
// (`pressable`, in the order drawn): for plots of every layout and kind,
// with y axes on either side, and cursors anywhere along their axes, in
// view, at its ends and corners, out of it, or not placed, a press on the
// centre of each cursor's tag and tab drags the cursor whose part lies on
// top there, and nothing else: no other cursor moves, and neither the y
// ranges nor the view do. A tag or a line is held where it was pressed, so
// the cursor moves as far as the pointer does; a tab, or the tag of a
// cursor out of view, brings its cursor under the pointer.
//
// And what follows the pointer, drawn over the rest and let through by a
// press: the hover's tip covers no cursor's tag or tab, and the pointer's
// value on a y axis has one place, where it is left out while it would
// cover a cursor's tag.
import { describe, expect, it } from 'vitest';
import { commands } from 'vitest/browser';
import fc from 'fast-check';
import { flushSync } from 'svelte';
import { OPEN, editCursors, feed, frames, leafOf, mountPage, settle, until, wait } from './harness/page.js';
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
/** A Y cursor at the foot of its axis beside an X cursor's tag along the plot's foot, which the pointer's value, wider than a cursor's name, reaches before it reaches the Y cursor's tags. */
const BESIDE_X_TAG = [
  [{ kind: 'time', layout: 'overlay', x: { a: null, b: -0.3 }, y: [{ a: null, b: 0 }, { a: null, b: null }, { a: null, b: null }], readoutAt: { x: 0, y: 0 } }],
  [{ kind: 'time', layout: 'one left, two right', x: { a: null, b: 1.3 }, y: [{ a: null, b: null }, { a: null, b: 0 }, { a: null, b: null }], readoutAt: { x: 0, y: 0 } }],
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

describe('a press in the first frame after the plot moved or changed size', () => {
  it('lands on what the plot shows where it is now, not on what it drew where it was', async () => {
    const page = await mountPage([[SIGNALS[0]], [SIGNALS[1]], [SIGNALS[2]], []], 'grid', 0, SIGNALS);
    try {
      feed(page.hub);
      await settle(6);
      const id = plotIds(page.ws.layout)[0], pv = page.pv(id);
      // The middle of its x axis, which the plot area covers once the plot fills the tab.
      const at = centreOf(pv.pressable.find((p) => p.part === 'xaxis'));
      expect(pv.pressAt(...at)?.part, 'the x axis before').toBe('xaxis');
      const before = JSON.stringify(leafOf(id).getBoundingClientRect());
      // Maximized, with no frame of the page's between it and the press.
      page.ws.maximize([id]);
      flushSync();
      expect(JSON.stringify(leafOf(id).getBoundingClientRect()), 'the plot fills the tab').not.toBe(before);
      expect(document.elementFromPoint(...at), 'the plot is under the pointer').toBe(pv.overlay);
      expect(pv.pressAt(...at)?.part, 'a press lands on the plot area').toBe('plot');
      // A right-click there opens the plot's menu, not the x axis's popover.
      await commands.mouse([['move', ...at], ['down', 'right'], ['up', 'right']]);
      expect(await until(() => document.querySelector(OPEN)), 'a menu opened').toBe(true);
      expect(document.querySelector('[data-slot=context-menu-content]'), "the plot's menu").not.toBe(null);
      expect(document.querySelector('[data-slot=popover-content]'), "the axis's popover").toBe(null);
    } finally {
      page.dispose();
    }
  });
});

/** Whether boxes `a` and `b`, each {x, y, w, h}, overlap. */
const overlap = (a, b) => a.x < b.x + b.w && b.x < a.x + a.w && a.y < b.y + b.h && b.y < a.y + a.h;
/** The cursors' tags and tabs that show, of what plot view `pv` drew. */
const shown = (pv) => pv.pressable.filter((p) => p.text && (p.alpha ?? 1) > 0);
/** Puts the pointer over the plot at `at`, with no press, and has plot view `pv` draw the frame after. */
function hover(pv, canvas, at) {
  canvas.dispatchEvent(new PointerEvent('pointermove', { bubbles: true, clientX: at[0], clientY: at[1] }));
  pv.draw(clock.now());
}

/** How wide plot view `pv`, of `kind`, draws the pointer's value on y axis `key` with the pointer at client y `y`: its text, in the canvas's font, and 4 px each side. */
function pointerTagWidth(pv, canvas, key, y, kind) {
  const v = pv.yValueAt(key, y), ctx = document.createElement('canvas').getContext('2d');
  ctx.font = canvas.getContext('2d').font;
  return ctx.measureText(kind === 'spectrum' ? v.toExponential(2) : v.toPrecision(4)).width + 8;
}

describe('what follows the pointer over a plot', () => {
  it('the hover’s tip, beside the pointer, covers no cursor’s tag or tab, wherever the pointer is about one', async () => {
    const m = await mounted(), { page, id, canvas } = m;
    const count = { tips: 0, about: 0, moved: 0, none: 0 };
    try {
      await fc.assert(
        fc.asyncProperty(plotCase, async (c) => {
          await arrange(m, c);
          const pv = page.pv(id), P = pv.geo.plot;
          for (const b of tagsAndTabs(pv).map(({ x, y, w, h }) => ({ x, y, w, h }))) {
            // On the tag, beside it and at its corners, and where a tip beside the pointer would reach it.
            const xs = [b.x - 150, b.x - 14, b.x + b.w / 2, b.x + b.w + 14], ys = [b.y - 14, b.y - 4, b.y + b.h / 2, b.y + b.h + 4, b.y + b.h + 14];
            for (const x of xs)
              for (const y of ys) {
                if (x < P.x || x > P.x + P.w || y < P.y || y > P.y + P.h) continue;
                hover(pv, canvas, [x, y]);
                const tip = pv.pressable.find((p) => p.part === 'tip');
                count.about++;
                if (!tip) {
                  count.none++;
                  continue;
                }
                for (const tag of shown(pv)) expect(overlap(tip, tag), `${c.kind} ${c.layout}: with the pointer at ${[x, y].map(Math.round)}, the tip ${JSON.stringify(tip)} covers ${nameOf(tag)} ${JSON.stringify([tag.x, tag.y, tag.w, tag.h])}`).toBe(false);
                expect(pv.pressAt(tip.x + tip.w / 2, tip.y + tip.h / 2)?.part, 'a press goes through the tip').not.toBe('tip');
                count.tips++;
                // Above and right of the pointer is where it sits with nothing in its way.
                if (tip.x !== x + 10 || tip.y !== y - 26) count.moved++;
              }
          }
          canvas.dispatchEvent(new PointerEvent('pointerleave', { bubbles: true }));
        }),
        { examples: EXAMPLES, numRuns: EXAMPLES.length + Math.ceil(fc.readConfigureGlobal().numRuns / 4) },
      );
      expect(count.tips, 'tips drawn').toBeGreaterThan(500);
      expect(count.moved, 'tips drawn somewhere other than above and right of the pointer').toBeGreaterThan(100);
      expect(count.none, 'hovers with no tip drawn, as with no signal under the pointer').toBeLessThan(count.about / 2);
    } finally {
      page.dispose();
    }
  }, 600_000);

  it('the pointer’s value on a y axis keeps its place as the pointer crosses a Y cursor a pixel at a time, and is not drawn where it would cover a cursor’s tag', async () => {
    const m = await mounted(), { page, id, canvas } = m;
    const count = { sweeps: 0, drawn: 0, hidden: 0, crossings: 0 };
    try {
      await fc.assert(
        fc.asyncProperty(plotCase, async (c) => {
          await arrange(m, c);
          const pv = page.pv(id);
          for (const line of pv.pressable.filter((p) => p.part === 'line' && p.kind === 'y').map(({ at, axis, name }) => ({ at, axis, name }))) {
            const lane = line.axis.lane, x = lane.x + lane.w / 2;
            // The pointer's tag is 16 px high and a cursor's 14: from clear above the cursor's to clear below it.
            const states = [];
            for (let y = Math.round(line.at) - 40; y <= Math.round(line.at) + 40; y++) {
              if (y < lane.y + 1 || y > lane.y + lane.h - 1) continue;
              hover(pv, canvas, [x, y]);
              const tags = shown(pv);
              for (const axis of pv.yAxes.filter((a) => a.lane.y === lane.y)) {
                const own = pv.pressable.find((p) => p.part === 'pointer' && p.key === axis.key), first = (axis.slot ?? 0) === 0;
                const where = `${c.kind} ${c.layout}: with the pointer at ${[x, y]}, crossing ${line.name} of ${line.axis.key}, the pointer's value on ${axis.key}`;
                if (own) {
                  for (const tag of tags) expect(overlap(own, tag), `${where} covers ${nameOf(tag)}`).toBe(false);
                  expect(own.y, `${where} is at the pointer's height`).toBe(y + 0.5 - 8);
                  expect(own.w, `${where} is as wide as its text`).toBeCloseTo(pointerTagWidth(pv, canvas, axis.key, y, c.kind), 3);
                  // Against its side of the lane, never moved in past a cursor's tag.
                  if (first) expect(axis.side === 'left' ? own.x : own.x + own.w, `${where} is against the lane's ${axis.side} side`).toBe(axis.side === 'left' ? lane.x + 1 : lane.x + lane.w - 1);
                  count.drawn++;
                } else {
                  // Left out only where a cursor's tag is in its place.
                  const w = pointerTagWidth(pv, canvas, axis.key, y, c.kind);
                  const place = { x: axis.side === 'left' ? lane.x + 1 : lane.x + lane.w - 1 - w, y: y + 0.5 - 8, w, h: 16 };
                  if (first) expect(tags.some((tag) => overlap(place, tag)), `${where} is not drawn, with no cursor's tag in its place ${JSON.stringify(place)}`).toBe(true);
                  count.hidden++;
                }
                if (axis.key === line.axis.key) states.push(own ? 'drawn' : 'hidden');
              }
            }
            // Each change from drawn to hidden and back, across the cursor's own tags and any other tag its sweep meets.
            count.crossings += states.filter((s, i) => i && s !== states[i - 1]).length;
            count.sweeps++;
          }
          canvas.dispatchEvent(new PointerEvent('pointerleave', { bubbles: true }));
        }),
        { examples: [...EXAMPLES, ...BESIDE_X_TAG], numRuns: EXAMPLES.length + BESIDE_X_TAG.length + Math.ceil(fc.readConfigureGlobal().numRuns / 4) },
      );
      for (const [what, n] of Object.entries(count)) expect(n, what).toBeGreaterThan(20);
    } finally {
      page.dispose();
    }
  }, 600_000);

  it('the pointer’s value is hidden over one stretch of heights about a lone Y cursor: drawn, hidden, drawn, with no height between where it shows again', async () => {
    const m = await mounted(), { page, id, canvas } = m;
    try {
      for (const layout of Object.keys(LAYOUTS)) {
        await arrange(m, { kind: 'time', layout, x: { a: null, b: null }, y: SIGNALS.map(() => ({ a: 0.4, b: null })), readoutAt: CORNERS[1] });
        const pv = page.pv(id);
        const lines = pv.pressable.filter((p) => p.part === 'line' && p.kind === 'y').map(({ at, axis }) => ({ at, axis }));
        expect(lines.length, `${layout}: a Y cursor on each axis`).toBe(pv.yAxes.length);
        for (const line of lines) {
          const lane = line.axis.lane, runs = [];
          for (let y = Math.round(line.at) - 40; y <= Math.round(line.at) + 40; y++) {
            hover(pv, canvas, [lane.x + lane.w / 2, y]);
            const state = pv.pressable.some((p) => p.part === 'pointer' && p.key === line.axis.key) ? 'drawn' : 'hidden';
            if (runs.at(-1)?.[0] === state) runs.at(-1)[1]++;
            else runs.push([state, 1]);
          }
          expect(runs.map((r) => r[0]), `${layout}, ${line.axis.key}: ${JSON.stringify(runs)}`).toEqual(['drawn', 'hidden', 'drawn']);
          // Hidden while the two tags, 16 px and 14 px high, would touch.
          expect(runs[1][1], `${layout}, ${line.axis.key}: the heights it is hidden over`).toBe(30);
        }
      }
      canvas.dispatchEvent(new PointerEvent('pointerleave', { bubbles: true }));
    } finally {
      page.dispose();
    }
  }, 600_000);
});
