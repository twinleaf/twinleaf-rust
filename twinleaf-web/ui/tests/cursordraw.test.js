// Where a plot draws its cursors, read from what its PlotView last drew:
// the box of each cursor's tag (its name and its value, or the tag at the
// plot's edge of one out of view), each tick label's box, and the stub
// each cursor line runs on by past the plot's edge. For any cursors, on
// plots of every mode and kind, with axes on either side: no tag lies over
// another tag or a tick label, every tag is inside the plot area, and each stub is in the
// axis's gutter beside the plot, short of the tick labels, so the line can
// be set against a tick. And where the tabs of the Y cursors not yet placed
// rest: at the foot of each y axis, in plots of every height.
import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { commands } from 'vitest/browser';
import { editCursors, feed, frames, leafOf, mountPage, settle, until, wait, withExamples } from './harness/page.js';
import * as clock from '../src/lib/clock.js';
import { plotIds } from '../src/lib/layout.js';

const SIGNALS = ['sine.sine', 'aux.triangle'];
/** Whether boxes `a` and `b`, each {x, y, w, h}, overlap. */
const overlap = (a, b) => a.x < b.x + b.w && b.x < a.x + a.w && a.y < b.y + b.h && b.y < a.y + a.h;
/** Where along a cursor's axis it is put, as a share of what the axis shows: in view, out of it on either side, or not placed. */
const place = fc.option(fc.oneof(fc.double({ min: 0, max: 1, noNaN: true }), fc.constantFrom(-0.3, 1.3, 0, 1, 0.5)));
const pair = fc.record({ a: place, b: place });

describe('a plot’s cursors, as drawn', () => {
  it('have their tags inside the plot and clear of every tick label, and lines that run a little into the axis, short of its labels', async () => {
    // An overlay, an independent plot with an axis on the right, a stacked plot and a spectrum.
    const page = await mountPage([SIGNALS, SIGNALS, SIGNALS, SIGNALS], 'rows', 0, SIGNALS);
    const count = { tags: 0, stubs: 0, edge: 0, labels: 0 };
    try {
      feed(page.hub);
      const ids = plotIds(page.ws.layout);
      page.history.group('Prepare', () => {
        page.ws.setPlot(ids[1], { mode: 'independent', axisSides: { [SIGNALS[1]]: 'right' } });
        page.ws.setPlot(ids[2], { mode: 'stacked' });
        page.ws.setPlot(ids[3], { kind: 'spectrum' });
      });
      for (const id of ids) page.views.get(id).pause(clock.now());
      await wait(300);
      await settle();
      await fc.assert(
        fc.asyncProperty(fc.array(fc.record({ x: pair, y: pair }), { minLength: 4, maxLength: 4 }), async (cursors) => {
          ids.forEach((id, i) => {
            const pv = page.pv(id), kind = pv.config().kind, log = (v) => (pv.logX ? Math.log10(v) : v), unlog = (v) => (pv.logX ? 10 ** v : v);
            const along = ([lo, hi], f, to = (v) => v, from = (v) => v) => (f === null ? null : from(to(lo) + f * (to(hi) - to(lo))));
            const ylog = pv.logY ? [Math.log10, (v) => 10 ** v] : [];
            const x = { a: along(pv.xd, cursors[i].x.a, log, unlog), b: along(pv.xd, cursors[i].x.b, log, unlog) };
            const y = Object.fromEntries(pv.yAxes.map((axis) => [axis.key, { a: along(pv.yBounds(axis.key), cursors[i].y.a, ...ylog), b: along(pv.yBounds(axis.key), cursors[i].y.b, ...ylog) }]));
            editCursors(page, id, () => ({ [kind === 'spectrum' ? 'f' : 'x']: x, y: { [kind]: y } }));
          });
          await settle();
          for (const id of ids) {
            const pv = page.pv(id), P = pv.geo.plot, body = pv.body.getBoundingClientRect(), n = ids.indexOf(id);
            expect(pv.tickLabels.length, `plot ${n} labels its ticks`).toBeGreaterThan(3);
            count.labels += pv.tickLabels.length;
            const tags = pv.pressable.filter((p) => p.part === 'tag');
            expect(tags.length > 0, `plot ${n} draws a tag for each cursor placed`).toBe(pv.cursorsPlaced() > 0);
            for (const { x, y, w, h } of tags) {
              const tag = { x, y, w, h };
              for (const label of pv.tickLabels) expect(overlap(tag, label), `plot ${n}: tag ${JSON.stringify(tag)} over tick label ${JSON.stringify(label)}`).toBe(false);
              expect(tag.x >= P.x - 0.5 && tag.x + tag.w <= P.x + P.w + 0.5 && tag.y >= P.y - 0.5 && tag.y + tag.h <= P.y + P.h + 0.5, `plot ${n}: tag ${JSON.stringify(tag)} inside the plot ${JSON.stringify(P)}`).toBe(true);
              count.tags++;
            }
            tags.forEach((a, i) => tags.slice(i + 1).forEach((b) => expect(overlap(a, b), `plot ${n}: tag ${a.text} at ${[a.x, a.y]} over tag ${b.text} at ${[b.x, b.y]}`).toBe(false)));
            count.edge += tags.filter((t) => t.edge).length;
            for (const s of pv.cursorStubs) {
              const box = { x: Math.min(s.x0, s.x1) - 0.5, y: Math.min(s.y0, s.y1) - 0.5, w: Math.abs(s.x1 - s.x0) + 1, h: Math.abs(s.y1 - s.y0) + 1 };
              // Wholly beside the plot, in the body, and some px long.
              const beside = box.x + box.w <= P.x + 1 || box.x >= P.x + P.w - 1 || box.y >= P.y + P.h - 1;
              const within = box.x >= body.left && box.x + box.w <= body.right && box.y + box.h <= body.bottom;
              expect([beside, within, Math.hypot(s.x1 - s.x0, s.y1 - s.y0) >= 3], `plot ${n}: stub ${JSON.stringify(s)}`).toEqual([true, true, true]);
              for (const label of pv.tickLabels) expect(overlap(box, label), `plot ${n}: stub ${JSON.stringify(s)} through tick label ${JSON.stringify(label)}`).toBe(false);
              count.stubs++;
            }
          }
        }),
        // Every cursor in view; every one out of it, on each side; a pair at the ends of the axes.
        withExamples([0.5, -0.3, 1.3, 0, 1].map((f) => [Array.from({ length: 4 }, () => ({ x: { a: f, b: f === 0.5 ? 0.25 : f }, y: { a: f, b: f === 0.5 ? 0.8 : f } }))])),
      );
      for (const [what, n] of Object.entries(count)) expect(n, what).toBeGreaterThan(10);
    } finally {
      page.dispose();
    }
  }, 600_000);
});

describe('the tabs Y cursors are dragged out of', () => {
  it('rest at the foot of each y axis, Y2 above Y1, above the time labels and clear of the header, in every lane with room for them', async () => {
    const count = { both: 0, one: 0, none: 0, monitors: 0, reached: 0, right: 0 };
    // Plots from the tile area's whole height down to ones too short for their header, which floats over them, each layout among them.
    for (const rows of [1, 4, 7, 10]) {
      const page = await mountPage(Array.from({ length: rows }, () => SIGNALS), 'rows', 0, SIGNALS);
      try {
        feed(page.hub);
        const ids = plotIds(page.ws.layout);
        page.history.group('Prepare', () => {
          ids.forEach((id, i) => page.ws.setPlot(id, [{}, { mode: 'independent', axisSides: { [SIGNALS[1]]: 'right' } }, { mode: 'stacked' }, { kind: 'spectrum', mode: rows === 1 ? 'stacked' : 'overlay' }][(i + rows) % 4]));
        });
        for (const id of ids) page.views.get(id).pause(clock.now());
        await wait(300);
        await settle(6);
        // With none placed both tabs rest; with Y1 placed on every axis, Y2's alone.
        for (const placed of [false, true]) {
          for (const id of ids) {
            const pv = page.pv(id), kind = pv.config().kind;
            editCursors(page, id, () => (placed ? { y: { [kind]: Object.fromEntries(pv.yAxes.map((axis) => [axis.key, { a: (pv.yBounds(axis.key)[0] + pv.yBounds(axis.key)[1]) / 2, b: null }])) } } : undefined));
          }
          await commands.mouse([['move', 4, 4]]);
          await wait(100);
          await settle();
          for (const id of ids) {
            const pv = page.pv(id), P = pv.geo.plot, n = `${rows} rows, plot ${ids.indexOf(id)}${placed ? ', Y1 placed' : ''}`;
            const all = pv.pressable.filter((p) => p.part === 'tab'), xTabs = all.filter((t) => t.kind === 'x');
            expect(xTabs.length, `${n}: X1 and X2 rest`).toBe(2);
            if (leafOf(id).querySelector('.plot-card.monitor')) count.monitors++;
            for (const axis of pv.yAxes) {
              const lane = axis.lane, foot = lane.y + lane.h, tabs = all.filter((t) => t.kind === 'y' && t.key === axis.key), resting = placed ? ['b'] : ['a', 'b'];
              // The lane's height below the header, where that floats over the plot's top.
              const head = leafOf(id).querySelector('.plot-card.monitor .plot-head')?.getBoundingClientRect().bottom ?? -Infinity, room = foot - Math.max(lane.y, head);
              const where = `${n}, the axis of ${axis.key}, its lane ${Math.round(lane.h)} px high with ${Math.round(room)} px clear`;
              // A tab is 14 px high with 2 px about it: both where two fit, the first alone where one does, none in a lane shorter than that.
              const fit = room >= 34 ? resting : room >= 18 ? resting.slice(0, 1) : [];
              expect(tabs.map((t) => t.name), `${where}: the tabs that rest`).toEqual(fit);
              count[room >= 34 ? 'both' : room >= 18 ? 'one' : 'none']++;
              if (axis.side === 'right') count.right++;
              for (const tab of tabs) {
                const at = `${where}: ${tab.text} at ${[tab.x, tab.y].map(Math.round)}`;
                expect(tab.y >= Math.max(lane.y, head) && tab.y + tab.h <= foot, `${at} is within its lane, below the header`).toBe(true);
                expect(tab.x >= axis.x0 && tab.x + tab.w <= axis.x1, `${at} is on its axis, ${axis.x0} to ${axis.x1}`).toBe(true);
                // At the axis's outer edge, away from the plot.
                expect(axis.side === 'left' ? tab.x - axis.x0 : axis.x1 - tab.x - tab.w, `${at} is at its axis's outer edge`).toBe(2);
                expect(tab.y + tab.h <= P.y + P.h, `${at} is above the time axis`).toBe(true);
                for (const other of [...xTabs, ...pv.tickLabels.filter((l) => l.y >= P.y + P.h)]) expect(overlap(tab, other), `${at} is clear of the time axis's tabs and labels`).toBe(false);
              }
              // Each has its own place where both fit, Y1 at the foot and Y2 a row up, whether or not the other rests there.
              for (const tab of tabs) expect(foot - tab.y - tab.h, `${where}: ${tab.text} from the lane's foot`).toBe(tab.name === 'b' && room >= 34 ? 18 : 2);
              if (tabs.length === 2) expect(tabs[1].x, `${where}: Y2 is above Y1`).toBe(tabs[0].x);
              // Under the pointer each shows, with nothing of the page over it: not the header, floating or not.
              if (!placed)
                for (const tab of tabs) {
                  await commands.mouse([['move', tab.x + tab.w / 2, tab.y + tab.h / 2]]);
                  await until(() => getComputedStyle(leafOf(id).querySelector('.plot-head')).opacity === '1');
                  await frames(2);
                  const now = pv.pressable.find((t) => t.part === 'tab' && t.kind === 'y' && t.key === tab.key && t.name === tab.name);
                  expect(document.elementFromPoint(now.x + now.w / 2, now.y + now.h / 2), `${where}: ${tab.text} is under the pointer, clear of the header`).toBe(pv.overlay);
                  expect(now.alpha, `${where}: ${tab.text} shows under the pointer`).toBe(1);
                  count.reached++;
                }
            }
          }
        }
      } finally {
        page.dispose();
      }
    }
    for (const [what, n] of Object.entries(count)) expect(n, what).toBeGreaterThan(3);
  }, 600_000);
});
