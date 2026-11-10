// Where a plot draws its cursors, read from what its PlotView last drew:
// the box of each cursor's tag (its name and its value, or the tag at the
// plot's edge of one out of view), each tick label's box, and the stub
// each cursor line runs on by past the plot's edge. For any cursors, on
// plots of every mode and kind, with axes on either side: no tag lies over
// a tick label, every tag is inside the plot area, and each stub is in the
// axis's gutter beside the plot, short of the tick labels, so the line can
// be set against a tick.
import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { editCursors, feed, mountPage, settle, wait, withExamples } from './harness/page.js';
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
      for (const id of ids) page.views.get(id).pause(performance.now());
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
            expect(pv.cursorTags.length > 0, `plot ${n} draws a tag for each cursor placed`).toBe(pv.cursorsPlaced() > 0);
            for (const tag of pv.cursorTags) {
              for (const label of pv.tickLabels) expect(overlap(tag, label), `plot ${n}: tag ${JSON.stringify(tag)} over tick label ${JSON.stringify(label)}`).toBe(false);
              expect(tag.x >= P.x - 0.5 && tag.x + tag.w <= P.x + P.w + 0.5 && tag.y >= P.y - 0.5 && tag.y + tag.h <= P.y + P.h + 0.5, `plot ${n}: tag ${JSON.stringify(tag)} inside the plot ${JSON.stringify(P)}`).toBe(true);
              count.tags++;
            }
            count.edge += pv.tabs.filter((t) => t.placed).length;
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
