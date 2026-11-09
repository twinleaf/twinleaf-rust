import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { GUTTER, equal, leaf, plotIds, rects, removeAll, split, swap } from '../src/lib/layout.js';
import { MIN_PLOT, SHARE, SIDES, isColumn, moveCells } from '../src/lib/moves.js';

/** A layout tree of the plots `ids`, splits in either direction at any ratio. */
const tree = (ids) =>
  ids.length === 1
    ? fc.constant(leaf(ids[0]))
    : fc.integer({ min: 1, max: ids.length - 1 }).chain((cut) =>
        fc.tuple(fc.constantFrom('row', 'col'), fc.double({ min: 0.1, max: 0.9, noNaN: true }), tree(ids.slice(0, cut)), tree(ids.slice(cut))).map(([dir, r, a, b]) => split(dir, a, b, r)),
      );
const ids = [...'abcdef'];
const anyTree = fc.integer({ min: 2, max: 6 }).chain((n) => tree(ids.slice(0, n)));
const anyBox = fc.record({ x: fc.integer({ min: 0, max: 300 }), y: fc.integer({ min: 0, max: 100 }), w: fc.integer({ min: 400, max: 2400 }), h: fc.integer({ min: 300, max: 1400 }) });
/** A layout, its box, and a group of its plots to move. */
const anyMove = fc.tuple(anyTree, anyBox, fc.array(fc.nat(), { minLength: 1, maxLength: 3 })).map(([t, box, picks]) => {
  const all = plotIds(t);
  return { t, box, group: new Set(picks.map((k) => all[k % all.length])) };
});
const near = (a, b) => Math.abs(a - b) < 1e-6;

/** Runs `check(cell, move)` on every cell of every move tried, and fails if no more than `least` were checked. */
function everyCell(check, least = 200) {
  let n = 0;
  fc.assert(
    fc.property(anyMove, (move) => {
      for (const c of moveCells(move.t, move.box, move.group)) {
        check(c, move);
        n++;
      }
    }),
  );
  expect(n).toBeGreaterThan(least);
}

describe('the cells a plot move offers', () => {
  it('each change the layout, and keep every plot and only those', () => {
    everyCell((c, { t }) => {
      expect(equal(c.next, t)).toBe(false);
      expect(plotIds(c.next).sort()).toEqual(plotIds(t).sort());
    });
  });

  it('shrink no plot below the smallest plot', () => {
    everyCell((c, { t, box }) => {
      const was = rects(t, box);
      for (const [id, r] of rects(c.next, box)) {
        const w = was.get(id);
        if (r.w < w.w - 0.5) expect(r.w).toBeGreaterThanOrEqual(MIN_PLOT.w - 0.5);
        if (r.h < w.h - 0.5) expect(r.h).toBeGreaterThanOrEqual(MIN_PLOT.h - 0.5);
      }
    });
  });

  it('show as their preview the box the moved plots take in the layout they make', () => {
    everyCell((c, { box, group }) => {
      const rs = [...group].map((id) => rects(c.next, box).get(id));
      const x = Math.min(...rs.map((r) => r.x)), y = Math.min(...rs.map((r) => r.y));
      expect(near(c.preview.x, x) && near(c.preview.y, y)).toBe(true);
      expect(near(c.preview.w, Math.max(...rs.map((r) => r.x + r.w)) - x) && near(c.preview.h, Math.max(...rs.map((r) => r.y + r.h)) - y)).toBe(true);
    });
  });

  it('keep a group moved together in the order it reads', () => {
    everyCell((c, { t, group }) => {
      expect(plotIds(c.next).filter((id) => group.has(id))).toEqual(plotIds(t).filter((id) => group.has(id)));
    });
  });

  it('swap a plot moved alone with the plot whose middle it is dropped on, and offer a group no middle', () => {
    everyCell((c, { t, group }) => {
      if (c.edge !== 'center') return;
      expect(group.size).toBe(1);
      expect(equal(c.next, swap(t, [...group][0], c.plotId))).toBe(true);
    }, 50);
  });

  it('include a dock beside every plot whose room holds two plots along that side', () => {
    /** Whether `moved` stands alone beside `id` on `edge` already. */
    const there = (node, moved, id, edge) => {
      if (!node || node.type === 'leaf') return false;
      const dir = isColumn(edge) ? 'row' : 'col', first = edge === 'left' || edge === 'top';
      const [x, y] = first ? [node.a, node.b] : [node.b, node.a];
      if (node.dir === dir && x.plotId === moved && y.plotId === id) return true;
      return there(node.a, moved, id, edge) || there(node.b, moved, id, edge);
    };
    let checked = 0;
    fc.assert(
      fc.property(anyTree, anyBox, fc.nat(), (t, box, k) => {
        const all = plotIds(t), moved = all[k % all.length];
        const cells = moveCells(t, box, new Set([moved]));
        for (const [id, r] of rects(removeAll(t, new Set([moved])), box)) {
          for (const edge of Object.keys(SIDES)) {
            const [along, across] = isColumn(edge) ? ['w', 'h'] : ['h', 'w'];
            if (r[along] - GUTTER < 2 * MIN_PLOT[along] + 1 || r[across] < MIN_PLOT[across] + 1) continue;
            const offered = cells.some((c) => c.plotId === id && c.edge === edge);
            expect(offered || there(t, moved, id, edge), `${moved} beside ${id} on ${edge}`).toBe(true);
            checked++;
          }
        }
      }),
    );
    expect(checked).toBeGreaterThan(200);
  });

  it('give a plot moved beside another the size it remembers that way, to the px, within its share', () => {
    let checked = 0;
    fc.assert(
      fc.property(anyMove, fc.double({ min: 0.05, max: 0.6, noNaN: true }), fc.nat(), ({ t, box }, want, k) => {
        const all = plotIds(t), id = all[k % all.length];
        for (const c of moveCells(t, box, new Set([id]), { w: want, h: want })) {
          if (c.edge === 'center') continue;
          const dim = isColumn(c.edge) ? 'w' : 'h';
          const after = rects(c.next, box), got = after.get(id)[dim];
          // What it shares is the split it made, along the edge's axis, less the gap.
          const room = (c.outer ? box[dim] : got + GUTTER + after.get(c.plotId)[dim]) - GUTTER;
          const share = (want * box[dim]) / room;
          const lo = Math.max(SHARE[0], MIN_PLOT[dim] / room), hi = Math.min(SHARE[1], 1 - MIN_PLOT[dim] / room);
          if (share > lo && share < hi) {
            expect(Math.abs(got - want * box[dim])).toBeLessThan(0.5);
            checked++;
          }
        }
      }),
    );
    expect(checked).toBeGreaterThan(100);
  });

  it('bring a plot moved to the far edge of the layout and back to the size it had', () => {
    let checked = 0;
    fc.assert(
      fc.property(anyBox, fc.constantFrom('row', 'col'), fc.double({ min: 0.25, max: 0.75, noNaN: true }), (box, dir, r) => {
        const t = split(dir, leaf('a'), split(dir === 'row' ? 'col' : 'row', leaf('b'), leaf('c')), r);
        const dim = dir === 'row' ? 'w' : 'h';
        const size = (tree) => ({ [dim]: rects(tree, box).get('a')[dim] / box[dim] });
        const [there, back] = dir === 'row' ? ['right', 'left'] : ['bottom', 'top'];
        // A size the move need not fit: within SHARE of the layout, and leaving the rest at least the smallest plot.
        const room = box[dim] - GUTTER, f = rects(t, box).get('a')[dim] / room;
        if (f <= Math.max(SHARE[0], MIN_PLOT[dim] / room) || f >= Math.min(SHARE[1], 1 - MIN_PLOT[dim] / room)) return;
        const out = moveCells(t, box, new Set(['a']), size(t)).find((c) => c.outer && c.edge === there);
        const home = out && moveCells(out.next, box, new Set(['a']), size(out.next)).find((c) => c.outer && c.edge === back);
        if (!home) return;
        expect(Math.abs(rects(home.next, box).get('a')[dim] - rects(t, box).get('a')[dim])).toBeLessThan(0.5);
        checked++;
      }),
    );
    expect(checked).toBeGreaterThan(20);
  });

  it('keep a plot moved back and forth beside the same plot at the size it had', () => {
    // A short plot under a tall one, moved above it and back: no gap is
    // lost or gained in each move.
    const box = { x: 0, y: 0, w: 1000, h: 1007 };
    let t = split('col', leaf('big'), leaf('small'), 0.8);
    const start = rects(t, box).get('small').h;
    for (let i = 0; i < 8; i++) {
      const size = { h: rects(t, box).get('small').h / box.h };
      const edge = plotIds(t)[0] === 'small' ? 'bottom' : 'top';
      t = moveCells(t, box, new Set(['small']), size).find((c) => c.plotId === 'big' && c.edge === edge).next;
    }
    expect(Math.abs(rects(t, box).get('small').h - start)).toBeLessThan(0.5);
  });

  it('let the plots of a 2 by 2 grid, 672 px wide, take the place of or dock beside those below', () => {
    const t = split('col', split('row', leaf('a'), leaf('b')), split('row', leaf('c'), leaf('d')));
    const box = { x: 0, y: 0, w: 672, h: 800 };
    const cells = moveCells(t, box, new Set(['a']), { w: rects(t, box).get('a').w / box.w });
    for (const id of ['c', 'd']) for (const edge of ['center', 'left', 'right']) expect(cells.some((c) => c.plotId === id && c.edge === edge), `${id} ${edge}`).toBe(true);
  });
});
