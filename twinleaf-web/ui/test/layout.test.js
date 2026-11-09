import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { append, dock, dockNode, dockOuter, gutters, leaf, leafSize, placeEdge, plotIds, presets, rects, removeAll, removePlot, restrict, setRatio, split, splitBeside } from '../src/lib/layout.js';

describe('the layout tree', () => {
  it('lists its plots in reading order', () => {
    expect(plotIds(presets.grid(['a', 'b', 'c', 'd']))).toEqual(['a', 'b', 'c', 'd']);
    expect(plotIds(null)).toEqual([]);
  });

  it('closes a plot by giving its sibling the split', () => {
    const tree = split('row', leaf('a'), leaf('b'));
    expect(removePlot(tree, 'a')).toEqual(tree.b);
    expect(removePlot(leaf('a'), 'a')).toBeNull();
  });

  it('docks a plot on an edge of another', () => {
    const tree = dock(split('row', leaf('a'), leaf('b')), 'a', 'b', 'top');
    expect(plotIds(tree)).toEqual(['a', 'b']);
    expect(tree.dir).toBe('col');
  });

  it('takes the place of another plot as a list reorders: after it from before, before it from after', () => {
    const take = (root, id, target) => {
      const order = plotIds(root), rest = removeAll(root, new Set([id]));
      return plotIds(dockNode(rest, leaf(id), target, placeEdge(root, target, order.indexOf(id) > order.indexOf(target))));
    };
    const rows = presets.rows(['a', 'b', 'c']);
    expect(take(rows, 'a', 'c')).toEqual(['b', 'c', 'a']);
    expect(take(rows, 'c', 'a')).toEqual(['c', 'a', 'b']);
    expect(take(rows, 'a', 'b')).toEqual(['b', 'a', 'c']);
    // Two plots alone in a column reorder within it, though moving one leaves the other alone there.
    const grid = split('row', leaf('x'), split('col', leaf('a'), leaf('b')));
    expect(placeEdge(grid, 'a', true)).toBe('top');
    expect(take(grid, 'b', 'a')).toEqual(['x', 'b', 'a']);
  });

  it('places along the split that holds the plot, side by side for a plot alone', () => {
    expect(placeEdge(presets.rows(['a', 'b']), 'b', true)).toBe('top');
    expect(placeEdge(presets.columns(['a', 'b']), 'b', false)).toBe('right');
    expect(placeEdge(leaf('a'), 'a', true)).toBe('left');
  });

  it('splits beside a plot and appends to the whole', () => {
    expect(plotIds(splitBeside(leaf('a'), 'a', 'b', 'row'))).toEqual(['a', 'b']);
    const three = append(append(leaf('a'), 'b'), 'c');
    expect(plotIds(three)).toEqual(['a', 'b', 'c']);
    expect(three.ratio).toBeCloseTo(2 / 3);
  });

  it('moves a group as its arrangement, beside a plot or along an edge', () => {
    const tree = presets.grid(['a', 'b', 'c', 'd']);
    const group = new Set(['a', 'b']);
    const moved = restrict(tree, group);
    expect(plotIds(moved)).toEqual(['a', 'b']);
    const rest = removeAll(tree, group);
    expect(plotIds(rest)).toEqual(['c', 'd']);
    expect(plotIds(dockNode(rest, moved, 'd', 'right', 0.3))).toEqual(['c', 'd', 'a', 'b']);
    const outer = dockOuter(rest, moved, 'left', 0.25);
    expect(plotIds(outer)).toEqual(['a', 'b', 'c', 'd']);
    expect(outer.ratio).toBe(0.25);
  });
});

/** A layout tree of the plots `ids`, splits in either direction at any ratio. */
const tree = (ids) =>
  ids.length === 1
    ? fc.constant(leaf(ids[0]))
    : fc.integer({ min: 1, max: ids.length - 1 }).chain((cut) =>
        fc.tuple(fc.constantFrom('row', 'col'), fc.double({ min: 0.05, max: 0.95, noNaN: true }), tree(ids.slice(0, cut)), tree(ids.slice(cut))).map(([dir, ratio, a, b]) => split(dir, a, b, ratio)),
      );
const anyTree = fc.integer({ min: 1, max: 7 }).chain((n) => tree([...'abcdefg'].slice(0, n)));
const anyBox = fc.record({ x: fc.integer({ min: -50, max: 50 }), y: fc.integer({ min: -50, max: 50 }), w: fc.integer({ min: 300, max: 2000 }), h: fc.integer({ min: 300, max: 2000 }) });
const near = (a, b) => Math.abs(a - b) < 1e-6;
const sameBox = (a, b) => near(a.x, b.x) && near(a.y, b.y) && near(a.w, b.w) && near(a.h, b.h);

describe('rects', () => {
  it('tiles the box with its plots and the gaps between them, one box per plot', () => {
    fc.assert(
      fc.property(anyTree, anyBox, fc.integer({ min: 0, max: 12 }), (t, box, gap) => {
        const rs = rects(t, box, gap);
        expect([...rs.keys()]).toEqual(plotIds(t));
        const covered = [...rs.values()].reduce((s, r) => s + r.w * r.h, 0) + gutters(t, box, gap).reduce((s, g) => s + g.w * g.h, 0);
        // Gaps cross: a split's gap spans only that split's box, so plots and gaps never overlap and fill it.
        expect(near(covered, box.w * box.h)).toBe(true);
        // A box too small for its layout gives some plots no size; the rest lie inside it.
        if ([...rs.values()].some((r) => r.w < 0 || r.h < 0)) return;
        for (const r of rs.values()) {
          expect(r.x >= box.x - 1e-9 && r.y >= box.y - 1e-9 && r.x + r.w <= box.x + box.w + 1e-9 && r.y + r.h <= box.y + box.h + 1e-9).toBe(true);
        }
      }),
    );
  });

  it('is linear in the box and the gap, so the page can draw it with calc()', () => {
    fc.assert(
      fc.property(anyTree, anyBox, fc.integer({ min: 0, max: 12 }), (t, box, gap) => {
        const unit = rects(t, { x: 0, y: 0, w: 1, h: 1 }, 0), per = rects(t, { x: 0, y: 0, w: 0, h: 0 }, 1);
        for (const [id, r] of rects(t, box, gap)) {
          const u = unit.get(id), p = per.get(id);
          expect(near(r.x, box.x + u.x * box.w + p.x * gap)).toBe(true);
          expect(near(r.y, box.y + u.y * box.h + p.y * gap)).toBe(true);
          expect(near(r.w, u.w * box.w + p.w * gap)).toBe(true);
          expect(near(r.h, u.h * box.h + p.h * gap)).toBe(true);
        }
      }),
    );
  });

  it('gives a split the gap between its sides, at its ratio of what the gap leaves', () => {
    const t = split('row', leaf('a'), split('col', leaf('b'), leaf('c'), 0.25), 0.6);
    const rs = rects(t, { x: 0, y: 0, w: 1007, h: 807 }, 7);
    expect(rs.get('a')).toEqual({ x: 0, y: 0, w: 600, h: 807 });
    expect(rs.get('b')).toEqual({ x: 607, y: 0, w: 400, h: 200 });
    expect(rs.get('c')).toEqual({ x: 607, y: 207, w: 400, h: 600 });
    expect(gutters(t, { x: 0, y: 0, w: 1007, h: 807 }, 7).map(({ path, x, y, w, h }) => ({ path, x, y, w, h }))).toEqual([
      { path: '', x: 600, y: 0, w: 7, h: 807 },
      { path: 'b', x: 607, y: 200, w: 400, h: 7 },
    ]);
  });
});

describe('setRatio', () => {
  it('changes only the boxes inside the split it sets', () => {
    fc.assert(
      fc.property(anyTree, anyBox, fc.nat(), fc.double({ min: 0.05, max: 0.95, noNaN: true }), (t, box, k, ratio) => {
        const gs = gutters(t, box);
        if (!gs.length) return;
        const g = gs[k % gs.length];
        const before = rects(t, box), after = rects(setRatio(t, g.path, ratio), box);
        const under = new Set(plotIds([...g.path].reduce((n, side) => n[side], t)));
        for (const [id, r] of before) if (!under.has(id)) expect(sameBox(after.get(id), r)).toBe(true);
        expect(gutters(setRatio(t, g.path, ratio), box).find((x) => x.path === g.path).ratio).toBe(ratio);
      }),
    );
  });

  it('gives back the same tree when the split is at that ratio already', () => {
    fc.assert(
      fc.property(anyTree, fc.nat(), (t, k) => {
        const gs = gutters(t, { x: 0, y: 0, w: 1000, h: 1000 });
        if (gs.length) expect(setRatio(t, gs[k % gs.length].path, gs[k % gs.length].ratio)).toBe(t);
      }),
    );
  });
});

describe('leafSize', () => {
  it('gives each leaf its share of the area by the splits’ ratios', () => {
    const tree = split('row', leaf('a'), split('col', leaf('b'), leaf('c'), 0.25), 0.6);
    expect(leafSize(tree, 'a', 1000, 800)).toEqual({ width: 600, height: 800 });
    expect(leafSize(tree, 'b', 1000, 800)).toEqual({ width: 400, height: 200 });
    expect(leafSize(tree, 'c', 1000, 800)).toEqual({ width: 400, height: 600 });
    expect(leafSize(tree, 'z', 1000, 800)).toBeNull();
  });
  it('moving a plot to the other end of its column keeps its height', () => {
    // A short plot under a tall one, moved above it: measured in the layout
    // without it, its height is the same share of the column again.
    const tiles = { width: 1000, height: 1000 };
    let tree = split('col', leaf('big'), leaf('small'), 0.8);
    for (let i = 0; i < 6; i++) {
      const order = plotIds(tree), small = leafSize(tree, 'small', tiles.width, tiles.height).height / tiles.height;
      const rest = removeAll(tree, new Set(['small']));
      const room = leafSize(rest, 'big', tiles.width, tiles.height);
      tree = dockNode(rest, leaf('small'), 'big', order[0] === 'small' ? 'bottom' : 'top', (small * tiles.height) / room.height);
    }
    expect(leafSize(tree, 'small', 1000, 1000).height).toBeCloseTo(200);
  });
});
