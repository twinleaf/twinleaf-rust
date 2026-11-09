import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { append, dock, dockNode, dockOuter, equal, fromJson, gutters, leaf, plotIds, presets, ratio as normal, rects, removeAll, removePlot, restrict, setRatio, split, splitBeside, swap } from '../src/lib/layout.js';

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
        expect(gutters(setRatio(t, g.path, ratio), box).find((x) => x.path === g.path).ratio).toBe(normal(ratio));
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

describe('a layout read from JSON', () => {
  it('is the tree every edit makes: no ids, ratios to the step', () => {
    const saved = { type: 'split', id: 'n1-x', dir: 'row', ratio: 0.333333333, a: { type: 'leaf', id: 'n2-y', plotId: 'a' }, b: { type: 'leaf', id: 'n3-z', plotId: 'b' } };
    expect(fromJson(saved)).toEqual(split('row', leaf('a'), leaf('b'), 0.3333));
  });
  it('is refused when it is not a tree of plots', () => {
    for (const bad of [null, { type: 'leaf' }, { type: 'split', dir: 'row', ratio: 0.5, a: leaf('a') }, { type: 'split', dir: 'up', ratio: 0.5, a: leaf('a'), b: leaf('b') }, { type: 'split', dir: 'row', ratio: 'half', a: leaf('a'), b: leaf('b') }]) {
      expect(fromJson(bad)).toBe(null);
    }
  });
  it('reads back every tree as itself', () => {
    fc.assert(
      fc.property(anyTree, (t) => {
        expect(equal(fromJson(JSON.parse(JSON.stringify(t))), t)).toBe(true);
      }),
    );
  });
});

describe('equal', () => {
  it('holds between trees that read the same and not when any plot, direction or ratio differs', () => {
    fc.assert(
      fc.property(anyTree, fc.nat(), (t, k) => {
        expect(equal(t, JSON.parse(JSON.stringify(t)))).toBe(true);
        const gs = gutters(t, { x: 0, y: 0, w: 1, h: 1 });
        if (!gs.length) return;
        const g = gs[k % gs.length];
        expect(equal(setRatio(t, g.path, g.ratio > 0.5 ? 0.25 : 0.75), t)).toBe(false);
      }),
    );
  });
});

describe('swap', () => {
  it('puts two plots in each other’s boxes, moves no other, keeps every split, and undoes itself', () => {
    fc.assert(
      fc.property(anyTree, anyBox, fc.nat(), fc.nat(), (t, box, i, j) => {
        const all = plotIds(t), a = all[i % all.length], b = all[j % all.length];
        const s = swap(t, a, b), was = rects(t, box), now = rects(s, box);
        expect(equal(swap(s, a, b), t)).toBe(true);
        expect(gutters(s, box).map((g) => [g.path, g.dir, g.ratio])).toEqual(gutters(t, box).map((g) => [g.path, g.dir, g.ratio]));
        expect(now.get(a)).toEqual(was.get(b));
        expect(now.get(b)).toEqual(was.get(a));
        for (const id of all) if (id !== a && id !== b) expect(now.get(id)).toEqual(was.get(id));
        if (a === b) expect(s).toBe(t);
      }),
    );
  });
});
