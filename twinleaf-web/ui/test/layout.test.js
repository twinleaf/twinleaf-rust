import { describe, expect, it } from 'vitest';
import { append, dock, dockNode, dockOuter, leaf, leafSize, placeEdge, plotIds, presets, removeAll, removePlot, restrict, split, splitBeside } from '../src/lib/layout.js';

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
