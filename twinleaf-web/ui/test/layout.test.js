import { describe, expect, it } from 'vitest';
import { append, dock, dockNode, dockOuter, leaf, plotIds, presets, removeAll, removePlot, restrict, split, splitBeside, swap } from '../src/lib/layout.js';

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

  it('swaps two plots in place', () => {
    expect(plotIds(swap(presets.rows(['a', 'b', 'c']), 'a', 'c'))).toEqual(['c', 'b', 'a']);
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
