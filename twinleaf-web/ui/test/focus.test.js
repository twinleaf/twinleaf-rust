import { describe, expect, it } from 'vitest';
import { deepest, down, region, step } from '../src/lib/focus.js';

/**
 * A tree from nested arrays of names: ['toolbar', ['live', 'clear']] is a
 * region `toolbar` with children `live` and `clear`. `last` maps a node to
 * the child it last had.
 */
function treeOf(spec, last = {}) {
  const parent = new Map(), children = new Map([[null, []]]);
  const add = (items, p) => {
    for (let i = 0; i < items.length; i++) {
      if (Array.isArray(items[i])) continue;
      const name = items[i];
      parent.set(name, p);
      children.set(name, []);
      children.get(p).push(name);
      if (Array.isArray(items[i + 1])) add(items[i + 1], name);
    }
  };
  add(spec, null);
  return { parent: (n) => parent.get(n) ?? null, children: (n) => children.get(n) ?? [], last: (n) => last[n ?? ''] };
}

const PAGE = ['toolbar', ['live', 'window', 'clear'], 'left', ['devices', 'signals'], 'plots', ['p1', ['kind', 'split', 'close'], 'p2'], 'status', ['history']];

describe('step', () => {
  const tree = treeOf(PAGE);
  it('moves among siblings, wrapping at both ends', () => {
    expect(step(tree, 'split', 1)).toBe('close');
    expect(step(tree, 'close', 1)).toBe('kind');
    expect(step(tree, 'kind', -1)).toBe('close');
  });
  it('moves among regions at the top', () => {
    expect(step(tree, 'status', 1)).toBe('toolbar');
  });
  it('stays on an only child', () => {
    expect(step(tree, 'history', 1)).toBe('history');
  });
});

describe('down', () => {
  it('goes to the first child', () => {
    expect(down(treeOf(PAGE), 'p1')).toBe('kind');
  });
  it('goes back to the child the node last had', () => {
    expect(down(treeOf(PAGE, { p1: 'close' }), 'p1')).toBe('close');
  });
  it('has nowhere to go from a leaf', () => {
    expect(down(treeOf(PAGE), 'p2')).toBeNull();
  });
});

describe('region', () => {
  const tree = treeOf(PAGE);
  it('steps from the region holding the node', () => {
    expect(region(tree, 'split', 1)).toBe('status');
    expect(region(tree, 'live', -1)).toBe('status');
  });
  it('starts at the first or last region with no cursor', () => {
    expect(region(tree, null, 1)).toBe('toolbar');
    expect(region(tree, null, -1)).toBe('status');
  });
});

describe('deepest', () => {
  it('follows the child each node last had', () => {
    expect(deepest(treeOf(PAGE, { plots: 'p1', p1: 'split' }), 'plots')).toBe('split');
  });
  it('stops at a node with no remembered child, or one gone', () => {
    expect(deepest(treeOf(PAGE), 'plots')).toBe('plots');
    expect(deepest(treeOf(PAGE, { plots: 'p9' }), 'plots')).toBe('plots');
  });
});
