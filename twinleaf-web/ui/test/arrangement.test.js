// The Layout dropdown's check (round 1 "Menu values", the presets): after
// any arrangement of any number of plots, the tab names the first
// arrangement of the menu (Rows, Columns, Grid, Quadrants, One large) that
// lays it out the same; after a ratio moved, none.
import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { Workspace } from '../src/lib/workspace.svelte.js';
import { makeTab, newPlot } from '../src/lib/tabs.js';
import { presets } from '../src/lib/layout.js';
import { ARRANGEMENTS } from '../src/lib/constants.js';

/**
 * The arrangement checked after arranging `n` plots as `name`, by hand:
 * one plot is a leaf, which Rows names first; Grid of two is the row
 * Columns makes; Quadrants fills the tab to four, or more, as Grid.
 */
function checked(name, n) {
  if (name === 'quadrants') return 'grid';
  if (n === 1) return 'rows';
  if (name === 'grid' && n === 2) return 'columns';
  return name;
}

describe('the arrangement a tab is in', () => {
  it('is the first of the menu that lays it out the same, and none once a ratio moves', () => {
    const seen = new Set();
    fc.assert(
      fc.property(fc.integer({ min: 1, max: 6 }), fc.constantFrom(...ARRANGEMENTS.map(([n]) => n)), fc.option(fc.constantFrom(0.27, 0.73)), (n, name, ratio) => {
        const plots = Array.from({ length: n }, () => newPlot('time', 'overlay', []));
        const tab = makeTab('A', { plots: Object.fromEntries(plots.map((p) => [p.id, p])), layout: presets.rows(plots.map((p) => p.id)) });
        const ws = new Workspace({ tabs: [tab], activeId: tab.id });
        ws.arrange(name);
        const moved = ratio !== null && ws.layout.type === 'split';
        if (moved) ws.setRatio('', ratio);
        expect(ws.arrangement(), `${name} of ${n}${moved ? `, its ratio ${ratio}` : ''}`).toBe(moved ? null : checked(name, n));
        seen.add(moved ? 'none' : 'one');
      }),
    );
    expect(seen.size).toBe(2);
  });
});
