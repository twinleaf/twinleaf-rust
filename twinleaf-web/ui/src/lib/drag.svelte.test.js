import { afterEach, describe, expect, it } from 'vitest';
import { Drags } from './drag.svelte.js';
import * as clock from './clock.js';
import { History } from './history.svelte.js';
import { Workspace } from './workspace.svelte.js';
import { Favorites } from './favorites.svelte.js';
import { makeTab, newPlot } from './tabs.js';
import { plotIds, presets } from './layout.js';

/** The virtual clock a test runs on. */
let time = null;
afterEach(() => time?.end());

describe('plots that land', () => {
  it('stay flashed for the whole flash, however soon after another they land', () => {
    time = clock.virtual();
    const drags = new Drags(null, () => {});
    drags.flash(['a']);
    time.advance(500);
    drags.flash(['b']);
    time.advance(500);
    expect(drags.landed).toEqual(new Set(['b']));
    time.advance(500);
    expect(drags.landed).toBe(null);
  });
});

function page(plots) {
  const ps = plots.map((keys) => newPlot('time', 'overlay', keys));
  const tab = makeTab('A', { plots: Object.fromEntries(ps.map((p) => [p.id, p])), layout: ps.length ? presets.columns(ps.map((p) => p.id)) : null });
  const ws = new Workspace({ tabs: [tab], activeId: tab.id });
  const history = new History(ws, new Favorites(null));
  return { ws, history, drags: new Drags(ws, () => {}, history), ids: ps.map((p) => p.id) };
}
const keysOf = (ws, id) => ws.plots[id]?.items.map((i) => i.key);

describe('a signal moved', () => {
  it('to another plot goes there, as one step', () => {
    const { ws, history, drags, ids: [a, b] } = page([['x', 'y'], ['z']]);
    const x = ws.plots[a].items[0];
    expect(drags.moveSignal(x, { plotId: b, items: [x, ...ws.plots[b].items] })).toBe(b);
    expect(keysOf(ws, a)).toEqual(['y']);
    expect(keysOf(ws, b)).toEqual(['x', 'z']);
    expect(history.undos.map((s) => s.label)).toEqual(['Move signal']);
    history.undo();
    expect(keysOf(ws, a)).toEqual(['x', 'y']);
    expect(keysOf(ws, b)).toEqual(['z']);
  });

  it('to a plot, with no place among its chips, goes after them', () => {
    const { ws, drags, ids: [a, b] } = page([['x', 'y'], ['z']]);
    drags.moveSignal(ws.plots[a].items[0], { plotId: b });
    expect(keysOf(ws, b)).toEqual(['z', 'x']);
  });

  it('closes the plot it was the last signal of, in the same step', () => {
    const { ws, history, drags, ids: [a, b] } = page([['x'], ['z']]);
    drags.moveSignal(ws.plots[a].items[0], { plotId: b });
    expect(plotIds(ws.layout)).toEqual([b]);
    expect(history.undos.length).toBe(1);
    history.undo();
    expect(plotIds(ws.layout)).toEqual([a, b]);
  });

  it('to a plot that shows it already leaves both plots as one, less it where it came from', () => {
    const { ws, drags, ids: [a, b] } = page([['x', 'y'], ['x']]);
    drags.moveSignal(ws.plots[a].items[0], { plotId: b });
    expect(keysOf(ws, b)).toEqual(['x']);
    expect(keysOf(ws, a)).toEqual(['y']);
  });

  it('to its own plot changes nothing and makes no step', () => {
    const { ws, history, drags, ids: [a] } = page([['x', 'y'], ['z']]);
    expect(drags.moveSignal(ws.plots[a].items[0], { plotId: a })).toBe(a);
    expect(keysOf(ws, a)).toEqual(['x', 'y']);
    expect(history.undos.length).toBe(0);
  });

  it('beside a plot’s edge opens a plot there with it, as one step with its leaving', () => {
    const { ws, history, drags, ids: [a, b] } = page([['x', 'y'], ['z']]);
    const to = drags.moveSignal(ws.plots[a].items[0], { plotId: b, edge: 'bottom' });
    expect(plotIds(ws.layout)).toEqual([a, b, to]);
    expect(keysOf(ws, to)).toEqual(['x']);
    expect(keysOf(ws, a)).toEqual(['y']);
    expect(history.undos.length).toBe(1);
  });

  it('onto an empty tab opens its first plot', () => {
    const { ws, drags } = page([]);
    const to = drags.moveSignal({ id: 'pal:w', key: 'w' }, { outer: true });
    expect(plotIds(ws.layout)).toEqual([to]);
    expect(keysOf(ws, to)).toEqual(['w']);
  });

  it('from the signal list gets a chip of its own', () => {
    const { ws, drags, ids: [a] } = page([['x'], ['z']]);
    drags.moveSignal({ id: 'pal:w', key: 'w' }, { plotId: a });
    expect(keysOf(ws, a)).toEqual(['x', 'w']);
    expect(ws.plots[a].items[1].id.startsWith('pal:')).toBe(false);
  });

  it('beside an independent plot’s side joins that side’s axes', () => {
    const { ws, drags, ids: [a, b] } = page([['x', 'y'], ['z']]);
    drags.moveSignal(ws.plots[a].items[0], { plotId: b, edge: 'axis', side: 'right' });
    expect(keysOf(ws, b)).toEqual(['z', 'x']);
    expect(ws.plots[b].axisSides).toEqual({ x: 'right' });
    expect(keysOf(ws, a)).toEqual(['y']);
  });
});

describe('a signal drag let go', () => {
  it('lands where the pointer was, whichever zone the drag library names', () => {
    const { ws, drags, ids: [a, b] } = page([['x', 'y'], ['z']]);
    const x = ws.plots[a].items[0];
    // Let go over b a moment after leaving a's chips: the library still names a's.
    drags.traceDrop = { plotId: b, edge: 'add' };
    expect(drags.landSignal(x, a, [...ws.plots[a].items])).toBe(b);
    expect(keysOf(ws, b)).toEqual(['z', 'x']);
    expect(drags.traceDrop).toBe(null);
  });

  it('among the chips of the plot it was let go over takes its place there', () => {
    const { ws, drags, ids: [a] } = page([['x', 'y', 'w'], ['z']]);
    const [x, y, w] = ws.plots[a].items;
    drags.traceDrop = { plotId: a, edge: 'add' };
    drags.landSignal(x, a, [y, x, w]);
    expect(keysOf(ws, a)).toEqual(['y', 'x', 'w']);
  });

  it('nowhere on the tiles changes nothing', () => {
    const { ws, history, drags, ids: [a] } = page([['x'], ['z']]);
    expect(drags.landSignal(ws.plots[a].items[0], a, [...ws.plots[a].items])).toBe(null);
    expect(keysOf(ws, a)).toEqual(['x']);
    expect(history.undos.length).toBe(0);
  });

  it('lands once: the next finalize of the same drop finds nothing to land', () => {
    const { ws, history, drags, ids: [a, b] } = page([['x', 'y'], ['z']]);
    const x = ws.plots[a].items[0];
    drags.traceDrop = { plotId: b, edge: 'bottom' };
    drags.landSignal(x);
    expect(drags.landSignal(x)).toBe(null);
    expect(history.undos.length).toBe(1);
  });
});
