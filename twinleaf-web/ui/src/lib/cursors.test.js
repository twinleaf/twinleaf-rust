import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { CursorEdits, cursorsFromJson, placedIn, withX, withY, withoutX, withoutY, xOf, yOf } from './cursors.js';
import { History } from './history.svelte.js';
import { Workspace } from './workspace.svelte.js';
import { Favorites } from './favorites.svelte.js';
import { loadTabs, makeTab, newPlot, saveTabs } from './tabs.js';
import { presets } from './layout.js';

describe('a plot’s cursors, as its tab keeps them', () => {
  it('hold X1 and X2 of each kind and Y1 and Y2 of each y axis of each kind, and nothing for what is not placed', () => {
    let c = withX(undefined, 'time', 'a', 12);
    c = withX(c, 'spectrum', 'b', 40);
    c = withY(c, 'time', 'shared', 'a', 0.5);
    expect(c).toEqual({ x: { a: 12, b: null }, f: { a: null, b: 40 }, y: { time: { shared: { a: 0.5, b: null } } } });
    expect([xOf(c, 'time'), xOf(c, 'spectrum'), yOf(c, 'time', 'shared'), yOf(c, 'spectrum', 'shared')]).toEqual([{ a: 12, b: null }, { a: null, b: 40 }, { a: 0.5, b: null }, { a: null, b: null }]);
    expect(placedIn(c, 'time')).toBe(2);
    expect(placedIn(c, 'spectrum')).toBe(2);
    c = withY(withX(withX(c, 'time', 'a', null), 'spectrum', 'b', null), 'time', 'shared', 'a', null);
    expect(c).toBeUndefined();
  });

  it('clear by kind and by axis, leaving the rest', () => {
    const c = { x: { a: 1, b: 2 }, f: { a: 3, b: null }, y: { time: { shared: { a: 1, b: null }, other: { a: null, b: 2 } }, spectrum: { shared: { a: 9, b: null } } } };
    expect(withoutX(c, 'time')).toEqual({ ...c, x: undefined });
    expect(withoutY(c, 'time', 'other').y).toEqual({ time: { shared: { a: 1, b: null } }, spectrum: c.y.spectrum });
    expect(withoutY(c, 'time').y).toEqual({ spectrum: c.y.spectrum });
    expect(withoutY(withoutX(withoutX(withoutY(c, 'time'), 'time'), 'spectrum'), 'spectrum')).toBeUndefined();
  });

  it('are never changed in place', () => {
    const c = Object.freeze({ x: Object.freeze({ a: 1, b: null }) });
    expect(withX(c, 'time', 'b', 2)).toEqual({ x: { a: 1, b: 2 } });
    expect(c).toEqual({ x: { a: 1, b: null } });
  });

  it('read from a file keep what holds together and drop the rest', () => {
    expect(cursorsFromJson({ x: { a: 1, b: 'two' }, f: 7, y: { time: { shared: { a: 0.1, b: 0.2 }, bad: { a: {} } }, spectrum: null }, more: 1 })).toEqual({ x: { a: 1, b: null }, y: { time: { shared: { a: 0.1, b: 0.2 } } } });
    for (const bad of [null, 3, 'x', [], { x: { a: NaN, b: Infinity } }, { y: { time: {} } }]) expect(cursorsFromJson(bad)).toBeUndefined();
    fc.assert(fc.property(fc.anything(), (v) => {
      const c = cursorsFromJson(v);
      // What it gives is what it would read again, so a saved tab reads the same twice.
      expect(cursorsFromJson(JSON.parse(JSON.stringify(c ?? null)))).toEqual(c);
    }));
  });

  it('are saved with the tab, and come back with it', () => {
    const p = { ...newPlot('time', 'overlay', ['sine.sine']), cursors: { x: { a: 5, b: 6 }, y: { time: { shared: { a: 1, b: null } } } } };
    const tab = makeTab('A', { plots: { [p.id]: p, q: { ...newPlot('time', 'overlay', []), id: 'q', cursors: { x: 'broken' } } }, layout: presets.rows([p.id, 'q']) });
    const map = new Map(), storage = { getItem: (k) => map.get(k) ?? null, setItem: (k, v) => map.set(k, v) };
    saveTabs(storage, [tab], tab.id);
    const back = loadTabs(storage).tabs[0];
    expect(back.plots[p.id].cursors).toEqual(p.cursors);
    expect('cursors' in back.plots.q).toBe(false);
  });
});

describe('cursor edits', () => {
  /** A tab of three time plots, its edit history and its CursorEdits. */
  const page = () => {
    const ps = [0, 1, 2].map(() => newPlot('time', 'overlay', []));
    const tab = makeTab('A', { plots: Object.fromEntries(ps.map((p) => [p.id, p])), layout: presets.rows(ps.map((p) => p.id)) });
    const ws = new Workspace({ tabs: [tab], activeId: tab.id });
    const history = new History(ws, new Favorites(null));
    return { ws, history, cursors: new CursorEdits(ws, history), ids: ps.map((p) => p.id) };
  };
  const X = (a, b = null) => ({ x: { a, b } });

  it('show a drag’s cursors while it moves them, and write them once, as one step, when it ends', () => {
    const { ws, history, cursors, ids } = page();
    for (const at of [1, 2, 3]) for (const id of ids.slice(0, 2)) cursors.draft(id, X(at));
    expect(ids.map((id) => cursors.get(id))).toEqual([X(3), X(3), undefined]);
    expect(history.undos.length, 'nothing written while it moves').toBe(0);
    expect(cursors.commit('Place X1')).toBe(true);
    expect(history.undos.map((s) => s.label)).toEqual(['Place X1 (2 plots)']);
    expect(ids.map((id) => ws.plots[id].cursors)).toEqual([X(3), X(3), undefined]);
    history.undo();
    expect(ids.map((id) => cursors.get(id))).toEqual([undefined, undefined, undefined]);
    history.redo();
    expect(ids.map((id) => cursors.get(id))).toEqual([X(3), X(3), undefined]);
  });

  it('write nothing for a drag given up, or one that ends where it began', () => {
    const { history, cursors, ids } = page();
    cursors.draft(ids[0], X(1));
    cursors.cancel();
    expect(cursors.get(ids[0])).toBeUndefined();
    cursors.set('Add X cursors', new Map([[ids[0], X(1, 2)]]));
    cursors.draft(ids[0], X(5, 2));
    cursors.draft(ids[0], X(1, 2));
    expect(cursors.commit('Move X1')).toBe(false);
    expect(history.undos.length).toBe(1);
  });

  it('clear the cursors of several plots as one step, and say whether any were placed', () => {
    const { ws, history, cursors, ids } = page();
    cursors.set('Add X cursors', new Map(ids.map((id) => [id, X(1, 2)])));
    expect(cursors.clear(ids)).toBe(true);
    expect(history.undos.length).toBe(2);
    expect(ids.map((id) => ws.plots[id].cursors)).toEqual([undefined, undefined, undefined]);
    expect(cursors.clear(ids)).toBe(false);
    expect(history.undos.length).toBe(2);
    history.undo();
    expect(ids.map((id) => ws.plots[id].cursors)).toEqual([X(1, 2), X(1, 2), X(1, 2)]);
  });

  it('take away the time X cursors of plots that follow live, and leave their others', () => {
    const { ws, history, cursors, ids } = page();
    const all = { x: { a: 1, b: 2 }, f: { a: 3, b: 4 }, y: { time: { shared: { a: 1, b: 2 } } } };
    cursors.set('Add', new Map([[ids[0], all], [ids[1], all]]));
    cursors.followed([ids[0], ids[2]]);
    expect(ids.map((id) => ws.plots[id].cursors)).toEqual([{ f: all.f, y: all.y }, all, undefined]);
    history.undo();
    expect(ws.plots[ids[0]].cursors).toEqual(all);
  });
});
