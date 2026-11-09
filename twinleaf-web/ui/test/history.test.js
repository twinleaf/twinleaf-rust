import { describe, expect, it } from 'vitest';
import { History, MERGE_MS } from '../src/lib/history.svelte.js';
import { Workspace } from '../src/lib/workspace.svelte.js';
import { Favorites } from '../src/lib/favorites.svelte.js';
import { emptyTab, makeTab } from '../src/lib/tabs.js';
import { plotIds } from '../src/lib/layout.js';

function setup() {
  const a = makeTab('A'), b = emptyTab('B');
  const ws = new Workspace({ tabs: [a, b], activeId: a.id });
  const favorites = new Favorites(null);
  let t = 0;
  const clock = { now: () => t, pass: (ms) => (t += ms) };
  const history = new History(ws, favorites, clock.now);
  return { ws, favorites, history, clock };
}

describe('undo and redo', () => {
  it('take back closing several plots as one step, and select them again', () => {
    const { ws, history } = setup();
    const [p, q, r] = plotIds(ws.layout);
    ws.selected.add(p);
    ws.selected.add(q);
    history.group('Close 2 plots', () => {
      ws.closePlot(p);
      ws.closePlot(q);
    });
    expect(plotIds(ws.layout)[0]).toBe(r);
    expect(plotIds(ws.layout)).not.toContain(p);
    expect(history.undoLabel).toBe('Close 2 plots');
    expect(history.undo()).toBe('Close 2 plots');
    expect(plotIds(ws.layout).slice(0, 3)).toEqual([p, q, r]);
    expect([...ws.selected].sort()).toEqual([p, q].sort());
    expect(history.redo()).toBe('Close 2 plots');
    expect(plotIds(ws.layout)).not.toContain(p);
    expect(ws.selected.size).toBe(0);
  });

  it('make a drag one step, however many changes it makes', () => {
    const { ws, history } = setup();
    history.begin('Resize');
    for (const r of [0.4, 0.3, 0.2]) {
      ws.layout.ratio = r;
      history.observe();
    }
    history.end();
    expect(history.undos.length).toBe(1);
    history.undo();
    expect(ws.layout.ratio).toBe(0.5);
  });

  it('join a change made just after a step to it, and keep later ones apart', () => {
    const { ws, history, clock } = setup();
    const [p] = plotIds(ws.layout);
    history.group('Legend', () => (ws.plots[p].legend = true));
    clock.pass(MERGE_MS / 2);
    ws.plots[p].mode = 'stacked';
    history.observe();
    expect(history.undos.length).toBe(1);
    clock.pass(MERGE_MS * 2);
    ws.plots[p].kind = 'spectrum';
    history.observe();
    expect(history.undos.length).toBe(2);
  });

  it('leave a view’s zoom out of steps, and keep it through an undo', () => {
    const { ws, history } = setup();
    const [p, q] = plotIds(ws.layout);
    history.group('Close', () => ws.closePlot(q));
    ws.plots[p].fRange = [1, 10];
    history.observe();
    expect(history.undos.length).toBe(1);
    history.undo();
    expect(ws.plots[p].fRange).toEqual([1, 10]);
  });

  it('ignore a pane’s rounding of its split', () => {
    const { ws, history } = setup();
    ws.layout.ratio = 0.5 + 1e-9;
    history.observe();
    expect(history.undos.length).toBe(0);
  });

  it('take back pinning several properties', () => {
    const { favorites, history } = setup();
    history.group('Pin 3 to Favorites', () => favorites.pin(['a', 'b', 'c'], true));
    history.undo();
    expect(favorites.names.size).toBe(0);
    history.redo();
    expect([...favorites.names].sort()).toEqual(['a', 'b', 'c']);
  });

  it('forget what redo would do once something new is done', () => {
    const { ws, history, clock } = setup();
    const [p, q] = plotIds(ws.layout);
    history.group('Close', () => ws.closePlot(p));
    history.undo();
    clock.pass(MERGE_MS * 2);
    history.group('Close', () => ws.closePlot(q));
    expect(history.redoLabel).toBeNull();
  });

  it('open the tab a step changed when it is undone', () => {
    const { ws, history } = setup();
    const [p] = plotIds(ws.layout);
    const first = ws.activeId;
    history.group('Close', () => ws.closePlot(p));
    ws.selectTab(ws.tabs[1].id);
    history.undo();
    expect(ws.activeId).toBe(first);
    expect(plotIds(ws.layout)).toContain(p);
  });

  it('do nothing when there is nothing to undo or redo', () => {
    const { history } = setup();
    expect(history.undo()).toBeNull();
    expect(history.redo()).toBeNull();
  });
});
