import { describe, expect, it } from 'vitest';
import { History } from './history.svelte.js';
import { Workspace } from './workspace.svelte.js';
import { Favorites } from './favorites.svelte.js';
import { emptyTab, makeTab } from './tabs.js';
import { plotIds } from './layout.js';

function setup() {
  const a = makeTab('A'), b = emptyTab('B');
  const ws = new Workspace({ tabs: [a, b], activeId: a.id });
  const favorites = new Favorites(null);
  const history = new History(ws, favorites);
  return { ws, favorites, history };
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

  it('make a group one step, however many changes it makes, a group inside it among them', () => {
    const { ws, history } = setup();
    history.group('Resize', () => {
      for (const r of [0.4, 0.3, 0.2]) history.group('Inner', () => ws.setRatio('', r));
    });
    expect(history.undos.length).toBe(1);
    expect(history.undoLabel).toBe('Resize');
    history.undo();
    expect(ws.layout.ratio).toBe(0.5);
  });

  it('put back what a group changed before it threw, and keep no step of it', () => {
    const { ws, history, favorites } = setup();
    const [p] = plotIds(ws.layout);
    const tabs = ws.tabs;
    ws.selected.add(p);
    expect(() =>
      history.group('Broken', () => {
        ws.closePlot(p);
        favorites.pin(['x'], true);
        throw new Error('no');
      }),
    ).toThrow('no');
    expect(ws.tabs).toBe(tabs);
    expect(favorites.names.size).toBe(0);
    expect([...ws.selected]).toEqual([p]);
    expect(history.undos.length).toBe(0);
    history.group('Resize', () => ws.setRatio('', 0.2));
    expect(history.undos.map((s) => s.label)).toEqual(['Resize']);
  });

  it('make no step of setting a field to what the plot already shows', () => {
    const { ws, history } = setup();
    const [p] = plotIds(ws.layout);
    history.group('Log scale', () => ws.setPlot(p, { yScale: 'log', xScale: 'log', legend: false }));
    expect(history.undos.length).toBe(0);
  });

  it('refuse a star changed outside any step', () => {
    const { favorites } = setup();
    expect(() => favorites.pin(['x'], true)).toThrow(/outside an undo step/);
  });

  it('refuse an edit made outside any step', () => {
    const { ws } = setup();
    expect(() => ws.setRatio('', 0.3)).toThrow(/outside an undo step/);
    expect(ws.layout.ratio).toBe(0.5);
  });

  it('make no step of a group that changes nothing', () => {
    const { ws, history } = setup();
    const [p] = plotIds(ws.layout);
    history.group('Same', () => ws.setPlot(p, { kind: ws.plots[p].kind }));
    history.group('There and back', () => {
      ws.setRatio('', 0.3);
      ws.setRatio('', 0.5);
    });
    expect(history.undos.length).toBe(0);
  });

  it('keep every step as it was, whatever comes after', () => {
    const { ws, history } = setup();
    const [p] = plotIds(ws.layout);
    history.group('Legend', () => ws.setPlot(p, { legend: true }));
    const after = ws.tabs;
    history.group('Mode', () => ws.setPlot(p, { mode: 'stacked' }));
    expect(history.undos[0].after.tabs).toBe(after);
    expect(after.find((t) => t.id === ws.activeId).plots[p].mode).not.toBe('stacked');
    expect(Object.isFrozen(after[0].plots[p])).toBe(true);
  });

  it('leave a view’s zoom out of steps, and keep it through an undo', () => {
    const { ws, history } = setup();
    const [p, q] = plotIds(ws.layout);
    history.group('Close', () => ws.closePlot(q));
    ws.setView(p, { fRange: [1, 10] });
    expect(history.undos.length).toBe(1);
    history.group('Zoom', () => ws.setView(p, { fRange: [2, 20] }));
    expect(history.undos.length).toBe(1);
    history.undo();
    expect(ws.plots[p].fRange).toEqual([2, 20]);
    expect(plotIds(ws.layout)).toContain(q);
  });

  it('take back pinning several properties', () => {
    const { favorites, history } = setup();
    history.group('Add 3 to Favorites', () => favorites.pin(['a', 'b', 'c'], true));
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
