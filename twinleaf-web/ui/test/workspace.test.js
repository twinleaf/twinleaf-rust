import { describe, expect, it } from 'vitest';
import { Workspace } from '../src/lib/workspace.svelte.js';
import { emptyTab, makeTab, newPlot } from '../src/lib/tabs.js';
import { plotIds } from '../src/lib/layout.js';

function workspace() {
  const a = makeTab('A'), b = emptyTab('B');
  return new Workspace({ tabs: [a, b], activeId: a.id });
}

describe('the workspace', () => {
  it('moves a plot to another tab, out of the selection', () => {
    const ws = workspace();
    const [id] = plotIds(ws.layout);
    ws.selected.add(id);
    ws.movePlotToTab(id, ws.tabs[1].id);
    expect(ws.selected.has(id)).toBe(false);
    expect(plotIds(ws.tabs[1].layout)).toEqual([id]);
    expect(ws.allPlotIds().filter((x) => x === id).length).toBe(1);
  });

  it('always keeps a tab, and opens a neighbour when the open one closes', () => {
    const ws = workspace();
    const [a, b] = ws.tabs;
    ws.closeTab(a);
    expect(ws.activeId).toBe(b.id);
    ws.closeTab(ws.tabs[0]);
    expect(ws.tabs.length).toBe(1);
    expect(ws.layout).toBeNull();
  });

  it('names the signals the open tab shows as spectra', () => {
    const ws = workspace();
    expect(ws.spectrumKeys().sort()).toEqual(['aux.triangle', 'sine.cosine', 'sine.sine']);
    ws.addPlot(newPlot('spectrum', 'overlay', ['x.y']));
    expect(ws.spectrumKeys()).toContain('x.y');
  });
});

describe('what follows the open tab', () => {
  it('hears a change to one plot only where it reads that plot', async () => {
    const { flushSync } = await import('svelte');
    const { probe } = await import('./workspace.svelte.js');
    const p = probe();
    p.change();
    flushSync();
    expect(p.runs).toEqual({ layout: 1, other: 1, changed: 2 });
    p.dispose();
  });

  it('keeps a plot moved to the tab it is in', () => {
    const t = makeTab('A');
    const ws = new Workspace({ tabs: [t], activeId: t.id });
    const [p] = plotIds(ws.layout);
    ws.movePlotToTab(p, t.id);
    expect(plotIds(ws.layout)).toContain(p);
    expect(ws.plots[p]).toBeTruthy();
  });
});
