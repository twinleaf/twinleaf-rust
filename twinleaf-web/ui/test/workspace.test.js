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

  it('adds a signal to the focused plot, or a new plot when the tab has none', () => {
    const ws = workspace();
    ws.selectTab(ws.tabs[1].id);
    ws.addToFocused('sine.sine');
    expect(plotIds(ws.layout).length).toBe(1);
    ws.addToFocused('sine.sine');
    expect(ws.plots[ws.focusedId].items.map((i) => i.key)).toEqual(['sine.sine']);
  });

  it('names the signals the open tab shows as spectra', () => {
    const ws = workspace();
    expect(ws.spectrumKeys().sort()).toEqual(['aux.triangle', 'sine.cosine', 'sine.sine']);
    ws.addPlot(newPlot('spectrum', 'overlay', ['x.y']));
    expect(ws.spectrumKeys()).toContain('x.y');
  });
});
