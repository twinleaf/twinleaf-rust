import { describe, expect, it } from 'vitest';
import { SAVE_KEY, defaultContent, exportText, importText, loadTabs, makeTab, newPlot, remapIds, saveTabs, tabFromJson } from './tabs.js';
import { plotIds, presets } from './layout.js';

/** A localStorage of one Map. */
function storage() {
  const map = new Map();
  return { getItem: (k) => map.get(k) ?? null, setItem: (k, v) => map.set(k, String(v)), map };
}

describe('tabs', () => {
  it('start as one default tab when nothing is kept', () => {
    const { tabs, activeId } = loadTabs(storage());
    expect(tabs.length).toBe(1);
    expect(activeId).toBe(tabs[0].id);
    expect(plotIds(tabs[0].layout).length).toBe(4);
  });

  it('come back as they were kept, under the key outside tools read', () => {
    const s = storage();
    const a = makeTab('A'), b = makeTab('B');
    saveTabs(s, [a, b], b.id);
    expect(JSON.parse(s.map.get(SAVE_KEY)).activeId).toBe(b.id);
    const back = loadTabs(s);
    expect(back.tabs.map((t) => t.name)).toEqual(['A', 'B']);
    expect(back.activeId).toBe(b.id);
    expect(Object.values(back.tabs[0].plots).map((p) => p.items.map((i) => i.key))).toEqual(Object.values(a.plots).map((p) => p.items.map((i) => i.key)));
  });

  it('refuse a tab whose layout names a plot it does not have', () => {
    const t = makeTab('A');
    const broken = { ...t, plots: {} };
    expect(tabFromJson(broken)).toBeNull();
    expect(tabFromJson({ name: 3 })).toBeNull();
    expect(tabFromJson(t)).not.toBeNull();
  });

  it('give a copy fresh ids throughout', () => {
    const t = makeTab('A');
    const copy = remapIds(t);
    expect(copy.id).not.toBe(t.id);
    const old = new Set(Object.keys(t.plots));
    for (const id of plotIds(copy.layout)) {
      expect(old.has(id)).toBe(false);
      expect(copy.plots[id]).toBeDefined();
    }
  });

  it('go out to a file and back with the same plots', () => {
    const tabs = [makeTab('A', defaultContent())];
    const back = importText(exportText(tabs));
    expect(back.map((t) => t.name)).toEqual(['A']);
    expect(plotIds(back[0].layout).length).toBe(4);
    expect(() => importText('{"format": "other"}')).toThrow('not a Twinleaf layout file');
    expect(() => importText('{"format": "twinleaf-plot-layouts", "tabs": []}')).toThrow('no layouts');
  });
});

describe('a plot’s view kept with the tabs', () => {
  const X = { x: { a: 1001, b: 1002 }, y: { time: { shared: { a: 0.5, b: null } } } };
  const PAUSED = { live: false, window: 10, span: 4, right: 1003, pausedAt: 1005, pausedHistory: 600, yRanges: { time: { shared: [-1, 1] } } };
  /** Tabs of a time plot and a spectrum, both with time X cursors, saved with each view as `state(id)` gives it, then read back. */
  function kept(state) {
    const s = storage(), time = { ...newPlot('time', 'overlay', ['a.b']), cursors: X }, spectrum = { ...newPlot('spectrum', 'overlay', ['a.b']), cursors: X };
    const t = makeTab('A', { plots: { [time.id]: time, [spectrum.id]: spectrum }, layout: presets.rows([time.id, spectrum.id]) });
    saveTabs(s, [t], t.id, (id) => ({ state: () => ({ ...state(id), spectra: new Map() }) }));
    return { s, time: time.id, spectrum: spectrum.id, back: loadTabs(s) };
  }

  it('keeps a paused time plot’s view and its time X cursors, and only those', () => {
    const { time, spectrum, back } = kept(() => PAUSED);
    expect([...back.views], 'the paused views').toEqual([[time, PAUSED]]);
    expect(back.tabs[0].plots[time].cursors, 'the time plot’s cursors').toEqual(X);
    expect(back.tabs[0].plots[time].view, 'no view in the tab').toBeUndefined();
    expect(back.tabs[0].plots[spectrum].cursors, 'a spectrum keeps no time X cursors').toEqual({ y: X.y });
  });

  it('keeps nothing of a live view, and the plot loads with no time X cursors', () => {
    const { time, back } = kept(() => ({ ...PAUSED, live: true }));
    expect([...back.views]).toEqual([]);
    expect(back.tabs[0].plots[time].cursors).toEqual({ y: X.y });
  });

  it('keeps nothing of a view that does not hold together, nor in a file of tabs', () => {
    for (const bad of [{ ...PAUSED, span: 'x' }, { ...PAUSED, right: null }, { ...PAUSED, yRanges: 3 }]) {
      const { time, back } = kept(() => bad);
      expect([...back.views], JSON.stringify(bad)).toEqual([]);
      expect(back.tabs[0].plots[time].cursors).toEqual({ y: X.y });
    }
    const { s } = kept(() => PAUSED);
    const [t] = importText(exportText(JSON.parse(s.map.get(SAVE_KEY)).tabs));
    expect(Object.values(t.plots).map((p) => p.cursors), 'imported').toEqual([{ y: X.y }, { y: X.y }]);
  });
});

describe('a tab read from JSON', () => {
  it('is refused when its layout names a plot twice', () => {
    const p = { id: 'p1', kind: 'time', mode: 'overlay', items: [] };
    const layout = { type: 'split', dir: 'row', ratio: 0.5, a: { type: 'leaf', plotId: 'p1' }, b: { type: 'leaf', plotId: 'p1' } };
    expect(tabFromJson({ name: 'A', plots: { p1: p }, layout })).toBe(null);
  });
});
