import { describe, expect, it } from 'vitest';
import { SAVE_KEY, defaultContent, exportText, importText, loadTabs, makeTab, remapIds, saveTabs, tabFromJson } from '../src/lib/tabs.js';
import { plotIds } from '../src/lib/layout.js';

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

describe('a tab read from JSON', () => {
  it('is refused when its layout names a plot twice', () => {
    const p = { id: 'p1', kind: 'time', mode: 'overlay', items: [] };
    const layout = { type: 'split', dir: 'row', ratio: 0.5, a: { type: 'leaf', plotId: 'p1' }, b: { type: 'leaf', plotId: 'p1' } };
    expect(tabFromJson({ name: 'A', plots: { p1: p }, layout })).toBe(null);
  });
});
