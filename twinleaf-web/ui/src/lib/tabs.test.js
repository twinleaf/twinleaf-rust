import { describe, expect, it } from 'vitest';
import { SAVE_KEY, defaultContent, exportText, importText, joined, kindWith, loadTabs, makeTab, newPlot, remapIds, saveTabs, tabFromJson, yLogOf, yScaleField } from './tabs.js';
import { drawnKind } from './constants.js';
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

  it('keeps of each plot the signals its kind shows, with a capture plot’s settings', () => {
    const plots = {
      p1: { id: 'p1', kind: 'time', mode: 'overlay', items: [{ key: 'sine.sine' }, { key: 'capture:fid.capture' }] },
      p2: { id: 'p2', kind: 'capture', mode: 'overlay', auto: true, autoDelay: 0.2, shots: 'average', shotCount: 64, items: [{ key: 'sine.sine' }, { key: '0/capture:fid.capture' }] },
    };
    const back = tabFromJson({ name: 'A', plots, layout: null });
    expect(back.plots.p1.items.map((i) => i.key)).toEqual(['sine.sine']);
    expect(back.plots.p2.items.map((i) => i.key)).toEqual(['0/capture:fid.capture']);
    expect(back.plots.p2).toMatchObject({ kind: 'capture', auto: true, autoDelay: 0.2, shots: 'average', shotCount: 64 });
  });
});

describe('a plot and the signals given it', () => {
  const CAPTURES = ['capture:fid.capture', '0/capture:phase.capture'], SIGNALS = ['sine.sine', '0/aux.x'];
  const plot = (kind, keys) => newPlot(kind, 'overlay', keys);
  const keysOf = (p) => p?.items.map((i) => i.key);

  it('shows captures or signals that stream, never both: with some it takes only its own sort, and its kind stays', () => {
    for (const kind of ['time', 'spectrum']) {
      for (const key of CAPTURES) expect(kindWith(plot(kind, ['sine.sine']), key)).toBe(null);
      for (const key of SIGNALS) expect(kindWith(plot(kind, ['sine.sine']), key)).toBe(kind);
    }
    for (const key of SIGNALS) expect(kindWith(plot('capture', [CAPTURES[0]]), key)).toBe(null);
    for (const key of CAPTURES) expect(kindWith(plot('capture', [CAPTURES[0]]), key)).toBe('capture');
  });

  it('with none takes either: a capture makes it a capture plot, a signal a time plot again, or leaves a spectrum one', () => {
    for (const kind of ['time', 'spectrum', 'capture']) for (const key of CAPTURES) expect(kindWith(plot(kind, []), key)).toBe('capture');
    expect(SIGNALS.map((key) => kindWith(plot('capture', []), key))).toEqual(['time', 'time']);
    expect(SIGNALS.map((key) => kindWith(plot('spectrum', []), key))).toEqual(['spectrum', 'spectrum']);
  });

  it('joins those it lacks and can show, after its own, in the order given, the first deciding for an empty plot', () => {
    const mixed = [SIGNALS[0], CAPTURES[0], SIGNALS[1], CAPTURES[1]];
    expect(joined(plot('time', []), mixed)).toMatchObject({ kind: 'time' });
    expect(keysOf(joined(plot('time', []), mixed))).toEqual(SIGNALS);
    expect(keysOf(joined(plot('time', []), mixed.toReversed()))).toEqual(CAPTURES.toReversed());
    expect(joined(plot('time', []), mixed.toReversed()).kind).toBe('capture');
    expect(keysOf(joined(plot('capture', [CAPTURES[0]]), mixed))).toEqual(CAPTURES);
    // One it shows already is not added again; with none to add it stays as it is.
    expect(keysOf(joined(plot('time', [SIGNALS[1]]), mixed))).toEqual([SIGNALS[1], SIGNALS[0]]);
    expect(joined(plot('time', SIGNALS), mixed)).toBe(null);
    expect(joined(plot('time', SIGNALS), CAPTURES)).toBe(null);
  });

  it('draws a log y axis as its own scale says: a spectrum log until set, a capture plot linear until set, a time plot never', () => {
    const made = (kind, more = {}) => ({ ...plot(kind, []), ...more });
    expect([made('spectrum'), made('capture'), made('time')].map(yLogOf)).toEqual([true, false, false]);
    expect([made('spectrum', { yScale: 'linear' }), made('capture', { captureYScale: 'log' }), made('time', { yScale: 'log', captureYScale: 'log' })].map(yLogOf)).toEqual([false, true, false]);
    // Each holds its own: a capture plot's density scale is no scale of its values, and drawn as a spectrum it takes a spectrum's.
    expect([made('capture', { yScale: 'log' })].map(yLogOf)).toEqual([false]);
    const spectral = made('capture', { captureView: 'spectrum', captureYScale: 'linear' });
    expect([yLogOf(spectral), yLogOf({ ...spectral, yScale: 'linear' }), drawnKind(spectral), drawnKind(made('capture'))]).toEqual([true, false, 'spectrum', 'capture']);
    expect(['spectrum', 'capture', 'time'].map((kind) => yScaleField(made(kind)))).toEqual(['yScale', 'captureYScale', 'yScale']);
    expect(yScaleField(spectral)).toBe('yScale');
  });
});
