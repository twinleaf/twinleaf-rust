import { describe, expect, it } from 'vitest';
import { Trace } from '../src/lib/session.svelte.js';
import { axisSlots, blurred, PlotView, rowCounts, timeAxis, View } from '../src/lib/plot.js';

const meta = { id: 0, route: '/', stream: 'sine', column: 'sine', units: 'V', rate: 1000 };

/** A trace of `ys`, one sample per millisecond from time 0. */
function trace(ys) {
  const t = new Trace(meta);
  t.push(ys.map((_, i) => i / 1000), ys, false);
  return t;
}

// Ten rows over [0, 1): row 0 holds values from 0 up to 0.1.
const tenths = (y) => Math.floor(y * 10);

describe('rowCounts', () => {
  it('counts the samples in the window by row', () => {
    const t = trace([0.05, 0.05, 0.55, 0.95, 0.95, 0.95]);
    const { counts, max } = rowCounts(t, 0, 1, tenths, 10);
    expect([...counts]).toEqual([2, 0, 0, 0, 0, 1, 0, 0, 0, 3]);
    expect(max).toBe(3);
  });

  it('counts only the window, its end left out', () => {
    const t = trace([0.05, 0.15, 0.25, 0.35]);
    const { counts } = rowCounts(t, 0.001, 0.003, tenths, 10);
    expect([...counts.slice(0, 4)]).toEqual([0, 1, 1, 0]);
  });

  it('leaves out samples beyond the rows', () => {
    const t = trace([-0.5, 0.05, 0.05, 1.5, NaN]);
    const { counts, max } = rowCounts(t, 0, 1, tenths, 10);
    expect(counts.reduce((a, b) => a + b)).toBe(2);
    expect(max).toBe(2);
  });

  it('has nothing to show for fewer than two samples, or none in the rows', () => {
    expect(rowCounts(trace([0.5]), 0, 1, tenths, 10)).toBeNull();
    expect(rowCounts(trace([0.5, 0.5]), 5, 6, tenths, 10)).toBeNull();
    expect(rowCounts(trace([2, 3]), 0, 1, tenths, 10)).toBeNull();
  });

  it('a sine is fullest at its peaks', () => {
    const ys = Array.from({ length: 1000 }, (_, i) => 0.5 + 0.5 * Math.sin((2 * Math.PI * i) / 100));
    const { counts } = rowCounts(trace(ys), 0, 1, (y) => Math.min(9, tenths(y)), 10);
    expect(counts[0]).toBeGreaterThan(counts[5]);
    expect(counts[9]).toBeGreaterThan(counts[4]);
  });
});

describe('View.zoom', () => {
  /** A view on a hub whose newest sample is always at time 1000. */
  const view = () => {
    const v = new View({ now: () => 1000 + 0.15 });
    v.span = 10;
    return v;
  };
  const zoomOut = (v, steps, anchor = 0.5) => {
    for (let i = 0; i < steps; i++) v.zoom(1.25, anchor, 0);
  };

  it('stays paused however far it zooms out, at the widest span', () => {
    const v = view();
    v.pause(0);
    v.right = 990;
    zoomOut(v, 40);
    expect(v.live).toBe(false);
    expect(v.span).toBe(120);
  });

  it('keeps the point under the pointer where it is as it zooms', () => {
    const v = view();
    v.pause(0);
    v.right = 990;
    v.zoom(2, 0.5, 0);
    expect(v.range(0)).toEqual([975, 995]);
  });

  it('pauses when zooming in away from the newest edge, and stays paused', () => {
    const v = view();
    v.zoom(0.8, 0.5, 0);
    expect(v.live).toBe(false);
    v.zoom(0.8, 0.5, 0);
    expect(v.live).toBe(false);
  });

  it('stays live zooming in over the newest stretch', () => {
    const v = view();
    v.zoom(0.8, 0.9, 0);
    expect(v.live).toBe(true);
  });
});

describe('blurred', () => {
  it('spreads a lone row over its neighbours, fullest at the middle', () => {
    const out = blurred(new Uint32Array([0, 0, 0, 0, 4, 0, 0, 0, 0]), 1);
    expect(out[4]).toBe(1);
    expect(out[3]).toBeCloseTo(out[5]);
    expect(out[3]).toBeGreaterThan(out[2]);
    expect(out[2]).toBeGreaterThan(0);
  });
  it('leaves an empty column empty', () => {
    expect([...blurred(new Uint32Array(5), 2)]).toEqual([0, 0, 0, 0, 0]);
  });
});

describe('axisSlots', () => {
  it('alternates sides by default, two a side, none past four', () => {
    expect(axisSlots(['a', 'b', 'c', 'd', 'e'])).toEqual({ left: ['a', 'c'], right: ['b', 'd'] });
  });
  it('places each signal where it was put, a full side showing no more', () => {
    expect(axisSlots(['a', 'b', 'c'], { a: 'right', b: 'right', c: 'right' })).toEqual({ left: [], right: ['a', 'b'] });
    expect(axisSlots(['a', 'b'], { a: 'none' })).toEqual({ left: [], right: ['b'] });
  });
});

describe('View.zoom, paused, on a hub that stores more than a live window', () => {
  it('goes as wide as the history the hub stores', () => {
    const v = new View({ now: () => 1000, historySeconds: () => 1800 });
    v.pause(0);
    for (let i = 0; i < 60; i++) v.zoom(1.25, 1, 0);
    expect(v.span).toBe(1800);
  });
  it('stays within a live window while live', () => {
    const v = new View({ now: () => 1000, historySeconds: () => 1800 });
    for (let i = 0; i < 60; i++) v.zoom(1.25, 1, 0);
    expect(v.span).toBe(120);
  });
});

describe('A spectrum panned', () => {
  const spectrum = (xd, logX) => {
    const got = [];
    const pv = Object.assign(Object.create(PlotView.prototype), { xd, fExtent: [1, 1000], logX, onFrequencies: (a, b) => got.push([a, b]) });
    return { pv, got };
  };
  it('moves along a log axis by a factor, keeping its decades', () => {
    const { pv, got } = spectrum([10, 100], true);
    pv.panFrequencies(0.5);
    expect(got[0][0]).toBeCloseTo(10 ** 1.5);
    expect(got[0][1]).toBeCloseTo(10 ** 2.5);
  });
  it('stops at the frequencies it has bins for', () => {
    const { pv, got } = spectrum([100, 1000], true);
    pv.panFrequencies(0.5);
    expect(got).toEqual([]);
    const lin = spectrum([800, 950], false);
    lin.pv.panFrequencies(1);
    expect(lin.got[0]).toEqual([850, 1000]);
  });
});

describe('A view shown a range', () => {
  it('pauses on exactly that range', () => {
    const v = new View({ now: () => 1000 });
    v.show(990, 994, 0);
    expect(v.live).toBe(false);
    expect(v.range(0)).toEqual([990, 994]);
  });
});

describe('A live view panned', () => {
  it('stays live toward the newest samples, where it already is', () => {
    const v = new View({ now: () => 1000 });
    v.pan(0.1, 0);
    expect(v.live).toBe(true);
  });
  it('pauses toward older ones', () => {
    const v = new View({ now: () => 1000 });
    v.pan(-0.1, 0);
    expect(v.live).toBe(false);
  });
});

describe('A paused view, given the history its plot captured', () => {
  const capture = () => {
    let now = 1000;
    const v = new View({ now: () => now, historySeconds: () => 1800 });
    v.pause(0);
    v.limits = [400, v.pausedAt];
    now = 5000; // the device goes on long after the pause
    return v;
  };
  it('zooms out no wider than the capture', () => {
    const v = capture();
    for (let i = 0; i < 60; i++) v.zoom(1.25, 1, 0);
    expect(v.span).toBeCloseTo(v.pausedAt - 400);
    expect(v.right).toBe(v.pausedAt);
  });
  it('pans no further than either end of it', () => {
    const v = capture();
    v.pan(100, 0);
    expect(v.right).toBe(v.pausedAt);
    v.pan(-1000, 0);
    expect(v.right - v.span).toBe(400);
  });
  it('forgets where it paused when it follows live again', () => {
    const v = capture();
    v.follow();
    expect(v.pausedAt).toBeNull();
  });
});

describe('A plot switched between kinds', () => {
  it('keeps each kind’s y ranges apart, so a spectrum never reads a time plot’s as log10', () => {
    const cfg = { kind: 'time' };
    const pv = Object.assign(Object.create(PlotView.prototype), { config: () => cfg, yByKind: new Map() });
    expect(pv.yRange('shared', -1, 1, true)).toEqual([-1.16, 1.16]);
    cfg.kind = 'spectrum';
    const [lo, hi] = pv.yRange('shared', -4, 0, false);
    expect(lo).toBeLessThan(-4);
    expect(hi).toBeGreaterThan(0);
    cfg.kind = 'time';
    expect(pv.yRange('shared', -5, 5, true)).toEqual([-1.16, 1.16]);
  });
});

describe('Y cursors', () => {
  // A plot of one shared y axis, 0 to 10 over a 100 px lane from y = 0.
  const plot = (kind = 'time') => {
    const cfg = { kind };
    const pv = Object.assign(Object.create(PlotView.prototype), { config: () => cfg, yCursorsByKind: new Map(), specCursors: { a: null, b: null }, log: false });
    pv.yAxes = [{ key: 'shared', lane: { x: 40, y: 0, w: 200, h: 100 }, range: [0, 10], x0: 0, x1: 40 }];
    return { pv, cfg };
  };
  it('are found near their line, across the lane or on their tag in the gutter', () => {
    const { pv } = plot();
    pv.setYCursor('shared', 'a', 5); // y = 50
    expect(pv.yCursorNear(100, 52)).toEqual({ key: 'shared', name: 'a' });
    expect(pv.yCursorNear(20, 48)).toEqual({ key: 'shared', name: 'a' });
    expect(pv.yCursorNear(100, 70)).toBeNull();
  });
  it('read the axis they are dragged on', () => {
    const { pv } = plot();
    expect(pv.yValueAt('shared', 25)).toBe(7.5);
  });
  it('are placed a third in from each end of an axis', () => {
    const { pv } = plot();
    pv.addYCursors('shared');
    const yc = pv.yCursors.get('shared');
    expect(yc.a).toBeCloseTo(10 / 3);
    expect(yc.b).toBeCloseTo(20 / 3);
  });
  it('belong to the plot’s kind, and all go when cleared', () => {
    const { pv, cfg } = plot();
    pv.setYCursor('shared', 'a', 5);
    cfg.kind = 'spectrum';
    expect(pv.yCursors.get('shared')).toBeUndefined();
    expect(pv.clearCursors()).toBe(true);
    cfg.kind = 'time';
    expect(pv.yCursors.get('shared')).toBeUndefined();
    expect(pv.clearCursors()).toBe(false);
  });
});

describe('Tags along a lane', () => {
  it('stand side by side at one height, from either side, and keep to the side at another', () => {
    const pv = Object.assign(Object.create(PlotView.prototype), { tagsPlaced: [] });
    const lane = { x: 0, y: 0, w: 300, h: 100 };
    expect(pv.tagSlot(lane, 'left', 50, 40, 14)).toBe(1);
    expect(pv.tagSlot(lane, 'left', 52, 30, 16)).toBe(43);
    expect(pv.tagSlot(lane, 'left', 80, 30, 14)).toBe(1);
    expect(pv.tagSlot(lane, 'right', 50, 40, 14)).toBe(259);
    expect(pv.tagSlot(lane, 'right', 50, 20, 14)).toBe(237);
  });
});

describe('timeAxis', () => {
  it('labels Unix time on a 24-hour clock, naming only what changed at a tick', () => {
    const at = (h, m, s, ms = 0) => new Date(2026, 10, 8, h, m, s, ms).getTime() / 1000;
    const { label } = timeAxis('unix');
    expect(label(at(0, 30, 0))).toBe('00:30');
    expect(label(at(13, 5, 0))).toBe('13:05');
    expect(label(at(13, 5, 15))).toBe(':15');
    expect(label(at(13, 5, 15, 250))).toBe('.250');
  });
  it('labels any other clock in its own seconds', () => {
    expect(timeAxis('systime').label(12.5)).toBe('12.5 s');
  });
});

describe('View.zoom, paused, at the newest samples', () => {
  it('widens into the past, never past what has arrived', () => {
    const v = new View({ now: () => 1000.15 });
    v.pause(0);
    v.zoom(4, 0.5, 0);
    expect(v.range(0)[1]).toBeCloseTo(1000);
    expect(v.span).toBe(40);
  });
});
