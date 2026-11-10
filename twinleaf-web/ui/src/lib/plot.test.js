import { describe, expect, it, vi } from 'vitest';
import fc from 'fast-check';
import { Points, Trace } from './session.svelte.js';
import { axisSlots, blurred, onPixelGrid, pickLevel, PlotView, rowCounts, timeAxis, View } from './plot.js';

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

describe('A paused view panned later', () => {
  it('stops at the newest samples, whatever its plot has drawn', () => {
    let now = 1000;
    const v = new View({ now: () => now });
    v.pause(0);
    now = 1001;
    v.pan(0.5, 0);
    expect(v.right).toBe(1001 - v.delay);
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
    const pv = Object.assign(Object.create(PlotView.prototype), { config: () => cfg, view: new View({ now: () => 0 }) });
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
  // A plot of one shared y axis, 0 to 10 over a 100 px lane from y = 0, drawing the cursors `held.now`.
  const plot = (kind = 'time', held = { now: undefined }) => {
    const cfg = { kind };
    const pv = Object.assign(Object.create(PlotView.prototype), { config: () => cfg, plotId: 'p', cursors: { get: () => held.now }, log: false });
    pv.yAxes = [{ key: 'shared', lane: { x: 40, y: 0, w: 200, h: 100 }, range: [0, 10], x0: 0, x1: 40 }];
    return { pv, cfg, held };
  };
  const Y = (kind, a, b = null) => ({ y: { [kind]: { shared: { a, b } } } });
  it('are found near their line, across the lane or on their tag in the gutter', () => {
    const { pv } = plot('time', { now: Y('time', 5) }); // y = 50
    expect(pv.yCursorNear(100, 52)).toEqual({ key: 'shared', name: 'a' });
    expect(pv.yCursorNear(20, 48)).toEqual({ key: 'shared', name: 'a' });
    expect(pv.yCursorNear(100, 70)).toBeNull();
  });
  it('read the axis they are dragged on', () => {
    const { pv } = plot();
    expect(pv.yValueAt('shared', 25)).toBe(7.5);
  });
  it('are placed a third in from each end of an axis, the plot’s other cursors kept', () => {
    const { pv } = plot('time', { now: { x: { a: 1, b: null } } });
    const c = pv.yCursorsAdded('shared');
    expect(c.x).toEqual({ a: 1, b: null });
    expect(c.y.time.shared.a).toBeCloseTo(10 / 3);
    expect(c.y.time.shared.b).toBeCloseTo(20 / 3);
  });
  it('belong to the plot’s kind, and are cleared by axis or all at once', () => {
    const { pv, cfg, held } = plot('time', { now: { ...Y('time', 5), f: { a: 3, b: 4 } } });
    expect([pv.hasYCursors('shared'), pv.cursorsPlaced()]).toEqual([true, 1]);
    cfg.kind = 'spectrum';
    expect([pv.hasYCursors('shared'), pv.cursorsPlaced()]).toEqual([false, 3]);
    expect(pv.yCursorsCleared('shared')).toEqual(held.now);
    expect(pv.xCursorsCleared()).toEqual(Y('time', 5));
    cfg.kind = 'time';
    expect(pv.yCursorsCleared()).toEqual({ f: { a: 3, b: 4 } });
  });
});

describe('A plot’s view', () => {
  const plot = (cfg, view) => Object.assign(Object.create(PlotView.prototype), { config: () => cfg, view, viewHistory: { step: (fn) => fn() } });
  it('holds its y ranges, so a plot drawn again with it finds them', () => {
    const view = new View({ now: () => 0 }), cfg = { kind: 'time' };
    plot(cfg, view).setYBounds(['shared'], 1, 2);
    expect(plot(cfg, view).yBounds('shared')).toEqual([1, 2]);
  });
  it('holds the spectrum a paused spectrum shows, so a plot drawn again with it shows that one', () => {
    const view = new View({ now: () => 0 }), t = { spectrum: 'first' };
    expect(plot({ kind: 'spectrum' }, view).spectrumOf(t)).toBe('first');
    view.pause(0);
    t.spectrum = 'second';
    expect(plot({ kind: 'spectrum' }, view).spectrumOf(t)).toBe('first');
  });
  it('fits a time plot’s y at once to what its last frame showed, so a fit is a change made then', () => {
    const view = new View({ now: () => 0 }), pv = plot({ kind: 'time' }, view);
    pv.setYBounds(['shared'], 5, 6);
    pv.extents = new Map([['shared', [-1, 1]]]);
    pv.fit(['shared']);
    expect(pv.yBounds('shared')).toEqual([-1.16, 1.16]);
    pv.setYBounds(['shared'], 5, 6);
    pv.autoscale();
    expect(view.state().yRanges).toEqual({ time: { shared: [-1.16, 1.16] } });
  });
  it('holds no cursor: a view’s state says nothing of them, and following live changes none', () => {
    const view = new View({ now: () => 0 });
    expect(Object.keys(view.state()).sort()).toEqual(['live', 'pausedAt', 'right', 'span', 'spectra', 'window', 'yRanges']);
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
  it('reads Unix time to the nearest millisecond, never the one below', () => {
    const t = new Date(2026, 10, 8, 14, 3, 7).getTime() / 1000 + 0.12;
    const { readout, label } = timeAxis('unix');
    expect(readout(t)).toBe('14:03:07.120');
    expect(label(t)).toBe('.120');
    expect(readout(new Date(2026, 10, 8, 14, 3, 7).getTime() / 1000 + 0.9996)).toBe('14:03:08.000');
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

describe('the level a plot draws', () => {
  // Levels 4 apart over 0 to 100 s, level k a point every 10·4^k ms, as
  // FPCS levels of one signal give about.
  const levels = (from = [0, 0, 0]) =>
    from.map((start, k) => {
      const l = new Points(1e9), dt = 0.01 * 4 ** k;
      const xs = [];
      for (let x = start; x <= 100; x += dt) xs.push(x);
      l.push(xs, xs.map(() => 0), false);
      return l;
    });
  // A plot 100 px wide wants 400 points from a level; the next level up
  // gives that at 16 s, and 1.25 times it at 20 s.
  const at = (ls, span, was) => pickLevel(ls, 100 - span, 100, 100, was);

  it('draws the coarsest level with enough points a pixel', () => {
    const ls = levels();
    expect(at(ls, 10, 0)).toBe(0);
    expect(at(ls, 21, 0)).toBe(1);
    expect(at(ls, 90, 0)).toBe(2);
  });

  it('holds the level it drew within the band', () => {
    const ls = levels();
    expect(at(ls, 18, 0)).toBe(0);
    expect(at(ls, 18, 1)).toBe(1);
    expect(at(ls, 15, 1)).toBe(0);
  });

  it('gives way to a coarser level when the finer one does not reach back', () => {
    const ls = levels([95, 0, 0]);
    expect(at(ls, 4, 0)).toBe(0);
    expect(at(ls, 8, 0)).toBe(1);
  });
});

describe('the window a view goes live at', () => {
  // A view on a hub whose newest sample is at time 1000, showing 10 s.
  const view = () => {
    const v = new View({ now: () => 1000.15 });
    v.setSpan(10);
    return v;
  };

  it('is set by a zoom that leaves the view live', () => {
    const v = view();
    v.zoom(2, 1, 0);
    expect(v.live).toBe(true);
    expect([v.window, v.span]).toEqual([20, 20]);
  });

  it('is kept by a zoom that pauses the view, and a double-click goes back to it', () => {
    const v = view();
    v.zoom(0.5, 0.5, 0);
    expect(v.live).toBe(false);
    expect([v.window, v.span]).toEqual([10, 5]);
    v.show(990, 991, 0);
    expect([v.window, v.span]).toEqual([10, 1]);
    v.follow(true);
    expect([v.live, v.window, v.span]).toEqual([true, 10, 10]);
  });

  it('is kept when play carries on live at the span shown, for a double-click after', () => {
    const v = view();
    v.zoom(0.5, 0.5, 0);
    v.follow();
    expect([v.live, v.window, v.span]).toEqual([true, 10, 5]);
    v.pause(0);
    v.follow(true);
    expect([v.live, v.window, v.span]).toEqual([true, 10, 10]);
  });

  it('is set by a zoom at the live edge, from the span shown', () => {
    const v = view();
    v.zoom(0.5, 0.5, 0);
    v.follow();
    v.zoom(2, 1, 0);
    expect([v.live, v.window, v.span]).toEqual([true, 10, 10]);
  });

  it('is what the window setting sets, paused or live', () => {
    const v = view();
    v.zoom(0.5, 0.5, 0);
    v.setSpan(30);
    expect([v.window, v.span]).toEqual([30, 30]);
    v.follow(true);
    expect(v.span).toBe(30);
  });
});

// A live plot moves its points by whole device pixels from frame to frame,
// so a column whose data did not change draws the same: checked over seeded
// random ranges at Unix-epoch times, spans from 2 ms to an hour and plots
// from 100 to 4000 device px wide. A failure names its seed to replay.
describe('a range on the device pixel grid', () => {
  /** A seeded generator of numbers in [0, 1). */
  const random = (seed) => () => ((seed = (seed * 1664525 + 1013904223) >>> 0) / 2 ** 32);
  const cases = (seed, n, f) => {
    const r = random(seed);
    for (let i = 0; i < n; i++) {
      const span = 0.002 * (3600 / 0.002) ** r();
      const width = Math.round(100 + 3900 * r());
      const x0 = 1.7e9 + 1e8 * r();
      f({ span, width, x0, r, i });
    }
  };
  /** How far `v` is from a whole number. */
  const offWhole = (v) => Math.abs(v - Math.round(v));
  // Times near the Unix epoch's 1.7e9 s resolve to about 0.24 us, so a pixel
  // of a 2 ms window can only be placed to that: every check allows a few of
  // those steps of the time it works at.
  const step = (t) => 4 * Number.EPSILON * Math.abs(t);

  it('keeps its span and starts less than a pixel before the range', () => {
    cases(1, 2000, ({ span, width, x0, i }) => {
      const tau = span / width;
      const [a, b] = onPixelGrid(x0, x0 + span, width);
      expect(Math.abs(b - a - span), `case ${i}`).toBeLessThanOrEqual(step(x0));
      expect(x0 - a, `case ${i}`).toBeGreaterThanOrEqual(-step(x0));
      expect(x0 - a, `case ${i}`).toBeLessThan(tau + step(x0));
    });
  });

  it('moves by whole pixels, and never back, as a live view scrolls', () => {
    cases(2, 300, ({ span, width, x0, r, i }) => {
      const tau = span / width;
      let t = x0, was = onPixelGrid(t, t + span, width)[0];
      for (let frame = 0; frame < 60; frame++) {
        t += tau * 3 * r();
        const now = onPixelGrid(t, t + span, width)[0];
        expect(now, `case ${i} frame ${frame}`).toBeGreaterThanOrEqual(was);
        expect(offWhole((now - was) / tau) * tau, `case ${i} frame ${frame}`).toBeLessThanOrEqual(step(t));
        was = now;
      }
    });
  });

  it('leaves a range it cannot place as it is', () => {
    expect(onPixelGrid(5, 5, 100)).toEqual([5, 5]);
    expect(onPixelGrid(0, 10, 0)).toEqual([0, 10]);
  });
});

// A live time plot drawn frame after frame, as its draw hands it to the GPU:
// the range each lane is set to and the points drawn in it, filled into
// device columns as the GPU fills them, each column the least and greatest
// value drawn in it. A fast signal, zoomed out so a column holds many
// points, scrolls by fractions of a pixel a frame; every column whose points
// did not change draws the same from frame to frame, moved by whole columns.
describe('a live plot drawn frame after frame', () => {
  /** A plot `cssWidth` px wide at `dpr` drawing `trace`, live over `span` s, as its draw sees the page. */
  function plotOf(trace, span, cssWidth, dpr) {
    const drawn = { lane: null, draws: [] };
    const renderer = {
      dpr,
      ring: () => ({ base: 0 }),
      lane: (rect, base, x0, x1) => ((drawn.lane = { x0, x1, width: Math.round(rect.w * dpr) }), true),
      drawTrace: (src, ring, a0, a1) => drawn.draws.push({ src, a0, a1 }),
    };
    const hub = { now: () => hub.t, t: 0, session: { windowOf: () => undefined, memory: null } };
    const view = new View(hub);
    view.span = span;
    view.delay = 0;
    const plot = { x: 0, y: 0, w: cssWidth, h: 100 }, cfg = { kind: 'time', mode: 'overlay' };
    const pv = new PlotView({ body: document.createElement('div'), overlay: document.createElement('canvas'), config: () => cfg, hub, view, targets: () => [view], hoverTime: { t: null }, plotId: 'p', renderer });
    // The overlay's axes, labels and readouts are drawn on a 2D canvas, which jsdom has none of; the traces are what the GPU gets.
    pv.drawOverlay = () => {};
    /** The device columns drawn at device time `t`, by column from time zero: each [least, greatest]. */
    const frame = (t) => {
      hub.t = t;
      drawn.draws = [];
      pv.drawTime(cfg, [trace], { plot, lanes: [plot] }, plot, 0);
      const { x0, x1, width } = drawn.lane, tau = (x1 - x0) / width, first = Math.round(x0 / tau);
      const columns = new Map();
      for (const { src, a0, a1 } of drawn.draws) {
        for (let i = a0; i < a1; i++) {
          const p = Math.floor(((src.xAt(i) - x0) / (x1 - x0)) * width);
          // The columns at either edge are cut off by the lane; the rest are whole.
          if (p < 1 || p >= width - 1) continue;
          const c = columns.get(first + p), y = src.yAt(i);
          columns.set(first + p, c ? [Math.min(c[0], y), Math.max(c[1], y)] : [y, y]);
        }
      }
      return columns;
    };
    return frame;
  }

  it('draws every column whose points did not change the same, moved by whole columns', () => {
    // jsdom draws nothing: a trace's colour, read off a canvas, is black.
    vi.spyOn(HTMLCanvasElement.prototype, 'getContext').mockReturnValue({ fillRect() {}, getImageData: () => ({ data: [0, 0, 0, 255] }) });
    let compared = 0;
    fc.assert(
      fc.property(
        fc.integer({ min: 120, max: 600 }),
        fc.constantFrom(1, 1.25, 1.5, 2),
        fc.integer({ min: 4, max: 24 }),
        fc.array(fc.double({ min: 0.05, max: 3, noNaN: true }), { minLength: 5, maxLength: 12 }),
        fc.integer({ min: 1, max: 2 ** 31 - 1 }),
        (cssWidth, dpr, perColumn, moves, seed) => {
          // Noise at an uneven rate, perColumn points a column: no column of it draws like the next.
          const width = Math.round(cssWidth * dpr), span = 4, rate = (perColumn * width) / span + Math.PI;
          let s = seed;
          const noise = () => ((s = (s * 1664525 + 1013904223) >>> 0) / 2 ** 32);
          const xs = Float64Array.from({ length: Math.ceil(rate * 8) }, (_, i) => i / rate);
          const trace = new Trace(meta);
          trace.push(xs, Float32Array.from(xs, () => noise()), false);
          const frame = plotOf(trace, span, cssWidth, dpr);
          let t = 6, was = frame(t);
          for (const move of moves) {
            t += (move * span) / width;
            const now = frame(t);
            for (const [c, v] of now) {
              if (!was.has(c)) continue;
              expect(v, `column ${c} at ${t}`).toEqual(was.get(c));
              compared++;
            }
            was = now;
          }
        },
      ),
      { numRuns: Math.ceil(fc.readConfigureGlobal().numRuns / 4) },
    );
    expect(compared).toBeGreaterThan(1000);
    vi.restoreAllMocks();
  }, 120_000);
});
