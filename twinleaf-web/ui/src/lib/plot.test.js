import { describe, expect, it, vi } from 'vitest';
import fc from 'fast-check';
import { Detail, Points, Trace } from './session.svelte.js';
import { scaleLinear, scaleLog } from 'd3-scale';
import { axisSlots, blurred, F_AUTO, laneTicks, onPixelGrid, pickLevel, pickSource, PlotView, rowCounts, tickTexts, timeAxis, View, Y_AUTO } from './plot.js';
import { axisFormat, formOf, numberText, si, tickText } from './units.js';
import { Colors } from './colors.svelte.js';

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

describe('laneTicks', () => {
  // A number is written 4 px below its tick's pixel row, its digits 9 px up and 2 down;
  // it may reach 2 px past its lane, and keeps 2 px from the next number.
  const UP = 5, DOWN = 6, REACH = 2, CLEAR = 2;
  /** How far a number may reach past a lane's edge: 2 px into the gap to the next lane, and at a plot's top and foot the room the plot keeps there. */
  const INNER = { above: REACH, below: REACH }, OUTER = { above: 6, below: 7 };
  const scaleOf = (lo, span, lane, log = false) => (log ? scaleLog() : scaleLinear()).domain([lo, lo + span]).range([lane.y + lane.h, lane.y]);
  /** The box each number of `labels` is drawn in, as plot.js draws it on the pixel grid. */
  const boxes = (sy, labels) => labels.map((v) => Math.round(sy(v)) + 0.5).map((py) => [py - UP, py + DOWN]);
  /** Whether a number at its tick's exact place fits the lane, with the pixel it may be drawn below that. */
  const fits = (sy, lane, v, reach = INNER) => sy(v) - UP >= lane.y - reach.above && sy(v) + 1 + DOWN <= lane.y + lane.h + reach.below;
  const kind = (labels) => Math.min(2, labels.length);
  const domain = fc.tuple(fc.double({ min: -1e6, max: 1e6, noNaN: true }), fc.double({ min: 1e-6, max: 1e6, noNaN: true }));
  // Lanes short enough to number none, as often as taller ones.
  const laneAt = fc.record({ y: fc.double({ min: 0, max: 800, noNaN: true }), h: fc.oneof(fc.integer({ min: 1, max: 60 }), fc.integer({ min: 1, max: 400 })) });
  // A lane's edges: each between two lanes or at the plot's top or foot, its top perhaps under a floating header, 8 px down into it.
  const reachAt = fc.record({ above: fc.constantFrom(INNER.above, OUTER.above, -8), below: fc.constantFrom(INNER.below, OUTER.below) });
  /** The step of `grid`, 1, 2 or 5 times a power of ten, which the difference of two ticks far from zero gives only to a few digits. */
  const stepOf = (grid) => Number((grid[1] - grid[0]).toPrecision(1));
  /** The ticks of `grid` every `k`th from zero, on its even steps. */
  const every = (grid, k) => grid.filter((v) => Math.round(v / stepOf(grid)) % k === 0);

  it('numbers ticks of its grid, each within the lane, past it at most as far as its edge lets, and 2 px or more from the next', () => {
    fc.assert(
      fc.property(domain, laneAt, reachAt, ([lo, span], lane, reach) => {
        const sy = scaleOf(lo, span, lane), { grid, labels } = laneTicks(sy, lane, undefined, reach);
        expect(grid).toEqual(sy.ticks(Math.max(3, lane.h / 40)));
        for (const v of labels) expect(grid).toContain(v);
        const drawn = boxes(sy, labels).sort((a, b) => a[0] - b[0]);
        for (const [top, bottom] of drawn) expect(top >= lane.y - reach.above && bottom <= lane.y + lane.h + reach.below, `${[top, bottom]} in ${lane.y} + ${lane.h}, ${JSON.stringify(reach)}`).toBe(true);
        drawn.forEach((b, i) => i && expect(b[0] - drawn[i - 1][1], 'the room between two numbers').toBeGreaterThanOrEqual(CLEAR));
      }),
    );
  });

  it('numbers every kth tick that fits, for the least k of 1, 2, 5, 10… that gives two or more with room between them as between two lanes, else none: never one alone', () => {
    fc.assert(
      fc.property(domain, laneAt, reachAt, ([lo, span], lane, reach) => {
        const sy = scaleOf(lo, span, lane), { grid, labels } = laneTicks(sy, lane, undefined, reach);
        expect(labels.length, `${labels} in ${lane.y} + ${lane.h}`).not.toBe(1);
        const spaced = (vs) => vs.length >= 2 && vs.every((v, i) => !i || Math.abs(sy(v) - sy(vs[i - 1])) >= 14);
        // Decided as between two lanes, or with less room where the lane has less.
        const inner = { above: Math.min(REACH, reach.above), below: Math.min(REACH, reach.below) };
        const ks = [1, 2, 5, 10, 20, 50, 100].filter((k) => spaced(every(grid, k).filter((v) => fits(sy, lane, v, inner))));
        if (!ks.length || grid.length < 2) expect(labels).toEqual([]);
        else expect(labels, `every ${ks[0]}th tick that reaches no further than its edges let`).toEqual(every(grid, ks[0]).filter((v) => fits(sy, lane, v, reach)));
      }),
      // Ticks 0.00001 apart far from zero, whose difference is 0.0000100000179: every 2nd from zero is -133677.194 alone, not the 1st and 3rd.
      // And ticks 0.0001 apart from 8163.3094, of which no two every 2nd from zero fit, not 8163.3095 and 8163.3097.
      {
        examples: [
          [[-133677.19401357145, 0.000030714247259311385], { y: 0, h: 43 }, INNER],
          [[-637471.14056, 0.000022307562176138166], { y: 0, h: 29 }, { above: 2, below: 7 }],
          [[8163.309400000001, 0.00033913043307620683], { y: 0, h: 26 }, INNER],
        ],
      },
    );
  });

  it('numbers lanes of one height and range alike, or leaves them all bare, whether their edges are between two lanes or at the plot’s top and foot', () => {
    fc.assert(
      fc.property(domain, laneAt, fc.double({ min: 0, max: 800, noNaN: true }), reachAt.filter((r) => r.above >= 0), ([lo, span], lane, y, reach) => {
        const other = { y, h: lane.h };
        const a = laneTicks(scaleOf(lo, span, lane), lane, undefined, INNER).labels, b = laneTicks(scaleOf(lo, span, other), other, undefined, reach).labels;
        expect(b.length > 0, `${a} at ${lane.y}, ${b} at ${y} with ${JSON.stringify(reach)}`).toBe(a.length > 0);
      }),
    );
  });

  it('at a plot’s top and foot, numbers each tick of its stride, those at the very ends of the range among them', () => {
    // A range set by hand, from one round value to another: m·u to (m + n)·u.
    const round = fc.tuple(fc.integer({ min: -20, max: 20 }), fc.integer({ min: 1, max: 20 }), fc.integer({ min: -4, max: 4 }));
    fc.assert(
      fc.property(round, laneAt, ([m, n, e], lane) => {
        const u = 10 ** e, sy = scaleOf(m * u, n * u, lane), { grid, labels } = laneTicks(sy, lane, undefined, OUTER);
        if (labels.length < 2) return;
        const k = Math.round((labels[1] - labels[0]) / stepOf(grid));
        expect(labels, `every ${k}th tick of ${grid}`).toEqual(every(grid, k));
      }),
      { examples: [[[0, 1, 0], { y: 0, h: 200 }], [[0, 10, -1], { y: 37.4, h: 300 }]] },
    );
    const lane = { y: 0, h: 200 };
    expect(laneTicks(scaleOf(0, 1, lane), lane, undefined, OUTER).labels).toEqual([0, 0.2, 0.4, 0.6, 0.8, 1]);
    // Between two lanes the same range leaves its ends out, whose numbers would cross into the next lane.
    expect(laneTicks(scaleOf(0, 1, lane), lane, undefined, INNER).labels).toEqual([0.2, 0.4, 0.6, 0.8]);
  });

  it('numbers several ticks in any lane 120 px high or more with four ticks or more', () => {
    fc.assert(
      fc.property(domain, laneAt, ([lo, span], lane) => {
        const tall = { ...lane, h: 120 + lane.h }, { grid, labels } = laneTicks(scaleOf(lo, span, tall), tall);
        // Two of them at most are at the lane's ends, where a number would run past it.
        if (grid.length >= 4) expect(labels.length).toBeGreaterThanOrEqual(2);
      }),
    );
  });

  it('goes from several to one to none as the lane shortens a pixel at a time, never back, wherever the lane starts', () => {
    fc.assert(
      fc.property(domain, fc.array(fc.double({ min: 0, max: 800, noNaN: true }), { minLength: 400, maxLength: 400 }), fc.boolean(), ([lo, span], ys, log) => {
        // A log axis numbers its decades only.
        if (log) [lo, span] = [10 ** (Math.abs(lo) % 6 - 3), 10 ** (Math.abs(lo) % 6 - 3) * (1 + (span % 1e4))];
        const decade = (v) => Number.isInteger(Math.round(Math.log10(v) * 1e6) / 1e6);
        let was = 2;
        for (let h = 400; h >= 1; h--) {
          const lane = { y: ys[h - 1], h }, now = kind(laneTicks(scaleOf(lo, span, lane, log), lane, log ? decade : undefined).labels);
          expect(now, `${h} px high, from ${lane.y}`).toBeLessThanOrEqual(was);
          was = now;
        }
      }),
    );
  });

  it('numbers nothing in a lane with room for one number and not two', () => {
    // 0 to 3.2 over 30 px: 1 at 20.6 px from the top, 2 at 11.3, too near each other for both.
    const lane = { y: 0, h: 30 }, sy = scaleOf(0, 3.2, lane);
    expect(laneTicks(sy, lane)).toEqual({ grid: [0, 1, 2, 3], labels: [], count: 3 });
  });

  it('numbers a log axis at its decades only', () => {
    const lane = { y: 0, h: 300 }, sy = scaleOf(1e-3, 1e3 - 1e-3, lane, true);
    const decade = (v) => Number.isInteger(Math.round(Math.log10(v) * 1e6) / 1e6);
    const { labels } = laneTicks(sy, lane, decade);
    expect(labels.length).toBeGreaterThanOrEqual(2);
    for (const v of labels) expect(decade(v), `${v}`).toBe(true);
  });
});

describe('an axis’s numbers', () => {
  const finite = fc.double({ min: -1e9, max: 1e9, noNaN: true }).map((v) => (Math.abs(v) < 1e-30 ? 0 : v));

  it('on Auto read at each place as the page writes them with no settings', () => {
    // The page's own formats, written out here as they were before an axis had settings; `si`, which the
    // tests of a measured value hold to its own, written out there.
    const PAGE = {
      'tag.time': (v) => v.toPrecision(4),
      'tag.spectrum': (v) => v.toExponential(2),
      'value.time': (v, u) => si(v, u),
      'value.spectrum': (v) => v.toExponential(3),
      'legend.time': (v) => v.toPrecision(5),
      'legend.spectrum': (v) => v.toExponential(3),
      'floor.spectrum': (v) => v.toExponential(2),
    };
    const auto = axisFormat(undefined);
    fc.assert(
      fc.property(finite, fc.constantFrom('V', 'nT', 'arb', ''), (v, u) => {
        for (const [place, kinds] of Object.entries(Y_AUTO))
          for (const [kind, form] of Object.entries(kinds)) expect(numberText(v, formOf(form, auto), kind === 'time' && place === 'value' ? u : ''), `${place} on a ${kind} plot`).toBe(PAGE[`${place}.${kind}`](v, u));
        expect(numberText(v, formOf(F_AUTO.at, auto), 'Hz')).toBe(`${v.toPrecision(4)} Hz`);
        expect(numberText(v, formOf(F_AUTO.span, auto), 'Hz')).toBe(si(v, 'Hz'));
      }),
    );
    expect(Object.keys(PAGE).length).toBe(Object.values(Y_AUTO).reduce((n, kinds) => n + Object.keys(kinds).length, 0));
  });

  it('number their ticks as the scale does on Auto, else in the notation and digits set, no two alike, plain ones to the decimals of the largest', () => {
    const domain = fc.tuple(fc.double({ min: -1e6, max: 1e6, noNaN: true }), fc.double({ min: 1e-6, max: 1e6, noNaN: true }));
    const set = fc.record({ digits: fc.constantFrom(null, 2, 3, 4, 5), notation: fc.constantFrom('auto', 'plain', 'exp') });
    const seen = new Set();
    fc.assert(
      fc.property(domain, fc.integer({ min: 40, max: 800 }), set, fc.boolean(), ([lo, span], h, f, log) => {
        if (log) [lo, span] = [10 ** (Math.abs(lo) % 6 - 3), 10 ** (Math.abs(lo) % 6 - 3) * (1 + (span % 1e4))];
        const lane = { y: 0, h }, sy = (log ? scaleLog() : scaleLinear()).domain([lo, lo + span]).range([h, 0]);
        const decade = (v) => Number.isInteger(Math.round(Math.log10(v) * 1e6) / 1e6);
        const { labels, count } = laneTicks(sy, lane, log ? decade : undefined);
        const auto = log ? (v) => v.toExponential(0) : sy.tickFormat(count);
        const texts = tickTexts(labels, { label: 'off', ...f }, auto, { log });
        if (f.digits === null && f.notation === 'auto') return void expect(texts).toEqual(labels.map(auto));
        expect(new Set(texts).size, `${texts}`).toBe(texts.length);
        const notation = f.notation === 'auto' ? (log ? 'exp' : 'plain') : f.notation;
        for (const t of texts) expect(/e[+-]\d+$/.test(t), `${t} in ${notation}`).toBe(notation === 'exp');
        if (notation === 'plain' && !log) expect(new Set(texts.map((t) => t.split('.')[1]?.length ?? 0)).size, `${texts}`).toBeLessThanOrEqual(1);
        // Exponents have as many figures as set, or more to tell two ticks apart.
        if (notation === 'exp' && f.digits) for (const t of texts) expect(t.split('e')[0].replace(/[-.]/g, '').length, t).toBeGreaterThanOrEqual(f.digits);
        seen.add(`${notation}:${log}`);
      }),
      { examples: [[[-1, 2], 300, { digits: 2, notation: 'auto' }, false], [[100, 0.05], 300, { digits: 2, notation: 'exp' }, false], [[1, 999], 300, { digits: null, notation: 'plain' }, true], [[1, 999], 300, { digits: 3, notation: 'auto' }, true]] },
    );
    for (const k of ['plain:false', 'exp:false', 'plain:true', 'exp:true']) expect(seen.has(k), k).toBe(true);
    // Ticks 0.01 apart about 100, which two figures would write alike, take as many as tell them apart.
    expect(tickTexts([100, 100.01, 100.02], { digits: 2, notation: 'plain' }, String)).toEqual(['100.00', '100.01', '100.02']);
    expect(tickTexts([0, 0.5, 1], { digits: 3, notation: 'auto' }, String)).toEqual(['0.00', '0.50', '1.00']);
    expect(tickTexts([500, 1000, 1500], { digits: 2, notation: 'auto' }, String, { x: 'Hz' })).toEqual(['500 Hz', '1.0 kHz', '1.5 kHz']);
    expect(tickTexts([500, 1000, 1500], { digits: null, notation: 'plain' }, String, { x: 'Hz' })).toEqual(['500 Hz', '1000 Hz', '1500 Hz']);
    // A capture's x axis, in the capture's x units: on Auto each tick with the SI prefix its size gives it and as few digits as it needs; else as its settings say.
    expect([0, 250e-6, 500e-6, 1e-3, 1.5e-3].map((v) => tickText(v, 's'))).toEqual(['0 s', '250 µs', '500 µs', '1 ms', '1.5 ms']);
    expect([0, 400, 1200].map((v) => tickText(v, 'samples'))).toEqual(['0 samples', '400 samples', '1200 samples']);
    expect(tickText(0.5, '')).toBe('0.5');
    expect(tickTexts([2e-4, 4e-4], { digits: null, notation: 'exp' }, String, { x: 's' })).toEqual(['2e-4 s', '4e-4 s']);
    expect(tickTexts([250e-6, 500e-6], { digits: 3, notation: 'auto' }, String, { x: 's' })).toEqual(['250 µs', '500 µs']);
  });
});

describe('View.zoom, on a hub that holds more than a live window', () => {
  const held = () => ({ oldest: -900, dropped: false, history: null });
  it('goes as wide, paused, as from the oldest time the hub holds to the pause, before any frame is drawn', () => {
    const v = Object.assign(new View({ now: () => 1000 }), { held });
    v.pause(0);
    for (let i = 0; i < 60; i++) v.zoom(1.25, 1, 0);
    expect(v.span).toBe(v.pausedAt + 900);
    expect(v.right).toBe(v.pausedAt);
  });
  it('stays within a live window while live', () => {
    const v = Object.assign(new View({ now: () => 1000 }), { held });
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

describe('A spectrum zoomed by the wheel', () => {
  const spectrum = (xd, logX) => {
    const got = [], bursts = [];
    const pv = Object.assign(Object.create(PlotView.prototype), {
      xd, fExtent: [1, 1000], logX, targets: () => ['its view'],
      onFrequencies: (a, b) => got.push([a, b]),
      viewHistory: { burst: (kind, targets, fn) => (bursts.push([kind, targets]), fn()) },
    });
    return { pv, got, bursts };
  };
  it('keeps the frequency under the pointer where it is, along a log axis or a linear one, as a zoom burst of the plots it acts on', () => {
    fc.assert(
      fc.property(fc.boolean(), fc.double({ min: 0.5, max: 2, noNaN: true }), fc.double({ min: 0, max: 1, noNaN: true }), (logX, factor, anchor) => {
        const { pv, got, bursts } = spectrum([10, 100], logX);
        pv.zoomFrequencies(factor, anchor);
        if (!got.length) return;
        const [to, from] = logX ? [Math.log10, (v) => 10 ** v] : [(v) => v, (v) => v];
        const at = (xd) => from(to(xd[0]) + anchor * (to(xd[1]) - to(xd[0])));
        // Unless an end has stopped at the bins it has.
        if (got[0][0] > 1 && got[0][1] < 1000) expect(at(got[0])).toBeCloseTo(at([10, 100]), 6);
        expect((to(got[0][1]) - to(got[0][0])) / (to(100) - to(10))).toBeLessThanOrEqual(factor + 1e-9);
        expect(bursts).toEqual([['zoom', ['its view']]]);
      }),
    );
  });
  it('stays within the frequencies it has bins for, and stops narrowing at a hundredth of them', () => {
    const out = spectrum([2, 900], false);
    out.pv.zoomFrequencies(4, 0.5);
    expect(out.got[0]).toEqual([1, 1000]);
    const all = spectrum([1, 1000], true);
    all.pv.zoomFrequencies(2, 0.5);
    expect(all.got, 'at every bin already, zooming out changes nothing').toEqual([]);
    const narrow = spectrum([500, 505], false);
    narrow.pv.zoomFrequencies(0.5, 0.5);
    expect(narrow.got).toEqual([]);
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
  it('stops at the moment it paused, however far the newest samples have gone on, whatever its plot has drawn or its hub has said it holds', () => {
    let now = 1000;
    const v = new View({ now: () => now });
    v.pause(0);
    now = 1001;
    v.pan(0.5, 0);
    expect(v.right).toBe(1000 - v.delay);
  });
});

describe('A paused view, given the history its plot captured', () => {
  const capture = () => {
    let now = 1000;
    const v = new View({ now: () => now });
    v.held = () => ({ oldest: 400, dropped: false, history: null });
    v.pause(0);
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
    expect(plot({ kind: 'spectrum' }, view).sliceOf(t)).toBe('first');
    view.pause(0);
    t.spectrum = 'second';
    expect(plot({ kind: 'spectrum' }, view).sliceOf(t)).toBe('first');
  });
  it('shows of a capture what its settings say of its shots, the same slice until a shot comes or a setting changes, and holds it while paused', () => {
    const shot = (number) => ({ number, n: 3, x0: 1, dx: 0.5, y: Float32Array.of(number, 2 * number, 3 * number) });
    const view = new View({ now: () => 0 }), c = { key: 'capture:fid.capture', shots: [] }, cfg = { kind: 'capture' }, pv = plot(cfg, view);
    expect(pv.sliceOf(c)).toBe(null);
    c.shots.push(shot(1), shot(2), shot(3));
    const latest = pv.sliceOf(c);
    expect([[...latest.f], [...latest.a], latest.under, latest.note, latest.number, latest.floor]).toEqual([[1, 1.5, 2], [3, 6, 9], [], '', 3, null]);
    expect(pv.sliceOf(c)).toBe(latest);
    // An overlay: the newest over the two before it, each with its own values; an average: their mean.
    cfg.shots = 'overlay';
    cfg.shotCount = 4;
    const overlay = pv.sliceOf(c);
    expect([[...overlay.a], overlay.under.map((u) => [u.shot.number, ...u.a]), overlay.note]).toEqual([[3, 6, 9], [[1, 1, 2, 3], [2, 2, 4, 6]], 'last 3 of 4']);
    cfg.shots = 'average';
    expect([[...pv.sliceOf(c).a], pv.sliceOf(c).note]).toEqual([[2, 4, 6], 'avg 3 of 4']);
    // Paused, as a copy of the plot is, it holds what it showed, whatever comes after.
    const held = pv.sliceOf(c);
    view.pause(0);
    c.shots.push(shot(4));
    cfg.shots = 'latest';
    expect(plot(cfg, view).sliceOf(c)).toBe(held);
  });
  it('draws of its signals those its kind shows: a capture plot the captures, any other the signals that stream', () => {
    const stream = { key: 'sine.sine' }, capture = { key: 'capture:fid.capture', shots: [] };
    const hub = { session: { byKey: new Map([[stream.key, stream], [capture.key, capture]]) } };
    const of = (kind) => Object.assign(plot({ kind, keys: [stream.key, capture.key, 'gone.signal'] }, null), { hub }).traces();
    expect([of('time'), of('spectrum'), of('capture')]).toEqual([[stream], [stream], [capture]]);
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
    expect(Object.keys(view.state()).sort()).toEqual(['live', 'pausedAt', 'pausedHistory', 'right', 'span', 'spectra', 'window', 'yRanges']);
  });
});

describe('A press on a plot', () => {
  // A y axis band beside a plot, a cursor's line across the plot and a little into the band, and two tags over the line, the second over half of the first.
  const pv = Object.assign(Object.create(PlotView.prototype), {
    pressable: [
      { part: 'plot', x: 40, y: 0, w: 200, h: 100 },
      { part: 'yaxis', band: { side: 'left' }, x: 0, y: 0, w: 40, h: 100 },
      { part: 'line', kind: 'y', name: 'a', x: 36, y: 44, w: 204, h: 12 },
      { part: 'tag', kind: 'y', name: 'a', x: 41, y: 43, w: 30, h: 14 },
      { part: 'tag', kind: 'x', name: 'b', x: 56, y: 43, w: 30, h: 14 },
    ],
  });
  const at = (x, y, parts) => {
    const p = pv.pressAt(x, y, parts);
    return p && [p.part, p.kind, p.name].filter(Boolean).join(' ');
  };
  it('lands on what was drawn last there: a tag over a line, an axis or another tag', () => {
    expect(at(100, 20)).toBe('plot');
    expect(at(10, 20)).toBe('yaxis');
    expect(at(100, 50)).toBe('line y a');
    expect(at(38, 50)).toBe('line y a');
    expect(at(45, 50)).toBe('tag y a');
    expect(at(60, 50)).toBe('tag x b');
    expect(at(80, 50)).toBe('tag x b');
  });
  it('lands on nothing where nothing was drawn, and each box ends short of its far edges', () => {
    expect(at(300, 50)).toBeNull();
    expect(at(240, 20)).toBeNull();
    expect(at(239, 99)).toBe('plot');
    expect(at(100, 100)).toBeNull();
  });
  it('finds what lies under a cursor when asked for the parts alone, as the axis a wheel turn or a menu is for', () => {
    expect(at(38, 50, ['yaxis'])).toBe('yaxis');
    expect(pv.yAxisAt(38, 50)).toEqual({ side: 'left' });
    expect(pv.yAxisAt(50, 50)).toBeNull();
    expect(at(60, 50, ['plot'])).toBe('plot');
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
  it('stand clear of a tag laid across the plot at their height, from either side', () => {
    const lane = { x: 0, y: 0, w: 300, h: 100 };
    const pv = Object.assign(Object.create(PlotView.prototype), { tagsPlaced: [{ lane, side: null, cy: 93, x: 0, w: 22, h: 14 }, { lane, side: null, cy: 93, x: 150, w: 22, h: 14 }] });
    expect(pv.tagSlot(lane, 'left', 90, 22, 14)).toBe(24);
    expect(pv.tagSlot(lane, 'left', 90, 40, 14)).toBe(48);
    expect(pv.tagSlot(lane, 'right', 90, 22, 14)).toBe(277);
    expect(pv.tagSlot(lane, 'left', 60, 22, 14)).toBe(1);
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

describe('what a paused plot draws from', () => {
  // A plot 200 px wide showing 0 to 100 s: half a second a pixel column.
  const X0 = 0, X1 = 100, WIDTH = 200;
  const xs = (from, to, n) => Array.from({ length: n }, (_, i) => from + ((to - from) * i) / Math.max(1, n - 1));
  /** A window of the hub's history from `from` to `to`, of `n` points, each of `ratio` rows. */
  const windowOf = (from, to, n, ratio = 8) => new Detail(new Trace(meta), { t0: from, t1: to, ratio, chunks: n ? [{ x: xs(from, to, n), y: xs(0, 1, n) }] : [] });
  /** A level of live points from `from` to `to`, `n` of them. */
  const levelOf = (from, to, n) => {
    const l = new Points(1e9);
    if (n) l.push(xs(from, to, n), xs(0, 1, n), false);
    return l;
  };
  const pick = (...held) => held.indexOf(pickSource(held, X0, X1, WIDTH));

  it('is the densest of what spans the view, whichever layer it is', () => {
    // A detail window read for a view thirty times as wide, the overview, and the live level.
    expect(pick(windowOf(-1000, 2000, 900), windowOf(-5000, 100, 2600), levelOf(-20, 120, 1400))).toBe(2);
    expect(pick(windowOf(-1000, 2000, 900), windowOf(-500, 100, 2400), levelOf(-20, 120, 280))).toBe(1);
    expect(pick(windowOf(-100, 200, 1500), windowOf(-5000, 100, 2600), levelOf(-20, 120, 280))).toBe(0);
  });

  it('is a window of the raw rows over anything else that spans the view, and the first of two as dense', () => {
    expect(pick(windowOf(-10, 110, 120, 1), windowOf(-10, 110, 600), levelOf(-10, 110, 1200))).toBe(0);
    expect(pick(windowOf(-10, 110, 600), windowOf(-10, 110, 120, 1), levelOf(-10, 110, 1200))).toBe(1);
    expect(pick(windowOf(-100, 200, 600), windowOf(-100, 200, 600), levelOf(-100, 200, 3))).toBe(0);
  });

  it('is what spans the most of the view when none spans it all, however coarse, to the pixel column', () => {
    expect(pick(windowOf(50, 150, 4000), windowOf(-900, 100, 300), levelOf(60, 100, 4000))).toBe(1);
    expect(pick(windowOf(50, 150, 4000), windowOf(20, 100, 300), levelOf(10, 100, 30))).toBe(2);
    // A fifth of a column short of the view's start counts as spanning it.
    expect(pick(windowOf(0.1, 150, 4000), levelOf(-50, 100, 30))).toBe(0);
    expect(pick(windowOf(0.4, 150, 4000), levelOf(-50, 100, 30))).toBe(1);
  });

  it('is the first held when none has a point', () => {
    expect(pick(windowOf(0, 100, 0), levelOf(0, 0, 0))).toBe(0);
  });

  it('spans as many pixel columns of the view as anything held, and no other that spans as many has more points across it', () => {
    const source = fc.record({
      window: fc.boolean(),
      from: fc.integer({ min: -300, max: 90 }),
      length: fc.integer({ min: 1, max: 600 }),
      n: fc.integer({ min: 0, max: 3000 }),
      raw: fc.boolean(),
    });
    const seen = { whole: 0, part: 0 };
    fc.assert(
      fc.property(fc.array(source, { minLength: 1, maxLength: 4 }), (made) => {
        const held = made.map((m) => (m.window ? windowOf(m.from, m.from + m.length, m.n, m.raw ? 1 : 8) : levelOf(m.from, m.from + m.length, m.n)));
        const columns = (s) => (s.length ? Math.round(((Math.min(s.xAt(s.absEnd - 1), X1) - Math.max(s.xAt(s.absStart), X0)) / (X1 - X0)) * WIDTH) : 0);
        const points = (s) => (s.ratio === 1 && s.length ? Infinity : [...s.x.subarray(s.start, s.end)].filter((x) => x >= X0 && x < X1).length);
        const src = pickSource(held, X0, X1, WIDTH);
        expect(held).toContain(src);
        const most = Math.max(0, ...held.map(columns));
        expect(Math.max(0, columns(src))).toBe(most);
        const rivals = held.filter((s) => Math.max(0, columns(s)) === most);
        expect(points(src)).toBe(Math.max(...rivals.map(points)));
        // Of those as dense, the first held.
        expect(rivals.find((s) => points(s) === points(src))).toBe(src);
        seen[most === WIDTH ? 'whole' : 'part']++;
      }),
      { numRuns: 500 },
    );
    expect(seen.whole).toBeGreaterThan(0);
    expect(seen.part).toBeGreaterThan(0);
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
      grid: () => dpr,
      ring: () => ({ base: 0 }),
      lane: (rect, base, x0, x1) => ((drawn.lane = { x0, x1, width: Math.round(rect.w * dpr) }), true),
      drawTrace: (src, ring, a0, a1) => drawn.draws.push({ src, a0, a1 }),
    };
    const hub = { now: () => hub.t, t: 0, session: { windowOf: () => undefined } };
    const view = new View(hub);
    view.span = span;
    view.delay = 0;
    const plot = { x: 0, y: 0, w: cssWidth, h: 100 }, cfg = { kind: 'time', mode: 'overlay' };
    const pv = new PlotView({ body: document.createElement('div'), overlay: document.createElement('canvas'), config: () => cfg, hub, view, targets: () => [view], hoverTime: { t: null }, plotId: 'p', renderer, colors: new Colors(null) });
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
