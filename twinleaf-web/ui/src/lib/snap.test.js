import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { features, nearestPeak, peaks, pickSnap, SNAP_ORDER, SNAP_PX, snapCandidates, STAND, stepPeak } from './snap.js';

/** A signal of points (xs[i], ys[i]), as snap.js reads one. */
const signal = (key, xs, ys, more = {}) => ({ key, n: xs.length, x: (i) => xs[i], y: (i) => ys[i], stand: 0, ...more });
/** A plot `w` px wide showing [lo, hi]: value to px. */
const scale = ([lo, hi], w) => (v) => ((v - lo) / (hi - lo)) * w;

const view = fc.tuple(fc.double({ min: -1e3, max: 1e3, noNaN: true }), fc.double({ min: 1, max: 1e3, noNaN: true })).map(([lo, span]) => [lo, lo + span]);
/** Values about `v`, some in it and some past its ends. */
const about = ([lo, hi], n) => fc.array(fc.double({ min: lo - (hi - lo) / 4, max: hi + (hi - lo) / 4, noNaN: true }), { maxLength: n });
const points = ([lo, hi]) =>
  fc.array(fc.double({ min: -5, max: 5, noNaN: true }), { minLength: 0, maxLength: 60 }).map((ys) => {
    const xs = ys.map((_, i) => lo - (hi - lo) / 8 + ((i + 0.5) / ys.length) * (hi - lo) * 1.25);
    return [xs, ys];
  });
const xCase = view.chain((v) =>
  fc.record({ view: fc.constant(v), ticks: about(v, 12), other: fc.option(about(v, 1).map((a) => a[0] ?? null)), sets: fc.array(points(v), { maxLength: 3 }), at: fc.double({ min: -20, max: 620, noNaN: true }), stand: fc.double({ min: 0, max: 3, noNaN: true }) }),
);

/** The candidate a cursor at `at` px should snap to, found the long way: every one by its distance, then by SNAP_ORDER. */
function expected(cands, at, px) {
  const near = cands.map((c) => ({ c, d: Math.abs(px(c.v) - at) })).filter(({ d }) => d <= SNAP_PX);
  near.sort((a, b) => a.d - b.d || SNAP_ORDER.indexOf(a.c.from) - SNAP_ORDER.indexOf(b.c.from));
  return near[0]?.c ?? null;
}

describe('the points a cursor dragged with Shift snaps to', () => {
  it('are in view, and the one chosen is the nearest within 10 px of where the cursor would be, a tie going to a tick, then the other cursor, then data, or none', () => {
    fc.assert(
      fc.property(xCase, ({ view, ticks, other, sets, at, stand }) => {
        const px = scale(view, 600);
        const signals = sets.map(([xs, ys], i) => signal(`s${i}`, xs, ys, { stand }));
        const near = [view[0] + ((at - SNAP_PX) / 600) * (view[1] - view[0]), view[0] + ((at + SNAP_PX) / 600) * (view[1] - view[0])];
        const cands = snapCandidates({ axis: 'x', view, ticks, other, signals, near });
        for (const c of cands) {
          expect(c.v, `${c.from} in view`).toBeGreaterThanOrEqual(view[0]);
          expect(c.v, `${c.from} in view`).toBeLessThanOrEqual(view[1]);
        }
        expect(cands.filter((c) => c.from === 'tick').map((c) => c.v)).toEqual(ticks.filter((v) => v >= view[0] && v <= view[1]));
        const got = pickSnap(cands, at, px);
        expect(got).toBe(expected(cands, at, px));
        if (got) expect(Math.abs(px(got.v) - at)).toBeLessThanOrEqual(SNAP_PX);
      }),
    );
  });

  it('break a tie at one distance by the order tick, other cursor, data, and look no further than 10 px', () => {
    const px = (v) => v;
    const tick = { v: 5, from: 'tick' }, cursor = { v: 5, from: 'cursor' }, data = { v: 5, from: 'data' };
    expect(pickSnap([data, cursor, tick], 0, px)).toBe(tick);
    expect(pickSnap([data, cursor], 0, px)).toBe(cursor);
    expect(pickSnap([data, { v: -6, from: 'tick' }], 0, px)).toBe(data);
    expect(pickSnap([{ v: 10.01, from: 'tick' }, { v: -10.5, from: 'data' }], 0, px)).toBeNull();
    expect(pickSnap([{ v: 10, from: 'data' }], 0, px)?.v).toBe(10);
  });

  it('of an X cursor, hold the peaks and troughs of each signal near it that stand out, and of a spectrum its peaks alone', () => {
    // A sine, three cycles over the view, with ripples that stand out by less than `stand`.
    const xs = Array.from({ length: 601 }, (_, i) => i / 100), ys = xs.map((x) => Math.sin(2 * Math.PI * x / 2) + 0.05 * Math.sin(2 * Math.PI * x * 7));
    const s = signal('sine', xs, ys, { stand: 0.5 });
    const found = features(s, [0, 6], [0, 6], s).map((i) => Math.round(xs[i] * 10) / 10);
    expect(found).toEqual([0.5, 1.5, 2.5, 3.5, 4.5, 5.5].map((x) => expect.closeTo(x, 0)));
    expect(found).toHaveLength(6);
    // Near 1.5 alone: the trough there.
    expect(features(s, [0, 6], [1.3, 1.7], s).map((i) => ys[i] < 0)).toEqual([true]);
    // Peaks only, as a spectrum has them.
    expect(features(s, [0, 6], [0, 6], { ...s, troughs: false }).every((i) => ys[i] > 0)).toBe(true);
    // A ripple alone stands out by 0.1 at most: none counts at a stand of 0.5, each at 0.01.
    const ripple = signal('ripple', xs, xs.map((x) => 0.05 * Math.sin(2 * Math.PI * x * 7)), { stand: 0.5 });
    expect(features(ripple, [0, 6], [0, 6], ripple)).toEqual([]);
    expect(features(ripple, [0, 6], [0, 6], { ...ripple, stand: 0.01 }).length).toBeGreaterThan(70);
  });

  it('of a spectrum, a peak standing 6 dB over the bins about it, in amplitude, and not one standing 5 dB', () => {
    const f = [1, 2, 3, 4, 5, 6, 7], db = (d) => 10 ** (d / 20);
    const spectrum = (d) => signal('asd', f, [1, 1, 1, db(d), 1, 1, 1], { stand: 6 / 20, troughs: false, level: Math.log10 });
    expect(features(spectrum(6.01), [1, 7], [1, 7], spectrum(6.01))).toEqual([3]);
    expect(features(spectrum(5), [1, 7], [1, 7], spectrum(5))).toEqual([]);
  });

  it('take a feature from the points in view only, its prominence measured within it', () => {
    const xs = [0, 1, 2, 3, 4, 5, 6], ys = [0, 5, 0, 3, 9, 0, 0];
    const s = signal('s', xs, ys, { stand: 2, troughs: false });
    // In view [0, 6] the 3 at x 3 stands 3 over the 0 to its left but not over the 9 to its right: no peak.
    expect(features(s, [0, 6], [0, 6], s)).toEqual([1, 4]);
    // The trough at x 2 stands 3 under both sides; the one at x 5 stands under nothing to its right.
    expect(features(s, [0, 6], [0, 6], { ...s, troughs: true })).toEqual([1, 2, 4]);
    // A view that ends at x 3 leaves the peak at x 4 out, and x 3 has no neighbour in view on its right.
    expect(features(s, [0, 3], [0, 6], s)).toEqual([1]);
  });

  it('of a Y cursor, hold the ticks, the other cursor, each signal at the X cursors, its least and greatest in view, and 0, each in view', () => {
    const xs = [0, 1, 2, 3, 4], ys = [2, -1, 4, 3, 7];
    const cands = snapCandidates({ axis: 'y', view: [-2, 6], ticks: [-2, 0, 2, 4, 6, 8], other: 1.5, signals: [signal('s', xs, ys)], xs: [1.2, null], xView: [0.5, 3.5] });
    const by = (from) => cands.filter((c) => c.from === from).map((c) => c.v);
    expect(by('tick')).toEqual([-2, 0, 2, 4, 6]);
    expect(by('cursor')).toEqual([1.5]);
    // At X1, x 1.2, the point at x 1; in view, x 1 to 3, the least -1 and the greatest 4; and 0. The 7 at x 4 is out of view.
    expect(by('data')).toEqual([-1, -1, 4, 0]);
    expect(cands.find((c) => c.from === 'data' && c.v === 4).point).toEqual({ x: 2, y: 4 });
  });
});

/** Slots of 10 points; a peak or a bump stands in the middle of its slot, and a slot's ends are on the floor. */
const SLOTS = 20, SLOT = 10;
/**
 * A signal of `kind` with known peaks, as {s, at}, `at` the x of each in
 * order: a triangle in each slot chosen, in `level`, on a flat floor of 0,
 * and in every other slot a bump that stands out by less than the stand. A
 * time series, x a tenth of a second apart, its peaks 1 to 3 over the floor
 * and its bumps 0.1, against a stand of STAND.timeShare of a range 0 to 3;
 * or a spectrum, bins 1 Hz from 1 Hz, its peaks 7 to 30 dB over the floor
 * and its bumps 4 dB.
 */
const peaked = (kind) =>
  fc.uniqueArray(fc.nat(SLOTS - 1), { maxLength: 8 }).chain((slots) =>
    fc.array(kind === 'time' ? fc.double({ min: 1, max: 3, noNaN: true }) : fc.double({ min: 7, max: 30, noNaN: true }), { minLength: slots.length, maxLength: slots.length }).map((tall) => {
      const height = Array.from({ length: SLOTS }, (_, k) => (slots.includes(k) ? tall[slots.indexOf(k)] : kind === 'time' ? 0.1 : 4));
      const lv = Array.from({ length: SLOTS * SLOT }, (_, i) => {
        const k = Math.floor(i / SLOT), d = Math.abs((i % SLOT) - SLOT / 2);
        return (kind === 'time' ? height[k] : height[k] / 20) * Math.max(0, 1 - d / 3);
      });
      const xs = lv.map((_, i) => (kind === 'time' ? i / 10 : i + 1));
      const s = kind === 'time' ? signal(kind, xs, lv, { stand: STAND.timeShare * 3 }) : signal(kind, xs, lv.map((v) => 10 ** v), { stand: STAND.spectrumDb / 20, troughs: false, level: Math.log10 });
      return { s, xs, at: [...slots].sort((a, b) => a - b).map((k) => xs[k * SLOT + SLOT / 2]) };
    }),
  );
/** The x of slot boundary `k`, on the floor, of a signal `peaked` made. */
const boundary = (xs, k) => xs[Math.min(k * SLOT, xs.length - 1)];

describe('the peaks an X cursor goes to', () => {
  it('of a time series or a spectrum, are those that stand out by the rule, each once in order; the nearest is the nearest of them', () => {
    fc.assert(
      fc.property(fc.constantFrom('time', 'spectrum').chain((kind) => fc.tuple(peaked(kind), fc.double({ min: 0, max: 1, noNaN: true }))), ([{ s, xs, at }, f]) => {
        const view = [xs[0], xs.at(-1)];
        // Merged with itself, each once.
        expect(peaks([s, s], view)).toEqual(at);
        const from = view[0] + f * (view[1] - view[0]);
        const nearest = at.reduce((best, x) => (best === null || Math.abs(x - from) < Math.abs(best - from) ? x : best), null);
        expect(nearestPeak(peaks([s], view), from)).toBe(nearest);
      }),
    );
  });

  it('step right and left through every peak of the view in order and stop at the last, then past the view’s edge to the nearest beyond it', () => {
    fc.assert(
      fc.property(fc.constantFrom('time', 'spectrum').chain((kind) => fc.tuple(peaked(kind), fc.integer({ min: 0, max: SLOTS }), fc.integer({ min: 0, max: SLOTS }))), ([{ s, xs, at }, i, j]) => {
        const held = [xs[0], xs.at(-1)];
        const view = [boundary(xs, Math.min(i, j)), boundary(xs, Math.max(i, j))];
        const inView = peaks([s], view);
        expect(inView).toEqual(at.filter((x) => x > view[0] && x < view[1]));
        const all = () => peaks([s], held);
        for (const dir of [1, -1]) {
          // From the view's far edge, each step the next peak of the view, to the last of them.
          const seen = [];
          for (let x = stepPeak(inView, view[dir > 0 ? 0 : 1], dir); x !== null; x = stepPeak(inView, x, dir)) seen.push(x);
          expect(seen).toEqual(dir > 0 ? inView : [...inView].reverse());
          // Past the last, the nearest of what the plot holds beyond the view, and on through every one to the end.
          const past = at.filter((x) => dir * (x - view[dir > 0 ? 1 : 0]) > 0);
          const beyond = [];
          for (let x = stepPeak(inView, seen.at(-1) ?? view[dir > 0 ? 1 : 0], dir, all); x !== null; x = stepPeak([], x, dir, all)) beyond.push(x);
          expect(beyond, `dir ${dir}`).toEqual(dir > 0 ? past : [...past].reverse());
        }
        // From the last peak of all, nothing further either way.
        if (at.length) {
          expect(stepPeak(peaks([s], held), at.at(-1), 1)).toBeNull();
          expect(stepPeak(peaks([s], held), at[0], -1)).toBeNull();
        }
      }),
    );
  });

  it('are peaks alone, on a time plot too, never a trough', () => {
    const s = signal('s', [0, 1, 2, 3, 4, 5, 6], [0, 1, 0, -5, 0, 1, 0], { stand: 0.5 });
    expect(peaks([s], [0, 6])).toEqual([1, 5]);
  });

  it('on a log axis, the nearest along the log scale', () => {
    // 10 and 1000 about 100: equal on a log scale, so the lower. 300 is nearer 100 than 20 is on a log scale, though not on a linear one.
    expect(nearestPeak([10, 1000], 100, Math.log10)).toBe(10);
    expect(nearestPeak([20, 300], 100, Math.log10)).toBe(300);
    expect(nearestPeak([20, 300], 100)).toBe(20);
    expect(nearestPeak([], 100)).toBeNull();
  });
});
