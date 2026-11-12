// A capture's shots: calibration, which ones a capture keeps, and what a
// plot shows of them, each against a rule worked out apart from the module.
import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { SHOTS_KEPT, SHOT_COUNT, SHOT_COUNTS, alike, calibrated, keep, mean, run, shotCount, shotMode, shotOf, shown, shownText, xsOf } from './shots.js';

/** A shot numbered `number` of shape `shape`, [n, x0, dx], its values `number` + i. */
const shot = (number, [n, x0, dx]) => ({ number, n, x0, dx, y: Float32Array.from({ length: n }, (_, i) => number + i) });
/** Shapes a device's shots may have: a few lengths, starts and strides. */
const shape = fc.tuple(fc.constantFrom(2, 3, 5), fc.constantFrom(0, 1), fc.constantFrom(0.5, 0.25));
/** Shots as a capture holds them, oldest first, numbered from 1, their shapes mostly the same and now and then another. */
const shots = fc.array(fc.oneof({ weight: 5, arbitrary: fc.constant(null) }, { weight: 1, arbitrary: shape }), { minLength: 1, maxLength: 80 }).map((changes) => {
  let at = [3, 0, 0.5];
  return changes.map((to, i) => shot(i + 1, (at = to ?? at)));
});
const count = fc.constantFrom(...SHOT_COUNTS);

describe('a capture’s elements', () => {
  it('are each times the calibration, as 32-bit floats, whatever their type', () => {
    fc.assert(
      fc.property(fc.array(fc.integer({ min: -32768, max: 32767 }), { maxLength: 30 }), fc.float({ noNaN: true, noDefaultInfinity: true, min: -4, max: 4 }), (values, calibration) => {
        const y = calibrated(Int16Array.from(values), calibration);
        expect(y).toBeInstanceOf(Float32Array);
        expect([...y]).toEqual(values.map((v) => Math.fround(v * calibration)));
      }),
    );
    // 64-bit elements are numbers too.
    expect([...calibrated(BigInt64Array.of(-3n, 4n), 0.5)]).toEqual([-1.5, 2]);
  });

  it('are the values themselves, not a copy, when they are 32-bit floats of calibration 1', () => {
    const elements = Float32Array.of(1, 2, 3);
    expect(calibrated(elements, 1)).toBe(elements);
    expect(calibrated(elements, 2)).not.toBe(elements);
    expect(calibrated(Float64Array.of(1, 2), 1)).toBeInstanceOf(Float32Array);
  });

  it('make a shot: the frame’s number, its length, where its x starts and steps, and its values', () => {
    const frame = { number: 7, x0: 1, dx: 0.25, calibration: 0.5, elements: Int16Array.of(1, -2, 3) };
    expect(shotOf(frame)).toEqual({ number: 7, n: 3, x0: 1, dx: 0.25, y: Float32Array.of(0.5, -1, 1.5) });
    expect([...xsOf(shotOf(frame))]).toEqual([1, 1.25, 1.5]);
  });
});

describe('the shots a capture keeps', () => {
  it('are its newest, SHOTS_KEPT at most, in the order they came', () => {
    fc.assert(
      fc.property(fc.nat(3 * SHOTS_KEPT), (n) => {
        const kept = [];
        for (let i = 1; i <= n; i++) keep(kept, shot(i, [2, 0, 1]));
        expect(kept.map((s) => s.number)).toEqual(Array.from({ length: Math.min(n, SHOTS_KEPT) }, (_, i) => n - Math.min(n, SHOTS_KEPT) + 1 + i));
      }),
    );
  });

  it('hold the most a plot may take', () => {
    expect(SHOTS_KEPT).toBe(Math.max(...SHOT_COUNTS));
    expect(SHOT_COUNTS).toContain(SHOT_COUNT);
  });
});

describe('what a plot shows of a capture’s shots', () => {
  it('is nothing with no shot, and in Latest the newest alone, one of one', () => {
    for (const mode of ['latest', 'overlay', 'average']) expect(shown([], mode, 16)).toBe(null);
    fc.assert(
      fc.property(shots, count, (held, n) => {
        expect(shown(held, 'latest', n)).toEqual({ line: held.at(-1), under: [], count: 1, of: 1 });
      }),
    );
  });

  it('in Overlay is the newest over the shots before it, as many as the count less one, oldest first and the older the fainter', () => {
    fc.assert(
      fc.property(shots, count, (held, n) => {
        const s = shown(held, 'overlay', n), before = held.slice(-n).slice(0, -1);
        expect(s.line).toBe(held.at(-1));
        expect(s.under.map((u) => u.shot)).toEqual(before);
        expect([s.count, s.of]).toEqual([Math.min(held.length, n), n]);
        // Each under the newest is fainter than it, and no older one is plainer than a newer one.
        s.under.forEach((u, i) => {
          expect(u.alpha).toBeGreaterThan(0);
          expect(u.alpha).toBeLessThan(1);
          if (i) expect(u.alpha).toBeGreaterThanOrEqual(s.under[i - 1].alpha);
        });
        if (s.under.length > 1) expect(s.under.at(-1).alpha).toBeGreaterThan(s.under[0].alpha);
      }),
    );
  });

  it('in Average is the mean, value by value, of the newest shots alike, the count at most: a shot that differs starts it again', () => {
    fc.assert(
      fc.property(shots, count, (held, n) => {
        const s = shown(held, 'average', n), newest = held.at(-1);
        // The run that ends at the newest, by the rule: back from it while each is alike.
        let from = held.length - 1;
        while (from > 0 && held.length - from < n && alike(held[from - 1], newest)) from--;
        const of = held.slice(from);
        expect([s.count, s.of, s.under]).toEqual([of.length, n, []]);
        expect([s.line.number, s.line.n, s.line.x0, s.line.dx]).toEqual([newest.number, newest.n, newest.x0, newest.dx]);
        // Each value is `number` + i, so the mean of the run is the mean of its numbers, plus i.
        const numbers = of.reduce((sum, x) => sum + x.number, 0) / of.length;
        [...s.line.y].forEach((v, i) => expect(v).toBeCloseTo(numbers + i, 3));
        expect(run(held, n)).toEqual(of);
      }),
    );
  });

  it('means shots alike exactly: of one it is that shot, of two halves their sum', () => {
    const a = shot(1, [3, 0, 1]), b = shot(4, [3, 0, 1]);
    expect([...mean([a])]).toEqual([...a.y]);
    expect([...mean([a, b])]).toEqual([2.5, 3.5, 4.5]);
    // A shot of another length, start or stride is not alike.
    for (const other of [[4, 0, 1], [3, 1, 1], [3, 0, 2]]) expect(alike(a, shot(2, other))).toBe(false);
  });

  it('says how many it shows: an average’s and an overlay’s count of how many it takes, nothing of one alone', () => {
    expect(shownText('average', { count: 5, of: 16 })).toBe('avg 5 of 16');
    expect(shownText('average', { count: 16, of: 16 })).toBe('avg 16 of 16');
    expect(shownText('overlay', { count: 3, of: 4 })).toBe('last 3 of 4');
    expect(shownText('latest', { count: 1, of: 1 })).toBe('');
  });

  it('takes a plot’s settings: a mode and a count it offers, or Latest and the count it starts at', () => {
    expect(['latest', 'overlay', 'average', 'nonsense', undefined].map(shotMode)).toEqual(['latest', 'overlay', 'average', 'latest', 'latest']);
    expect([4, 16, 64, 5, undefined].map(shotCount)).toEqual([4, 16, 64, SHOT_COUNT, SHOT_COUNT]);
  });
});
