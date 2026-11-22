// A capture's shots: calibration, which ones a capture keeps, and what a
// plot shows of them, each against a rule worked out apart from the module.
import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { SHOTS_KEPT, SHOT_COUNT, SHOT_COUNTS, alike, calibrated, keep, mean, run, shotCount, shotMode, shotOf, shotsCsv, shown, shownText, spectraShown, xsOf } from './shots.js';

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

  it('make a shot: the frame’s number, its length, where its x starts and steps, its values, its element type and when it was received', () => {
    const frame = { number: 7, x0: 1, dx: 0.25, calibration: 0.5, type: 0x21, elements: Int16Array.of(1, -2, 3) };
    expect(shotOf(frame, 1234)).toEqual({ number: 7, n: 3, x0: 1, dx: 0.25, y: Float32Array.of(0.5, -1, 1.5), type: 0x21, received: 1234, spectrum: null });
    expect([...xsOf(shotOf(frame, 0))]).toEqual([1, 1.25, 1.5]);
  });

  it('carry the hub’s spectrum in Hz and per √Hz where their x is time, and none where it is not', () => {
    const frame = (xUnits) => ({ number: 1, x0: 0, dx: 1, calibration: 1, type: 0x42, xUnits, elements: Float32Array.of(0, 1), spectrum: { f: Float32Array.of(0.25, 0.5), a: Float32Array.of(2, 4) } });
    expect(shotOf(frame('s'), 0).spectrum).toEqual({ f: Float32Array.of(0.25, 0.5), a: Float32Array.of(2, 4) });
    // Cycles a µs are a million Hz; a density per root cycle a µs is a thousandth of it per √Hz.
    const us = shotOf(frame('µs'), 0).spectrum;
    expect([...us.f]).toEqual([250000, 500000]);
    expect([...us.a].map((a) => +a.toPrecision(6))).toEqual([0.002, 0.004]);
    expect(shotOf(frame('rad'), 0).spectrum, 'an x that is no time').toBe(null);
    expect(shotOf({ ...frame('s'), spectrum: { f: new Float32Array(), a: new Float32Array() } }, 0).spectrum, 'no estimate').toBe(null);
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

  it('as spectra: the newest’s, each of an overlay’s, or the power average of the spectra of the shots alike, not the spectrum of their mean', () => {
    const withSpectrum = (s) => ({ ...s, spectrum: { f: Float32Array.of(1, 2, 3), a: Float32Array.from({ length: 3 }, (_, i) => s.number + i) } });
    fc.assert(
      fc.property(shots, count, (plain, n) => {
        const held = plain.map(withSpectrum), newest = held.at(-1).spectrum;
        expect(spectraShown(held, 'latest', n)).toMatchObject({ line: newest, under: [], count: 1 });
        const o = spectraShown(held, 'overlay', n), s = shown(held, 'overlay', n);
        expect([o.line, o.under.map((u) => u.shot), o.under.map((u) => u.a), o.under.map((u) => u.alpha)]).toEqual([newest, s.under.map((u) => u.shot), s.under.map((u) => u.shot.spectrum.a), s.under.map((u) => u.alpha)]);
        // Each bin the root of the mean of the squares over the run of shots alike.
        const of = run(held, n), a = spectraShown(held, 'average', n);
        expect([a.count, a.under]).toEqual([of.length, []]);
        [...a.line.a].forEach((v, i) => expect(v).toBeCloseTo(Math.sqrt(of.reduce((sum, x) => sum + (x.number + i) ** 2, 0) / of.length), 3));
      }),
    );
    // Two shots of opposite sign average to nothing; their spectra, each of density 1, to 1.
    const a = { ...shot(1, [2, 0, 1]), y: Float32Array.of(1, -1), spectrum: { f: Float32Array.of(1), a: Float32Array.of(1) } }, b = { ...a, number: 2, y: Float32Array.of(-1, 1) };
    expect([...shown([a, b], 'average', 4).line.y]).toEqual([0, 0]);
    expect([...spectraShown([a, b], 'average', 4).line.a]).toEqual([1]);
    expect(spectraShown([{ ...a, spectrum: null }], 'latest', 4)).toBe(null);
  });

  it('is written as CSV: a header of what it is, then its x and a column per shot shown, as the plot shows them', () => {
    const fidShot = (number) => ({ ...shot(number, [3, 1e-6, 0.5e-6]), type: 0x21, received: Date.UTC(2026, 10, 12, 14, 5, number) });
    const capture = { title: 'FID', rpc: 'fid.capture', meta: { units: 'V', xName: 'Time', xUnits: 's' } };
    const held = [1, 2, 3].map(fidShot);
    const lines = (mode, n = 4, c = capture, h = held) => shotsCsv(c, h, mode, n, 'test 12 (/)').trimEnd().split('\n');
    expect(lines('latest')).toEqual([
      '# FID, capture RPC fid.capture of test 12 (/)',
      '# 3 samples of i16, in V, sample rate 2000000 Hz',
      '# shows the newest shot',
      '# shot 3 received 2026-11-12T14:05:03.000Z',
      'time (s),FID shot 3 (V)',
      '0.000001,3', '0.0000015,4', '0.000002,5',
    ]);
    expect(lines('overlay').slice(2, 7)).toEqual([
      '# shows the last 3 shots of 4, overlaid',
      '# shot 1 received 2026-11-12T14:05:01.000Z', '# shot 2 received 2026-11-12T14:05:02.000Z', '# shot 3 received 2026-11-12T14:05:03.000Z',
      'time (s),FID shot 1 (V),FID shot 2 (V),FID shot 3 (V)',
    ]);
    expect(lines('overlay').slice(7)).toEqual(['0.000001,1,2,3', '0.0000015,2,3,4', '0.000002,3,4,5']);
    // An average names how many it took, and when each was received.
    expect(lines('average', 2).slice(2)).toEqual([
      '# shows the average of the last 2 shots alike of 2',
      '# shot 2 received 2026-11-12T14:05:02.000Z', '# shot 3 received 2026-11-12T14:05:03.000Z',
      'time (s),FID average of 2 (V)',
      '0.000001,2.5', '0.0000015,3.5', '0.000002,4.5',
    ]);
    // An x in µs is written in seconds; an x that is no time in its own units, with no sample rate.
    expect(lines('latest', 4, { ...capture, meta: { ...capture.meta, xUnits: 'µs' } }, [{ ...fidShot(1), x0: 2, dx: 0.5 }]).slice(1, 6)).toEqual(['# 3 samples of i16, in V, sample rate 2000000 Hz', '# shows the newest shot', '# shot 1 received 2026-11-12T14:05:01.000Z', 'time (s),FID shot 1 (V)', '0.000002,1']);
    expect(lines('latest', 4, { title: 'phase, wrapped', rpc: 'phase.capture', meta: { units: '', xName: 'Angle', xUnits: 'rad' } }).slice(1, 6)).toEqual(['# 3 samples of i16', '# shows the newest shot', '# shot 3 received 2026-11-12T14:05:03.000Z', 'Angle (rad),"phase, wrapped shot 3"', '0.000001,3']);
    expect(shotsCsv(capture, [], 'latest', 4, '')).toBe(null);
  });

  it('takes a plot’s settings: a mode and a count it offers, or Latest and the count it starts at', () => {
    expect(['latest', 'overlay', 'average', 'nonsense', undefined].map(shotMode)).toEqual(['latest', 'overlay', 'average', 'latest', 'latest']);
    expect([4, 16, 64, 5, undefined].map(shotCount)).toEqual([4, 16, 64, SHOT_COUNT, SHOT_COUNT]);
  });
});
