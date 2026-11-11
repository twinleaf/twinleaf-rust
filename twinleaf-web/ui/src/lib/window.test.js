import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { WINDOW_LIMITS, parseSpan, windowParts, windowText, windowTyped } from './window.js';

describe('The window field', () => {
  it('shows a span in the unit that reads shortest, exactly where one does', () => {
    const shown = [90, 30, 60, 120, 600, 3600, 5400, 86400, 0.5, 0.002, 1.5, 7.408, 231, 110, 1100, 3599.5, 45 * 60].map(windowText);
    expect(shown).toEqual(['1.5 min', '30 s', '1 min', '2 min', '10 min', '1 h', '1.5 h', '24 h', '500 ms', '2 ms', '1.5 s', '7.41 s', '3.85 min', '110 s', '18.3 min', '1 h', '45 min']);
    expect(windowParts(90)).toEqual(['1.5', 'min']);
  });

  it('writes any span from 1 ms to 24 h in at most four characters and a unit, which reads back as it within the precision shown', () => {
    fc.assert(
      // Evenly on a log scale: a double drawn between two exponents clusters at 0 and the ends.
      fc.property(fc.integer({ min: 0, max: 1e9 }).map((k) => 0.001 * 86_400_000 ** (k / 1e9)), (s) => {
        const [n, unit] = windowParts(s);
        expect(n.length, `${s} s as ${n} ${unit}`).toBeLessThanOrEqual(4);
        expect(Math.abs(parseSpan(`${n} ${unit}`) / s - 1), `${s} s as ${n} ${unit}`).toBeLessThanOrEqual(0.005 + 1e-12);
        expect(parseSpan(`${n}${unit}`)).toBe(parseSpan(`${n} ${unit}`));
        // The number the field shows, typed alone, is in the unit it shows.
        expect(parseSpan(n, unit)).toBe(parseSpan(`${n} ${unit}`));
      }),
    );
  });

  it('reads ms, s, sec, m, min, h, hr with or without a space, decimals, in either case, and a number alone in the unit shown', () => {
    const typed = { 90: 90, '90s': 90, '90 sec': 90, '5m': 300, '1.5 min': 90, '2h': 7200, '2 HR': 7200, '500ms': 0.5, '.5 S': 0.5, ' 1.25 Min ': 75, '3.': 3 };
    for (const [text, s] of Object.entries(typed)) expect(parseSpan(text), text).toBeCloseTo(s, 9);
    const alone = { s: 90, min: 5400, h: 324000, ms: 0.09 };
    for (const [unit, s] of Object.entries(alone)) expect(parseSpan('90', unit), unit).toBeCloseTo(s, 9);
    expect(parseSpan('90 s', 'h')).toBe(90);
  });

  it('refuses no span, nothing, a negative or zero span, text and a unit it does not know', () => {
    for (const text of ['', '  ', '0', '0 s', '-5', '-5 min', 'soon', '5 days', '5 m s', '1e3', '1.2.3', 'min']) expect(parseSpan(text), JSON.stringify(text)).toBe(null);
  });

  it('refuses a window more than the view can show past the precision it is shown to, says which refusal, and takes one under the narrowest as the narrowest', () => {
    expect(windowTyped('10 min', 600)).toEqual({ s: 600 });
    expect(windowTyped('601', 600)).toEqual({ s: 600 });
    expect(windowTyped('11 min', 600)).toEqual({ refused: 'long' });
    expect(windowTyped('11', 600, 'min')).toEqual({ refused: 'long' });
    expect(windowTyped('9', 600, 'min')).toEqual({ s: 540 });
    expect(windowTyped('2h', 86400)).toEqual({ s: 7200 });
    expect(windowTyped('0.5ms', 600)).toEqual({ s: WINDOW_LIMITS[0] });
    expect(windowTyped('soon', 600)).toEqual({ refused: 'none' });
  });
});
