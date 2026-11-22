// How the menus name the X cursors (round 2 Menus, the suggested
// formatters): over every time base, any time and any span, X1 and X2 read
// apart, a Unix pair that leaves its day or is not today's carries its date,
// a span past a minute reads in minutes, hours or days, and another clock's
// seconds group their digits.
import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { cursorText } from './plot.js';

/** A span from a microsecond to a year, spread evenly over its decades. */
const span = fc.double({ min: -6, max: 7.5, noNaN: true }).map((e) => 10 ** e);
/** A date before the clock, as 30 Sept 21:24:21. */
const DATED = /^\d{1,2} \p{L}+ \d\d:/u;

describe('the cursors’ text', () => {
  it('tells X1 from X2, whatever the time base, the time and the span', () => {
    let checked = 0;
    fc.assert(
      fc.property(fc.constantFrom('unix', 'zero'), fc.double({ min: 0, max: 2e9, noNaN: true }), span, (kind, t, s) => {
        // A Unix time far from now, and a span too small for its float, cannot be told apart by any text.
        fc.pre(s > Math.abs(t) * 1e-14);
        const text = cursorText(kind);
        expect(text.at(t, s), `${kind} ${t} and ${s} later`).not.toBe(text.at(t + s, s));
        checked++;
      }),
    );
    expect(checked).toBeGreaterThan(50);
  });

  it('dates a Unix pair that is not today’s or runs past its day, and no other', () => {
    const seen = new Set();
    const midnight = new Date();
    midnight.setHours(0, 0, 0, 0);
    const today = midnight.getTime() / 1000;
    fc.assert(
      fc.property(fc.integer({ min: -40, max: 0 }), fc.double({ min: 0, max: 86399, noNaN: true }), span, (days, into, s) => {
        const t = today + days * 86400 + into;
        const day = (x) => new Date(x * 1000).toDateString();
        const dated = days !== 0 || day(t) !== day(t + s);
        expect(DATED.test(cursorText('unix').at(t, s)), `${days} d, ${into} s in, ${s} s apart`).toBe(dated);
        seen.add(dated);
      }),
      // A pair within today, which a random day is one time in forty, and one of yesterday.
      { examples: [[0, 10, 1], [-1, 100, 5]] },
    );
    expect(seen.size).toBe(2);
  });

  it('names a span past a minute in minutes, hours or days, and a shorter one in seconds', () => {
    for (const kind of ['unix', 'zero']) {
      fc.assert(
        fc.property(span, (s) => {
          const text = cursorText(kind).span(s);
          if (s < 60) expect(text).toMatch(/^\d+(\.\d+)? [µmn]?s$/);
          else expect(text).toMatch(/^\d+ (min \d\d s|h \d\d min|d \d\d h)$/);
        }),
      );
    }
  });

  it('groups another clock’s digits in threes, and gives a spectrum’s frequencies an SI prefix', () => {
    fc.assert(
      fc.property(fc.double({ min: 0, max: 1e9, noNaN: true }), fc.oneof(fc.constant(0), fc.double({ min: 1e-3, max: 1e6, noNaN: true })), span, (t, f, s) => {
        expect(cursorText('zero').at(t, s).split('.')[0]).not.toMatch(/\d{4}/);
        expect(cursorText('hz').at(f, s)).toMatch(/^\d+(\.\d+)? [µmkM]?Hz$/);
      }),
    );
  });

  it('gives a capture’s x an SI prefix on its own unit, a place and a distance alike', () => {
    const seconds = cursorText({ unit: 's' });
    expect([seconds.at(250e-6, 1e-4), seconds.span(1.5e-3), seconds.at(0, 1)]).toEqual(['250.0 µs', '1.500 ms', '0 s']);
    // A unit the page does not know is written as reported.
    expect(cursorText({ unit: 'samples' }).at(1200, 10)).toBe('1200 samples');
    fc.assert(
      fc.property(fc.double({ min: 1e-9, max: 10, noNaN: true }), (x) => {
        expect(seconds.at(x, 1)).toMatch(/^\d+(\.\d+)? [nµm]?s$/);
        expect(seconds.span(x)).toBe(seconds.at(x, 1));
      }),
    );
  });
});
