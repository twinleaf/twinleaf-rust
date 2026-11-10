// The rows a panel of properties shows under a filter: those of its rows
// whose name holds every word of the filter, whatever the case, in their
// order; no word shows them all.
import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { panelRows } from './properties.js';

const NAMES = ['dev.name', 'data.rate', 'field.gain', 'field.offset', 'therm.pid.Kp', 'therm.pid.Ki', 'Laser.power'];
const device = (names) => ({ route: '/', table: { rpcs: names.map((name) => ({ name, type: 'f32', access: ['read', 'write'], channel: 'setting', readable: true, persistent: false })) } });
const word = fc.oneof(fc.constantFrom('pid', 'FIELD', 'k', '.', 'rate', 'zz'), fc.string({ maxLength: 3 }).filter((s) => !/\s/.test(s)));

describe('a filter on a panel of properties', () => {
  it('shows the rows whose name holds every word, and only those, in order', () => {
    let narrowed = 0;
    fc.assert(
      fc.property(fc.subarray(NAMES, { minLength: 1 }), fc.array(word, { maxLength: 3 }), fc.constantFrom(' ', '  ', '\t'), (names, words, gap) => {
        const all = panelRows([device(names)], ['setting'], new Set()).map((r) => r.name);
        const shown = panelRows([device(names)], ['setting'], new Set(), ` ${words.join(gap)} `).map((r) => r.name);
        const real = words.filter(Boolean).map((w) => w.toLowerCase());
        expect(shown).toEqual(all.filter((n) => real.every((w) => n.toLowerCase().includes(w))));
        if (shown.length && shown.length < all.length) narrowed++;
      }),
      { examples: [[NAMES, ['pid', 'k'], ' '], [NAMES, ['FIELD'], ' '], [NAMES, [], ' ']] },
    );
    expect(narrowed).toBeGreaterThan(0);
  });
});
