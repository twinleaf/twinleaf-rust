import { describe, expect, it } from 'vitest';
import { controlOf, mergeRows, writersOf } from './properties.js';
import { KEYMAP, actionFor, sheet } from './keymap.js';
import { lanesOf } from './plot.js';
import { Views } from './views.js';
import fc from 'fast-check';
import { BASE_UNITS, DIGITS, axisFormat, fixedUnit, formOf, fromUnit, inUnit, numberText, rangeText, si, siRange, unitChoices, unitOf, bytesEstimate, bytesText, spanText } from './units.js';

const rpc = (name, type, access, channel = 'setting') => ({ name, type, access, channel, readable: access.includes('read'), persistent: false });

describe('the properties table', () => {
  it('merges devices by name, groups by channel, and marks a different declaration', () => {
    const rows = mergeRows([
      { route: '/0', table: { rpcs: [rpc('reset', null, [], 'action'), rpc('gain', 'f32', ['read', 'write'])] } },
      { route: '/1', table: { rpcs: [rpc('gain', 'u8', ['read', 'write']), rpc('temp', 'f32', ['read'], 'report')] } },
    ]);
    expect(rows.map((r) => [r.group, r.name])).toEqual([['Settings', 'gain'], ['Reports', 'temp'], ['Actions', 'reset']]);
    expect(rows[0].differs).toBe(true);
    expect(rows[0].entries.size).toBe(2);
  });

  it('infers a control and the devices a write reaches', () => {
    expect(controlOf(rpc('on', 'bool', ['read', 'write']))).toBe('toggle');
    expect(controlOf(rpc('t', 'f32', ['read']))).toBe('display');
    expect(controlOf(rpc('go', null, [], 'action'))).toBe('action');
    const devices = [{ route: '/0', table: { rpcs: [rpc('g', 'f32', ['read', 'write'])] } }, { route: '/1', table: { rpcs: [rpc('g', 'f32', ['read'])] } }];
    const [row] = mergeRows(devices);
    expect(writersOf(row, devices).map((d) => d.route)).toEqual(['/0']);
  });
});

describe('the keymap', () => {
  it('names each key’s action where focus is, and none with Ctrl', () => {
    expect(actionFor('plot', { key: 'a' })).toBe('addSignals');
    expect(actionFor('page', { key: '3' })).toBe('goToPlot');
    expect(actionFor('list', { key: 'Enter', shiftKey: true })).toBe('newPlot');
    expect(actionFor('list', { key: 'Enter', shiftKey: false })).toBe('addToPlots');
    expect(actionFor('list', { key: 'Backspace' })).toBe('removeFromPlots');
    expect(actionFor('list', { key: 'a', ctrlKey: true })).toBe('pickAll');
    expect(actionFor('plot', { key: 'a', ctrlKey: true })).toBeNull();
  });

  it('lists every action once per place', () => {
    for (const place of KEYMAP) {
      const actions = place.entries.map((e) => e.action).filter(Boolean);
      expect(new Set(actions).size, place.place).toBe(actions.length);
    }
  });

  it('sheets the places of one title as one section, a row per keys', () => {
    const lists = sheet().find((s) => s.title === 'Lists');
    expect(lists.rows.find((r) => r.id === 'Delete ').what).toBe('Remove signal, Remove from the selected plots, close tab');
    expect(lists.rows.filter((r) => r.what === 'Move')).toHaveLength(1);
    expect(sheet().filter((s) => s.title === 'Lists')).toHaveLength(1);
  });
});

describe('a plot', () => {
  it('has a lane per signal stacked, and one otherwise', () => {
    expect(lanesOf('stacked', 3)).toBe(3);
    expect(lanesOf('stacked', 0)).toBe(1);
    expect(lanesOf('independent', 3)).toBe(1);
    expect(lanesOf('overlay', 3)).toBe(1);
  });
});

describe('the views', () => {
  const hub = { now: () => 100 };

  it('go with the plots that leave every tab, so none is paused for them', () => {
    const views = new Views(hub, () => {});
    views.of('a').pause(0);
    views.of('b');
    expect(views.anyPaused(() => true)).toBe(true);
    views.prune(new Set(['b']));
    expect(views.get('a')).toBeUndefined();
    expect(views.anyPaused(() => true)).toBe(false);
  });

  it('a paused spectrum reads no history', () => {
    const views = new Views(hub, () => {});
    views.of('s').pause(0);
    expect(views.anyPaused((id) => id !== 's')).toBe(false);
  });
});

describe('a range of values', () => {
  it("shares the larger end's prefix", () => {
    expect(siRange(-2.438, 0.4413, 'V')).toBe('-2.438 … 0.4413 V');
    expect(siRange(-0.0012, 0.0031, 'V')).toBe('-1.200 … 3.100 mV');
    expect(siRange(-1, 2, 'arb')).toBe('-1.000 … 2.000');
  });
});

describe('a measured value', () => {
  const PREFIX = { f: -15, p: -12, n: -9, µ: -6, m: -3, k: 3, M: 6, G: 9 };
  /** Units in their base form, which the page wrote alike before it read a unit's prefix. */
  const BASE = ['V', 'Hz', 'V/√Hz', 'arb', ''];
  const UNITS = [...BASE, 'nT', 'mV', 'kHz', 'pT/√Hz', 'psi'];
  /** Units the page does not know: each is written as reported, never rescaled or prefixed. */
  const UNKNOWN = ['psi', 'ppm', 'min', 'mol', 'm', 'deg', 'arb', '%', '°C', 'm/s^2', 'second', 'u', 'Gauss', 'G', 'Gs', 'GV', 'V nT'];
  /** Every known unit as a device may report it: each base unit with each prefix the page writes but G, or none, alone and per √Hz. */
  const KNOWN = BASE_UNITS.flatMap((b) => ['f', 'p', 'n', 'µ', 'm', '', 'k', 'M'].flatMap((p) => [`${p}${b}`, `${p}${b}/√Hz`]));
  /**
   * The value text `s` writes of a value in `unit`, read back in `unit`: its
   * prefix and unit taken off, and for a known unit its prefix put back;
   * and its significant figures, the trailing zeros of a whole number among them.
   */
  function read(s, unit) {
    let t = s, scale = 0;
    const u = unitOf(unit), written = u ? `${u.base}${u.per}` : unit;
    if (written && written !== 'arb') {
      expect(t.endsWith(` ${written}`) || t.endsWith(written), `${s} ends in ${written}`).toBe(true);
      t = t.slice(0, t.lastIndexOf(written)).trimEnd();
    }
    if (u) {
      const last = t.at(-1);
      if (last in PREFIX) (scale = PREFIX[last]), (t = t.slice(0, -1).trimEnd());
      scale -= u.e;
    }
    expect(t, s).toMatch(/^-?\d+(\.\d+)?(e[+-]\d+)?$/);
    const mantissa = t.replace(/^-/, '').replace(/e.*$/, '');
    const digits = mantissa.replace('.', '').replace(/^0+/, '');
    return { v: Number(t) * 10 ** scale, figures: digits.length, nonzero: digits.replace(/0+$/, '').length };
  }
  // Values of every size, both signs and zero, as an axis meets them: none so small that plain numbers would need over 100 decimals.
  const value = fc
    .oneof(fc.double({ min: -1e12, max: 1e12, noNaN: true, noDefaultInfinity: true }), fc.double({ min: -1e-3, max: 1e-3, noNaN: true }), fc.constantFrom(0, 1, -1, 999.96, 0.0995, 1e-9, 567.8, 1e5))
    .map((v) => (Math.abs(v) < 1e-30 ? 0 : v));
  const unit = fc.constantFrom(...UNITS);
  // Every unit of KNOWN and UNKNOWN as the page reads it, by name.
  it('knows a unit by its base unit and one prefix, and no other', () => {
    for (const u of KNOWN) expect(unitOf(u), u).not.toBe(null);
    for (const u of UNKNOWN) expect(unitOf(u), u).toBe(null);
  });

  it('on Auto writes as the page writes it with no settings: an SI prefix to four figures, or as toPrecision or toExponential', () => {
    // The page's own formats, written out here as they were before an axis had settings.
    const PREFIXES = { '-15': 'f', '-12': 'p', '-9': 'n', '-6': 'µ', '-3': 'm', 0: '', 3: 'k', 6: 'M', 9: 'G' };
    const si = (v, unit) => {
      if (!isFinite(v)) return '—';
      if (!unit || unit === 'arb') return v.toPrecision(4);
      if (v === 0) return `0 ${unit}`;
      const e = Math.max(-15, Math.min(9, Math.floor(Math.log10(Math.abs(v)) / 3) * 3));
      return `${(v / 10 ** e).toPrecision(4)} ${PREFIXES[e]}${unit}`;
    };
    const range = (lo, hi, unit) => {
      const big = Math.max(Math.abs(lo), Math.abs(hi));
      if (!unit || unit === 'arb' || big === 0) return `${lo.toPrecision(4)} … ${hi.toPrecision(4)}${unit && unit !== 'arb' ? ` ${unit}` : ''}`;
      const e = Math.max(-15, Math.min(9, Math.floor(Math.log10(big) / 3) * 3));
      return `${(lo / 10 ** e).toPrecision(4)} … ${(hi / 10 ** e).toPrecision(4)} ${PREFIXES[e]}${unit}`;
    };
    const auto = axisFormat(undefined);
    fc.assert(
      fc.property(value, value, fc.constantFrom(...BASE), fc.constantFrom(3, 4, 5), (v, w, u, d) => {
        expect(formOf({ digits: d, notation: 'si' }, auto)).toEqual({ digits: d, notation: 'si' });
        expect(numberText(v, formOf({ digits: 4, notation: 'si' }, auto), u)).toBe(si(v, u));
        expect(numberText(v, { digits: d, notation: 'fig' })).toBe(v.toPrecision(d));
        expect(numberText(v, { digits: d, notation: 'fig' }, 'Hz')).toBe(`${v.toPrecision(d)} Hz`);
        expect(numberText(v, { digits: d, notation: 'exp' })).toBe(v.toExponential(d - 1));
        expect(rangeText(Math.min(v, w), Math.max(v, w), { digits: 4, notation: 'si' }, u)).toBe(range(Math.min(v, w), Math.max(v, w), u));
      }),
    );
  });

  it('to fixed digits reads back as the value within its last figure, in as many figures, in each notation', () => {
    const seen = new Set();
    fc.assert(
      fc.property(value, unit, fc.constantFrom(...DIGITS), fc.constantFrom('si', 'fig', 'plain', 'exp'), (v, u, d, notation) => {
        const text = numberText(v, { digits: d, notation }, u), { v: back, figures, nonzero } = read(text, u);
        // Half the last figure's place, as the figures round it, and a hair for the float of reading it back.
        const place = v === 0 ? 0 : Math.floor(Math.log10(Number(Math.abs(v).toPrecision(d))));
        expect(Math.abs(back - v), `${v} as ${text}`).toBeLessThanOrEqual(0.5 * 10 ** (place - d + 1) * (1 + 1e-9) + Math.abs(v) * 1e-12);
        // A whole number may end in zeros past its figures, as 570; else it has exactly as many.
        const whole = !/[.e]/.test(text.replace(/\s.*$/, ''));
        if (whole && back !== 0) expect(nonzero <= d && figures >= d, `${v} as ${text}: ${d} figures`).toBe(true);
        else if (back !== 0) expect(figures, `${v} as ${text}`).toBe(d);
        // An exponent only where asked, or where toPrecision writes one, or for an SI value past the prefixes.
        const e = unitOf(u)?.e, base = e !== undefined && (e >= 0 ? Math.abs(v) * 10 ** e : Math.abs(v) / 10 ** -e);
        if (notation === 'plain' || (notation === 'si' && base < 1e12 && base >= 1e-15)) expect(text, `${v} in ${notation}`).not.toMatch(/e/);
        if (notation === 'exp') expect(text).toMatch(/e[+-]\d+/);
        seen.add(`${notation}:${whole}`);
      }),
      { examples: [[567.8, 'V', 2, 'si'], [123456, '', 2, 'plain'], [0.0995, 'V', 2, 'plain'], [999.96, 'V', 3, 'si'], [0, 'V', 3, 'exp'], [0.005, 'nT', 4, 'si'], [2000, 'nT', 3, 'plain'], [0.5, 'psi', 3, 'si']] },
    );
    for (const k of ['si:true', 'plain:true', 'plain:false', 'exp:false', 'fig:false']) expect(seen.has(k), k).toBe(true);
  });

  it('in plain numbers shares the decimals of the axis’s largest', () => {
    expect([0, 0.5, 1].map((v) => numberText(v, { digits: 2, notation: 'plain' }, '', 1))).toEqual(['0.0', '0.5', '1.0']);
    expect([-200, 0, 200, 400].map((v) => numberText(v, { digits: 3, notation: 'plain' }, '', 400))).toEqual(['-200', '0', '200', '400']);
    expect(numberText(0.0001978, { digits: 4, notation: 'plain' }, 'V')).toBe('0.0001978 V');
    expect(numberText(0.5678, { digits: 2, notation: 'si' }, 'V')).toBe('570 mV');
  });

  it('in a prefixed unit takes the value to its base unit before it picks a prefix', () => {
    expect(si(0.005, 'nT')).toBe('5.000 pT');
    expect(si(2000, 'nT')).toBe('2.000 µT');
    expect(si(0, 'nT')).toBe('0 nT');
    expect(si(1500, 'mHz')).toBe('1.500 Hz');
    expect(si(0.25, 'uV')).toBe('250.0 nV');
    expect(numberText(0.005, { digits: 4, notation: 'si' }, 'nT/√Hz')).toBe('5.000 pT/√Hz');
    expect(siRange(-0.002, 0.005, 'nT')).toBe('-2.000 … 5.000 pT');
    expect([unitOf('nT'), unitOf('nT/√Hz'), unitOf('uV'), unitOf('kΩ'), unitOf('Hz')]).toEqual([
      { e: -9, base: 'T', per: '' },
      { e: -9, base: 'T', per: '/√Hz' },
      { e: -6, base: 'V', per: '' },
      { e: 3, base: 'Ω', per: '' },
      { e: 0, base: 'Hz', per: '' },
    ]);
  });

  it('in a known unit with an SI prefix reads back as the value in its reported unit, its base unit written with the prefix its size takes', () => {
    const seen = new Set();
    fc.assert(
      fc.property(value, fc.constantFrom(...KNOWN), fc.constantFrom(...DIGITS), (v, u, d) => {
        const text = numberText(v, { digits: d, notation: 'si' }, u), { v: back } = read(text, u), { e, base, per } = unitOf(u);
        const place = v === 0 ? 0 : Math.floor(Math.log10(Number(Math.abs(v).toPrecision(d))));
        expect(Math.abs(back - v), `${v} ${u} as ${text}`).toBeLessThanOrEqual(0.5 * 10 ** (place - d + 1) * (1 + 1e-9) + Math.abs(v) * 1e-12);
        expect(text.endsWith(`${base}${per}`), `${text} in ${base}${per}`).toBe(true);
        // The prefix is the one the value's size in the base unit takes, within the prefixes the page writes.
        if (v !== 0) {
          const prefix = text.slice(text.lastIndexOf(' ') + 1, -`${base}${per}`.length), m = e >= 0 ? Math.abs(v) * 10 ** e : Math.abs(v) / 10 ** -e;
          const want = Math.max(-15, Math.min(9, Math.floor(Math.log10(m) / 3) * 3));
          expect(prefix === '' ? 0 : PREFIX[prefix], `${v} ${u} as ${text}`).toBe(want);
          seen.add(base);
          if (prefix !== u.slice(0, -`${base}${per}`.length)) seen.add('re-prefixed');
        }
      }),
      { examples: [...BASE_UNITS.map((b) => [0.005, `n${b}`, 4]), [2000, 'nT/√Hz', 4]] },
    );
    for (const k of [...BASE_UNITS, 're-prefixed']) expect(seen.has(k), k).toBe(true);
  });

  it('in a unit the page does not know is written in it as reported, never rescaled or given a prefix', () => {
    fc.assert(
      fc.property(value, fc.constantFrom(...DIGITS), (v, d) => {
        for (const u of UNKNOWN) {
          const tail = u === 'arb' ? '' : ` ${u}`;
          expect(unitOf(u), u).toBe(null);
          expect(unitChoices(u), u).toEqual([]);
          expect(formOf({ digits: d, notation: 'si' }, { ...axisFormat(undefined), unit: -12 }, u), u).toEqual({ digits: d, notation: 'si' });
          expect(numberText(v, { digits: d, notation: 'si' }, u), u).toBe(`${v.toPrecision(d)}${tail}`);
          expect(numberText(v, { digits: d, notation: 'exp' }, u), u).toBe(`${v.toExponential(d - 1)}${tail}`);
          expect(rangeText(-Math.abs(v), Math.abs(v), { digits: d, notation: 'si' }, u), u).toBe(`${(-Math.abs(v)).toPrecision(d)} … ${Math.abs(v).toPrecision(d)}${tail}`);
        }
      }),
      { examples: [[0.005, 4], [2000, 3], [0, 2]] },
    );
  });

  it('in a fixed unit writes that unit at every value, the value taken to it', () => {
    const seen = new Set();
    const choice = fc.constantFrom(...KNOWN).chain((u) => fc.constantFrom(...unitChoices(u)).map((c) => [u, c]));
    fc.assert(
      fc.property(value, choice, fc.constantFrom(...DIGITS), fc.constantFrom('si', 'fig', 'plain', 'exp'), (v, [u, c], d, notation) => {
        const form = formOf({ digits: d, notation }, { ...axisFormat(undefined), unit: c.e }, u), text = numberText(v, form, u);
        expect(form.fixed?.text, `${u} fixed at ${c.text}`).toBe(c.text);
        expect(text.endsWith(` ${c.text}`), `${v} ${u} as ${text} in ${c.text}`).toBe(true);
        const figures = text.slice(0, -c.text.length - 1);
        expect(figures, `${text}: a number with no prefix`).toMatch(/^-?\d+(\.\d+)?(e[+-]\d+)?$/);
        const back = Number(figures) * 10 ** (c.e - unitOf(u).e), place = v === 0 ? 0 : Math.floor(Math.log10(Number(Math.abs(v).toPrecision(d))));
        expect(Math.abs(back - v), `${v} ${u} as ${text}`).toBeLessThanOrEqual(0.5 * 10 ** (place - d + 1) * (1 + 1e-9) + Math.abs(v) * 1e-12);
        // Written where no unit is, it is the value taken to the unit fixed alone; 'si' with no unit writes figures as 'fig' does.
        expect(numberText(v, form), `${v} ${u} in ${c.text}, with no unit written`).toBe(notation === 'si' ? numberText(inUnit(v, form.fixed), { digits: d, notation: 'fig' }) : figures);
        expect(rangeText(v, v, form, u).endsWith(` ${c.text}`), `${v} as a range in ${c.text}`).toBe(true);
        seen.add(`${unitOf(u).base}:${notation}`);
      }),
      { examples: BASE_UNITS.flatMap((b) => ['si', 'fig', 'plain', 'exp'].map((n) => [0.005, [`n${b}`, unitChoices(b)[1]], 4, n])) },
    );
    for (const b of BASE_UNITS) for (const n of ['si', 'fig', 'plain', 'exp']) expect(seen.has(`${b}:${n}`), `${b} in ${n}`).toBe(true);
    // A value typed in the unit fixed is the value in the reported unit that it writes as.
    expect(fixedUnit({ unit: -12 }, 'nT')).toEqual({ shift: 3, text: 'pT' });
    expect(inUnit(0.005, fixedUnit({ unit: -12 }, 'nT'))).toBe(5);
    expect(fromUnit(5, fixedUnit({ unit: -12 }, 'nT'))).toBe(0.005);
    expect(numberText(0.005, formOf({ digits: 4, notation: 'si' }, { unit: -12 }, 'nT'), 'nT')).toBe('5.000 pT');
    expect(numberText(2000, formOf({ digits: 4, notation: 'si' }, { unit: -9 }, 'nT'), 'nT')).toBe('2000 nT');
  });

  it('offers the prefixes from f to the base unit, and k and M on Hz and Ω', () => {
    expect(unitChoices('nT').map((c) => c.text)).toEqual(['fT', 'pT', 'nT', 'µT', 'mT', 'T']);
    expect(unitChoices('V/√Hz').map((c) => c.text)).toEqual(['fV/√Hz', 'pV/√Hz', 'nV/√Hz', 'µV/√Hz', 'mV/√Hz', 'V/√Hz']);
    expect(unitChoices('kHz').map((c) => c.text)).toEqual(['fHz', 'pHz', 'nHz', 'µHz', 'mHz', 'Hz', 'kHz', 'MHz']);
    expect(unitChoices('Ω').map((c) => c.text).slice(-2)).toEqual(['kΩ', 'MΩ']);
    // A prefix the axis's units do not offer fixes nothing.
    expect(fixedUnit({ unit: 3 }, 'nT')).toBeUndefined();
    expect(fixedUnit({ unit: null }, 'nT')).toBeUndefined();
  });

  it('takes an axis’s settings as they are kept, any it does not know at Auto', () => {
    expect(axisFormat(undefined)).toEqual({ label: 'off', digits: null, notation: 'auto', unit: null });
    expect(axisFormat({ label: 'units', digits: 3, notation: 'exp', unit: -12 })).toEqual({ label: 'units', digits: 3, notation: 'exp', unit: -12 });
    expect(axisFormat({ label: 'big', digits: 9, notation: 'si', unit: -2 })).toEqual({ label: 'off', digits: null, notation: 'auto', unit: null });
    expect(formOf({ digits: 4, notation: 'si' }, { digits: 2, notation: 'auto' })).toEqual({ digits: 2, notation: 'si' });
    expect(formOf({ digits: 4, notation: 'si' }, { digits: null, notation: 'plain' })).toEqual({ digits: 4, notation: 'plain' });
  });
});

describe('sizes and spans', () => {
  it('reads as people write them', () => {
    expect(bytesText(740e3)).toBe('740 kB');
    expect(bytesText(12.4e6)).toBe('12.4 MB');
    expect(bytesText(1.5e9)).toBe('1.50 GB');
    expect([bytesEstimate(126.4e6), bytesEstimate(2.34e9), bytesEstimate(0)]).toEqual(['~130 MB', '~2.3 GB', '~0 kB']);
    expect([spanText(45), spanText(720), spanText(3.5 * 3600), spanText(2 * 86400), spanText(Infinity)]).toEqual(['45 s', '12 min', '3.5 h', '2 days', 'forever']);
  });
});
