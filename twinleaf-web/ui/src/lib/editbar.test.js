// The edit bar's model (round 2, Selection #8 to #11) over random rows of
// every kind, each declared alike or not on each device, and random values:
// one row edits as its control says; rows of several kinds edit nothing
// and count each kind; several switches read On, Off or Mixed and Enter
// turns them all on, or all off when every one is on; several values of
// one type share a value only when every device of every row shows it;
// several actions keep their order; rows that only show have no bar.
import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { checkValue, editBar, typeProblem } from './editbar.js';
import { mergeRows } from './properties.js';
import { entry, message } from '../../tests/fixtures/hub.js';

const DECLS = {
  toggle: entry('bool', { access: ['read', 'write'], channel: 'setting' }),
  u8: entry('u8', { access: ['read', 'write'], channel: 'setting' }),
  f32: entry('f32', { access: ['read', 'write'], channel: null }),
  action: entry(null, { access: [], channel: 'action' }),
  display: entry('f32', { access: ['read'], channel: 'report' }),
};
const KIND = { toggle: 'toggle', u8: 'value:u8', f32: 'value:f32', action: 'action', display: 'display' };
const ROUTES = ['/0', '/1', '/2'];

/** A row declared as `decl` gives, on some devices, with a value on each. */
const spec = (decl) => fc.record({ decl, on: fc.subarray(ROUTES, { minLength: 1 }), values: fc.array(fc.oneof(fc.boolean(), fc.constantFrom(1, 2)), { minLength: 3, maxLength: 3 }) });
/** Rows of any kinds, or as often several of one kind. */
const pick = fc.oneof(
  fc.array(spec(fc.constantFrom(...Object.keys(DECLS))), { minLength: 1, maxLength: 5 }),
  fc.constantFrom(...Object.keys(DECLS)).chain((decl) => fc.array(spec(fc.constant(decl)), { minLength: 2, maxLength: 5 })),
);

describe('the edit bar', () => {
  it('edits the rows it is given as one kind, or says they are of several', () => {
    const seen = { none: 0, one: 0, kinds: 0, switches: 0, mixed: 0, on: 0, values: 0, shared: 0, actions: 0 };
    fc.assert(
      fc.property(pick, (specs) => {
        const devices = ROUTES.map((route) => ({
          route,
          table: { rpcs: specs.flatMap((s, i) => (s.on.includes(route) ? [{ ...DECLS[s.decl], name: `p${i}` }] : [])) },
          values: new Map(specs.map((s, i) => [`p${i}`, s.decl === 'toggle' ? !!s.values[ROUTES.indexOf(route)] : s.values[ROUTES.indexOf(route)]])),
        }));
        const byName = new Map(mergeRows(devices).map((r) => [r.name, r]));
        const rows = specs.map((_, i) => byName.get(`p${i}`));
        const bar = editBar(rows, devices, (d, name) => d.values.get(name));
        const kinds = specs.map((s) => KIND[s.decl]);
        const shown = rows.flatMap((r, i) => devices.filter((d) => specs[i].on.includes(d.route)).map((d) => d.values.get(r.name)));
        if (kinds.every((k) => k === 'display')) {
          seen.none++;
          expect(bar).toBe(null);
          return;
        }
        expect(bar.rows).toBe(rows);
        if (rows.length === 1) {
          seen.one++;
          expect(bar.kind).toBe('one');
          return;
        }
        const distinct = [...new Set(kinds)];
        if (distinct.length > 1) {
          seen.kinds++;
          expect(bar.kind).toBe('kinds');
          expect(bar.hint).toBe('Pick one kind to edit');
          // The label is short whatever is picked; its tooltip counts each kind once, in the order first picked, the counts adding up to the rows.
          expect(bar.label).toBe(`${rows.length} rows of ${distinct.length} kinds`);
          const parts = bar.detail.split(', ');
          expect(parts.length).toBe(distinct.length);
          expect(parts.reduce((n, p) => n + parseInt(p, 10), 0)).toBe(rows.length);
          return;
        }
        if (distinct[0] === 'toggle') {
          seen.switches++;
          const state = shown.every((v) => v === true) ? 'On' : shown.every((v) => v === false) ? 'Off' : 'Mixed';
          if (state === 'Mixed') seen.mixed++;
          if (state === 'On') seen.on++;
          expect(bar).toMatchObject({ kind: 'switches', state, label: `${rows.length} switches · ${state}`, enter: state === 'On' ? 'false' : 'true' });
        } else if (distinct[0] === 'action') {
          seen.actions++;
          expect(bar).toMatchObject({ kind: 'actions', label: `${rows.length} actions` });
        } else {
          seen.values++;
          const same = shown.every((v) => v === shown[0]);
          if (same) seen.shared++;
          expect(bar).toMatchObject({ kind: 'values', label: `${rows.length} values (${distinct[0].slice(6)})`, shared: same ? shown[0] : undefined });
        }
        expect(bar.reach.map((d) => d.route)).toEqual(ROUTES.filter((r) => specs.some((s) => s.decl !== 'display' && s.on.includes(r))));
      }),
      {
        // Switches all on and mixed, and values all alike, whatever the seed.
        examples: [
          [[{ decl: 'toggle', on: ['/0', '/1'], values: [true, true, true] }, { decl: 'toggle', on: ['/2'], values: [true, true, true] }]],
          [[{ decl: 'toggle', on: ROUTES, values: [true, false, true] }, { decl: 'toggle', on: ['/1'], values: [false, false, false] }]],
          [[{ decl: 'u8', on: ROUTES, values: [2, 2, 2] }, { decl: 'u8', on: ['/0'], values: [2, 1, 1] }]],
        ],
      },
    );
    for (const [what, n] of Object.entries(seen)) expect(n, what).toBeGreaterThan(0);
  });
});

describe('a value typed for an entry', () => {
  const INTS = message('table').rpcs.filter((r) => r.class === 'int');
  it('takes a whole number in its entry’s range and no other, saying why', () => {
    fc.assert(
      fc.property(fc.constantFrom(...INTS), fc.bigInt({ min: -(1n << 70n), max: 1n << 70n }), fc.constantFrom('', '+', '.5', 'x', ' '), (rpc, v, junk) => {
        const text = String(v), within = v >= BigInt(rpc.min) && v <= BigInt(rpc.max);
        expect(typeProblem(rpc, text) === null, `${text} as ${rpc.type}`).toBe(within);
        if (!within) expect(typeProblem(rpc, text)).toContain('out of range');
        if (junk && junk !== '+') expect(typeProblem(rpc, text + junk)).toContain('Not a whole number');
      }),
      { examples: [[entry('u8'), 255n, ''], [entry('u8'), 256n, ''], [entry('u8'), -1n, ''], [entry('u32'), (1n << 32n) - 1n, 'x']] },
    );
  });
  it('takes no minus sign for an unsigned type, not even of 0', () => {
    for (const rpc of INTS) {
      expect(typeProblem(rpc, '-0'), rpc.type).toContain('out of range');
      expect(typeProblem(rpc, '0'), rpc.type).toBe(null);
    }
  });
  it('takes a number for a float, true or false for a bool, text within its length, and hex in pairs', () => {
    const [f32, f64, bool, bytes12] = ['f32', 'f64', 'bool', 'bytes<12>'].map((type) => entry(type));
    const string3 = entry('string', { type: 'string<3>', length: 3 }), string1 = entry('string', { type: 'string<1>', length: 1 }), bytes = entry('bytes<12>', { type: 'bytes', length: null });
    for (const [rpc, text, ok] of [
      [f32, '1.5', true], [f32, '-2e-3', true], [f64, '.5', true], [f64, 'inf', true], [f32, 'NaN', true], [f32, '1,5', false], [f32, '', false], [f64, '0x10', false],
      [bool, 'true', true], [bool, 'false', true], [bool, 'True', false], [bool, '1', false],
      [entry('string'), 'anything at all', true], [string3, 'abc', true], [string3, 'abcd', false], [string3, 'é', true], [string1, 'é', false],
      [bytes, '0aFF', true], [bytes, '0a1', false], [bytes, 'zz', false], [bytes12, '0a'.repeat(12), true], [bytes12, '0a', false],
      [entry('capture'), 'whatever', true], [entry(null), 'whatever', true],
    ]) expect(typeProblem(rpc, text) === null, `${text} as ${rpc.type}`).toBe(ok);
  });
  it('checks each device’s entry by its own type, on the devices a write reaches', () => {
    const devices = ['/0', '/1'].map((route) => ({ route, table: { rpcs: [entry(route === '/0' ? 'u16' : 'u8', { name: 'gain', access: ['read', 'write'], channel: 'setting' })] } }));
    const rows = mergeRows(devices);
    expect(checkValue(rows, devices, '300')).toContain('u8 takes 0 to 255');
    expect(checkValue(rows, [devices[0]], '300')).toBe(null);
    expect(checkValue(rows, devices, '200')).toBe(null);
  });
});
