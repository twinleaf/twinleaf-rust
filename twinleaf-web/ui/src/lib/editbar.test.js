// The edit bar's model (round 2, Selection #8 to #11) over random rows of
// every kind, each declared alike or not on each device, and random values:
// one row edits as its control says; rows of several kinds edit nothing
// and count each kind; several switches read On, Off or Mixed and Enter
// turns them all on, or all off when every one is on; several values of
// one type share a value only when every device of every row shows it;
// several actions keep their order; rows that only show have no bar.
import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { editBar } from './editbar.js';
import { mergeRows } from './properties.js';

const DECLS = {
  toggle: { type: 'bool', access: ['read', 'write'], channel: 'setting' },
  u8: { type: 'u8', access: ['read', 'write'], channel: 'setting' },
  f32: { type: 'f32', access: ['read', 'write'], channel: null },
  action: { type: null, access: [], channel: 'action' },
  display: { type: 'f32', access: ['read'], channel: 'report' },
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
          table: { rpcs: specs.flatMap((s, i) => (s.on.includes(route) ? [{ name: `p${i}`, ...DECLS[s.decl] }] : [])) },
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
