// The command palette's properties (palette.js): the matcher over random
// names and fragments, its ranking, and the lock as a model of what Space,
// Tab, Backspace, Enter and Escape do in each state, for each kind of
// property, an argument typed or not, a read shown or not.
import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { actsOf, argProblem, findProperties, fragments, match, searching, step } from './palette.js';
import { mergeRows } from './properties.js';

const strip = (s) => s.toLowerCase().replace(/[._]/g, '');
const name = fc.stringMatching(/^[a-c._]{1,10}$/);

describe('the palette’s matcher', () => {
  it('matches when every fragment is in the name, in order, dots and underscores skipped, whatever the case', () => {
    let hits = 0, misses = 0;
    fc.assert(
      fc.property(name, fc.array(fc.stringMatching(/^[a-cA-C]{1,3}$/), { maxLength: 3 }), (n, frags) => {
        const m = match(n, frags.map((f) => f.toLowerCase()));
        // The model: some placing of each fragment, one after the other, in the name with its separators taken out, each fragment starting on a letter.
        const s = n.toLowerCase();
        const places = (i, k) => {
          if (k === frags.length) return true;
          for (let at = i; at < s.length; at++) {
            if (/[._]/.test(s[at])) continue;
            for (let end = at + 1; end <= s.length; end++) if (strip(s.slice(at, end)) === frags[k].toLowerCase() && places(end, k + 1)) return true;
          }
          return false;
        };
        expect(!!m, `${frags} in ${n}`).toBe(places(0, 0));
        if (!m) return void misses++;
        hits++;
        // Each span holds its fragment, the spans in order.
        m.spans.forEach(([a, b], k) => expect(strip(n.slice(a, b))).toBe(frags[k].toLowerCase()));
        m.spans.slice(1).forEach(([a], k) => expect(a).toBeGreaterThanOrEqual(m.spans[k][1]));
      }),
      { examples: [['data.rate', ['datarate']], ['data_rate', ['data', 'rate']], ['Data.Rate', ['rate', 'data']]] },
    );
    expect(hits).toBeGreaterThan(0);
    expect(misses).toBeGreaterThan(0);
  });

  it('splits what is typed at spaces, dots and underscores', () => {
    expect(fragments(' Data.rate_x  y ')).toEqual(['data', 'rate', 'x', 'y']);
    expect(match('data.rate', fragments('dat rat'))).toBeTruthy();
    expect(match('data.rate', fragments('rate data'))).toBe(null);
    expect(match('gain', fragments('xyz'))).toBe(null);
  });

  it('ranks starred first, then earlier, then tighter matches, and scores falling along the list', () => {
    fc.assert(
      fc.property(fc.uniqueArray(name, { minLength: 1, maxLength: 12 }), fc.stringMatching(/^[a-c]{0,2}$/), fc.func(fc.boolean()), (names, query, star) => {
        const rows = names.map((n) => ({ name: n }));
        const starred = new Set(names.filter((n) => star(n)));
        const { shown, total } = findProperties(rows, query, starred, 5);
        const frags = fragments(query);
        expect(total).toBe(names.filter((n) => match(n, frags)).length);
        expect(shown.length).toBe(Math.min(5, total));
        for (let i = 1; i < shown.length; i++) {
          const a = shown[i - 1], b = shown[i], sa = starred.has(a.row.name), sb = starred.has(b.row.name);
          expect(sa || !sb, 'starred first').toBe(true);
          if (sa === sb) expect(a.match.start < b.match.start || (a.match.start === b.match.start && a.match.gaps <= b.match.gaps), 'earlier, then tighter').toBe(true);
          expect(b.score).toBeLessThan(a.score);
        }
        // What is left out ranks no better than what shows.
        const out = names.filter((n) => match(n, frags) && !shown.some((s) => s.row.name === n));
        if (shown.length && out.length) expect(out.some((n) => starred.has(n)) && !starred.has(shown.at(-1).row.name)).toBe(false);
      }),
    );
  });
});

// One device with a property of each kind.
const TABLE = [
  { name: 'gain', type: 'u8', access: ['read', 'write'], channel: 'setting', readable: true },
  { name: 'enable', type: 'bool', access: ['read', 'write'], channel: 'setting', readable: true },
  { name: 'temp', type: 'f32', access: ['read'], channel: 'report', readable: true },
  { name: 'secret', type: 'f32', access: ['write'], channel: 'setting', readable: false },
  { name: 'reboot', type: null, access: [], channel: 'action', readable: false },
];
const DEVICES = [{ route: '/0', table: { rpcs: TABLE } }];
const ROWS = new Map(mergeRows(DEVICES).map((r) => [r.name, r]));
const KEYS = [' ', 'Tab', 'Backspace', 'Enter', 'Escape', 'a'];

/** What the model says key `key` does in state `s` with row `row`: the expected {mode, arg, read, keep, act, close}. */
function expected(s, key, row, pick) {
  const acts = actsOf(row);
  if (s.mode === 'search') {
    if (key === ' ' || key === 'Tab') return { mode: 'locked', arg: '', read: false, keep: true, act: null, close: false };
    if (key === 'Enter') {
      if (acts.action || acts.writable) return { mode: 'locked', arg: '', read: false, keep: true, act: null, close: false };
      return { mode: 'locked', arg: '', read: true, keep: true, act: 'read', close: false };
    }
    return { mode: 'search', keep: false, act: null, close: false };
  }
  const same = { mode: 'locked', arg: s.arg, read: s.read, keep: true, act: null, close: false };
  const pass = { ...same, keep: false };
  switch (key) {
    case ' ': return s.arg === '' ? same : pass;
    case 'Tab': return same;
    case 'Backspace': return s.arg === '' ? { mode: 'search', keep: true, act: null, close: false } : pass;
    case 'Escape':
      if (s.arg !== '') return { ...same, arg: '', read: false };
      return s.read ? { ...pass, close: true } : { mode: 'search', keep: true, act: null, close: false };
    case 'Enter': {
      if (s.read && s.arg === '') return { ...pass, close: true };
      if (argProblem(row, DEVICES, s.arg)) return same;
      if (acts.action) return { ...same, act: 'run', close: true };
      if (s.arg !== '') return { ...same, act: 'write', close: true };
      if (acts.toggle && pick !== null) return { ...same, act: 'write', close: true };
      if (acts.readable) return { ...same, read: true, act: 'read' };
      return same;
    }
    default: return pass;
  }
}

describe('the palette’s lock', () => {
  it('does what the model says for each key, state and kind of property, and writes or runs only on Enter on a locked one it takes', () => {
    const seen = new Set();
    fc.assert(
      fc.property(
        fc.constantFrom(...ROWS.keys()),
        fc.constantFrom('search', 'locked'),
        fc.constantFrom('', '7', '300', 'x', 'true'),
        fc.boolean(),
        fc.constantFrom(null, 'true', 'false'),
        fc.constantFrom(...KEYS),
        (n, mode, arg, read, pick, key) => {
          const row = ROWS.get(n);
          const s = mode === 'search' ? searching('ga') : { mode: 'locked', name: n, query: 'ga', arg, read };
          const r = step(s, key, { row, devices: DEVICES, pick });
          const want = expected(s, key, row, pick);
          expect({ mode: r.state.mode, keep: r.keep, act: r.act?.type ?? null, close: r.close }, `${key} on ${n} ${JSON.stringify(s)}`).toEqual({ mode: want.mode, keep: want.keep, act: want.act, close: want.close });
          if (want.mode === 'locked') expect([r.state.name, r.state.arg, r.state.read]).toEqual([n, want.arg, want.read]);
          if (r.state.mode === 'search') expect(r.state.query).toBe('ga');
          if (r.act) {
            expect(r.act.name).toBe(n);
            if (r.act.type === 'write') expect(r.act.text).toBe(arg !== '' ? arg : pick);
          }
          // A write or a run comes only from Enter on a locked property with an argument it takes.
          if (r.act && r.act.type !== 'read') {
            expect([key, mode]).toEqual(['Enter', 'locked']);
            expect(argProblem(row, DEVICES, arg)).toBe(null);
          }
          seen.add(`${mode} ${key} ${r.act?.type ?? '-'}`);
        },
      ),
      {
        numRuns: 2000,
        // Each case counted below, whatever the seed: (name, mode, arg, read, pick, key).
        examples: [
          ['gain', 'locked', '7', false, null, 'Enter'],
          ['reboot', 'locked', '', false, null, 'Enter'],
          ['gain', 'locked', '', false, null, 'Enter'],
          ['temp', 'search', '', false, null, 'Enter'],
          ['reboot', 'search', '', false, null, 'Enter'],
          ['gain', 'locked', '', false, null, 'Escape'],
        ],
      },
    );
    for (const s of ['locked Enter write', 'locked Enter run', 'locked Enter read', 'search Enter read', 'search Enter -', 'locked Escape -']) expect(seen.has(s), s).toBe(true);
  });

  it('writes nothing for an argument the property does not take, and says why', () => {
    const at = (name, arg) => ({ mode: 'locked', name, query: '', arg, read: false });
    for (const [n, arg, why] of [['gain', '300', 'out of range'], ['gain', 'x', 'Not a whole number'], ['enable', 'yes', 'true or false'], ['temp', '5', 'read-only'], ['reboot', 'now', 'takes no value']]) {
      const r = step(at(n, arg), 'Enter', { row: ROWS.get(n), devices: DEVICES });
      expect(r.act, `${arg} to ${n}`).toBe(null);
      expect(r.close).toBe(false);
      expect(argProblem(ROWS.get(n), DEVICES, arg)).toContain(why);
    }
    expect(step(at('gain', '200'), 'Enter', { row: ROWS.get('gain'), devices: DEVICES }).act).toEqual({ type: 'write', name: 'gain', text: '200' });
  });

  it('runs an action only from a lock: Enter on one not locked locks it', () => {
    const r = step(searching('reb'), 'Enter', { row: ROWS.get('reboot'), devices: DEVICES });
    expect([r.act, r.state.mode]).toEqual([null, 'locked']);
    expect(step(r.state, 'Enter', { row: ROWS.get('reboot'), devices: DEVICES }).act).toEqual({ type: 'run', name: 'reboot', text: '' });
  });
});
