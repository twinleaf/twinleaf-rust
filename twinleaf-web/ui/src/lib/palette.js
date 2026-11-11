// The device properties the command palette offers, and what its keys do
// to them.
//
// A property is found by typing part of its name: every fragment typed,
// split at spaces, dots and underscores, appears in the name in order,
// whatever the case, a dot or an underscore in the name skipped within a
// fragment, so "datarate" and "data rate" both find data.rate. The best
// SHOWN matches show: starred properties first, then the ones matched
// earlier in the name, then the tighter, then the shorter, then in the
// panels' order.
//
// Space or Tab on a property locks it: the field takes its argument. Enter
// with no argument reads a readable property, the value showing in place,
// after which Enter or Escape closes; Enter writes the argument to a
// writable one, once it passes the edit bar's check (editbar.js
// `checkValue`), and closes; and runs an action, which takes no argument,
// and closes. A switch with no argument offers On and Off, and Enter
// writes the one highlighted. Enter on a property not locked reads a
// readable one, locked, as a read-only one is, and locks any other, so an
// action never runs but from a lock. Space or Tab on a writable property
// locked with no argument turns it to taking one, the read shown gone, so
// a switch read offers On and Off again. Backspace on an empty argument,
// and Escape, unlock it; Escape with an argument typed clears it first.
// Nothing is written or run but by Enter on a locked property with a value
// it takes, or a switch's On or Off.

import { controlOf } from './properties.js';
import { checkValue } from './editbar.js';
import * as words from './words.js';

/** The most properties the palette shows at once. */
export const SHOWN = 30;

/** The fragments of `query`: its pieces between spaces, dots and underscores, lower case. */
export const fragments = (query) => query.toLowerCase().split(/[\s._]+/).filter(Boolean);

const SEPARATOR = /[._]/;

/** The first place from `from` on where fragment `f` matches lower-case name `s`, a dot or an underscore skipped within it, as [start, end), or null. */
function find(s, f, from) {
  for (let i = from; i < s.length; i++) {
    if (s[i] !== f[0]) continue;
    let k = 1, j = i + 1;
    while (k < f.length && j < s.length) {
      if (s[j] === f[k]) (k++, j++);
      else if (SEPARATOR.test(s[j])) j++;
      else break;
    }
    if (k === f.length) return [i, j];
  }
  return null;
}

/**
 * Where fragments `frags` match `name`, each after the one before, each as
 * early as it can: {start, gaps, spans}, `start` where the first begins,
 * `gaps` the characters between the first's start and the last's end that
 * no fragment matched, and `spans` each fragment's [start, end); or null
 * when they do not all match.
 */
export function match(name, frags) {
  const s = name.toLowerCase(), spans = [];
  let at = 0;
  for (const f of frags) {
    const hit = find(s, f, at);
    if (!hit) return null;
    spans.push(hit);
    at = hit[1];
  }
  const start = spans[0]?.[0] ?? 0, end = spans.at(-1)?.[1] ?? 0;
  return { start, gaps: end - start - frags.reduce((n, f) => n + f.length, 0), spans };
}

/** How well a match goes, from 1 down: earlier and tighter is better. */
const quality = (m) => 1 / (1 + 0.1 * m.start + 0.05 * m.gaps);

/**
 * The properties of `rows`, merged rows in the panels' order, that `query`
 * matches, best first as the palette ranks them, `starred` holding the
 * starred names: {shown, total}, `shown` the first `limit`, each {row,
 * match, score}, and `total` how many match. `score`, from 1 down, falls
 * along `shown`, so a list sorted by it keeps their order.
 */
export function findProperties(rows, query, starred, limit = SHOWN) {
  const frags = fragments(query);
  const found = [];
  rows.forEach((row, i) => {
    const m = match(row.name, frags);
    if (m) found.push({ row, match: m, starred: starred.has(row.name), i });
  });
  found.sort((a, b) => b.starred - a.starred || a.match.start - b.match.start || a.match.gaps - b.match.gaps || a.row.name.length - b.row.name.length || a.i - b.i);
  let last = 1;
  const shown = found.slice(0, limit).map(({ row, match: m }, i) => {
    last = Math.min(quality(m), last - (i ? 1e-6 : 0));
    return { row, match: m, score: Math.max(last, 1e-6) };
  });
  return { shown, total: found.length };
}

/**
 * What property `row` lets one do: `action` runs it, `toggle` turns it on
 * or off, `writable` takes a value, `readable` is read; and `mark`, the
 * palette's mark for it: action, setting or read.
 */
export function actsOf(row) {
  const control = controlOf(row.rpc), action = control === 'action';
  const writable = !action && row.rpc.access.includes('write'), readable = !action && row.rpc.access.includes('read');
  return { action, toggle: control === 'toggle', writable, readable, mark: action ? 'action' : writable ? 'setting' : 'read' };
}

/** Why argument `arg` cannot go to locked property `row` on `devices`, or null: an action takes none, a read-only property none, and a writable one what the edit bar takes. */
export function argProblem(row, devices, arg) {
  if (arg === '') return null;
  const acts = actsOf(row);
  if (acts.action) return words.NO_ARGUMENT;
  if (!acts.writable) return words.readOnly(row.name);
  return checkValue([row], devices, arg);
}

/** The palette searching, `query` typed. */
export const searching = (query = '') => ({ mode: 'search', query });
/** Property `name` locked, `query` the search to go back to, `arg` its argument, `read` true once a read of it was asked. */
const locked = (name, query, arg = '', read = false) => ({ mode: 'locked', name, query, arg, read });

/**
 * What key `key` does to palette state `state`, a searching or a locked
 * one, as {state, keep, act, close}: `keep` that the field keeps the key,
 * its own editing skipped; `act` what to call, {type: read, write or run,
 * name, text}; `close` that the palette closes. `row` is the property
 * locked, or, searching, the one highlighted, or null; `devices` the
 * devices a call reaches; `pick`, on a locked switch with no argument, the
 * value of On or Off highlighted. A key the state has no use for leaves
 * it, and is not kept.
 */
export function step(state, key, { row = null, devices = [], pick = null } = {}) {
  const stay = { state, keep: false, act: null, close: false };
  const to = (next, more = {}) => ({ ...stay, state: next, keep: true, ...more });
  if (state.mode === 'search') {
    if (!row) return stay;
    const acts = actsOf(row);
    if (key === ' ' || key === 'Tab') return to(locked(row.name, state.query));
    if (key !== 'Enter') return stay;
    if (acts.readable) return to(locked(row.name, state.query, '', true), { act: { type: 'read', name: row.name, text: '' } });
    if (acts.action || acts.writable) return to(locked(row.name, state.query));
    return stay;
  }
  const { arg } = state;
  // With nothing typed, Space or Tab turns a writable property to taking a value: a read shown goes.
  const typing = () => to(arg === '' && row && actsOf(row).writable ? { ...state, read: false } : state);
  if (key === ' ') return arg === '' ? typing() : stay;
  if (key === 'Tab') return typing();
  if (key === 'Backspace') return arg === '' ? to(searching(state.query)) : stay;
  if (key === 'Escape') {
    if (arg !== '') return to({ ...state, arg: '', read: false });
    return state.read ? { ...stay, close: true } : to(searching(state.query));
  }
  if (key !== 'Enter' || !row) return stay;
  if (state.read && arg === '') return { ...stay, close: true };
  const acts = actsOf(row);
  if (argProblem(row, devices, arg)) return to(state);
  if (acts.action) return to(state, { act: { type: 'run', name: row.name, text: '' }, close: true });
  if (arg !== '') return to(state, { act: { type: 'write', name: row.name, text: arg }, close: true });
  if (acts.toggle && pick !== null) return to(state, { act: { type: 'write', name: row.name, text: pick }, close: true });
  if (acts.readable) return to({ ...state, read: true }, { act: { type: 'read', name: row.name, text: '' } });
  return to(state);
}
