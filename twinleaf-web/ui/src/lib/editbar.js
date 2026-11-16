// What the edit bar under the Properties panels offers for the rows it
// edits: the rows picked, each property once, in the order they were
// picked, or the row the arrows are on when none is.
//
// One row edits as its control says (properties.js `controlOf`). Several
// rows edit together only when they are all of one kind: switches, values
// of one type, or actions. Rows of several kinds edit nothing, and say
// so. Rows that only show a value have no bar. What the bar says is
// words.js's.
//
// A value typed is checked against the type of each device's entry before
// anything is written (`checkValue`), as the hub reads it
// (twinleaf/src/session/rpc/value.rs `parse`): a whole number in its
// type's range, a number, true or false, text within its length, or hex
// digits in pairs. A type the table names no rule for is left to the hub,
// which refuses what it cannot take.

import { controlOf, sameShown, switchOf, writersOf } from './properties.js';
import * as words from './words.js';

/** Switches showing `values`: `state` On, Off or Mixed (as when any is unknown), and `enter` the value Enter writes, off when every one is on, else on. */
function switching(values) {
  const states = new Set(values.map(switchOf));
  const state = states.size === 1 && states.has('on') ? 'On' : states.size === 1 && states.has('off') ? 'Off' : 'Mixed';
  return { state, enter: state === 'On' ? 'false' : 'true' };
}

/** The kind a row is edited as: action, toggle, display (nothing to edit), or `value:${type}`. */
export function kindOf(row) {
  const control = controlOf(row.rpc);
  if (control === 'value') return `value:${row.rpc.type ?? ''}`;
  return control === 'none' ? 'display' : control;
}

/**
 * The edit bar for `rows`, merged rows each once, of `devices`, whose
 * values `valueOf(d, name)` gives, or null for none. It is {kind, label,
 * rows, reach}, `reach` the devices a write of any of the rows reaches,
 * and by its kind:
 *  - 'one', a row alone: `control` its control, and for a switch, as for
 *    several, `state` and `enter`;
 *  - 'kinds', rows of several kinds, which edits nothing: `hint` says so,
 *    and `detail` names the kinds;
 *  - 'switches': `state` On, Off or Mixed, and `enter`, the value Enter
 *    writes: all of them off when every one is on, else all on;
 *  - 'values', of one type: `shared`, the value every device of every row
 *    shows, or undefined when they differ;
 *  - 'actions': run in the order of `rows`.
 */
export function editBar(rows, devices, valueOf) {
  if (!rows.length) return null;
  const kinds = rows.map(kindOf);
  const reach = devices.filter((d) => rows.some((r) => writersOf(r, [d]).length));
  if (kinds.every((k) => k === 'display')) return null;
  const values = rows.flatMap((r) => devices.filter((d) => r.entries.has(d.route)).map((d) => valueOf(d, r.name)));
  if (rows.length === 1) return { kind: 'one', label: rows[0].name, rows, reach, control: controlOf(rows[0].rpc), ...(kinds[0] === 'toggle' ? switching(values) : {}) };
  const distinct = [...new Set(kinds)];
  if (distinct.length > 1) {
    const counts = distinct.map((kind) => ({ kind, n: kinds.filter((k) => k === kind).length }));
    return { kind: 'kinds', label: words.severalKinds(rows.length, distinct.length), detail: words.kinds(counts), hint: words.ONE_KIND, rows, reach };
  }
  const kind = distinct[0];
  if (kind === 'toggle') {
    const s = switching(values);
    return { kind: 'switches', label: words.switches(rows.length, s.state), rows, reach, ...s };
  }
  if (kind === 'action') return { kind: 'actions', label: words.actions(rows.length), rows, reach };
  const shared = values.length && values.every((v) => sameShown(v, values[0])) ? values[0] : undefined;
  return { kind: 'values', label: words.values(rows.length, kind.slice(6)), rows, reach, shared };
}

const INT = /^([ui])(8|16|32|64)$/;
const FLOAT = /^f(32|64)$/;
const FLOAT_TEXT = /^[+-]?(inf|infinity|nan|(\d+\.?\d*|\.\d+)(e[+-]?\d+)?)$/i;
const SIZED = /^(string|bytes)(?:<(\d+)>)?$/;

/** Why `text` is no value of type `type`, as words.js says it, or null when it is one, or when `type` has no rule here. */
export function typeProblem(type, text) {
  const int = INT.exec(type ?? '');
  if (int) {
    // A negative number for an unsigned type is out of its range.
    const signed = int[1] === 'i', bits = BigInt(int[2]);
    const lo = signed ? -(1n << (bits - 1n)) : 0n, hi = signed ? (1n << (bits - 1n)) - 1n : (1n << bits) - 1n;
    if (!/^[+-]?\d+$/.test(text)) return words.notWhole(type, lo, hi);
    const v = BigInt(text.replace(/^\+/, ''));
    return v < lo || v > hi ? words.outOfRange(text, type, lo, hi) : null;
  }
  if (FLOAT.test(type ?? '')) return FLOAT_TEXT.test(text) ? null : words.notNumber(type);
  if (type === 'bool') return text === 'true' || text === 'false' ? null : words.NOT_BOOL;
  const sized = SIZED.exec(type ?? '');
  if (!sized) return null;
  const n = sized[2] === undefined ? null : Number(sized[2]);
  if (sized[1] === 'string') {
    const len = new TextEncoder().encode(text).length;
    return n !== null && len > n ? words.tooLong(type, n, len) : null;
  }
  if (!/^([0-9a-f]{2})*$/i.test(text)) return words.notHex(type);
  return n !== null && text.length !== 2 * n ? words.wrongLength(type, n) : null;
}

/**
 * Why `text` cannot be written to `rows` on `devices`, or null when it
 * can: the first problem of its type on a device a write of a row reaches,
 * each device's entry checked by its own type.
 */
export function checkValue(rows, devices, text) {
  for (const r of rows) {
    for (const d of writersOf(r, devices)) {
      const problem = typeProblem(r.entries.get(d.route).type, text);
      if (problem) return problem;
    }
  }
  return null;
}
