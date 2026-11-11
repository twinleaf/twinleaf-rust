// Every word the page says about writing to devices and running their
// actions: the notice of a write, which says what was set, on how many
// devices and from what, and what Revert will leave out; what Ctrl+Z says
// right after a write; the notice of a
// Revert, which says what it put back and what it left; the report of a
// run; the question a run of several actions asks first; and the edit
// bar's and the command palette's labels, and why a value typed cannot be
// written. A notice is one function of what came back, each device's
// outcome (writes.svelte.js): {level, text, detail}, `detail` the lines
// under the text.
//
// And the question lowering a History limit asks before it drops history
// the hub holds.

import { switchOf } from './properties.js';
import { spanText } from './units.js';

/** `items` joined as a sentence lists them: "a", "a and b", "a, b and c", the rest past `most` counted. */
export function list(items, most = 3) {
  const shown = items.length > most ? [...items.slice(0, most - 1), `${items.length - most + 1} more`] : items;
  return shown.length < 2 ? (shown[0] ?? '') : `${shown.slice(0, -1).join(', ')} and ${shown.at(-1)}`;
}
/** `items` in the order they run: "a, then b, then c". */
export const inOrder = (items) => items.join(', then ');
/** `n` of a noun, as "1 switch" or "3 switches". */
const count = (n, one, many) => `${n} ${n === 1 ? one : many}`;
/** How a notice names device `d`: by its name, and its route unless it is the device at the root, as "test (/0)". */
export const device = (d) => (d.route === '/' ? (d.name ?? 'device') : `${d.name ?? 'device'} (${d.route})`);
/** How a value reads in a notice: a switch's as on or off. */
const valueText = (v, toggle) => (toggle ? (switchOf(v) ?? 'unknown') : String(v));
/** Rows of one kind as a notice names them: the row's name, or "3 switches" or "3 values". */
const rowsText = (rows, toggle) => (rows.length === 1 ? rows[0] : toggle ? count(rows.length, 'switch', 'switches') : count(rows.length, 'value', 'values'));
/** What ends a notice's text: " · 3 devices", " · 2 of 3 devices", or nothing for one device that took it. */
const devicesText = (ok, of) => (ok < of ? ` · ${ok} of ${of} devices` : of === 1 ? '' : ` · ${of} devices`);
/** How many devices of `results` there are, or that came to `state`. */
const devicesOf = (results, state) => new Set(results.filter((r) => !state || r.state === state).map((r) => r.route)).size;

/** The button that puts a write back. */
export const REVERT = 'Revert';
/** What Ctrl+Z says right after a write, while its Revert is offered, as it takes back no edit made before the write. */
export const USE_REVERT = 'Nothing undone: press Revert to undo the write';
/** What Ctrl+Z says right after a write that offers no Revert, as it takes back no edit made before the write. */
export const UNDO_UNKNOWN = "Nothing undone: the old value isn't known";
/** What Ctrl+Z says right after a device action, as it takes back no edit made before the action. */
export const UNDO_ACTION = "Nothing undone: a device action can't be undone";

/**
 * What each device made of a call, as lines: the devices that refused,
 * with why, first, then those that answered an error, then those that did
 * not answer; then, when a refusal kept the call from the others, that
 * nothing was sent to them. Each outcome is {label, name, state, error};
 * `named` puts the row's name after its device, as when several rows were
 * written.
 */
export function outcomes(results, named = false) {
  const who = (r) => `${r.label}${named ? ` ${r.name}` : ''}`;
  const says = {
    refused: (r) => `${who(r)} refused: ${r.error}`,
    error: (r) => `${who(r)}: ${r.error}`,
    unknown: (r) => `${who(r)} did not answer: ${r.error}`,
  };
  const bad = ['refused', 'error', 'unknown'].flatMap((state) => results.filter((r) => r.state === state).map(says[state]));
  const skipped = devicesOf(results, 'skipped');
  return [...(bad.length ? [list(bad, 2)] : []), ...(skipped ? [`Nothing was sent to the ${skipped === 1 ? 'other device' : `${skipped} other devices`}`] : [])];
}
/** What a cell's tooltip says of its device's last call that did not take. */
export function outcome(state, error) {
  if (state === 'refused') return `Refused: ${error}`;
  if (state === 'skipped') return 'Not sent: another device refused';
  if (state === 'unknown') return `No answer: ${error}`;
  return error;
}
/** The level of a notice of `results`: success when every one took and nothing was `left`, error when none took, else warning. */
const levelOf = (results, left = false) => (results.every((r) => r.state === 'ok') && !left ? 'success' : results.some((r) => r.state === 'ok') || !results.length ? 'warning' : 'error');

/**
 * The notice of a write of `value` to `rows`, switches when `toggle`:
 * `results` each device's outcome of each row, `was` the old value when
 * one row had one on every device that took it, `unknownOld` the devices
 * whose old value was unknown, and `revert` whether it offers Revert.
 */
export function wrote({ rows, toggle, value, results, was, unknownOld, revert }) {
  const took = devicesOf(results, 'ok'), of = devicesOf(results);
  const text = !took
    ? toggle
      ? `Could not turn ${valueText(value, true)} ${rowsText(rows, true)}`
      : `Could not set ${rowsText(rows, false)}`
    : `${toggle ? `Turned ${valueText(value, true)} ${rowsText(rows, true)}` : `Set ${rowsText(rows, false)} to ${value}`}${was === undefined || took < of ? '' : ` (was ${valueText(was, toggle)})`}${devicesText(took, of)}`;
  const detail = outcomes(results, rows.length > 1);
  if (took && !revert) detail.push('No Revert: old value unknown');
  else if (revert && unknownOld.length) detail.push(`Revert skips ${list(unknownOld)}: old value unknown`);
  return { level: levelOf(results), text, detail };
}

/**
 * The notice of a Revert of `rows`, switches when `toggle`: `results` each
 * device's outcome of putting a row back, `old` the value put back when
 * one row had one, and `left` the devices it left as they are, each
 * {label, name, now, why}, `why` changed (since the write) or unchecked.
 */
export function reverted({ rows, toggle, results, old, left }) {
  const back = devicesOf(results, 'ok');
  const put = [...new Set(results.filter((r) => r.state === 'ok').map((r) => r.name))];
  const text = back
    ? `Put ${rowsText(put, toggle)} back${old === undefined ? '' : ` to ${valueText(old, toggle)}`}${devicesText(back, back)}`
    : results.length
      ? `Could not put ${rowsText(rows, toggle)} back`
      : 'Nothing put back';
  const named = rows.length > 1;
  const why = (l) => `${l.label}${named ? ` ${l.name}` : ''} ${l.why === 'unchecked' ? 'is unchecked; left as is' : `changed since; left at ${valueText(l.now, toggle)}`}`;
  return { level: levelOf(results, left.length > 0), text, detail: [...outcomes(results, named), ...(left.length ? [list(left.map(why), 2)] : [])] };
}

/** Why Revert went away: device `label` (or the hub, with none) `why`, disconnected or reconnected. */
export const revertOff = (label, why) => `Revert is off: ${label ?? 'the hub'} ${why}`;

/**
 * The report of a run of actions `names` in order: `done` the actions that
 * ran on every device, and `failed`, when one did not, {name, results},
 * its devices' outcomes, which stopped the run; `devices` how many devices
 * it reached.
 */
export function ran({ names, done, failed, devices }) {
  if (!failed) return { level: 'success', text: `Ran ${inOrder(done)}${devicesText(devices, devices)}`, detail: [] };
  const { name, results } = failed;
  const bad = results.filter((r) => r.state !== 'ok' && r.state !== 'skipped').length;
  const text = results.some((r) => r.state === 'refused') ? `Could not run ${name}` : `${name} failed${bad === results.length ? '' : ` on ${bad} of ${results.length} devices`}`;
  const rest = names.slice(names.indexOf(name) + 1);
  const detail = [...outcomes(results), ...(done.length ? [`Ran first: ${inOrder(done)}`] : []), ...(rest.length ? [`Not run: ${rest.join(', ')}`] : [])];
  return { level: done.length || results.some((r) => r.state === 'ok') ? 'warning' : 'error', text, detail };
}

/** The question running several actions asks first, of `names` in order on `devices` devices. */
export const runAsk = (names, devices) => ({
  title: `Run ${names.length} actions?`,
  text: `${inOrder(names)} on ${count(devices, 'device', 'devices')}.`,
  cancel: 'Cancel',
  run: `Run ${names.length}`,
});

/** The edit bar's label for `rows` rows of `kinds` kinds: "5 rows of 4 kinds". */
export const severalKinds = (rows, kinds) => `${rows} rows of ${kinds} kinds`;
/** What those rows are, each {kind, n}, for the label's tooltip: "2 switches, 1 value (u8)". */
export const kinds = (counts) => counts.map(({ kind, n }) => kindCount(kind, n)).join(', ');
/** `n` rows of kind `kind`: action, toggle, display, or `value:${type}`. */
function kindCount(kind, n) {
  if (kind === 'action') return count(n, 'action', 'actions');
  if (kind === 'toggle') return count(n, 'switch', 'switches');
  if (kind === 'display') return count(n, 'read-only row', 'read-only rows');
  return `${count(n, 'value', 'values')} (${kind.slice(6) || 'no type'})`;
}
/** What the edit bar says to rows of several kinds, and what Enter shows on them. */
export const ONE_KIND = 'Pick one kind to edit';
/** The edit bar's label for `n` switches, `state` On, Off or Mixed. */
export const switches = (n, state) => `${count(n, 'switch', 'switches')} · ${state}`;
/** The edit bar's label for `n` values of type `type`. */
export const values = (n, type) => `${count(n, 'value', 'values')} (${type})`;
/** The edit bar's field for values that differ. */
export const MIXED = '— mixed';
/** The edit bar's label for `n` actions. */
export const actions = (n) => count(n, 'action', 'actions');
/** The edit bar's buttons, and an action button's tooltip, for `n` devices. */
export const ON = 'On';
export const OFF = 'Off';
export const writeTo = (n) => `Write to ${count(n, 'device', 'devices')}`;
export const runOn = (n) => `Run on ${count(n, 'device', 'devices')}`;
export const runSeveral = (n) => `Run ${n}…`;
/** The command palette's read of a property, on `n` devices. */
export const readFrom = (n) => `Read from ${count(n, 'device', 'devices')}`;
/** How many devices a property's call reaches, as the edit bar says it. */
export const reach = (n) => count(n, 'device', 'devices');

// Why a value typed cannot be written (editbar.js `checkValue`).
/** Text that is no whole number for integer type `type`, of range `lo` to `hi`. */
export const notWhole = (type, lo, hi) => `Not a whole number: ${type} takes ${lo} to ${hi}`;
/** Whole number `text` outside the range `lo` to `hi` of `type`. */
export const outOfRange = (text, type, lo, hi) => `${text} is out of range: ${type} takes ${lo} to ${hi}`;
/** Text that is no number for float type `type`. */
export const notNumber = (type) => `Not a number: ${type} takes a number, as 1.5 or 2e-3`;
/** Text that is no switch value. */
export const NOT_BOOL = 'Not true or false: bool takes true or false';
/** Text of `len` bytes, longer than the `max` that `type` holds. */
export const tooLong = (type, max, len) => `Too long: ${type} takes at most ${max} bytes, not ${len}`;
/** Text that is not hex digits in pairs, for bytes type `type`. */
export const notHex = (type) => `Not hex: ${type} takes hex digits in pairs, as 0aff`;
/** Hex of the wrong length for `type`, which takes `n` bytes. */
export const wrongLength = (type, n) => `${type} takes ${n} bytes, as ${2 * n} hex digits`;
/** Text typed for an action, which takes none. */
export const NO_ARGUMENT = 'An action takes no value: Enter runs it';
/** Text typed for property `name`, which only reads. */
export const readOnly = (name) => `${name} is read-only: Enter reads it`;

/**
 * The question lowering a History limit asks first, of what it drops
 * (session.svelte.js `dropping`): `live` and `paused` the oldest seconds of
 * the live history and of what paused plots hold, `keep` the limit now
 * set, as the setting shows it. Its lines, and its two buttons.
 */
export function lowerAsk({ live, paused }, keep) {
  const lines = [];
  if (live > 0) lines.push(`This drops the oldest ${spanText(live)} now held`);
  if (paused > 0) lines.push(`Paused plots lose the oldest ${spanText(paused)} they show`);
  return { lines, drop: `Drop ${spanText(Math.max(live, paused))}`, keep: `Keep ${keep}` };
}
