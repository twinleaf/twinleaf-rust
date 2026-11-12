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
// And the History popover's: the question lowering a limit asks before it
// drops history the hub holds, what a paused plot keeps, and what its
// waterline reads as to a screen reader.
//
// And what the page says of lost data and of the hub's log: the status
// bar's loss mark and its tooltip, and the log drawer's rows, a loss in the
// page's own words from what the hub says was lost.

import { switchOf } from './properties.js';
import { bytesEstimate, bytesText, spanText } from './units.js';

/** `n` things counted, to two figures from a thousand on, as "312", "2.1 k" or "1.5 M". */
export const countText = (n) => (n >= 1e6 ? `${+(n / 1e6).toPrecision(2)} M` : n >= 1e3 ? `${+(n / 1e3).toPrecision(2)} k` : String(n));
/**
 * What the status bar says of one cause of loss in the last minute, {kind,
 * n, seconds} (loss.svelte.js), as the parts of a phrase, each a string or
 * {num, ch}, a number and the characters it is given, so it keeps its
 * width as it grows: "link dropped 4×, 11 s", "312 samples skipped", "hub
 * dropped 2.1 k", "page behind 3×".
 */
export function lossWords({ kind, n, seconds }) {
  const times = { num: `${n}×`, ch: 3 }, count = { num: countText(n), ch: 5 };
  if (kind === 'link') return ['link dropped ', times, ', ', { num: spanText(seconds), ch: 6 }];
  if (kind === 'skipped') return [count, n === 1 ? ' sample skipped' : ' samples skipped'];
  if (kind === 'dropped') return ['hub dropped ', count];
  return ['page behind ', times];
}
/** The status bar's one word for the cause of loss that stands for the most data. */
export const LOSS_CAUSE = { link: 'link', skipped: 'skipped', dropped: 'dropped', behind: 'page' };
/** A share of the data, from 0 to 1, as the status bar's loss mark reads it, in whole percents and never as nothing: "<1%", "18%". */
export const shareText = (share) => (share < 0.005 ? '<1%' : `${Math.round(share * 100)}%`);
/** What the mark says while a device's link is down, before how long. */
export const LINK_DOWN = 'link down';
/** A time of day, `ms` since 1970, to the minute, as "14:02". */
const minuteText = (ms) => {
  const d = new Date(ms);
  return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
};
/**
 * The loss mark's tooltip, one line: the device whose link is down now,
 * `down` as `device` names it, or null for none; then what the page counts
 * lost since it opened at `since`, `totals` as loss.svelte.js gives them,
 * each cause in `lossWords`; and what a press does.
 */
export function lossTip(down, totals, since) {
  const phrases = totals.map((cause) => lossWords(cause).map((part) => part.num ?? part).join(''));
  const counted = phrases.length ? `${since === null ? 'Lost' : `Since ${minuteText(since)}`}: ${phrases.join(' · ')}. ` : '';
  return `${down ? `The link to ${down} is down. ` : ''}${counted}Click for the log`;
}

/** Each level of the log as the drawer names it. */
export const LEVEL_NAMES = { error: 'Error', warn: 'Warn', info: 'Info', debug: 'Debug' };
/** The drawer's toggle that leaves only the lines of a loss. */
export const LOG_LOSS = 'Data loss';
/** The drawer's choice of every source, and the headings its list of sources has: the devices, and the parts of the proxy. */
export const LOG_ALL = 'All sources';
export const LOG_DEVICES = 'Devices';
export const LOG_PARTS = 'Proxy';
/** What the drawer says in place of its rows when none shows: with only the lines of a loss showing, or under any other filter. */
export const logEmpty = (lossOnly, any) => (lossOnly ? 'No data has been lost' : any ? 'Nothing matches' : 'No lines yet');
/** Why the proxy lost a device, in the page's words, by what a link's line says of it (protocol.js `why`). */
export const LOST_WHY = {
  disconnected: 'sensor disconnected',
  stopped: 'the proxy stopped',
  failed_to_connect: 'the link did not open',
  failed_to_reconnect: 'the proxy gave up reconnecting',
  quiet: 'silent for 3 s',
};
/**
 * What a line of a loss says, of `loss` {cause, stream, samples, seconds,
 * why, url} (protocol.js), in the page's words: a link's drop with why the
 * proxy lost the device and the link's address, where the hub knows them,
 * and its return with the address.
 */
export function lossLine({ cause, stream, samples, seconds, why, url }) {
  if (cause === 'link') {
    const said = seconds == null ? `link down${LOST_WHY[why] ? `, ${LOST_WHY[why]}` : ''}` : `link back after ${seconds.toFixed(1)} s`;
    return url ? `${said} · ${url}` : said;
  }
  const n = `${countText(samples)} ${samples === 1 ? 'sample' : 'samples'}`;
  if (cause === 'skipped') return `${n} of ${stream} skipped`;
  return `${cause} dropped ${n} of ${stream}`;
}
/** Who said log row `row`, as its source column reads: the part of the proxy that said it, or its device as `named(route)` names it. */
export const logSource = (row, named) => (row.source === 'proxy' ? row.part : named(row.route));
/** What log row `row` says, as its message column reads: a loss in `lossLine`'s words, any other line in its own, and the proxy's line of a device after that device's name. */
export function logMessage(row, named) {
  const said = row.loss ? lossLine(row.loss) : row.text;
  return row.source === 'proxy' && row.route !== null ? `${named(row.route)}: ${said}` : said;
}
/** The divider of an outage of the link to the device `label` names: how long it lasted, `back` seconds, once the link is back. */
export const outageText = (label, back) => (back === undefined ? `link to ${label} down` : `link to ${label} down ${back.toFixed(1)} s`);
/** The drawer's button that keeps the newest line in view, and the one that copies the lines shown. */
export const FOLLOW = 'Follow';
export const COPY_LINES = 'Copy the lines shown';
/** Why a row has no moment to show on the plots. */
export const NO_MOMENT = 'No samples of this device had arrived';
/** What Show on plots says on a tab with no time plot. */
export const NO_TIME_PLOT = 'No time plot on this tab to show it on';
/** The notice of a copy of `n` lines. */
export const copiedLines = (n) => `Copied ${n.toLocaleString()} ${n === 1 ? 'line' : 'lines'}`;

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

/**
 * What a plot paused keeps under a time limit of `maxAge` seconds, by
 * session.svelte.js `pausing`: its full span, for how long before it loses
 * its oldest, and what keeping it all needs.
 */
export function pausedKeeps({ need, keeps }, maxAge) {
  const span = spanText(maxAge);
  if (keeps >= maxAge) return `A paused plot keeps its full ${span}`;
  if (keeps >= 1) return `A paused plot keeps its full ${span} for ~${spanText(keeps)}, then loses its oldest. All of it needs ${bytesEstimate(need)}`;
  return `A paused plot loses its oldest at once. Its full ${span} needs ${bytesEstimate(need)}`;
}

/**
 * The History waterline as a screen reader reads it: the memory held of
 * the budget, then, while the time limit comes first, its two marks, the
 * live history at the time limit and that with a full paused plot beside
 * it (session.svelte.js `pausing`), the second past the limit when it is.
 */
export function waterText(bytes, budget, pause, timeFirst) {
  const held = `${bytesText(bytes)} of ${bytesText(budget)}`;
  if (!pause || !timeFirst) return held;
  const past = pause.need > budget ? ', past the limit' : '';
  return `${held}. The time limit fills ${bytesEstimate(pause.steady)}, and ${bytesEstimate(pause.need)} with a paused plot${past}`;
}
