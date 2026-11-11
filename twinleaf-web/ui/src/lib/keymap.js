// Every key and gesture the page reads, by where focus is. The keys sheet
// lists this table, and the key router (keyrouter.js) finds each key's
// action here with `actionFor`, so what the sheet says is what the keys do.
// A plot's keys, Space, Ctrl+C, the Signals list's keys that put signals
// on plots and a tab's keys that rename and close it are commands, listed
// here from their table (commands.js).
//
// An entry is {show, on, keys, ctrl, shift, action, what}: `show` is how the
// sheet writes the keys, and an entry without it is not on the sheet; `on`
// where a gesture is made; `keys` the KeyboardEvent.key values that do it
// (none for what the browser or the plot's pointer handling reads), in
// lower case for an entry with `ctrl`; `ctrl` true takes them with Ctrl or
// ⌘ only, and without it they are taken with neither; `shift` true or false
// makes an entry take only that Shift, and without it, either; `action` is
// the name its claimant in the router runs, and `what` what it does. Places
// of one title share a section of the sheet (`sheet`), and a place without
// a title has none. A plot's key is a plot command, which with no plot
// selected does nothing, unless its `scope` is 'view': a view command,
// which with none selected acts on every plot.
import { COMMANDS, commandOf } from './commands.js';

/** Plot command `c`'s keys as an entry, its id the action of its keys where it has any. */
const keysOf = (c) => ({ show: c.show, ...(c.keys && { keys: c.keys, action: c.id }), ...(c.shift !== undefined && { shift: c.shift }), scope: c.scope, what: c.what });

const digits = ['1', '2', '3', '4', '5', '6', '7', '8', '9'];
const MOVE = { show: ['Arrows', 'Home', 'End'], action: 'move', what: 'Move' };

export const KEYMAP = [
  {
    place: 'page',
    title: 'Anywhere',
    entries: [
      { show: ['Tab'], what: 'Next; with Shift, back' },
      { show: ['Enter'], what: 'Go in' },
      { show: ['Esc'], keys: ['Escape'], action: 'escape', what: 'Restore, deselect, or go out a level' },
      { show: ['F6'], what: 'Next region; with Shift, back' },
      { show: ['1–9'], keys: digits, action: 'goToPlot', what: 'Go to plot' },
      { show: ['[', ']'], keys: ['[', ']'], action: 'stepTab', what: 'Previous, next tab' },
      keysOf(commandOf('pause')),
      { show: ['Ctrl', 'A'], keys: ['a'], ctrl: true, action: 'selectAll', what: 'Select every plot' },
      { ...keysOf(commandOf('copyData')), keys: ['c'], ctrl: true, shift: false, action: 'copyData' },
      { show: ['Ctrl', 'Z'], keys: ['z'], ctrl: true, shift: false, action: 'undo', what: 'Undo' },
      { show: ['Ctrl', 'Y'], keys: ['y', 'z'], ctrl: true, action: 'redo', what: 'Redo; or Ctrl Shift Z' },
      { show: ['Ctrl', 'K'], keys: ['k'], ctrl: true, action: 'palette', what: 'Commands' },
      { show: ['?'], keys: ['?'], action: 'help', what: 'This sheet' },
    ],
  },
  {
    place: 'plot',
    title: 'Plot',
    entries: COMMANDS.filter((c) => c.keys && !c.place).map(keysOf),
  },
  {
    place: 'signals',
    title: 'Lists',
    entries: [
      { ...MOVE, keys: ['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown', 'Home', 'End'] },
      { show: ['Delete'], keys: ['Delete', 'Backspace'], action: 'remove', what: 'Remove signal' },
      { keys: ['Enter'], action: 'leave' },
    ],
  },
  {
    place: 'list',
    title: 'Lists',
    entries: [
      { ...MOVE, keys: ['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown', 'Home', 'End'] },
      { show: ['Shift', '↑', '↓'], on: 'signals', what: 'Extend the pick' },
      { show: ['Space'], on: 'signals', keys: [' '], action: 'pick', what: 'Pick the signal the arrows are on; with Shift, the run to it' },
      { show: ['Click'], on: 'signal', what: 'Pick it; with Ctrl or Shift, as rows are picked' },
      { show: ['Double-click'], on: 'signal', what: 'Add to the selected plots' },
      { show: ['Ctrl', 'A'], on: 'signals', keys: ['a'], ctrl: true, action: 'pickAll', what: 'Pick every signal shown' },
      ...COMMANDS.filter((c) => c.place === 'list').map(keysOf),
    ],
  },
  {
    place: 'tabs',
    title: 'Lists',
    entries: [
      { ...MOVE, keys: ['ArrowLeft', 'ArrowRight', 'Home', 'End'] },
      { show: ['Enter'], keys: ['Enter', ' '], action: 'open', what: 'open tab' },
      ...COMMANDS.filter((c) => c.place === 'tabs').map(keysOf),
    ],
  },
  {
    place: 'mouse',
    title: 'Mouse',
    entries: [
      { show: ['Drag'], what: 'Pan time or frequency' },
      { show: ['Ctrl', 'Drag'], what: 'Zoom to a range' },
      { show: ['Wheel'], what: 'Zoom time or frequency' },
      { show: ['Shift', 'Wheel'], what: 'Zoom y' },
      { show: ['Double-click'], what: 'Follow live' },
      { show: ['Right-click'], what: 'Menu' },
      { show: ['Drag'], on: 'time axis', what: 'Seek' },
      { show: ['Drag'], on: 'y axis', what: 'Offset y' },
      { show: ['Wheel'], on: 'y axis', what: 'Zoom y' },
      { show: ['Double-click'], on: 'y axis', what: 'Fit y' },
      { show: ['Right-click'], on: 'axis', what: 'Range, scale, numbers, cursors' },
      { show: ['Click'], what: 'Select plot' },
      { show: ['Shift', 'Click'], what: 'Add to selection, or remove' },
      { show: ['Drag'], on: 'header', what: 'Move plot, or the selection' },
      { show: ['Drag'], on: 'background', what: 'Select plots in a box' },
      { show: ['Drag'], on: 'gap', what: 'Resize' },
    ],
  },
  {
    place: 'cursors',
    title: 'Cursors',
    entries: [
      { show: ['Drag'], on: 'y axis inward', what: 'X1, then X2' },
      { show: ['Drag'], on: 'x axis upward', what: 'Y1, then Y2' },
      { show: ['Drag'], on: 'X1 X2 tab, x axis', what: 'X cursor' },
      { show: ['Drag'], on: 'Y1 Y2 tab, y axis', what: 'Y cursor' },
      { show: ['Drag'], on: 'line or tag', what: 'Move' },
      { show: ['Shift'], on: 'during a drag', what: 'Snap to a tick, the other cursor or the data' },
      { show: ['Drag'], on: 'off its lane', what: 'Remove' },
      { show: ['Right-click'], on: 'readout', what: 'Copy, clear' },
    ],
  },
  {
    place: 'properties',
    title: 'Properties',
    entries: [
      { show: ['Click'], what: 'Pick a row' },
      { show: ['Shift', 'Click'], what: 'Pick a run of rows' },
      { show: ['Ctrl', 'Click'], what: 'Add or drop a row' },
      { keys: ['ArrowUp', 'ArrowDown', 'PageUp', 'PageDown', 'Home', 'End'], action: 'move' },
      { show: ['Shift', '↑', '↓'], what: 'Extend the pick' },
      { show: ['Ctrl', 'A'], keys: ['a'], ctrl: true, action: 'pickAll', what: 'Pick every row' },
      { show: ['Ctrl', 'C'], keys: ['c'], ctrl: true, action: 'copy', what: 'Copy, as a sheet pastes' },
      { show: ['Enter'], keys: ['Enter'], action: 'enter', what: 'Run, switch or edit the picked rows' },
      { show: ['Ctrl', 'Click'], on: 'action', what: 'Pick it to run with others' },
      { show: ['☆'], what: 'Add to Favorites' },
    ],
  },
  // What the sheet does not list: a panel's header, the window field, a
  // plot's kind and layout dropdowns side by side, and a drag under way.
  {
    place: 'panel',
    entries: [
      { keys: [' '], action: 'fold' },
      { keys: ['Enter'], action: 'enter' },
    ],
  },
  {
    place: 'window',
    entries: [{ keys: ['ArrowUp', 'ArrowDown'], action: 'step' }],
  },
  { place: 'kind', entries: [{ keys: ['ArrowLeft', 'ArrowRight'], action: 'across' }] },
  { place: 'drag', entries: [{ keys: ['Escape'], action: 'cancel' }] },
];

/**
 * The keys sheet: a section per title, and in each a row per keys and
 * place, whose `what` joins its entries' in order.
 */
export function sheet() {
  const sections = [];
  for (const { title, entries } of KEYMAP) {
    if (!title) continue;
    let section = sections.find((s) => s.title === title);
    if (!section) sections.push((section = { title, rows: [] }));
    for (const { show, on, what } of entries) {
      if (!show) continue;
      const id = [...show, on ?? ''].join(' ');
      const row = section.rows.find((r) => r.id === id);
      if (!row) section.rows.push({ id, show, on, what });
      else if (!row.what.split(', ').includes(what)) row.what += `, ${what}`;
    }
  }
  return sections;
}

/**
 * The action `e` names in `place`, or null. A key with Alt names none, as
 * those are the browser's, and one with Ctrl or ⌘ only an entry with `ctrl`.
 */
export function actionFor(place, e) {
  if (e.altKey) return null;
  const ctrl = !!(e.ctrlKey || e.metaKey), key = ctrl ? e.key.toLowerCase() : e.key;
  const entries = KEYMAP.find((p) => p.place === place)?.entries ?? [];
  const hit = entries.find((x) => !!x.ctrl === ctrl && x.keys?.includes(key) && (x.shift === undefined || x.shift === !!e.shiftKey));
  return hit?.action ?? null;
}

/** Whether plot command `action` is a 'view' command or a 'plot' command. */
export function scopeOf(action) {
  return commandOf(action)?.scope ?? 'plot';
}

/**
 * The index a list's arrow, Home or End key moves to from `at` in a list of
 * `n`, or null for another key.
 */
export function stepIn(e, at, n) {
  const to = { ArrowLeft: at - 1, ArrowUp: at - 1, ArrowRight: at + 1, ArrowDown: at + 1, Home: 0, End: n - 1 }[e.key];
  return to === undefined ? null : Math.max(0, Math.min(n - 1, to));
}
