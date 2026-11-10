// Every key and gesture the page reads, by where focus is. The keys sheet
// lists this table, and each place's handler finds its action here with
// `actionFor`, so what the sheet says is what the keys do. A plot's keys,
// Space and Ctrl+C are plot commands, listed here from their table
// (commands.js).
//
// An entry is {show, on, keys, action, what}: `show` is how the sheet
// writes the keys, `on` where a gesture is made, `keys` the
// KeyboardEvent.key values that do it (none for what the browser or the
// plot's pointer handling reads), `action` the name its place's handler
// runs, and `what` what it does. `shift` true or false makes an entry take
// only that Shift; without it, either. Places of one title share a section
// of the sheet (`sheet`). A plot's key is a plot command, which with no
// plot selected does nothing, unless its `scope` is 'view': a view
// command, which with none selected acts on every plot.
import { COMMANDS, commandOf } from './commands.js';

/** Plot command `c`'s keys as an entry, its id the action of its keys where it has any. */
const keysOf = (c) => ({ show: c.show, ...(c.keys && { keys: c.keys, action: c.id }), scope: c.scope, what: c.what });

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
      { show: ['Ctrl', 'A'], what: 'Select every plot' },
      keysOf(commandOf('copyData')),
      { show: ['Ctrl', 'Z'], what: 'Undo' },
      { show: ['Ctrl', 'Y'], what: 'Redo; or Ctrl Shift Z' },
      { show: ['Ctrl', 'K'], what: 'Commands' },
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
      { show: ['Esc'], keys: ['Escape', 'Enter'], action: 'leave', what: 'Back to plot' },
    ],
  },
  {
    place: 'list',
    title: 'Lists',
    entries: [
      { ...MOVE, keys: ['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown', 'Home', 'End'] },
      { show: ['Enter'], keys: ['Enter'], shift: false, action: 'add', what: 'Add to the selected plots' },
      { show: ['Shift', 'Enter'], keys: ['Enter'], shift: true, action: 'newPlot', what: 'New plot with it' },
    ],
  },
  {
    place: 'tabs',
    title: 'Lists',
    entries: [
      { ...MOVE, keys: ['ArrowLeft', 'ArrowRight', 'Home', 'End'] },
      { show: ['Enter'], keys: ['Enter', ' '], action: 'open', what: 'open tab' },
      { show: ['F2'], keys: ['F2'], action: 'rename', what: 'Rename tab' },
      { show: ['Delete'], keys: ['Delete'], action: 'close', what: 'close tab' },
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
      { show: ['Right-click'], on: 'axis', what: 'Range, scale, cursors' },
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
      { show: ['Shift', '↑', '↓'], what: 'Extend the pick' },
      { show: ['Ctrl', 'A'], what: 'Pick every row' },
      { show: ['Ctrl', 'C'], what: 'Copy, as a sheet pastes' },
      { show: ['Enter'], what: 'Run, switch or edit the picked rows' },
      { show: ['Ctrl', 'Click'], on: 'action', what: 'Pick it to run with others' },
      { show: ['☆'], what: 'Add to Favorites' },
    ],
  },
];

/**
 * The keys sheet: a section per title, and in each a row per keys and
 * place, whose `what` joins its entries' in order.
 */
export function sheet() {
  const sections = [];
  for (const { title, entries } of KEYMAP) {
    let section = sections.find((s) => s.title === title);
    if (!section) sections.push((section = { title, rows: [] }));
    for (const { show, on, what } of entries) {
      const id = [...show, on ?? ''].join(' ');
      const row = section.rows.find((r) => r.id === id);
      if (!row) section.rows.push({ id, show, on, what });
      else if (!row.what.split(', ').includes(what)) row.what += `, ${what}`;
    }
  }
  return sections;
}

/**
 * The action `e` names in `place`, or null. A key with Ctrl, ⌘ or Alt names
 * none, as those are the browser's.
 */
export function actionFor(place, e) {
  if (e.ctrlKey || e.metaKey || e.altKey) return null;
  const entries = KEYMAP.find((p) => p.place === place)?.entries ?? [];
  const hit = entries.find((x) => x.keys?.includes(e.key) && (x.shift === undefined || x.shift === e.shiftKey));
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
