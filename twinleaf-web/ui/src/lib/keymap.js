// Every key the page reads, by where focus is. The keys dialog lists this
// table, and each place's handler finds its action here with `actionFor`, so
// what the dialog says is what the keys do.
//
// An entry is {show, keys, action, what}: `show` is how the dialog writes
// the keys, `keys` the KeyboardEvent.key values that do it (none for a key
// the browser handles), `action` the name its place's handler runs, and
// `what` what it does. `shift` true or false makes an entry take only that
// Shift; without it, either.

const digits = ['1', '2', '3', '4', '5', '6', '7', '8', '9'];

export const KEYMAP = [
  {
    place: 'page',
    title: 'Anywhere',
    entries: [
      { show: ['Tab'], keys: [], what: 'Next plot, list or control; Shift+Tab goes back' },
      { show: ['1', '…', '9'], keys: digits, action: 'goToPlot', what: 'Go to that plot' },
      { show: ['[', ']'], keys: ['[', ']'], action: 'stepTab', what: 'Previous or next layout' },
      { show: ['Space'], keys: [' '], action: 'pause', what: 'Pause the highlighted plots, or stream from where they are' },
      { show: ['Esc'], keys: ['Escape'], action: 'escape', what: 'Restore, clear the selection, then the cursors' },
      { show: ['Ctrl', 'K'], keys: [], what: 'Commands' },
      { show: ['?'], keys: ['?'], action: 'help', what: 'These keys' },
    ],
  },
  {
    place: 'plot',
    title: 'On a plot',
    entries: [
      { show: ['A'], keys: ['a', 'A', '+'], action: 'addSignals', what: 'Add signals: type to filter, ↑ ↓ to choose, Enter adds' },
      { show: ['Enter'], keys: ['Enter'], action: 'enterSignals', what: 'Go into its signals' },
      { show: ['|'], keys: ['|', '\\'], action: 'splitRight', what: 'New plot to the right' },
      { show: ['−'], keys: ['-', '_'], action: 'splitDown', what: 'New plot below' },
      { show: ['Delete'], keys: ['Delete', 'Backspace'], action: 'close', what: 'Close it' },
      { show: ['F'], keys: ['f', 'F'], action: 'maximize', what: 'Maximize or restore' },
      { show: ['K'], keys: ['k', 'K'], action: 'kind', what: 'Kind and layout' },
      { show: ['←', '→'], keys: ['ArrowLeft', 'ArrowRight'], action: 'pan', what: 'Pan; with Shift, a whole window' },
      { show: ['↑', '↓'], keys: ['ArrowUp', 'ArrowDown'], action: 'zoom', what: 'Zoom time in, out' },
      { show: ['End'], keys: ['End'], action: 'follow', what: 'Follow live' },
      { show: ['Y'], keys: ['y', 'Y'], action: 'fitY', what: 'Fit y' },
      { show: ['L'], keys: ['l', 'L'], action: 'legend', what: 'Pin the legend in the corner, or unpin it' },
    ],
  },
  {
    place: 'signals',
    title: 'In its signals',
    entries: [
      { show: ['←', '→', 'Home', 'End'], keys: ['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown', 'Home', 'End'], action: 'move', what: 'Move between them' },
      { show: ['Delete'], keys: ['Delete', 'Backspace'], action: 'remove', what: 'Remove the signal' },
      { show: ['Esc'], keys: ['Escape', 'Enter'], action: 'leave', what: 'Back to the plot' },
    ],
  },
  {
    place: 'list',
    title: 'In the signal list',
    entries: [
      { show: ['↑', '↓', 'Home', 'End'], keys: ['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown', 'Home', 'End'], action: 'move', what: 'Move along it' },
      { show: ['Enter'], keys: ['Enter'], shift: false, action: 'add', what: 'Add to the focused plot' },
      { show: ['Shift', 'Enter'], keys: ['Enter'], shift: true, action: 'newPlot', what: 'New plot with it' },
    ],
  },
  {
    place: 'tabs',
    title: 'On the layout tabs',
    entries: [
      { show: ['←', '→', 'Home', 'End'], keys: ['ArrowLeft', 'ArrowRight', 'Home', 'End'], action: 'move', what: 'Move along them' },
      { show: ['Enter'], keys: ['Enter', ' '], action: 'open', what: 'Open' },
      { show: ['F2'], keys: ['F2'], action: 'rename', what: 'Rename' },
      { show: ['Delete'], keys: ['Delete'], action: 'close', what: 'Close' },
    ],
  },
  {
    place: 'mouse',
    title: 'With the mouse',
    entries: [
      { show: ['Right-click'], keys: [], what: 'A plot or tab: its menu' },
      { show: ['Drag', 'plot header'], keys: [], what: "Move the plot: onto another plot's edge to dock, its middle to swap, or a tab to move it there; Shift+drag moves a selected plot from anywhere" },
      { show: ['Drag', 'gap'], keys: [], what: 'Resize the plots beside it' },
      { show: ['Click', 'time axis'], keys: [], what: 'Cursor A, then B, which pauses; drag a cursor off the side to remove it' },
      { show: ['Drag', 'along time axis'], keys: [], what: 'Seek' },
      { show: ['Drag'], keys: [], what: 'Pan' },
      { show: ['Drag', 'y axis'], keys: [], what: 'Offset y' },
      { show: ['Wheel'], keys: [], what: 'Zoom time' },
      { show: ['Wheel', 'y axis'], keys: [], what: 'Zoom y, as Shift+wheel does' },
      { show: ['Double-click'], keys: [], what: 'Follow live' },
      { show: ['Double-click', 'y axis'], keys: [], what: 'Fit y' },
    ],
  },
];

/**
 * The action `e` names in `place`, or null. A key with Ctrl, ⌘ or Alt names
 * none, as those are the browser's.
 */
export function actionFor(place, e) {
  if (e.ctrlKey || e.metaKey || e.altKey) return null;
  const entries = KEYMAP.find((p) => p.place === place)?.entries ?? [];
  const hit = entries.find((x) => x.keys.includes(e.key) && (x.shift === undefined || x.shift === e.shiftKey));
  return hit?.action ?? null;
}

/**
 * The index a list's arrow, Home or End key moves to from `at` in a list of
 * `n`, or null for another key.
 */
export function stepIn(e, at, n) {
  const to = { ArrowLeft: at - 1, ArrowUp: at - 1, ArrowRight: at + 1, ArrowDown: at + 1, Home: 0, End: n - 1 }[e.key];
  return to === undefined ? null : Math.max(0, Math.min(n - 1, to));
}
