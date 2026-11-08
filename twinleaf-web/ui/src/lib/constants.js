// The choices the page offers in more than one place.

/** The time windows a time plot offers, in seconds. */
export const SPANS = [0.1, 0.5, 1, 2, 5, 10, 30, 60];

/** The decimations the settings offer, in points per second per trace. */
export const DECIMATIONS = [100, 500, 2000, 5000];

/** Plot kinds and their names. */
export const KINDS = [['time', 'Time'], ['spectrum', 'Spectrum']];

/** Plot modes and their names; plot.js says what each draws. */
export const MODES = [['overlay', 'Overlay'], ['independent', 'Independent y'], ['stacked', 'Stacked']];

/** A plot's kind and mode as the page names them, as "Time · Stacked". */
export const kindLabel = (kind, mode) => `${KINDS.find((k) => k[0] === kind)[1]} · ${MODES.find((m) => m[0] === mode)[1]}`;

/** The arrangements of a tab's plots, by `layout.js` preset, and their names. */
export const ARRANGEMENTS = [
  ['rows', 'Rows'],
  ['columns', 'Columns'],
  ['grid', 'Grid'],
  ['quadrants', 'Quadrants'],
  ['focus', 'Focus (one large)'],
];

/**
 * How far, in px, the pointer moves from a press before it is a drag. A
 * click that wobbles less than this stays a click, and pans, moves and
 * pauses nothing.
 */
export const DRAG_START = 5;
