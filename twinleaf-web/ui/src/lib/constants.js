// The choices the page offers in more than one place.

/** The spans of raw history the settings offer, in seconds of each stream. */
export const HISTORY_SPANS = [60, 300, 600, 1800, 3600, 4 * 3600, 86400];

/** The memory the settings offer the raw history, in bytes across every stream. */
export const HISTORY_BUDGETS = [0.25, 0.5, 1, 2, 4, 8, 16, 32].map((gb) => gb * 1e9);

/** The decimations the settings offer, in points per second per trace. */
export const DECIMATIONS = [100, 500, 2000, 5000];

/**
 * Plot kinds and their names. A time plot and a spectrum show the signals
 * that stream, along time and frequency; a capture plot shows captures,
 * each along its own x. A spectrum and a capture are slices: what they
 * show has no place in time, and the next takes its place.
 */
export const KINDS = [['time', 'Time'], ['spectrum', 'Spectrum'], ['capture', 'Capture']];
/** The kinds a plot of `kind` may be: a plot of captures stays one and no other becomes one, as they show different signals (tabs.js `kindWith`). */
export const kindsFor = (kind) => KINDS.filter(([k]) => (k === 'capture') === (kind === 'capture'));
/** Whether a plot of `kind` shows slices, along an x axis of their own, and not a stretch of time. */
export const isSlice = (kind) => kind !== 'time';
/**
 * The kind `plot` is drawn as: a capture plot in its Spectrum view
 * (`captureView`, tabs.js) as a spectrum, with a spectrum's axes, cursors
 * and scales; any other plot as its kind.
 */
export const drawnKind = (plot) => (plot.kind === 'capture' && plot.captureView === 'spectrum' ? 'spectrum' : plot.kind);

/** The delays a capture plot on Auto offers between two captures, in seconds, and the one it takes until set. The hub takes 0.05 s to 10 s. */
export const CAPTURE_DELAYS = [0.1, 0.2, 0.5, 1, 2, 5];
export const CAPTURE_DELAY = 0.5;

/** Plot modes and their names; plot.js says what each draws. */
export const MODES = [['overlay', 'Overlay'], ['independent', 'Independent Y'], ['stacked', 'Stacked']];

/** A plot's kind and mode as the page names them, as "Time · Stacked". */
export const kindLabel = (kind, mode) => `${KINDS.find((k) => k[0] === kind)[1]} · ${MODES.find((m) => m[0] === mode)[1]}`;

/** The arrangements of a tab's plots, by `layout.js` preset, and their names. */
export const ARRANGEMENTS = [
  ['rows', 'Rows'],
  ['columns', 'Columns'],
  ['grid', 'Grid'],
  ['quadrants', 'Quadrants'],
  ['focus', 'One large'],
];

/**
 * How far, in px, the pointer moves from a press before it is a drag. A
 * click that wobbles less than this stays a click, and pans, moves and
 * pauses nothing.
 */
export const DRAG_START = 5;
