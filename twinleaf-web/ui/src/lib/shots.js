// A capture's shots, and what a plot shows of them.
//
// A shot is one capture the hub read, {number, n, x0, dx, y}: the hub's
// number of it, its `n` values `y`, calibrated, in the capture's units, and
// value i at `x0 + i * dx` in its x units. A capture keeps its newest
// SHOTS_KEPT shots, oldest first. A plot shows them one of three ways
// (SHOT_MODES): the newest alone; the newest over the `count` - 1 before
// it, the older the fainter; or the mean of the newest `count`, value by
// value. A mean is of shots alike, as many values at the same x: it takes
// the run of them that ends at the newest, so a shot that differs from the
// one before starts the mean again.

/** How a plot shows a capture's shots, and their names. */
export const SHOT_MODES = [['latest', 'Latest'], ['overlay', 'Overlay'], ['average', 'Average']];
/** How many shots an overlay or a mean may take. */
export const SHOT_COUNTS = [4, 16, 64];
/** How many it takes until set. */
export const SHOT_COUNT = 16;
/** How many shots a capture keeps: the most a plot may take. */
export const SHOTS_KEPT = Math.max(...SHOT_COUNTS);
/** How opaque an overlay draws its oldest shot, and the one before its newest. */
const FAINT = [0.12, 0.5];

/** The `mode` a plot's settings name, 'latest' for none of SHOT_MODES. */
export const shotMode = (mode) => (SHOT_MODES.some(([m]) => m === mode) ? mode : 'latest');
/** The count a plot's settings name, SHOT_COUNT for none of SHOT_COUNTS. */
export const shotCount = (count) => (SHOT_COUNTS.includes(count) ? count : SHOT_COUNT);

/**
 * `elements`, a device's own, each times `calibration`, as 32-bit floats:
 * `elements` itself when it is of them already and the calibration is 1.
 */
export function calibrated(elements, calibration) {
  if (elements instanceof Float32Array && calibration === 1) return elements;
  const y = new Float32Array(elements.length);
  for (let i = 0; i < y.length; i++) y[i] = Number(elements[i]) * calibration;
  return y;
}

/** The shot of a decoded capture frame (protocol.js `decodeFrame`). */
export const shotOf = (frame) => ({ number: frame.number, n: frame.elements.length, x0: frame.x0, dx: frame.dx, y: calibrated(frame.elements, frame.calibration) });

/** Adds `shot` to `shots` as the newest, and drops the oldest past SHOTS_KEPT. */
export function keep(shots, shot) {
  shots.push(shot);
  if (shots.length > SHOTS_KEPT) shots.splice(0, shots.length - SHOTS_KEPT);
}

/** Whether shots `a` and `b` are alike: as many values, at the same x. */
export const alike = (a, b) => a.n === b.n && a.x0 === b.x0 && a.dx === b.dx;

/** The newest shots of `shots` that are alike, `count` at most, oldest first: the run that ends at the newest. */
export function run(shots, count) {
  let from = shots.length;
  while (from > 0 && shots.length - from < count && alike(shots[from - 1], shots.at(-1))) from--;
  return shots.slice(from);
}

/** The mean of `shots`, alike, value by value. */
export function mean(shots) {
  const y = new Float32Array(shots[0].n);
  for (let i = 0; i < y.length; i++) {
    let sum = 0;
    for (const shot of shots) sum += shot.y[i];
    y[i] = sum / shots.length;
  }
  return y;
}

/**
 * What a plot shows of `shots` in `mode` of `count`: {line, under, count,
 * of}, or null with no shot. `line` is the shot it draws in full, the
 * newest, or in 'average' the mean of the run that ends there, numbered as
 * the newest. `under` is what 'overlay' draws beneath it, the shots before
 * the newest, oldest first, each {shot, alpha}, the older the fainter.
 * `count` is how many shots it shows or took the mean of, and `of` how
 * many it would with that many alike: 1 of 1 in 'latest'.
 */
export function shown(shots, mode, count) {
  const newest = shots.at(-1);
  if (!newest) return null;
  if (mode === 'average') {
    const of = run(shots, count);
    return { line: of.length > 1 ? { ...newest, y: mean(of) } : newest, under: [], count: of.length, of: count };
  }
  if (mode !== 'overlay') return { line: newest, under: [], count: 1, of: 1 };
  const before = shots.slice(-count, -1);
  const alpha = (i) => (before.length < 2 ? FAINT[1] : FAINT[0] + ((FAINT[1] - FAINT[0]) * i) / (before.length - 1));
  return { line: newest, under: before.map((shot, i) => ({ shot, alpha: alpha(i) })), count: before.length + 1, of: count };
}

/** What a plot says of what `shown` gave it, beside the capture's state: "avg 5 of 16", "last 5 of 16", or nothing of one shot alone. */
export const shownText = (mode, { count, of }) => (mode === 'average' ? `avg ${count} of ${of}` : mode === 'overlay' ? `last ${count} of ${of}` : '');

const xs = new WeakMap();
/** The x of each value of `shot`, as 32-bit floats, made once a shot. */
export function xsOf(shot) {
  let x = xs.get(shot);
  if (!x) {
    x = new Float32Array(shot.n);
    for (let i = 0; i < x.length; i++) x[i] = shot.x0 + i * shot.dx;
    xs.set(shot, x);
  }
  return x;
}
