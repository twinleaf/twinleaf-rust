// A capture's shots, and what a plot shows of them.
//
// A shot is one capture the hub read, {number, n, x0, dx, y, type,
// received, spectrum}: the hub's number of it, its `n` values `y`,
// calibrated, in the capture's units, value i at `x0 + i * dx` in its x
// units, the protocol's code of the device's element type, the wall time in
// ms the page received it, as the capture carries no time, and the hub's
// estimate of its spectrum, where its x is time. A capture keeps its newest
// SHOTS_KEPT shots, oldest first. A plot shows them one of three ways
// (SHOT_MODES): the newest alone; the newest over the `count` - 1 before
// it, the older the fainter; or the mean of the newest `count`, value by
// value. A mean is of shots alike, as many values at the same x: it takes
// the run of them that ends at the newest, so a shot that differs from the
// one before starts the mean again. A plot shows them along their x or, in
// its Spectrum view, as their spectra (CAPTURE_VIEWS, `spectraShown`).

import { value } from './copy.js';
import { ELEMENT_NAMES } from './protocol.js';
import { unitOf } from './units.js';

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

/**
 * The shot of a decoded capture frame (protocol.js `decodeFrame`), which
 * the page received at wall time `received`, in ms. Its `spectrum` is the
 * frame's, the hub's estimate of its amplitude spectral density, {f, a}, in
 * Hz and in its units per √Hz, where its x is time (`secondsPer`) and the
 * hub made one; else null.
 */
export function shotOf(frame, received) {
  const s = secondsPer(frame.xUnits), bins = frame.spectrum;
  const spectrum = s && bins?.f.length ? { f: bins.f.map((f) => f / s), a: bins.a.map((a) => a * Math.sqrt(s)) } : null;
  return { number: frame.number, n: frame.elements.length, x0: frame.x0, dx: frame.dx, y: calibrated(frame.elements, frame.calibration), type: frame.type, received, spectrum };
}

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

/**
 * The spectra of what a plot in `mode` of `count` shows of `shots`, as
 * `shown` picks the shots: {line, under, count, of}, `line` and each of
 * `under` {f, a} of a shot's spectrum (`shotOf`), `under` with its `shot`
 * and `alpha`; or null where the newest has none. Of 'average', `line` is
 * the power average of the spectra of the run of shots alike, bin by bin,
 * the root of the mean of their squares, as a spectrum averages its
 * segments, and not the spectrum of their mean.
 */
export function spectraShown(shots, mode, count) {
  const s = shown(shots, mode, count), newest = shots.at(-1)?.spectrum;
  if (!s || !newest) return null;
  if (mode === 'average') {
    const of = run(shots, count).filter((shot) => shot.spectrum?.a.length === newest.a.length);
    const a = new Float32Array(newest.a.length);
    for (let i = 0; i < a.length; i++) {
      let sum = 0;
      for (const shot of of) sum += shot.spectrum.a[i] ** 2;
      a[i] = Math.sqrt(sum / of.length);
    }
    return { line: { f: newest.f, a }, under: [], count: of.length, of: count };
  }
  const under = s.under.filter((u) => u.shot.spectrum).map((u) => ({ shot: u.shot, f: u.shot.spectrum.f, a: u.shot.spectrum.a, alpha: u.alpha }));
  return { line: newest, under, count: s.count, of: s.of };
}

/** How a capture plot shows its shots: along their x, or as their spectra. */
export const CAPTURE_VIEWS = [['time', 'Time'], ['spectrum', 'Spectrum']];
/** The view a plot's settings name, 'time' for none of CAPTURE_VIEWS. */
export const captureView = (view) => (view === 'spectrum' ? 'spectrum' : 'time');

/** What a plot says of what `shown` gave it, beside the capture's state: "avg 5 of 16", "last 5 of 16", or nothing of one shot alone. */
export const shownText = (mode, { count, of }) => (mode === 'average' ? `avg ${count} of ${of}` : mode === 'overlay' ? `last ${count} of ${of}` : '');

/**
 * How many seconds one of x units `units` is, where they are seconds with
 * an SI prefix or none (units.js `unitOf`), as 1e-6 of µs; else null, for
 * an x that is no time.
 */
export function secondsPer(units) {
  const u = unitOf(units);
  return u && u.base === 's' && !u.per ? 10 ** u.e : null;
}

/**
 * The CSV of what a plot in `mode` of `count` shows of `shots`, the shots
 * of capture `capture` ({title, rpc, meta}) of the device named `device`;
 * null with no shot. A header of lines after '#' says what it is of: the
 * capture, its RPC and its device; its sample count, its element type, its
 * units and, where its x is time, its sample rate, one over its x step;
 * how the plot shows its shots; and when the page received each shot it
 * holds. Then a row per value: its x, in seconds where the capture's x is
 * time (`secondsPer`), else in its x units, and a column per shot shown,
 * oldest first, as the plot draws them: the newest alone, each of an
 * overlay, or their mean, in the capture's units.
 */
export function shotsCsv(capture, shots, mode, count, device) {
  const s = shown(shots, mode, count);
  if (!s) return null;
  const { title, rpc, meta } = capture, newest = s.line, seconds = secondsPer(meta.xUnits);
  const took = mode === 'average' ? run(shots, count) : mode === 'overlay' ? [...s.under.map((u) => u.shot), newest] : [newest];
  const columns = mode === 'average' ? [{ name: `${title} average of ${took.length}`, y: newest.y }] : took.map((shot) => ({ name: `${title} shot ${shot.number}`, y: shot.y }));
  const shows = { latest: 'the newest shot', overlay: `the last ${took.length} shots of ${count}, overlaid`, average: `the average of the last ${took.length} shots alike of ${count}` }[mode];
  const units = meta.units ? ` (${meta.units})` : '';
  const head = [
    `# ${title}, capture RPC ${rpc}${device ? ` of ${device}` : ''}`,
    `# ${newest.n} samples of ${ELEMENT_NAMES[newest.type] ?? 'unknown type'}${meta.units ? `, in ${meta.units}` : ''}${seconds ? `, sample rate ${+(1 / (newest.dx * seconds)).toPrecision(6)} Hz` : ''}`,
    `# shows ${shows}`,
    ...took.map((shot) => `# shot ${shot.number} received ${new Date(shot.received).toISOString()}`),
    [seconds ? 'time (s)' : `${meta.xName || 'x'}${meta.xUnits ? ` (${meta.xUnits})` : ''}`, ...columns.map((c) => `${c.name}${units}`)].map(field).join(','),
  ];
  const rows = Array.from({ length: newest.n }, (_, i) => [value((newest.x0 + i * newest.dx) * (seconds ?? 1)), ...columns.map((c) => value(c.y[i]))].join(','));
  return [...head, ...rows].join('\n') + '\n';
}

/** `text` as a CSV field: quoted where it holds a comma, a quote or a line break. */
const field = (text) => (/[",\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text);

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
