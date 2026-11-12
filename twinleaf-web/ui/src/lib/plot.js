// One plot: turns its settings into lanes, y ranges and draw calls on the
// shared renderer, and draws its own axes, gaps, cursor and legend on a 2D
// canvas above the WebGL one.
//
// Modes, and their lanes, as `lanesOf` counts them:
//   overlay      every trace on one lane and one y axis
//   independent  one lane, each trace with its own y range (scope style),
//                and up to two axes on each side, one per trace, which
//                `sides` in the settings places (axisSlots)
//   stacked      one lane per trace, sharing x
// Kinds: time, each plot with a View of its own, which a gesture on a
// selected plot moves for every selected plot; or a slice, drawn by one
// path (`drawSlice`): a spectrum, each signal's newest estimate along
// frequency, its axes log or linear; or a capture, each capture's shots
// (shots.js) along the x its device gives them, its axes linear. A slice
// is {f, a, floor}, its x and y values and a spectrum's noise floor; a
// capture's holds `under` too, the shots an overlay draws beneath it, and
// `note`, what it says of how many shots it shows.

import { scaleLinear, scaleLog, scaleTime } from 'd3-scale';
import { DRAG_START, isSlice } from './constants.js';
import { prefs } from './prefs.svelte.js';
import { axisFormat, fixedUnit, formOf, numberText, rangeText, si, spanText, tickText } from './units.js';
import { shotCount, shotMode, shown, shownText, xsOf } from './shots.js';
import { WINDOW_LIMITS, windowText } from './window.js';
import { placedIn, withX, withY, withoutX, withoutY, xOf, yOf } from './cursors.js';
import * as clock from './clock.js';
import { aimed, eased, LINE_PX, STILL, turn } from './wheelzoom.js';
import { pickSnap, SNAP, snapCandidates, STAND } from './snap.js';
import { watchShift } from './keys.js';

/** A CSS colour as [r, g, b, a] in 0..1, resolved by the browser, so theme tokens work. */
function cssRgba(css) {
  const c = document.createElement('canvas');
  c.width = c.height = 1;
  const ctx = c.getContext('2d');
  ctx.fillStyle = css;
  ctx.fillRect(0, 0, 1, 1);
  const [r, g, b, a] = ctx.getImageData(0, 0, 1, 1).data;
  return [r / 255, g / 255, b / 255, a / 255];
}
const LANE_GAP = 6;
/**
 * The least room, in px, between two of a y axis's numbers, between a lane's
 * numbers and the next lane's, and about the tabs of the Y cursors resting
 * in a lane.
 */
const CLEAR = 2;
/** How far past its lane, in px, a lane's numbers may reach into the gap to the next lane: half the gap, less half the clearance. */
const LABEL_REACH = (LANE_GAP - CLEAR) / 2;
/**
 * How far past a plot's top and foot, in px, its y numbers may reach, so a
 * number at the very end of the range is drawn whole: as far as a number
 * reaches past its tick, as `laneTicks` measures it, 5 px above and 7 below,
 * or further. The body keeps this room above the plot, and the time axis
 * begins CLEAR px below it.
 */
const EDGE = { above: 6, below: 7 };
/** How far below a plot's foot, in px, its time axis's band begins: its tabs and readout, 3 px into the band, clear the y numbers at the foot by CLEAR px. */
const AXIS_BELOW = EDGE.below + CLEAR - 3;
/** Of a y axis's ticks, every how many it numbers when it cannot number each one: so the numbers it keeps stay round. */
const LABEL_EVERY = [1, 2, 5, 10, 20, 50, 100];

/**
 * The ticks of a y axis over `lane`, {y, h}, with scale `sy` (value to
 * client y), and those it writes numbers at, by the lane's height. The grid
 * is at every tick. A lane numbers every kth tick whose number fits, for
 * the least k of 1, 2, 5, 10 and so on that gives two or more CLEAR px
 * apart, and where no k does it numbers none. Whether it numbers and which
 * k it takes are decided as between two lanes, a number reaching
 * LABEL_REACH px past the lane's top and foot at most, or less where
 * `reach` is less; then the lane numbers each kth tick whose number lies
 * within `reach.above` and `reach.below` px of it, so lanes of one height
 * and range number alike and a plot's top and foot add their end numbers.
 * A tick is numbered only where `numbered(value)` holds, as a log axis
 * numbers its decades. `count` is the tick count asked of the scale, for
 * its format.
 *
 * It measures from where a tick falls, not the pixel it is drawn on, which
 * is up to 1 px lower: so what a lane shows changes only one way as the
 * lane grows or shrinks, whatever pixel the lane starts on.
 */
export function laneTicks(sy, lane, numbered = () => true, reach = { above: LABEL_REACH, below: LABEL_REACH }) {
  const count = Math.max(3, lane.h / 40), grid = sy.ticks(count);
  const within = (r) => (v) => numbered(v) && sy(v) + 4 - LABEL.up >= lane.y - r.above && sy(v) + 1 + 4 + LABEL.down <= lane.y + lane.h + r.below;
  const fits = within({ above: Math.min(LABEL_REACH, reach.above), below: Math.min(LABEL_REACH, reach.below) }), reaches = within(reach);
  const room = LABEL.up + LABEL.down + CLEAR + 1;
  const spaced = (vs) => vs.every((v, i) => i === 0 || Math.abs(sy(v) - sy(vs[i - 1])) >= room);
  // The kth tick from zero, on a linear axis's even steps, or from the first numbered one. A linear
  // axis steps by 1, 2 or 5 times a power of ten, which the difference of two ticks far from zero
  // gives only roughly, so the step is that difference rounded to one significant digit.
  const gap = grid.length > 1 ? grid[1] - grid[0] : 1, unit = 10 ** Math.floor(Math.log10(Math.abs(gap)));
  const step = Math.round(gap / unit) * unit;
  const even = grid.every((v, i) => i === 0 || Math.abs(v - grid[i - 1] - step) <= Math.abs(step) * 1e-6 + Math.abs(v) * 1e-15);
  const kth = (v, i, k) => (even ? Math.round(v / step) : i) % k === 0;
  const candidates = grid.filter(numbered);
  for (const k of LABEL_EVERY) {
    const labels = candidates.filter((v, i) => kth(v, i, k) && fits(v));
    if (labels.length >= 2 && spaced(labels)) return { grid, labels: candidates.filter((v, i) => kth(v, i, k) && reaches(v)), count };
    if (labels.length < 2 && k > 1 && candidates.filter((v, i) => kth(v, i, k)).length < 2) break;
  }
  return { grid, labels: [], count };
}

/** How many y axes an independent plot shows on each side. */
export const AXES_PER_SIDE = 2;

/**
 * Which signals an independent plot's axes show, as {left, right}, each the
 * signal keys from the plot line outward. `sides` maps a key to 'left',
 * 'right' or 'none'; a signal not in it takes the next side in turn, left,
 * right, left, right, and none after four. A side already full shows no more.
 */
export function axisSlots(keys, sides = {}) {
  const out = { left: [], right: [] };
  keys.forEach((k, i) => {
    const side = sides[k] ?? (i < 2 * AXES_PER_SIDE ? (i % 2 ? 'right' : 'left') : 'none');
    if (out[side] && out[side].length < AXES_PER_SIDE) out[side].push(k);
  });
  return out;
}

/**
 * How a time axis reads, by the timescale of its first trace's time: Unix
 * seconds as the time of day, any other in the source's own seconds. Each
 * reads the same from visit to visit, as the time is the device's.
 * `ticks(x0, x1, n)` gives about n tick times, `label(t)` a tick's label,
 * and `readout(t)` a time to the millisecond. With an `origin`, any
 * timescale reads as the seconds from that time.
 */
export function timeAxis(epoch, origin = null) {
  if (origin !== null) {
    return {
      ticks: (x0, x1, n) => scaleLinear().domain([x0 - origin, x1 - origin]).ticks(n).map((v) => v + origin),
      label: (t) => `${+(t - origin).toFixed(6)} s`,
      readout: (t) => `${(t - origin).toFixed(3)} s`,
    };
  }
  if (epoch === 'unix') {
    // To the nearest millisecond: seconds times 1000 can land a hair under a
    // whole millisecond, which a Date would cut down to the one before.
    const date = (t) => new Date(Math.round(t * 1000));
    const two = (n) => String(n).padStart(2, '0');
    const clock = (d) => `${two(d.getHours())}:${two(d.getMinutes())}:${two(d.getSeconds())}`;
    return {
      ticks: (x0, x1, n) => scaleTime().domain([date(x0), date(x1)]).ticks(n).map((d) => d.getTime() / 1000),
      // A tick names only what changed at it, on a 24-hour clock: the hour
      // and minute on the minute, the seconds within one, the milliseconds
      // within a second.
      label(t) {
        const d = date(t), ms = d.getMilliseconds(), sec = d.getSeconds();
        if (ms) return `.${String(ms).padStart(3, '0')}`;
        if (sec) return `:${two(sec)}`;
        return `${two(d.getHours())}:${two(d.getMinutes())}`;
      },
      readout(t) {
        const d = date(t);
        return `${clock(d)}.${String(d.getMilliseconds()).padStart(3, '0')}`;
      },
    };
  }
  return {
    ticks: (x0, x1, n) => scaleLinear().domain([x0, x1]).ticks(n),
    label: (t) => `${+t.toFixed(6)} s`,
    readout: (t) => `${t.toFixed(3)} s`,
  };
}

/**
 * How a menu names the X cursors of a plot, by the kind of its x axis:
 * 'unix', any other epoch, 'hz' for a spectrum's frequencies, or {unit}
 * for a capture's x in that unit, which reads as frequencies do.
 * `at(x, span)` names a cursor at x of a pair `span` apart, to as many
 * decimals as tell the pair apart: whole seconds for a span of 2 s or more,
 * milliseconds from 2 ms, microseconds below. A Unix time is the time of
 * day, with its date when that is not today's or the pair runs past the
 * day; another clock's seconds have their digits grouped. `span(s)` names
 * the distance between them: in seconds with an SI prefix below a minute,
 * then in minutes, hours or days and the next unit down. Frequencies take
 * an SI prefix throughout.
 */
export function cursorText(kind) {
  const unit = kind === 'hz' ? 'Hz' : kind?.unit;
  if (unit !== undefined) return { at: (f) => si(f, unit), span: (s) => si(s, unit) };
  const decimals = (span) => (span >= 2 ? 0 : span >= 2e-3 ? 3 : 6);
  /** `t` to `d` decimals, as its whole seconds and the digits after the point. */
  const split = (t, d) => {
    const [whole, frac = ''] = t.toFixed(d).split('.');
    return [Number(whole), frac];
  };
  const two = (n) => String(n).padStart(2, '0');
  const span = (s) => {
    s = Math.abs(s);
    if (s < 60) return si(s, 's');
    if (s < 3600) return `${Math.floor(s / 60)} min ${two(Math.floor(s % 60))} s`;
    if (s < 86400) return `${Math.floor(s / 3600)} h ${two(Math.floor((s % 3600) / 60))} min`;
    return `${Math.floor(s / 86400)} d ${two(Math.floor((s % 86400) / 3600))} h`;
  };
  if (kind === 'unix') {
    const day = (t) => new Date(t * 1000).toDateString();
    return {
      at(t, between) {
        const [whole, frac] = split(t, decimals(between));
        const d = new Date(whole * 1000);
        const hms = `${two(d.getHours())}:${two(d.getMinutes())}:${two(d.getSeconds())}${frac ? `.${frac}` : ''}`;
        const dated = day(t) !== day(clock.wall() / 1000) || day(t) !== day(t + between);
        return dated ? `${d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short' })} ${hms}` : hms;
      },
      span,
    };
  }
  return {
    at(t, between) {
      const [whole, frac] = split(t, decimals(between));
      const grouped = String(Math.abs(whole)).replace(/\B(?=(\d{3})+(?!\d))/g, ' ');
      return `${t < 0 ? '−' : ''}${grouped}${frac ? `.${frac}` : ''} s`;
    },
    span,
  };
}

/** How many lanes a plot of `n` signals in `mode` has. */
export const lanesOf = (mode, n) => (mode === 'stacked' ? Math.max(1, n) : 1);

// A plot smaller than these, in px, is a monitor: too narrow or too short
// for its header, or a lane too short to read.
const MONITOR = { width: 360, height: 160, lane: 100 };

/**
 * Whether a plot of `size` {width, height} showing `n` traces in `mode` is a
 * monitor: the lines fill it, read by their colours, and its header shows
 * over it only while the pointer is on it or it has focus.
 */
export const monitorOf = (size, mode, n) =>
  size.width < MONITOR.width || size.height < MONITOR.height || size.height / lanesOf(mode, n) < MONITOR.lane;
// The fraction across a live plot past which zooming in stays live, at the
// least: the newest stretch, where a zoom is to resize the window, not to
// hold a moment. A plot with a header reaches further, to below its actions.
const LIVE_EDGE = 0.85;
/** How far left of the header's actions, in px, that stretch begins. */
const LIVE_EDGE_BUFFER = 62;
/**
 * How often the hover's numbers read again while the pointer holds still,
 * in ms. A live plot scrolls under a still pointer, and numbers that changed
 * every frame could not be read, so they hold between reads, as a meter's
 * display does. The marker on the line follows every frame, and moving the
 * pointer reads at once.
 */
const READ_MS = 250;
/** How fast the live edge's wash and LIVE line come in and go, as the time constant of their fade, in ms. */
const LIVE_FADE_MS = { in: 60, out: 100 };
/** How long a time plot shows its window on the time axis after the window changes, in ms. */
const WINDOW_SHOWN_MS = 1200;
const reducedMotion = typeof matchMedia === 'undefined' ? null : matchMedia('(prefers-reduced-motion: reduce)');
/**
 * The fewest points a pixel a plot draws a trace with, while a level that
 * fine exists. A coarser level gives about a quarter as many, and moves a
 * line by under half a pixel at the switch from this many.
 */
export const LEVEL_FLOOR = 4;
/**
 * The oldest time a view can show: `history` seconds, the hub's History
 * setting, before `time`, the view's time, or `oldest`, the oldest time the
 * hub holds of the plot's signals, whichever is later. A view's time is
 * where it ends zoomed all the way out: the newest samples less the display
 * delay while live, the moment of the pause while paused. So a view is as
 * wide as the setting at most, and reads as the setting does once the hub
 * holds that long: the hub keeps more than the setting by a headroom
 * (twinleaf-web/src/hub.rs HISTORY_HEADROOM), so what it holds reaches the
 * setting before any view no further than that behind its newest sample.
 * A paused view is a snapshot of what could be shown at its pause, less
 * what is dropped since: `atPause`, the History setting at its pause, or
 * null for a live view, limits it too, so the smaller of the two settings
 * counts. A setting raised since leaves a paused view as wide as it was,
 * with none of the headroom, and one lowered since moves its limit in.
 * It is `oldest` while the hub has said neither setting, and null when
 * nothing says what is held. The zoom limit, the history bar, Back and a
 * plot an undo brings back all read it from `View.oldest`.
 */
export function oldestShown(oldest, time, history, atPause = null) {
  const seconds = Math.min(history || Infinity, atPause || Infinity);
  return oldest === null || seconds === Infinity ? oldest : Math.max(oldest, time - seconds);
}
/**
 * How far past the floor a coarser level must reach before a plot moves to
 * it, as a factor. A plot then moves back to the finer level only once the
 * coarser one falls under the floor, so a zoom that rests near a switch
 * does not flip between the two.
 */
export const LEVEL_BAND = 1.25;

/**
 * Range [x0, x1] moved back onto the device pixel grid of a plot `width`
 * device px wide: its start at the whole multiple of a pixel's time at or
 * before x0, counted from time zero, its span kept. A live view's range
 * moves by fractions of a pixel each frame, which samples a fast signal
 * at a new phase in every column, so columns whose data did not change
 * flicker. On the grid each frame moves every point by whole pixels, and
 * such a column draws the same from frame to frame.
 */
export function onPixelGrid(x0, x1, width) {
  const tau = (x1 - x0) / width;
  if (!(tau > 0) || !Number.isFinite(tau)) return [x0, x1];
  const start = Math.floor(x0 / tau) * tau;
  return [start, start + (x1 - x0)];
}

/**
 * The level of `levels`, finest first, that a plot `width` px wide draws
 * [x0, x1] from, given the level it drew last, `was`: the coarsest with at
 * least LEVEL_FLOOR points a pixel, held within LEVEL_BAND. A level that
 * does not reach back to x0, while a coarser one holds points before it,
 * gives way to the coarser one.
 */
export function pickLevel(levels, x0, x1, width, was = 0) {
  const count = (k) => levels[k].lowerBound(x1) - levels[k].lowerBound(x0);
  const reaches = (k) => levels[k].length > 0 && levels[k].xAt(levels[k].absStart) <= x0;
  let k = Math.min(Math.max(was, 0), levels.length - 1);
  while (k + 1 < levels.length && count(k + 1) >= LEVEL_FLOOR * LEVEL_BAND * width) k++;
  while (k > 0 && count(k) < LEVEL_FLOOR * width) k--;
  while (k + 1 < levels.length && !reaches(k) && levels[k + 1].length && levels[k + 1].xAt(levels[k + 1].absStart) < levels[k].xAt(levels[k].absStart)) k++;
  return k;
}

/** The trace's point nearest time t, or null outside the points it keeps. */
function traceAt(t, x) {
  if (t.length === 0 || x < t.xAt(t.absStart) || x > t.xAt(t.absEnd - 1)) return null;
  let k = Math.min(Math.max(t.lowerBound(x), t.absStart), t.absEnd - 1);
  if (k > t.absStart && x - t.xAt(k - 1) < t.xAt(k) - x) k--;
  return { x: t.xAt(k), y: t.yAt(k) };
}

/**
 * The lowest and highest of the trace's points in [x0, x1], as {lo, hi}, or
 * null when it keeps fewer than two there.
 */
function traceSpan(t, x0, x1) {
  if (t.length === 0) return null;
  let lo = Infinity, hi = -Infinity, n = 0;
  for (let k = Math.max(t.lowerBound(x0), t.absStart); k < t.absEnd && t.xAt(k) <= x1; k++, n++) {
    const y = t.yAt(k);
    if (y < lo) lo = y;
    if (y > hi) hi = y;
  }
  return n > 1 ? { lo, hi } : null;
}

// Over a y axis, the axis's gutter shows how often each signal in the lane
// is at each height in view, as a scope's intensity grading does: a column
// per signal, each row as opaque as the share of samples there. It is drawn
// to be easy on the eye rather than exact: blurred over a few rows, and
// eased toward each frame's counts.

/** The opacity of the faintest row shown, which the cube root of a row's share lifts sparse rows toward. */
const STRIP_MIN_ALPHA = 40 / 255;
/** The most samples a strip counts; past it, every k-th, evenly through the window. */
const MAX_BINNED = 100_000;
/**
 * The axis's columns: each `w` px wide, `step` px further out than the one
 * before, and its fullest row `alpha` opaque, which leaves the axis's
 * labels readable over them. Rows blur with a Gaussian `blur` px wide, and
 * each frame moves a row `ease` of the way to its new share.
 */
const AXIS_STRIP = { w: 8, step: 4, alpha: 0.55, blur: 3, ease: 0.25 };
/** Below this share a row is left clear, so the blur's long tails draw nothing. */
const STRIP_FLOOR = 0.01;

/**
 * `counts` blurred by a Gaussian `sigma` rows wide, and scaled so its
 * fullest row is 1.
 */
export function blurred(counts, sigma) {
  const r = Math.ceil(3 * sigma);
  const kernel = Array.from({ length: 2 * r + 1 }, (_, i) => Math.exp(-((i - r) ** 2) / (2 * sigma * sigma)));
  const out = new Float32Array(counts.length);
  let max = 0;
  for (let i = 0; i < counts.length; i++) {
    let v = 0;
    for (let k = -r; k <= r; k++) {
      const j = i + k;
      if (j >= 0 && j < counts.length) v += counts[j] * kernel[k + r];
    }
    out[i] = v;
    if (v > max) max = v;
  }
  if (max > 0) for (let i = 0; i < out.length; i++) out[i] /= max;
  return out;
}

/**
 * How many of trace t's points in [x0, x1) fall in each of `rows` rows, a
 * point of value y in row `rowOf(y)`, as {counts, max}. Points outside the
 * rows are not counted. Null when the window holds fewer than two points,
 * or none in the rows.
 */
export function rowCounts(t, x0, x1, rowOf, rows) {
  if (t.length === 0 || rows < 1) return null;
  const a0 = Math.max(t.lowerBound(x0), t.absStart), a1 = Math.min(t.lowerBound(x1), t.absEnd);
  if (a1 - a0 < 2) return null;
  const step = Math.ceil((a1 - a0) / MAX_BINNED);
  const counts = new Uint32Array(rows);
  let max = 0;
  for (let k = a0; k < a1; k += step) {
    const r = rowOf(t.yAt(k));
    if (r >= 0 && r < rows && ++counts[r] > max) max = counts[r];
  }
  return max ? { counts, max } : null;
}

/** Draws shares `share`, 0 to 1 a row, as a strip `w` px wide from x, a row per px down from `top`, the fullest row `alpha` opaque. */
function drawStrip(ctx, share, color, x, w, top, alpha) {
  ctx.fillStyle = color;
  for (let r = 0; r < share.length; r++) {
    if (share[r] < STRIP_FLOOR) continue;
    ctx.globalAlpha = alpha * (STRIP_MIN_ALPHA + (1 - STRIP_MIN_ALPHA) * Math.cbrt(share[r]));
    ctx.fillRect(x, top + r, w, 1);
  }
  ctx.globalAlpha = 1;
}

/**
 * What a plot `width` px wide draws [x0, x1] of a trace from, of `held`:
 * the windows of the hub's history and the level of its points that the
 * page holds, in the order a tie goes by. It is the one whose points span
 * the most of the view's pixel columns; of those that span as many, the one
 * with the most points across the view, where a window of the raw rows has
 * the most there are; and of those, the first.
 */
export function pickSource(held, x0, x1, width) {
  const column = (x1 - x0) / width;
  const score = (s) => {
    if (s.length === 0) return [0, 0];
    const spanned = Math.min(s.xAt(s.absEnd - 1), x1) - Math.max(s.xAt(s.absStart), x0);
    return [Math.round(Math.max(0, spanned) / column), s.ratio === 1 ? Infinity : s.lowerBound(x1) - s.lowerBound(x0)];
  };
  let best = held[0], most = score(best);
  for (const s of held.slice(1)) {
    const [columns, points] = score(s);
    if (columns > most[0] || (columns === most[0] && points > most[1])) [best, most] = [s, [columns, points]];
  }
  return best;
}

/** The spectrum's bin nearest frequency f. */
function spectrumAt(s, f) {
  if (!s || f < s.f[0] || f > s.f.at(-1)) return null;
  let lo = 0, hi = s.f.length - 1;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (s.f[mid] < f) lo = mid + 1;
    else hi = mid;
  }
  if (lo > 0 && f - s.f[lo - 1] < s.f[lo] - f) lo--;
  return { x: s.f[lo], y: s.a[lo] };
}

/** A tick on a linear frequency axis, as few digits as it needs: 500 Hz, 1.5 kHz. */
const hzLabel = (f) => (Math.abs(f) >= 1000 ? `${+(f / 1000).toPrecision(4)} kHz` : `${+f.toPrecision(4)} Hz`);

/**
 * How each place a plot writes a value of a y axis writes it on Auto
 * (units.js `formOf`), a time plot's and a spectrum's: `tag` the pointer's
 * tag on the axis; `value` the cursors' tags and readout and the hover's
 * tip, a time plot's in the signal's units with an SI prefix; `legend` the
 * legend's value; `floor` a spectrum's noise floor. A spectrum's values
 * keep their exponent and their units apart, being per √Hz.
 */
export const Y_AUTO = {
  tag: { time: { digits: 4, notation: 'fig' }, spectrum: { digits: 3, notation: 'exp' } },
  value: { time: { digits: 4, notation: 'si' }, spectrum: { digits: 4, notation: 'exp' } },
  legend: { time: { digits: 5, notation: 'fig' }, spectrum: { digits: 4, notation: 'exp' } },
  floor: { spectrum: { digits: 3, notation: 'exp' } },
};
/** The units of a y axis whose signals are in `units`, as `PlotView.axisUnits` names them: each once and in order, a spectrum's per √Hz. */
const unitsOf = (units, spectrum) => [...new Set(units.map((u) => (spectrum ? `${u}/√Hz` : u)))].sort().join(' ');

/**
 * The unit signal `key` is shown in on a plot of settings `cfg`, {kind,
 * mode, keys, yFormat}, `keys` the signals it draws, where `unitOf(k)`
 * is each signal's reported unit: the unit its y axis fixes (units.js
 * `fixedUnit`), or else its reported unit, a spectrum's per √Hz; '' for a
 * signal with none. Everything within a plot that names a signal's unit
 * names this one.
 */
export function shownUnit(cfg, key, unitOf) {
  const u = unitOf(key);
  if (!u) return '';
  const spectrum = cfg.kind === 'spectrum', overlay = cfg.mode === 'overlay';
  const fixed = fixedUnit(axisFormat(cfg.yFormat?.[overlay ? 'shared' : key]), unitsOf((overlay ? cfg.keys : [key]).map(unitOf), spectrum));
  return fixed ? fixed.text : spectrum ? `${u}/√Hz` : u;
}
/** How a spectrum's x axis writes a frequency on Auto: a cursor's place, and the distance between two. */
export const F_AUTO = { at: { digits: 4, notation: 'fig' }, span: { digits: 4, notation: 'si' } };
/** How a slice's x axis writes a value on Auto, by the plot's kind, as F_AUTO lays it out: a capture's with the SI prefix its size gives it. */
export const X_AUTO = { spectrum: F_AUTO, capture: { at: { digits: 4, notation: 'si' }, span: { digits: 4, notation: 'si' } } };
/** How a tick of a slice's linear x axis in `unit` reads on Auto: a frequency as `hzLabel`, any other as units.js `tickText`. */
const xTickText = (unit) => (unit === 'Hz' ? hzLabel : (v) => tickText(v, unit));

/**
 * The text of each of a y axis's numbered `ticks`, or of a slice's x
 * axis's in unit `x`, in axis format `f` (units.js `axisFormat`).
 * On Auto they read as `auto(v)`: the scale's own tick format, a log
 * axis's decades as 1e+3, or `xTickText`. Else they read in f's notation, or
 * with Auto in that of `auto`'s place, plain on a linear y axis, an
 * exponent on a log one and an SI prefix on an x axis, to f's digits, or with Auto
 * the fewest; and with more where fewer would write two ticks alike. Plain
 * numbers on a linear axis, `log` false, take the decimals of the largest.
 * An axis in `units` whose format fixes a unit (units.js `fixedUnit`)
 * writes its ticks in that unit, Auto or not.
 */
export function tickTexts(ticks, f, auto, { log = false, x = null, units = '' } = {}) {
  const fixed = fixedUnit(f, x ?? units);
  if (f.digits === null && f.notation === 'auto' && !fixed) return ticks.map(auto);
  const notation = f.notation !== 'auto' ? f.notation : x !== null ? 'si' : log ? 'exp' : 'plain';
  const of = Math.max(0, ...ticks.map(Math.abs));
  for (let digits = f.digits ?? 1; ; digits++) {
    const texts = ticks.map((v) => numberText(v, { digits, notation, fixed }, x ?? '', log ? v : of || v));
    if (digits >= 17 || new Set(texts).size === texts.length) return texts;
  }
}

/** What a plot names trace `t`: a capture's title, a signal's key. */
export const titleOf = (t) => t.title ?? t.key;

/**
 * The plot's colours, read from the theme's tokens in app.css. They are the
 * background, also as GL's rgba, the axis text, grid, frame, gap marks,
 * crosshair, the span between cursors, the text on tags, the cursors, the
 * seek strip and the traces, each trace also as GL's rgba. They are kept
 * once every token has a value. `dark` is the look, dark or light, and
 * by default the page's (prefs.svelte.js), which a reader in an effect or
 * a template follows; the other look's tokens are those of an element of
 * class `dark` or `light` (app.css). A canvas ignores an empty colour and
 * keeps the last one drawn with, so a token the page has not loaded yet is
 * read again on the next frame.
 */
const themed = new Map();
export function theme(dark = prefs.dark) {
  if (!themed.has(dark)) {
    let probe = null;
    if (dark !== prefs.dark) {
      probe = document.body.appendChild(document.createElement('div'));
      probe.className = dark ? 'dark' : 'light';
      probe.hidden = true;
    }
    const style = getComputedStyle(probe ?? document.documentElement);
    let missing = false;
    const token = (name) => {
      const value = style.getPropertyValue(name).trim();
      if (!value) missing = true;
      return value;
    };
    const traces = [1, 2, 3, 4, 5, 6, 7, 8].map((i) => token(`--trace-${i}`));
    const read = {
      dark,
      bg: token('--plot-bg'),
      axisText: token('--plot-axis-text'),
      text: token('--foreground'),
      grid: token('--plot-grid'),
      frame: token('--plot-frame'),
      gap: token('--plot-gap'),
      crosshair: token('--plot-crosshair'),
      span: token('--plot-span'),
      tagText: token('--plot-tag-text'),
      tipBg: token('--popover'),
      tipText: token('--popover-foreground'),
      tipBorder: token('--border'),
      cursorA: token('--cursor-a'),
      cursorB: token('--cursor-b'),
      dataFont: token('--font-data'),
      seekBg: token('--plot-seek-bg'),
      seekWindow: token('--plot-seek-window'),
      seekEdge: token('--plot-seek-edge'),
      live: token('--primary'),
      liveText: token('--primary-foreground'),
      // The live edge's wash: the plot's foreground, as an rgb triple to give an alpha.
      wash: dark ? '255 255 255' : '0 0 0',
      traces,
      tracesRgba: traces.map(cssRgba),
    };
    read.bgRgba = cssRgba(read.bg);
    probe?.remove();
    if (missing) return read;
    themed.set(dark, read);
  }
  return themed.get(dark);
}
/** Each picked colour other than the theme's, as GL's rgba. */
const customRgba = new Map();
/** The trace's colour among `from`, a Colors (colors.svelte.js), as it gives it or it was picked, in the look its colours are drawn in. */
export const colorOf = (trace, from) => {
  const c = from.of(trace.key);
  return typeof c === 'number' ? theme(from.dark ?? undefined).traces[c] : c;
};
/** The trace's colour among `from`, a Colors, as GL's rgba. */
const rgbaOf = (trace, from) => {
  const c = from.of(trace.key);
  if (typeof c === 'number') return theme(from.dark ?? undefined).tracesRgba[c];
  if (!customRgba.has(c)) customRgba.set(c, cssRgba(c));
  return customRgba.get(c);
};

export class PlotView {
  /**
   * `hub` holds the session whose traces it draws. `view` is this plot's own
   * time view, which it draws. `targets` returns
   * the views a gesture on this plot acts on: every selected plot's, when
   * this one is selected, or else its own. `hoverTime` holds the time under the
   * pointer, which every time plot draws a line at. `viewHistory` is the
   * page's ViewHistory: each drag, burst of wheel turns and command here
   * that moves the views is a step of it. `cursors` is the page's
   * CursorEdits, which holds the cursors the plot draws and writes those a
   * drag here moves, on what `cursorTargets(key)` returns: the PlotViews,
   * each as [pv, axis], that a drag of an X cursor, with no `key`, or of a Y
   * cursor on y axis `key` moves (commands.js `cursorPeers`), this one among
   * them. `onFollow()` has the page follow live on the plots of `targets`,
   * back at the windows they had before they paused. `renderer` is the
   * Renderer of the canvas its traces are drawn on (gl.js), whose pixel
   * ratio its overlay is drawn at too. `colors` is the Colors its signals'
   * colours are read from.
   */
  constructor({ body, overlay, config, hub, view, targets, cursorTargets, hoverTime, plotId, renderer, onFocus, onFrequencies, onFitY, onFollow, viewHistory, cursors, colors }) {
    cursorTargets ??= (key) => [[this, key]];
    Object.assign(this, { body, overlay, config, hub, view, targets, cursorTargets, hoverTime, plotId, renderer, onFocus, onFrequencies, onFitY, onFollow, viewHistory, cursors, colors });
    this.zoomBand = null; // [x0, x1] in client px while a Ctrl+drag marks a range to zoom to
    this.ctx = overlay.getContext('2d');
    this.specHover = null;
    this.yAxes = []; // the y axes, as last drawn: {key, lane, range, side, slot, x0, x1, color, plans}
    /**
     * What the plot last drew that a press lands on, each {part, x, y, w, h}
     * in client px, in the order drawn: the last one at a point is on top
     * there and takes the press (`pressAt`). The parts:
     * - 'plot', the plot area; 'xaxis', the band under it; 'yaxis', the
     *   band of a y axis, `band` being {side, slot, x0, x1, keys};
     * - a cursor's, with `kind` 'x' or 'y', `name` 'a' or 'b', and of a Y
     *   cursor `key` and `axis`, its y axis: 'line', the box about its
     *   line, with the `runs` drawn of it; 'tag', its name or its value,
     *   with `text` and `fill`, or with `edge` its tag at the plot's edge
     *   while it is out of view; 'tab', where it rests while not placed,
     *   with `alpha`, how plainly it shows. A line and a tag of a cursor in
     *   view hold `at`, where the cursor is along its axis, in client px.
     * - what a press goes through to what is under, each with `through`:
     *   'snap', the mark on what a cursor dragged with Shift snapped to, a
     *   ring centred at (`cx`, `cy`) or else an outline of its box, with
     *   `name` and `fill` its cursor's, drawn over the cursors' lines and
     *   under their tags; and what follows the pointer, drawn over the
     *   rest: 'pointer', the pointer's value on a y axis, with that axis's
     *   `key`; 'tip', the hover's tip; each with the `value` it writes.
     */
    this.pressable = [];
    this.snapped = null; // what the cursor being dragged with Shift snapped to, see `snapAt`, with the cursor's `kind`, `name` and y `axisKey`
    this.drawnOn = null; // where the plot's body was as it last drew, {x, y, w, h} in client px
    this.tagsParted = {}; // the pairs of tags that are on their outer sides, by pair, see `parted`
    this.tagsPlaced = [];
    this.tickLabels = []; // each tick label as last drawn, {text, x, y, w, h}, its box in client px, and a y label's axis, its range key
    this.labelBoxes = []; // each y axis's label as last drawn, {axis, text, x, y, w, h}
    this.gaps = []; // each gap in a trace as last drawn, {index, x0, x1, band}: the trace's place in the plot, and the px the gap runs from and to, one place unless it is a band
    this.cursorStubs = []; // each cursor line's run past the plot's edge into the axis, {x0, y0, x1, y1}
    this.axisHover = null; // clientX over the x axis, where a drag seeks
    this.phosphor = new Map(); // signal key -> the shares its axis column shows, eased frame to frame
    this.drawnLevel = new Map(); // trace -> the level of it the plot drew last, see `pickLevel`
    this.gutter = { left: [], right: [] }; // each y axis's width, in px, from the plot outward, fitted to its labels
    this.hover = null; // the pointer over the plot, {x, y} in client px, for the crosshair
    this.hidden = false;
    this.bindInput();
  }

  /**
   * Zooms a spectrum's frequencies by `factor` about the one at fraction
   * `anchor` across the plot, which stays where it is, along a log axis by
   * its decades. It stays within the frequencies it has bins for, and no
   * narrower than a hundredth of them. Each is part of a 'zoom' burst of the
   * views the plot's gestures act on, so a run of wheel turns is one view
   * step.
   */
  zoomFrequencies(factor, anchor) {
    const xd = this.xd, ext = this.fExtent;
    if (!xd || !ext) return;
    const [to, from] = this.logX ? [Math.log10, (v) => 10 ** v] : [(v) => v, (v) => v];
    const [a, b] = xd.map(to), [lo, hi] = ext.map(to), at = a + anchor * (b - a);
    const [na, nb] = [Math.max(lo, at - (at - a) * factor), Math.min(hi, at + (b - at) * factor)];
    if ((na === a && nb === b) || nb - na < (hi - lo) / 100) return;
    // An end that has reached its last bin is that bin's frequency itself.
    const lower = na === lo ? ext[0] : from(na), upper = nb === hi ? ext[1] : from(nb);
    this.viewHistory.burst('zoom', this.targets(), () => this.onFrequencies?.(lower, upper));
  }

  /**
   * Turns the wheel `dy` px, positive out, over the time axis when `axis`,
   * else over the plot at fraction `at` across it. Each view the gesture
   * acts on gets a goal span along the ladder the wheel's speed picks, in
   * the turn's direction from the goal it was easing to, or from the span
   * it shows when it was at rest or the turn went the other way, within
   * the spans it can zoom to (wheelzoom.js). Then frame by frame it eases
   * there: over the time axis a live view about its newest edge, going on
   * streaming; over the plot about the pointer, which holds that moment,
   * so zooming in pauses it (`View.zoomTo`). The frames of a run of turns
   * are one 'zoom' burst, one view step. A view something else moves
   * meanwhile drops out of the ease.
   */
  turnTime(dy, axis, at) {
    const now = clock.now(), z = (this.timeZoom ??= { wheel: STILL, eases: new Map(), frame: null });
    const turned = turn(z.wheel, dy, now);
    Object.assign(z, { wheel: turned.wheel, axis, at, targets: this.targets() });
    for (const v of z.targets) z.eases.set(v, { ...aimed(z.eases.get(v), v.shown(now), now, turned, v.spans(now)), mark: viewMark(v) });
    const step = (t) => {
      z.frame = null;
      const edge = this.liveEdge();
      this.viewHistory.burst('zoom', z.targets, () => {
        for (const [v, e] of z.eases) {
          if (viewMark(v) !== e.mark) {
            z.eases.delete(v);
            continue;
          }
          const span = reducedMotion?.matches ? e.goal : eased(e.start, e.goal, t - e.t0);
          if (span !== v.shown(t)) v.zoomTo(span, z.axis && v.live ? 1 : z.at, t, edge);
          if (span === e.goal) z.eases.delete(v);
          else e.mark = viewMark(v);
        }
      });
      if (z.eases.size) z.frame = clock.frame(step);
    };
    z.frame ??= clock.frame(step);
  }

  /** Ends the wheel's ease of time where it has reached. The wheel keeps what it turned short of a step. */
  stopTime() {
    const z = this.timeZoom;
    if (!z) return;
    clock.cancel(z.frame);
    z.frame = null;
    z.eases.clear();
  }

  /** The plot's GPU slot for trace `t`, which holds its window, or, by `variant`, its spectrum drawn in a pair of scales. */
  slotOf(t, variant = '') {
    this.slots ??= new WeakMap(); // Trace -> {variant -> slot}
    let slots = this.slots.get(t);
    if (!slots) this.slots.set(t, (slots = {}));
    return (slots[variant] ??= {});
  }

  /** The traces of the plot's signals the hub has: a capture plot's captures, any other's signals that stream. */
  traces() {
    const out = [];
    const session = this.hub.session, cfg = this.config(), captures = cfg.kind === 'capture';
    for (const key of cfg.keys) {
      const t = session.byKey.get(key);
      if (t && !!t.shots === captures) out.push(t);
    }
    return out;
  }

  /** The y axes' places: how many on each side, as {left, right}. */
  axisCounts(cfg) {
    if (cfg.mode !== 'independent') return { left: 1, right: 0 };
    const slots = axisSlots(cfg.keys, cfg.sides);
    return { left: slots.left.length, right: slots.right.length };
  }

  /**
   * Plot area and lanes, in CSS px relative to the viewport, and what the
   * frame draws in the gutters by them. `head` is the foot of a floating
   * header, or -Infinity; `tabs[i]` how many tabs of Y cursors lane i has
   * room for (`tabRoom`); `widths` each y axis's width by side, from the plot
   * outward: as wide as its numbers last needed, and as a tab's room at
   * least while a lane has room for a tab, so the tabs this frame rests
   * always lie on their axis.
   */
  geometry(body, cfg, n) {
    const top = body.y + EDGE.above, height = body.h - EDGE.above - AXIS_BELOW - 22;
    const count = lanesOf(cfg.mode, n), h = (height - LANE_GAP * (count - 1)) / count;
    const ys = Array.from({ length: count }, (_, i) => ({ y: top + i * (h + LANE_GAP), h }));
    const head = this.body.closest('.plot-card.monitor')?.querySelector('.plot-head')?.getBoundingClientRect().bottom ?? -Infinity;
    const tabs = ys.map((lane) => this.tabRoom(lane, head));
    const least = tabs.some((room) => room > 0) ? GUTTER.min : GUTTER.bare;
    const counts = this.axisCounts(cfg), labels = this.axisLabels(cfg, ys, tabs, head);
    const labelled = (side, i) => [...labels.values()].some((l) => l.side === side && l.slot === i);
    const widths = Object.fromEntries(['left', 'right'].map((side) => [side, Array.from({ length: counts[side] }, (_, i) => Math.max(this.gutter[side][i] ?? GUTTER.min, least) + (labelled(side, i) ? AXIS_LABEL : 0))]));
    const width = (side) => Math.max(widths[side].reduce((a, b) => a + b, 0), GUTTER.bare);
    const left = width('left'), right = width('right');
    const plot = { x: body.x + left, y: top, w: body.w - left - right, h: height };
    const lanes = count === 1 ? [plot] : ys.map(({ y }) => ({ x: plot.x, y, w: plot.w, h }));
    return { plot, lanes, head, tabs, widths, labels };
  }

  /**
   * The label each y axis draws along its outer edge, as its settings ask
   * (units.js `axisFormat`), by range key, as {text, side, slot, y0, y1}:
   * its units, or its signals' names with their units, as "sine.sine (V)",
   * each of an axis of several units with its own, and those of an axis
   * that fixes its unit the unit it fixes. It runs up the axis in a
   * column of the gutter AXIS_LABEL px wide, which widens the axis's band
   * for it, centred between `y0` and `y1`: the room of its lane in `ys`
   * below a floating header whose foot is at `head`, and above the tabs of
   * Y cursors resting at the lane's foot, which `tabs` counts, CLEAR px
   * from each. A label longer than that room is the units alone, and where
   * those are longer too, or there are none, the axis has no label.
   */
  axisLabels(cfg, ys, tabs, head) {
    const out = new Map(), traces = this.traces(), spectrum = cfg.kind === 'spectrum';
    // Each axis as [key, its traces, its lane, side, slot].
    let axes;
    if (cfg.mode === 'overlay') axes = [['shared', traces, 0, 'left', 0]];
    else if (cfg.mode === 'stacked') axes = traces.map((t, i) => [t.key, [t], i, 'left', 0]);
    else {
      const slots = axisSlots(cfg.keys, cfg.sides);
      axes = ['left', 'right'].flatMap((side) => slots[side].map((key, slot) => [key, traces.filter((t) => t.key === key), 0, side, slot]));
    }
    this.ctx.font = `11px ${this.theme().dataFont}`;
    for (const [key, ts, i, side, slot] of axes) {
      const f = axisFormat(cfg.yFormat?.[key]), lane = ys[i];
      if (f.label === 'off' || !ts.length || !lane) continue;
      // A signal's units as the plot shows them (`shownUnit`).
      const unitOf = (t) => shownUnit(cfg, t.key, (k) => traces.find((x) => x.key === k)?.meta.units ?? '');
      const units = [...new Set(ts.map(unitOf).filter(Boolean))];
      const named = units.length > 1 ? ts.map((t) => (unitOf(t) ? `${titleOf(t)} (${unitOf(t)})` : titleOf(t))).join(', ') : `${ts.map(titleOf).join(', ')}${units.length ? ` (${units[0]})` : ''}`;
      const y0 = Math.max(lane.y, head + CLEAR) + CLEAR, y1 = lane.y + lane.h - Math.min(2, tabs[i]) * (TAB.h + CLEAR) - CLEAR;
      const text = [f.label === 'name' && named, units.join(', ')].find((s) => s && this.ctx.measureText(s).width <= y1 - y0);
      if (text) out.set(key, { text, side, slot, y0, y1 });
    }
    return out;
  }

  /**
   * The y ranges of the plot's kind, which its view holds, range key ->
   * {range, auto}. Each kind keeps its own, and a spectrum its own for each
   * y scale, as a log axis's are log10 of what it shows: switching kind or
   * scale and back finds each as it was left.
   */
  get y() {
    const cfg = this.config();
    const kind = cfg.kind === 'spectrum' ? `spectrum:${cfg.yLog ? 'log' : 'linear'}` : cfg.kind;
    let ranges = this.view.yByKind.get(kind);
    if (!ranges) this.view.yByKind.set(kind, (ranges = new Map()));
    return ranges;
  }

  /** The plot's cursors as it draws them: the tab's, or those of the drag that is moving one (cursors.js). */
  get placed() {
    return this.cursors.get(this.plotId);
  }

  /** Y1 and Y2 of the y axis with range key `key`, as {a, b}, each a value in the axis's units or null. */
  yPair(key) {
    return yOf(this.placed, this.config().kind, key);
  }

  rangeKey(cfg, trace) {
    return cfg.mode === 'overlay' ? 'shared' : trace.key;
  }

  /**
   * The y range of `key`, given the extent [lo, hi] of what it shows now.
   *
   * With `hold`, as a time plot asks, the range fits that extent once, when
   * it first has data, and again at each `refit`. Between fits it
   * holds, as a scope's vertical scale does, so zooming, panning and a
   * passing burst never rescale it. Without, as a spectrum asks, whose first
   * estimates are rough, it follows the extent, growing at once and
   * shrinking only when far too big. A range set by hand holds either way.
   */
  yRange(key, lo, hi, hold) {
    let st = this.y.get(key);
    if (!st) this.y.set(key, (st = { range: null, auto: true }));
    if (!st.auto && st.range) return st.range;
    const want = fitted(lo, hi), cur = st.range;
    if (!want) return st.range ?? [-1, 1];
    if (hold) {
      st.range = want;
      st.auto = false;
    } else if (!cur || want[0] < cur[0] || want[1] > cur[1] || want[1] - want[0] < 0.5 * (cur[1] - cur[0])) {
      const extra = (want[1] - want[0]) * 0.1;
      st.range = [want[0] - extra, want[1] + extra];
    }
    return st.range;
  }

  /** The plot's colours, in the look its Colors are drawn in. */
  theme() {
    return theme(this.colors.dark ?? undefined);
  }

  /**
   * Draws the plot at page time `perf`. A `still` frame is drawn as if no
   * pointer were on any plot: no hover line, tip or value, and no window
   * readout, as a picture of the plot has none.
   */
  draw(perf, still = false) {
    if (still) {
      const kept = { hover: this.hover, axisHover: this.axisHover, specHover: this.specHover, windowUntil: this.windowUntil, t: this.hoverTime.t };
      Object.assign(this, { hover: null, axisHover: null, specHover: null, windowUntil: -Infinity });
      this.hoverTime.t = null;
      try {
        this.draw(perf);
      } finally {
        this.hoverTime.t = kept.t;
        delete kept.t;
        Object.assign(this, kept);
      }
      return;
    }
    const r = this.body.getBoundingClientRect();
    // Off screen, collapsed, or hidden behind a maximized plot: draw nothing.
    const shown = this.body.checkVisibility ? this.body.checkVisibility({ visibilityProperty: true }) : true;
    const body = { x: r.left, y: r.top, w: r.width, h: r.height };
    const visible = shown && r.width > 0 && r.height > 0 && this.renderer.shows(body);
    this.sizeOverlay(r);
    if (!visible) return;
    this.drawnOn = body;
    this.renderer.fill(body, this.theme().bgRgba);
    const cfg = this.config();
    const traces = this.traces();
    const geo = this.geometry(body, cfg, traces.length);
    this.geo = geo;
    this.publishFrame(body, geo.plot);
    if (isSlice(cfg.kind)) this.drawSlice(cfg, traces, geo, body);
    else this.drawTime(cfg, traces, geo, body, perf);
    this.placeReadout(body, cfg.readoutAt ?? null);
  }

  /**
   * Places the cursors' readout, the page's table over the plot
   * (`.cursor-readout` in the body), clear of every cursor's tag and tab the
   * plot drew (`pressable`). It sits at `at`, its place as a share of the
   * body's room across and down, while that covers none of them. Where one
   * lies under it, it takes the place nearest that covers none: its own
   * moved along an edge out of the way of what is in it, across the way
   * those parts move, up or down past an X cursor's and aside past a Y
   * cursor's; or a corner, or a corner moved so. It stays where it moved to
   * while that covers none, and comes back once its own place is TAG_HELD
   * px clear of them all, so a cursor dragged past it moves it once away
   * and once back. Where no place covers none it is not drawn: the tags
   * win. With no `at`, as while it is dragged, the page places it alone.
   * A place it moved to is held from the body's edges on the sides of its
   * own place, the right one for a place at the right, the bottom one for
   * a place at the foot, so it stays put there as its size changes with
   * what it reads. The place is set on the body: `data-readout` 'moved',
   * with --readout-x and --readout-y the px from those edges and
   * `data-readout-from` naming them, or 'hidden'.
   */
  placeReadout(body, at) {
    const el = at && this.body.querySelector('.cursor-readout');
    if (!el) return this.publishReadout(null);
    const { width: w, height: h } = el.getBoundingClientRect(), I = READOUT_INSET;
    const right = at.x === 1, foot = at.y === 1;
    const room = { l: body.x + I, t: body.y + I, r: body.x + body.w - I, b: body.y + body.h - I };
    const placed = (a) => ({ x: room.l + a.x * Math.max(0, room.r - room.l - w), y: room.t + a.y * Math.max(0, room.b - room.t - h), w, h });
    const parts = this.pressable.filter((p) => p.part === 'tag' || p.part === 'tab');
    const clear = (box, by = 0) => !parts.some((p) => overlaps({ x: p.x - by, y: p.y - by, w: p.w + 2 * by, h: p.h + 2 * by }, box));
    const inRoom = (b) => b.x >= room.l - 0.5 && b.x + w <= room.r + 0.5 && b.y >= room.t - 0.5 && b.y + h <= room.b + 0.5;
    const home = placed(at), was = this.readoutPlace, same = was?.at === `${at.x},${at.y}`;
    let to = null;
    if (!clear(home, same && was.box ? TAG_HELD : 0)) {
      const kept = same && typeof was.box === 'object' && was.box && { x: right ? body.x + body.w - was.box.ax - w : body.x + was.box.ax, y: foot ? body.y + body.h - was.box.ay - h : body.y + was.box.ay, w, h };
      if (kept && inRoom(kept) && clear(kept)) to = kept;
      else {
        const corners = [home, ...[{ x: 0, y: 0 }, { x: 1, y: 0 }, { x: 0, y: 1 }, { x: 1, y: 1 }].map(placed)];
        const ways = corners.flatMap((c) => [c, ...parts.flatMap((p) => (p.kind === 'x' ? [{ ...c, y: p.y + p.h + READOUT_GAP }, { ...c, y: p.y - READOUT_GAP - h }] : [{ ...c, x: p.x + p.w + READOUT_GAP }, { ...c, x: p.x - READOUT_GAP - w }]))]);
        const d = (b) => Math.hypot(b.x - home.x, b.y - home.y);
        to = ways.filter((b) => inRoom(b) && clear(b)).reduce((best, b) => (!best || d(b) < d(best) ? b : best), null) ?? 'hidden';
      }
    }
    const box = to && to !== 'hidden' ? { ax: right ? body.x + body.w - to.x - w : to.x - body.x, ay: foot ? body.y + body.h - to.y - h : to.y - body.y, from: `${right ? 'right' : 'left'} ${foot ? 'bottom' : 'top'}` } : to;
    this.readoutPlace = { at: `${at.x},${at.y}`, box };
    this.publishReadout(box);
  }

  /** Sets where `placeReadout` has the readout on the body: `place` {ax, ay, from}, 'hidden', or null for where the page has it. Only a change is written. */
  publishReadout(place) {
    const key = place === null || place === 'hidden' ? place : `${place.ax},${place.ay},${place.from}`;
    if (this.readoutShown === key) return;
    this.readoutShown = key;
    if (place === null) delete this.body.dataset.readout;
    else this.body.dataset.readout = place === 'hidden' ? 'hidden' : 'moved';
    if (place && place !== 'hidden') {
      this.body.dataset.readoutFrom = place.from;
      this.body.style.setProperty('--readout-x', `${place.ax}px`);
      this.body.style.setProperty('--readout-y', `${place.ay}px`);
    }
  }

  /**
   * Sets the plot area's distance from each edge of the body, in px, as
   * --frame-l, --frame-t, --frame-r and --frame-b on the body, for what
   * the page places within the plot area, as the legend. Only a change is
   * written.
   */
  publishFrame(body, P) {
    const edges = [P.x - body.x, P.y - body.y, body.x + body.w - P.x - P.w, body.y + body.h - P.y - P.h].map(Math.round);
    if (this.frameEdges?.every((v, i) => v === edges[i])) return;
    this.frameEdges = edges;
    ['l', 't', 'r', 'b'].forEach((side, i) => this.body.style.setProperty(`--frame-${side}`, `${edges[i]}px`));
  }

  /** The lane of the plot's `i`th trace, one of `lanesOf(cfg.mode, n)`. */
  laneOf(cfg, i) {
    return cfg.mode === 'stacked' ? i : 0;
  }

  drawTime(cfg, traces, geo, body, perf) {
    // The range on the device pixel grid, which everything drawn below uses,
    // so a live plot scrolls by whole pixels and does not flicker: the
    // pixels of its pictures, so a preview drawn coarser shows their range.
    const [x0, x1] = onPixelGrid(...this.view.range(this.hub.now(perf)), Math.max(1, Math.round(geo.plot.w * this.renderer.grid())));
    const session = this.hub.session;
    const plans = traces.map((t, index) => {
      // Live, it draws the level of the live points that suits the window.
      // Paused, it draws from whichever of the hub's history the page holds
      // shows the view best (`pickSource`): the detail window, the overview
      // of the whole capture, or that level.
      const level = pickLevel(t.levels, x0, x1, geo.plot.w, this.drawnLevel.get(t));
      this.drawnLevel.set(t, level);
      const live = t.levels[level];
      const d = this.view.live ? null : session.windowOf(this.plotId, 'detail', t);
      const o = this.view.live ? null : session.windowOf(this.plotId, 'overview', t);
      const src = pickSource([d, o, live].filter(Boolean), x0, x1, geo.plot.w);
      const a0 = Math.max(src.absStart, src.lowerBound(x0) - 1);
      const a1 = Math.min(src.absEnd, src.lowerBound(x1) + 1);
      const [lo, hi] = a1 > a0 ? src.extent(a0, a1) : [Infinity, -Infinity];
      return { t, src, fromWindow: src !== live, level, a0, a1, lo, hi, index };
    });
    // What this frame draws: the times, and what each trace is drawn from.
    this.drawn = { x0, x1, plans };
    // The history the plot moves through, oldest to newest: what its view can show.
    this.history = this.view.limits(perf);
    // Shared ranges take the union of their traces. A fit fits to what this frame shows.
    const ext = new Map();
    for (const p of plans) {
      const k = this.rangeKey(cfg, p.t), e = ext.get(k) ?? [Infinity, -Infinity];
      ext.set(k, [Math.min(e[0], p.lo), Math.max(e[1], p.hi)]);
    }
    this.extents = ext;
    const ranges = new Map([...ext].map(([k, [lo, hi]]) => [k, this.yRange(k, lo, hi, true)]));
    plans.forEach((p) => {
      const lane = geo.lanes[this.laneOf(cfg, p.index)];
      const [y0, y1] = ranges.get(this.rangeKey(cfg, p.t));
      if (p.fromWindow) {
        const st = this.renderer.detailBuffer(this.slotOf(p.t), p.src);
        if (this.renderer.lane(lane, st.base, x0, x1, y0, y1, prefs.traceWidth, rgbaOf(p.t, this.colors)))
          this.renderer.drawDetail(st, p.src, p.a0, p.a1);
        return;
      }
      const ring = this.renderer.ring(p.src);
      if (this.renderer.lane(lane, ring.base, x0, x1, y0, y1, prefs.traceWidth, rgbaOf(p.t, this.colors))) this.renderer.drawTrace(p.src, ring, p.a0, p.a1);
    });
    this.drawOverlay(cfg, geo, body, plans, ranges, [x0, x1], null);
  }

  /**
   * What sets the plot's x axis apart as a slice's, {density, unit, log}:
   * `density` whether it shows spectra, densities along frequency, whose
   * axes are log where the plot says so, or captures, along the x their
   * device gives them, in `unit`, on linear axes.
   */
  sliceAxes(cfg) {
    const density = cfg.kind === 'spectrum';
    return { density, unit: density ? 'Hz' : (this.traces()[0]?.meta.xUnits ?? ''), log: { x: density && !!cfg.xLog, y: density && !!cfg.yLog } };
  }

  /** Draws a spectrum plot or a capture plot: of each signal the slice it shows (`sliceOf`), over the shots an overlay draws beneath it. */
  drawSlice(cfg, traces, geo, body) {
    const slice = this.sliceAxes(cfg);
    // Each axis is log or linear: a log axis's GL coordinates are log10.
    const lx = slice.log.x ? Math.log10 : (v) => v, ly = slice.log.y ? Math.log10 : (v) => v;
    const plans = [];
    let fmin = Infinity, fmax = -Infinity;
    for (const t of traces) {
      const s = this.sliceOf(t);
      if (!s || s.f.length < 2) continue;
      let lo = Infinity, hi = -Infinity;
      for (const line of [s, ...(s.under ?? [])]) {
        for (const a of line.a) {
          if (a < lo) lo = a;
          if (a > hi) hi = a;
        }
        fmin = Math.min(fmin, line.f[0]);
        fmax = Math.max(fmax, line.f.at(-1));
      }
      plans.push({ t, s, lo: ly(lo), hi: ly(hi), index: traces.indexOf(t) });
    }
    const ext = new Map();
    for (const p of plans) {
      const k = this.rangeKey(cfg, p.t), e = ext.get(k) ?? [Infinity, -Infinity];
      ext.set(k, [Math.min(e[0], p.lo), Math.max(e[1], p.hi)]);
    }
    const ranges = new Map([...ext].map(([k, [lo, hi]]) => [k, this.yRange(k, lo, hi, false)]));
    // The frequencies shown: those the plot was set to, or every bin.
    const [f0, f1] = cfg.fRange ?? [fmin, fmax];
    const lx0 = lx(f0), lx1 = lx(f1);
    const variant = `slice:${slice.log.x}:${slice.log.y}`;
    for (const p of plans) {
      const lane = geo.lanes[this.laneOf(cfg, p.index)];
      const [y0, y1] = ranges.get(this.rangeKey(cfg, p.t));
      const rgba = rgbaOf(p.t, this.colors);
      // Each line from a buffer of its own, uploaded once: the slice's by the signal, a shot's beneath it by the shot.
      const line = ({ f, a }, of, source, alpha) => {
        if (!this.renderer.lane(lane, 0, lx0, lx1, y0, y1, prefs.traceWidth, [rgba[0], rgba[1], rgba[2], rgba[3] * alpha])) return;
        this.renderer.drawStatic(this.slotOf(of, variant), source, () => {
          const pts = new Float32Array(f.length * 2);
          for (let i = 0; i < f.length; i++) {
            pts[i * 2] = lx(f[i]);
            pts[i * 2 + 1] = ly(a[i]);
          }
          return pts;
        });
      };
      for (const under of p.s.under ?? []) line(under, under.shot, under.shot, under.alpha);
      line(p.s, p.t, p.s, 1);
    }
    const xd = plans.length ? [f0, f1] : null;
    // What this frame draws: the stretch of x, and each signal's slice.
    this.drawn = { x0: f0, x1: f1, plans };
    this.fExtent = plans.length ? [fmin, fmax] : null;
    this.drawOverlay(cfg, geo, body, plans, ranges, xd, slice, traces);
  }

  /**
   * Moves a spectrum's frequencies by \`fraction\` of the plot's width, higher
   * for a positive one, along its scale, within the frequencies it has bins
   * for. Showing every frequency, it has nowhere to go.
   */
  panFrequencies(fraction) {
    const xd = this.xd, ext = this.fExtent;
    if (!xd || !ext) return;
    // Along a log axis a pan multiplies; along a linear one it adds.
    const [to, from] = this.logX ? [Math.log10, (v) => 10 ** v] : [(v) => v, (v) => v];
    const [a, b] = xd.map(to), [lo, hi] = ext.map(to);
    const shift = Math.min(hi - b, Math.max(lo - a, fraction * (b - a)));
    if (shift) this.onFrequencies?.(from(a + shift), from(b + shift));
  }

  /** Sizes the overlay's canvas to the body's box `r` at the renderer's pixel ratio. */
  sizeOverlay(r) {
    const dpr = this.renderer.dpr;
    const w = Math.max(1, Math.round(r.width * dpr)), h = Math.max(1, Math.round(r.height * dpr));
    if (this.overlay.width !== w || this.overlay.height !== h) {
      this.overlay.width = w;
      this.overlay.height = h;
    }
    this.dpr = dpr;
  }

  /**
   * The hover's tip: one line naming the signal nearest the pointer and its
   * value there, so a plot of many signals answers which line is which
   * without a legend. It sits beside the pointer, at (px, py), inside the
   * lane, and stays there while the signal moves under it. It covers no
   * cursor's tag or tab, nor the pointer's own tags: where it would, it
   * takes the place nearest the pointer that is clear of them and of the
   * pointer, beside or above or below the pointer or one of them, and where
   * the lane has no such place it is not drawn. It is a part of
   * `pressable`, 'tip', with the `value` it writes, which a press goes through.
   */
  drawTip(ctx, T, lane, near, value, px, py) {
    const title = titleOf(near.t), name = title.length > 18 ? `${title.slice(0, 17)}…` : title;
    const SWATCH = 8, PAD = 5, H = 16, GAP = 10;
    const nameW = ctx.measureText(name).width, valueW = ctx.measureText(value).width;
    const w = PAD + SWATCH + PAD + nameW + PAD + valueW + PAD;
    // Above and right of the pointer, flipped where that leaves the lane.
    let x = px + GAP, y = py - GAP - H;
    if (x + w > lane.x + lane.w) x = px - GAP - w;
    if (y < lane.y) y = py + GAP;
    x = Math.max(lane.x, x);
    y = Math.max(lane.y, Math.min(lane.y + lane.h - H, y));
    const kept = this.pressable.filter((p) => p.part === 'pointer' || (p.text && (p.alpha ?? 1) > 0));
    const clear = (x, y) => !kept.some((p) => overlaps(p, { x, y, w, h: H }));
    if (!clear(x, y)) {
      const xs = [px + GAP, px - GAP - w, ...kept.flatMap((p) => [p.x + p.w + 2, p.x - 2 - w])];
      const ys = [py - GAP - H, py + GAP, ...kept.flatMap((p) => [p.y - 2 - H, p.y + p.h + 2])];
      let best = null;
      for (const cx of xs)
        for (const cy of ys) {
          const inLane = cx >= lane.x && cx + w <= lane.x + lane.w && cy >= lane.y && cy + H <= lane.y + lane.h;
          const onPointer = px > cx - 2 && px < cx + w + 2 && py > cy - 2 && py < cy + H + 2;
          if (!inLane || onPointer || !clear(cx, cy)) continue;
          const d = Math.hypot(Math.max(cx - px, 0, px - cx - w), Math.max(cy - py, 0, py - cy - H));
          if (!best || d < best.d) best = { x: cx, y: cy, d };
        }
      if (!best) return;
      ({ x, y } = best);
    }
    this.pressable.push({ part: 'tip', through: true, value, x, y, w, h: H });
    ctx.fillStyle = T.tipBg;
    ctx.fillRect(x, y, w, H);
    ctx.strokeStyle = T.tipBorder;
    ctx.strokeRect(x + 0.5, y + 0.5, w - 1, H - 1);
    const color = colorOf(near.t, this.colors);
    ctx.fillStyle = color;
    ctx.fillRect(x + PAD, y + H / 2 - 1.5, SWATCH, 3);
    ctx.textAlign = 'left';
    ctx.fillStyle = T.tipText;
    ctx.fillText(name, x + PAD + SWATCH + PAD, y + 12);
    ctx.fillStyle = color;
    ctx.fillText(value, x + w - PAD - valueW, y + 12);
  }

  /**
   * Draws all but the traces: axes, grid, gaps, cursors, the hover and its
   * readouts. `slice` is what `sliceAxes` gives of a plot that shows
   * slices, whose x axis is theirs and whose axes are each log or linear
   * as it says, or null for a time plot, whose axes are linear. With
   * `cfg.grid` false the grid is not drawn, and with `cfg.xTime`
   * 'seconds' a time axis reads the seconds from its left end.
   */
  drawOverlay(cfg, geo, body, plans, ranges, xd, slice, allTraces) {
    const spectrum = !!slice?.density, logX = !!slice?.log.x, logY = !!slice?.log.y, kind = spectrum ? 'spectrum' : 'time';
    const ctx = this.ctx;
    const T = cfg.grid === false ? { ...this.theme(), grid: 'rgb(0 0 0 / 0)' } : this.theme();
    ctx.setTransform(this.dpr, 0, 0, this.dpr, -body.x * this.dpr, -body.y * this.dpr);
    ctx.clearRect(body.x, body.y, body.w, body.h);
    ctx.font = `11px ${T.dataFont}`;
    ctx.lineWidth = 1;
    const traces = allTraces ?? plans.map((p) => p.t);
    if (!xd || plans.length === 0) {
      this.legend = { rows: {}, cursors: null };
      this.pressable = [];
      ctx.fillStyle = T.axisText;
      const msg = !traces.length ? 'drop signals here' : slice && !spectrum ? 'no capture yet' : 'waiting for data…';
      ctx.textAlign = 'center';
      ctx.fillText(msg, body.x + body.w / 2, body.y + body.h / 2);
      return;
    }
    const P = geo.plot;
    const sx = (logX ? scaleLog() : scaleLinear()).domain(xd).range([P.x, P.x + P.w]);
    const time = timeAxis(plans[0]?.t.meta.epoch, cfg.xTime === 'seconds' ? xd[0] : null);

    // The live edge, while the pointer is over it on a live time plot, where
    // a zoom keeps the plot live: a wash rising toward the newest edge, under
    // the grid, and a LIVE line on that edge, drawn with the hover, where the
    // zoom anchors. Both fade in and out.
    const frameAt = clock.now();
    const dt = Math.min(100, frameAt - (this.overlayAt ?? frameAt));
    this.overlayAt = frameAt;
    const pointer = this.hover;
    const edgeX = pointer && !slice && this.view.live ? P.x + this.liveEdge() * P.w : Infinity;
    const target = pointer && this.inPlot(pointer.x, pointer.y) && pointer.x >= edgeX ? 1 : 0, was = this.zone ?? 0;
    this.zone = reducedMotion?.matches ? target : was + (target - was) * (1 - Math.exp(-dt / (target ? LIVE_FADE_MS.in : LIVE_FADE_MS.out)));
    if (Math.abs(this.zone - target) < 0.01) this.zone = target;
    if (this.zone > 0) {
      const from = isFinite(edgeX) ? edgeX : (this.zoneFrom ?? P.x + P.w);
      this.zoneFrom = from;
      const g = ctx.createLinearGradient(from, 0, P.x + P.w, 0);
      g.addColorStop(0, `rgb(${T.wash} / 0)`);
      g.addColorStop(0.35, `rgb(${T.wash} / ${0.03 * this.zone})`);
      g.addColorStop(1, `rgb(${T.wash} / ${0.075 * this.zone})`);
      ctx.fillStyle = g;
      for (const lane of geo.lanes) ctx.fillRect(from, lane.y, P.x + P.w - from, lane.h);
    }
    const unlog = (r) => (logY ? [10 ** r[0], 10 ** r[1]] : r);
    const yScale = (range, lane) => (logY ? scaleLog() : scaleLinear()).domain(unlog(range)).range([lane.y + lane.h, lane.y]);
    // A log axis labels its decades only.
    const decade = (v) => Number.isInteger(Math.round(Math.log10(v) * 1e6) / 1e6);

    // X grid and labels. A log axis showing under two decades labels its 2s
    // and 5s too. A label that would run into the one before it, or past the
    // plot's edge, is left out, so a narrow plot labels fewer ticks.
    const xticks = logX ? sx.ticks(6) : slice ? sx.ticks(Math.max(2, P.w / 90)) : time.ticks(xd[0], xd[1], Math.max(2, P.w / 120));
    const fewDecades = logX && Math.log10(xd[1] / xd[0]) < 2;
    const labelled = (f) => decade(f) || (fewDecades && [2, 5].includes(Math.round(f / 10 ** Math.floor(Math.log10(f) + 1e-9))));
    // The ticks an X cursor dragged with Shift snaps to: each one the grid has a line at.
    this.xTicks = xticks;
    ctx.fillStyle = T.axisText;
    ctx.textAlign = 'center';
    this.tickLabels = [];
    this.labelBoxes = [];
    // With no room beside the plot, the X cursors' tabs rest on the axis's left end.
    const xTabsBeside = P.x - body.x >= 2 * TAB.w + 6;
    let labelEnd = xTabsBeside ? -Infinity : P.x + 2 * TAB.w + 4;
    // A slice's x as its x axis's settings write it.
    const xFormat = axisFormat(slice ? cfg.xFormat : null), fTicks = slice ? xticks.filter((f) => !logX || labelled(f)) : [];
    const fTexts = new Map(tickTexts(fTicks, xFormat, xTickText(slice?.unit), { x: slice?.unit ?? null, log: logX }).map((text, i) => [fTicks[i], text]));
    for (const tv of xticks) {
      const px = Math.round(sx(tv)) + 0.5;
      if (px < P.x || px > P.x + P.w) continue;
      ctx.strokeStyle = T.grid;
      for (const lane of geo.lanes) {
        ctx.beginPath();
        ctx.moveTo(px, lane.y);
        ctx.lineTo(px, lane.y + lane.h);
        ctx.stroke();
      }
      const label = slice ? (fTexts.get(tv) ?? '') : time.label(tv);
      if (!label) continue;
      const half = ctx.measureText(label).width / 2;
      if (px - half < labelEnd + X_LABEL_GAP || px + half > body.x + body.w) continue;
      ctx.fillText(label, px, P.y + P.h + AXIS_BELOW + 15);
      this.tickLabels.push({ text: label, x: px - half, y: P.y + P.h + AXIS_BELOW + 15 - LABEL.up, w: 2 * half, h: LABEL.up + LABEL.down });
      labelEnd = px + half;
    }

    // Y axes: which range each lane's grid and labels follow, and the plans
    // that range scales.
    const axes = []; // {key, lane, range, side, color, plans}: key the range key
    if (cfg.mode === 'stacked') {
      plans.forEach((p) => {
        const lane = geo.lanes[this.laneOf(cfg, p.index)];
        axes.push({ key: p.t.key, lane, range: ranges.get(p.t.key), side: 'left', color: colorOf(p.t, this.colors), plans: [p] });
      });
    } else if (cfg.mode === 'independent') {
      const slots = axisSlots(plans.map((p) => p.t.key), cfg.sides);
      for (const side of ['left', 'right'])
        slots[side].forEach((key, slot) => {
          const p = plans.find((p) => p.t.key === key);
          axes.push({ key, lane: geo.lanes[0], range: ranges.get(key), side, slot, color: colorOf(p.t, this.colors), plans: [p], keys: [key] });
        });
    } else {
      axes.push({ key: 'shared', lane: geo.lanes[0], range: ranges.get('shared'), side: 'left', color: T.axisText, plans });
    }
    // Each axis's band of the gutter, from the plot outward; the outermost
    // reaches the plot's edge. Each writes its numbers as its settings say,
    // in the units of its signals.
    for (const a of axes) {
      a.format = axisFormat(cfg.yFormat?.[a.key]);
      a.units = unitsOf(a.plans.map((p) => p.t.meta.units), spectrum);
      a.slot ??= 0;
      let inner = 0;
      for (let i = 0; i < a.slot; i++) inner += geo.widths[a.side][i];
      const w = geo.widths[a.side][a.slot];
      const outermost = !axes.some((b) => b.side === a.side && b.slot > a.slot);
      if (a.side === 'left') [a.x0, a.x1] = [outermost ? body.x : P.x - inner - w, P.x - inner];
      else [a.x0, a.x1] = [P.x + P.w + inner, outermost ? body.x + body.w : P.x + P.w + inner + w];
    }
    this.axisBands = axes.map(({ side, slot, x0, x1, keys }) => ({ side, slot, x0, x1, keys }));
    // The regions a press lands on, under everything drawn over them.
    this.pressable = [
      { part: 'plot', x: P.x, y: P.y, w: P.w, h: P.h },
      { part: 'xaxis', x: P.x, y: P.y + P.h, w: P.w, h: body.y + body.h - P.y - P.h },
      ...this.axisBands.map((band) => ({ part: 'yaxis', band, x: band.x0, y: P.y, w: band.x1 - band.x0, h: P.h })),
    ];
    this.yAxes = axes;
    this.tagsPlaced = []; // the tags laid along the lanes' sides this frame, see tagSlot
    // Over a y axis, the axis's gutter shows behind its labels how often each
    // signal it scales is at each height in view: a column per signal out
    // from the axis line, each overlapping the one before by half.
    const hy = this.hover?.y ?? null;
    const hoverLane = hy === null ? null : geo.lanes.find((l) => hy >= l.y && hy <= l.y + l.h);
    const overAxis = this.hover ? this.yAxisAt(this.hover.x, hy) : null; // the band the pointer is on
    // The source whose samples a strip counts: the plot's own when it is raw
    // or a live level, else the live level the plot would draw.
    const rawOf = (p) => (!p.fromWindow || p.src.ratio === 1 ? p.src : p.t.levels[p.level]);
    const rowsOf = (lane, sy) => [(y) => Math.floor(sy(y) - lane.y), Math.ceil(lane.h)];
    // Leaving the axis lets the columns go, so the next hover fades them in.
    if (!hoverLane || !overAxis || slice) this.phosphor.clear();
    else {
      const axis = axes.find((a) => a.lane === hoverLane && a.side === overAxis.side && a.slot === overAxis.slot);
      const scaled = axis?.plans ?? [];
      scaled.forEach((p, i) => {
        const hist = rowCounts(rawOf(p), xd[0], xd[1], ...rowsOf(hoverLane, yScale(ranges.get(this.rangeKey(cfg, p.t)), hoverLane)));
        const target = hist ? blurred(hist.counts, AXIS_STRIP.blur) : new Float32Array(Math.ceil(hoverLane.h));
        let shown = this.phosphor.get(p.t.key);
        if (!shown || shown.length !== target.length) this.phosphor.set(p.t.key, (shown = new Float32Array(target.length)));
        for (let r = 0; r < shown.length; r++) shown[r] += (target[r] - shown[r]) * AXIS_STRIP.ease;
        const out = i * AXIS_STRIP.step;
        const x = axis.side === 'left' ? axis.x1 - out - AXIS_STRIP.w : axis.x0 + out;
        drawStrip(ctx, shown, colorOf(p.t, this.colors), x, AXIS_STRIP.w, hoverLane.y, AXIS_STRIP.alpha);
      });
    }
    // Each axis's grid and numbers, as `laneTicks` picks them by its lane's
    // height, and the width its numbers need, or with none the bare edge,
    // for the next frame's geometry, which widens it further for the tabs
    // of Y cursors in that frame. Numbers reach past the plot's top and foot
    // as far as the room it keeps there, and past a lane's edge between two
    // lanes half way into the gap; none reach above a floating header's
    // foot, with CLEAR px below it.
    const head = geo.head;
    const need = { left: [], right: [] };
    const reachOf = (lane) => ({
      above: Math.min(lane === geo.lanes[0] ? EDGE.above : LABEL_REACH, lane.y - head - CLEAR),
      below: lane === geo.lanes.at(-1) ? EDGE.below : LABEL_REACH,
    });
    axes.forEach((ax, i) => {
      const sy = yScale(ax.range, ax.lane);
      const { grid, labels, count } = laneTicks(sy, ax.lane, logY ? decade : undefined, reachOf(ax.lane));
      // Numbers take the precision of the ticks they label, so no two read the same.
      const texts = tickTexts(labels, ax.format, logY ? (v) => v.toExponential(0) : sy.tickFormat(count), { log: logY, units: ax.units });
      // The ticks a Y cursor dragged with Shift snaps to: where the axis draws its grid, or else its numbers.
      ax.ticks = i === 0 || cfg.mode === 'stacked' ? grid : labels;
      if (i === 0 || cfg.mode === 'stacked')
        for (const tv of grid) {
          const py = Math.round(sy(tv)) + 0.5;
          ctx.strokeStyle = T.grid;
          ctx.beginPath();
          ctx.moveTo(ax.lane.x, py);
          ctx.lineTo(ax.lane.x + ax.lane.w, py);
          ctx.stroke();
        }
      ctx.textAlign = ax.side === 'left' ? 'right' : 'left';
      ctx.fillStyle = ax.color;
      let widest = 0;
      for (const [n, tv] of labels.entries()) {
        const py = Math.round(sy(tv)) + 0.5, label = texts[n], w = ctx.measureText(label).width;
        widest = Math.max(widest, w);
        ctx.fillText(label, ax.side === 'left' ? ax.x1 - GUTTER.gap : ax.x0 + GUTTER.gap, py + 4);
        this.tickLabels.push({ axis: ax.key, text: label, x: ax.side === 'left' ? ax.x1 - GUTTER.gap - w : ax.x0 + GUTTER.gap, y: py + 4 - LABEL.up, w, h: LABEL.up + LABEL.down });
      }
      const want = labels.length ? Math.max(GUTTER.min, Math.ceil(widest + GUTTER.gap + GUTTER.pad)) : GUTTER.bare;
      need[ax.side][ax.slot] = Math.max(need[ax.side][ax.slot] ?? 0, want);
      // Its label, which the frame's geometry made room for, running up the column at the band's outer edge.
      const named = geo.labels.get(ax.key);
      if (named) {
        const w = geo.widths[ax.side][ax.slot], tw = ctx.measureText(named.text).width, cy = (named.y0 + named.y1) / 2;
        const x = ax.side === 'left' ? ax.x1 - w + CLEAR + LABEL.up : ax.x0 + w - CLEAR - LABEL.down;
        ctx.save();
        ctx.translate(x, cy);
        ctx.rotate(-Math.PI / 2);
        ctx.textAlign = 'center';
        ctx.fillText(named.text, 0, 0);
        ctx.restore();
        this.labelBoxes.push({ axis: ax.key, text: named.text, x: x - LABEL.up, y: cy - tw / 2, w: LABEL.up + LABEL.down, h: tw });
      }
    });
    for (const side of ['left', 'right'])
      this.gutter[side] = need[side].map((want, slot) => {
        const had = this.gutter[side][slot];
        return had !== undefined && want <= had && want >= had - GUTTER.slack ? had : want;
      });
    ctx.strokeStyle = T.frame;
    for (const lane of geo.lanes) ctx.strokeRect(lane.x + 0.5, lane.y + 0.5, lane.w - 1, lane.h - 1);

    // Gaps, in the lane of their trace. Where a sample or a few are missing,
    // a dashed line marks the place. A gap wide enough to see, as when the
    // link to the device was down, is a band from the last point before it
    // to the first after, between two such lines.
    this.gaps = [];
    if (!slice) {
      ctx.setLineDash([3, 3]);
      ctx.strokeStyle = ctx.fillStyle = T.gap;
      plans.forEach((p) => {
        const lane = geo.lanes[this.laneOf(cfg, p.index)];
        const mark = (px) => {
          ctx.beginPath();
          ctx.moveTo(px, lane.y);
          ctx.lineTo(px, lane.y + lane.h);
          ctx.stroke();
        };
        for (const b of p.src.breaks) {
          if (b <= p.a0 || b >= p.a1) continue;
          const x1 = Math.round(sx(p.src.xAt(b))) + 0.5, from = Math.max(lane.x, Math.round(sx(p.src.xAt(b - 1)))) + 0.5;
          const band = x1 - from >= GAP_BAND, x0 = band ? from : x1;
          this.gaps.push({ index: p.index, x0, x1, band });
          if (band) {
            ctx.globalAlpha = GAP_BAND_ALPHA;
            ctx.fillRect(x0, lane.y, x1 - x0, lane.h);
            ctx.globalAlpha = 1;
            mark(x0);
          }
          mark(x1);
        }
      });
      ctx.setLineDash([]);
    }

    // Cursors: the hover crosshair, and the placed A and B.
    this.xd = xd;
    this.slice = slice;
    this.spectrum = spectrum;
    this.logX = logX;
    this.logY = logY;
    // The hover is where the pointer is now: a live plot scrolls under a still
    // pointer. Beside the plot, over a y axis, it reads no time.
    const hx = this.hover?.x ?? null;
    const overTime = hx !== null && hx >= P.x && hx <= P.x + P.w;
    this.setHover(overTime ? this.dataAt(hx) : null);
    const marks = this.markers();
    const inView = (v) => v !== null && v >= xd[0] && v <= xd[1];
    const vline = (v, style, dash = []) => {
      const px = Math.round(sx(v)) + 0.5;
      ctx.strokeStyle = style;
      ctx.setLineDash(dash);
      for (const lane of geo.lanes) {
        ctx.beginPath();
        ctx.moveTo(px, lane.y);
        ctx.lineTo(px, lane.y + lane.h);
        ctx.stroke();
      }
      ctx.setLineDash([]);
    };
    // A paused plot's history, as a scrollbar along its foot: what it
    // captured, from the oldest time the hub holds to the moment it paused,
    // and the window a thumb over it, as wide as the window is a share of it.
    // A seek, dragging along the time axis, moves it. Over the time axis, or
    // while a seek goes on, it takes the axis's band and says what it spans,
    // clear of the X cursors' tabs where they rest on the axis. Drawn before
    // the cursors, whose names on the axis show over it.
    const open = this.seeking || (!this.view.live && this.axisHover !== null);
    if ((this.seeking || !this.view.live) && !slice && this.history) {
      const [h0, h1] = this.history;
      const [top, height] = open ? [P.y + P.h + AXIS_BELOW + 2, 18] : [body.y + body.h - SEEK_BAR, SEEK_BAR];
      const at = (t) => P.x + ((Math.min(Math.max(t, h0), h1) - h0) / (h1 - h0)) * P.w;
      ctx.fillStyle = T.seekBg;
      ctx.fillRect(P.x, top, P.w, height);
      const x1 = Math.max(at(xd[1]), P.x + SEEK_MIN), x0 = Math.min(at(xd[0]), x1 - SEEK_MIN);
      ctx.fillStyle = T.seekWindow;
      ctx.fillRect(x0, top, x1 - x0, height);
      ctx.strokeStyle = T.seekEdge;
      ctx.strokeRect(Math.round(x0) + 0.5, top + 0.5, Math.round(x1 - x0) - 1, height - 1);
      if (open && !this.view.live) {
        // Where the capture begins, how long it is, and when it paused, as
        // much of that as fits: on a narrow plot the start goes, then the
        // milliseconds, then the pause.
        const span = `${spanText(h1 - h0)} captured`, start = time.readout(h0);
        const paused = (t) => `paused ${t}`, whole = time.readout(h1), short = whole.replace(/\.\d+(?= s$|$)/, '');
        const inset = xTabsBeside ? 6 : 2 * TAB.w + 10, room = P.w - inset - 6, gap = 16, w = (s) => ctx.measureText(s).width;
        // The span sits centred between the start and the pause, or beside the pause alone.
        const fits = ([l, c, r]) => (l ? 2 * Math.max(w(l), w(r)) + w(c) + 2 * gap : r ? w(c) + gap + w(r) : w(c)) <= room;
        const layout =
          [
            [start, span, paused(whole)],
            [null, span, paused(whole)],
            [null, span, paused(short)],
          ].find(fits) ?? [null, span, null];
        const [l, c, r] = layout;
        ctx.fillStyle = T.tipText;
        const y = top + 13;
        if (l) (ctx.textAlign = 'left'), ctx.fillText(l, P.x + inset, y);
        if (r) (ctx.textAlign = 'right'), ctx.fillText(r, P.x + P.w - 6, y);
        // The span centred, or, beside the pause alone, left.
        ctx.textAlign = l || !r ? 'center' : 'left';
        ctx.fillText(c, l || !r ? P.x + P.w / 2 : P.x + inset, y);
      }
    }

    this.drawCursors(ctx, T, P, body, marks, axes, xd, sx, time, xTabsBeside, hoverLane);
    // A range being marked to zoom to: shaded through every lane, edged.
    if (this.zoomBand) {
      const [l, r] = [Math.min(...this.zoomBand), Math.max(...this.zoomBand)];
      ctx.fillStyle = T.seekWindow;
      ctx.strokeStyle = T.seekEdge;
      for (const lane of geo.lanes) {
        ctx.fillRect(l, lane.y, r - l, lane.h);
        ctx.strokeRect(Math.round(l) + 0.5, lane.y + 0.5, Math.round(r - l), lane.h - 1);
      }
    }
    if (inView(marks.hover)) vline(marks.hover, T.crosshair);
    if (this.zone > 0) {
      const px = P.x + P.w - 1, top = P.y + TAB.h + 2;
      ctx.save();
      ctx.globalAlpha = this.zone;
      ctx.strokeStyle = T.live;
      ctx.lineWidth = 1.5;
      for (const lane of geo.lanes) {
        ctx.beginPath();
        ctx.moveTo(px, Math.max(lane.y, top));
        ctx.lineTo(px, lane.y + lane.h);
        ctx.stroke();
      }
      const w = ctx.measureText('LIVE').width + 8;
      ctx.fillStyle = T.live;
      ctx.fillRect(P.x + P.w - w, P.y + 1, w, TAB.h);
      ctx.fillStyle = T.liveText;
      ctx.textAlign = 'center';
      ctx.fillText('LIVE', P.x + P.w - w / 2, P.y + 1 + TAB.h / 2 + 4);
      ctx.restore();
    }
    // The crosshair's horizontal line, in the lane under the pointer, with the
    // pointer's value tagged on each of that lane's y axes, just inside the
    // plot, clear of a pointer on the axis and of the axis's labels.
    if (hoverLane) {
      const py = Math.round(hy) + 0.5;
      ctx.strokeStyle = T.crosshair;
      ctx.beginPath();
      ctx.moveTo(hoverLane.x, py);
      ctx.lineTo(hoverLane.x + hoverLane.w, py);
      ctx.stroke();
      // The pointer's tags have one place each: against their side of the
      // lane at the pointer's height, those of several axes side by side. One
      // that would cover a cursor's tag there is not drawn, at whatever
      // height they would touch; it never moves aside.
      const cursorTags = this.pressable.filter((p) => p.text && (p.alpha ?? 1) > 0);
      this.tagsPlaced = [];
      for (const a of axes) {
        if (a.lane !== hoverLane) continue;
        const v = yScale(a.range, a.lane).invert(hy);
        const text = numberText(v, formOf(Y_AUTO.tag[kind], a.format, a.units));
        const w = ctx.measureText(text).width + 8;
        const box = { x: this.tagSlot(a.lane, a.side, py, w, 16), y: py - 8, w, h: 16 };
        if (cursorTags.some((p) => overlaps(p, box))) continue;
        this.pressable.push({ part: 'pointer', through: true, key: a.key, value: text, ...box });
        ctx.fillStyle = a.color;
        ctx.fillRect(box.x, box.y, w, 16);
        ctx.fillStyle = T.tagText;
        ctx.textAlign = 'center';
        ctx.fillText(text, box.x + w / 2, py + 4);
      }
    }

    // Each spectrum's white noise floor, as the hub estimates it: a dashed
    // line across its lane, unless its signal's floor is turned off.
    if (spectrum)
      for (const p of plans) {
        if (!p.s.floor || cfg.floorsHidden?.includes(p.t.key)) continue;
        const lane = geo.lanes[this.laneOf(cfg, p.index)];
        const y = Math.round(yScale(ranges.get(this.rangeKey(cfg, p.t)), lane)(p.s.floor)) + 0.5;
        if (y < lane.y || y > lane.y + lane.h) continue;
        ctx.save();
        ctx.strokeStyle = colorOf(p.t, this.colors);
        ctx.globalAlpha = 0.75;
        ctx.setLineDash([5, 4]);
        ctx.beginPath();
        ctx.moveTo(P.x, y);
        ctx.lineTo(P.x + P.w, y);
        ctx.stroke();
        ctx.restore();
      }

    // Readouts, which the page shows in the plot's header: each trace's value
    // at the hover, or its newest value while live, and its values at A and
    // B with B - A. Each is written as its y axis's settings say, and a
    // slice's x as its x axis's.
    const fx = slice ? this.fText() : time.readout;
    const yText = (key, place, units) => this.yText(key, place, units);
    const rows = {};
    // Where a pixel's column holds several points, the hover reads all of
    // them, lowest to highest, as one point there would be any of them.
    const halfPx = slice || marks.hover === null ? 0 : (xd[1] - xd[0]) / P.w / 2;
    const now = clock.now(), held = this.hoverRead;
    const hold = held && this.hover && held.x === this.hover.x && held.y === this.hover.y && now - held.at < READ_MS;
    // What the numbers say, held between reads; the marker reads anew.
    const reads = hold ? held.reads : new Map();
    let near = null; // the signal whose point at the hover is nearest the pointer, in the pointer's lane
    plans.forEach((p) => {
      const lane = this.laneOf(cfg, p.index);
      const sy = yScale(ranges.get(this.rangeKey(cfg, p.t)), geo.lanes[lane]);
      const at = (v) => (v === null ? null : slice ? spectrumAt(p.s, v) : traceAt(p.src, v));
      const hover = at(marks.hover);
      const span = halfPx && hover ? traceSpan(p.src, marks.hover - halfPx, marks.hover + halfPx) : null;
      if (!hold) reads.set(p.t.key, { v: hover?.y, span });
      const read = reads.get(p.t.key);
      const pts = { hover, a: at(marks.a), b: at(marks.b) };
      if (pts.hover && hoverLane && geo.lanes[lane] === hoverLane) {
        const top = sy(span ? span.hi : pts.hover.y), bottom = sy(span ? span.lo : pts.hover.y);
        const d = hy < top ? top - hy : hy > bottom ? hy - bottom : 0;
        if (!near || d < near.d) near = { d, t: p.t, read };
      }
      if (span) {
        ctx.fillStyle = colorOf(p.t, this.colors);
        ctx.fillRect(sx(marks.hover) - 1.5, sy(span.hi) - 1.5, 3, sy(span.lo) - sy(span.hi) + 3);
      }
      for (const [name, color] of [['hover', colorOf(p.t, this.colors)], ['a', T.cursorA], ['b', T.cursorB]]) {
        const pt = pts[name];
        if (!pt || (name === 'hover' && span)) continue;
        ctx.fillStyle = color;
        ctx.beginPath();
        ctx.arc(sx(pt.x), sy(pt.y), 3, 0, Math.PI * 2);
        ctx.fill();
      }
      const key = this.rangeKey(cfg, p.t), fy = yText(key, 'legend');
      let value = '';
      if (pts.hover && read?.v !== undefined) value = fy(read.v);
      else if (!slice && this.view.live && p.t.length) value = fy(p.t.yAt(p.t.absEnd - 1));
      // Where the points come from: raw samples, or how many raw rows per point; a capture's, how many it is of.
      let info = p.t.shots ? `${p.s.f.length} points` : `${p.t.meta.rate} Hz`;
      if (!slice) info += p.fromWindow ? (p.src.ratio === 1 ? ' · raw' : ` · 1:${p.src.ratio}`) : p.level ? ` · live, level ${p.level}` : ' · live';
      const fv = yText(key, 'value', p.t.meta.units);
      const cell = (pt) => (pt ? fv(pt.y) : '—');
      // A spectrum's floor, which its chip shows in place of a value, or a dash while there is none.
      const floor = spectrum ? (p.s.floor ? yText(key, 'floor')(p.s.floor) : '—') : null;
      rows[p.t.key] = { value, floor, info, note: p.s?.note ?? '', a: cell(pts.a), b: cell(pts.b), d: pts.a && pts.b ? fv(pts.b.y - pts.a.y) : '—' };
    });
    // The cursors' readout: where X1 and X2 are, with each signal's value
    // there, and each y axis's Y1 and Y2, a spectrum's also as a ratio in dB.
    const delta = marks.a !== null && marks.b !== null;
    const xs =
      marks.a === null && marks.b === null
        ? null
        : {
            v: spectrum ? 'f' : slice ? 'x' : 't',
            a: marks.a === null ? '—' : fx(marks.a),
            b: marks.b === null ? '—' : fx(marks.b),
            d: delta ? (slice ? numberText(marks.b - marks.a, formOf(X_AUTO[cfg.kind].span, xFormat), slice.unit) : si(marks.b - marks.a, 's')) : '—',
            rate: delta && !slice && marks.b !== marks.a ? si(1 / Math.abs(marks.b - marks.a), 'Hz') : '',
          };
    const ys = [];
    for (const a of axes) {
      const yc = this.yPair(a.key);
      if (!yc || (yc.a === null && yc.b === null)) continue;
      const fv = this.yValueText(a.key);
      const both = yc.a !== null && yc.b !== null;
      ys.push({
        key: a.key,
        name: a.plans.map((p) => titleOf(p.t)).join(', '),
        color: a.key === 'shared' ? null : a.color,
        a: yc.a === null ? '—' : fv(yc.a),
        b: yc.b === null ? '—' : fv(yc.b),
        d: both ? fv(yc.b - yc.a) : '—',
        ratio: both && spectrum && yc.a > 0 && yc.b > 0 ? `${(20 * Math.log10(yc.b / yc.a)).toFixed(2)} dB` : '',
      });
    }
    const cursors = xs || ys.length ? { x: xs, ys } : null;
    this.legend = { rows, cursors };
    if (!hold) this.hoverRead = this.hover ? { x: this.hover.x, y: this.hover.y, at: now, reads } : null;
    if (near) {
      const u = near.t.meta.units, key = this.rangeKey(cfg, near.t);
      const { v, span } = near.read ?? {};
      const value = v === undefined ? '' : span ? rangeText(span.lo, span.hi, this.yForm(key, 'value'), u) : yText(key, 'value', u)(v);
      this.drawTip(ctx, T, hoverLane, near, value, sx(marks.hover), hy);
    }
    if (!slice) this.drawWindow(ctx, T, P, open, frameAt);
  }

  /**
   * A time plot's window, on the time axis's right end: a dot, the primary
   * colour while live and a grey square while paused, and the seconds shown.
   * It shows while the pointer is on the time axis, unless the history bar
   * has the axis, and for WINDOW_SHOWN_MS after the window changes.
   */
  drawWindow(ctx, T, P, historyOpen, frameAt) {
    // It reads the seconds shown, and shows itself as the span asked of the view changes.
    const span = this.view.shown(frameAt), asked = this.view.span;
    if (this.shownSpan !== asked) {
      if (this.shownSpan !== undefined) this.windowUntil = frameAt + WINDOW_SHOWN_MS;
      this.shownSpan = asked;
    }
    if (historyOpen || (this.axisHover === null && !(frameAt < this.windowUntil))) return;
    const text = windowText(span), H = 16;
    const w = ctx.measureText(text).width + 22, x = P.x + P.w - w, y = P.y + P.h + AXIS_BELOW + 3;
    ctx.fillStyle = T.tipBg;
    ctx.fillRect(x, y, w, H);
    ctx.strokeStyle = T.tipBorder;
    ctx.strokeRect(x + 0.5, y + 0.5, w - 1, H - 1);
    if (this.view.live) {
      ctx.fillStyle = T.live;
      ctx.beginPath();
      ctx.arc(x + 9, y + H / 2, 3, 0, Math.PI * 2);
      ctx.fill();
    } else {
      ctx.fillStyle = T.axisText;
      ctx.fillRect(x + 6, y + H / 2 - 3, 6, 6);
    }
    ctx.fillStyle = T.tipText;
    ctx.textAlign = 'left';
    ctx.fillText(text, x + 16, y + 12);
  }

  /**
   * Lays out the cursors as parts of `pressable`, after the regions under
   * them, and draws them from it in its order: every line, then the mark on
   * what a cursor dragged with Shift snapped to, then every tag, then the
   * tabs, so a tag lies over any line and what lies over another takes the
   * press there.
   *
   * X1 and X2 are lines through every lane, named just above the x axis,
   * their place at the top of the plot beside the line, and where the tags
   * of the two would come near each other, each on its line's outer side;
   * Y1 and Y2 lines across their
   * axis's lane, named just inside it beside the axis, their level next to
   * the name; the span between a pair shaded. Every tag is inside the plot,
   * so none covers a tick label, and each line runs on CURSOR_STUB px past
   * the plot's edge into its axis, as a tick mark does, to be set against
   * one. A cursor out of view is named at the edge it lies beyond, with an
   * arrow, by a tag to drag it back into view by. A cursor not yet placed
   * rests as a tab to drag out, shown while the pointer is on its axis: X1
   * and X2 at the x axis's left end, and Y1 with Y2 above it at the foot of
   * each y axis, at the axis's outer edge, above the time labels and clear
   * of a header floating over the plot's top. A stacked plot has a pair at
   * the foot of each lane; a lane with room for one tab alone has that of
   * the first cursor not placed, and a lane shorter than a tab has none. A
   * pull across an axis draws a cursor out too.
   */
  drawCursors(ctx, T, P, body, marks, axes, xd, sx, time, xTabsBeside, hoverLane) {
    const color = { a: T.cursorA, b: T.cursorB };
    const xName = { a: 'X1', b: 'X2' }, yName = { a: 'Y1', b: 'Y2' };
    const inView = (v) => v !== null && v >= xd[0] && v <= xd[1];
    const fx = this.slice ? this.fText() : time.readout;
    // The box and text of a tag centred on (cx, cy), or, with `align`, beside cx on that side.
    const tag = (cx, cy, text, align = 'center') => {
      const w = Math.max(TAB.w, ctx.measureText(text).width + 6);
      const x = align === 'left' ? cx : align === 'right' ? cx - w : cx - w / 2;
      return { x: Math.round(x), y: Math.round(cy - TAB.h / 2), w, h: TAB.h, text };
    };
    // An X cursor's tag where it is laid: at `box`, or, where that would cover
    // a tag laid before it, `row` px further into the plot, a row at a time.
    // The tags laid along the lanes' sides after it stand clear of it (`tagSlot`).
    const laid = (box, row) => {
      const covers = (b) => this.tagsPlaced.some((t) => b.x < t.x + t.w && t.x < b.x + b.w && b.y < t.cy + t.h / 2 && t.cy - t.h / 2 < b.y + b.h);
      while (covers(box) && box.y + row >= P.y && box.y + row + box.h <= P.y + P.h) box = { ...box, y: box.y + row };
      const cy = box.y + box.h / 2;
      this.tagsPlaced.push({ lane: this.geo.lanes.find((l) => cy >= l.y && cy <= l.y + l.h), side: null, cy, x: box.x, w: box.w, h: box.h });
      return box;
    };
    const ROW = TAB.h + 2;
    const lines = [], tags = [], tabs = [];
    this.cursorStubs = [];
    if (marks.a !== null && marks.b !== null) {
      const l = Math.max(P.x, sx(Math.min(marks.a, marks.b))), r = Math.min(P.x + P.w, sx(Math.max(marks.a, marks.b)));
      if (r > l) {
        ctx.fillStyle = T.span;
        for (const lane of this.geo.lanes) ctx.fillRect(l, lane.y, r - l, lane.h);
      }
    }
    for (const a of axes) {
      const yc = this.yPair(a.key);
      if (!yc || yc.a === null || yc.b === null) continue;
      const sy = this.axisScale(a), y0 = sy(yc.a), y1 = sy(yc.b);
      const t = Math.max(a.lane.y, Math.min(y0, y1)), b = Math.min(a.lane.y + a.lane.h, Math.max(y0, y1));
      if (b > t) {
        ctx.fillStyle = T.span;
        ctx.fillRect(a.lane.x, t, a.lane.w, b - t);
      }
    }
    const xTagY = P.y + P.h - TAB.h / 2 - 1, xValueY = P.y + TAB.h / 2 + 2;
    const within = (v, lo, hi) => Math.min(hi, Math.max(lo, v));
    // The X cursors in view, each with its tags where they sit with the other cursor away: its name
    // centred on its line, held inside the plot, and its value right of the line, or left of it at the plot's right edge.
    const seen = ['a', 'b'].filter((name) => inView(marks[name])).map((name) => {
      const px = Math.round(sx(marks[name])) + 0.5, text = fx(marks[name]), flip = px + ctx.measureText(text).width + 10 > P.x + P.w;
      return { name, px, text, nameTag: tag(within(px, P.x + TAB.w / 2, P.x + P.w - TAB.w / 2), xTagY, xName[name]), valueTag: tag(flip ? px - 2 : px + 2, xValueY, text, flip ? 'right' : 'left') };
    });
    // Two whose tags would come near each other have them on their outer sides, the left cursor's left of its
    // line and the right one's right of its, whichever of X1 and X2 each is, so each stays against its own line.
    // Staying inside the plot comes first: a value with no room on its outer side stays where it sits alone, and
    // a name is held in at the edge. Tags that still meet there are laid in rows (`laid`).
    if (seen.length === 2) {
      const [l, r] = seen[0].px <= seen[1].px ? seen : [seen[1], seen[0]];
      if (this.parted('x value', l.valueTag, r.valueTag)) {
        const left = tag(l.px - 2, xValueY, l.text, 'right'), right = tag(r.px + 2, xValueY, r.text, 'left');
        if (left.x >= P.x) l.valueTag = left;
        if (right.x + right.w <= P.x + P.w) r.valueTag = right;
      }
      if (this.parted('x name', l.nameTag, r.nameTag)) {
        const left = tag(l.px, xTagY, xName[l.name], 'right'), right = tag(r.px, xTagY, xName[r.name], 'left');
        const held = (b) => ({ ...b, x: within(b.x, Math.ceil(P.x), Math.floor(P.x + P.w - b.w)) });
        [l.nameTag, r.nameTag] = [held(left), held(right)];
      }
    } else this.tagsParted = {};
    for (const name of ['a', 'b']) {
      const v = marks[name];
      if (v === null) continue;
      const cursor = { kind: 'x', name, fill: color[name] };
      const c = seen.find((s) => s.name === name);
      if (c) {
        const px = c.px;
        const runs = [...this.geo.lanes.map((lane) => [px, lane.y, px, lane.y + lane.h]), [px, P.y + P.h, px, P.y + P.h + CURSOR_STUB]];
        lines.push({ part: 'line', ...cursor, at: px, runs, x: px - CURSOR_GRAB, y: P.y, w: 2 * CURSOR_GRAB, h: P.h + CURSOR_STUB });
        this.cursorStubs.push({ x0: px, y0: P.y + P.h, x1: px, y1: P.y + P.h + CURSOR_STUB });
        tags.push({ part: 'tag', ...cursor, at: px, ...laid(c.nameTag, -ROW) });
        tags.push({ part: 'tag', ...cursor, at: px, ...laid(c.valueTag, ROW) });
      } else {
        const step = name === 'b' ? TAB.w + 10 : 0;
        const box = v < xd[0] ? tag(P.x + TAB.w / 2 + 4 + step, xTagY, `‹${xName[name]}`) : tag(P.x + P.w - TAB.w / 2 - 4 - step, xTagY, `${xName[name]}›`);
        tags.push({ part: 'tag', ...cursor, edge: true, ...laid(box, -ROW) });
      }
    }
    for (const a of axes) {
      const yc = this.yPair(a.key);
      if (!yc) continue;
      const sy = this.axisScale(a);
      // The lane's edge on the axis's side, which its tags sit just inside and its lines run on past.
      const left = a.side === 'left', edge = left ? a.lane.x : a.lane.x + a.lane.w;
      const fv = this.yValueText(a.key);
      for (const name of ['a', 'b']) {
        const v = yc[name];
        const py = v === null ? NaN : sy(v);
        if (!isFinite(py)) continue;
        const cursor = { kind: 'y', name, key: a.key, axis: a, fill: color[name] };
        if (py >= a.lane.y && py <= a.lane.y + a.lane.h) {
          const y = Math.round(py) + 0.5;
          const x0 = a.lane.x - (left ? CURSOR_STUB : 0), x1 = a.lane.x + a.lane.w + (left ? 0 : CURSOR_STUB);
          lines.push({ part: 'line', ...cursor, at: py, runs: [[x0, y, x1, y]], x: x0, y: y - CURSOR_GRAB, w: x1 - x0, h: 2 * CURSOR_GRAB });
          this.cursorStubs.push({ x0: edge, y0: y, x1: edge + (left ? -CURSOR_STUB : CURSOR_STUB), y1: y });
          // Its name, then its level, side by side from the axis inward, kept within the lane.
          const cy = within(py, a.lane.y + TAB.h / 2, a.lane.y + a.lane.h - TAB.h / 2);
          tags.push({ part: 'tag', ...cursor, at: py, ...tag(this.tagSlot(a.lane, a.side, cy, TAB.w, TAB.h), cy, yName[name], 'left') });
          const text = fv(v), w = Math.max(TAB.w, ctx.measureText(text).width + 6);
          tags.push({ part: 'tag', ...cursor, at: py, ...tag(this.tagSlot(a.lane, a.side, cy, w, TAB.h), cy, text, 'left') });
        } else {
          const step = name === 'b' ? TAB.h + 2 : 0;
          const [cy, text] = py < a.lane.y ? [a.lane.y + TAB.h / 2 + 1 + step, `${yName[name]}˄`] : [a.lane.y + a.lane.h - TAB.h / 2 - 1 - step, `${yName[name]}˅`];
          const w = Math.max(TAB.w, ctx.measureText(text).width + 6);
          tags.push({ part: 'tag', ...cursor, edge: true, ...tag(this.tagSlot(a.lane, a.side, cy, w, TAB.h), cy, text, 'left') });
        }
      }
    }
    // The tabs of the cursors not yet placed, X1 and X2 beside the plot under the y axis when there is room.
    ['a', 'b'].forEach((name, i) => {
      if (marks[name] !== null) return;
      const x = (xTabsBeside ? P.x - 2 * TAB.w - 4 : P.x + 2) + i * (TAB.w + 2);
      tabs.push({ part: 'tab', kind: 'x', name, fill: color[name], text: xName[name], x, y: P.y + P.h + AXIS_BELOW + 3, w: TAB.w, h: TAB.h });
    });
    // As many as the lane has room for below a floating header, as the frame's geometry decided, which made the axis wide enough for them.
    for (const a of axes) {
      const yc = this.yPair(a.key);
      const resting = ['a', 'b'].filter((name) => !yc || yc[name] === null);
      const room = this.geo.tabs[this.geo.lanes.indexOf(a.lane)];
      const x = a.side === 'left' ? a.x0 + 2 : a.x1 - TAB.w - 2;
      for (const name of room >= 2 ? resting : resting.slice(0, room)) {
        const up = room >= 2 && name === 'b' ? TAB.h + 2 : 0;
        tabs.push({ part: 'tab', kind: 'y', name, key: a.key, axis: a, fill: color[name], text: yName[name], x, y: a.lane.y + a.lane.h - 2 - TAB.h - up, w: TAB.w, h: TAB.h });
      }
    }
    // While a cursor dragged with Shift is snapped, a mark on what it snapped
    // to: a ring on the signal's point; an outline about the tick's number
    // or the other cursor's name; or, for a tick with no number or 0, a ring
    // on the cursor's line at the pointer.
    const snaps = [], s = this.snapped, hov = this.hover;
    if (s) {
      const axis = s.kind === 'x' ? s.point && axes.find((a) => a.plans.some((p) => p.t.key === s.key)) : axes.find((a) => a.key === s.axisKey);
      const along = s.kind === 'x' ? this.pxOf(s.v) : axis && this.axisScale(axis)(s.v);
      const mark = { part: 'snap', through: true, name: s.name, fill: color[s.name] };
      const ring = (cx, cy) => ({ ...mark, cx, cy, x: cx - SNAP_RING, y: cy - SNAP_RING, w: 2 * SNAP_RING, h: 2 * SNAP_RING });
      const about = (b) => ({ ...mark, x: b.x - 2, y: b.y - 2, w: b.w + 4, h: b.h + 4 });
      const label = s.from === 'tick' && this.tickLabels.find((b) => (s.kind === 'x' ? b.axis === undefined && Math.abs(b.x + b.w / 2 - along) < 1 : b.axis === s.axisKey && Math.abs(b.y + b.h / 2 - along) < 2));
      const named = s.from === 'cursor' && tags.find((t) => t.kind === s.kind && t.name !== s.name && t.key === s.axisKey && !t.edge && t.text === (s.kind === 'x' ? xName : yName)[t.name]);
      const pt = s.point && axis && [this.pxOf(s.point.x), this.axisScale(axis)(s.point.y)];
      const it = pt ? ring(...pt) : label ? about(label) : named ? about(named) : hov ? ring(...(s.kind === 'x' ? [along, hov.y] : [hov.x, along])) : null;
      if (it && [it.x, it.y].every(Number.isFinite)) snaps.push(it);
    }
    this.pressable.push(...lines, ...snaps, ...tags, ...tabs);
    // A tab shows while the pointer is on it, plainly, or on its axis; or on another tab or edge tag of its y axis.
    const on = hov ? this.pressAt(hov.x, hov.y) : null;
    const overXAxis = !!hov && hov.y > P.y + P.h && hov.x >= body.x && hov.x <= body.x + body.w;
    const overAxis = hov ? this.yAxisAt(hov.x, hov.y) : null;
    for (const t of tabs) {
      const beside = t.kind === 'x' ? overXAxis : (overAxis && overAxis.side === t.axis.side && overAxis.slot === (t.axis.slot ?? 0) && t.axis.lane === hoverLane) || ((on?.part === 'tab' || on?.edge) && on.axis === t.axis);
      t.alpha = t === on ? 1 : beside ? 0.6 : 0;
    }
    for (const p of this.pressable) {
      if (p.part === 'line') {
        ctx.strokeStyle = p.fill;
        ctx.beginPath();
        for (const [x0, y0, x1, y1] of p.runs) {
          ctx.moveTo(x0, y0);
          ctx.lineTo(x1, y1);
        }
        ctx.stroke();
      } else if (p.part === 'snap') {
        ctx.strokeStyle = p.fill;
        ctx.lineWidth = 1.5;
        ctx.beginPath();
        if (p.cx === undefined) ctx.rect(p.x + 0.5, p.y + 0.5, p.w - 1, p.h - 1);
        else ctx.arc(p.cx, p.cy, SNAP_RING, 0, Math.PI * 2);
        ctx.stroke();
        ctx.lineWidth = 1;
      } else if (p.text && (p.alpha ?? 1) > 0) {
        ctx.globalAlpha = p.alpha ?? 1;
        ctx.fillStyle = p.fill;
        ctx.fillRect(p.x, p.y, p.w, p.h);
        ctx.fillStyle = T.tagText;
        ctx.textAlign = 'center';
        ctx.fillText(p.text, p.x + p.w / 2, p.y + TAB.h / 2 + 4);
        ctx.globalAlpha = 1;
      }
    }
  }

  /**
   * The fraction across the plot past which zooming in leaves a live view
   * live: from a little left of the header's actions, so the header's
   * buttons mark the newest stretch with room to spare, though never more
   * than its newer half; or LIVE_EDGE
   * where the plot shows no header.
   */
  liveEdge() {
    const P = this.geo?.plot;
    const actions = this.body.closest('.plot-card')?.querySelector('.plot-head > .plot-actions')?.getBoundingClientRect();
    if (!P || !actions?.width) return LIVE_EDGE;
    return Math.min(LIVE_EDGE, Math.max(0.5, (actions.left - LIVE_EDGE_BUFFER - P.x) / P.w));
  }

  /** Whether (x, y) is on the plot's lanes, where a Ctrl+drag marks a range of x to zoom to. */
  inPlot(x, y) {
    const P = this.geo?.plot;
    return !!P && x >= P.x && x <= P.x + P.w && y >= P.y && y <= P.y + P.h;
  }

  /** Zooms to the x range between client px x0 and x1. */
  zoomTo(x0, x1) {
    this.showRange(this.dataAt(Math.min(x0, x1)), this.dataAt(Math.max(x0, x1)));
  }

  /** Shows x from `a` to `b`: a time plot's views that time, paused; a slice that stretch of its x. */
  showRange(a, b) {
    if (a === null || b === null || !(b > a)) return;
    if (isSlice(this.config().kind)) this.onFrequencies?.(a, b);
    else for (const v of this.targets()) v.show(a, b, clock.now());
  }

  /** Zooms to the time or frequencies between X1 and X2, when both are placed, as one view step. The cursors stay. */
  zoomToCursors() {
    const { a, b } = this.markers();
    if (a !== null && b !== null) this.viewHistory.step(() => this.showRange(Math.min(a, b), Math.max(a, b)));
  }

  /**
   * What a press at (x, y) lands on: of `pressable`, the one drawn last
   * there that a press does not go through, which lies over the others, or
   * null where there is none. With
   * `parts`, only those parts count, as for what lies under a cursor.
   * Where the plot's body is not where it last drew, as in the frame after
   * a layout change, the plot draws first, so the press is read against
   * where everything is now.
   */
  pressAt(x, y, parts = null) {
    const d = this.drawnOn, r = d && this.body.getBoundingClientRect();
    if (d && (r.left !== d.x || r.top !== d.y || r.width !== d.w || r.height !== d.h)) this.draw(clock.now());
    return this.pressable.findLast((p) => !p.through && (!parts || parts.includes(p.part)) && x >= p.x && x < p.x + p.w && y >= p.y && y < p.y + p.h) ?? null;
  }

  /**
   * Whether a pair of tags, `key` naming the pair, part to their outer
   * sides, given the boxes `a` and `b` they have side by side: once those
   * come within TAG_NEAR px of each other along x, and from then until they
   * are TAG_HELD px further apart, so a pair held at the distance where
   * they part does not change sides back and forth.
   */
  parted(key, a, b) {
    const gap = Math.max(a.x, b.x) - Math.min(a.x + a.w, b.x + b.w);
    return (this.tagsParted[key] = gap < TAG_NEAR + (this.tagsParted[key] ? TAG_HELD : 0));
  }

  /**
   * How many tabs of Y cursors `lane` has room for, one above another from
   * its foot, CLEAR px about each, below a floating header whose foot is at
   * client y `head`. As with its numbers (`laneTicks`), what does not fit in
   * the lane with that room about it is not drawn.
   */
  tabRoom(lane, head) {
    return Math.max(0, Math.floor((lane.y + lane.h - Math.max(lane.y, head) - CLEAR) / (TAB.h + CLEAR)));
  }

  /**
   * The left edge of a tag `w` wide and `h` high centred at height `cy`,
   * laid along `side` of `lane`: against the side, or further in past the
   * tags already laid there that it would overlap, so tags at one height
   * stand side by side.
   */
  tagSlot(lane, side, cy, w, h) {
    let x = side === 'left' ? lane.x + 1 : lane.x + lane.w - 1 - w;
    const near = this.tagsPlaced.filter((t) => t.lane === lane && (t.side === side || t.side === null) && Math.abs(t.cy - cy) < (t.h + h) / 2);
    for (let moved = true; moved; ) {
      moved = false;
      for (const t of near)
        if (x < t.x + t.w + 2 && x + w + 2 > t.x) {
          x = side === 'left' ? t.x + t.w + 2 : t.x - 2 - w;
          moved = true;
        }
    }
    this.tagsPlaced.push({ lane, side, cy, x, w, h });
    return x;
  }

  /** The y scale of a drawn y axis, value to client y. */
  axisScale(axis) {
    const range = this.logY ? axis.range.map((v) => 10 ** v) : axis.range;
    return (this.logY ? scaleLog() : scaleLinear()).domain(range).range([axis.lane.y + axis.lane.h, axis.lane.y]);
  }

  /** The value of y axis `key` at client y, or null without that axis. */
  yValueAt(key, clientY) {
    const a = this.yAxes.find((a) => a.key === key);
    return a ? this.axisScale(a).invert(clientY) : null;
  }

  // A command on the cursors gives the cursors the plot would have after
  // it; the page writes them, on every plot the command acts on, as one
  // edit (cursors.js CursorEdits).

  /** The plot's cursors with X1 and X2 a third of the way in from each side of the view; as they are before it has drawn an x axis. */
  xCursorsAdded() {
    const P = this.geo?.plot, kind = this.config().kind;
    if (!P || !this.xd) return this.placed;
    return withX(withX(this.placed, kind, 'a', this.dataAt(P.x + P.w / 3)), kind, 'b', this.dataAt(P.x + (2 * P.w) / 3));
  }

  /** The plot's cursors with Y1 and Y2 a third of the way in from the bottom and top of y axis `key`, or of the first axis. */
  yCursorsAdded(key) {
    const a = this.yAxes.find((a) => a.key === key) ?? this.yAxes[0], kind = this.config().kind;
    if (!a) return this.placed;
    const sy = this.axisScale(a);
    return withY(withY(this.placed, kind, a.key, 'a', sy.invert(a.lane.y + (2 * a.lane.h) / 3)), kind, a.key, 'b', sy.invert(a.lane.y + a.lane.h / 3));
  }

  /** The plot's cursors without X1 and X2. */
  xCursorsCleared() {
    return withoutX(this.placed, this.config().kind);
  }

  /** The plot's cursors without the Y cursors of y axis `key`, or with none, of every y axis. */
  yCursorsCleared(key) {
    return withoutY(this.placed, this.config().kind, key);
  }

  /** Whether y axis `key` has a Y cursor placed. */
  hasYCursors(key) {
    const yc = this.yPair(key);
    return yc.a !== null || yc.b !== null;
  }

  /** How many cursors are placed: X1 and X2, and each Y1 and Y2 of every kind. */
  cursorsPlaced() {
    return placedIn(this.placed, this.config().kind);
  }

  /** How many of the plot's y axes have a Y cursor placed. */
  yCursorAxes() {
    return this.yAxes.filter((a) => this.hasYCursors(a.key)).length;
  }

  /** The signals of y axis `key`, as the readout names them, or null while the plot draws no such axis. */
  axisName(key) {
    const a = this.yAxes.find((a) => a.key === key);
    return a ? a.plans.map((p) => titleOf(p.t)).join(', ') : null;
  }

  /** What the plot's x axis counts, for `cursorText`: 'hz' on a spectrum, a capture plot's unit as {unit}, else its first trace's epoch. */
  xKind() {
    const cfg = this.config();
    if (!isSlice(cfg.kind)) return this.traces()[0]?.meta.epoch ?? 'zero';
    return cfg.kind === 'spectrum' ? 'hz' : { unit: this.sliceAxes(cfg).unit };
  }

  /** The hover position and the X1 and X2 cursors, as a and b, in this plot's x units. */
  markers() {
    const kind = this.config().kind;
    return { hover: isSlice(kind) ? this.specHover : this.hoverTime.t, ...xOf(this.placed, kind) };
  }

  /** Sets the hover position: a slice's own, or the time every time plot marks. */
  setHover(value) {
    if (isSlice(this.config().kind)) this.specHover = value;
    else this.hoverTime.t = value;
  }

  /**
   * Shows X cursor `name` at `value`, or removed with null, while a drag
   * moves it, on every plot the drag moves it on (`cursorTargets`). A time
   * cursor holds a moment a live view would scroll away, so each of those
   * plots pauses as the drag first places it there: a press let go where it
   * was made pauses none.
   */
  draftX(name, value) {
    const kind = this.config().kind;
    for (const [p] of this.cursorTargets()) {
      if (kind === 'time' && value !== null) p.view.pause(clock.now());
      this.cursors.draft(p.plotId, withX(this.cursors.get(p.plotId), kind, name, value));
    }
  }

  /** Shows Y cursor `name` of y axis `key` at `value`, or removed with null, while a drag moves it, on the axis in its units of every plot the drag moves it on. */
  draftY(key, name, value) {
    const kind = this.config().kind;
    for (const [p, axis] of this.cursorTargets(key)) this.cursors.draft(p.plotId, withY(this.cursors.get(p.plotId), kind, axis, name, value));
  }

  /**
   * The signals of `plans`, as this frame draws them, for snap.js: a time
   * plot's points from what it draws them from, a slice's values, each
   * with what its features stand out by: a spectrum's peaks by decibels,
   * any other's peaks and troughs by a share of the y range shown.
   */
  snapSignals(plans) {
    if (this.spectrum)
      return plans.map(({ t, s }) => ({ key: t.key, n: s.f.length, x: (i) => s.f[i], y: (i) => s.a[i], stand: STAND.spectrumDb / 20, troughs: false, level: Math.log10 }));
    return plans.map(({ t, s, src, a0, a1 }) => {
      const range = this.yAxes.find((a) => a.plans.some((p) => p.t === t))?.range ?? [0, 0], stand = STAND.timeShare * (range[1] - range[0]);
      if (s) return { key: t.key, n: s.f.length, x: (i) => s.f[i], y: (i) => s.a[i], stand };
      return { key: t.key, n: Math.max(0, a1 - a0), x: (i) => src.xAt(a0 + i), y: (i) => src.yAt(a0 + i), stand };
    });
  }

  /**
   * What a cursor dragged with Shift held snaps to, from what the plot last
   * drew (snap.js), when without Shift it would be at client px `at` along
   * its axis with the pointer at client px `across` the other way: X cursor
   * `name`, or with `key` Y cursor `name` of that y axis. `held` is the id
   * of what it snapped to on the move before, or null. As
   * {v, from, id, key, point} with `v` where the cursor goes, or null to
   * follow the pointer.
   *
   * Each target is placed as the plot draws it: a tick of the x axis at its
   * mark on the plot's bottom edge, of a y axis at its mark on the edge of
   * the axis's gutter by the plot, and a signal's point where its lane
   * draws it.
   */
  snapAt(name, at, across, key, held = null) {
    const P = this.geo?.plot, xd = this.xd, plans = this.drawn?.plans;
    if (!P || !xd || !plans) return null;
    const other = name === 'a' ? 'b' : 'a';
    const pointer = { along: at, across };
    if (key === undefined) {
      const near = [this.dataAt(at - SNAP.jump), this.dataAt(at + SNAP.jump)];
      const candidates = snapCandidates({ axis: 'x', view: xd, ticks: this.xTicks, other: this.markers()[other], signals: this.snapSignals(plans), near });
      const drawnAt = (key, y) => {
        const axis = this.yAxes.find((a) => a.plans.some((p) => p.t.key === key));
        return axis ? this.axisScale(axis)(y) : null;
      };
      const placed = candidates.map((c) => ({ ...c, along: this.pxOf(c.v), across: c.from === 'tick' ? P.y + P.h : c.point ? drawnAt(c.key, c.point.y) : null }));
      return pickSnap(placed, pointer, held);
    }
    const axis = this.yAxes.find((a) => a.key === key);
    if (!axis) return null;
    const sy = this.axisScale(axis), { a, b } = this.markers(), edge = axis.side === 'left' ? axis.x1 : axis.x0;
    const candidates = snapCandidates({ axis: 'y', view: [sy.invert(axis.lane.y + axis.lane.h), sy.invert(axis.lane.y)], ticks: axis.ticks, other: this.yPair(key)[other], signals: this.snapSignals(axis.plans), xs: [a, b], xView: xd });
    const placed = candidates.map((c) => ({ ...c, along: sy(c.v), across: c.from === 'tick' ? edge : c.point ? this.pxOf(c.point.x) : null }));
    return pickSnap(placed, pointer, held);
  }

  /** The data x under clientX, and back. */
  dataAt(clientX) {
    const P = this.geo?.plot, xd = this.xd;
    if (!P || !xd) return null;
    const f = (clientX - P.x) / P.w;
    if (this.logX) return 10 ** (Math.log10(xd[0]) + f * (Math.log10(xd[1]) - Math.log10(xd[0])));
    return xd[0] + f * (xd[1] - xd[0]);
  }
  pxOf(v) {
    const P = this.geo?.plot, xd = this.xd;
    if (!P || !xd || v === null) return null;
    const f = this.logX ? (Math.log10(v) - Math.log10(xd[0])) / (Math.log10(xd[1]) - Math.log10(xd[0])) : (v - xd[0]) / (xd[1] - xd[0]);
    return P.x + f * P.w;
  }
  /**
   * What is under (x, y), for its context menu: a y axis and the range keys
   * it shows, the x axis, or the plot area and the range keys there.
   */
  regionAt(x, y) {
    const band = this.yAxisAt(x, y);
    if (band) return { kind: 'y', keys: this.axisTargets(band, y), side: band.side };
    if (this.pressAt(x, y, ['xaxis'])) return { kind: 'x' };
    return { kind: 'plot', keys: this.yTargets(y) };
  }

  /**
   * The units y axis `key` shows, as its signals' units, each once and in
   * order, a spectrum's per √Hz; or null while the plot draws no such axis.
   */
  axisUnits(key) {
    const a = this.yAxes.find((a) => a.key === key);
    if (!a) return null;
    return unitsOf(a.plans.map((p) => p.t.meta.units), this.config().kind === 'spectrum');
  }

  /** The form y axis `key` writes a value in at `place` of Y_AUTO, as its settings say, in the units it shows (units.js `formOf`). */
  yForm(key, place) {
    const cfg = this.config();
    return formOf(Y_AUTO[place][cfg.kind === 'spectrum' ? 'spectrum' : 'time'], axisFormat(cfg.yFormat?.[key]), this.axisUnits(key) ?? '');
  }

  /**
   * How y axis `key` writes a value at `place` of Y_AUTO, its cursors'
   * tags, the readout and the hover's tip at 'value': `units` after it on
   * a time plot, and none on a spectrum, which keeps its units apart.
   */
  yText(key, place, units = '') {
    const form = this.yForm(key, place), spectrum = this.config().kind === 'spectrum';
    return (v) => numberText(v, form, spectrum ? '' : units);
  }

  /** How y axis `key` writes a value of its own, as its Y cursors' tags and their readout do: in the units of its first signal. */
  yValueText(key) {
    return this.yText(key, 'value', this.yAxes.find((a) => a.key === key)?.plans[0]?.t.meta.units ?? '');
  }

  /** The unit y axis `key` fixes its numbers in, as {shift, text} (units.js `fixedUnit`), or undefined where it fixes none. */
  yFixed(key) {
    return fixedUnit(axisFormat(this.config().yFormat?.[key]), this.axisUnits(key) ?? '');
  }

  /** How a slice's x axis writes a value at a cursor's place, as its settings say, in its unit. */
  fText() {
    const cfg = this.config(), form = formOf((X_AUTO[cfg.kind] ?? F_AUTO).at, axisFormat(cfg.xFormat)), unit = this.sliceAxes(cfg).unit;
    return (f) => numberText(f, form, unit);
  }

  /** The key of this plot's y axis in `units`, as `axisUnits` names them: signal `key`'s axis when it is one of them, else the first; or null for none. */
  axisIn(units, key) {
    const axes = this.yAxes.filter((a) => this.axisUnits(a.key) === units);
    return (axes.find((a) => a.key === key) ?? axes[0])?.key ?? null;
  }

  /** The y range of `key` in the units its axis shows, or null before it has one. */
  yBounds(key) {
    const range = this.y.get(key)?.range;
    if (!range) return null;
    return this.logY ? range.map((v) => 10 ** v) : [...range];
  }

  /** Sets the y range of `keys` to [lo, hi], in the units the axis shows, to hold until a fit, as one view step. */
  setYBounds(keys, lo, hi) {
    this.viewHistory.step(() => {
      for (const k of keys) {
        let st = this.y.get(k);
        if (!st) this.y.set(k, (st = { range: null, auto: false }));
        st.auto = false;
        st.range = this.logY ? [Math.log10(lo), Math.log10(hi)] : [lo, hi];
      }
    });
  }

  /** Fits the y range of `keys` to what they show, as one view step. */
  fit(keys) {
    this.viewHistory.step(() => {
      for (const k of keys) {
        const st = this.y.get(k);
        if (st) this.refit(k, st);
      }
    });
  }

  /**
   * Fits `st`, the y range of key `k`: a time plot's at once, to what it
   * showed at the last frame, or on the next before it has shown any; a
   * spectrum's to follow what it shows.
   */
  refit(k, st) {
    const e = this.config().kind === 'time' ? this.extents?.get(k) : null, want = e && fitted(...e);
    if (want) Object.assign(st, { range: want, auto: false });
    else st.auto = true;
  }

  /** The y axis band (x, y) is over, {side, slot, x0, x1, keys}, or null. */
  yAxisAt(x, y) {
    return this.pressAt(x, y, ['yaxis'])?.band ?? null;
  }

  /**
   * The range keys a gesture on y axis `band` at clientY acts on: an
   * independent axis's one signal, or what the lane there shows.
   */
  axisTargets(band, clientY) {
    return band.keys ?? this.yTargets(clientY);
  }

  /**
   * Which side's axes a signal dropped at (x, y) would join, on an
   * independent plot, as {side, rect}: the gutter and a band of the plot
   * beside it, in client px. Null elsewhere, or on a plot of another layout.
   */
  axisDropAt(x, y) {
    const P = this.geo?.plot;
    if (this.config().mode !== 'independent' || !P || y < P.y || y > P.y + P.h) return null;
    const b = this.body.getBoundingClientRect();
    if (x < P.x + AXIS_DROP) return { side: 'left', rect: { x: b.left, y: P.y, w: P.x + AXIS_DROP - b.left, h: P.h } };
    if (x > P.x + P.w - AXIS_DROP) return { side: 'right', rect: { x: P.x + P.w - AXIS_DROP, y: P.y, w: b.right - (P.x + P.w - AXIS_DROP), h: P.h } };
    return null;
  }

  /** The range keys the y gestures at clientY act on. */
  yTargets(clientY) {
    const cfg = this.config(), traces = this.traces();
    if (cfg.mode === 'overlay') return ['shared'];
    if (cfg.mode === 'independent') return traces.map((t) => t.key);
    const lanes = this.geo?.lanes ?? [];
    const i = lanes.findIndex((l) => clientY >= l.y && clientY <= l.y + l.h);
    return i >= 0 && traces[i] ? [traces[i].key] : [];
  }

  bindInput() {
    const el = this.overlay;
    let drag = null;
    const fracX = (clientX) => {
      const P = this.geo?.plot;
      return P ? (clientX - P.x) / P.w : 0;
    };
    const isTime = () => this.config().kind === 'time';
    // The wheel zooms y over a y axis, or anywhere with Shift, about the
    // value under the pointer, and zooms x elsewhere: time, eased to a span
    // that reads well (`turnTime`); a spectrum's frequencies, about the one
    // under the pointer.
    el.addEventListener('wheel', (e) => {
      e.preventDefault();
      const factor = Math.exp(e.deltaY * 0.0015);
      const band = this.yAxisAt(e.clientX, e.clientY);
      if (band || e.shiftKey) {
        const keys = band ? this.axisTargets(band, e.clientY) : this.yTargets(e.clientY);
        const lane = this.geo?.lanes.find((l) => e.clientY >= l.y && e.clientY <= l.y + l.h) ?? this.geo?.lanes[0];
        if (!lane) return;
        const at = (lane.y + lane.h - e.clientY) / lane.h;
        // This plot's y alone, a burst of its own kind.
        this.viewHistory.burst('zoomY', [this.view], () => {
          for (const k of keys) {
            const st = this.y.get(k);
            if (!st?.range) continue;
            const [a, b] = st.range, v = a + at * (b - a);
            st.auto = false;
            st.range = [v - (v - a) * factor, v + (b - v) * factor];
          }
        });
      } else if (isTime()) this.turnTime(e.deltaMode === 1 ? e.deltaY * LINE_PX : e.deltaY, onAxis(e), fracX(e.clientX));
      else this.zoomFrequencies(factor, Math.min(1, Math.max(0, fracX(e.clientX))));
    }, { passive: false });
    // The band under the plot holds the x axis.
    const onAxis = (e) => !!this.pressAt(e.clientX, e.clientY, ['xaxis']);
    // Of a pair, the cursor a pull places: the first not placed, else the
    // nearer to the pointer by `dist`.
    const pick = (pair, dist) => (pair.a === null ? 'a' : pair.b === null ? 'b' : dist(pair.a) <= dist(pair.b) ? 'a' : 'b');
    // A drag's mode, by what its press landed on (`pressAt`): 'xcursor' and
    // 'ycursor', on a cursor's line, tag or tab, move the cursor, a line or
    // a tag held where it was pressed, and a tab, or the tag of a cursor out
    // of view, bringing its cursor under the pointer; 'axis' on the x
    // axis and 'y' on a y axis wait to learn their direction, a pull across
    // the axis into the plot drawing a cursor out of it, as a guide comes off
    // a ruler, and a drag along it seeking ('seek', the time axis only) or
    // offsetting ('y'); 'plot' pans; 'none' does nothing more. A drag, from
    // its press to its release, is one view step for what it moves of the
    // views, and one edit for a cursor it places, moves or removes.
    el.addEventListener('pointerdown', (e) => {
      this.stopTime();
      this.onFocus?.();
      if (e.button !== 0) return;
      el.setPointerCapture(e.pointerId);
      this.viewHistory.begin();
      const on = this.pressAt(e.clientX, e.clientY);
      const cursor = on?.kind ? on : null;
      const band = on?.part === 'yaxis' ? on.band : null;
      // With Shift a click selects the plot, so a press off the cursors does
      // nothing here: no pan or offset moves the plot by accident. On a
      // cursor's part it drags the cursor, snapping from its first move; let
      // go where it was pressed, it is a Shift+click as anywhere.
      const mode = cursor ? `${cursor.kind}cursor` : e.shiftKey ? 'none' : on?.part === 'xaxis' ? 'axis' : band ? 'y' : (e.ctrlKey || e.metaKey) && on?.part === 'plot' ? 'zoom' : 'plot';
      const targets = band ? this.axisTargets(band, e.clientY) : this.yTargets(e.clientY);
      // How far from the cursor the pointer holds it, along the cursor's axis.
      // Taken from where the cursor is on the scale a drag places it by, not from its line's place on the pixel grid.
      const grab = cursor?.at === undefined ? 0 : cursor.kind === 'x' ? e.clientX - this.pxOf(this.markers()[cursor.name]) : e.clientY - this.axisScale(cursor.axis)(this.yPair(cursor.key)[cursor.name]);
      drag = { x: e.clientX, y: e.clientY, x0: e.clientX, y0: e.clientY, moved: 0, mode, targets, band, cursor: cursor?.name, key: cursor?.key, grab, shift: e.shiftKey };
      clickAfter = true;
      unwatchShift?.();
      unwatchShift = watchShift(onShift);
    });
    // Turns an axis drag that has moved off the press into what its
    // direction says, and says whether it is still to decide.
    const decide = (e) => {
      const ax = e.clientX - drag.x0, ay = e.clientY - drag.y0;
      if (Math.hypot(ax, ay) < DRAG_START) return false;
      const across = drag.mode === 'axis' ? -ay > Math.abs(ax) : (drag.band.side === 'left' ? ax : -ax) > Math.abs(ay);
      if (across && drag.mode === 'axis') {
        // Up from the x axis: a Y cursor on the axis of the lowest lane.
        const lane = this.geo.lanes.at(-1);
        const key = lane && this.yTargets(lane.y + lane.h / 2)[0];
        if (!key) return (drag.mode = 'none'), true;
        const a = this.yAxes.find((a) => a.key === key);
        drag.mode = 'ycursor';
        drag.key = key;
        drag.cursor = pick(this.yPair(key), (v) => Math.abs(this.axisScale(a)(v) - e.clientY));
      } else if (across) {
        // In from a y axis: an X cursor.
        drag.mode = 'xcursor';
        drag.cursor = pick(this.markers(), (v) => Math.abs(this.pxOf(v) - e.clientX));
      } else if (drag.mode === 'axis') {
        if (isTime()) (drag.mode = 'seek'), (this.seeking = true);
        else drag.mode = 'none';
      } else drag.started = true;
      return true;
    };
    // A dragged cursor's place: where the pointer holds it, or, while Shift
    // is held, what it snaps to near there (`snapAt`), noted as `snapped`
    // for the mark drawn on it and as the target the next move holds to. Shift is read from the pointer as it moves,
    // and as it goes down or up mid-drag, which places the cursor at once.
    // A cursor held where it was pressed moves by the pointer's travel from
    // the press: its grab is taken from where it is, not where its line is
    // drawn on the pixel grid.
    const placeCursor = () => {
      const x = drag.mode === 'xcursor', at = (x ? drag.x : drag.y) - drag.grab;
      const snap = drag.shift ? this.snapAt(drag.cursor, at, x ? drag.y : drag.x, x ? undefined : drag.key, this.snapped?.id ?? null) : null;
      this.snapped = snap && { ...snap, kind: x ? 'x' : 'y', name: drag.cursor, axisKey: drag.key };
      if (x) this.draftX(drag.cursor, snap ? snap.v : this.dataAt(at));
      else this.draftY(drag.key, drag.cursor, snap ? snap.v : this.yValueAt(drag.key, at));
    };
    let unwatchShift = null;
    // Whether the click that follows a press is one: not after a drag whose
    // pointer went DRAG_START px or more from the press, wherever it was let
    // go, so a drag never selects or deselects the plot.
    let clickAfter = true;
    el.addEventListener('click', (e) => clickAfter || e.stopPropagation());
    const onShift = (held) => {
      if (!drag?.moved || (drag.mode !== 'xcursor' && drag.mode !== 'ycursor')) return;
      drag.shift = held;
      placeCursor();
    };
    // Offsets the y ranges of `keys` by dy pixels of the lane at clientY.
    const offsetY = (keys, dy, clientY) => {
      const lane = this.geo.lanes.find((l) => clientY >= l.y && clientY <= l.y + l.h) ?? this.geo.lanes[0];
      for (const k of keys) {
        const st = this.y.get(k);
        if (!st?.range) continue;
        const [a, b] = st.range, shift = (dy / lane.h) * (b - a);
        st.auto = false;
        st.range = [a + shift, b + shift];
      }
    };
    el.addEventListener('pointermove', (e) => {
      const P = this.geo?.plot;
      if (!P) return;
      this.hover = { x: e.clientX, y: e.clientY };
      this.setHover(this.dataAt(e.clientX));
      this.axisHover = onAxis(e) ? e.clientX : null;
      const yAxis = this.yAxisAt(e.clientX, e.clientY);
      // Each region names its drag: an X cursor or the time axis moves
      // sideways, a Y cursor or a y axis up and down, and the plot pans by hand.
      const mode = drag?.mode, over = drag ? null : this.pressAt(e.clientX, e.clientY);
      if (mode === 'plot' && drag.started) el.style.cursor = 'grabbing';
      else if (over?.part === 'tab' || over?.edge) el.style.cursor = 'grab';
      else if (mode === 'ycursor' || over?.kind === 'y') el.style.cursor = 'ns-resize';
      else if (mode === 'zoom' || (!drag && (e.ctrlKey || e.metaKey) && this.inPlot(e.clientX, e.clientY))) el.style.cursor = 'zoom-in';
      else el.style.cursor = mode === 'xcursor' || over?.kind === 'x' || this.axisHover !== null ? 'ew-resize' : yAxis ? 'ns-resize' : 'grab';
      if (!drag) return;
      if (Math.hypot(e.clientX - drag.x0, e.clientY - drag.y0) >= DRAG_START) clickAfter = false;
      // A pan, an offset or a pull starts once the pointer has moved off the
      // press, and its first step counts from the press, so nothing is lost.
      if ((drag.mode === 'axis' || (drag.mode === 'y' && !drag.started)) && !decide(e)) return;
      if (drag.mode === 'plot' && !drag.started) {
        if (Math.hypot(e.clientX - drag.x0, e.clientY - drag.y0) < DRAG_START) return;
        drag.started = true;
      }
      if (drag.mode === 'none') return;
      const dx = e.clientX - drag.x, dy = e.clientY - drag.y;
      // A move that goes nowhere from the press is no drag yet: a cursor's tab pressed and let go there places nothing.
      if (!drag.moved && !dx && !dy) return;
      drag.moved += Math.abs(dx) + Math.abs(dy);
      drag.x = e.clientX;
      drag.y = e.clientY;
      if (drag.mode === 'zoom') {
        // The range marked holds still under it: a live view pauses as it begins.
        if (!this.zoomBand && isTime()) for (const v of this.targets()) v.pause(clock.now());
        const P0 = this.geo.plot;
        this.zoomBand = [drag.x0, Math.min(P0.x + P0.w, Math.max(P0.x, e.clientX))];
      } else if (drag.mode === 'xcursor' || drag.mode === 'ycursor') {
        drag.shift = e.shiftKey;
        placeCursor();
      } else if (drag.mode === 'seek') {
        // The window moves with the pointer, at the scale of the whole
        // history, and stays within it.
        const [lo, hi] = this.history ?? [0, 0];
        for (const v of this.targets()) {
          v.pan((dx * Math.max(hi - lo, v.span)) / P.w / v.span, clock.now());
          v.right = Math.min(hi, Math.max(lo + v.span, v.right));
        }
      } else if (drag.mode === 'y') {
        offsetY(drag.targets, dy, drag.y0);
      } else if (!isTime()) {
        this.panFrequencies(-dx / P.w);
      } else {
        for (const v of this.targets()) v.pan(-dx / P.w, clock.now());
      }
    });
    el.addEventListener('pointerup', (e) => {
      const P = this.geo?.plot;
      // A cursor let go off its lane is removed: an X cursor beside the plot,
      // a Y cursor above or below its lane, as a tab let go where it rests.
      if (drag?.mode === 'xcursor' && drag.moved && P && (e.clientX < P.x || e.clientX > P.x + P.w)) this.draftX(drag.cursor, null);
      if (drag?.mode === 'ycursor' && drag.moved) {
        const lane = this.yAxes.find((a) => a.key === drag.key)?.lane;
        if (lane && (e.clientY < lane.y || e.clientY > lane.y + lane.h)) this.draftY(drag.key, drag.cursor, null);
      }
      // A cursor drag is one edit, written as it ends: the cursor placed, moved or removed.
      if (drag?.mode === 'xcursor' || drag?.mode === 'ycursor') {
        const saved = this.cursors.saved(this.plotId), kind = this.config().kind;
        const pair = (c) => (drag.mode === 'xcursor' ? xOf(c, kind) : yOf(c, kind, drag.key));
        const was = pair(saved)[drag.cursor], now = pair(this.placed)[drag.cursor];
        this.cursors.commit(`${was === null ? 'Place' : now === null ? 'Remove' : 'Move'} ${drag.mode === 'xcursor' ? 'X' : 'Y'}${drag.cursor === 'a' ? 1 : 2}`);
      }
      // A range marked with Ctrl, wider than a slip, zooms to it.
      if (drag?.mode === 'zoom' && this.zoomBand && Math.abs(this.zoomBand[1] - this.zoomBand[0]) >= DRAG_START) this.zoomTo(...this.zoomBand);
      this.letGo();
    });
    /**
     * Ends the drag under way, if any, its view step with it: as its release
     * does, or the pointer lost, or the plot going. A cursor it was moving
     * and has not written stays where the tab has it.
     */
    this.letGo = () => {
      this.stopTime();
      this.zoomBand = null;
      this.seeking = false;
      this.snapped = null;
      unwatchShift?.();
      unwatchShift = null;
      if (!drag) return;
      drag = null;
      this.cursors.cancel();
      this.viewHistory.end();
    };
    el.addEventListener('pointercancel', () => this.letGo());
    el.addEventListener('pointerleave', () => {
      this.setHover(null);
      this.axisHover = null;
      this.hover = null;
    });
    // A double-click on a y axis fits it, through `onFitY(key)` when the
    // page gives one, so it fits that axis on every plot the gesture acts on.
    // On the plot it has the page follow the newest samples again
    // (`onFollow`), from wherever the view was, at the window it had before
    // it paused.
    el.addEventListener('dblclick', (e) => {
      // The time axis seeks; two clicks there do nothing more.
      if (onAxis(e)) return;
      const band = this.yAxisAt(e.clientX, e.clientY);
      if (band) this.onFitY ? this.onFitY(this.axisTargets(band, e.clientY)[0]) : this.autoscale();
      else this.onFollow();
    });
  }

  /** Fits every y range to what it shows, as one view step. */
  autoscale() {
    this.viewHistory.step(() => {
      for (const [k, st] of this.y) this.refit(k, st);
    });
  }

  /**
   * The slice of trace `t` this plot shows, a signal's spectrum or what the
   * plot shows of a capture's shots (`shotsOf`): the newest while its view
   * streams, and the one it showed when the view paused until it streams again.
   */
  sliceOf(t) {
    const held = this.view.spectra;
    if (this.view.live || !held.has(t)) held.set(t, t.shots ? this.shotsOf(t) : t.spectrum);
    return held.get(t);
  }

  /**
   * What the plot shows of capture `c`'s shots, as its settings say
   * (shots.js `shown`), as a slice: {f, a, floor, under, note, number},
   * `under` each shot an overlay draws beneath it as {shot, f, a, alpha},
   * `note` what it says of how many it shows, and `number` the hub's of
   * its newest; or null with no shot. The same slice until a shot arrives
   * or a setting changes.
   */
  shotsOf(c) {
    const cfg = this.config(), mode = shotMode(cfg.shots), count = shotCount(cfg.shotCount), newest = c.shots.at(-1);
    this.shotSlices ??= new WeakMap(); // Capture -> {newest, mode, count, slice}
    const was = this.shotSlices.get(c);
    if (was && was.newest === newest && was.mode === mode && was.count === count) return was.slice;
    const s = shown(c.shots, mode, count);
    const slice = s && { f: xsOf(s.line), a: s.line.y, floor: null, under: s.under.map(({ shot, alpha }) => ({ shot, f: xsOf(shot), a: shot.y, alpha })), note: shownText(mode, s), number: newest.number };
    this.shotSlices.set(c, { newest, mode, count, slice });
    return slice;
  }

}

/**
 * A y axis's width: its widest label, `gap` px from the plot and `pad` px
 * from the edge, never narrower than `min`, the room of a Y cursor's tab
 * resting there. An axis that draws no numbers in any lane, nor has a lane
 * with room for a tab, is `bare`, as is the plot's edge on a side with no
 * axis: room for the cursors' lines to run past the plot. It widens at
 * once, and narrows only once it is `slack` px wider than it needs, so
 * labels that change width as the plot moves do not shift it.
 */
const GUTTER = { gap: 6, pad: 6, min: 24, bare: 12, slack: 10 };
/** How wide a gap in a trace is, in px, from which it is drawn as a band, and how strongly the band is filled with the gap's colour: narrower, it is one dashed line. */
const GAP_BAND = 4;
const GAP_BAND_ALPHA = 0.3;
/**
 * The width, in px, of the column at a y axis's outer edge its label runs
 * up in (`axisLabels`), outside the room its numbers take: the axes' 11 px
 * type on its side, as far as a tick label's digits reach above and below
 * their line (LABEL), and CLEAR px either side.
 */
const AXIS_LABEL = 11 + 2 * CLEAR;
/**
 * A paused plot's history scrollbar: its height, and its thumb's least
 * width, in px. The thumb is as wide as the window is a share of the history,
 * however narrow, as a seek drags anywhere along the axis, not the thumb; the
 * least width only keeps it in sight.
 */
const SEEK_BAR = 4, SEEK_MIN = 2;
/**
 * How far a cursor's line runs past the plot's edge into its axis, in px:
 * as a tick mark would, short of the tick labels, which begin GUTTER.gap px
 * from the plot beside it and further below it.
 */
const CURSOR_STUB = 4;
/** The radius, in px, of the ring on what a cursor dragged with Shift snapped to. */
const SNAP_RING = 5;
/** How near, in px, the tags of two cursors come before they part to their outer sides, and how much further apart they are before they come back. */
const TAG_NEAR = 4, TAG_HELD = 6;
/** The space between the cursors' readout and the plot body's edge, in px, as plot.css keeps it; and the least it leaves beside a cursor's part it moved off. */
const READOUT_INSET = 4, READOUT_GAP = 2;
/** Whether boxes `a` and `b`, each {x, y, w, h}, overlap. */
const overlaps = (a, b) => a.x < b.x + b.w && b.x < a.x + a.w && a.y < b.y + b.h && b.y < a.y + a.h;
/** How far to either side of a cursor's line, in px, a press still lands on it. */
const CURSOR_GRAB = 6;
/** A cursor's tab, in px: its name, on the axis it reads, or resting where it is dragged out of. */
const TAB = { w: 22, h: 14 };
/** How far a tick label's digits reach above and below the line they are written on, in px, in the axes' 11 px type. */
const LABEL = { up: 9, down: 2 };
/** The least room, in px, between two x axis labels. */
const X_LABEL_GAP = 8;
/** How far into the plot, in px, a signal dropped beside an axis still joins that side's axes. */
const AXIS_DROP = 48;

/** A y range fitted to values from `lo` to `hi`, with room above and below, or null when they are not numbers. */
function fitted(lo, hi) {
  if (!isFinite(lo) || !isFinite(hi)) return null;
  if (lo === hi) (lo -= 0.5), (hi += 0.5);
  const pad = (hi - lo) * 0.08;
  return [lo - pad, hi + pad];
}

/** Of `m`, kind -> key -> value, the values `keep` takes, as `copy` gives them, in an object by kind and key in their order. */
function byKindOf(m, keep, copy) {
  const out = {};
  for (const [kind, byKey] of [...m].sort(([a], [b]) => (a < b ? -1 : 1)))
    for (const [key, v] of [...byKey].sort(([a], [b]) => (a < b ? -1 : 1))) if (keep(v)) (out[kind] ??= {})[key] = copy(v);
  return out;
}
/** The maps, kind -> key -> value, of `o`, an object `byKindOf` gave, each value as `make` makes it. */
const mapsOf = (o, make) => new Map(Object.entries(o).map(([kind, byKey]) => [kind, new Map(Object.entries(byKey).map(([key, v]) => [key, make(v)]))]));

/**
 * Whether View states `a` and `b`, as `state` gives them, show the same.
 * Where a live view's edge was does not count, as it moves with the newest
 * samples; held spectra count by which they are.
 */
export function sameView(a, b) {
  const plain = ({ spectra, ...s }) => JSON.stringify(s.live ? { ...s, right: null, pausedAt: null, pausedHistory: null } : s);
  if (plain(a) !== plain(b)) return false;
  if (!a.spectra || !b.spectra) return !a.spectra === !b.spectra;
  return a.spectra.size === b.spectra.size && [...a.spectra].every(([t, sp]) => b.spectra.get(t) === sp);
}

/** What a view's span and edge are set to, as a string: two are equal while nothing has moved the view. */
const viewMark = (v) => (v.live ? `live ${v.span}` : `${v.span} ${v.right}`);

/**
 * What one plot shows: a time plot's x range, live, trailing the newest
 * samples by `delay`, or paused at `right`; the plot's y ranges; and the
 * spectra a paused spectrum holds. The plot's cursors are not here: they
 * are edits, kept in its tab (cursors.js). It reads the time from
 * the hub, so it outlives any one hub's session, and it outlives the plot's
 * component, so a remount keeps all of them.
 *
 * A view that follows live has no time X cursors on its plot: they mark
 * moments, which scroll away (viewhistory.svelte.js holds the rule). Going
 * live, it keeps the paused view it leaves as `left`, which an undo that
 * brings those cursors back shows again.
 */
export class View {
  live = true;
  /** The seconds a live view shows, which the window setting sets. A paused view keeps it to go back to. */
  window = 10;
  /** The seconds asked of the view. Paused, it shows them. Live, they are its window, and it shows as much of them as its limits allow (`shown`). */
  span = 10;
  delay = 0.15;
  right = 0; // device time of the right edge when paused
  pausedAt = null; // device time it paused at, the end of what it captured
  /** The hub's History setting when the view paused, in seconds: null while live, or when the hub had said none. `oldestShown` reads it. */
  pausedHistory = null;
  /** What the hub holds of the plot's signals, as Session `held` gives it. Views sets it for the plot the view is of. */
  held = () => ({ oldest: null, dropped: false, history: null });
  yByKind = new Map(); // plot kind, a spectrum's by y scale -> range key -> {range, auto}: PlotView's `y`
  spectra = new Map(); // Trace or Capture -> the slice a spectrum or a capture plot shows of it, held while paused: PlotView's `sliceOf`
  /** The paused view, as `state` gives it, that the view left when it last went live, or null while it never has. */
  left = null;
  listeners = new Set();

  constructor(hub) {
    this.hub = hub;
  }

  // What a view shows lies within its limits, [the oldest time it can show,
  // its time], however it is set: `fit` is the one clamp. A paused view's
  // limits stand still, so `keepWithin` holds its span and its right edge
  // to them as each is set. A live view's limits move with the clock and
  // widen as a hub fills, so it keeps the span asked of it and shows as
  // much of that as its limits allow whenever it is read (`shown`,
  // `range`): on a hub that holds less than its window it shows what is
  // held, and its window again once the hub holds that much.

  /** `span`, or the width of `limits` when that is less, never under the narrowest span. */
  static fit(span, limits) {
    return limits ? Math.min(span, Math.max(WINDOW_LIMITS[0], limits[1] - limits[0])) : span;
  }

  /** [oldest, newest] of what a view whose time is device time `time` can show, or null when nothing before it can be. */
  limitsAt(time) {
    const { oldest, history } = this.held();
    const from = oldestShown(oldest, time, history, this.pausedHistory);
    return from !== null && from < time ? [from, time] : null;
  }

  /** The times the view shows at device time `now`. */
  range(now) {
    const right = this.live ? now - this.delay : this.right;
    return [right - (this.live ? View.fit(this.span, this.limitsAt(right)) : this.span), right];
  }

  /** The seconds the view shows at page time `perf`: its span, within its limits. */
  shown(perf) {
    return this.live ? View.fit(this.span, this.limits(perf)) : this.span;
  }

  /**
   * The seconds of its window the view can show at page time `perf`: what
   * the window fields read. The window asked for is kept, and
   * reads again once the limits hold it.
   */
  windowShown(perf) {
    return View.fit(this.window, this.limits(perf));
  }

  /**
   * The view's time at page time `perf`, where it ends zoomed all the way
   * out: the newest samples less the delay while live, and the moment it
   * paused while paused, whatever `perf`.
   */
  time(perf) {
    return this.live || this.pausedAt === null ? this.hub.now(perf) - this.delay : this.pausedAt;
  }

  /** The oldest time the view can show at page time `perf`, as `oldestShown` gives it, or null when nothing says. */
  oldest(perf) {
    const { oldest, history } = this.held();
    return oldestShown(oldest, this.time(perf), history, this.pausedHistory);
  }

  /**
   * [oldest, newest], what the view can show at page time `perf`, live or
   * paused: from the oldest time it can show to its time. A paused view
   * stays within it. Null when no time before the view's can be shown.
   */
  limits(perf) {
    return this.limitsAt(this.time(perf));
  }

  /**
   * Zooms by `factor` about the point at fraction `anchor` across the plot,
   * which stays under the pointer. Zooming in anywhere but the newest edge
   * holds a moment the live view would scroll away, so it pauses the view.
   * A paused view stays paused however it zooms, so a slip of the wheel
   * never sends a held history off streaming; going live is the history
   * bar's end, or Follow live. A zoom that leaves the view live sets its
   * window; one that pauses it, or of a paused view, leaves the window as it
   * was, for going live again. It zooms what the view shows, as wide as
   * what the view can show, and live no wider than the decimation keeps. A
   * zoom out of a live view that shows all it can already changes nothing:
   * its window stays as asked.
   */
  zoom(factor, anchor, perf, edge = LIVE_EDGE) {
    this.zoomTo(this.shown(perf) * factor, anchor, perf, edge);
  }

  /**
   * [narrowest, widest], the spans a zoom takes the view to at page time
   * `perf`: as wide as what the view can show, and live no wider than the
   * decimation keeps.
   */
  spans(perf) {
    const limits = this.limits(perf), held = limits ? limits[1] - limits[0] : WINDOW_LIMITS[1];
    return [WINDOW_LIMITS[0], this.live ? Math.min(this.liveSeconds(), held) : held];
  }

  /** Zooms to show `asked` seconds, within `spans`, about the point at fraction `anchor` across the plot, as `zoom`. */
  zoomTo(asked, anchor, perf, edge = LIVE_EDGE) {
    const [narrowest, widest] = this.spans(perf), shown = this.shown(perf);
    const span = Math.min(widest, Math.max(narrowest, asked));
    if (this.live && asked < shown && anchor < edge) this.pause(perf);
    if (!this.live) {
      // It stays within what has arrived: zooming out past the newest
      // sample widens it into the past instead.
      const at = this.right - (1 - anchor) * this.span;
      this.right = Math.min(this.hub.now(perf) - this.delay, at + (1 - anchor) * span);
      this.span = span;
    } else if (span !== shown) this.window = this.span = span;
    this.keepWithin();
    this.notify();
  }

  /** The widest the view goes while live, in seconds. */
  liveSeconds() {
    return this.hub.liveSeconds?.() ?? WINDOW_LIMITS[1];
  }

  /**
   * Keeps a paused view within its limits, the history its plot captured,
   * and, while nothing says what that is, ending no later than the moment
   * it paused, where its limits end: every way of setting a paused view's
   * span or its right edge ends here. A live view is kept within its own
   * as it is read.
   */
  keepWithin() {
    if (this.live) return;
    const limits = this.limits();
    if (limits) [this.span, this.right] = this.#within(limits);
    else if (this.pausedAt !== null) this.right = Math.min(this.right, this.pausedAt);
  }

  /** [span, right], the paused view's span and right edge kept within `limits`. */
  #within(limits) {
    const [lo, hi] = limits, span = View.fit(this.span, limits);
    return [span, Math.min(hi, Math.max(lo + span, this.right))];
  }

  /**
   * Whether the view is paused within what it can show at page time
   * `perf`, as `keepWithin` leaves it, and ends no later than the newest
   * samples: whether the hub still holds all of what it shows.
   */
  holds(perf) {
    const limits = this.live ? null : this.limits(perf);
    if (!limits || this.pausedAt > this.hub.now(perf) - this.delay) return false;
    const [span, right] = this.#within(limits);
    return span === this.span && right === this.right;
  }

  /**
   * Sets the window to `span` seconds, and shows that much, ending where it
   * ends now, within what a paused view captured.
   */
  setSpan(span) {
    this.window = Math.min(this.liveSeconds(), Math.max(WINDOW_LIMITS[0], span));
    this.span = this.live ? this.window : Math.max(WINDOW_LIMITS[0], span);
    this.keepWithin();
    this.notify();
  }

  /** Holds the view where it is, at the span it shows; samples keep arriving behind it. */
  pause(perf) {
    if (!this.live) return;
    this.span = this.shown(perf);
    this.right = this.hub.now(perf) - this.delay;
    this.pausedAt = this.right;
    this.pausedHistory = this.held().history;
    this.live = false;
    this.notify();
  }

  /**
   * Shows times [t0, t1], paused, within what it captured and the narrowest
   * span. The window stays as it was, for going live again.
   */
  show(t0, t1, perf) {
    this.pause(perf);
    this.span = Math.max(WINDOW_LIMITS[0], t1 - t0);
    this.right = t0 + this.span;
    this.keepWithin();
    this.notify();
  }

  /**
   * Moves the view by `fraction` of its span, later for a positive one, and
   * pauses it, within what it captured and never past the newest samples. A
   * live view is at the newest samples already, so a pan later leaves it live.
   */
  pan(fraction, perf) {
    if (this.live && fraction >= 0) return;
    this.pause(perf);
    this.right = Math.min(this.hub.now(perf) - this.delay, this.right + fraction * this.span);
    this.keepWithin();
    this.notify();
  }

  /**
   * Follows the newest samples again. As unpausing, it goes on at the span
   * it shows, and keeps its window. With `restore`, as a double-click, it
   * goes back to its window.
   */
  follow(restore = false) {
    if (!this.live) this.left = this.state();
    this.live = true;
    this.span = restore ? this.window : Math.min(this.liveSeconds(), this.span);
    this.pausedAt = this.pausedHistory = null;
    this.notify();
  }

  /**
   * What the view shows, as a value a view history step keeps: live or
   * paused and where, the History setting of its pause, its span and window, the y
   * ranges it holds (one that fits itself to the data is left out), and,
   * paused, the spectra it holds. `sameView` compares two.
   */
  state() {
    const { live, window, span, right, pausedAt, pausedHistory } = this;
    return {
      live, window, span, right, pausedAt, pausedHistory,
      yRanges: byKindOf(this.yByKind, (st) => !st.auto && !!st.range, (st) => [...st.range]),
      spectra: live ? null : new Map(this.spectra),
    };
  }

  /**
   * Shows `s`, a value `state` gave, again at page time `perf`: a live
   * one follows the newest samples, and a paused one ends no later than
   * they do, once any have arrived, with the History setting of the pause
   * `s` was taken in, within its limits. One whose start the hub has
   * dropped is left for `startHeld` to move, which says that it did. A y
   * range it does not hold fits itself to the data again.
   */
  restore(s, perf) {
    const newest = this.hub.now(perf) - this.delay, end = (t) => (newest > -Infinity ? Math.min(t, newest) : t);
    if (s.live && !this.live) this.left = this.state();
    Object.assign(this, { live: s.live, window: s.window, span: s.span });
    if (s.live) this.pausedAt = this.pausedHistory = null;
    else [this.right, this.pausedAt, this.pausedHistory] = [end(s.right), end(s.pausedAt ?? s.right), s.pausedHistory ?? null];
    if (!this.held().dropped) this.keepWithin();
    this.yByKind = mapsOf(s.yRanges, (range) => ({ range: [...range], auto: false }));
    this.spectra = new Map(s.spectra ?? []);
    this.notify();
  }

  /**
   * Moves a paused view that starts before the oldest time its plot can
   * show, on a hub that has dropped samples of its signals, later to start
   * there, ending no later than the newest samples at page time `perf` and
   * within its limits, and says whether it did. A view as `keepWithin`
   * leaves it starts no earlier than that time, so it stays.
   */
  startHeld(perf) {
    const oldest = this.oldest();
    if (this.live || oldest === null || !this.held().dropped) return false;
    const newest = this.pausedAt ?? this.right, now = this.hub.now(perf) - this.delay;
    if (this.span <= newest - oldest && this.right >= Math.min(newest, oldest + this.span)) return false;
    const span = View.fit(this.span, [oldest, now]), right = Math.min(now, Math.max(this.right, oldest + span));
    if (span === this.span && right === this.right) return false;
    Object.assign(this, { span, right, pausedAt: Math.max(this.pausedAt ?? right, right) });
    this.notify();
    return true;
  }

  onChange(fn) {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }

  notify() {
    for (const fn of this.listeners) fn();
  }
}
