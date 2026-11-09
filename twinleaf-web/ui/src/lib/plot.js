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
// selected plot moves for every selected plot; or spectrum (log-log).

import { scaleLinear, scaleLog, scaleTime } from 'd3-scale';
import { DRAG_START } from './constants.js';
import { colors } from './colors.svelte.js';
import { prefs } from './prefs.svelte.js';
import { spanText } from './units.js';
import { WINDOW_LIMITS, windowText } from './window.js';

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
 * and `readout(t)` a time to the millisecond.
 */
export function timeAxis(epoch) {
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
    label: (t) => `${+t.toFixed(4)} s`,
    readout: (t) => `${t.toFixed(3)} s`,
  };
}

/**
 * How a menu names the X cursors of a plot, by the kind of its x axis:
 * 'unix', any other epoch, or 'hz' for a spectrum's frequencies.
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
  if (kind === 'hz') return { at: (f) => si(f, 'Hz'), span: (s) => si(s, 'Hz') };
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
        const clock = `${two(d.getHours())}:${two(d.getMinutes())}:${two(d.getSeconds())}${frac ? `.${frac}` : ''}`;
        const dated = day(t) !== day(Date.now() / 1000) || day(t) !== day(t + between);
        return dated ? `${d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short' })} ${clock}` : clock;
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
// hold a moment. A plot with a header reaches further, to below its + button.
const LIVE_EDGE = 0.85;
/** How far left of the header's + button, in px, that stretch begins. */
const LIVE_EDGE_BUFFER = 32;
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

/** How much of [x0, x1] the points of `src`, a trace or a window, span. */
function spanWithin(src, x0, x1) {
  if (src.length === 0) return 0;
  return Math.max(0, Math.min(src.xAt(src.absEnd - 1), x1) - Math.max(src.xAt(src.absStart), x0));
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

const PREFIXES = { '-15': 'f', '-12': 'p', '-9': 'n', '-6': 'µ', '-3': 'm', 0: '', 3: 'k', 6: 'M', 9: 'G' };

/**
 * `v` with four significant digits and an SI prefix on `unit`, as 197.8 mV.
 * A value without a physical unit, or in arbitrary units, has no prefix.
 */
/** A tick on a linear frequency axis, as few digits as it needs: 500 Hz, 1.5 kHz. */
const hzLabel = (f) => (Math.abs(f) >= 1000 ? `${+(f / 1000).toPrecision(4)} kHz` : `${+f.toPrecision(4)} Hz`);

export function si(v, unit) {
  if (!isFinite(v)) return '—';
  if (!unit || unit === 'arb') return v.toPrecision(4);
  if (v === 0) return `0 ${unit}`;
  const e = Math.max(-15, Math.min(9, Math.floor(Math.log10(Math.abs(v)) / 3) * 3));
  return `${(v / 10 ** e).toPrecision(4)} ${PREFIXES[e]}${unit}`;
}

/** The range lo … hi in `unit`, both ends with the prefix of the larger. */
export function siRange(lo, hi, unit) {
  if (!isFinite(lo) || !isFinite(hi)) return '—';
  const big = Math.max(Math.abs(lo), Math.abs(hi));
  if (!unit || unit === 'arb' || big === 0) return `${lo.toPrecision(4)} … ${hi.toPrecision(4)}${unit && unit !== 'arb' ? ` ${unit}` : ''}`;
  const e = Math.max(-15, Math.min(9, Math.floor(Math.log10(big) / 3) * 3));
  return `${(lo / 10 ** e).toPrecision(4)} … ${(hi / 10 ** e).toPrecision(4)} ${PREFIXES[e]}${unit}`;
}

/**
 * The plot's colours, read from the theme's tokens in app.css. They are the
 * background, also as GL's rgba, the axis text, grid, frame, gap marks,
 * crosshair, the span between cursors, the text on tags, the cursors, the
 * seek strip and the traces, each trace also as GL's rgba. They are kept
 * once every token has a value, until the page turns light or dark
 * (prefs.svelte.js), which a reader in an effect or a template follows. A
 * canvas ignores an empty colour and keeps the last one drawn with, so a
 * token the page has not loaded yet is read again on the next frame.
 */
let themed = null;
export function theme() {
  const dark = prefs.dark;
  if (themed?.dark !== dark) {
    const style = getComputedStyle(document.documentElement);
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
    if (missing) return read;
    themed = read;
  }
  return themed;
}
/** Each picked colour other than the theme's, as GL's rgba. */
const customRgba = new Map();
/** The trace's colour, as colors.svelte.js gives it or it was picked. */
export const colorOf = (trace) => {
  const c = colors.of(trace.key);
  return typeof c === 'number' ? theme().traces[c] : c;
};
/** The trace's colour as GL's rgba. */
const rgbaOf = (trace) => {
  const c = colors.of(trace.key);
  if (typeof c === 'number') return theme().tracesRgba[c];
  if (!customRgba.has(c)) customRgba.set(c, cssRgba(c));
  return customRgba.get(c);
};

export class PlotView {
  /**
   * `hub` holds the session whose traces it draws. `view` is this plot's own
   * time view, which it draws. `targets` returns
   * the views a gesture on this plot acts on: every selected plot's, when
   * this one is selected, or else its own. `hoverTime` holds the time under the
   * pointer, which every time plot draws a line at.
   */
  constructor({ body, overlay, config, hub, view, targets, hoverTime, plotId, renderer, onFocus, onFrequencies, onFitY }) {
    Object.assign(this, { body, overlay, config, hub, view, targets, hoverTime, plotId, renderer, onFocus, onFrequencies, onFitY });
    this.zoomBand = null; // [x0, x1] in client px while a Ctrl+drag marks a range to zoom to
    this.ctx = overlay.getContext('2d');
    this.specHover = null;
    this.yAxes = []; // the y axes, as last drawn: {key, lane, range, side, slot, x0, x1, color, plans}
    this.tabs = []; // the tabs cursors not yet placed rest as, as last drawn
    this.tagsPlaced = [];
    this.axisHover = null; // clientX over the x axis, where a drag seeks
    this.phosphor = new Map(); // signal key -> the shares its axis column shows, eased frame to frame
    this.drawnLevel = new Map(); // trace -> the level of it the plot drew last, see `pickLevel`
    this.gutter = { left: [], right: [] }; // each y axis's width, in px, from the plot outward, fitted to its labels
    this.hover = null; // the pointer over the plot, {x, y} in client px, for the crosshair
    this.hidden = false;
    this.bindInput();
  }

  /** The plot's GPU slot for trace `t`, which holds its window, or, by `variant`, its spectrum drawn in a pair of scales. */
  slotOf(t, variant = '') {
    this.slots ??= new WeakMap(); // Trace -> {variant -> slot}
    let slots = this.slots.get(t);
    if (!slots) this.slots.set(t, (slots = {}));
    return (slots[variant] ??= {});
  }

  /** The traces of the plot's signals the hub has. */
  traces() {
    const out = [];
    const session = this.hub.session;
    for (const key of this.config().keys) {
      const t = session.byKey.get(key);
      if (t) out.push(t);
    }
    return out;
  }

  /** The y axes' places: how many on each side, as {left, right}. */
  axisCounts(cfg) {
    if (cfg.mode !== 'independent') return { left: 1, right: 0 };
    const slots = axisSlots(cfg.keys, cfg.sides);
    return { left: slots.left.length, right: slots.right.length };
  }

  /** The width of the `slot`th y axis out on `side`, as its labels last needed. */
  axisWidth(side, slot) {
    return this.gutter[side][slot] ?? GUTTER.min;
  }

  /** Plot area and lanes, in CSS px relative to the viewport. Each y axis is as wide as its labels need. */
  geometry(body, cfg, n) {
    const counts = this.axisCounts(cfg);
    const width = (side) => Array.from({ length: counts[side] }, (_, i) => this.axisWidth(side, i)).reduce((a, b) => a + b, 0);
    const left = Math.max(width('left'), 12), right = Math.max(width('right'), 12);
    const plot = { x: body.x + left, y: body.y + 6, w: body.w - left - right, h: body.h - 6 - 22 };
    const count = lanesOf(cfg.mode, n);
    const h = (plot.h - LANE_GAP * (count - 1)) / count;
    const lanes = count === 1 ? [plot] : Array.from({ length: count }, (_, i) => ({ x: plot.x, y: plot.y + i * (h + LANE_GAP), w: plot.w, h }));
    return { plot, lanes };
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

  /**
   * The Y cursors of the plot's kind, which its view holds, range key ->
   * {a, b}: Y1 and Y2 of the y axis with that key, each a value in the
   * axis's units or null. Each kind keeps its own, as its axes differ.
   */
  get yCursors() {
    const kind = this.config().kind;
    let cursors = this.view.yCursorsByKind.get(kind);
    if (!cursors) this.view.yCursorsByKind.set(kind, (cursors = new Map()));
    return cursors;
  }

  rangeKey(cfg, trace) {
    return cfg.mode === 'overlay' ? 'shared' : trace.key;
  }

  /**
   * The y range of `key`, given the extent [lo, hi] of what it shows now.
   *
   * With `hold`, as a time plot asks, the range fits that extent once, when
   * it first has data and again each time `autoscale` asks. Between fits it
   * holds, as a scope's vertical scale does, so zooming, panning and a
   * passing burst never rescale it. Without, as a spectrum asks, whose first
   * estimates are rough, it follows the extent, growing at once and
   * shrinking only when far too big. A range set by hand holds either way.
   */
  yRange(key, lo, hi, hold) {
    let st = this.y.get(key);
    if (!st) this.y.set(key, (st = { range: null, auto: true }));
    if (!st.auto && st.range) return st.range;
    if (!isFinite(lo) || !isFinite(hi)) return st.range ?? [-1, 1];
    if (lo === hi) {
      lo -= 0.5;
      hi += 0.5;
    }
    const pad = (hi - lo) * 0.08, want = [lo - pad, hi + pad], cur = st.range;
    if (hold) {
      st.range = want;
      st.auto = false;
    } else if (!cur || want[0] < cur[0] || want[1] > cur[1] || want[1] - want[0] < 0.5 * (cur[1] - cur[0])) {
      const extra = (want[1] - want[0]) * 0.1;
      st.range = [want[0] - extra, want[1] + extra];
    }
    return st.range;
  }

  draw(perf) {
    const r = this.body.getBoundingClientRect();
    // Off screen, collapsed, or hidden behind a maximized plot: draw nothing.
    const shown = this.body.checkVisibility ? this.body.checkVisibility({ visibilityProperty: true }) : true;
    const visible = shown && r.width > 0 && r.height > 0 && r.bottom > 0 && r.top < window.innerHeight;
    this.sizeOverlay(r);
    if (!visible) return;
    const body = { x: r.left, y: r.top, w: r.width, h: r.height };
    this.renderer.fill(body, theme().bgRgba);
    const cfg = this.config();
    const traces = this.traces();
    const geo = this.geometry(body, cfg, traces.length);
    this.geo = geo;
    this.publishFrame(body, geo.plot);
    if (cfg.kind === 'spectrum') this.drawSpectrum(cfg, traces, geo, body);
    else this.drawTime(cfg, traces, geo, body, perf);
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
    // so a live plot scrolls by whole pixels and does not flicker.
    const dpr = this.renderer.dpr ?? (typeof window === 'undefined' ? 1 : window.devicePixelRatio || 1);
    const [x0, x1] = onPixelGrid(...this.view.range(this.hub.now(perf)), Math.max(1, Math.round(geo.plot.w * dpr)));
    const session = this.hub.session;
    const plans = traces.map((t, index) => {
      // Live, it draws the level of the live points that suits the window.
      // Paused, it draws from the hub's history: the detail window when it
      // covers the view, else whichever of it, the overview of the whole
      // capture and that level covers most, the sharper on a tie.
      const level = pickLevel(t.levels, x0, x1, geo.plot.w, this.drawnLevel.get(t));
      this.drawnLevel.set(t, level);
      const live = t.levels[level];
      const d = this.view.live ? null : session.windowOf(this.plotId, 'detail', t);
      const o = this.view.live ? null : session.windowOf(this.plotId, 'overview', t);
      const src = d?.covers(x0, x1) ? d : [d, o, live].filter(Boolean).reduce((best, s) => (spanWithin(s, x0, x1) > spanWithin(best, x0, x1) ? s : best));
      const a0 = Math.max(src.absStart, src.lowerBound(x0) - 1);
      const a1 = Math.min(src.absEnd, src.lowerBound(x1) + 1);
      const [lo, hi] = a1 > a0 ? src.extent(a0, a1) : [Infinity, -Infinity];
      return { t, src, fromWindow: src !== live, level, a0, a1, lo, hi, index };
    });
    // The history the plot moves through, oldest to newest: from the oldest
    // time the hub holds of its signals to the newest point, or, paused, to
    // the moment it paused, the end of what it captured. A paused view stays
    // within it.
    let lo = Infinity, hi = -Infinity;
    const oldest = this.hub.session.memory?.oldest ?? {};
    for (const t of traces) {
      for (const l of t.levels) if (l.length) [lo, hi] = [Math.min(lo, l.xAt(l.absStart)), Math.max(hi, l.xAt(l.absEnd - 1))];
      if (oldest[t.meta.id] !== undefined) lo = Math.min(lo, oldest[t.meta.id]);
    }
    if (!this.view.live && this.view.pausedAt !== null) hi = this.view.pausedAt;
    this.history = lo < hi ? [lo, hi] : null;
    this.view.limits = this.view.live ? null : this.history;
    // Shared ranges take the union of their traces.
    const ext = new Map();
    for (const p of plans) {
      const k = this.rangeKey(cfg, p.t), e = ext.get(k) ?? [Infinity, -Infinity];
      ext.set(k, [Math.min(e[0], p.lo), Math.max(e[1], p.hi)]);
    }
    const ranges = new Map([...ext].map(([k, [lo, hi]]) => [k, this.yRange(k, lo, hi, true)]));
    plans.forEach((p) => {
      const lane = geo.lanes[this.laneOf(cfg, p.index)];
      const [y0, y1] = ranges.get(this.rangeKey(cfg, p.t));
      if (p.fromWindow) {
        const st = this.renderer.detailBuffer(this.slotOf(p.t), p.src);
        if (this.renderer.lane(lane, st.base, x0, x1, y0, y1, prefs.traceWidth, rgbaOf(p.t)))
          this.renderer.drawDetail(st, p.src, p.a0, p.a1);
        return;
      }
      const ring = this.renderer.ring(p.src);
      if (this.renderer.lane(lane, ring.base, x0, x1, y0, y1, prefs.traceWidth, rgbaOf(p.t))) this.renderer.drawTrace(p.src, ring, p.a0, p.a1);
    });
    this.drawOverlay(cfg, geo, body, plans, ranges, [x0, x1], false);
  }

  drawSpectrum(cfg, traces, geo, body) {
    // Each axis is log or linear: a log axis's GL coordinates are log10.
    const lx = cfg.xLog ? Math.log10 : (v) => v, ly = cfg.yLog ? Math.log10 : (v) => v;
    const plans = [];
    let fmin = Infinity, fmax = -Infinity;
    for (const t of traces) {
      const s = this.spectrumOf(t);
      if (!s || s.f.length < 2) continue;
      let lo = Infinity, hi = -Infinity;
      for (const a of s.a) {
        if (a < lo) lo = a;
        if (a > hi) hi = a;
      }
      fmin = Math.min(fmin, s.f[0]);
      fmax = Math.max(fmax, s.f.at(-1));
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
    const variant = `spectrum:${cfg.xLog}:${cfg.yLog}`;
    for (const p of plans) {
      const lane = geo.lanes[this.laneOf(cfg, p.index)];
      const [y0, y1] = ranges.get(this.rangeKey(cfg, p.t));
      if (!this.renderer.lane(lane, 0, lx0, lx1, y0, y1, prefs.traceWidth, rgbaOf(p.t))) continue;
      this.renderer.drawStatic(this.slotOf(p.t, variant), p.s, () => {
        const pts = new Float32Array(p.s.f.length * 2);
        for (let i = 0; i < p.s.f.length; i++) {
          pts[i * 2] = lx(p.s.f[i]);
          pts[i * 2 + 1] = ly(p.s.a[i]);
        }
        return pts;
      });
    }
    const xd = plans.length ? [f0, f1] : null;
    this.fExtent = plans.length ? [fmin, fmax] : null;
    this.drawOverlay(cfg, geo, body, plans, ranges, xd, true, traces);
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

  sizeOverlay(r) {
    const dpr = window.devicePixelRatio || 1;
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
   * lane, and stays there while the signal moves under it.
   */
  drawTip(ctx, T, lane, near, value, px, py) {
    const name = near.t.key.length > 18 ? `${near.t.key.slice(0, 17)}…` : near.t.key;
    const SWATCH = 8, PAD = 5, H = 16, GAP = 10;
    const nameW = ctx.measureText(name).width, valueW = ctx.measureText(value).width;
    const w = PAD + SWATCH + PAD + nameW + PAD + valueW + PAD;
    // Above and right of the pointer, flipped where that leaves the lane.
    let x = px + GAP, y = py - GAP - H;
    if (x + w > lane.x + lane.w) x = px - GAP - w;
    if (y < lane.y) y = py + GAP;
    x = Math.max(lane.x, x);
    y = Math.max(lane.y, Math.min(lane.y + lane.h - H, y));
    ctx.fillStyle = T.tipBg;
    ctx.fillRect(x, y, w, H);
    ctx.strokeStyle = T.tipBorder;
    ctx.strokeRect(x + 0.5, y + 0.5, w - 1, H - 1);
    const color = colorOf(near.t);
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
   * readouts. `spectrum` says the plot shows spectra, whose axes are each
   * log or linear as the plot sets them; a time plot's are linear.
   */
  drawOverlay(cfg, geo, body, plans, ranges, xd, spectrum, allTraces) {
    const logX = spectrum && cfg.xLog, logY = spectrum && cfg.yLog;
    const ctx = this.ctx;
    const T = theme();
    ctx.setTransform(this.dpr, 0, 0, this.dpr, -body.x * this.dpr, -body.y * this.dpr);
    ctx.clearRect(body.x, body.y, body.w, body.h);
    ctx.font = `11px ${T.dataFont}`;
    ctx.lineWidth = 1;
    const traces = allTraces ?? plans.map((p) => p.t);
    if (!xd || plans.length === 0) {
      this.legend = { rows: {}, cursors: null };
      ctx.fillStyle = T.axisText;
      const msg = traces.length ? 'waiting for data…' : 'drop signals here';
      ctx.textAlign = 'center';
      ctx.fillText(msg, body.x + body.w / 2, body.y + body.h / 2);
      return;
    }
    const P = geo.plot;
    const sx = (logX ? scaleLog() : scaleLinear()).domain(xd).range([P.x, P.x + P.w]);
    const time = timeAxis(plans[0]?.t.meta.epoch);

    // The live edge, while the pointer is over it on a live time plot, where
    // a zoom keeps the plot live: a wash rising toward the newest edge, under
    // the grid, and a LIVE line on that edge, drawn with the hover, where the
    // zoom anchors. Both fade in and out.
    const frameAt = performance.now();
    const dt = Math.min(100, frameAt - (this.overlayAt ?? frameAt));
    this.overlayAt = frameAt;
    const pointer = this.hover;
    const edgeX = pointer && !spectrum && this.view.live ? P.x + this.liveEdge() * P.w : Infinity;
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
    const xticks = logX ? sx.ticks(6) : spectrum ? sx.ticks(Math.max(2, P.w / 90)) : time.ticks(xd[0], xd[1], Math.max(2, P.w / 120));
    const fewDecades = logX && Math.log10(xd[1] / xd[0]) < 2;
    const labelled = (f) => decade(f) || (fewDecades && [2, 5].includes(Math.round(f / 10 ** Math.floor(Math.log10(f) + 1e-9))));
    ctx.fillStyle = T.axisText;
    ctx.textAlign = 'center';
    // With no room beside the plot, the X cursors' tabs rest on the axis's left end.
    const xTabsBeside = P.x - body.x >= 2 * TAB.w + 6;
    let labelEnd = xTabsBeside ? -Infinity : P.x + 2 * TAB.w + 4;
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
      const label = logX ? (labelled(tv) ? hzLabel(tv) : '') : spectrum ? hzLabel(tv) : time.label(tv);
      if (!label) continue;
      const half = ctx.measureText(label).width / 2;
      if (px - half < labelEnd + X_LABEL_GAP || px + half > body.x + body.w) continue;
      ctx.fillText(label, px, P.y + P.h + 15);
      labelEnd = px + half;
    }

    // Y axes: which range each lane's grid and labels follow, and the plans
    // that range scales.
    const axes = []; // {key, lane, range, side, color, plans}: key the range key
    if (cfg.mode === 'stacked') {
      plans.forEach((p) => {
        const lane = geo.lanes[this.laneOf(cfg, p.index)];
        axes.push({ key: p.t.key, lane, range: ranges.get(p.t.key), side: 'left', color: colorOf(p.t), plans: [p] });
      });
    } else if (cfg.mode === 'independent') {
      const slots = axisSlots(plans.map((p) => p.t.key), cfg.sides);
      for (const side of ['left', 'right'])
        slots[side].forEach((key, slot) => {
          const p = plans.find((p) => p.t.key === key);
          axes.push({ key, lane: geo.lanes[0], range: ranges.get(key), side, slot, color: colorOf(p.t), plans: [p], keys: [key] });
        });
    } else {
      axes.push({ key: 'shared', lane: geo.lanes[0], range: ranges.get('shared'), side: 'left', color: T.axisText, plans });
    }
    // Each axis's band of the gutter, from the plot outward; the outermost
    // reaches the plot's edge.
    for (const a of axes) {
      a.slot ??= 0;
      let inner = 0;
      for (let i = 0; i < a.slot; i++) inner += this.axisWidth(a.side, i);
      const w = this.axisWidth(a.side, a.slot);
      const outermost = !axes.some((b) => b.side === a.side && b.slot > a.slot);
      if (a.side === 'left') [a.x0, a.x1] = [outermost ? body.x : P.x - inner - w, P.x - inner];
      else [a.x0, a.x1] = [P.x + P.w + inner, outermost ? body.x + body.w : P.x + P.w + inner + w];
    }
    this.axisBands = axes.map(({ side, slot, x0, x1, keys }) => ({ side, slot, x0, x1, keys }));
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
    if (!hoverLane || !overAxis || spectrum) this.phosphor.clear();
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
        drawStrip(ctx, shown, colorOf(p.t), x, AXIS_STRIP.w, hoverLane.y, AXIS_STRIP.alpha);
      });
    }
    const widest = { left: [], right: [] };
    axes.forEach((ax, i) => {
      const sy = yScale(ax.range, ax.lane);
      // Labels take the precision of the ticks they label, so no two read the same.
      const count = Math.max(3, ax.lane.h / 40);
      const ticks = sy.ticks(count);
      const fmt = logY ? (v) => (decade(v) ? v.toExponential(0) : '') : sy.tickFormat(count);
      ctx.textAlign = ax.side === 'left' ? 'right' : 'left';
      for (const tv of ticks) {
        const py = Math.round(sy(tv)) + 0.5;
        if (i === 0 || cfg.mode === 'stacked') {
          ctx.strokeStyle = T.grid;
          ctx.beginPath();
          ctx.moveTo(ax.lane.x, py);
          ctx.lineTo(ax.lane.x + ax.lane.w, py);
          ctx.stroke();
        }
        const label = fmt(tv);
        if (!label) continue;
        widest[ax.side][ax.slot] = Math.max(widest[ax.side][ax.slot] ?? 0, ctx.measureText(label).width);
        ctx.fillStyle = ax.color;
        ctx.fillText(label, ax.side === 'left' ? ax.x1 - GUTTER.gap : ax.x0 + GUTTER.gap, py + 4);
      }
    });
    for (const side of ['left', 'right'])
      this.gutter[side] = widest[side].map((w, slot) => {
        const need = Math.max(GUTTER.min, Math.ceil((w ?? 0) + GUTTER.gap + GUTTER.pad)), had = this.gutter[side][slot];
        return had !== undefined && need <= had && need >= had - GUTTER.slack ? had : need;
      });
    ctx.strokeStyle = T.frame;
    for (const lane of geo.lanes) ctx.strokeRect(lane.x + 0.5, lane.y + 0.5, lane.w - 1, lane.h - 1);

    // Gaps, in the lane of their trace.
    if (!spectrum) {
      ctx.setLineDash([3, 3]);
      ctx.strokeStyle = T.gap;
      plans.forEach((p) => {
        const lane = geo.lanes[this.laneOf(cfg, p.index)];
        const breaks = p.src.breaks;
        for (const b of breaks) {
          if (b <= p.a0 || b >= p.a1) continue;
          const px = Math.round(sx(p.src.xAt(b))) + 0.5;
          ctx.beginPath();
          ctx.moveTo(px, lane.y);
          ctx.lineTo(px, lane.y + lane.h);
          ctx.stroke();
        }
      });
      ctx.setLineDash([]);
    }

    // Cursors: the hover crosshair, and the placed A and B.
    this.xd = xd;
    this.spectrum = spectrum;
    this.logX = logX;
    this.logY = logY;
    // The hover is where the pointer is now: a live plot scrolls under a still
    // pointer. Beside the plot, over a y axis, it reads no time.
    const hx = this.hover?.x ?? null;
    const overTime = hx !== null && hx >= P.x && hx <= P.x + P.w;
    this.setMarker('hover', overTime ? this.dataAt(hx) : null);
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
    if ((this.seeking || !this.view.live) && !spectrum && this.history) {
      const [h0, h1] = this.history;
      const [top, height] = open ? [P.y + P.h + 2, 18] : [body.y + body.h - SEEK_BAR, SEEK_BAR];
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
      for (const a of axes) {
        if (a.lane !== hoverLane) continue;
        // On a Y cursor of this axis, the cursor's own tag reads the height.
        const yc = this.yCursors.get(a.key);
        const sy = yScale(a.range, a.lane);
        if (yc && ['a', 'b'].some((n) => yc[n] !== null && Math.abs(sy(yc[n]) - hy) < TAB.h)) continue;
        const v = sy.invert(hy);
        const text = spectrum ? v.toExponential(2) : v.toPrecision(4);
        const w = ctx.measureText(text).width + 8;
        const x = this.tagSlot(a.lane, a.side, py, w, 16);
        ctx.fillStyle = a.color;
        ctx.fillRect(x, py - 8, w, 16);
        ctx.fillStyle = T.tagText;
        ctx.textAlign = 'center';
        ctx.fillText(text, x + w / 2, py + 4);
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
        ctx.strokeStyle = colorOf(p.t);
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
    // B with B - A.
    const fx = spectrum ? (f) => `${f.toPrecision(4)} Hz` : time.readout;
    const fy = spectrum ? (v) => v.toExponential(3) : (v) => v.toPrecision(5);
    const rows = {};
    // Where a pixel's column holds several points, the hover reads all of
    // them, lowest to highest, as one point there would be any of them.
    const halfPx = spectrum || marks.hover === null ? 0 : (xd[1] - xd[0]) / P.w / 2;
    const now = performance.now(), held = this.hoverRead;
    const hold = held && this.hover && held.x === this.hover.x && held.y === this.hover.y && now - held.at < READ_MS;
    // What the numbers say, held between reads; the marker reads anew.
    const reads = hold ? held.reads : new Map();
    let near = null; // the signal whose point at the hover is nearest the pointer, in the pointer's lane
    plans.forEach((p) => {
      const lane = this.laneOf(cfg, p.index);
      const sy = yScale(ranges.get(this.rangeKey(cfg, p.t)), geo.lanes[lane]);
      const at = (v) => (v === null ? null : spectrum ? spectrumAt(p.s, v) : traceAt(p.src, v));
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
        ctx.fillStyle = colorOf(p.t);
        ctx.fillRect(sx(marks.hover) - 1.5, sy(span.hi) - 1.5, 3, sy(span.lo) - sy(span.hi) + 3);
      }
      for (const [name, color] of [['hover', colorOf(p.t)], ['a', T.cursorA], ['b', T.cursorB]]) {
        const pt = pts[name];
        if (!pt || (name === 'hover' && span)) continue;
        ctx.fillStyle = color;
        ctx.beginPath();
        ctx.arc(sx(pt.x), sy(pt.y), 3, 0, Math.PI * 2);
        ctx.fill();
      }
      let value = '';
      if (pts.hover && read?.v !== undefined) value = fy(read.v);
      else if (!spectrum && this.view.live && p.t.length) value = fy(p.t.yAt(p.t.absEnd - 1));
      // Where the points come from: raw samples, or how many raw rows per point.
      let info = `${p.t.meta.rate} Hz`;
      if (!spectrum) info += p.fromWindow ? (p.src.ratio === 1 ? ' · raw' : ` · 1:${p.src.ratio}`) : p.level ? ` · live, level ${p.level}` : ' · live';
      // A spectrum's values keep their exponent, its units being per √Hz.
      const fv = spectrum ? fy : (v) => si(v, p.t.meta.units);
      const cell = (pt) => (pt ? fv(pt.y) : '—');
      // A spectrum's floor, which its chip shows in place of a value, or a dash while there is none.
      const floor = spectrum ? (p.s.floor ? p.s.floor.toExponential(2) : '—') : null;
      rows[p.t.key] = { value, floor, info, a: cell(pts.a), b: cell(pts.b), d: pts.a && pts.b ? fv(pts.b.y - pts.a.y) : '—' };
    });
    // The cursors' readout: where X1 and X2 are, with each signal's value
    // there, and each y axis's Y1 and Y2, a spectrum's also as a ratio in dB.
    const delta = marks.a !== null && marks.b !== null;
    const xUnit = spectrum ? 'Hz' : 's';
    const xs =
      marks.a === null && marks.b === null
        ? null
        : {
            v: spectrum ? 'f' : 't',
            a: marks.a === null ? '—' : fx(marks.a),
            b: marks.b === null ? '—' : fx(marks.b),
            d: delta ? si(marks.b - marks.a, xUnit) : '—',
            rate: delta && !spectrum && marks.b !== marks.a ? si(1 / Math.abs(marks.b - marks.a), 'Hz') : '',
          };
    const ys = [];
    for (const a of axes) {
      const yc = this.yCursors.get(a.key);
      if (!yc || (yc.a === null && yc.b === null)) continue;
      const units = a.plans[0]?.t.meta.units ?? '';
      const fv = spectrum ? fy : (v) => si(v, units);
      const both = yc.a !== null && yc.b !== null;
      ys.push({
        key: a.key,
        name: a.plans.map((p) => p.t.key).join(', '),
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
      const u = near.t.meta.units;
      const { v, span } = near.read ?? {};
      const value = v === undefined ? '' : spectrum ? fy(v) : span ? siRange(span.lo, span.hi, u) : si(v, u);
      this.drawTip(ctx, T, hoverLane, near, value, sx(marks.hover), hy);
    }
    if (!spectrum) this.drawWindow(ctx, T, P, open, frameAt);
  }

  /**
   * A time plot's window, on the time axis's right end: a dot, the primary
   * colour while live and a grey square while paused, and the seconds shown.
   * It shows while the pointer is on the time axis, unless the history bar
   * has the axis, and for WINDOW_SHOWN_MS after the window changes.
   */
  drawWindow(ctx, T, P, historyOpen, frameAt) {
    if (this.shownSpan !== this.view.span) {
      if (this.shownSpan !== undefined) this.windowUntil = frameAt + WINDOW_SHOWN_MS;
      this.shownSpan = this.view.span;
    }
    if (historyOpen || (this.axisHover === null && !(frameAt < this.windowUntil))) return;
    const text = `${windowText(this.view.span)} s`, H = 16;
    const w = ctx.measureText(text).width + 22, x = P.x + P.w - w, y = P.y + P.h + 3;
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
   * Draws the placed cursors, and the tabs of those not yet placed. X1 and
   * X2 are lines through every lane, named on the x axis, their place at the
   * top of the plot; Y1 and Y2 lines across their axis's lane, named in its
   * gutter, their level just inside it; the span between a pair shaded. One
   * out of view is named at the edge it lies beyond, with an arrow. A cursor
   * not yet placed rests as a tab to drag out, X1 and X2 at the x axis's
   * left end and Y1 and Y2 at the top of each y axis, shown while the
   * pointer is on that axis; a pull across an axis draws one out too.
   */
  drawCursors(ctx, T, P, body, marks, axes, xd, sx, time, xTabsBeside, hoverLane) {
    const color = { a: T.cursorA, b: T.cursorB };
    const xName = { a: 'X1', b: 'X2' }, yName = { a: 'Y1', b: 'Y2' };
    const inView = (v) => v !== null && v >= xd[0] && v <= xd[1];
    const fx = this.spectrum ? (f) => `${f.toPrecision(4)} Hz` : time.readout;
    // A tag centred on (cx, cy), or, with `align`, beside cx on that side.
    const tag = (cx, cy, text, fill, align = 'center') => {
      const w = Math.max(TAB.w, ctx.measureText(text).width + 6);
      const x = align === 'left' ? cx : align === 'right' ? cx - w : cx - w / 2;
      ctx.fillStyle = fill;
      ctx.fillRect(Math.round(x), Math.round(cy - TAB.h / 2), w, TAB.h);
      ctx.fillStyle = T.tagText;
      ctx.textAlign = 'center';
      ctx.fillText(text, x + w / 2, cy + 4);
    };
    if (marks.a !== null && marks.b !== null) {
      const l = Math.max(P.x, sx(Math.min(marks.a, marks.b))), r = Math.min(P.x + P.w, sx(Math.max(marks.a, marks.b)));
      if (r > l) {
        ctx.fillStyle = T.span;
        for (const lane of this.geo.lanes) ctx.fillRect(l, lane.y, r - l, lane.h);
      }
    }
    for (const a of axes) {
      const yc = this.yCursors.get(a.key);
      if (!yc || yc.a === null || yc.b === null) continue;
      const sy = this.axisScale(a), y0 = sy(yc.a), y1 = sy(yc.b);
      const t = Math.max(a.lane.y, Math.min(y0, y1)), b = Math.min(a.lane.y + a.lane.h, Math.max(y0, y1));
      if (b > t) {
        ctx.fillStyle = T.span;
        ctx.fillRect(a.lane.x, t, a.lane.w, b - t);
      }
    }
    const xTagY = P.y + P.h + 3 + TAB.h / 2;
    for (const name of ['a', 'b']) {
      const v = marks[name];
      if (v === null) continue;
      if (inView(v)) {
        const px = Math.round(sx(v)) + 0.5;
        ctx.strokeStyle = color[name];
        for (const lane of this.geo.lanes) {
          ctx.beginPath();
          ctx.moveTo(px, lane.y);
          ctx.lineTo(px, lane.y + lane.h);
          ctx.stroke();
        }
        tag(px, xTagY, xName[name], color[name]);
        const text = fx(v), right = px + ctx.measureText(text).width + 10 > P.x + P.w;
        tag(right ? px - 2 : px + 2, P.y + TAB.h / 2 + 2, text, color[name], right ? 'right' : 'left');
      } else {
        const step = name === 'b' ? TAB.w + 10 : 0;
        if (v < xd[0]) tag(P.x + TAB.w / 2 + 4 + step, xTagY, `‹${xName[name]}`, color[name]);
        else tag(P.x + P.w - TAB.w / 2 - 4 - step, xTagY, `${xName[name]}›`, color[name]);
      }
    }
    for (const a of axes) {
      const yc = this.yCursors.get(a.key);
      if (!yc) continue;
      const sy = this.axisScale(a);
      const tx = a.side === 'left' ? a.x1 - TAB.w / 2 - 1 : a.x0 + TAB.w / 2 + 1;
      const units = a.plans[0]?.t.meta.units ?? '';
      const fv = this.spectrum ? (v) => v.toExponential(3) : (v) => si(v, units);
      for (const name of ['a', 'b']) {
        const v = yc[name];
        const py = v === null ? NaN : sy(v);
        if (!isFinite(py)) continue;
        if (py >= a.lane.y && py <= a.lane.y + a.lane.h) {
          const y = Math.round(py) + 0.5;
          ctx.strokeStyle = color[name];
          ctx.beginPath();
          ctx.moveTo(a.lane.x, y);
          ctx.lineTo(a.lane.x + a.lane.w, y);
          ctx.stroke();
          tag(tx, py, yName[name], color[name]);
          const text = fv(v), w = Math.max(TAB.w, ctx.measureText(text).width + 6);
          tag(this.tagSlot(a.lane, a.side, py, w, TAB.h), py, text, color[name], 'left');
        } else {
          const step = name === 'b' ? TAB.h + 2 : 0;
          if (py < a.lane.y) tag(tx, a.lane.y + TAB.h / 2 + 1 + step, `${yName[name]}˄`, color[name]);
          else tag(tx, a.lane.y + a.lane.h - TAB.h / 2 - 1 - step, `${yName[name]}˅`, color[name]);
        }
      }
    }
    // The tabs, beside the plot under the y axis when there is room.
    const tabs = [];
    ['a', 'b'].forEach((name, i) => {
      if (marks[name] !== null) return;
      const x = (xTabsBeside ? P.x - 2 * TAB.w - 4 : P.x + 2) + i * (TAB.w + 2);
      tabs.push({ kind: 'x', name, x, y: P.y + P.h + 3, w: TAB.w, h: TAB.h });
    });
    for (const a of axes) {
      const yc = this.yCursors.get(a.key);
      ['a', 'b'].forEach((name, i) => {
        if (yc && yc[name] !== null) return;
        const x = a.side === 'left' ? a.x1 - TAB.w - 1 : a.x0 + 1;
        tabs.push({ kind: 'y', key: a.key, axis: a, name, x, y: a.lane.y + 2 + i * (TAB.h + 2), w: TAB.w, h: TAB.h });
      });
    }
    this.tabs = tabs;
    const hov = this.hover;
    if (!hov) return;
    const on = this.tabAt(hov.x, hov.y);
    const overXAxis = hov.y > P.y + P.h && hov.x >= body.x && hov.x <= body.x + body.w;
    const overAxis = this.yAxisAt(hov.x, hov.y);
    for (const t of tabs) {
      const shown =
        t === on ||
        (t.kind === 'x'
          ? overXAxis
          : (overAxis && overAxis.side === t.axis.side && overAxis.slot === (t.axis.slot ?? 0) && t.axis.lane === hoverLane) || on?.axis === t.axis);
      if (!shown) continue;
      ctx.globalAlpha = t === on ? 1 : 0.6;
      tag(t.x + t.w / 2, t.y + t.h / 2, t.kind === 'x' ? xName[t.name] : yName[t.name], color[t.name]);
      ctx.globalAlpha = 1;
    }
  }

  /**
   * The fraction across the plot past which zooming in leaves a live view
   * live: from a little left of the header's + button, so the header's
   * buttons mark the newest stretch with room to spare, though never more
   * than its newer half; or LIVE_EDGE
   * where the plot shows no header.
   */
  liveEdge() {
    const P = this.geo?.plot;
    const add = this.body.closest('.plot-card')?.querySelector('.add-signal')?.getBoundingClientRect();
    if (!P || !add?.width) return LIVE_EDGE;
    return Math.min(LIVE_EDGE, Math.max(0.5, (add.left - LIVE_EDGE_BUFFER - P.x) / P.w));
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

  /** Shows x from `a` to `b`: a time plot's views that time, paused; a spectrum those frequencies. */
  showRange(a, b) {
    if (a === null || b === null || !(b > a)) return;
    if (this.config().kind === 'spectrum') this.onFrequencies?.(a, b);
    else for (const v of this.targets()) v.show(a, b, performance.now());
  }

  /** Zooms to the time or frequencies between X1 and X2, when both are placed. */
  zoomToCursors() {
    const { a, b } = this.markers();
    if (a !== null && b !== null) this.showRange(Math.min(a, b), Math.max(a, b));
  }

  /** The tab at (x, y) a cursor not yet placed rests as, if any. */
  tabAt(x, y) {
    return this.tabs.find((t) => x >= t.x && x <= t.x + t.w && y >= t.y && y <= t.y + t.h) ?? null;
  }

  /**
   * The left edge of a tag `w` wide and `h` high centred at height `cy`,
   * laid along `side` of `lane`: against the side, or further in past the
   * tags already laid there that it would overlap, so tags at one height
   * stand side by side.
   */
  tagSlot(lane, side, cy, w, h) {
    let x = side === 'left' ? lane.x + 1 : lane.x + lane.w - 1 - w;
    const near = this.tagsPlaced.filter((t) => t.lane === lane && t.side === side && Math.abs(t.cy - cy) < (t.h + h) / 2);
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

  /** The placed Y cursor within a few pixels of (x, y), across its lane or on its tag in the gutter, as {key, name}, or null. */
  yCursorNear(x, y) {
    for (const a of this.yAxes) {
      const yc = this.yCursors.get(a.key);
      if (!yc || x < Math.min(a.x0, a.lane.x) || x > Math.max(a.x1, a.lane.x + a.lane.w)) continue;
      if (y < a.lane.y - 4 || y > a.lane.y + a.lane.h + 4) continue;
      const sy = this.axisScale(a);
      for (const name of ['b', 'a']) if (yc[name] !== null && Math.abs(sy(yc[name]) - y) < 6) return { key: a.key, name };
    }
    return null;
  }

  /** Sets Y cursor `name` of the y axis `key` to `v`, or removes it with null. */
  setYCursor(key, name, v) {
    let yc = this.yCursors.get(key);
    if (!yc) this.yCursors.set(key, (yc = { a: null, b: null }));
    yc[name] = v;
  }

  /** The value of y axis `key` at client y, or null without that axis. */
  yValueAt(key, clientY) {
    const a = this.yAxes.find((a) => a.key === key);
    return a ? this.axisScale(a).invert(clientY) : null;
  }

  /** Places X1 and X2 a third of the way in from each side of the view, pausing a time plot, as dragging them out does. */
  addXCursors() {
    const P = this.geo?.plot;
    if (!P || !this.xd) return;
    if (this.config().kind === 'time') for (const v of this.targets()) v.pause(performance.now());
    this.setMarker('a', this.dataAt(P.x + P.w / 3));
    this.setMarker('b', this.dataAt(P.x + (2 * P.w) / 3));
  }

  /** Places Y1 and Y2 a third of the way in from the bottom and top of y axis `key`, or of the first axis. */
  addYCursors(key) {
    const a = this.yAxes.find((a) => a.key === key) ?? this.yAxes[0];
    if (!a) return;
    const sy = this.axisScale(a);
    this.setYCursor(a.key, 'a', sy.invert(a.lane.y + (2 * a.lane.h) / 3));
    this.setYCursor(a.key, 'b', sy.invert(a.lane.y + a.lane.h / 3));
  }

  /** Removes X1 and X2. */
  clearXCursors() {
    this.setMarker('a', null);
    this.setMarker('b', null);
  }

  /** Whether y axis `key` has a Y cursor placed. */
  hasYCursors(key) {
    const yc = this.yCursors.get(key);
    return !!yc && (yc.a !== null || yc.b !== null);
  }

  /** Removes the Y cursors of y axis `key`, or with none, of every y axis. */
  clearYCursors(key) {
    if (key === undefined) this.yCursors.clear();
    else this.yCursors.delete(key);
  }

  /** How many cursors are placed: X1 and X2, and each Y1 and Y2 of every kind. */
  cursorsPlaced() {
    const { a, b } = this.markers();
    let n = (a !== null) + (b !== null);
    for (const cursors of this.view.yCursorsByKind.values()) for (const c of cursors.values()) n += (c.a !== null) + (c.b !== null);
    return n;
  }

  /** How many of the plot's y axes have a Y cursor placed. */
  yCursorAxes() {
    return this.yAxes.filter((a) => this.hasYCursors(a.key)).length;
  }

  /** The signals of y axis `key`, as the readout names them, or null while the plot draws no such axis. */
  axisName(key) {
    const a = this.yAxes.find((a) => a.key === key);
    return a ? a.plans.map((p) => p.t.key).join(', ') : null;
  }

  /** What the plot's x axis counts, for `cursorText`: 'hz' on a spectrum, else its first trace's epoch. */
  xKind() {
    return this.config().kind === 'spectrum' ? 'hz' : (this.traces()[0]?.meta.epoch ?? 'zero');
  }

  /** The hover position and the X1 and X2 cursors, as a and b, in this plot's x units. */
  markers() {
    if (this.config().kind === 'spectrum') return { hover: this.specHover, ...this.view.specCursors };
    return { hover: this.hoverTime.t, ...this.view.cursors };
  }

  setMarker(name, value) {
    if (this.config().kind === 'spectrum') {
      if (name === 'hover') this.specHover = value;
      else this.view.specCursors[name] = value;
    } else if (name === 'hover') this.hoverTime.t = value;
    else for (const v of this.targets()) v.cursors[name] = value;
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
  /** The placed X cursor within a few pixels of clientX, if any. */
  cursorNear(clientX) {
    const m = this.markers();
    for (const name of ['b', 'a']) {
      const px = this.pxOf(m[name]);
      if (px !== null && Math.abs(px - clientX) < 6) return name;
    }
    return null;
  }

  /**
   * What is under (x, y), for its context menu: a y axis and the range keys
   * it shows, the x axis, or the plot area and the range keys there.
   */
  regionAt(x, y) {
    const band = this.yAxisAt(x, y);
    if (band) return { kind: 'y', keys: this.axisTargets(band, y), side: band.side };
    const P = this.geo?.plot;
    if (P && y > P.y + P.h && x >= P.x && x <= P.x + P.w) return { kind: 'x' };
    return { kind: 'plot', keys: this.yTargets(y) };
  }

  /**
   * The units y axis `key` shows, as its signals' units, each once and in
   * order, a spectrum's per √Hz; or null while the plot draws no such axis.
   */
  axisUnits(key) {
    const a = this.yAxes.find((a) => a.key === key);
    if (!a) return null;
    const spectrum = this.config().kind === 'spectrum';
    return [...new Set(a.plans.map((p) => (spectrum ? `${p.t.meta.units}/√Hz` : p.t.meta.units)))].sort().join(' ');
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

  /** Sets the y range of `keys` to [lo, hi], in the units the axis shows, to hold until a fit. */
  setYBounds(keys, lo, hi) {
    for (const k of keys) {
      let st = this.y.get(k);
      if (!st) this.y.set(k, (st = { range: null, auto: false }));
      st.auto = false;
      st.range = this.logY ? [Math.log10(lo), Math.log10(hi)] : [lo, hi];
    }
  }

  /** Fits the y range of `keys` to what they show, on the next frame. */
  fit(keys) {
    for (const k of keys) {
      const st = this.y.get(k);
      if (st) st.auto = true;
    }
  }

  /** The y axis band (x, y) is over, {side, slot, x0, x1, keys}, or null. */
  yAxisAt(x, y) {
    const P = this.geo?.plot;
    if (!P || y < P.y || y > P.y + P.h) return null;
    return this.axisBands?.find((b) => x >= b.x0 && x < b.x1) ?? null;
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
    // value under the pointer, and zooms time elsewhere: over the time axis
    // about the live edge while live, over the plot about the pointer.
    el.addEventListener('wheel', (e) => {
      e.preventDefault();
      const factor = Math.exp(e.deltaY * 0.0015);
      const band = this.yAxisAt(e.clientX, e.clientY);
      if (band || e.shiftKey) {
        const keys = band ? this.axisTargets(band, e.clientY) : this.yTargets(e.clientY);
        const lane = this.geo?.lanes.find((l) => e.clientY >= l.y && e.clientY <= l.y + l.h) ?? this.geo?.lanes[0];
        if (!lane) return;
        const at = (lane.y + lane.h - e.clientY) / lane.h;
        for (const k of keys) {
          const st = this.y.get(k);
          if (!st?.range) continue;
          const [a, b] = st.range, v = a + at * (b - a);
          st.auto = false;
          st.range = [v - (v - a) * factor, v + (b - v) * factor];
        }
      } else if (isTime()) {
        // Over the time axis a live view zooms about its newest edge and goes
        // on streaming; over the plot it zooms about the pointer, which holds
        // that moment, so it pauses.
        const axis = onAxis(e);
        for (const v of this.targets()) v.zoom(factor, axis && v.live ? 1 : fracX(e.clientX), performance.now(), this.liveEdge());
      }
    }, { passive: false });
    // The band under the plot holds the x axis.
    const onAxis = (e) => {
      const P = this.geo?.plot;
      return !!P && this.xd && e.clientY > P.y + P.h && e.clientX >= P.x && e.clientX <= P.x + P.w;
    };
    // Of a pair, the cursor a pull places: the first not placed, else the
    // nearer to the pointer by `dist`.
    const pick = (pair, dist) => (pair.a === null ? 'a' : pair.b === null ? 'b' : dist(pair.a) <= dist(pair.b) ? 'a' : 'b');
    // A drag's mode: 'xcursor' and 'ycursor' move a cursor, or drag one out
    // of its tab; 'axis' on the x
    // axis and 'y' on a y axis wait to learn their direction, a pull across
    // the axis into the plot drawing a cursor out of it, as a guide comes off
    // a ruler, and a drag along it seeking ('seek', the time axis only) or
    // offsetting ('y'); 'plot' pans; 'none' does nothing more.
    el.addEventListener('pointerdown', (e) => {
      this.onFocus?.();
      if (e.button !== 0) return;
      el.setPointerCapture(e.pointerId);
      const tab = this.tabAt(e.clientX, e.clientY);
      const ycursor = tab ? null : this.yCursorNear(e.clientX, e.clientY);
      const cursor = tab || ycursor ? null : this.cursorNear(e.clientX);
      const band = this.yAxisAt(e.clientX, e.clientY);
      // With Shift a click selects the plot, so a press does nothing here: no
      // pan, offset or cursor moves the plot by accident.
      const mode = e.shiftKey ? 'none' : tab ? `${tab.kind}cursor` : ycursor ? 'ycursor' : cursor ? 'xcursor' : onAxis(e) ? 'axis' : band ? 'y' : (e.ctrlKey || e.metaKey) && this.inPlot(e.clientX, e.clientY) ? 'zoom' : 'plot';
      const targets = band ? this.axisTargets(band, e.clientY) : this.yTargets(e.clientY);
      drag = { x: e.clientX, y: e.clientY, x0: e.clientX, y0: e.clientY, moved: 0, mode, targets, band, cursor: tab?.name ?? ycursor?.name ?? cursor, key: tab?.key ?? ycursor?.key };
      // A time cursor holds a moment the live view would scroll away, so
      // dragging one out pauses the view.
      if (mode === 'xcursor' && tab && isTime()) for (const v of this.targets()) v.pause(performance.now());
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
        drag.cursor = pick(this.yCursors.get(key) ?? { a: null, b: null }, (v) => Math.abs(this.axisScale(a)(v) - e.clientY));
      } else if (across) {
        // In from a y axis: an X cursor. A time cursor holds a moment the
        // live view would scroll away, so it pauses the view.
        drag.mode = 'xcursor';
        drag.cursor = pick(this.markers(), (v) => Math.abs(this.pxOf(v) - e.clientX));
        if (isTime()) for (const v of this.targets()) v.pause(performance.now());
      } else if (drag.mode === 'axis') {
        if (isTime()) (drag.mode = 'seek'), (this.seeking = true);
        else drag.mode = 'none';
      } else drag.started = true;
      return true;
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
      this.setMarker('hover', this.dataAt(e.clientX));
      this.axisHover = onAxis(e) ? e.clientX : null;
      const yAxis = this.yAxisAt(e.clientX, e.clientY);
      // Each region names its drag: an X cursor or the time axis moves
      // sideways, a Y cursor or a y axis up and down, and the plot pans by hand.
      const mode = drag?.mode;
      if (mode === 'plot' && drag.started) el.style.cursor = 'grabbing';
      else if (!drag && this.tabAt(e.clientX, e.clientY)) el.style.cursor = 'grab';
      else if (mode === 'ycursor' || (!drag && this.yCursorNear(e.clientX, e.clientY))) el.style.cursor = 'ns-resize';
      else if (mode === 'zoom' || (!drag && (e.ctrlKey || e.metaKey) && this.inPlot(e.clientX, e.clientY))) el.style.cursor = 'zoom-in';
      else el.style.cursor = mode === 'xcursor' || this.cursorNear(e.clientX) || this.axisHover !== null ? 'ew-resize' : yAxis ? 'ns-resize' : 'grab';
      if (!drag) return;
      // A pan, an offset or a pull starts once the pointer has moved off the
      // press, and its first step counts from the press, so nothing is lost.
      if ((drag.mode === 'axis' || (drag.mode === 'y' && !drag.started)) && !decide(e)) return;
      if (drag.mode === 'plot' && !drag.started) {
        if (Math.hypot(e.clientX - drag.x0, e.clientY - drag.y0) < DRAG_START) return;
        drag.started = true;
      }
      if (drag.mode === 'none') return;
      const dx = e.clientX - drag.x, dy = e.clientY - drag.y;
      drag.moved += Math.abs(dx) + Math.abs(dy);
      drag.x = e.clientX;
      drag.y = e.clientY;
      if (drag.mode === 'zoom') {
        // The range marked holds still under it: a live view pauses as it begins.
        if (!this.zoomBand && isTime()) for (const v of this.targets()) v.pause(performance.now());
        const P0 = this.geo.plot;
        this.zoomBand = [drag.x0, Math.min(P0.x + P0.w, Math.max(P0.x, e.clientX))];
      } else if (drag.mode === 'xcursor') {
        this.setMarker(drag.cursor, this.dataAt(e.clientX));
      } else if (drag.mode === 'ycursor') {
        this.setYCursor(drag.key, drag.cursor, this.yValueAt(drag.key, e.clientY));
      } else if (drag.mode === 'seek') {
        // The window moves with the pointer, at the scale of the whole
        // history, and stays within it.
        const [lo, hi] = this.history ?? [0, 0];
        for (const v of this.targets()) {
          v.pan((dx * Math.max(hi - lo, v.span)) / P.w / v.span, performance.now());
          v.right = Math.min(hi, Math.max(lo + v.span, v.right));
        }
      } else if (drag.mode === 'y') {
        offsetY(drag.targets, dy, drag.y0);
      } else if (!isTime()) {
        this.panFrequencies(-dx / P.w);
      } else {
        for (const v of this.targets()) v.pan(-dx / P.w, performance.now());
      }
    });
    el.addEventListener('pointerup', (e) => {
      const P = this.geo?.plot;
      // A cursor let go off its lane is removed: an X cursor beside the plot,
      // a Y cursor above or below its lane, as a tab let go where it rests.
      if (drag?.mode === 'xcursor' && drag.moved && P && (e.clientX < P.x || e.clientX > P.x + P.w)) this.setMarker(drag.cursor, null);
      if (drag?.mode === 'ycursor' && drag.moved) {
        const lane = this.yAxes.find((a) => a.key === drag.key)?.lane;
        if (lane && (e.clientY < lane.y || e.clientY > lane.y + lane.h)) this.setYCursor(drag.key, drag.cursor, null);
      }
      // A range marked with Ctrl, wider than a slip, zooms to it.
      if (drag?.mode === 'zoom' && this.zoomBand && Math.abs(this.zoomBand[1] - this.zoomBand[0]) >= DRAG_START) this.zoomTo(...this.zoomBand);
      this.zoomBand = null;
      drag = null;
      this.seeking = false;
    });
    el.addEventListener('pointercancel', () => {
      this.zoomBand = null;
      drag = null;
      this.seeking = false;
    });
    el.addEventListener('pointerleave', () => {
      this.setMarker('hover', null);
      this.axisHover = null;
      this.hover = null;
    });
    // A double-click on a y axis fits it, through `onFitY(key)` when the
    // page gives one, so it fits that axis on every plot the gesture acts on.
    // On the plot it
    // follows the newest samples again, from wherever the view was, at the
    // window it had before it paused.
    el.addEventListener('dblclick', (e) => {
      // The time axis seeks; two clicks there do nothing more.
      if (onAxis(e)) return;
      const band = this.yAxisAt(e.clientX, e.clientY);
      if (band) this.onFitY ? this.onFitY(this.axisTargets(band, e.clientY)[0]) : this.autoscale();
      else for (const v of this.targets()) v.follow(true);
    });
  }

  /** Fits every y range to what it shows, on the next frame. */
  autoscale() {
    for (const st of this.y.values()) st.auto = true;
  }

  /**
   * The spectrum of trace `t` this plot shows: the newest while its view
   * streams, and the one it showed when the view paused until it streams again.
   */
  spectrumOf(t) {
    const held = this.view.spectra;
    if (this.view.live || !held.has(t)) held.set(t, t.spectrum);
    return held.get(t);
  }

}

/**
 * A y axis's width: its widest label, `gap` px from the plot and `pad` px
 * from the edge, never narrower than `min`. It widens at once for a wider
 * label, and narrows only once it is `slack` px wider than it needs, so
 * labels that change width as the plot moves do not shift it.
 */
const GUTTER = { gap: 6, pad: 6, min: 24, slack: 10 };
/**
 * A paused plot's history scrollbar: its height, and its thumb's least
 * width, in px. The thumb is as wide as the window is a share of the history,
 * however narrow, as a seek drags anywhere along the axis, not the thumb; the
 * least width only keeps it in sight.
 */
const SEEK_BAR = 4, SEEK_MIN = 2;
/** A cursor's tab, in px: its name, on the axis it reads, or resting where it is dragged out of. */
const TAB = { w: 22, h: 14 };
/** The least room, in px, between two x axis labels. */
const X_LABEL_GAP = 8;
/** How far into the plot, in px, a signal dropped beside an axis still joins that side's axes. */
const AXIS_DROP = 48;

/**
 * What one plot shows: a time plot's x range, live, trailing the newest
 * samples by `delay`, or paused at `right`; the plot's cursors and y
 * ranges; and the spectra a paused spectrum holds. It reads the time from
 * the hub, so it outlives any one hub's session, and it outlives the plot's
 * component, so a remount keeps all of them.
 */
export class View {
  live = true;
  /** The seconds a live view shows, which the window setting sets. A paused view keeps it to go back to. */
  window = 10;
  /** The seconds the view shows now. While live it is the window. */
  span = 10;
  delay = 0.15;
  right = 0; // device time of the right edge when paused
  pausedAt = null; // device time it paused at, the end of what it captured
  limits = null; // [oldest, newest] a paused view stays within, as its plot last drew it
  cursors = { a: null, b: null }; // a time plot's X1 and X2, placed, device time
  specCursors = { a: null, b: null }; // a spectrum's X1 and X2, in Hz
  yCursorsByKind = new Map(); // plot kind -> range key -> Y1 and Y2, {a, b}: PlotView's `yCursors`
  yByKind = new Map(); // plot kind, a spectrum's by y scale -> range key -> {range, auto}: PlotView's `y`
  spectra = new Map(); // Trace -> the spectrum a spectrum plot shows of it, held while paused: PlotView's `spectrumOf`
  listeners = new Set();

  constructor(hub) {
    this.hub = hub;
  }

  range(now) {
    const right = this.live ? now - this.delay : this.right;
    return [right - this.span, right];
  }

  /**
   * Zooms by `factor` about the point at fraction `anchor` across the plot,
   * which stays under the pointer. Zooming in anywhere but the newest edge
   * holds a moment the live view would scroll away, so it pauses the view.
   * A paused view stays paused however it zooms, so a slip of the wheel
   * never sends a held history off streaming; going live is the history
   * bar's end, or Follow live. A zoom that leaves the view live sets its
   * window; one that pauses it, or of a paused view, leaves the window as it
   * was, for going live again.
   */
  zoom(factor, anchor, perf, edge = LIVE_EDGE) {
    const widest = this.live ? this.liveSeconds() : this.limits ? this.limits[1] - this.limits[0] : Math.max(WINDOW_LIMITS[1], this.hub.historySeconds?.(perf) ?? 0);
    const span = Math.min(widest, Math.max(WINDOW_LIMITS[0], this.span * factor));
    if (this.live && factor < 1 && anchor < edge) this.pause(perf);
    if (!this.live) {
      // It stays within what has arrived: zooming out past the newest
      // sample widens it into the past instead.
      const at = this.right - (1 - anchor) * this.span;
      this.right = Math.min(this.hub.now(perf) - this.delay, at + (1 - anchor) * span);
    }
    this.span = span;
    if (this.live) this.window = span;
    this.keepWithin();
    this.notify();
  }

  /** The widest the view goes while live, in seconds. */
  liveSeconds() {
    return this.hub.liveSeconds?.() ?? WINDOW_LIMITS[1];
  }

  /** Keeps a paused view within its limits, the history its plot captured. */
  keepWithin() {
    if (this.live || !this.limits) return;
    const [lo, hi] = this.limits;
    this.span = Math.min(this.span, Math.max(WINDOW_LIMITS[0], hi - lo));
    this.right = Math.min(hi, Math.max(lo + this.span, this.right));
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

  /** Whether time cursors hold the view where it is. */
  held() {
    return this.cursors.a !== null || this.cursors.b !== null;
  }

  /** Holds the view where it is; samples keep arriving behind it. */
  pause(perf) {
    if (!this.live) return;
    this.right = this.hub.now(perf) - this.delay;
    this.pausedAt = this.right;
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
   * pauses it, within what it captured. A live view is at the newest
   * samples already, so a pan later leaves it live.
   */
  pan(fraction, perf) {
    if (this.live && fraction >= 0) return;
    this.pause(perf);
    this.right += fraction * this.span;
    this.keepWithin();
    this.notify();
  }

  /**
   * Follows the newest samples again. As unpausing, it goes on at the span
   * it shows, and keeps its window. With `restore`, as a double-click, it
   * goes back to its window. The time cursors go: live, they would scroll
   * away.
   */
  follow(restore = false) {
    this.live = true;
    this.span = restore ? this.window : Math.min(this.liveSeconds(), this.span);
    this.pausedAt = null;
    this.cursors = { a: null, b: null };
    this.notify();
  }

  /** Removes every cursor: X1 and X2 of either kind and every Y cursor. Says whether any were placed. */
  clearCursors() {
    const placed = (c) => c.a !== null || c.b !== null;
    let had = placed(this.cursors) || placed(this.specCursors);
    for (const byKey of this.yCursorsByKind.values()) for (const c of byKey.values()) had ||= placed(c);
    this.cursors = { a: null, b: null };
    this.specCursors = { a: null, b: null };
    this.yCursorsByKind.clear();
    return had;
  }

  onChange(fn) {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }

  notify() {
    for (const fn of this.listeners) fn();
  }
}
