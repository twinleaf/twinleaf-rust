// One plot: turns its settings into lanes, y ranges and draw calls on the
// shared renderer, and draws its own axes, gaps, cursor and legend on a 2D
// canvas above the WebGL one.
//
// Modes, and their lanes, as `lanesOf` counts them:
//   overlay      every trace on one lane and one y axis
//   independent  one lane, each trace with its own y range (scope style);
//                the first two get axes on the left and right
//   stacked      one lane per trace, sharing x
// Kinds: time, each plot with a View of its own, which a gesture on a
// selected plot moves for every selected plot; or spectrum (log-log).

import { scaleLinear, scaleLog } from 'd3-scale';
import { DRAG_START } from './constants.js';

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
let panelRgba = null;
/** The plot background, the theme's --plot-bg colour. */
function panelColor() {
  if (!panelRgba) panelRgba = cssRgba(getComputedStyle(document.documentElement).getPropertyValue('--plot-bg').trim());
  return panelRgba;
}
const LINE_WIDTH = 1.3;
const LANE_GAP = 6;

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
// The fraction across a live plot past which zooming in stays live.
const LIVE_EDGE = 0.95;
/**
 * How often the hover's numbers read again while the pointer holds still,
 * in ms. A live plot scrolls under a still pointer, and numbers that changed
 * every frame could not be read, so they hold between reads, as a meter's
 * display does. The marker on the line follows every frame, and moving the
 * pointer reads at once.
 */
const READ_MS = 250;

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
 * axis text, grid, frame, gap marks, crosshair, the span between cursors,
 * the text on tags, the cursors, the seek strip and the traces, each trace
 * also as GL's rgba. They are kept once every token has a value. A canvas
 * ignores an empty colour and keeps the last one drawn with, so a token the
 * page has not loaded yet is read again on the next frame.
 */
let themed = null;
export function theme() {
  if (!themed) {
    const style = getComputedStyle(document.documentElement);
    let missing = false;
    const token = (name) => {
      const value = style.getPropertyValue(name).trim();
      if (!value) missing = true;
      return value;
    };
    const traces = [1, 2, 3, 4, 5, 6, 7, 8].map((i) => token(`--trace-${i}`));
    const read = {
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
      traces,
      tracesRgba: traces.map(cssRgba),
    };
    if (missing) return read;
    themed = read;
  }
  return themed;
}
/** The trace's colour, one of eight by its signal key, so a signal keeps its colour from hub to hub. */
function colorIndex(trace) {
  if (trace.colorIndex === undefined) {
    let h = 0;
    for (let i = 0; i < trace.key.length; i++) h = (h * 31 + trace.key.charCodeAt(i)) | 0;
    trace.colorIndex = Math.abs(h) % 8;
  }
  return trace.colorIndex;
}
export const colorOf = (trace) => theme().traces[colorIndex(trace)];
/** The trace's colour as GL's rgba. */
const rgbaOf = (trace) => theme().tracesRgba[colorIndex(trace)];

export class PlotView {
  /**
   * `hub` holds the session whose traces it draws. `view` is this plot's own
   * time view, which it draws. `targets` returns
   * the views a gesture on this plot acts on: every selected plot's, when
   * this one is selected, or else its own. `hoverTime` holds the time under the
   * pointer, which every time plot draws a line at.
   */
  constructor({ body, overlay, config, hub, view, targets, hoverTime, plotId, renderer, onFocus }) {
    Object.assign(this, { body, overlay, config, hub, view, targets, hoverTime, plotId, renderer, onFocus });
    this.ctx = overlay.getContext('2d');
    this.y = new Map(); // range key -> {range, auto}
    this.specHover = null;
    this.specCursors = { a: null, b: null };
    this.axisHover = null; // clientX over the x axis, where a drag places a cursor
    this.hover = null; // the pointer over the plot, {x, y} in client px, for the crosshair
    this.hidden = false;
    this.bindInput();
  }

  /** The plot's GPU slot for trace `t`, which holds its window or spectrum. */
  slotOf(t) {
    this.slots ??= new WeakMap(); // Trace -> slot
    let slot = this.slots.get(t);
    if (!slot) this.slots.set(t, (slot = {}));
    return slot;
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

  /** Plot area and lanes, in CSS px relative to the viewport. */
  geometry(body, cfg, n) {
    const right = cfg.mode === 'independent' && n > 1 ? 58 : 12;
    const plot = { x: body.x + 58, y: body.y + 6, w: body.w - 58 - right, h: body.h - 6 - 22 };
    const count = lanesOf(cfg.mode, n);
    const h = (plot.h - LANE_GAP * (count - 1)) / count;
    const lanes = count === 1 ? [plot] : Array.from({ length: count }, (_, i) => ({ x: plot.x, y: plot.y + i * (h + LANE_GAP), w: plot.w, h }));
    return { plot, lanes };
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
    this.renderer.fill(body, panelColor());
    const cfg = this.config();
    const traces = this.traces();
    const geo = this.geometry(body, cfg, traces.length);
    this.geo = geo;
    if (cfg.kind === 'spectrum') this.drawSpectrum(cfg, traces, geo, body);
    else this.drawTime(cfg, traces, geo, body, perf);
  }

  /** The lane of the plot's `i`th trace, one of `lanesOf(cfg.mode, n)`. */
  laneOf(cfg, i) {
    return cfg.mode === 'stacked' ? i : 0;
  }

  drawTime(cfg, traces, geo, body, perf) {
    const [x0, x1] = this.view.range(this.hub.now(perf));
    const session = this.hub.session;
    const plans = traces.map((t, index) => {
      // Paused, a full-resolution window from the server's buffer replaces
      // the live decimated points wherever it covers the view.
      const d = this.view.live ? null : session.detailOf(this.plotId, t);
      const src = d && d.covers(x0, x1) ? d : t;
      const a0 = Math.max(src.absStart, src.lowerBound(x0) - 1);
      const a1 = Math.min(src.absEnd, src.lowerBound(x1) + 1);
      const [lo, hi] = a1 > a0 ? src.extent(a0, a1) : [Infinity, -Infinity];
      return { t, src, a0, a1, lo, hi, index };
    });
    // The time the traces hold, oldest to newest, which a seek moves across.
    let lo = Infinity, hi = -Infinity;
    for (const t of traces) {
      if (!t.length) continue;
      lo = Math.min(lo, t.xAt(t.absStart));
      hi = Math.max(hi, t.xAt(t.absEnd - 1));
    }
    this.history = lo < hi ? [lo, hi] : null;
    // Shared ranges take the union of their traces.
    const ext = new Map();
    for (const p of plans) {
      const k = this.rangeKey(cfg, p.t), e = ext.get(k) ?? [Infinity, -Infinity];
      ext.set(k, [Math.min(e[0], p.lo), Math.max(e[1], p.hi)]);
    }
    const ranges = new Map([...ext].map(([k, [lo, hi]]) => [k, this.yRange(k, lo, hi, true)]));
    plans.forEach((p) => {
      const ring = this.renderer.ring(p.t); // keeps the ring current either way
      const lane = geo.lanes[this.laneOf(cfg, p.index)];
      const [y0, y1] = ranges.get(this.rangeKey(cfg, p.t));
      if (p.src !== p.t) {
        const st = this.renderer.detailBuffer(this.slotOf(p.t), p.src);
        if (this.renderer.lane(lane, st.base, x0, x1, y0, y1, LINE_WIDTH, rgbaOf(p.t)))
          this.renderer.drawDetail(st, p.src, p.a0, p.a1);
      } else if (this.renderer.lane(lane, ring.base, x0, x1, y0, y1, LINE_WIDTH, rgbaOf(p.t))) {
        this.renderer.drawTrace(p.t, ring, p.a0, p.a1);
      }
    });
    this.drawOverlay(cfg, geo, body, plans, ranges, [x0, x1], false);
  }

  drawSpectrum(cfg, traces, geo, body) {
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
      plans.push({ t, s, lo: Math.log10(lo), hi: Math.log10(hi), index: traces.indexOf(t) });
    }
    const ext = new Map();
    for (const p of plans) {
      const k = this.rangeKey(cfg, p.t), e = ext.get(k) ?? [Infinity, -Infinity];
      ext.set(k, [Math.min(e[0], p.lo), Math.max(e[1], p.hi)]);
    }
    const ranges = new Map([...ext].map(([k, [lo, hi]]) => [k, this.yRange(k, lo, hi, false)]));
    const lx0 = Math.log10(fmin), lx1 = Math.log10(fmax);
    for (const p of plans) {
      const lane = geo.lanes[this.laneOf(cfg, p.index)];
      const [y0, y1] = ranges.get(this.rangeKey(cfg, p.t));
      if (!this.renderer.lane(lane, 0, lx0, lx1, y0, y1, LINE_WIDTH, rgbaOf(p.t))) continue;
      this.renderer.drawStatic(this.slotOf(p.t), p.s, () => {
        const pts = new Float32Array(p.s.f.length * 2);
        for (let i = 0; i < p.s.f.length; i++) {
          pts[i * 2] = Math.log10(p.s.f[i]);
          pts[i * 2 + 1] = Math.log10(p.s.a[i]);
        }
        return pts;
      });
    }
    const xd = plans.length ? [fmin, fmax] : null;
    this.drawOverlay(cfg, geo, body, plans, ranges, xd, true, traces);
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

  drawOverlay(cfg, geo, body, plans, ranges, xd, log, allTraces) {
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
    const sx = (log ? scaleLog() : scaleLinear()).domain(xd).range([P.x, P.x + P.w]);
    const t0 = this.hub.session.t0 ?? 0;
    const unlog = (r) => (log ? [10 ** r[0], 10 ** r[1]] : r);
    const yScale = (range, lane) => (log ? scaleLog() : scaleLinear()).domain(unlog(range)).range([lane.y + lane.h, lane.y]);

    // X grid and labels.
    const xticks = log ? sx.ticks(6) : scaleLinear().domain([xd[0] - t0, xd[1] - t0]).ticks(Math.max(2, P.w / 120));
    ctx.fillStyle = T.axisText;
    ctx.textAlign = 'center';
    for (const tv of xticks) {
      const px = Math.round(log ? sx(tv) : sx(tv + t0)) + 0.5;
      if (px < P.x || px > P.x + P.w) continue;
      ctx.strokeStyle = T.grid;
      for (const lane of geo.lanes) {
        ctx.beginPath();
        ctx.moveTo(px, lane.y);
        ctx.lineTo(px, lane.y + lane.h);
        ctx.stroke();
      }
      const label = log ? (Number.isInteger(Math.round(Math.log10(tv) * 1e6) / 1e6) ? `${tv} Hz` : '') : `${+tv.toFixed(4)} s`;
      if (label) ctx.fillText(label, px, P.y + P.h + 15);
    }

    // Y axes: which range each lane's grid and labels follow.
    const axes = []; // {lane, range, side, color}
    if (cfg.mode === 'stacked') {
      plans.forEach((p) => {
        const lane = geo.lanes[this.laneOf(cfg, p.index)];
        axes.push({ lane, range: ranges.get(p.t.key), side: 'left', color: colorOf(p.t) });
      });
    } else if (cfg.mode === 'independent') {
      plans.slice(0, 2).forEach((p, i) => {
        axes.push({ lane: geo.lanes[0], range: ranges.get(p.t.key), side: i === 0 ? 'left' : 'right', color: colorOf(p.t) });
      });
    } else {
      axes.push({ lane: geo.lanes[0], range: ranges.get('shared'), side: 'left', color: T.axisText });
    }
    axes.forEach((ax, i) => {
      const sy = yScale(ax.range, ax.lane);
      const ticks = sy.ticks(Math.max(3, ax.lane.h / 40));
      const fmt = log ? (v) => (Number.isInteger(Math.round(Math.log10(v) * 1e6) / 1e6) ? v.toExponential(0) : '') : sy.tickFormat(8);
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
        ctx.fillStyle = ax.color;
        ctx.fillText(label, ax.side === 'left' ? ax.lane.x - 6 : ax.lane.x + ax.lane.w + 6, py + 4);
      }
    });
    ctx.strokeStyle = T.frame;
    for (const lane of geo.lanes) ctx.strokeRect(lane.x + 0.5, lane.y + 0.5, lane.w - 1, lane.h - 1);

    // Gaps, in the lane of their trace.
    if (!log) {
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
    this.log = log;
    // The hover is where the pointer is now: a live plot scrolls under a still pointer.
    if (this.hover) this.setMarker('hover', this.dataAt(this.hover.x));
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
    if (marks.a !== null && marks.b !== null) {
      const l = Math.max(P.x, sx(Math.min(marks.a, marks.b))), r = Math.min(P.x + P.w, sx(Math.max(marks.a, marks.b)));
      if (r > l) {
        ctx.fillStyle = T.span;
        for (const lane of geo.lanes) ctx.fillRect(l, lane.y, r - l, lane.h);
      }
    }
    // A placed cursor's handle sits on the x axis, where cursors are dragged from.
    const handle = (px, name, color) => {
      ctx.fillStyle = color;
      ctx.fillRect(px - 7, P.y + P.h + 3, 14, 14);
      ctx.fillStyle = T.tagText;
      ctx.textAlign = 'center';
      ctx.fillText(name.toUpperCase(), px, P.y + P.h + 14);
    };
    for (const [name, color] of [['a', T.cursorA], ['b', T.cursorB]]) {
      if (!inView(marks[name])) continue;
      vline(marks[name], color);
      handle(sx(marks[name]), name, color);
    }
    // Hovering the axis shows the cursor a drag there would place.
    const next = marks.a === null ? 'a' : marks.b === null ? 'b' : null;
    const ax = this.axisHover;
    if (next && ax !== null && ax >= P.x && ax <= P.x + P.w && !this.cursorNear(ax)) {
      ctx.globalAlpha = 0.45;
      handle(ax, next, next === 'a' ? T.cursorA : T.cursorB);
      ctx.globalAlpha = 1;
    }
    if (inView(marks.hover)) vline(marks.hover, T.crosshair);
    // The crosshair's horizontal line, in the lane under the pointer, with the
    // pointer's value tagged on each of that lane's y axes.
    const hy = this.hover?.y ?? null;
    const hoverLane = hy === null ? null : geo.lanes.find((l) => hy >= l.y && hy <= l.y + l.h);
    if (hoverLane && marks.hover !== null) {
      const py = Math.round(hy) + 0.5;
      ctx.strokeStyle = T.crosshair;
      ctx.beginPath();
      ctx.moveTo(hoverLane.x, py);
      ctx.lineTo(hoverLane.x + hoverLane.w, py);
      ctx.stroke();
      for (const a of axes) {
        if (a.lane !== hoverLane) continue;
        const v = yScale(a.range, a.lane).invert(hy);
        const text = log ? v.toExponential(2) : v.toPrecision(4);
        const w = ctx.measureText(text).width + 8;
        const x = a.side === 'left' ? a.lane.x - w : a.lane.x + a.lane.w;
        ctx.fillStyle = a.color;
        ctx.fillRect(x, py - 8, w, 16);
        ctx.fillStyle = T.tagText;
        ctx.textAlign = 'center';
        ctx.fillText(text, x + w / 2, py + 4);
      }
    }

    // Readouts, which the page shows in the plot's header: each trace's value
    // at the hover, or its newest value while live, and its values at A and
    // B with B - A.
    const fx = log ? (f) => `${f.toPrecision(4)} Hz` : (t) => `${(t - t0).toFixed(3)} s`;
    const fy = log ? (v) => v.toExponential(3) : (v) => v.toPrecision(5);
    const rows = {};
    // Where a pixel's column holds several points, the hover reads all of
    // them, lowest to highest, as one point there would be any of them.
    const halfPx = log || marks.hover === null ? 0 : (xd[1] - xd[0]) / P.w / 2;
    const now = performance.now(), held = this.hoverRead;
    const hold = held && this.hover && held.x === this.hover.x && held.y === this.hover.y && now - held.at < READ_MS;
    // What the numbers say, held between reads; the marker reads anew.
    const reads = hold ? held.reads : new Map();
    let near = null; // the signal whose point at the hover is nearest the pointer, in the pointer's lane
    plans.forEach((p) => {
      const lane = this.laneOf(cfg, p.index);
      const sy = yScale(ranges.get(this.rangeKey(cfg, p.t)), geo.lanes[lane]);
      const at = (v) => (v === null ? null : log ? spectrumAt(p.s, v) : traceAt(p.src, v));
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
      else if (!log && this.view.live && p.t.length) value = fy(p.t.yAt(p.t.absEnd - 1));
      // Where the points come from: raw samples, or how many raw rows per point.
      let info = `${p.t.meta.rate} Hz`;
      if (log && p.s.floor) info += ` · floor ${p.s.floor.toExponential(2)}/√Hz`;
      if (!log) info += p.src !== p.t ? (p.src.ratio === 1 ? ' · raw' : ` · 1:${p.src.ratio}`) : ' · live';
      // A spectrum's values keep their exponent, its units being per √Hz.
      const fv = log ? fy : (v) => si(v, p.t.meta.units);
      const cell = (pt) => (pt ? fv(pt.y) : '—');
      rows[p.t.key] = { value, info, a: cell(pts.a), b: cell(pts.b), d: pts.a && pts.b ? fv(pts.b.y - pts.a.y) : '—' };
    });
    const delta = marks.a !== null && marks.b !== null;
    const xUnit = log ? 'Hz' : 's';
    const cursors =
      marks.a === null && marks.b === null
        ? null
        : {
            x: log ? 'f' : 't',
            a: marks.a === null ? '—' : fx(marks.a),
            b: marks.b === null ? '—' : fx(marks.b),
            d: delta ? si(marks.b - marks.a, xUnit) : '—',
            rate: delta && !log && marks.b !== marks.a ? si(1 / Math.abs(marks.b - marks.a), 'Hz') : '',
          };
    this.legend = { rows, cursors };
    if (!hold) this.hoverRead = this.hover ? { x: this.hover.x, y: this.hover.y, at: now, reads } : null;
    if (near) {
      const u = near.t.meta.units;
      const { v, span } = near.read ?? {};
      const value = v === undefined ? '' : log ? fy(v) : span ? siRange(span.lo, span.hi, u) : si(v, u);
      this.drawTip(ctx, T, hoverLane, near, value, sx(marks.hover), hy);
    }

    // While a seek drags along the time axis, the axis shows the whole
    // history, and the window as a light box over it.
    if (this.seeking && !log && this.history) {
      const [h0, h1] = this.history, top = P.y + P.h + 2, height = 18;
      const at = (t) => P.x + ((Math.min(Math.max(t, h0), h1) - h0) / (h1 - h0)) * P.w;
      ctx.fillStyle = T.seekBg;
      ctx.fillRect(P.x, top, P.w, height);
      const x0 = at(xd[0]), x1 = Math.max(at(xd[1]), x0 + 3);
      ctx.fillStyle = T.seekWindow;
      ctx.fillRect(x0, top, x1 - x0, height);
      ctx.strokeStyle = T.seekEdge;
      ctx.strokeRect(Math.round(x0) + 0.5, top + 0.5, Math.round(x1 - x0) - 1, height - 1);
    }
  }

  /** The hover position and the A and B cursors, in this plot's x units. */
  markers() {
    if (this.config().kind === 'spectrum') return { hover: this.specHover, ...this.specCursors };
    return { hover: this.hoverTime.t, ...this.view.cursors };
  }

  setMarker(name, value) {
    if (this.config().kind === 'spectrum') {
      if (name === 'hover') this.specHover = value;
      else this.specCursors[name] = value;
    } else if (name === 'hover') this.hoverTime.t = value;
    else for (const v of this.targets()) v.cursors[name] = value;
  }

  /** The data x under clientX, and back. */
  dataAt(clientX) {
    const P = this.geo?.plot, xd = this.xd;
    if (!P || !xd) return null;
    const f = (clientX - P.x) / P.w;
    if (this.log) return 10 ** (Math.log10(xd[0]) + f * (Math.log10(xd[1]) - Math.log10(xd[0])));
    return xd[0] + f * (xd[1] - xd[0]);
  }
  pxOf(v) {
    const P = this.geo?.plot, xd = this.xd;
    if (!P || !xd || v === null) return null;
    const f = this.log ? (Math.log10(v) - Math.log10(xd[0])) / (Math.log10(xd[1]) - Math.log10(xd[0])) : (v - xd[0]) / (xd[1] - xd[0]);
    return P.x + f * P.w;
  }
  /** The placed cursor within a few pixels of clientX, if any. */
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
    const side = this.yAxisAt(x, y);
    if (side) return { kind: 'y', keys: this.axisTargets(side, y) };
    const P = this.geo?.plot;
    if (P && y > P.y + P.h && x >= P.x && x <= P.x + P.w) return { kind: 'x' };
    return { kind: 'plot', keys: this.yTargets(y) };
  }

  /** The y range of `key` in the units its axis shows, or null before it has one. */
  yBounds(key) {
    const range = this.y.get(key)?.range;
    if (!range) return null;
    return this.log ? range.map((v) => 10 ** v) : [...range];
  }

  /** Sets the y range of `keys` to [lo, hi], in the units the axis shows, to hold until a fit. */
  setYBounds(keys, lo, hi) {
    for (const k of keys) {
      let st = this.y.get(k);
      if (!st) this.y.set(k, (st = { range: null, auto: false }));
      st.auto = false;
      st.range = this.log ? [Math.log10(lo), Math.log10(hi)] : [lo, hi];
    }
  }

  /** Fits the y range of `keys` to what they show, on the next frame. */
  fit(keys) {
    for (const k of keys) {
      const st = this.y.get(k);
      if (st) st.auto = true;
    }
  }

  /** Which y axis band (x, y) is over, 'left' or 'right', or null. */
  yAxisAt(x, y) {
    const P = this.geo?.plot;
    if (!P || y < P.y || y > P.y + P.h) return null;
    if (x < P.x) return 'left';
    if (x > P.x + P.w) return 'right';
    return null;
  }

  /**
   * The range keys a gesture on the `side` y axis at clientY acts on. In
   * independent mode the left axis is the first trace's and the right the
   * second's.
   */
  axisTargets(side, clientY) {
    const cfg = this.config(), traces = this.traces();
    if (cfg.mode === 'independent') {
      const t = traces[side === 'left' ? 0 : 1];
      return t ? [t.key] : [];
    }
    return side === 'left' ? this.yTargets(clientY) : [];
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
    // value under the pointer, and zooms time elsewhere.
    el.addEventListener('wheel', (e) => {
      e.preventDefault();
      const factor = Math.exp(e.deltaY * 0.0015);
      const side = this.yAxisAt(e.clientX, e.clientY);
      if (side || e.shiftKey) {
        const keys = side ? this.axisTargets(side, e.clientY) : this.yTargets(e.clientY);
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
        for (const v of this.targets()) v.zoom(factor, fracX(e.clientX), performance.now());
      }
    }, { passive: false });
    // The band under the plot holds the x axis, and cursors are dragged out of it.
    const onAxis = (e) => {
      const P = this.geo?.plot;
      return !!P && this.xd && e.clientY > P.y + P.h && e.clientX >= P.x && e.clientX <= P.x + P.w;
    };
    // Places a new cursor at clientX, or moves the nearer one when both are
    // placed, and returns its name. A time cursor holds a moment the live
    // view would scroll away, so placing one pauses the view.
    const placeCursor = (clientX) => {
      const m = this.markers();
      const cursor = m.a === null ? 'a' : m.b === null ? 'b' : Math.abs(this.pxOf(m.a) - clientX) <= Math.abs(this.pxOf(m.b) - clientX) ? 'a' : 'b';
      if (isTime()) for (const v of this.targets()) v.pause(performance.now());
      this.setMarker(cursor, this.dataAt(clientX));
      return cursor;
    };
    // A drag's mode: 'cursor' moves one, 'axis' waits to learn its
    // direction on the time axis, 'seek' moves the window along the whole
    // history, 'y' offsets a y axis, and 'plot' pans.
    el.addEventListener('pointerdown', (e) => {
      this.onFocus?.();
      if (e.button !== 0) return;
      el.setPointerCapture(e.pointerId);
      const cursor = this.cursorNear(e.clientX);
      const side = this.yAxisAt(e.clientX, e.clientY);
      const mode = cursor ? 'cursor' : onAxis(e) ? 'axis' : side ? 'y' : 'plot';
      const targets = side ? this.axisTargets(side, e.clientY) : this.yTargets(e.clientY);
      drag = { x: e.clientX, y: e.clientY, x0: e.clientX, y0: e.clientY, moved: 0, cursor, mode, targets };
    });
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
      // Each region names its drag: a cursor or the time axis moves sideways,
      // a y axis up and down, and the plot pans by hand.
      if (drag?.mode === 'plot' && drag.started) el.style.cursor = 'grabbing';
      else el.style.cursor = drag?.cursor || this.cursorNear(e.clientX) || this.axisHover !== null ? 'ew-resize' : yAxis ? 'ns-resize' : 'grab';
      if (!drag) return;
      // A pan or an offset starts once the pointer has moved off the press,
      // and its first step counts from the press, so nothing is lost.
      if ((drag.mode === 'plot' || drag.mode === 'y') && !drag.started) {
        if (Math.hypot(e.clientX - drag.x0, e.clientY - drag.y0) < DRAG_START) return;
        drag.started = true;
      }
      if (drag.mode === 'axis') {
        // Along the time axis seeks, and up into the plot draws a cursor out.
        const ax = e.clientX - drag.x0, ay = e.clientY - drag.y0;
        if (Math.hypot(ax, ay) < DRAG_START) return;
        if (Math.abs(ax) >= Math.abs(ay) && isTime()) {
          drag.mode = 'seek';
          this.seeking = true;
        } else {
          drag.mode = 'cursor';
          drag.cursor = placeCursor(drag.x0);
        }
      }
      const dx = e.clientX - drag.x, dy = e.clientY - drag.y;
      drag.moved += Math.abs(dx) + Math.abs(dy);
      drag.x = e.clientX;
      drag.y = e.clientY;
      if (drag.mode === 'cursor') {
        this.setMarker(drag.cursor, this.dataAt(e.clientX));
      } else if (drag.mode === 'seek') {
        // The window moves with the pointer, at the scale of the whole history.
        const [lo, hi] = this.history ?? [0, 0];
        for (const v of this.targets()) v.pan((dx * Math.max(hi - lo, v.span)) / P.w / v.span, performance.now());
      } else if (drag.mode === 'y' || e.altKey || !isTime()) {
        offsetY(drag.targets, dy, drag.y0);
      } else {
        for (const v of this.targets()) v.pan(-dx / P.w, performance.now());
      }
    });
    el.addEventListener('pointerup', (e) => {
      const P = this.geo?.plot;
      // A click on the time axis places a cursor there.
      if (drag?.mode === 'axis') placeCursor(drag.x0);
      // A cursor dragged off the side of the plot is removed.
      if (drag?.mode === 'cursor' && P && (e.clientX < P.x || e.clientX > P.x + P.w)) this.setMarker(drag.cursor, null);
      drag = null;
      this.seeking = false;
    });
    el.addEventListener('pointercancel', () => {
      drag = null;
      this.seeking = false;
    });
    el.addEventListener('pointerleave', () => {
      this.setMarker('hover', null);
      this.axisHover = null;
      this.hover = null;
    });
    // A double-click on a y axis fits y. On the plot it follows the newest
    // samples again, from wherever the view was.
    el.addEventListener('dblclick', (e) => {
      // Two clicks on the time axis place cursors, so they do nothing more.
      if (onAxis(e)) return;
      if (this.yAxisAt(e.clientX, e.clientY)) this.autoscale();
      else for (const v of this.targets()) v.follow();
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
    this.shownSpectra ??= new WeakMap(); // Trace -> spectrum
    if (this.view.live || !this.shownSpectra.has(t)) this.shownSpectra.set(t, t.spectrum);
    return this.shownSpectra.get(t);
  }

}

/**
 * One time plot's x range: live, trailing the newest samples by `delay`, or
 * paused at `right`. It reads the time from the hub, so it outlives any one
 * hub's session.
 */
export class View {
  live = true;
  span = 10;
  delay = 0.15;
  right = 0; // device time of the right edge when paused
  lag = 0; // how far a streaming view trails the newest samples, in seconds
  cursors = { a: null, b: null }; // placed, device time
  listeners = new Set();

  constructor(hub) {
    this.hub = hub;
  }

  range(now) {
    const right = this.live ? now - this.delay - this.lag : this.right;
    return [right - this.span, right];
  }

  /** Whether the view follows the newest samples, streaming and not behind them. */
  get atNewest() {
    return this.live && this.lag === 0;
  }

  /**
   * Zooms by `factor` about the point at fraction `anchor` across the plot,
   * which stays under the pointer. Zooming in anywhere but the newest edge
   * holds a moment the live view would scroll away, so it pauses the view.
   * A paused view that zooms out until its right edge reaches the newest
   * samples goes back to the stream, unless time cursors hold it.
   */
  zoom(factor, anchor, perf) {
    const span = Math.min(120, Math.max(0.002, this.span * factor));
    if (this.live && factor < 1 && anchor < LIVE_EDGE) this.pause(perf);
    if (!this.live) {
      const newest = this.hub.now(perf) - this.delay;
      const [x0] = this.range(newest);
      const at = x0 + anchor * this.span;
      this.right = at + (1 - anchor) * span;
      if (this.right >= newest && !this.held()) [this.live, this.lag] = [true, 0];
    }
    this.span = span;
    this.notify();
  }

  /** Whether time cursors hold the view where it is. */
  held() {
    return this.cursors.a !== null || this.cursors.b !== null;
  }

  /** Holds the view where it is; samples keep arriving behind it. */
  pause(perf) {
    if (!this.live) return;
    this.right = this.hub.now(perf) - this.delay - this.lag;
    this.live = false;
    this.notify();
  }

  /**
   * Streams again from where the paused view is. It scrolls on with the
   * samples as they come, as far behind the newest, or ahead of them, as it
   * is now. The time cursors go, as they would scroll away.
   */
  resume(perf) {
    if (this.live) return;
    this.lag = this.hub.now(perf) - this.delay - this.right;
    this.live = true;
    this.cursors = { a: null, b: null };
    this.notify();
  }

  /**
   * Moves the view by `fraction` of its span, later for a positive one, and
   * pauses it. A pan goes anywhere, past the newest samples too. A view
   * left past them stays where it is while new samples fill it in, and only
   * Follow live makes it follow them again.
   */
  pan(fraction, perf) {
    this.pause(perf);
    this.right += fraction * this.span;
    this.notify();
  }

  /** Follows the newest samples again. The time cursors go: live, they would scroll away. */
  follow() {
    this.live = true;
    this.lag = 0;
    this.cursors = { a: null, b: null };
    this.notify();
  }

  onChange(fn) {
    this.listeners.add(fn);
    return () => this.listeners.delete(fn);
  }

  notify() {
    for (const fn of this.listeners) fn();
  }
}
