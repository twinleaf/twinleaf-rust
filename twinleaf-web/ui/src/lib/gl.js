// A WebGL canvas behind the plots drawn on it: the page's one, behind every
// plot of its tabs, or a figure's own (plothost.js). A Renderer draws on the
// canvas within its frame: where the canvas is on the page, how large, and
// how many device pixels it gives a CSS px, which whoever makes the Renderer
// says, so a figure is drawn at the size and pixel ratio asked of it and not
// at the window's.
//
// Each trace has one GPU ring buffer of (x - base, y) points, uploaded once
// and drawn by every plot that shows it. A frame sends only the points that
// arrived since the last one; pan, zoom and scrolling change uniforms only.
// A window or a spectrum has a buffer per plot and trace, a slot the plot
// keeps, uploaded again only when what it holds changes. Buffers are keyed
// by Trace and slot in WeakMaps, so they go with their trace or plot, and a
// new hub's traces never meet an old hub's buffers.
//
// A line is drawn as one anti-aliased quad per segment, instanced over the
// ring buffer itself: attribute `a` reads point i and `b` reads point i + 1.
// A gap is just the end of one draw range and the start of the next.
//
// Precision: x is stored relative to a base near the newest point and kept
// within REBASE_SECONDS of it, and the shader subtracts the view's origin
// before it scales, so f32 never holds absolute device time.

const REBASE_SECONDS = 30;

const VS = `#version 300 es
precision highp float;
layout(location = 0) in vec2 a;
layout(location = 1) in vec2 b;
layout(location = 2) in vec2 corner; // (along 0..1, side -1..1)
uniform vec2 uOrigin;   // data value at the lane's left/bottom edge, relative to base
uniform vec2 uScale;    // clip units per data unit, times two
uniform vec2 uViewport; // lane size in device pixels
uniform float uWidth;   // line width in device pixels
out float vSide;
void main() {
  vec2 ca = (a - uOrigin) * uScale - 1.0;
  vec2 cb = (b - uOrigin) * uScale - 1.0;
  vec2 half_vp = 0.5 * uViewport;
  vec2 pa = ca * half_vp, pb = cb * half_vp;
  vec2 d = pb - pa;
  float len = length(d);
  vec2 dir = len > 1e-6 ? d / len : vec2(1.0, 0.0);
  vec2 n = vec2(-dir.y, dir.x);
  float hw = 0.5 * uWidth + 1.0;
  vec2 p = mix(pa, pb, corner.x) + n * corner.y * hw + dir * (corner.x * 2.0 - 1.0) * 0.5 * uWidth;
  vSide = corner.y * hw;
  gl_Position = vec4(p / half_vp, 0.0, 1.0);
}`;

const FS = `#version 300 es
precision highp float;
in float vSide;
uniform vec4 uColor;
uniform float uWidth;
out vec4 outColor;
void main() {
  float alpha = clamp(0.5 * uWidth + 0.5 - abs(vSide), 0.0, 1.0);
  outColor = vec4(uColor.rgb * uColor.a * alpha, uColor.a * alpha);
}`;

function compile(gl, type, src) {
  const s = gl.createShader(type);
  gl.shaderSource(s, src);
  gl.compileShader(s);
  if (!gl.getShaderParameter(s, gl.COMPILE_STATUS)) throw new Error(gl.getShaderInfoLog(s));
  return s;
}

/** One trace's ring buffer on the GPU. Slot `cap` mirrors slot 0, so a draw across the wrap stays one strip. */
class GpuRing {
  constructor(gl) {
    this.gl = gl;
    this.buf = gl.createBuffer();
    this.cap = 0;
    this.base = 0;
    this.uploadedTo = 0; // absolute index uploaded up to
    this.uploadedBytes = 0;
  }

  slot(abs) {
    return abs % this.cap;
  }

  /** Writes absolute points [from, to) of `trace` into their ring slots. */
  write(trace, from, to) {
    const gl = this.gl;
    gl.bindBuffer(gl.ARRAY_BUFFER, this.buf);
    let a = from;
    while (a < to) {
      const s = this.slot(a);
      const n = Math.min(to - a, this.cap - s);
      const data = new Float32Array(n * 2);
      for (let i = 0; i < n; i++) {
        const local = a + i - trace.shift;
        data[i * 2] = trace.x[local] - this.base;
        data[i * 2 + 1] = trace.y[local];
      }
      gl.bufferSubData(gl.ARRAY_BUFFER, s * 8, data);
      if (s === 0) gl.bufferSubData(gl.ARRAY_BUFFER, this.cap * 8, data.subarray(0, 2));
      this.uploadedBytes += data.byteLength;
      a += n;
    }
  }

  sync(trace) {
    const gl = this.gl;
    const start = trace.absStart, end = trace.absEnd;
    if (end === start) return;
    const newest = trace.x[trace.end - 1];
    let full = false;
    if (this.cap < (end - start) * 1.1 || this.cap === 0) {
      let cap = 4096;
      while (cap < (end - start) * 2) cap *= 2;
      this.cap = cap;
      gl.bindBuffer(gl.ARRAY_BUFFER, this.buf);
      gl.bufferData(gl.ARRAY_BUFFER, (cap + 1) * 8, gl.DYNAMIC_DRAW);
      full = true;
    }
    if (Math.abs(newest - this.base) > REBASE_SECONDS) {
      this.base = newest;
      full = true;
    }
    if (this.uploadedTo < Math.max(start, end - this.cap)) full = true;
    const from = full ? Math.max(start, end - this.cap) : this.uploadedTo;
    if (from < end) this.write(trace, from, end);
    this.uploadedTo = end;
  }

  /** The oldest absolute index still in the ring. */
  oldest(trace) {
    return Math.max(trace.absStart, trace.absEnd - this.cap);
  }
}

/** A buffer uploaded once, whole, for a window or a spectrum. */
class GpuStatic {
  constructor(gl) {
    this.gl = gl;
    this.buf = gl.createBuffer();
    this.n = 0;
    this.source = null; // what its points are of
  }

  /** Uploads `points` of `source` in place of what it held, and returns their bytes. */
  set(source, points) {
    const gl = this.gl;
    gl.bindBuffer(gl.ARRAY_BUFFER, this.buf);
    gl.bufferData(gl.ARRAY_BUFFER, points, gl.DYNAMIC_DRAW);
    this.n = points.length / 2;
    this.source = source;
    return points.byteLength;
  }
}

/** The frame of the page's canvas: the window, at the screen's pixel ratio. */
export const windowFrame = () => ({ x: 0, y: 0, w: window.innerWidth, h: window.innerHeight, dpr: window.devicePixelRatio || 1 });

/** The frame of a canvas that fills element `el`, at `dpr()` device pixels a CSS px. */
export const boxFrame = (el, dpr) => () => {
  const r = el.getBoundingClientRect();
  return { x: r.left, y: r.top, w: r.width, h: r.height, dpr: dpr() };
};

export class Renderer {
  /** Gives the context and every buffer back to the browser. */
  dispose() {
    this.gl.getExtension('WEBGL_lose_context')?.loseContext();
  }

  /**
   * `canvas` is drawn on within what `frame()` gives as each frame begins,
   * {x, y, w, h, dpr}: the canvas's box in CSS px of the viewport, and the
   * device pixels it gives each (`windowFrame`, `boxFrame`). `grid()` is the
   * pixel ratio whose pixels a plot keeps its time axis on (plot.js
   * `onPixelGrid`): its pictures', the frame's own unless given.
   */
  constructor(canvas, frame, grid = () => this.dpr) {
    this.canvas = canvas;
    this.frame = frame;
    this.grid = grid;
    /** The frame as the last `begin` read it. */
    this.box = frame();
    this.dpr = this.box.dpr;
    const gl = canvas.getContext('webgl2', { antialias: false, alpha: true, premultipliedAlpha: true });
    if (!gl) throw new Error('WebGL2 is not available');
    this.gl = gl;
    const prog = gl.createProgram();
    gl.attachShader(prog, compile(gl, gl.VERTEX_SHADER, VS));
    gl.attachShader(prog, compile(gl, gl.FRAGMENT_SHADER, FS));
    gl.linkProgram(prog);
    if (!gl.getProgramParameter(prog, gl.LINK_STATUS)) throw new Error(gl.getProgramInfoLog(prog));
    this.prog = prog;
    this.u = Object.fromEntries(
      ['uOrigin', 'uScale', 'uViewport', 'uWidth', 'uColor'].map((n) => [n, gl.getUniformLocation(prog, n)]),
    );
    this.vao = gl.createVertexArray();
    gl.bindVertexArray(this.vao);
    const corners = gl.createBuffer();
    gl.bindBuffer(gl.ARRAY_BUFFER, corners);
    gl.bufferData(gl.ARRAY_BUFFER, new Float32Array([0, -1, 0, 1, 1, -1, 1, 1]), gl.STATIC_DRAW);
    gl.enableVertexAttribArray(2);
    gl.vertexAttribPointer(2, 2, gl.FLOAT, false, 0, 0);
    gl.enableVertexAttribArray(0);
    gl.enableVertexAttribArray(1);
    gl.vertexAttribDivisor(0, 1);
    gl.vertexAttribDivisor(1, 1);
    this.rings = new WeakMap(); // Trace -> GpuRing
    this.statics = new WeakMap(); // slot -> GpuStatic
    this.stats = { drawCalls: 0, segments: 0, uploadBytes: 0 };
    const dbg = gl.getExtension('WEBGL_debug_renderer_info');
    this.gpu = dbg ? gl.getParameter(dbg.UNMASKED_RENDERER_WEBGL) : 'unknown';
  }

  /**
   * Whether the canvas can be drawn on whole at `w` by `h` device pixels:
   * within the largest viewport and render buffer GL has, and given a
   * drawing buffer that size, which a browser makes smaller past what it
   * allows. The canvas is cleared, and drawn again at the next frame.
   */
  fits(w, h) {
    const gl = this.gl, [vw, vh] = gl.getParameter(gl.MAX_VIEWPORT_DIMS), most = gl.getParameter(gl.MAX_RENDERBUFFER_SIZE);
    if (w > Math.min(vw, most) || h > Math.min(vh, most)) return false;
    const { width, height } = this.canvas;
    this.canvas.width = w;
    this.canvas.height = h;
    const whole = gl.drawingBufferWidth === w && gl.drawingBufferHeight === h;
    this.canvas.width = width;
    this.canvas.height = height;
    return whole;
  }

  /** Reads the frame, sizes the canvas to it, at `ratio` device pixels a CSS px in place of the frame's when given, and clears it. Returns the device pixel ratio. */
  begin(ratio = null) {
    const gl = this.gl, read = this.frame(), box = (this.box = ratio === null ? read : { ...read, dpr: ratio }), dpr = box.dpr;
    const w = Math.round(box.w * dpr), h = Math.round(box.h * dpr);
    if (this.canvas.width !== w || this.canvas.height !== h) {
      this.canvas.width = w;
      this.canvas.height = h;
    }
    this.dpr = dpr;
    gl.disable(gl.SCISSOR_TEST);
    gl.clearColor(0, 0, 0, 0);
    gl.clear(gl.COLOR_BUFFER_BIT);
    gl.enable(gl.BLEND);
    gl.blendFunc(gl.ONE, gl.ONE_MINUS_SRC_ALPHA);
    gl.useProgram(this.prog);
    gl.bindVertexArray(this.vao);
    this.stats = { drawCalls: 0, segments: 0, uploadBytes: 0 };
    return dpr;
  }

  /** Whether a CSS-pixel rect in the viewport, {x, y, w, h}, has any height within the frame. */
  shows(r) {
    return r.y + r.h > this.box.y && r.y < this.box.y + this.box.h;
  }

  /** Where a CSS-pixel rect in the viewport lies on the canvas, as {x, y, w, h} in device pixels from its top left. */
  deviceRect(r) {
    const dpr = this.dpr;
    return { x: Math.round((r.x - this.box.x) * dpr), y: Math.round((r.y - this.box.y) * dpr), w: Math.round(r.w * dpr), h: Math.round(r.h * dpr) };
  }

  /** Converts a CSS-pixel rect in the viewport to a GL rect in device pixels, or null when off the canvas. */
  glRect(r) {
    const dpr = this.dpr, box = this.box;
    const x = Math.round((r.x - box.x) * dpr), w = Math.round(r.w * dpr), h = Math.round(r.h * dpr);
    const y = this.canvas.height - Math.round((r.y - box.y + r.h) * dpr);
    if (w <= 0 || h <= 0 || x + w < 0 || y + h < 0 || x > this.canvas.width || y > this.canvas.height) return null;
    return { x, y, w, h };
  }

  /** Fills `rect` (CSS px) with an opaque color. */
  fill(rect, rgba) {
    const g = this.glRect(rect);
    if (!g) return;
    const gl = this.gl;
    gl.enable(gl.SCISSOR_TEST);
    gl.scissor(g.x, g.y, g.w, g.h);
    gl.clearColor(...rgba);
    gl.clear(gl.COLOR_BUFFER_BIT);
  }

  /** The ring of `trace`, made current. */
  ring(trace) {
    let ring = this.rings.get(trace);
    if (!ring) this.rings.set(trace, (ring = new GpuRing(this.gl)));
    const before = ring.uploadedBytes;
    ring.sync(trace);
    this.stats.uploadBytes += ring.uploadedBytes - before;
    return ring;
  }

  /** Sets the lane's viewport, scissor and transform. x0/x1, y0/y1 are absolute data values. */
  lane(rect, base, x0, x1, y0, y1, width, color) {
    const g = this.glRect(rect);
    if (!g) return false;
    const gl = this.gl;
    gl.viewport(g.x, g.y, g.w, g.h);
    gl.enable(gl.SCISSOR_TEST);
    gl.scissor(g.x, g.y, g.w, g.h);
    gl.uniform2f(this.u.uOrigin, x0 - base, y0);
    gl.uniform2f(this.u.uScale, 2 / (x1 - x0), 2 / (y1 - y0));
    gl.uniform2f(this.u.uViewport, g.w, g.h);
    // Never thinner than a device pixel, so a picture drawn small keeps its lines whole.
    gl.uniform1f(this.u.uWidth, Math.max(1, width * this.dpr));
    gl.uniform4f(this.u.uColor, ...color);
    return true;
  }

  /** Draws the strip of points [first, first + count) of `buf`, as count - 1 segments. */
  strip(buf, first, count) {
    if (count < 2) return;
    const gl = this.gl;
    gl.bindBuffer(gl.ARRAY_BUFFER, buf);
    gl.vertexAttribPointer(0, 2, gl.FLOAT, false, 8, first * 8);
    gl.vertexAttribPointer(1, 2, gl.FLOAT, false, 8, (first + 1) * 8);
    gl.drawArraysInstanced(gl.TRIANGLE_STRIP, 0, 4, count - 1);
    this.stats.drawCalls++;
    this.stats.segments += count - 1;
  }

  /** Draws absolute points [a0, a1) of a trace's ring, split at its breaks. */
  drawTrace(trace, ring, a0, a1) {
    a0 = Math.max(a0, ring.oldest(trace));
    if (a1 - a0 < 2) return;
    let s = a0;
    const breaks = trace.breaks;
    let k = lowerBound(breaks, a0 + 1);
    for (; k < breaks.length && breaks[k] < a1; k++) {
      this.ringRange(ring, s, breaks[k]);
      s = breaks[k];
    }
    this.ringRange(ring, s, a1);
  }

  ringRange(ring, a0, a1) {
    const n = a1 - a0;
    if (n < 2) return;
    const s0 = ring.slot(a0);
    if (s0 + n <= ring.cap + 1) {
      this.strip(ring.buf, s0, n);
    } else {
      // Up to the mirror of slot 0, then on from slot 0.
      const first = ring.cap + 1 - s0;
      this.strip(ring.buf, s0, first);
      this.strip(ring.buf, 0, n - first + 1);
    }
  }

  /** The buffer of `slot`, holding the points `makePoints` makes of `source`, uploaded again only when `source` changes. */
  staticBuffer(slot, source, makePoints) {
    let st = this.statics.get(slot);
    if (!st) this.statics.set(slot, (st = new GpuStatic(this.gl)));
    if (st.source !== source) this.stats.uploadBytes += st.set(source, makePoints());
    return st;
  }

  /** The buffer in `slot` of a full-resolution window. Its x is relative to the window's t0, its `base`. */
  detailBuffer(slot, detail) {
    const st = this.staticBuffer(slot, detail, () => {
      const pts = new Float32Array(detail.length * 2);
      for (let i = 0; i < detail.length; i++) {
        pts[i * 2] = detail.x[i] - detail.t0;
        pts[i * 2 + 1] = detail.y[i];
      }
      return pts;
    });
    st.base = detail.t0;
    return st;
  }

  /** Draws points [a0, a1) of a window, split at its breaks. */
  drawDetail(st, detail, a0, a1) {
    let s = a0;
    let k = lowerBound(detail.breaks, a0 + 1);
    for (; k < detail.breaks.length && detail.breaks[k] < a1; k++) {
      this.strip(st.buf, s, detail.breaks[k] - s);
      s = detail.breaks[k];
    }
    this.strip(st.buf, s, a1 - s);
  }

  /** Draws the whole buffer of `slot`, the points `makePoints` makes of `source`. */
  drawStatic(slot, source, makePoints) {
    const st = this.staticBuffer(slot, source, makePoints);
    this.strip(st.buf, 0, st.n);
  }
}

export function lowerBound(arr, v) {
  let lo = 0, hi = arr.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (arr[mid] < v) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}
