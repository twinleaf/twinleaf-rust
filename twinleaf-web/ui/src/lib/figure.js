// A plot as a figure for an image file: the plot's picture (PlotHost
// `picture`) with what an image adds and a live plot has no use for, each
// in a band of its own: a title above, and below an x label and a caption,
// the device at its left and the time shown at its right, on one line or,
// where they do not both fit, two. Also how large an image is, in CSS px
// and in the file's pixels, and how what the page lays over a plot, as its
// legend, is drawn onto a canvas.
//
// A figure is {title, xLabel, caption}: the title and the x label as text,
// '' for none, and the caption as {left, right}, either '' for none, or
// null for no caption.

/** Each band's font, as a canvas takes it but for the face, its size and the room it takes, in CSS px. */
export const BANDS = {
  title: { font: '600 15px', face: '--font-ui', h: 26, base: 18 },
  xLabel: { font: '12px', face: '--font-ui', h: 16, base: 11 },
  caption: { font: '11px', face: '--font-data', line: 15, base: 11, pad: 3, gap: 16, side: 8 },
};

/** What a length is in CSS px: 96 a CSS inch. */
export const UNITS = { px: 1, in: 96, cm: 96 / 2.54 };
/** The shapes an image may take, as [value, name, width over height]; 'screen' is the plot's own on the page, and 'custom' any. */
export const SHAPES = [
  ['screen', 'As on screen', null],
  ['wide', '16 : 9', 16 / 9],
  ['std', '4 : 3', 4 / 3],
  ['square', 'Square', 1],
  ['custom', 'Custom', null],
];
/** The image formats, by value: the name the picker shows, the MIME type and the extension. */
export const IMAGE_FORMATS = {
  png: { name: 'PNG', mime: 'image/png', ext: 'png', description: 'PNG image' },
  jpg: { name: 'JPEG', mime: 'image/jpeg', ext: 'jpg', description: 'JPEG image', quality: 0.92 },
};
/** The fewest CSS px an image is across or down. */
export const LEAST_SIDE = 120;

/**
 * The pixel ratio of an image `unit` long a side at `dpi`, and its size in
 * the file's pixels when it is `width` by `height` CSS px: a size in px is
 * the file's pixels, and one in inches or centimetres holds `dpi` pixels an
 * inch.
 */
export function pictureSize({ width, height, unit, dpi }) {
  const ratio = unit === 'px' ? 1 : dpi / 96;
  return { ratio, pixels: [Math.round(width * ratio), Math.round(height * ratio)] };
}

/** The value of CSS token `name` on the page's root. */
const token = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
/** Band `b`'s font, as a canvas takes it. */
const fontOf = (b) => `${b.font} ${token(b.face)}`;
let measurer = null;
/** The width of `text` in `font`, in CSS px. */
function measure(text, font) {
  measurer ??= document.createElement('canvas').getContext('2d');
  measurer.font = font;
  return measurer.measureText(text).width;
}

/**
 * The room `figure`'s bands take of an image `width` CSS px across, in CSS
 * px, as {top, bottom, lines}: `lines`, the caption's lines, each
 * [left, right], one where both fit side by side, else the left then the
 * right.
 */
export function bandsOf(figure, width) {
  const c = BANDS.caption, cap = figure.caption;
  let lines = [];
  if (cap && (cap.left || cap.right)) {
    const font = fontOf(c), room = width - 2 * c.side;
    const both = cap.left && cap.right ? measure(cap.left, font) + c.gap + measure(cap.right, font) : 0;
    lines = !cap.left || !cap.right || both <= room ? [[cap.left, cap.right]] : [[cap.left, ''], ['', cap.right]];
  }
  const top = figure.title ? BANDS.title.h : 0;
  const bottom = (figure.xLabel ? BANDS.xLabel.h : 0) + (lines.length ? 2 * c.pad + lines.length * c.line : 0);
  return { top, bottom, lines };
}

/**
 * Draws `figure`'s bands on `ctx`, whose units are the figure's CSS px: an
 * image `width` across, its plot `plotHeight` high under the title, `area`
 * the plot area's {x, w} across it, which the x label is centred on, in
 * the colours of `look` (plot.js `theme`). Text that runs past the image
 * is cut at its edge.
 */
export function drawFigure(ctx, figure, { width, plotHeight, area, look }) {
  const { top, lines } = bandsOf(figure, width);
  ctx.save();
  ctx.beginPath();
  ctx.rect(0, 0, width, top + plotHeight + bandsOf(figure, width).bottom);
  ctx.clip();
  ctx.textBaseline = 'alphabetic';
  if (figure.title) {
    ctx.font = fontOf(BANDS.title);
    ctx.fillStyle = look.text;
    ctx.textAlign = 'center';
    ctx.fillText(figure.title, width / 2, BANDS.title.base);
  }
  let y = top + plotHeight;
  if (figure.xLabel) {
    ctx.font = fontOf(BANDS.xLabel);
    ctx.fillStyle = look.axisText;
    ctx.textAlign = 'center';
    ctx.fillText(figure.xLabel, area.x + area.w / 2, y + BANDS.xLabel.base);
    y += BANDS.xLabel.h;
  }
  if (lines.length) {
    const c = BANDS.caption;
    ctx.fillStyle = look.frame;
    ctx.fillRect(0, y, width, 1);
    ctx.font = fontOf(c);
    ctx.fillStyle = look.axisText;
    lines.forEach(([left, right], i) => {
      const base = y + c.pad + i * c.line + c.base;
      ctx.textAlign = 'left';
      if (left) ctx.fillText(left, c.side, base);
      ctx.textAlign = 'right';
      if (right) ctx.fillText(right, width - c.side, base);
    });
  }
  ctx.restore();
}

/** Whether CSS colour `c`, as computed, shows at all. */
const shows = (c) => !!c && c !== 'transparent' && !/^rgba\(.*,\s*0\)$/.test(c) && !/\/\s*0\)$/.test(c);

/**
 * Draws element `el` and what is in it on `ctx`, whose units are the
 * page's client px, as the page shows it: each element's background and
 * border, with its rounded corners, and its text in its font and colour,
 * cut at the box of an element that hides its overflow. What is not shown,
 * as an element of no display, hidden or wholly transparent, is left out;
 * shadows and filters are.
 */
export function paintElement(ctx, el, alpha = 1) {
  const s = getComputedStyle(el);
  if (s.display === 'none' || s.visibility === 'hidden') return;
  const a = alpha * parseFloat(s.opacity || '1');
  if (a <= 0) return;
  const r = el.getBoundingClientRect();
  ctx.save();
  ctx.globalAlpha = a;
  const radius = Math.min(parseFloat(s.borderTopLeftRadius) || 0, r.width / 2, r.height / 2);
  if (shows(s.backgroundColor)) {
    ctx.fillStyle = s.backgroundColor;
    ctx.beginPath();
    ctx.roundRect(r.left, r.top, r.width, r.height, radius);
    ctx.fill();
  }
  const bw = parseFloat(s.borderTopWidth) || 0;
  if (bw > 0 && s.borderTopStyle !== 'none' && shows(s.borderTopColor)) {
    ctx.strokeStyle = s.borderTopColor;
    ctx.lineWidth = bw;
    ctx.beginPath();
    ctx.roundRect(r.left + bw / 2, r.top + bw / 2, r.width - bw, r.height - bw, Math.max(0, radius - bw / 2));
    ctx.stroke();
  }
  if (s.overflowX !== 'visible' || s.overflowY !== 'visible') {
    ctx.beginPath();
    ctx.rect(r.left, r.top, r.width, r.height);
    ctx.clip();
  }
  for (const n of el.childNodes) {
    if (n.nodeType === Node.ELEMENT_NODE) paintElement(ctx, n, a);
    else if (n.nodeType === Node.TEXT_NODE && n.textContent.trim()) {
      const range = document.createRange();
      range.selectNodeContents(n);
      const box = range.getBoundingClientRect();
      ctx.font = `${s.fontStyle} ${s.fontWeight} ${s.fontSize} ${s.fontFamily}`;
      ctx.fillStyle = s.color;
      ctx.textAlign = 'left';
      ctx.textBaseline = 'alphabetic';
      const m = ctx.measureText(n.textContent.trim());
      const base = box.top + (box.height - (m.fontBoundingBoxAscent + m.fontBoundingBoxDescent)) / 2 + m.fontBoundingBoxAscent;
      ctx.fillText(n.textContent.replace(/\s+/g, ' ').trim(), box.left, base);
    }
  }
  ctx.restore();
}

/** Whether the browser draws a 2D canvas `w` by `h` pixels whole: it holds a pixel at its far corner. */
export function canvasFits(w, h) {
  const c = document.createElement('canvas');
  c.width = w;
  c.height = h;
  if (c.width !== w || c.height !== h) return false;
  const ctx = c.getContext('2d');
  if (!ctx) return false;
  ctx.fillRect(w - 1, h - 1, 1, 1);
  const whole = ctx.getImageData(w - 1, h - 1, 1, 1).data[3] !== 0;
  c.width = c.height = 0;
  return whole;
}

/** The device line of a caption: one or two devices named, more counted. */
export const devicesText = (names) => {
  const d = [...new Set(names.filter(Boolean))];
  return d.length > 2 ? `${d.length} devices` : d.join(' · ');
};

/**
 * The time line of a caption for [t0, t1], a time plot's stretch on the
 * timescale `epoch`: a Unix time as the date, the times of day and the UTC
 * offset, 2026-11-10 01:51:30 – 01:51:40 UTC−05:00, the second date only
 * where the stretch runs into another day; any other in its own seconds.
 */
export function stampText(t0, t1, epoch) {
  if (epoch !== 'unix') return `${t0.toFixed(3)} – ${t1.toFixed(3)} s`;
  const two = (n) => String(n).padStart(2, '0');
  const day = (d) => `${d.getFullYear()}-${two(d.getMonth() + 1)}-${two(d.getDate())}`;
  const time = (d) => `${two(d.getHours())}:${two(d.getMinutes())}:${two(d.getSeconds())}`;
  const a = new Date(Math.round(t0 * 1000)), b = new Date(Math.round(t1 * 1000));
  const off = -a.getTimezoneOffset(), sign = off < 0 ? '−' : '+';
  const zone = `UTC${sign}${two(Math.floor(Math.abs(off) / 60))}:${two(Math.abs(off) % 60)}`;
  return `${day(a)} ${time(a)} – ${day(b) === day(a) ? '' : `${day(b)} `}${time(b)} ${zone}`;
}

/** A file name of `text`: what a file system takes, with no character it refuses. */
export const fileSafe = (text) => text.replace(/[\\/:*?"<>|]+/g, '_').slice(0, 120);
