// A time plot's window, the seconds it shows, as the window field reads and
// writes it: typed as "90", "5m", "1.5 min", "2h" or "500ms", and shown back
// in the unit that reads shortest.

/**
 * The narrowest a time plot's window goes, and the widest while live until
 * the hub says how long its levels of decimation keep, in seconds. Live, a
 * plot then goes as wide as the coarsest level keeps. Paused, it reads the
 * hub's history, and goes as wide as all of it.
 */
export const WINDOW_LIMITS = [0.002, 120];

/** The units a span is written in, longest first, each with its seconds. */
const UNITS = [
  ['h', 3600],
  ['min', 60],
  ['s', 1],
  ['ms', 0.001],
];

/** What a typed unit means, in seconds. */
const TYPED_UNITS = { ms: 0.001, s: 1, sec: 1, m: 60, min: 60, h: 3600, hr: 3600 };

/** The significant figures a span is written to. */
const FIGURES = 3;

/**
 * Span `s` as the window field shows it, [number, unit], to three figures:
 * in the longest unit that writes it exactly ("1.5 min" for 90 s, "2 h"),
 * else the longest in which it is at least 1 ("7.41 s", "18.3 min"). A unit
 * but hours writes under 1000 of it.
 */
export function windowParts(s) {
  let longest = null;
  for (const [unit, u] of UNITS) {
    const n = +(s / u).toPrecision(FIGURES);
    if (!(n >= 1 && (n < 1000 || unit === 'h'))) continue;
    if (Math.abs(n * u - s) <= s * 1e-9) return [String(n), unit];
    longest ??= [String(n), unit];
  }
  return longest ?? [String(+(s / 0.001).toPrecision(FIGURES)), 'ms'];
}

/** Span `s` as the window's readouts write it: "30 s", "1.5 min", "2 h", "250 ms". */
export function windowText(s) {
  return windowParts(s).join(' ');
}

/**
 * The seconds `text` is as a span, typed as "90", "90s", "5 m", "1.5 min",
 * "2H" or "500ms", a number alone in `unit`, one of the units a span is
 * shown in; or null when it is none or not over 0.
 */
export function parseSpan(text, unit = 's') {
  const m = /^\s*(\d+\.?\d*|\.\d+)\s*([a-z]*)\s*$/i.exec(String(text));
  const u = m && TYPED_UNITS[(m[2] || unit).toLowerCase()];
  const s = u ? +m[1] * u : 0;
  return s > 0 && isFinite(s) ? s : null;
}

/** How far past a limit a typed window may go and be taken as the limit: the precision a span is shown to. */
const SLACK = 1 + 0.5 * 10 ** (1 - FIGURES);

/**
 * The window typed `text` asks for, a number alone in `unit`: {s}, or
 * {refused} when it is 'none' (not a span) or 'long' (more than `widest`
 * beyond the precision it is shown to). One under the narrowest window is
 * the narrowest, and one just over `widest` is `widest`.
 */
export function windowTyped(text, widest = WINDOW_LIMITS[1], unit = 's') {
  const s = parseSpan(text, unit);
  if (s === null) return { refused: 'none' };
  if (s > widest * SLACK) return { refused: 'long' };
  return { s: Math.min(widest, Math.max(WINDOW_LIMITS[0], s)) };
}
