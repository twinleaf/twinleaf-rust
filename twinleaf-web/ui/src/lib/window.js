// A time plot's window, the seconds it shows, as its slider sets it: on a log
// scale from the shortest preset to the longest, settling into a preset's
// detent near it, and the exact value written beside it.
import { SPANS } from './constants.js';

/**
 * The narrowest a time plot's window goes, and the widest while live until
 * the hub says how long its levels of decimation keep, in seconds. Live, a
 * plot then goes as wide as the coarsest level keeps. Paused, it reads the
 * hub's history, and goes as wide as all of it.
 */
export const WINDOW_LIMITS = [0.002, 120];

const SHORTEST = SPANS[0], LONGEST = SPANS.at(-1);

/** How near a preset, in px along the slider, a drag settles into it. */
export const DETENT_PX = 7;

/** Where window `s` sits along the slider, 0 at the shortest preset and 1 at the longest. */
export function positionOf(s) {
  return Math.min(1, Math.max(0, Math.log(s / SHORTEST) / Math.log(LONGEST / SHORTEST)));
}

/**
 * The window at fraction `p` along a slider `width` px wide: the preset
 * within DETENT_PX of it, else the scale's value there to two figures.
 */
export function windowFrom(p, width) {
  p = Math.min(1, Math.max(0, p));
  const preset = SPANS.find((s) => Math.abs(p - positionOf(s)) * width < DETENT_PX);
  return preset ?? +(SHORTEST * (LONGEST / SHORTEST) ** p).toPrecision(2);
}

/** The next preset past window `s`, longer for `dir` 1 and shorter for -1, or the last there is. */
export function nextPreset(s, dir) {
  const past = dir > 0 ? SPANS.filter((x) => x > s * 1.0001) : SPANS.filter((x) => x < s / 1.0001).reverse();
  return past[0] ?? (dir > 0 ? LONGEST : SHORTEST);
}

/** A window as the slider's field writes it, in seconds: 30, 7.4, 0.25. */
export function windowText(s) {
  if (s >= 10) return s.toFixed(0);
  if (s >= 1) return String(+s.toFixed(1));
  return String(+s.toPrecision(2));
}

/** The window typed `text` asks for, from the narrowest to `widest`, or null when it is not one. */
export function windowTyped(text, widest = WINDOW_LIMITS[1]) {
  const n = parseFloat(String(text).replace(/s$/i, ''));
  return n > 0 ? Math.min(widest, Math.max(WINDOW_LIMITS[0], n)) : null;
}
