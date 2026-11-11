// The wheel's zoom of a time plot. Each turn moves a goal along a ladder of
// spans that read well, in the turn's direction, and the span shown eases
// after the goal until it rests on it. How fast the wheel turns picks the
// ladder: a turn on its own steps a fine span (10, 11, 12 s), a few in quick
// succession a middling one (10, 12, 15, 20 s), a spin a clock span (10, 15,
// 20, 30 s). A trackpad's stream of small turns is the same rule: its pixels
// add up to steps, and its rate picks the ladder.
import { WINDOW_LIMITS } from './window.js';

/** The ms in which the ease closes all but 1/e of the way to its goal, along a log scale. */
export const EASE_MS = 70;
/** How near its goal the ease comes, as |ln(span / goal)|, before it rests on it. */
export const REST = 0.004;
/** The px of wheel that move the goal one span along its ladder. */
export const NOTCH_PX = 100;
/** The px a line of wheel is, for a wheel that counts in lines. */
export const LINE_PX = 33;
/** How far back, in ms, the turns are counted that say how fast the wheel turns. */
export const RATE_MS = 280;

const DAY = 86400;
/** Each of `ks` times each power of ten, to three figures, within the narrowest span and a day. */
const decades = (ks) => {
  const out = [];
  for (let e = -3; e <= 4; e++) for (const k of ks) out.push(+(k * 10 ** e).toPrecision(3));
  return out.filter((v) => v >= WINDOW_LIMITS[0] && v <= DAY);
};
const SIXTY = [1, 2, 5, 10, 15, 20, 30, 45];

/** The spans the wheel steps through, ascending, finest first: every span to two figures, a middling ladder, and the clock's. */
export const LADDERS = {
  fine: decades([1, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.8, 2, 2.2, 2.5, 2.8, 3, 3.5, 4, 4.5, 5, 5.5, 6, 7, 8, 9]),
  middle: decades([1, 1.2, 1.5, 2, 2.5, 3, 4, 5, 6, 8]),
  clock: [...decades([1, 2, 5]).filter((v) => v < 1), ...SIXTY, ...SIXTY.map((v) => v * 60), ...[1, 2, 3, 4, 6, 8, 12, 24].map((v) => v * 3600)],
};

/** The ladder for a turn that is the `turns`th within RATE_MS: fine alone, middling for two or three, the clock's for more. */
export function ladderOf(turns) {
  return turns >= 4 ? LADDERS.clock : turns >= 2 ? LADDERS.middle : LADDERS.fine;
}

/** The span `steps` spans of `ladder` from `span`, longer for positive `steps`; past the ladder's end it stays at the last. */
export function stepped(span, steps, ladder) {
  for (; steps > 0; steps--) span = ladder.find((v) => v > span * 1.001) ?? span;
  for (; steps < 0; steps++) span = ladder.findLast((v) => v < span * 0.999) ?? span;
  return span;
}

/** The wheel before any turn: the times of its recent turns, the px turned short of a step, and the way it turned last. */
export const STILL = Object.freeze({ at: [], acc: 0, dir: 0 });

/**
 * A turn of `dy` px, positive out, at page time `now` ms, of `wheel`: the
 * wheel after it, the steps it makes along `ladder`, and whether it turned
 * the other way from the turn before. A turn the other way starts its
 * steps afresh, and so does one after the wheel was still for RATE_MS.
 */
export function turn(wheel, dy, now) {
  const at = [...wheel.at.filter((t) => now - t < RATE_MS), now];
  const dir = Math.sign(dy) || wheel.dir;
  const reversed = wheel.dir !== 0 && dir !== wheel.dir;
  let acc = (reversed || at.length === 1 ? 0 : wheel.acc) + dy;
  const steps = Math.trunc(acc / NOTCH_PX) + 0;
  acc -= steps * NOTCH_PX;
  return { wheel: { at, acc, dir }, steps, ladder: ladderOf(at.length), reversed };
}

/**
 * The ease of a view after turn `turned` (as `turn` gives it) at page time
 * `now`, of a view showing span `shown`, `was` its ease before, null when
 * at rest: {start, t0, goal}. The goal is `turned.steps` along its ladder
 * from the goal `was` eased to, or from `shown` when at rest or the turn
 * went the other way, so a turn the other way answers from what shows;
 * within [`lo`, `hi`], the spans the view can zoom to. The ease starts
 * again from `shown`.
 */
export function aimed(was, shown, now, turned, [lo, hi]) {
  const from = was && !turned.reversed ? was.goal : shown;
  return { start: shown, t0: now, goal: Math.min(hi, Math.max(lo, stepped(from, turned.steps, turned.ladder))) };
}

/**
 * The span `ms` into an ease from span `start` toward `goal`: along a log
 * scale, all but e^(-ms / EASE_MS) of the way, and `goal` itself once
 * within REST of it. An ease turned to a new goal starts again from the
 * span it had reached, so the span shown moves on without a jump.
 */
export function eased(start, goal, ms) {
  const off = Math.log(start / goal) * Math.exp(-Math.max(0, ms) / EASE_MS);
  return Math.abs(off) <= REST ? goal : goal * Math.exp(off);
}
