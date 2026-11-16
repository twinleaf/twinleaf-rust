// Where a cursor dragged with Shift held comes to rest, by where the
// pointer is: a target near it, or nowhere, when the cursor follows the
// pointer freely.
//
// What it snaps to, in the cursor's own units, of what the plot shows:
//
//   tick    the axis's ticks the plot draws a grid line or a number at
//   cursor  the other cursor of its pair
//   data    of an X cursor, the features of each signal shown near it: its
//           peaks and troughs, a spectrum's peaks; of a Y cursor, each
//           signal's value at the X cursors, its least and greatest in
//           view, and 0
//
// Every target is in view. A target is a point, with a place on the plot
// across the cursor's direction as well as along it, or a line, with a
// place along it alone. A tick is both: its mark on the axis a point, its
// grid line a line. A signal's point is a point; the other cursor and 0
// are lines. From the pointer, in CSS px as the plot draws them, a
// target's jump is its distance along the cursor's direction and its
// sideways distance the one across it. A point qualifies within SNAP.jump
// and SNAP.sideways, and the qualifying point of least cost wins,
// hypot(jump, SNAP.weight * sideways), the target of the move before taken
// at SNAP.hold of its cost. With no point qualifying, the line of least
// jump within SNAP.jump catches. At one cost or one jump, the first.
//
// A signal is its points as the plot draws them, {key, n, x(i), y(i)}, in
// order of x. A feature is a point higher (or lower) than both its
// neighbours that stands out from the points about it by `stand`: from the
// lowest point (or highest) on each side before a higher one (or lower), or
// the view's end, as a peak's prominence is measured. A spectrum's stand
// out by STAND.spectrumDb, in amplitude; a time plot's by STAND.timeShare
// of the y range its axis shows.

/**
 * How a cursor dragged with Shift held picks its target, in CSS px: the
 * farthest `jump` along its direction, the farthest a point is `sideways`,
 * the `weight` sideways distance has in a point's cost, and the share of
 * its cost the target of the move before is taken at, `hold`.
 */
export const SNAP = Object.freeze({ jump: 6, sideways: 100, weight: 0.5, hold: 0.7 });

/** How far a feature stands out from the points about it: on a spectrum in dB of amplitude, on a time plot as a share of the y range shown. */
export const STAND = { spectrumDb: 6, timeShare: 0.1 };

/** The first index of signal `s` whose x is at or past `x`. */
function lowerBound(s, x) {
  let lo = 0, hi = s.n;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (s.x(mid) < x) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

/** The first index of signal `s` whose x is past `x`. */
function upperBound(s, x) {
  let lo = 0, hi = s.n;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (s.x(mid) <= x) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

/** The index of signal `s`'s point nearest `x`, or -1 where `x` lies outside its points. */
function nearestAt(s, x) {
  if (s.n === 0 || x < s.x(0) || x > s.x(s.n - 1)) return -1;
  const k = Math.min(lowerBound(s, x), s.n - 1);
  return k > 0 && x - s.x(k - 1) < s.x(k) - x ? k - 1 : k;
}

/**
 * The indices of signal `s`'s features with x in `near`, [lo, hi], among its
 * points in `view`, [lo, hi]: its peaks, and with `troughs` its troughs, that
 * stand out by `stand` in `level(y)`.
 */
export function features(s, view, near, { stand, troughs = true, level = (y) => y }) {
  // The points in view, first to last; a feature has a neighbour each side among them.
  const first = lowerBound(s, view[0]), last = upperBound(s, view[1]) - 1;
  const lo = Math.max(first + 1, lowerBound(s, near[0])), hi = Math.min(last - 1, upperBound(s, near[1]) - 1);
  const out = [];
  for (let i = lo; i <= hi; i++) {
    const y = level(s.y(i)), before = level(s.y(i - 1)), after = level(s.y(i + 1));
    for (const sign of troughs ? [1, -1] : [1]) {
      // A peak, or with sign -1 a trough, the first point of a flat top.
      if (!(sign * y > sign * before && sign * y >= sign * after)) continue;
      if (standsOut(s, i, first, last, sign, stand, level)) out.push(i);
    }
  }
  return out;
}

/** Whether point `i` of `s` stands `stand` above (sign 1) or below (-1) the points each side of it in [first, last], before one past it. */
function standsOut(s, i, first, last, sign, stand, level) {
  const top = sign * level(s.y(i));
  for (const dir of [-1, 1]) {
    let reached = false;
    for (let j = i + dir; j >= first && j <= last && !reached; j += dir) {
      const v = sign * level(s.y(j));
      if (v > top) break;
      reached = top - v >= stand;
    }
    if (!reached) return false;
  }
  return true;
}

/**
 * The targets a cursor along `axis`, 'x' or 'y', may snap to, as
 * {v, from, id, line, key?, point?}: `v` in the cursor's units, `from`
 * 'tick', 'cursor' or 'data', `id` alike for the target of one place from
 * frame to frame, `line` whether it catches as a line, and of a signal's
 * point its key and {x, y}. A tick and a signal's point are points, a tick
 * at its mark on the axis; the other cursor and 0 are lines alone.
 *
 * `view` is what the axis shows, [lo, hi]; `ticks` its ticks; `other` the
 * other cursor of the pair, or null. `signals` are the signals the cursor
 * reads, each with what its features stand out by, `stand`, and with
 * `level` and `troughs` as `features` takes them. An X cursor looks for
 * features with x in `near`; a Y cursor reads each signal at the values of
 * `xs`, the X cursors, and over `xView`, what the x axis shows.
 */
export function snapCandidates({ axis, view, ticks = [], other = null, signals = [], near = view, xs = [], xView = null }) {
  const inView = (v) => Number.isFinite(v) && v >= view[0] && v <= view[1];
  const out = [];
  for (const v of ticks) if (inView(v)) out.push({ v, from: 'tick', id: `tick ${v}`, line: true });
  if (other !== null && inView(other)) out.push({ v: other, from: 'cursor', id: 'cursor', line: true });
  const point = (s, i, v) => ({ v, from: 'data', id: `${s.key} ${s.x(i)}`, line: false, key: s.key, point: { x: s.x(i), y: s.y(i) } });
  if (axis === 'x') {
    for (const s of signals) for (const i of features(s, view, near, s)) out.push(point(s, i, s.x(i)));
    return out.filter((c) => inView(c.v));
  }
  for (const s of signals) {
    for (const x of xs) {
      const i = x === null || !xView || x < xView[0] || x > xView[1] ? -1 : nearestAt(s, x);
      if (i >= 0) out.push(point(s, i, s.y(i)));
    }
    if (!xView) continue;
    let least = -1, most = -1;
    for (let i = lowerBound(s, xView[0]); i < s.n && s.x(i) <= xView[1]; i++) {
      if (least < 0 || s.y(i) < s.y(least)) least = i;
      if (most < 0 || s.y(i) > s.y(most)) most = i;
    }
    if (least >= 0) out.push(point(s, least, s.y(least)), point(s, most, s.y(most)));
  }
  out.push({ v: 0, from: 'data', id: 'zero', line: true });
  return out.filter((c) => inView(c.v));
}

/**
 * Of `targets`, the one a cursor dragged with Shift held snaps to with the
 * pointer at `pointer`, {along, across}, in px along the cursor's direction
 * and across it, by the rule above; or null for none, when the cursor
 * follows the pointer. Each target is placed at `along` and a point at
 * `across` too, null for a line alone; a target catches as a line by its
 * `line`. `held` is the id of the target of the move before, or null.
 */
export function pickSnap(targets, pointer, held = null) {
  let best = null, least = Infinity;
  for (const t of targets) {
    if (t.across === null || t.across === undefined) continue;
    const jump = Math.abs(t.along - pointer.along), side = Math.abs(t.across - pointer.across);
    if (!(jump <= SNAP.jump && side <= SNAP.sideways)) continue;
    const cost = Math.hypot(jump, SNAP.weight * side) * (held !== null && t.id === held ? SNAP.hold : 1);
    if (cost < least) [best, least] = [t, cost];
  }
  if (best) return best;
  for (const t of targets) {
    const jump = Math.abs(t.along - pointer.along);
    if (t.line && jump <= SNAP.jump && jump < least) [best, least] = [t, jump];
  }
  return best;
}

// The peaks an X cursor goes to by a command (commands.js): each signal's
// features that are peaks, by what its own stand out by, as above, the
// signals' merged in order of x, each x once.

/** The x of the peaks of `signals` among their points in `view`, [lo, hi], with x in `near`, in order, each once. */
export function peaks(signals, view, near = view) {
  const xs = signals.flatMap((s) => features(s, view, near, { ...s, troughs: false }).map((i) => s.x(i)));
  return [...new Set(xs)].sort((a, b) => a - b);
}

/** Of `xs`, peaks in order, the one nearest `at` along the axis `to(x)` places them on, the lower at one distance, or null for none. */
export function nearestPeak(xs, at, to = (v) => v) {
  let best = null;
  for (const x of xs) if (best === null || Math.abs(to(x) - to(at)) < Math.abs(to(best) - to(at))) best = x;
  return best;
}

/**
 * The x a cursor at `at` steps to at the next peak right (`dir` 1) or left
 * (-1): of `inView`, the peaks of the view in order, the first past `at`;
 * past the last of them, the first past `at` of `held()`, the peaks of all
 * the points the plot holds, in order; or null where there is none.
 */
export function stepPeak(inView, at, dir, held = () => []) {
  const past = (x) => dir * (x - at) > 0;
  const next = (xs) => (dir > 0 ? xs.find(past) : xs.findLast(past)) ?? null;
  return next(inView) ?? next(held());
}
