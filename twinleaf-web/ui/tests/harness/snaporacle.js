// The rule a cursor dragged with Shift held snaps by, stated on its own,
// the long way, for the tests to hold snap.js to.
//
// A target is {id, along, across, line}: its place in CSS px along the
// cursor's direction and, for a point, across it (null for a line alone),
// and whether it catches as a line. The pointer is {along, across}. A
// point qualifies at a jump (along) of 6 px or less and a sideways distance
// (across) of 100 px or less; of those the least cost wins, hypot(jump,
// 0.5 * sideways), the target held from the move before at 0.7 of its
// cost. With no point qualifying, the line of least jump of 6 px or less
// catches. At one cost or one jump, the first in order.

/** The rule's numbers: the farthest jump, the farthest sideways, the weight of sideways in a point's cost, and the share of its cost the held target is taken at. */
export const RULE = Object.freeze({ jump: 6, sideways: 100, weight: 0.5, hold: 0.7 });

/** Target `t`'s jump and sideways distance from `pointer`, sideways null for a line alone. */
export function reach(t, pointer) {
  const point = t.across !== null && t.across !== undefined;
  return { jump: Math.abs(t.along - pointer.along), side: point ? Math.abs(t.across - pointer.across) : null };
}

/** Whether target `t` qualifies as a point with the pointer at `pointer`. */
export function qualifies(t, pointer) {
  const { jump, side } = reach(t, pointer);
  return side !== null && jump <= RULE.jump && side <= RULE.sideways;
}

/** The cost of point `t` with the pointer at `pointer`, `held` the id of the target held from the move before. */
export function cost(t, pointer, held = null) {
  const { jump, side } = reach(t, pointer);
  return Math.hypot(jump, RULE.weight * side) * (held !== null && t.id === held ? RULE.hold : 1);
}

/**
 * What the rule picks of `targets` with the pointer at `pointer` and
 * `held` held: {target, tier, close}, the target or null, 'point', 'line'
 * or null, and `close` how near the pick came to going another way, the
 * least of each target's distance from a threshold and the gap from the
 * pick's cost or jump to that of the next target of another id and, where
 * targets carry the value `v` they put the cursor at, another value.
 */
export function oracleSnap(targets, pointer, held = null) {
  const points = targets.filter((t) => qualifies(t, pointer)).map((t) => ({ t, by: cost(t, pointer, held) }));
  const lines = targets.filter((t) => t.line && reach(t, pointer).jump <= RULE.jump).map((t) => ({ t, by: reach(t, pointer).jump }));
  const pool = (points.length ? points : lines).sort((a, b) => a.by - b.by);
  const next = pool.find((e) => e.t.id !== pool[0].t.id && (e.t.v === undefined || e.t.v !== pool[0].t.v));
  let close = next ? next.by - pool[0].by : Infinity;
  for (const t of targets) {
    const { jump, side } = reach(t, pointer);
    close = Math.min(close, Math.abs(jump - RULE.jump), side === null ? Infinity : Math.abs(side - RULE.sideways));
  }
  return { target: pool[0]?.t ?? null, tier: points.length ? 'point' : lines.length ? 'line' : null, close };
}
