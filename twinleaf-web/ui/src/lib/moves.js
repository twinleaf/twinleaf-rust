// Where a plot being moved can land, and what each landing makes: the cells
// the compass shows over every other plot and along the layout's edge, each
// with the layout it leaves and the box the moved plots take in it, so what
// the preview shows is what the drop gives.
//
// A cell is offered only where it changes the layout and leaves no plot it
// shrinks smaller than MIN_PLOT. A plot dropped alone on another's middle
// swaps places with it, each taking the other's box. Docked, what is moved
// takes the size it last had
// that way: docked beside a plot, the width it had when it last stood beside
// one, and docked above or below, the height it had when it last stood above
// or below one, each kept as a share of the layout's box (`size`). With
// none, it takes half of what it shares: the plot it docks by, or the whole
// layout along an outer edge. Either way, within SHARE of that, and within
// what leaves both sides of the split at least MIN_PLOT along it.

import { GUTTER, dock, dockNode, dockOuter, equal, leaf, plotIds, presets, rects, removeAll, restrict, swap } from './layout.js';

/** The smallest plot a move may leave, in px. */
export const MIN_PLOT = { w: 160, h: 100 };
/** A compass cell's size, the distance from the middle to a side cell, and an outer cell's inset, in px. */
export const CELL = 34, STEP = 42, OUTER_INSET = 24;
/** The least and most of what it shares a moved plot takes. */
export const SHARE = [0.2, 0.8];
export const SIDES = { left: [-1, 0], right: [1, 0], top: [0, -1], bottom: [0, 1] };

export const isColumn = (edge) => edge === 'left' || edge === 'right';
/** The share of the layout a signal dropped on an outer cell opens its plot with. */
export const OUTER_SHARE = 1 / 3;

/** Where the cells over a plot laid out at `r` go, by edge ('center' its middle), each {x, y, lx, ly}: named under the compass, clear of every cell. */
function plotPlaces(r) {
  const cx = r.x + r.w / 2, cy = r.y + r.h / 2, lx = cx, ly = cy + STEP + CELL;
  const at = { center: [0, 0], ...SIDES };
  return Object.fromEntries(Object.entries(at).map(([edge, [dx, dy]]) => [edge, { x: cx + dx * STEP, y: cy + dy * STEP, lx, ly }]));
}

/** Where the cells along the edge of a layout laid out in `box` go, by edge, each {x, y, lx, ly}: named a little inward. */
function outerPlaces(box) {
  const mx = box.x + box.w / 2, my = box.y + box.h / 2;
  const at = { left: [box.x + OUTER_INSET, my], right: [box.x + box.w - OUTER_INSET, my], top: [mx, box.y + OUTER_INSET], bottom: [mx, box.y + box.h - OUTER_INSET] };
  const inward = { left: [64, 0], right: [-64, 0], top: [0, 40], bottom: [0, -40] };
  return Object.fromEntries(Object.entries(at).map(([edge, [x, y]]) => [edge, { x, y, lx: x + inward[edge][0], ly: y + inward[edge][1] }]));
}

const outerLabel = (edge) => `New ${isColumn(edge) ? 'column' : 'row'} at ${edge}`;

/**
 * Whether layout `next` leaves no plot of `tree` smaller than MIN_PLOT in a
 * dimension it shrank in, and every plot new to it at least MIN_PLOT, both
 * laid out in `box`.
 */
export function fits(tree, next, box, gap = GUTTER) {
  const was = rects(tree, box, gap);
  for (const [id, r] of rects(next, box, gap)) {
    const w = was.get(id) ?? { w: Infinity, h: Infinity };
    if ((r.w < MIN_PLOT.w - 0.5 && r.w < w.w - 0.5) || (r.h < MIN_PLOT.h - 0.5 && r.h < w.h - 0.5)) return false;
  }
  return true;
}

/** The box around boxes `rs`. */
function around(rs) {
  const x = Math.min(...rs.map((r) => r.x)), y = Math.min(...rs.map((r) => r.y));
  return { x, y, w: Math.max(...rs.map((r) => r.x + r.w)) - x, h: Math.max(...rs.map((r) => r.y + r.h)) - y };
}

/**
 * The cells for moving the plots of Set `group` in layout `tree`, laid out
 * in `box` with `gap` between tiles: each {edge, plotId} over a plot (edge
 * 'center' swaps a plot moved alone with it) or {edge, outer} along the layout's edge, with
 * its middle (x, y), where its name goes (lx, ly), its `label`, the layout
 * it makes, `next`, and the box the moved plots take in it, `preview`.
 * `size` {w, h} is what the moved plot remembers, as shares of the box.
 */
export function moveCells(tree, box, group, size = {}, gap = GUTTER) {
  const shown = rects(tree, box, gap);
  const rest = removeAll(tree, group);
  const room = rects(rest, box, gap);
  const order = plotIds(restrict(tree, group));
  // A group lines up across the edge it lands on: stacked beside a plot, in a row above or below one.
  const moved = (edge) => (group.size === 1 ? restrict(tree, group) : isColumn(edge) ? presets.rows(order) : presets.columns(order));
  /**
   * The share of a split `of` px along `edge`'s axis that gives what is
   * moved its remembered size, kept within SHARE and so that both sides of
   * the split keep MIN_PLOT along it; null when the split cannot hold two.
   */
  const share = (edge, of) => {
    const dim = isColumn(edge) ? 'w' : 'h', want = size[dim], room = of[dim] - gap;
    const lo = Math.max(SHARE[0], MIN_PLOT[dim] / room), hi = Math.min(SHARE[1], 1 - MIN_PLOT[dim] / room);
    if (lo > hi) return null;
    return Math.min(hi, Math.max(lo, want === undefined ? 0.5 : (want * box[dim]) / room));
  };
  const cells = [];
  const offer = (cell, next) => {
    if (!next || equal(next, tree) || !fits(tree, next, box, gap)) return;
    const after = rects(next, box, gap);
    cells.push({ ...cell, next, preview: around(order.map((id) => after.get(id))) });
  };
  /** The layout `make(share)` gives, the share for `edge` of `of`; null when there is not room for two plots there. */
  const beside = (edge, of, make) => {
    const f = share(edge, of);
    return f === null ? null : make(f);
  };
  for (const [id, r] of shown) {
    if (group.has(id)) continue;
    const places = plotPlaces(r);
    // A plot dropped on another's middle swaps places with it, each taking
    // the other's size; a group docks only.
    if (group.size === 1) offer({ edge: 'center', plotId: id, ...places.center, label: 'Swap' }, swap(tree, [...group][0], id));
    for (const edge of Object.keys(SIDES)) {
      offer({ edge, plotId: id, ...places[edge], label: `Dock ${edge}` }, beside(edge, room.get(id), (f) => dockNode(rest, moved(edge), id, edge, f)));
    }
  }
  for (const [edge, place] of Object.entries(outerPlaces(box))) {
    offer({ edge, outer: true, ...place, label: outerLabel(edge) }, beside(edge, box, (f) => dockOuter(rest, moved(edge), edge, f)));
  }
  return cells;
}

/** The plot id the cells a signal offers stand a new plot in for. */
const NEW = 'new plot';

/**
 * The cells a signal dragged over plot `plotId` of layout `tree`, laid out
 * in `box`, offers, as `moveCells` gives them, without `next` or
 * `preview`: its middle adds the signal to the plot, a side opens a plot
 * with it on that side, half the plot's room, and an outer cell opens one
 * along that edge of the layout, OUTER_SHARE of it. A side or outer cell is
 * offered only where the new plot leaves every plot at least MIN_PLOT.
 * With no `plotId`, the outer cells alone.
 */
export function signalCells(tree, box, plotId = null, gap = GUTTER) {
  const cells = [];
  const r = plotId && rects(tree, box, gap).get(plotId);
  if (r) {
    const places = plotPlaces(r);
    cells.push({ edge: 'center', plotId, ...places.center, label: 'Add to this plot' });
    for (const edge of Object.keys(SIDES)) {
      if (!fits(tree, dock(tree, NEW, plotId, edge), box, gap)) continue;
      cells.push({ edge, plotId, ...places[edge], label: `New plot ${edge === 'top' ? 'above' : edge === 'bottom' ? 'below' : `on the ${edge}`}` });
    }
  }
  for (const [edge, place] of Object.entries(outerPlaces(box))) {
    if (fits(tree, dockOuter(tree, leaf(NEW), edge, OUTER_SHARE), box, gap)) cells.push({ edge, outer: true, ...place, label: outerLabel(edge) });
  }
  return cells;
}

/** The cell of `cells` under (x, y), or null. */
export const cellAt = (cells, x, y) => cells.find((c) => Math.abs(x - c.x) <= CELL / 2 && Math.abs(y - c.y) <= CELL / 2) ?? null;
