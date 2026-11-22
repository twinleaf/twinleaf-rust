// The tiling layout of a tab: a binary split tree of its plots.
//
//   leaf  {type: 'leaf', plotId}
//   split {type: 'split', dir, ratio, a, b}
//
// dir 'row' puts a and b side by side; 'col' stacks a above b. ratio is a's
// share, to RATIO_STEP. Trees carry no ids: two that read the same are
// `equal`, and an edit returns a new tree, or, for setRatio and taking
// plots out, the same one when it changes nothing.

/** The step every split's ratio is a whole number of. */
export const RATIO_STEP = 1e-4;
/** `r` as a split's ratio: to RATIO_STEP, inside (0, 1). */
export const ratio = (r) => Math.min(1 - RATIO_STEP, Math.max(RATIO_STEP, Math.round(r / RATIO_STEP) * RATIO_STEP));

export const leaf = (plotId) => ({ type: 'leaf', plotId });
export const split = (dir, a, b, r = 0.5) => ({ type: 'split', dir, ratio: ratio(r), a, b });

/** Whether trees `a` and `b` read the same: the same plots, splits and ratios. */
export function equal(a, b) {
  if (a === b) return true;
  if (!a || !b || a.type !== b.type) return false;
  if (a.type === 'leaf') return a.plotId === b.plotId;
  return a.dir === b.dir && a.ratio === b.ratio && equal(a.a, b.a) && equal(a.b, b.b);
}

/**
 * The tree saved or imported JSON describes, in the form every edit makes,
 * or null when it is not one: leaves of string plot ids, splits of 'row' or
 * 'col' with a ratio.
 */
export function fromJson(node) {
  if (node?.type === 'leaf') return typeof node.plotId === 'string' ? leaf(node.plotId) : null;
  if (node?.type !== 'split' || (node.dir !== 'row' && node.dir !== 'col') || !Number.isFinite(node.ratio)) return null;
  const a = fromJson(node.a), b = fromJson(node.b);
  return a && b ? split(node.dir, a, b, node.ratio) : null;
}

export function plotIds(node) {
  if (!node) return [];
  return node.type === 'leaf' ? [node.plotId] : [...plotIds(node.a), ...plotIds(node.b)];
}

/**
 * The tree of only the leaves whose plot `keep` keeps, as they are arranged.
 * A split that loses one side gives the other its place.
 */
function keepOnly(node, keep) {
  if (!node) return null;
  if (node.type === 'leaf') return keep(node.plotId) ? node : null;
  const a = keepOnly(node.a, keep), b = keepOnly(node.b, keep);
  if (!a) return b;
  if (!b) return a;
  return a === node.a && b === node.b ? node : { ...node, a, b };
}

/** The tree without the leaf showing `plotId`; its sibling takes the split's place. */
export const removePlot = (node, plotId) => keepOnly(node, (id) => id !== plotId);

function mapLeaf(node, plotId, fn) {
  if (node.type === 'leaf') return node.plotId === plotId ? fn(node) : node;
  return { ...node, a: mapLeaf(node.a, plotId, fn), b: mapLeaf(node.b, plotId, fn) };
}

/** Puts `plotId` beside `targetId`, on `edge`: left, right, top or bottom. */
export function dock(root, plotId, targetId, edge) {
  if (plotId === targetId) return root;
  const without = removePlot(root, plotId);
  if (!without) return leaf(plotId);
  return mapLeaf(without, targetId, (target) => {
    const moved = leaf(plotId);
    const dir = edge === 'left' || edge === 'right' ? 'row' : 'col';
    const first = edge === 'left' || edge === 'top';
    return split(dir, first ? moved : target, first ? target : moved);
  });
}

/** The tree with plots `a` and `b` in each other's place, every split as it was; the same tree when they are one. */
export function swap(node, a, b) {
  if (!node || a === b) return node;
  if (node.type === 'leaf') return node.plotId === a ? leaf(b) : node.plotId === b ? leaf(a) : node;
  const x = swap(node.a, a, b), y = swap(node.b, a, b);
  return x === node.a && y === node.b ? node : { ...node, a: x, b: y };
}

/** Splits the leaf showing `targetId`, putting `plotId` after it. */
export function splitBeside(root, targetId, plotId, dir) {
  if (!root) return leaf(plotId);
  return mapLeaf(root, targetId, (target) => split(dir, target, leaf(plotId)));
}

/** Adds `plotId` beside the whole layout. */
export function append(root, plotId, dir = 'col') {
  if (!root) return leaf(plotId);
  const count = plotIds(root).length;
  return split(dir, root, leaf(plotId), count / (count + 1));
}

/** Nodes in a chain of equal shares along `dir`. */
function chain(nodes, dir) {
  if (nodes.length === 1) return nodes[0];
  return split(dir, nodes[0], chain(nodes.slice(1), dir), 1 / nodes.length);
}

export const presets = {
  rows: (ids) => chain(ids.map(leaf), 'col'),
  columns: (ids) => chain(ids.map(leaf), 'row'),
  grid: (ids) => {
    const cols = Math.ceil(Math.sqrt(ids.length));
    const rows = [];
    for (let i = 0; i < ids.length; i += cols) rows.push(chain(ids.slice(i, i + cols).map(leaf), 'row'));
    return chain(rows, 'col');
  },
  /** One large plot on the left, the rest stacked on the right. */
  focus: (ids) => (ids.length < 2 ? leaf(ids[0]) : split('row', leaf(ids[0]), chain(ids.slice(1).map(leaf), 'col'), 0.6)),
};

/** The gap between two tiles, in px; Tiles.svelte sets the page's `--gutter` from it. */
export const GUTTER = 7;

/**
 * The box {x, y, w, h} of each plot of `node` laid out in `box`, by plot id,
 * with `gap` between the two sides of every split: a split gives side a its
 * ratio of what the gap leaves, and side b the rest. Every coordinate is
 * linear in the box and the gap, so the page draws a tile at
 * `rects(node, unit, 0)` of its box plus `rects(node, zero, 1)` gaps.
 */
export function rects(node, box, gap = GUTTER, out = new Map()) {
  if (!node) return out;
  if (node.type === 'leaf') return out.set(node.plotId, box);
  const [a, b] = sides(node, box, gap);
  rects(node.a, a, gap, out);
  return rects(node.b, b, gap, out);
}

/** The boxes of split `node`'s two sides in `box`, `gap` between them. */
function sides(node, { x, y, w, h }, gap) {
  if (node.dir === 'row') {
    const wa = (w - gap) * node.ratio;
    return [{ x, y, w: wa, h }, { x: x + wa + gap, y, w: w - gap - wa, h }];
  }
  const ha = (h - gap) * node.ratio;
  return [{ x, y, w, h: ha }, { x, y: y + ha + gap, w, h: h - gap - ha }];
}

/**
 * The gap of every split of `node` laid out in `box`, as {path, dir, ratio,
 * x, y, w, h, split}: its box, and `split` the split's own. `path` names the
 * split by the sides taken from the root to it, as 'ab'.
 */
export function gutters(node, box, gap = GUTTER, path = '', out = []) {
  if (!node || node.type === 'leaf') return out;
  const [a, b] = sides(node, box, gap);
  const g = node.dir === 'row' ? { x: a.x + a.w, y: box.y, w: gap, h: box.h } : { x: box.x, y: a.y + a.h, w: box.w, h: gap };
  out.push({ path, dir: node.dir, ratio: node.ratio, ...g, split: box });
  gutters(node.a, a, gap, path + 'a', out);
  return gutters(node.b, b, gap, path + 'b', out);
}

/** The tree with the split at `path` (as `gutters` names it) at ratio `r`; the same tree when it is there already. */
export function setRatio(node, path, r) {
  if (!node || node.type === 'leaf') return node;
  if (!path) return node.ratio === ratio(r) ? node : { ...node, ratio: ratio(r) };
  const side = path[0];
  const next = setRatio(node[side], path.slice(1), r);
  return next === node[side] ? node : { ...node, [side]: next };
}

/** The tree with only the leaves showing `ids`, a Set, kept as they are arranged. */
export const restrict = (node, ids) => keepOnly(node, (id) => ids.has(id));

/** The tree without the leaves showing `ids`, a Set. */
export const removeAll = (node, ids) => keepOnly(node, (id) => !ids.has(id));

/** The split that puts `node` on `edge` of `other`, with `share` of the space. */
function beside(node, other, edge, share) {
  const dir = edge === 'left' || edge === 'right' ? 'row' : 'col';
  const first = edge === 'left' || edge === 'top';
  return split(dir, first ? node : other, first ? other : node, first ? share : 1 - share);
}

/**
 * Puts `node`, a subtree, beside the leaf showing `targetId`, on `edge`,
 * with `share` of the leaf's space.
 */
export function dockNode(root, node, targetId, edge, share = 0.5) {
  if (!root) return node;
  return mapLeaf(root, targetId, (target) => beside(node, target, edge, share));
}

/** Puts `node`, a subtree, along `edge` of the whole layout, with `share` of it. */
export function dockOuter(root, node, edge, share = 0.5) {
  return root ? beside(node, root, edge, share) : node;
}
