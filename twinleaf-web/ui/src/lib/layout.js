// The tiling layout of a tab: a binary split tree of its plots.
//
//   leaf  {type: 'leaf', id, plotId}
//   split {type: 'split', id, dir, ratio, a, b}
//
// dir 'row' puts a and b side by side; 'col' stacks a above b. ratio is a's
// share. Every edit returns a new tree.

let n = 0;
const uid = () => `n${++n}-${Math.random().toString(36).slice(2, 6)}`;

export const leaf = (plotId) => ({ type: 'leaf', id: uid(), plotId });
export const split = (dir, a, b, ratio = 0.5) => ({ type: 'split', id: uid(), dir, ratio, a, b });

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

/** The direction of the split holding the leaf showing `targetId`: null for a leaf alone, undefined when no leaf shows it. */
export function holderDir(node, targetId, dir = null) {
  if (node.type === 'leaf') return node.plotId === targetId ? dir : undefined;
  return holderDir(node.a, targetId, node.dir) ?? holderDir(node.b, targetId, node.dir);
}

/**
 * The edge of the leaf showing `targetId` that a plot taking its place
 * goes on, as a list's reorder places an item: along the split holding the
 * leaf, before it when `before`, after it otherwise. A leaf alone splits
 * side by side.
 */
export function placeEdge(root, targetId, before) {
  const row = (holderDir(root, targetId) ?? 'row') === 'row';
  return row ? (before ? 'left' : 'right') : before ? 'top' : 'bottom';
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

/**
 * The size {width, height} the leaf showing `plotId` has in a `width` by
 * `height` area, by its splits' ratios, or null when no leaf shows it.
 */
export function leafSize(node, plotId, width, height) {
  if (!node) return null;
  if (node.type === 'leaf') return node.plotId === plotId ? { width, height } : null;
  const row = node.dir === 'row';
  const [wa, ha] = row ? [width * node.ratio, height] : [width, height * node.ratio];
  const [wb, hb] = row ? [width - wa, height] : [width, height - ha];
  return leafSize(node.a, plotId, wa, ha) ?? leafSize(node.b, plotId, wb, hb);
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
