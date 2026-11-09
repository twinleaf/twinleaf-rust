// The page's pointer drags over the tiles: moving plots, dropping signals on
// a plot's edge, and the selection box. `Drags` holds what the overlay
// draws while one goes on (DragOverlay.svelte), and edits the workspace.
//
// The compass: drop cells over the plot under the pointer, and for a moved
// plot, cells along the layout's outer edge. A cell shows only where a drop
// could go, and the one under the pointer is the target, named beside it.

import { dock, dockNode, dockOuter, holderDir, leaf, leafSize, placeEdge, plotIds, presets, removeAll, restrict } from './layout.js';
import { chip, newPlot } from './tabs.js';
import { DRAG_START } from './constants.js';

/** The smallest plot a split may leave, in px. */
const MIN_PLOT = { w: 160, h: 100 };
/** A compass cell's size, the distance from the middle to a side cell, and an outer cell's inset, in px. */
const CELL = 34, STEP = 42, OUTER_INSET = 24;
const SIDES = { left: [-1, 0], right: [1, 0], top: [0, -1], bottom: [0, 1] };
/**
 * What is moved takes the size it last had that way: docked beside a plot,
 * the width it had when it last stood beside one, and docked above or below,
 * the height it had when it last stood above or below one, each kept as a
 * share of the whole layout (as VS Code keeps its panel's size per
 * position). With none, it takes half of the plot it docks by. Either way,
 * within SHARE of that plot.
 */
const SHARE = [0.2, 0.8];
/** The share of the layout a signal dropped on an outer cell opens its plot with. */
const OUTER_SHARE = 1 / 3;
const LANDED_MS = 700;
/** The empty background a box selection starts on, besides what listens for it: a dock, a panel's body, a list's own space below its items. */
const EMPTY = '.dock, .dock-side, .dock-body, .palette, [data-slot=scroll-area-viewport]';

const isColumn = (edge) => edge === 'left' || edge === 'right';
const roomFor = (r, edge) => (isColumn(edge) ? r.width / 2 >= MIN_PLOT.w : r.height / 2 >= MIN_PLOT.h);
const clampShare = (f) => Math.min(SHARE[1], Math.max(SHARE[0], f));

/**
 * The cells for a pointer at (x, y): over the plot `plotId` laid out at `r`,
 * if any, with a middle cell when `middle` names it, and along the layout
 * `tiles` when `outer` names its cells. Returns the cells and the one under
 * the pointer.
 */
function compassAt(x, y, { plotId, r, middle, side, tiles, outer }) {
  const cells = [];
  if (plotId) {
    const cx = r.left + r.width / 2, cy = r.top + r.height / 2;
    // A plot's cells are named under the compass, clear of every cell.
    const lx = cx, ly = cy + STEP + CELL;
    if (middle) cells.push({ edge: 'center', plotId, x: cx, y: cy, lx, ly, label: middle });
    for (const [edge, [dx, dy]] of Object.entries(SIDES)) {
      if (roomFor(r, edge)) cells.push({ edge, plotId, x: cx + dx * STEP, y: cy + dy * STEP, lx, ly, label: side(edge) });
    }
  }
  if (outer) {
    const mx = tiles.left + tiles.width / 2, my = tiles.top + tiles.height / 2;
    const at = { left: [tiles.left + OUTER_INSET, my], right: [tiles.right - OUTER_INSET, my], top: [mx, tiles.top + OUTER_INSET], bottom: [mx, tiles.bottom - OUTER_INSET] };
    // An edge cell is named a little inward of it.
    const inward = { left: [64, 0], right: [-64, 0], top: [0, 40], bottom: [0, -40] };
    for (const [edge, [ex, ey]] of Object.entries(at)) cells.push({ edge, outer: true, x: ex, y: ey, lx: ex + inward[edge][0], ly: ey + inward[edge][1], label: outer(edge) });
  }
  const hit = cells.find((c) => Math.abs(x - c.x) <= CELL / 2 && Math.abs(y - c.y) <= CELL / 2) ?? null;
  for (const c of cells) c.active = c === hit;
  return { cells, hit };
}

/** Runs `move` on every pointer move until the pointer is let go, then `up` with the last event. */
function follow(move, up) {
  const done = (ev) => {
    window.removeEventListener('pointermove', move);
    window.removeEventListener('pointerup', done);
    window.removeEventListener('pointercancel', done);
    up(ev);
  };
  window.addEventListener('pointermove', move);
  window.addEventListener('pointerup', done);
  window.addEventListener('pointercancel', done);
}

export class Drags {
  /** The plot being moved: {plotId, group, x, y, target}. */
  moving = $state(null);
  /**
   * Where a signal being dragged would go: {plotId, edge}, edge `add` for
   * into the plot, or {outer, edge} for a new plot along the layout's edge,
   * with no edge on an empty tab.
   */
  drop = $state(null);
  /** The compass cells shown. */
  compass = $state(null);
  /** Where a plot being moved would land, {x, y, w, h, outer} in client px, drawn over the plots. */
  preview = $state(null);
  /** The selection box, {x, y, w, h} in client px. */
  marquee = $state(null);
  /** Plots that just landed, flashed so the eye finds them. */
  landed = $state(null);
  /**
   * Which side's axes a signal dropped at (x, y) on plot `plotId` would
   * join, as {side, rect}, or null: set by the page, which has the plots.
   */
  axisDropAt = null;
  /** Where the last signal drag ended, {plotId, edge}, read as its drop lands. */
  traceDrop = null;
  traceDragging = false;

  /**
   * `ws` is the Workspace, `changed()` runs when the selection changes, and
   * `history` is the page's History, which takes each drag as one step.
   */
  constructor(ws, changed, history = null) {
    this.ws = ws;
    this.changed = changed;
    this.history = history;
  }

  flash(ids) {
    this.landed = new Set(ids);
    setTimeout(() => (this.landed = null), LANDED_MS);
  }

  /**
   * A drag that starts on empty background draws a box and selects every
   * plot it touches. Shift adds to the selection. A click there clears it
   * and the plot clicked last. Empty background is the element listening,
   * or one of EMPTY: a dock below its panels, a panel's body, or a list
   * below its last item.
   */
  startMarquee(e) {
    if (e.button !== 0 || (e.target !== e.currentTarget && !e.target.matches?.(EMPTY))) return;
    const ws = this.ws;
    const base = e.shiftKey ? [...ws.selected] : [];
    const [x0, y0] = [e.clientX, e.clientY];
    let drawn = false;
    follow(
      (ev) => {
        if (!drawn && Math.hypot(ev.clientX - x0, ev.clientY - y0) < DRAG_START) return;
        drawn = true;
        const m = { x: Math.min(x0, ev.clientX), y: Math.min(y0, ev.clientY), w: Math.abs(ev.clientX - x0), h: Math.abs(ev.clientY - y0) };
        this.marquee = m;
        const hit = new Set(base);
        for (const el of document.querySelectorAll('.leaf[data-plot]')) {
          const r = el.getBoundingClientRect();
          if (r.left < m.x + m.w && r.right > m.x && r.top < m.y + m.h && r.bottom > m.y) hit.add(el.dataset.plot);
        }
        ws.selected.clear();
        for (const id of hit) ws.selected.add(id);
      },
      () => {
        if (!drawn && !e.shiftKey) {
          ws.selected.clear();
          ws.focusedId = null;
        }
        this.marquee = null;
        this.changed();
      },
    );
  }

  /**
   * Picks plot `plotId` up by its header or grip once the pointer moves a
   * few pixels, so a click or double-click there stays one.
   *
   * While it is held, the layout stays as it is, and a box over it shows
   * where the plot would land; the layout changes once, on the drop. A
   * selected plot carries the whole selection, which docks beside another
   * plot or moves to another tab. Dropped on a plot's middle, what is moved
   * takes its place, as in a list: before it when it came from after it in
   * reading order, after it otherwise, along the split that holds it, and
   * that plot moves along beside it. Dropped on an
   * outer cell, what is moved becomes a row or column across the layout. A
   * group that lands in a column stacks, and one in a row lines up, in the
   * order it read.
   */
  startMove(plotId, e) {
    if (e.button !== 0) return;
    e.stopPropagation();
    const ws = this.ws;
    const [x0, y0] = [e.clientX, e.clientY];
    const group = new Set(ws.selected.has(plotId) && ws.selected.size > 1 ? ws.selected : [plotId]);
    let lifted = null; // {original, rects, tiles, size} once picked up
    let shown = null; // the target the preview shows
    let wasSize; // the plot's remembered size before it was picked up, put back when it lands nowhere
    const targetAt = (x, y) => {
      const under = document.elementsFromPoint(x, y).find((el) => !el.closest('.move-ghost, .compass'));
      const tabEl = under?.closest('[data-tab]');
      if (tabEl) {
        this.compass = null;
        return tabEl.dataset.tab === ws.activeId ? null : { tabId: tabEl.dataset.tab };
      }
      // The plot under the pointer, as laid out when the move began.
      let over = null;
      for (const [id, r] of lifted.rects) if (!group.has(id) && x >= r.left && x < r.right && y >= r.top && y < r.bottom) over = { id, r };
      const { cells, hit } = compassAt(x, y, {
        plotId: over?.id,
        r: over?.r,
        middle: 'Take its place',
        side: (edge) => `Dock ${edge}`,
        tiles: lifted.tiles,
        outer: (edge) => `New ${isColumn(edge) ? 'column' : 'row'} at ${edge}`,
      });
      this.compass = cells;
      if (!hit) return null;
      if (hit.outer) return { outer: true, edge: hit.edge, label: hit.label };
      // Taking a plot's place is docking on the edge a list's reorder would
      // pick, along the split that held it before anything moved.
      if (hit.edge === 'center') {
        const order = plotIds(lifted.original);
        const first = order.find((id) => group.has(id));
        const before = order.indexOf(first) > order.indexOf(hit.plotId);
        return { plotId: hit.plotId, edge: placeEdge(lifted.original, hit.plotId, before), label: hit.label };
      }
      return { plotId: hit.plotId, edge: hit.edge, label: hit.label };
    };
    const across = (edge) => (isColumn(edge) ? 'width' : 'height');
    const shareOf = (edge, room) => {
      const dim = across(edge), remembered = lifted.size[dim === 'width' ? 'w' : 'h'];
      return clampShare(remembered === undefined ? 0.5 : (remembered * lifted.tiles[dim]) / room[dim]);
    };
    /** The part of box `r` along its `edge`, `share` of it deep. */
    const part = (r, edge, share) => {
      const w = isColumn(edge) ? r.width * share : r.width, h = isColumn(edge) ? r.height : r.height * share;
      return { x: edge === 'right' ? r.right - w : r.left, y: edge === 'bottom' ? r.bottom - h : r.top, w, h };
    };
    /** Where `target` would put what is moved, as a box over the plots, or null. */
    /**
     * The size plot `id` will have once what is moved is out of the layout,
     * which a dock beside it shares: taking a plot out of a split gives its
     * neighbour its room, so measuring the neighbour as it is now would give
     * what is moved more than it had, a little more at every move.
     */
    const roomOf = (id) => leafSize(removeAll(lifted.original, group), id, lifted.tiles.width, lifted.tiles.height) ?? lifted.rects.get(id);
    const previewOf = (target) => {
      if (!target || target.tabId) return null;
      if (target.outer) return { ...part(lifted.tiles, target.edge, shareOf(target.edge, lifted.tiles)), outer: true };
      // Drawn over the plot as it is now, at the size the dock will give.
      const r = lifted.rects.get(target.plotId), room = roomOf(target.plotId), dim = across(target.edge);
      return part(r, target.edge, Math.min(0.95, (shareOf(target.edge, room) * room[dim]) / r[dim]));
    };
    /** The layout `target` leaves. */
    const layoutFor = (target) => {
      const { original, tiles } = lifted;
      const rest = removeAll(original, group);
      const order = plotIds(restrict(original, group));
      const moved = group.size === 1 ? restrict(original, group) : isColumn(target.edge) ? presets.rows(order) : presets.columns(order);
      if (target.outer) return dockOuter(rest, moved, target.edge, shareOf(target.edge, tiles));
      return dockNode(rest, moved, target.plotId, target.edge, shareOf(target.edge, roomOf(target.plotId)));
    };
    follow(
      (ev) => {
        if (!lifted) {
          if (Math.hypot(ev.clientX - x0, ev.clientY - y0) < DRAG_START) return;
          const leaves = [...document.querySelectorAll('.leaf[data-plot]')];
          const rects = new Map(leaves.map((el) => [el.dataset.plot, el.getBoundingClientRect()]));
          const tiles = document.querySelector('.tiles').getBoundingClientRect();
          // A plot remembers the width it has beside another, or the height
          // it has above or below one, as a share of the layout. A group
          // remembers nothing and takes half.
          let size = {};
          const plot = ws.plots[plotId], r = rects.get(plotId);
          this.history?.begin(group.size > 1 ? `Move ${group.size} plots` : 'Move plot');
          wasSize = plot?.dockSize;
          if (group.size === 1 && plot && r) {
            const dir = holderDir(ws.tab.layout, plotId);
            if (dir === 'row') plot.dockSize = { ...plot.dockSize, w: r.width / tiles.width };
            if (dir === 'col') plot.dockSize = { ...plot.dockSize, h: r.height / tiles.height };
            size = plot.dockSize ?? {};
          }
          lifted = { original: ws.tab.layout, rects, size, tiles };
          ws.maximized = null;
        }
        shown = targetAt(ev.clientX, ev.clientY);
        this.moving = { plotId, group, x: ev.clientX, y: ev.clientY, target: shown };
        this.preview = previewOf(shown);
      },
      (ev) => {
        this.compass = null;
        this.preview = null;
        this.moving = null;
        if (!lifted) return;
        if (!shown || ev.type === 'pointercancel') {
          if (ws.plots[plotId]) ws.plots[plotId].dockSize = wasSize;
        } else if (shown.tabId) {
          for (const id of group) ws.movePlotToTab(id, shown.tabId);
          ws.selected.clear();
        } else {
          ws.tab.layout = layoutFor(shown);
          this.flash(group);
        }
        this.history?.end();
      },
    );
  }

  /**
   * Follows a signal being dragged. Anywhere on a plot adds it there, a side
   * cell of the plot's compass opens a new plot on that side with it, and an
   * outer cell opens one along that edge of the layout. On an empty tab,
   * anywhere on the tiles opens the tab's first plot. The cell is read at
   * pointerup, before the drag library finalizes, and the drop goes through
   * `dockTrace`: from the plot that takes it in its finalize, or from the
   * signal list when it lands in no zone.
   */
  traceDragStart() {
    if (this.traceDragging) return;
    this.traceDragging = true;
    this.traceDrop = null;
    this.history?.begin('Move signal');
    // Over a plot's header the signal joins its chips, and shows no compass.
    const move = (ev) => {
      const [x, y] = [ev.clientX, ev.clientY];
      const under = document.elementsFromPoint(x, y).find((e) => !e.closest('#dnd-action-dragged-el, .compass'));
      const el = under?.closest('[data-plot]');
      const plotId = el?.dataset.plot;
      const tiles = document.querySelector('.tiles')?.getBoundingClientRect();
      const onTiles = tiles && x >= tiles.left && x < tiles.right && y >= tiles.top && y < tiles.bottom;
      // Beside an independent plot's edge, the signal joins that side's axes.
      const axis = plotId && under.closest('.plot-body') ? this.axisDropAt?.(plotId, x, y) : null;
      if (axis) {
        this.compass = null;
        this.drop = { plotId, edge: 'axis', side: axis.side };
        this.preview = { ...axis.rect, label: `${axis.side === 'left' ? 'Left' : 'Right'} axis` };
        return;
      }
      this.preview = null;
      if (!this.ws.layout || !onTiles || (plotId && !under.closest('.plot-body'))) {
        this.compass = null;
        this.drop = plotId ? { plotId, edge: 'add' } : onTiles && !this.ws.layout ? { outer: true } : null;
        return;
      }
      const { cells, hit } = compassAt(x, y, {
        plotId,
        r: el?.getBoundingClientRect(),
        middle: 'Add to this plot',
        side: (edge) => `New plot ${edge === 'top' ? 'above' : edge === 'bottom' ? 'below' : `on the ${edge}`}`,
        tiles,
        outer: (edge) => `New ${isColumn(edge) ? 'column' : 'row'} at ${edge}`,
      });
      this.compass = cells;
      if (hit?.outer) this.drop = { outer: true, edge: hit.edge };
      else if (plotId) this.drop = hit && hit.edge !== 'center' ? { plotId, edge: hit.edge } : { plotId, edge: 'add' };
      else this.drop = null;
    };
    follow(move, (ev) => {
      if (ev.type === 'pointerup') move(ev);
      else this.drop = null;
      this.traceDrop = this.drop?.edge === 'add' ? null : this.drop;
      this.drop = null;
      this.compass = null;
      this.preview = null;
      this.traceDragging = false;
      // The drop lands after the drag library finalizes and dockTrace's
      // microtask runs, so the step closes a task later, with it inside.
      setTimeout(() => this.history?.end());
    });
  }

  /**
   * Opens a plot with `key` where the last signal drag ended, beside a
   * plot's edge or along the layout's, or gives it an axis on an independent
   * plot's side, if it ended on one. Returns {plotId} of the plot it went
   * to, or false. The layout changes after the caller's drop handler returns,
   * since the change replaces the plot that is handling the drop.
   */
  dockTrace(key) {
    if (!this.traceDrop) return false;
    const { plotId, edge, side } = this.traceDrop;
    this.traceDrop = null;
    const ws = this.ws;
    // Beside an independent plot's edge: the signal joins that plot, its axis on that side.
    if (edge === 'axis') {
      queueMicrotask(() => {
        const p = ws.plots[plotId];
        if (!p) return;
        if (!p.items.some((i) => i.key === key)) p.items.push(chip(key));
        p.axisSides = { ...p.axisSides, [key]: side };
      });
      return { plotId };
    }
    queueMicrotask(() => {
      const p = newPlot(ws.plots[plotId]?.kind ?? 'time', 'overlay', [key]);
      ws.tab.plots[p.id] = p;
      ws.tab.layout = plotId ? dock(ws.layout, p.id, plotId, edge) : dockOuter(ws.layout, leaf(p.id), edge, OUTER_SHARE);
      ws.focusedId = p.id;
      this.flash([p.id]);
    });
    return true;
  }
}
