// The page's pointer drags over the tiles: moving plots, dropping signals on
// a plot's edge, and the selection box. `Drags` holds what the overlay
// draws while one goes on (DragOverlay.svelte), and edits the workspace.
//
// The compass: drop cells over the plot under the pointer, and for a moved
// plot, cells along the layout's outer edge. A cell shows only where a drop
// could go, and the one under the pointer is the target, named beside it.

import { dock, dockNode, dockOuter, leaf, plotIds, presets, removeAll, restrict, swap } from './layout.js';
import { newPlot } from './tabs.js';
import { DRAG_START } from './constants.js';

/** The smallest plot a split may leave, in px. */
const MIN_PLOT = { w: 160, h: 100 };
/** A compass cell's size, the distance from the middle to a side cell, and an outer cell's inset, in px. */
const CELL = 34, STEP = 42, OUTER_INSET = 24;
const SIDES = { left: [-1, 0], right: [1, 0], top: [0, -1], bottom: [0, 1] };
/**
 * What is moved keeps its size: docked beside a plot, it takes as much of
 * that plot's width or height as it had, within SHARE.
 */
const SHARE = [0.2, 0.8];
/** The share of the layout a signal dropped on an outer cell opens its plot with. */
const OUTER_SHARE = 1 / 3;
const LANDED_MS = 700;

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
  /** The selection box, {x, y, w, h} in client px. */
  marquee = $state(null);
  /** Plots that just landed, flashed so the eye finds them. */
  landed = $state(null);
  /** Where the last signal drag ended, {plotId, edge}, read as its drop lands. */
  traceDrop = null;
  traceDragging = false;

  /** `ws` is the Workspace; `changed()` runs when the selection changes. */
  constructor(ws, changed) {
    this.ws = ws;
    this.changed = changed;
  }

  flash(ids) {
    this.landed = new Set(ids);
    setTimeout(() => (this.landed = null), LANDED_MS);
  }

  /**
   * A drag that starts on empty background draws a box and selects every
   * plot it touches. Shift adds to the selection. A click there clears it
   * and the plot clicked last.
   */
  startMarquee(e) {
    if (e.button !== 0 || e.target !== e.currentTarget) return;
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
   * While it is held, the tab shows the layout a drop would leave. The
   * target is read against the plots as they were laid out when it was
   * picked up, so the plots moving under the pointer do not move it. A
   * selected plot carries the whole selection, which docks beside another
   * plot or moves to another tab, and swaps with nothing. Dropped on an
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
    let shown = null; // the target the tab shows
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
        middle: group.size === 1 && 'Swap places',
        side: (edge) => `Dock ${edge}`,
        tiles: lifted.tiles,
        outer: (edge) => `New ${isColumn(edge) ? 'column' : 'row'} at ${edge}`,
      });
      this.compass = cells;
      if (!hit) return null;
      return hit.outer ? { outer: true, edge: hit.edge, label: hit.label } : { plotId: hit.plotId, edge: hit.edge, label: hit.label };
    };
    const show = (target) => {
      const same = (a, b) => a?.plotId === b?.plotId && a?.edge === b?.edge && a?.tabId === b?.tabId && a?.outer === b?.outer;
      if (same(target, shown)) return;
      shown = target;
      const { original, tiles, size } = lifted;
      const across = (edge) => (isColumn(edge) ? 'w' : 'h');
      const rest = removeAll(original, group);
      const order = plotIds(restrict(original, group));
      const moved = !target?.edge || group.size === 1 ? restrict(original, group) : isColumn(target.edge) ? presets.rows(order) : presets.columns(order);
      if (!target || target.tabId) ws.tab.layout = original;
      else if (target.outer) {
        const total = across(target.edge) === 'w' ? tiles.width : tiles.height;
        ws.tab.layout = dockOuter(rest, moved, target.edge, clampShare(size[across(target.edge)] / total));
      } else if (target.edge === 'center') ws.tab.layout = swap(original, plotId, target.plotId);
      else {
        const r = lifted.rects.get(target.plotId);
        const room = across(target.edge) === 'w' ? r.width : r.height;
        ws.tab.layout = dockNode(rest, moved, target.plotId, target.edge, clampShare(size[across(target.edge)] / room));
      }
    };
    follow(
      (ev) => {
        if (!lifted) {
          if (Math.hypot(ev.clientX - x0, ev.clientY - y0) < DRAG_START) return;
          const leaves = [...document.querySelectorAll('.leaf[data-plot]')];
          const rects = new Map(leaves.map((el) => [el.dataset.plot, el.getBoundingClientRect()]));
          const own = [...group].map((id) => rects.get(id)).filter(Boolean);
          // One plot keeps its size; a group that fits a slot keeps the size of its largest plot.
          const size =
            group.size > 1
              ? { w: Math.max(...own.map((r) => r.width)), h: Math.max(...own.map((r) => r.height)) }
              : { w: own[0]?.width ?? 0, h: own[0]?.height ?? 0 };
          lifted = { original: ws.tab.layout, rects, size, tiles: document.querySelector('.tiles').getBoundingClientRect() };
          ws.maximized = null;
        }
        const target = targetAt(ev.clientX, ev.clientY);
        this.moving = { plotId, group, x: ev.clientX, y: ev.clientY, target };
        show(target);
      },
      (ev) => {
        this.compass = null;
        if (!lifted) return;
        // The layout shown is the one kept, but for a move to another tab, and
        // a cancelled drag puts everything back.
        if (ev.type === 'pointercancel') ws.tab.layout = lifted.original;
        else if (shown?.tabId) {
          ws.tab.layout = lifted.original;
          for (const id of group) ws.movePlotToTab(id, shown.tabId);
          ws.selected.clear();
        } else if (shown) this.flash(group);
        this.moving = null;
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
    // Over a plot's header the signal joins its chips, and shows no compass.
    const move = (ev) => {
      const [x, y] = [ev.clientX, ev.clientY];
      const under = document.elementsFromPoint(x, y).find((e) => !e.closest('#dnd-action-dragged-el, .compass'));
      const el = under?.closest('[data-plot]');
      const plotId = el?.dataset.plot;
      const tiles = document.querySelector('.tiles')?.getBoundingClientRect();
      const onTiles = tiles && x >= tiles.left && x < tiles.right && y >= tiles.top && y < tiles.bottom;
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
      this.traceDragging = false;
    });
  }

  /**
   * Opens a plot with `key` where the last signal drag ended, beside a
   * plot's edge or along the layout's, if it ended on one, and says whether
   * it did. The layout changes after the caller's drop handler returns,
   * since the change replaces the plot that is handling the drop.
   */
  dockTrace(key) {
    if (!this.traceDrop) return false;
    const { plotId, edge } = this.traceDrop;
    this.traceDrop = null;
    const ws = this.ws;
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
