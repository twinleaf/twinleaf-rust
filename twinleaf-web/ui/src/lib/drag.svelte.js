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
 * share of the whole layout. With none, it takes half of the plot it docks by. Either way,
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
  /** Where the last signal drag was let go, as `drop` names it, read as its drop lands. */
  traceDrop = null;
  traceDragging = false;
  /** Closes plot `id`: set by the page, which also lets go of what the plot holds. */
  closePlot = (id) => this.ws.closePlot(id);

  /**
   * `ws` is the Workspace, `changed()` runs when the selection changes, and
   * `history` is the page's History, which takes each drag as one step.
   */
  constructor(ws, changed, history = null) {
    this.ws = ws;
    this.changed = changed;
    this.history = history;
  }

  /** Runs `fn` as one undo step named `label`. */
  group(label, fn) {
    return this.history ? this.history.group(label, fn) : fn();
  }

  #landedTimer;
  flash(ids) {
    this.landed = new Set(ids);
    clearTimeout(this.#landedTimer);
    this.#landedTimer = setTimeout(() => (this.landed = null), LANDED_MS);
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
    let lifted = null; // {original, rects, tiles, size, remembers} once picked up
    let shown = null; // the target the preview shows
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
    /**
     * The size plot `id` will have once what is moved is out of the layout,
     * which a dock beside it shares: taking a plot out of a split gives its
     * neighbour its room, so measuring the neighbour as it is now would give
     * what is moved more than it had, a little more at every move.
     */
    const roomOf = (id) => leafSize(removeAll(lifted.original, group), id, lifted.tiles.width, lifted.tiles.height) ?? lifted.rects.get(id);
    /** Where `target` would put what is moved, as a box over the plots, or null. */
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
          // it has above or below one, as a share of the layout, kept once it
          // lands (`remembers`). A group remembers nothing and takes half.
          const plot = ws.plots[plotId], r = rects.get(plotId);
          const dir = group.size === 1 && plot && r ? holderDir(ws.tab.layout, plotId) : null;
          const remembers = dir === 'row' ? { ...plot.dockSize, w: r.width / tiles.width } : dir === 'col' ? { ...plot.dockSize, h: r.height / tiles.height } : null;
          const size = group.size === 1 ? (remembers ?? plot?.dockSize ?? {}) : {};
          lifted = { original: ws.tab.layout, rects, size, remembers, tiles };
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
        if (!lifted || !shown || ev.type === 'pointercancel') return;
        // The drop is one step: where the plots land, and the size the plot
        // moved remembers.
        const label = group.size > 1 ? `Move ${group.size} plots` : 'Move plot';
        this.group(label, () => {
          if (lifted.remembers && ws.plots[plotId]) ws.plots[plotId].dockSize = lifted.remembers;
          if (shown.tabId) {
            for (const id of group) ws.movePlotToTab(id, shown.tabId);
            ws.selected.clear();
          } else ws.tab.layout = layoutFor(shown);
        });
        if (!shown.tabId) this.flash(group);
      },
    );
  }

  /**
   * Follows a signal being dragged. Anywhere on a plot adds it there, a side
   * cell of the plot's compass opens a new plot on that side with it, and an
   * outer cell opens one along that edge of the layout. On an empty tab,
   * anywhere on the tiles opens the tab's first plot. The cell is read at
   * pointerup, before the drag library finalizes, and the drop lands
   * through `landSignal`.
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
      this.traceDrop = this.drop;
      this.drop = null;
      this.compass = null;
      this.preview = null;
      this.traceDragging = false;
    });
  }

  /**
   * Lands a dragged signal, `item` {id, key}, from the finalize of a drop
   * zone: `into` is the plot whose chips it was dropped among, with `items`
   * those chips as dropped, or null when the drop is in no plot's chips.
   * Where the pointer was let go decides where it goes, as the drag library
   * can name the zone the pointer was over a moment before; the zone's
   * chips give its place only when they are of that plot. Returns the id of
   * the plot it went to, or null.
   */
  landSignal(item, into = null, items = null) {
    const drop = this.traceDrop;
    this.traceDrop = null;
    if (!drop) return null;
    if (drop.edge !== 'add') return this.moveSignal(item, drop);
    return this.moveSignal(item, { plotId: drop.plotId, items: into === drop.plotId ? items : null });
  }

  /**
   * Moves signal `item` {id, key} as one step, and returns the id of the
   * plot it went to, or null. `to` is where: {plotId, edge} beside a plot's
   * edge, or {outer, edge} along the layout's, opens a plot there with it;
   * {plotId, edge: 'axis', side} joins an independent plot on that side's
   * axes; and {plotId, items} joins that plot, at its place in `items`, its
   * chips with it among them, or after the others without them. A chip that
   * came from a plot leaves it, and a plot whose last signal it was closes;
   * one emptied with × stays, to be filled again.
   */
  moveSignal(item, to) {
    const ws = this.ws;
    const from = Object.values(ws.plots).find((p) => p.items.some((i) => i.id === item.id));
    // A chip from the signal list has the list's id until it gets its own.
    const own = (i) => (i.id.startsWith('pal:') ? chip(i.key) : i);
    const opens = to.outer || (to.edge && to.edge !== 'axis');
    /** The plot it goes to, the layout changed to hold it. */
    const place = () => {
      if (opens) {
        const p = newPlot(ws.plots[to.plotId]?.kind ?? 'time', 'overlay', [item.key]);
        ws.tab.plots[p.id] = p;
        ws.tab.layout = to.plotId ? dock(ws.layout, p.id, to.plotId, to.edge) : dockOuter(ws.layout, leaf(p.id), to.edge, OUTER_SHARE);
        ws.focusedId = p.id;
        return p.id;
      }
      const p = ws.plots[to.plotId];
      if (!p) return null;
      const seen = new Set();
      p.items = (to.items ?? [...p.items, item]).map(own).filter((i) => !seen.has(i.key) && seen.add(i.key));
      if (to.edge === 'axis') p.axisSides = { ...p.axisSides, [item.key]: to.side };
      return p.id;
    };
    const id = this.group('Move signal', () => {
      const id = place();
      if (id && from && from.id !== id) {
        from.items = from.items.filter((i) => i.id !== item.id);
        if (!from.items.length) this.closePlot(from.id);
      }
      return id;
    });
    if (id && opens) this.flash([id]);
    return id;
  }
}
