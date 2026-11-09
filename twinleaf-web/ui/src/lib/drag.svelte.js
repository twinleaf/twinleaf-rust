// The page's pointer drags over the tiles: moving plots, dropping signals on
// a plot's edge, and the selection box. `Drags` holds what the overlay
// draws while one goes on (DragOverlay.svelte), and edits the workspace.
//
// The compass: drop cells over the plot under the pointer, and cells along
// the layout's outer edge, as moves.js offers them. A cell shows only where
// a drop could go, and the one under the pointer is the target, named
// beside it.

import { dock, dockOuter, holderDir, leaf, rects } from './layout.js';
import { OUTER_SHARE, cellAt, moveCells, signalCells } from './moves.js';
import { chip, newPlot } from './tabs.js';
import { DRAG_START } from './constants.js';

const LANDED_MS = 700;
/** The empty background a box selection starts on, besides what listens for it: a dock, a panel's body, a list's own space below its items. */
const EMPTY = '.dock, .dock-side, .dock-body, .palette, [data-slot=scroll-area-viewport]';

/**
 * Runs `move` on every pointer move until the pointer is let go, then `up`
 * with the last event. Returns a function that stops following, without `up`.
 */
function follow(move, up) {
  const stop = () => {
    window.removeEventListener('pointermove', move);
    window.removeEventListener('pointerup', done);
    window.removeEventListener('pointercancel', done);
  };
  const done = (ev) => {
    stop();
    up(ev);
  };
  window.addEventListener('pointermove', move);
  window.addEventListener('pointerup', done);
  window.addEventListener('pointercancel', done);
  return stop;
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
   * and the plot clicked last, unless it is in what keeps the selection (the
   * Signals list); Escape clears them anywhere, putting the box away. Empty background is the element listening,
   * or one of EMPTY: a dock below its panels, a panel's body, or a list
   * below its last item.
   */
  startMarquee(e) {
    if (e.button !== 0 || (e.target !== e.currentTarget && !e.target.matches?.(EMPTY))) return;
    const ws = this.ws;
    const base = e.shiftKey ? [...ws.selected] : [];
    const [x0, y0] = [e.clientX, e.clientY];
    let drawn = false;
    // Escape puts the box away and clears the selection, Shift or not.
    const key = (ev) => {
      if (ev.key !== 'Escape') return;
      ev.preventDefault();
      ev.stopPropagation();
      stop();
      window.removeEventListener('keydown', key, true);
      this.marquee = null;
      ws.selected.clear();
      ws.focusedId = null;
      this.changed();
    };
    window.addEventListener('keydown', key, true);
    const stop = follow(
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
        window.removeEventListener('keydown', key, true);
        // A click that draws no box clears the selection, but in what keeps it, as the Signals list.
        if (!drawn && !e.shiftKey && !e.target.closest?.('[data-keeps-selection]')) {
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
   * While it is held, the layout stays as it is, the compass shows the
   * cells moves.js offers over the plot under the pointer and along the
   * layout's edge, and a box shows where the plot would land; the drop makes
   * the layout that cell gives, as one step. A selected plot carries the
   * whole selection, which docks beside another plot or moves to another
   * tab. Escape, or a tab opened or a layout changed while it is held, as
   * by a key, puts it down where it was; Escape also clears the selection.
   */
  startMove(plotId, e) {
    if (e.button !== 0) return;
    e.stopPropagation();
    const ws = this.ws;
    const [x0, y0] = [e.clientX, e.clientY];
    const group = new Set(ws.selected.has(plotId) && ws.selected.size > 1 ? ws.selected : [plotId]);
    let lifted = null; // {tabId, tree, cells, shown, remembers, maximized} once picked up
    let target = null; // the cell or tab the pointer is over: {cell, label} or {tabId}
    const targetAt = (x, y) => {
      const under = document.elementsFromPoint(x, y).find((el) => !el.closest('.move-ghost, .compass'));
      const tabEl = under?.closest('[data-tab]');
      if (tabEl) {
        this.compass = null;
        return tabEl.dataset.tab === ws.activeId ? null : { tabId: tabEl.dataset.tab };
      }
      // The plot under the pointer, as the layout lays it out.
      let over = null;
      for (const [id, r] of lifted.shown) if (!group.has(id) && x >= r.x && x < r.x + r.w && y >= r.y && y < r.y + r.h) over = id;
      const cells = lifted.cells.filter((c) => c.outer || c.plotId === over);
      const hit = cellAt(cells, x, y);
      this.compass = cells.map((c) => ({ ...c, active: c === hit }));
      return hit && { cell: hit, label: hit.label };
    };
    const stop = follow(
      (ev) => {
        if (!lifted) {
          if (Math.hypot(ev.clientX - x0, ev.clientY - y0) < DRAG_START) return;
          const r = document.querySelector('.tile-box').getBoundingClientRect();
          const box = { x: r.left, y: r.top, w: r.width, h: r.height };
          const tree = ws.tab.layout, shown = rects(tree, box);
          // A plot remembers the width it has beside another, or the height
          // it has above or below one, as a share of the layout's box, kept
          // once it lands (`remembers`). A group remembers nothing.
          const plot = ws.plots[plotId], at = shown.get(plotId);
          const dir = group.size === 1 && plot && at ? holderDir(tree, plotId) : null;
          const remembers = dir === 'row' ? { ...plot.dockSize, w: at.w / box.w } : dir === 'col' ? { ...plot.dockSize, h: at.h / box.h } : null;
          const size = group.size === 1 ? (remembers ?? plot?.dockSize ?? {}) : {};
          lifted = { tabId: ws.activeId, tree, cells: moveCells(tree, box, group, size), shown, remembers, maximized: ws.maximized };
          ws.maximized = null;
        }
        // A tab opened or a layout changed under the move, as by a key, ends it.
        if (ws.activeId !== lifted.tabId || ws.tab.layout !== lifted.tree) return cancel();
        target = targetAt(ev.clientX, ev.clientY);
        this.moving = { plotId, group, x: ev.clientX, y: ev.clientY, target };
        this.preview = target?.cell ? { ...target.cell.preview, outer: target.cell.outer } : null;
      },
      (ev) => end(ev.type === 'pointerup'),
    );
    /** Ends the move with nothing put down. */
    const cancel = () => {
      stop();
      end(false);
    };
    const key = (ev) => {
      if (ev.key !== 'Escape') return;
      ev.preventDefault();
      ev.stopPropagation();
      cancel();
      ws.selected.clear();
      ws.focusedId = null;
      this.changed();
    };
    window.addEventListener('keydown', key, true);
    const end = (drop) => {
      window.removeEventListener('keydown', key, true);
      this.compass = null;
      this.preview = null;
      this.moving = null;
      if (!lifted) return;
      const lands = drop && target && ws.activeId === lifted.tabId && ws.tab.layout === lifted.tree;
      // What is put down nowhere leaves the tab as it was, its plot maximized again.
      if (!lands) {
        if (ws.activeId === lifted.tabId && ws.maximized === null && ws.plots[lifted.maximized]) ws.maximized = lifted.maximized;
        return;
      }
      // The drop is one step: where the plots land, and the size the plot
      // moved remembers.
      const label = group.size > 1 ? `Move ${group.size} plots` : 'Move plot';
      this.group(label, () => {
        if (lifted.remembers) ws.setPlot(plotId, { dockSize: lifted.remembers });
        if (target.tabId) {
          for (const id of group) ws.movePlotToTab(id, target.tabId);
          ws.selected.clear();
        } else ws.setLayout(target.cell.next);
      });
      if (!target.tabId) this.flash(group);
    };
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
    // The cells over each plot, and along the edge, as the layout lays them out when the drag begins.
    const tiles = document.querySelector('.tile-box')?.getBoundingClientRect();
    const box = tiles && { x: tiles.left, y: tiles.top, w: tiles.width, h: tiles.height };
    const tree = this.ws.layout, cellsOver = new Map();
    const cellsFor = (plotId) => {
      if (!cellsOver.has(plotId)) cellsOver.set(plotId, signalCells(tree, box, plotId));
      return cellsOver.get(plotId);
    };
    // Over a plot's header the signal joins its chips, and shows no compass.
    const move = (ev) => {
      const [x, y] = [ev.clientX, ev.clientY];
      const under = document.elementsFromPoint(x, y).find((e) => !e.closest('#dnd-action-dragged-el, .compass'));
      const el = under?.closest('[data-plot]');
      const plotId = el?.dataset.plot;
      const area = document.querySelector('.tiles')?.getBoundingClientRect();
      const onTiles = area && x >= area.left && x < area.right && y >= area.top && y < area.bottom;
      // Beside an independent plot's edge, the signal joins that side's axes.
      const axis = plotId && under.closest('.plot-body') ? this.axisDropAt?.(plotId, x, y) : null;
      if (axis) {
        this.compass = null;
        this.drop = { plotId, edge: 'axis', side: axis.side };
        this.preview = { ...axis.rect, label: `${axis.side === 'left' ? 'Left' : 'Right'} axis` };
        return;
      }
      this.preview = null;
      if (!tree || !box || !onTiles || (plotId && !under.closest('.plot-body'))) {
        this.compass = null;
        this.drop = plotId ? { plotId, edge: 'add' } : onTiles && !this.ws.layout ? { outer: true } : null;
        return;
      }
      const cells = cellsFor(plotId ?? null), hit = cellAt(cells, x, y);
      this.compass = cells.map((c) => ({ ...c, active: c === hit }));
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
        ws.placePlot(p, to.plotId ? dock(ws.layout, p.id, to.plotId, to.edge) : dockOuter(ws.layout, leaf(p.id), to.edge, OUTER_SHARE));
        return p.id;
      }
      const p = ws.plots[to.plotId];
      if (!p) return null;
      const seen = new Set();
      const items = (to.items ?? [...p.items, item]).map(own).filter((i) => !seen.has(i.key) && seen.add(i.key));
      ws.setPlot(p.id, to.edge === 'axis' ? { items, axisSides: { ...p.axisSides, [item.key]: to.side } } : { items });
      return p.id;
    };
    const id = this.group('Move signal', () => {
      const id = place();
      if (id && from && from.id !== id) {
        const left = from.items.filter((i) => i.id !== item.id);
        if (left.length) ws.setPlot(from.id, { items: left });
        else this.closePlot(from.id);
      }
      return id;
    });
    if (id && opens) this.flash([id]);
    return id;
  }
}
