// What a plot needs of its surroundings: a PlotHost. A plot's component
// (PlotCard.svelte) is given a host and the settings of one of the host's
// plots, and reaches everything else through the host, so a plot is drawn
// and edited the same wherever it lives. The page's tiles are the plots of
// the host main.js makes over the page's tabs; a paused copy of one of them
// (`copy`) is the one plot of a host of its own, whose settings, view,
// cursors, colours and undo steps are the copy's alone.
//
// A host is:
// - where the data comes from: `hub`, whose session holds the traces, and
//   the windows of the hub's history a paused plot reads (`readWindows`),
//   which the host asks for as each view moves;
// - the settings document: `plot(id)`, the settings of plot `id`, of a
//   Workspace of its own (`ws`), edited through `edit`, `each` and
//   `setView`, each edit a step of `history`;
// - the view: `view(id)`, plot `id`'s View, and `viewHistory`, the steps
//   Back and Forward go through;
// - where commands land: `commands`, the command table (commands.js) over
//   this host's plots, and `targets(id)`, the plots a command or a gesture
//   made on plot `id` acts on; `cursors`, the one writer of the plots'
//   cursors; `pickers`, what the keys open on each plot;
// - how it looks: `colors`, each signal's colour; `hover`, the time under
//   the pointer; and the surface it is drawn on, `renderer`, a canvas with
//   its place, size and pixel ratio (gl.js), which whoever shows the plots
//   gives with `surface` and draws each frame with `draw`. `picture(id)` is
//   the one way to a plot's pixels;
// - the selection: `selected`, `select`, `point` and `press`;
// - `place`, what a page that lays its plots out in tabs adds, or null:
//   {startMove(id, e), otherTabs(), tabName(), moveToTab(id, tabId),
//   otherPlots(id), moveSignal(id, key, to), traceDragStart(),
//   landSignal(id, item, items)}. The page gives it, with what its
//   commands need of a page, through `serve`; a host never served has no
//   workspace (commands.js `needs`), and its commands that need one do not
//   run and have no rows.

import { CursorEdits } from './cursors.js';
import { ViewHistory } from './viewhistory.svelte.js';
import { Views } from './views.js';
import { Workspace } from './workspace.svelte.js';
import { History } from './history.svelte.js';
import { Favorites } from './favorites.svelte.js';
import { commands as plotCommands } from './commands.js';
import { Renderer, windowFrame } from './gl.js';
import { leaf } from './layout.js';
import { makeTab, uid } from './tabs.js';
import { counted } from './menus.js';
import { toast } from './toast.svelte.js';
import * as clock from './clock.js';

/**
 * The share of the view's span asked for either side of it, so a pan or a
 * zoom out stays within the window read until the next one lands.
 */
const DETAIL_PAD = 1;
/** The points a window read asks for, for each CSS px of the surface's width, at least. */
const DETAIL_POINTS = 2;

export class PlotHost {
  /** Each plot's pickers the keys open, by plot id: `open(action)` opens the one for keymap.js plot action `action` and says whether it had one. */
  pickers = new Map();
  /** The time under the pointer, which every time plot here marks. */
  hover = { t: null };
  /** The Renderer of the canvas the plots are drawn on, or null before `surface` gives one. */
  renderer = null;
  /** What the page adds for plots laid out in its tabs (`serve`), or null. */
  place = null;
  /** The page's services (`serve`), or null. */
  #page = null;
  /** Plot id to what its overview was read for. */
  #overviews = new Map();

  /**
   * `hub` is the Hub; `ws` the Workspace whose plots these are, `history`
   * its History and `views` its Views; `registry` the Set of PlotViews on
   * the surface, which `draw` draws; `colors` the Colors of its signals;
   * `frame` the frame its surface is drawn within (gl.js), the window
   * unless given; `live` whether its plots can follow the newest samples.
   */
  constructor({ hub, ws, history, views, registry = new Set(), colors, frame = windowFrame, live = true }) {
    Object.assign(this, { hub, ws, history, views, registry, colors, frame, live, selection: ws.selection, cursors: new CursorEdits(ws, history) });
    this.viewHistory = new ViewHistory(ws, views, { notice: (text) => toast.warning(text), edit: (label, fn) => history.group(label, fn), cursors: this.cursors });
    views.onChange = (id) => this.#viewChanged(id);
    // Each view reads the oldest time its plot can show from what the hub holds of these.
    views.tracesOf = (id) => this.#tracesOf(this.#anywhere(id) ?? { items: [] });
    this.commands = plotCommands({
      has: (need) => (need === 'live' ? this.live : this.#page !== null),
      targets: (on) => this.targets(on),
      pageTargets: (scope) => this.selection.pageTargets(scope),
      signalTargets: (key) => this.selection.signalTargets(key),
      plotOf: (id) => ({ id, plot: this.plot(id), view: this.view(id), pv: this.pv(id) }),
      last: () => ws.focusedId,
      ws,
      selection: this.selection,
      viewHistory: this.viewHistory,
      cursors: this.cursors,
      toast,
      edit: (ids, what, fn) => this.editEach(ids, what, fn),
      split: (ids, dir, fromKeys) => this.#page.split(ids, dir, fromKeys),
      close: (ids, fromKeys) => this.#page.close(ids, fromKeys),
      newPlot: (keys) => this.#page.newPlot(keys),
      copy: (plots) => this.#page.copy(plots),
      copyCheck: (plots) => this.#page.copyCheck(plots),
      openExport: (plots) => this.#page.openExport(plots),
      picker: (id, action) => !!this.pickers.get(id)?.(action),
    });
  }

  /**
   * Has the page serve this host's plots: `page` is what the commands that
   * need a workspace run through, {split(ids, dir, fromKeys), close(ids,
   * fromKeys), newPlot(keys), copy(plots), copyCheck(plots),
   * openExport(plots)}, with `viewChanged(id)`, told as plot `id`'s view
   * moves, and `place`, as the top of this file lists it.
   */
  serve({ place, ...page }) {
    this.#page = page;
    this.place = place;
  }

  /** The settings of plot `id` of the open tab. */
  plot(id) {
    return this.ws.plots[id];
  }

  /** Plot `id`'s View. */
  view(id) {
    return this.views.of(id);
  }

  /** The PlotView of plot `id` while it is on the surface. */
  pv(id) {
    for (const p of this.registry) if (p.plotId === id) return p;
    return undefined;
  }

  /** The plots a command or a gesture made on plot `id` acts on, by id: every selected plot when it is one of them, else it alone. */
  targets(id) {
    return this.selection.targets(id);
  }

  /** Whether plot `id` is selected. */
  selected(id) {
    return this.ws.selected.has(id);
  }

  /** A click on plot `id`, with Shift when `shift`, the `clicks`th of a run. */
  select(id, shift, clicks) {
    this.selection.click(id, shift, clicks);
  }

  /** Plot `id` is the one pointed at: the plot focus is in, or a press landed on. */
  point(id) {
    this.selection.point(id);
  }

  /** A press on plot `id`'s controls, or a menu opened on it. */
  press(id) {
    this.selection.pressOn(id);
  }

  /** Gives each of plots `ids` the fields `fn(plot)` returns, if any, as one step undo names `what`. */
  editEach(ids, what, fn) {
    // A plot's kind is what one is looking at too: switching it is a view step, which Back crosses.
    this.viewHistory.step(() =>
      this.history.group(ids.length > 1 ? `${what} (${counted(ids.length, 'plot')})` : what, () => {
        for (const i of ids) {
          const patch = fn(this.ws.plots[i]);
          if (patch) this.ws.setPlot(i, patch);
        }
      }),
    );
  }

  /** `editEach` on the plots a command made on plot `id` acts on. */
  each(id, what, fn) {
    this.editEach(this.targets(id), what, fn);
  }

  /** Gives plot `id` the fields of `patch`, as one step undo names `what`. */
  edit(id, what, patch) {
    this.history.group(what, () => this.ws.setPlot(id, patch));
  }

  /** Gives the fields of `patch`, what a view shows (tabs.js VIEW_FIELDS), to each plot of plot `id`'s kind that a command made on it acts on, as one view step. */
  setView(id, patch) {
    const kind = this.ws.plots[id]?.kind;
    this.viewHistory.step(() => this.targets(id).forEach((i) => this.ws.plots[i]?.kind === kind && this.ws.setView(i, patch)));
  }

  /**
   * Has the plots drawn on `canvas`, within `frame` when given (gl.js),
   * until the function it returns is called, which gives the canvas's
   * context back. Whoever gives a frame of another width or pixel ratio
   * has the paused plots' windows read again for it (`rereadWindows`).
   */
  surface(canvas, frame = this.frame) {
    this.frame = frame;
    const renderer = (this.renderer = new Renderer(canvas, frame));
    return () => {
      renderer.dispose();
      if (this.renderer === renderer) this.renderer = null;
    };
  }

  /** Draws every plot on the surface, at page time `perf`: one frame. */
  draw(perf) {
    this.renderer.begin();
    for (const pv of this.registry) pv.draw(perf);
  }

  /**
   * Plot `id`'s picture now, as a new canvas: its traces and, over them,
   * its axes, cursors and readouts, the size of its body in CSS px times
   * the surface's pixel ratio. The surface is drawn for it, as a frame is,
   * and read before the browser takes that frame, since a WebGL canvas
   * keeps no pixels after. What the page lays over the plot, as its legend,
   * is not in it.
   */
  picture(id) {
    const pv = this.pv(id);
    this.draw(clock.now());
    const at = this.renderer.deviceRect(pv.drawnOn), out = document.createElement('canvas');
    out.width = pv.overlay.width;
    out.height = pv.overlay.height;
    const ctx = out.getContext('2d');
    ctx.drawImage(this.renderer.canvas, at.x, at.y, out.width, out.height, 0, 0, out.width, out.height);
    ctx.drawImage(pv.overlay, 0, 0);
    return out;
  }

  /** The plot of `id` in any tab. */
  #anywhere(id) {
    return this.ws.tabs.find((t) => t.plots[id])?.plots[id];
  }

  /** The traces of `plot`'s signals the hub has. */
  #tracesOf(plot) {
    return plot.items.map((i) => this.hub.session.byKey.get(i.key)).filter(Boolean);
  }

  /** The most points a window read asks for: DETAIL_POINTS for each CSS px across the surface, a point for each device pixel at least. */
  #points() {
    const { w, dpr } = this.frame();
    return w * Math.max(DETAIL_POINTS, dpr);
  }

  #viewChanged(id) {
    this.readWindows(id);
    this.holdPaused();
    this.#page?.viewChanged?.(id);
  }

  /** Tells the hub whether a time plot here, in any tab, is paused: it holds its points, where a paused spectrum holds the spectrum it shows, and a closed plot nothing. */
  holdPaused() {
    this.hub.hold(this, this.views.anyPaused((id) => this.#anywhere(id)?.kind === 'time'));
  }

  // Paused, every pan or zoom asks the server for the plot's view at full
  // resolution, read from its raw buffer, which reaches back as far as the
  // history kept. Live, the decimated stream is used.
  // Every move asks, and the session paces the reads (`Session.fetchWindow`).
  /** Reads what paused plot `id` shows of the hub's history, and its whole capture; a plot that follows live holds none. */
  readWindows(id) {
    const v = this.views.get(id);
    if (!v || v.live) return this.dropWindows(id);
    this.#readOverview(id, v);
    const plot = this.#anywhere(id);
    if (!plot || plot.kind !== 'time') return;
    const [x0, x1] = v.range(this.hub.now(clock.now()));
    this.hub.session.fetchWindow(id, 'detail', this.#tracesOf(plot), x0, x1, DETAIL_PAD * (x1 - x0), this.#points());
  }

  /** Reads paused plot `id`'s whole captured history once, again only when its signals or its pause change. */
  #readOverview(id, v) {
    const plot = this.#anywhere(id);
    if (!plot || plot.kind !== 'time') return;
    const traces = this.#tracesOf(plot);
    const oldest = this.hub.session.memory?.oldest ?? {};
    const lo = Math.min(...traces.map((t) => oldest[t.meta.id] ?? Infinity));
    const hi = v.pausedAt;
    if (!isFinite(lo) || hi === null) return;
    const key = `${hi}|${traces.map((t) => t.meta.id).join(',')}`;
    if (this.#overviews.get(id) === key) return;
    this.#overviews.set(id, key);
    this.hub.session.fetchWindow(id, 'overview', traces, lo, hi, 0, this.#points());
  }

  /** Reads every paused plot's windows again, as after the session dropped them all, or for a surface of another width or pixel ratio. */
  rereadWindows() {
    this.#overviews.clear();
    for (const id of this.ws.allPlotIds()) if (this.views.get(id)?.live === false) this.readWindows(id);
  }

  /** Lets go of the windows read for plot `id`, as it closes or follows live. */
  dropWindows(id) {
    this.#overviews.delete(id);
    this.hub.session.clearWindows(id);
  }

  /**
   * A paused copy of plot `id`, as a host of its own whose one plot, `id`
   * of the host returned, has the plot's settings, cursors and colours and
   * shows what it shows now: the stretch of time it is paused at, or shows
   * as this is called, its y ranges and a paused spectrum's spectra. The
   * copy does not follow live. Whatever is done to it changes it alone:
   * neither this host's plots, their views and undo steps, nor the page's
   * colours. `dispose` ends it.
   */
  copy(id) {
    const from = this.plot(id), perf = clock.now();
    const plot = { ...from, id: uid('p'), items: from.items.map((i) => ({ ...i, id: uid('c') })) };
    const tab = makeTab('Copy', { plots: { [plot.id]: plot }, layout: leaf(plot.id) });
    const ws = new Workspace({ tabs: [tab], activeId: tab.id });
    const views = new Views(this.hub);
    views.setDelay(this.views.delay);
    const host = new PlotHost({ hub: this.hub, ws, history: new History(ws, new Favorites(null)), views, colors: this.colors.copy(), frame: this.frame, live: false });
    host.id = plot.id;
    const view = host.view(plot.id);
    view.restore(this.view(id).state(), perf);
    if (view.live) view.pause(perf);
    return host;
  }

  /** Ends a host made by `copy`: the windows read for its plots and its hold on the hub's history go. */
  dispose() {
    this.views.onChange = () => {};
    for (const id of this.ws.allPlotIds()) this.dropWindows(id);
    this.hub.hold(this, false);
  }
}
