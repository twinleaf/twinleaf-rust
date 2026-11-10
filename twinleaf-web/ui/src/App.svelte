<script>
  // The page: a toolbar, a sidebar of devices and signals beside the layout
  // tabs and their plots, and the dialogs. What it shows comes from the hub
  // (hub.svelte.js), what it lays out from the workspace
  // (workspace.svelte.js), and every plot is drawn on one WebGL canvas
  // behind the page (gl.js), each by its PlotView (plot.js).
  //
  // Each time plot has a View of its own (views.js). A gesture on a plot acts
  // on every selected plot when the plot is one of them, or else on it alone.
  // A command from the page or the keys acts on the selection; with none, a
  // view command (the toolbar, Space, Ctrl+C, Export, the eraser, pan, zoom,
  // follow, fit y) on every plot shown, and a plot command on none
  // (selection.svelte.js `pageTargets`). Space pauses spectra too; the
  // toolbar, pan and zoom set time plots only. Each gesture and command
  // that moves views is a step of the tab's view history
  // (viewhistory.svelte.js), which each plot's Back and Forward go through.
  import { getContext, onMount, setContext, tick, untrack } from 'svelte';
  import { MediaQuery, SvelteSet } from 'svelte/reactivity';
  import { ViewHistory } from './lib/viewhistory.svelte.js';
  import { Drags } from './lib/drag.svelte.js';
  import { Renderer } from './lib/gl.js';
  import { plotIds } from './lib/layout.js';
  import { exportText, importText, newPlot, saveTabs } from './lib/tabs.js';
  import { ARRANGEMENTS, DECIMATIONS } from './lib/constants.js';
  import { actionFor, scopeOf } from './lib/keymap.js';
  import { keepsKey, plotPickers } from './lib/keys.js';
  import { api } from './lib/protocol.js';
  import { THEMES, TRACE_WIDTH, prefs } from './lib/prefs.svelte.js';
  import RiSun from '@lucide/svelte/icons/sun';
  import RiMoon from '@lucide/svelte/icons/moon';
  import RiSystem from '@lucide/svelte/icons/monitor';
  import PlotCard from './lib/PlotCard.svelte';
  import Tiles from './lib/Tiles.svelte';
  import Devices from './lib/Devices.svelte';
  import Properties from './lib/Properties.svelte';
  import Dock from './lib/Dock.svelte';
  import { Docks, PANELS, SIDES } from './lib/docks.svelte.js';
  import { byRoute, panelRows } from './lib/properties.js';
  import { ROWS } from './lib/selection.svelte.js';
  import SignalList from './lib/SignalList.svelte';
  import TabStrip from './lib/TabStrip.svelte';
  import DragOverlay from './lib/DragOverlay.svelte';
  import FocusRing from './lib/FocusRing.svelte';
  import Toasts from './lib/Toasts.svelte';
  import { noticeOf, toast } from './lib/toast.svelte.js';
  import { DeviceWrites } from './lib/writes.svelte.js';
  import WriteNotice from './lib/WriteNotice.svelte';
  import { MAX_COPY_ROWS, blockTable, lostSeconds, writeTables } from './lib/copy.js';
  import { timeAxis } from './lib/plot.js';
  import { leave, navigate, remember } from './lib/focus.js';
  import KeysDialog from './lib/KeysDialog.svelte';
  import ExportDialog from './lib/ExportDialog.svelte';
  import WindowSlider from './lib/WindowSlider.svelte';
  import { windowText } from './lib/window.js';
  import RiExport from '@lucide/svelte/icons/file-down';
  import StatusBar from './lib/StatusBar.svelte';
  import HistorySettings from './lib/HistorySettings.svelte';
  import CommandPalette from './lib/CommandPalette.svelte';
  import IconButton from './lib/IconButton.svelte';
  import Tip from './lib/Tip.svelte';
  import { Button } from '$lib/components/ui/button/index.js';
  import * as Select from '$lib/components/ui/select/index.js';
  import { Slider } from '$lib/components/ui/slider/index.js';
  import { Checkbox } from '$lib/components/ui/checkbox/index.js';
  import * as Tooltip from '$lib/components/ui/tooltip/index.js';
  import * as DropdownMenu from '$lib/components/ui/dropdown-menu/index.js';
  import * as AlertDialog from '$lib/components/ui/alert-dialog/index.js';
  import * as Popover from '$lib/components/ui/popover/index.js';
  import * as Resizable from '$lib/components/ui/resizable/index.js';
  import RiSearch from '@lucide/svelte/icons/search';
  import RiSettings from '@lucide/svelte/icons/settings-2';
  import RiPlay from '@lucide/svelte/icons/play';
  import RiPause from '@lucide/svelte/icons/pause';
  import RiEraser from '@lucide/svelte/icons/eraser';
  import RiAdd from '@lucide/svelte/icons/plus';
  import RiGrid from '@lucide/svelte/icons/layout-grid';
  import RiDownload from '@lucide/svelte/icons/download';
  import RiReset from '@lucide/svelte/icons/rotate-ccw';
  import RiRows from '@lucide/svelte/icons/rows-3';
  import RiColumns from '@lucide/svelte/icons/columns-3';
  import RiGridArrange from '@lucide/svelte/icons/layout-grid';
  import RiQuadrants from '@lucide/svelte/icons/grid-2x2';
  import RiFocusArrange from '@lucide/svelte/icons/layout-panel-left';
  import RiUpload from '@lucide/svelte/icons/upload';
  import { counted } from './lib/menus.js';
  import RiCheck from '@lucide/svelte/icons/check';
  import RiChevron from '@lucide/svelte/icons/chevron-down';

  // The hub, the tabs as the page edits them, their undo and redo, the
  // PlotViews on the page, which the frame loop draws, and each plot's view,
  // as main.js makes them: the panels reach the History to make a command
  // one step.
  const { hub, ws, history, registry, views } = getContext('page');
  setContext('history', history);
  views.onChange = viewChanged;
  const viewHistory = new ViewHistory(ws, views, { dropped: droppedOf, notice: (text) => toast.warning(text) });
  // What a press, a click, a key or an undo does to the plot selection: the Workspace's.
  const selection = ws.selection;
  selection.changed = () => refreshToolbar();
  // The toolbar shows the open tab's views: opening another tab shows its.
  $effect(() => {
    ws.activeId;
    untrack(refreshToolbar);
  });
  const drags = new Drags(ws, refreshToolbar, history);
  setContext('selection', selection);
  // What the panels write to devices and run on them, and its notices
  // (writes.svelte.js). No device write is an undo step, and Revert writes
  // only to the devices checked in the Devices panel.
  const writes = new DeviceWrites(hub, {
    open: () => writeNotice.open(),
    close: () => writeNotice.close(),
    report: (level, text, detail) => toast[level](text, [], detail),
  }, { checked: (route) => !unchecked.has(route) });
  const writeNotice = noticeOf('device-write', WriteNotice, { writes }, () => writes.closed());
  // The notice of device writes, whose Revert writes through this page, goes with it.
  onMount(() => () => writeNotice.close());
  setContext('writes', writes);
  drags.closePlot = (id) => closePlot(id);
  drags.axisDropAt = (plotId, x, y) => [...registry].find((pv) => pv.plotId === plotId)?.axisDropAt(x, y) ?? null;
  /** The time under the pointer, which every time plot marks. */
  const hover = { t: null };
  let renderer = $state();
  let glCanvas;

  $effect(() => saveTabs(localStorage, ws.tabs, ws.activeId));
  // A plot keeps its view while an undo or a redo can bring it back, and a
  // tab its view history, each going once none can. A closed plot comes back
  // showing what it showed, by the rule Back shows a view by, as the hub may
  // have dropped its history meanwhile; paused, it reads its history again.
  let placed = new Set();
  $effect(() => {
    const held = history.held(), ids = new Set(ws.allPlotIds());
    views.prune(held.plots);
    viewHistory.prune(held.tabs);
    untrack(() => {
      const back = [...ids].filter((id) => !placed.has(id));
      viewHistory.returned(back);
      for (const id of back) if (views.get(id)?.live === false) requestDetail(id);
      placed = ids;
      refreshToolbar();
    });
  });
  // The hub estimates the spectra of the open tab's spectrum plots only.
  $effect(() => hub.showSpectra(ws.spectrumKeys()));
  // A new session, or a new decimation, drops every window, so each paused
  // plot reads its own again.
  $effect(() => {
    hub.session.detailsDropped;
    untrack(() => {
      overviewKeys.clear();
      for (const id of ws.allPlotIds()) if (views.get(id)?.live === false) requestDetail(id);
    });
  });

  const targetsOf = (id) => selection.targets(id).map((i) => views.of(i));
  /** The plots a command made on plot `id` acts on, as the Selection says. */
  const targetIdsOf = (id) => selection.targets(id);
  const plotsText = (n) => counted(n, 'plot');
  /** Plot `id`'s name in a list of plots: its signals, or Empty plot. */
  const labelOf = (id) => ws.plots[id]?.items.map((i) => i.key).join(', ') || 'Empty plot';
  /**
   * Gives each of plots `ids` the fields `fn(plot)` returns, if any, as
   * one step undo names `what`.
   */
  function eachPlot(ids, what, fn) {
    history.group(ids.length > 1 ? `${what} (${plotsText(ids.length)})` : what, () => {
      for (const i of ids) {
        const patch = fn(ws.plots[i]);
        if (patch) ws.setPlot(i, patch);
      }
    });
  }
  /** The views the toolbar acts on: those of the time plots it acts on. */
  function chosen() {
    return timeViews(selection.pageTargets('view'));
  }
  /** The views of the time plots of `ids`. */
  const timeViews = (ids) => ids.filter((id) => ws.plots[id]?.kind === 'time').map((id) => views.of(id));
  /** The plot of `id` in any tab. */
  const plotAnywhere = (id) => ws.tabs.find((t) => t.plots[id])?.plots[id];
  function viewChanged(id) {
    requestDetail(id);
    refreshToolbar();
  }
  /**
   * The browser's own right-click menu shows only where it has something to
   * offer: in a field, and over text that is selected. Everywhere else a
   * right-click opens the page's menu for what is under it, or nothing.
   */
  function nativeMenu(e) {
    if (e.target instanceof Element && e.target.closest('input, textarea, [contenteditable]')) return;
    if (getSelection()?.toString()) return;
    e.preventDefault();
  }

  /** Every time plot's view in the tab, which the toolbar's Live button acts on. */
  const tabViews = () => ws.timeIds().map((id) => views.of(id));
  // The top bar's menus and popovers: each opens 2 px below the bar, as a
  // plot's header menus open below the header (its 24 px buttons sit 5 px
  // above the 34 px bar's foot), and a press on another's button opens it
  // (overlay.svelte.js).
  const BAR_OFFSET = 7;
  /** Each theme's icon. */
  const THEME_ICONS = { light: RiSun, dark: RiMoon, system: RiSystem };
  /** Each arrangement's picture in the Layout menu. */
  const ARRANGE_ICONS = { rows: RiRows, columns: RiColumns, grid: RiGridArrange, quadrants: RiQuadrants, focus: RiFocusArrange };
  /** Shows the views' state in the toolbar, and keeps every paused view's points. */
  function refreshToolbar() {
    const vs = chosen();
    live = tabViews().every((v) => v.live);
    if (vs.length) span = +vs[0].window.toFixed(3);
    // A paused time plot in a tab holds its points: a paused spectrum holds
    // the spectrum it shows, and a closed plot nothing.
    hub.hold(views.anyPaused((id) => plotAnywhere(id)?.kind === 'time'));
  }
  // Paused, every pan or zoom asks the server for the plot's view at full
  // resolution, read from its raw buffer, which reaches back as far as the
  // history kept. Live, the decimated stream is used.
  // A move asks at once, then at most every DETAIL_MS while it goes on, and
  // once more after it stops, so a drag keeps the detail close behind it.
  const DETAIL_MS = 100;
  const detailAsks = new Map(); // plot id -> {last, timer}
  const overviewKeys = new Map(); // plot id -> what its overview was read for
  function requestDetail(id) {
    const v = views.get(id);
    if (!v || v.live) {
      overviewKeys.delete(id);
      return hub.session.clearWindows(id);
    }
    requestOverview(id, v);
    let ask = detailAsks.get(id);
    if (!ask) detailAsks.set(id, (ask = { last: 0, timer: null }));
    if (ask.timer) return;
    const run = () => {
      ask.last = performance.now();
      ask.timer = null;
      const plot = plotAnywhere(id);
      if (!plot || plot.kind !== 'time' || views.get(id)?.live !== false) return;
      const [x0, x1] = views.get(id).range(hub.now(performance.now()));
      // A span either side, so zooming out a few steps or panning stays covered.
      const pad = x1 - x0;
      hub.session.fetchWindow(id, 'detail', tracesOf(plot), x0 - pad, x1 + pad, window.innerWidth * 2);
    };
    const wait = DETAIL_MS - (performance.now() - ask.last);
    if (wait <= 0) run();
    else ask.timer = setTimeout(run, wait);
  }
  const tracesOf = (plot) => plot.items.map((i) => hub.session.byKey.get(i.key)).filter(Boolean);
  /** The time before which the hub has dropped what it held of plot `id`'s signals, or null when it has dropped none. */
  function droppedOf(id) {
    const plot = plotAnywhere(id);
    return plot ? hub.session.droppedBefore(tracesOf(plot).map((t) => t.meta.id)) : null;
  }
  /** Reads paused plot `id`'s whole captured history once, again only when its signals or its pause change. */
  function requestOverview(id, v) {
    const plot = plotAnywhere(id);
    if (!plot || plot.kind !== 'time') return;
    const traces = tracesOf(plot);
    const oldest = hub.session.memory?.oldest ?? {};
    const lo = Math.min(...traces.map((t) => oldest[t.meta.id] ?? Infinity));
    const hi = v.pausedAt;
    if (!isFinite(lo) || hi === null) return;
    const key = `${hi}|${traces.map((t) => t.meta.id).join(',')}`;
    if (overviewKeys.get(id) === key) return;
    overviewKeys.set(id, key);
    hub.session.fetchWindow(id, 'overview', traces, lo, hi, window.innerWidth * 2);
  }


  // Closing a tab with plots asks first: `closing` is the tab the dialog
  // asks about, and shows it. The dialog's Close button closes the tab and
  // the dialog; its Cancel and Esc close the dialog only.
  let closing = $state(null);
  function closeTab(t) {
    if (plotIds(t.layout).length) closing = t;
    else history.group('Close tab', () => ws.closeTab(t));
  }
  function confirmClose() {
    const t = closing;
    history.group('Close tab', () => ws.closeTab(t));
    closing = null;
  }

  // Export and import: a JSON file of tabs, to share layouts.
  function exportTabs() {
    const url = URL.createObjectURL(new Blob([exportText(ws.tabs)], { type: 'application/json' }));
    const a = Object.assign(document.createElement('a'), { href: url, download: 'twinleaf-layouts.json' });
    a.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }
  let importInput;
  let importError = $state('');
  async function importTabs(e) {
    const file = e.currentTarget.files?.[0];
    e.currentTarget.value = '';
    if (!file) return;
    try {
      const tabs = importText(await file.text());
      history.group('Import', () => ws.addTabs(tabs));
      importError = '';
    } catch (err) {
      importError = `Import failed: ${err.message}`;
    }
  }

  let live = $state(true);
  let span = $state(10);
  let delayMs = $state(150);
  let hud = $state({ fps: 0, frameMs: 0, segments: 0, drawCalls: 0, uploadKBs: 0, rxKBs: 0, gpu: '' });
  // The docks beside the tiles and the panels in them (docks.svelte.js).
  const docks = new Docks(localStorage);
  // The devices unchecked in the Devices panel, by route, one set per hub,
  // and the checked ones the Properties panels show.
  const unchecked = $derived.by(() => {
    hub.session;
    return new SvelteSet();
  });
  const checked = $derived([...hub.session.devices.values()].sort(byRoute).filter((d) => !unchecked.has(d.route)));
  // The property rows the page shows, in page order, which a Shift pick runs
  // along: each open panel of rows or action buttons of the docks shown,
  // left then right, top to bottom.
  selection.rowOrder = () =>
    SIDES.filter((side) => docks.shown(side))
      .flatMap((side) => docks[side].filter((p) => p.open && PANELS[p.id].channels))
      .flatMap(({ id }) => panelRows(checked, PANELS[id].channels, history.favorites).map((r) => `${id}:${r.name}`));

  // Each dock's width, a share of the page, dragged at its edge or moved
  // with the arrows there, and kept for the next visit. A double-click puts
  // it back.
  const DOCK = { default: 22, min: 12, max: 45, keys: { left: 'tl-side-size', right: 'tl-right-size' } };
  // A narrow window puts the left dock above the tiles and the right below.
  const narrow = new MediaQuery('max-width: 760px');
  const dockPanes = $state({ left: null, right: null });
  const dockSize = Object.fromEntries(
    ['left', 'right'].map((side) => {
      try {
        const saved = Number(localStorage.getItem(DOCK.keys[side]));
        if (saved >= DOCK.min && saved <= DOCK.max) return [side, saved];
      } catch {}
      return [side, DOCK.default];
    }),
  );
  function docksResized(sizes) {
    // The panes are left, tiles and right, a hidden dock's collapsed to its rail.
    try {
      [['left', sizes[0]], ['right', sizes[2]]].forEach(([side, size]) => {
        if (docks.shown(side) && size >= DOCK.min) localStorage.setItem(DOCK.keys[side], String((dockSize[side] = size)));
      });
    } catch {}
  }
  /** Each dock's width as a drag of its handle began, which it opens to again when that drag collapsed it. */
  const dragFrom = { left: null, right: null };
  /** Hides dock `side` when a drag of its handle took its pane below a dock's least width, collapsing it to the rail. */
  function dockCollapsed(side, size) {
    if (size >= DOCK.min || !docks.shown(side)) return;
    if (dragFrom[side] !== null) {
      dockSize[side] = dragFrom[side];
      try {
        localStorage.setItem(DOCK.keys[side], String(dockSize[side]));
      } catch {}
    }
    docks.hide(side, true);
  }

  // A dock that hides or shows slides, as its pane collapses or opens to its
  // width, the tiles and their plots giving it room or taking its room back.
  // The pane group's `sliding` class carries the transition only for the
  // slide, so a drag of a handle follows the pointer.
  //
  // A dock and its rail are one frame in the dock's pane, which collapses to
  // the rail's width rather than to nothing, so the row keeps its width and
  // nothing beside the panes comes or goes. Sliding, the frame's outer edge,
  // top and bottom stay put and its inner edge moves with the pane's, the
  // tiles beside it in step. Its panels keep the width they have open,
  // `contentSize` in px, pinned to the outer edge and cut off by the moving
  // edge, so they never reflow; shutting, they fade out by the slide's
  // midpoint, and opening, they fade in after it. The toggle sits at the
  // frame's top right, as the rail's does: the right dock shrinks into it,
  // and the left dock's rides its inner edge to the rail's spot.
  let dockGroup = $state(null);
  /** The room the panes share, in px: the group less its handles. */
  let paneRoom = $state(0);
  /** A rail's width in px: one holding panels has its show button; an empty one is a strip to drop on, wider while a panel is dragged. */
  const RAIL_PX = { panels: 26, empty: 8, awaiting: 56 };
  const railPx = (side) => (docks.dragging ? RAIL_PX.awaiting : docks[side].length ? RAIL_PX.panels : RAIL_PX.empty);
  /** Each side's rail as a share of the panes' room, the size its pane collapses to. */
  const railPct = $derived({ left: paneRoom ? (railPx('left') / paneRoom) * 100 : 0, right: paneRoom ? (railPx('right') / paneRoom) * 100 : 0 });
  /**
   * The size `side`'s pane starts at, its width or its rail's while hidden.
   * Read once as the panes mount: a hide or a show after that is a slide.
   */
  const startSize = (side) => untrack(() => (docks.shown(side) ? dockSize[side] : railPct[side]));
  const paneSize = (el) => (narrow.current ? el.offsetHeight : el.offsetWidth);
  const panesOf = () => [...(dockGroup?.children ?? [])].filter((el) => el.hasAttribute('data-pane-id'));
  $effect(() => {
    if (!dockGroup) return;
    const measure = () => (paneRoom = panesOf().reduce((n, el) => n + paneSize(el), 0));
    const watch = new ResizeObserver(() => !dockGroup.classList.contains('sliding') && measure());
    watch.observe(dockGroup);
    measure();
    return () => watch.disconnect();
  });
  // Whether a side's frame is its rail or holds its panels. It turns to the
  // rail only once a slide shut ends.
  let railed = $state(untrack(() => ({ left: !docks.shown('left'), right: !docks.shown('right') })));
  /** Each side's slide under way, 'opening' or 'closing', or null. */
  let slide = $state({ left: null, right: null });
  let contentSize = $state({ left: null, right: null });
  /**
   * How long a dock takes to slide: opening over Material's duration-medium2,
   * shutting over medium1, as a collapse is quicker; both on its standard
   * easing (plot.css).
   */
  const DOCK_SLIDE_MS = { opening: 300, closing: 250 };
  /** How long a slide waits to start when no frame comes, in ms. */
  const SLIDE_START_MS = 100;
  let slideDone;
  let settled = false;
  $effect(() => {
    const want = { left: docks.shown('left'), right: docks.shown('right') };
    const rail = { left: railPct.left, right: railPct.right };
    untrack(() => {
      for (const side of ['left', 'right']) {
        const pane = dockPanes[side];
        const now = pane?.getSize?.();
        if (!pane || now === undefined || !paneRoom) continue;
        const target = want[side] ? dockSize[side] : rail[side];
        // Already there, as a drag of the handle collapses the pane itself:
        // the frame turns to the rail, or back, at once.
        if (Math.abs(now - target) < 0.01) {
          if (railed[side] === want[side]) railed = { ...railed, [side]: !want[side] };
          continue;
        }
        // The first measure only sets a hidden dock's rail to its width: no slide.
        if (!settled) {
          pane.resize(target);
          continue;
        }
        const own = panesOf().find((el) => el.dataset.paneId === `dock-${side}`);
        // A rail growing or shrinking, as a panel is dragged, has no panels to pin.
        const kind = want[side] === railed[side] ? (want[side] ? 'opening' : 'closing') : null;
        // The panels keep the width the frame has open: the width it has now
        // when shutting, the width it opens to when opening. A frame's border
        // and padding, 1 and 6 px a side, are not theirs.
        const open = want[side] ? (paneRoom * dockSize[side]) / 100 : own ? paneSize(own) : 0;
        contentSize = { ...contentSize, [side]: kind ? open - 14 : null };
        slide = { ...slide, [side]: kind };
        if (want[side]) railed = { ...railed, [side]: false };
        clearTimeout(slideDone);
        const ms = kind === 'opening' ? DOCK_SLIDE_MS.opening : DOCK_SLIDE_MS.closing;
        // The slide starts a frame after the panels mount, so the work of
        // mounting them is not the slide's first frame; or after
        // SLIDE_START_MS, on a page the browser draws no frames for, so the
        // dock still ends where it should.
        let started = false;
        const start = () => {
          if (started) return;
          started = true;
          dockGroup?.style.setProperty('--slide-ms', `${ms}ms`);
          dockGroup?.classList.add('sliding');
          pane.resize(target);
          slideDone = setTimeout(() => {
            dockGroup?.classList.remove('sliding');
            railed = { left: !docks.shown('left'), right: !docks.shown('right') };
            slide = { left: null, right: null };
            contentSize = { left: null, right: null };
          }, ms + 50);
        };
        requestAnimationFrame(() => requestAnimationFrame(start));
        setTimeout(start, SLIDE_START_MS);
      }
      if (paneRoom) settled = true;
    });
  });

  function newPlotWith(keys) {
    history.group('New plot', () => ws.addPlot(newPlot('time', 'overlay', keys)));
  }
  /**
   * Adds signal `key`, from the Signals list or the command palette, to
   * every selected plot. With none selected it does nothing: a signal goes
   * to a plot by drag there.
   */
  function addSignal(key) {
    const ids = selection.pageTargets('plot');
    if (ids.length) history.group(ids.length > 1 ? `Add ${key} (${plotsText(ids.length)})` : `Add ${key}`, () => ws.addTo(ids, key));
  }
  /**
   * What the Signals list's Add to plot says of signal `key`, as {text,
   * can}: the plot it would join, or how many, from the same selection the
   * command acts on; or why it joins none.
   */
  function addsTo(key) {
    const ids = selection.pageTargets('plot'), lacking = ids.filter((id) => !ws.plots[id].items.some((i) => i.key === key));
    if (!ids.length) return { text: 'No plot selected', can: false };
    if (!lacking.length) return { text: 'Already shown', can: false };
    return { text: lacking.length > 1 ? plotsText(lacking.length) : labelOf(lacking[0]), can: true };
  }
  /** Moves keyboard focus to plot `id`, once it is on the page. */
  async function focusPlot(id) {
    if (!ws.plots[id]) return;
    selection.point(id);
    await tick();
    document.querySelector(`.leaf[data-plot="${id}"] .plot-card`)?.focus();
  }
  function closePlot(id) {
    ws.closePlot(id);
    hub.session.clearWindows(id);
    overviewKeys.delete(id);
  }
  /**
   * Closes plots `ids` as one step. From the keys, focus goes to the plot
   * after the last of them, or else the one before the first.
   */
  function closePlots(ids, fromKeys = false) {
    // The plots as they show, so focus goes to one that does.
    const order = ws.shownIds();
    const at = ids.map((id) => order.indexOf(id)).filter((i) => i >= 0);
    history.group(ids.length > 1 ? `Close ${plotsText(ids.length)}` : 'Close plot', () => {
      for (const id of ids) closePlot(id);
    });
    if (!fromKeys) return;
    const next = order.slice(Math.max(...at) + 1).find((id) => ws.plots[id]) ?? order.slice(0, Math.min(...at)).findLast((id) => ws.plots[id]);
    if (next) focusPlot(next);
    else selection.point(null);
  }
  /** Goes `by` layout tabs along, wrapping, to the plot last focused there, its first plot, or else its tab. */
  async function stepTab(by) {
    const i = ws.tabs.findIndex((t) => t.id === ws.activeId);
    ws.selectTab(ws.tabs[(i + by + ws.tabs.length) % ws.tabs.length].id);
    const to = ws.lastFocus.get(ws.activeId);
    if (ws.plots[to]) return focusPlot(to);
    if (plotIds(ws.layout).length) return focusPlot(plotIds(ws.layout)[0]);
    await tick();
    document.querySelector('[role=tab][aria-selected=true]')?.focus();
  }

  onMount(() => {
    renderer = new Renderer(glCanvas);
    hub.connect();
    const HUD_MS = 500;
    let frames = 0, cpu = 0, last = performance.now(), lastBytes = 0, upload = 0, raf;
    const loop = (perf) => {
      const start = performance.now();
      hub.session.tick(perf);
      renderer.begin();
      for (const pv of registry) pv.draw(perf);
      const st = renderer.stats;
      upload += st.uploadBytes;
      cpu += performance.now() - start;
      frames++;
      const elapsed = perf - last;
      if (elapsed >= HUD_MS) {
        hud = {
          fps: (frames * 1000) / elapsed,
          frameMs: cpu / frames,
          segments: st.segments,
          drawCalls: st.drawCalls,
          uploadKBs: upload / 1e3 / (elapsed / 1000),
          rxKBs: (hub.bytes - lastBytes) / 1e3 / (elapsed / 1000),
          gpu: renderer.gpu,
        };
        frames = 0; cpu = 0; upload = 0; last = perf; lastBytes = hub.bytes;
      }
      raf = requestAnimationFrame(loop);
    };
    raf = requestAnimationFrame(loop);
    // A hot reload unmounts the page without leaving it, so the socket, the
    // GPU context and the window reads still to ask go with the copy that
    // made them.
    return () => {
      cancelAnimationFrame(raf);
      for (const ask of detailAsks.values()) clearTimeout(ask.timer);
      hub.close();
      renderer.dispose();
    };
  });

  function setSpan(v) {
    const vs = chosen();
    viewHistory.burst('window', vs, () => vs.forEach((vw) => vw.setSpan(v)));
  }
  function followLive() {
    viewHistory.step(() => chosen().forEach((vw) => vw.follow(true)));
  }
  /**
   * The toolbar's Live button: pauses every time plot of the tab while all
   * follow live, or has them all follow live. Space does the same for the
   * selected plots alone.
   */
  function toggleLive() {
    // Whether to pause, read once: each view that moves sets `live` again.
    const perf = performance.now(), pause = live;
    viewHistory.step(() => tabViews().forEach((vw) => (pause ? vw.pause(perf) : vw.follow())));
    refreshToolbar();
  }
  function setDelay(ms) {
    delayMs = ms;
    views.setDelay(ms / 1000);
  }
  /** Clears the cursors of plots `ids`, by default the selection's or every plot's, and says whether any were placed. */
  function clearCursors(ids = selection.pageTargets('view')) {
    let had = false;
    viewHistory.step(() => {
      for (const id of ids) had = views.get(id)?.clearCursors() || had;
    });
    return had;
  }
  const act = { toggleDock: (side) => docks.hide(side, docks.shown(side)), dockShown: (side) => docks.shown(side), addSignal, newPlot: newPlotWith, followLive, clearCursors: () => clearCursors(), exportTabs, importTabs: () => importInput.click() };

  let paletteOpen = $state(false);
  let keysOpen = $state(false);
  /**
   * Copies the data of plots `ids`, as copy.js lays it out: a time plot's
   * every raw sample in its visible window, and a spectrum plot's spectra
   * as it shows them, each signal's density by frequency. Past MAX_COPY_ROWS
   * it copies nothing and says how many rows that would be; a window whose
   * start the history no longer holds is copied, and the notice says what is
   * missing.
   */
  // The export the dialog offers, or null: plots' signals, the time they
  // show and the time the history holds of them.
  let exporting = $state(null);
  /** Offers to export the signals of plots `ids`. */
  function openExport(ids) {
    const session = hub.session, now = hub.now(performance.now());
    const plots = ids.map((id) => [id, plotAnywhere(id)]).filter(([, p]) => p?.items.length);
    const traces = [...new Map(plots.flatMap(([, p]) => p.items.map((i) => session.byKey.get(i.key)).filter(Boolean)).map((t) => [t.key, t])).values()];
    if (!traces.length) return toast.warning('Nothing to export: the plots show no signals');
    const ranges = plots.filter(([, p]) => p.kind === 'time').map(([id]) => views.of(id).range(now));
    const shown = ranges.length ? [Math.min(...ranges.map((r) => r[0])), Math.max(...ranges.map((r) => r[1]))] : null;
    const oldest = Math.min(...traces.map((t) => session.memory?.oldest?.[t.meta.id] ?? Infinity));
    exporting = { hub: session.id, traces, shown, whole: [isFinite(oldest) ? oldest : -Infinity, now] };
  }

  /**
   * What Ctrl+C on plots `ids` would copy, from the hub's count of the rows,
   * as {state, rows, reason}: 'ok', 'too-many' past what a paste holds, or
   * 'partial' when history no longer holds the window's start, the reason
   * saying which.
   */
  async function copyCheck(ids) {
    const session = hub.session, now = hub.now(performance.now());
    let total = 0, lost = 0;
    for (const id of ids) {
      const plot = plotAnywhere(id);
      if (!plot?.items.length) continue;
      if (plot.kind === 'spectrum') {
        total += Math.max(0, ...plot.items.map((i) => session.byKey.get(i.key)?.spectrum?.f.length ?? 0));
        continue;
      }
      const [t0, t1] = views.of(id).range(now);
      const traces = plot.items.map((i) => session.byKey.get(i.key)).filter(Boolean);
      const replies = await Promise.all(traces.map((t) => api.rows({ hub: session.id, id: t.meta.id, t0, t1, limit: 0 })));
      total += replies.reduce((n, r) => n + r.count, 0);
      replies.forEach((r, i) => (lost = Math.max(lost, lostSeconds(t0, r.oldest, traces[i].meta.rate))));
    }
    if (total > MAX_COPY_ROWS) return { state: 'too-many', rows: total, reason: 'More than a paste holds: export instead' };
    if (lost) return { state: 'partial', rows: total, reason: `The first ${lost.toFixed(1)} s are no longer in history` };
    return { state: 'ok', rows: total };
  }

  async function copyData(ids) {
    const session = hub.session, now = hub.now(performance.now());
    const plots = ids.map((id) => [id, plotAnywhere(id)]).filter(([, p]) => p?.items.length);
    if (!plots.length) return toast.warning('Nothing to copy: the plots show no signals');
    const blocks = [];
    let total = 0, lost = 0;
    for (const [id, plot] of plots) {
      if (plot.kind === 'spectrum') {
        const pv = [...registry].find((v) => v.plotId === id);
        const traces = plot.items.map((i) => session.byKey.get(i.key)).filter(Boolean);
        const spectra = traces.map((t) => pv?.spectrumOf(t) ?? t.spectrum).map((sp) => sp ?? { f: [], a: [] });
        const table = blockTable(
          traces.map((t, i) => ({ key: t.key, units: t.meta.units ? `${t.meta.units}/√Hz` : '/√Hz', x: [...spectra[i].f], y: [...spectra[i].a] })),
          'frequency (Hz)',
        );
        total += table.rows.length;
        blocks.push({ title: `Spectrum of ${traces.map((t) => t.key).join(', ')}`, ...table });
        continue;
      }
      const [t0, t1] = views.of(id).range(now);
      const traces = plot.items.map((i) => session.byKey.get(i.key)).filter(Boolean);
      const replies = await Promise.all(traces.map((t) => api.rows({ hub: session.id, id: t.meta.id, t0, t1, limit: MAX_COPY_ROWS })));
      total += replies.reduce((n, r) => n + r.count, 0);
      if (total > MAX_COPY_ROWS) continue;
      replies.forEach((r, i) => (lost = Math.max(lost, lostSeconds(t0, r.oldest, traces[i].meta.rate))));
      const time = timeAxis(traces[0].meta.epoch);
      const table = blockTable(
        traces.map((t, i) => ({ key: t.key, units: t.meta.units, x: replies[i].x, y: replies[i].y })),
        traces[0].meta.epoch === 'unix' ? 'time (Unix s)' : 'time (s)',
      );
      blocks.push({ title: `${traces.map((t) => t.key).join(', ')} · ${time.readout(t0)} – ${time.readout(t1)}`, ...table });
    }
    if (total > MAX_COPY_ROWS)
      return toast.warning(`${total.toLocaleString()} rows is more than a paste holds well (${MAX_COPY_ROWS.toLocaleString()}); narrow the window, or export it`);
    const rows = blocks.reduce((n, b) => n + b.rows.length, 0);
    await writeTables(blocks);
    const missing = lost ? `; the first ${lost.toFixed(1)} s of the window are no longer in history` : '';
    toast.success(`Copied ${rows.toLocaleString()} rows of ${blocks.length} plot${blocks.length === 1 ? '' : 's'}${missing}`);
  }

  /** Moves signal `key` from plot `from` to plot `to`, or to a new plot to its right with null, as a drag would. */
  function moveSignal(from, key, to) {
    const item = ws.plots[from]?.items.find((i) => i.key === key);
    if (item) drags.moveSignal(item, to ? { plotId: to } : { plotId: from, edge: 'right' });
  }

  /**
   * A button presses on Space's keyup, not its keydown. A Space the page took
   * for its own, to pause, takes the keyup too, so a button focused by a
   * click presses nothing.
   */
  function onKeyUp(e) {
    if (e.key === ' ' && !keepsKey(e)) e.preventDefault();
  }

  /** The plot `el` is in, by id, or null. */
  const plotAt = (el) => el?.closest?.('.leaf[data-plot]')?.dataset.plot ?? null;

  /** How far an arrow pans, as a share of the span, and zooms, as a factor. */
  const PAN = 0.1, ZOOM = 1.25;
  /**
   * Acts by plot key `e` (keymap.js, place 'plot'), wherever it was
   * pressed, on the plots it acts on (selection.svelte.js `pageTargets`).
   * A picker opens on the plot clicked last when it is one of them, else
   * on the first. A key with no plots to act on does nothing, and goes on.
   */
  function plotKey(e) {
    const action = actionFor('plot', e);
    // F leaves the maximized view whatever is selected, as Escape does.
    if (action === 'maximize' && ws.maximized) {
      e.preventDefault();
      ws.maximized = null;
      return;
    }
    const ids = action ? selection.pageTargets(scopeOf(action)) : [];
    if (!ids.length) return;
    const perf = performance.now(), back = e.key === 'ArrowLeft' || e.key === 'ArrowDown';
    const lead = ids.includes(ws.focusedId) ? ws.focusedId : ids[0];
    const act = {
      splitRight: () => splitPlots(ids, 'row', true),
      splitDown: () => splitPlots(ids, 'col', true),
      maximize: () => (ws.maximized = ids),
      close: () => closePlots(ids, true),
      fitY: () => viewHistory.step(() => registry.forEach((p) => ids.includes(p.plotId) && p.autoscale())),
      legend: () => {
        const on = !ws.plots[lead].legend;
        eachPlot(ids, on ? 'Show the legend' : 'Hide the legend', () => ({ legend: on }));
      },
      follow: () => viewHistory.step(() => ids.forEach((id) => views.of(id).follow(true))),
      pan: () => viewHistory.burst('pan', timeViews(ids), () => timeViews(ids).forEach((v) => v.pan((e.shiftKey ? 1 : PAN) * (back ? -1 : 1), perf))),
      zoom: () => viewHistory.burst('zoom', timeViews(ids), () => timeViews(ids).forEach((v) => v.zoom(back ? ZOOM : 1 / ZOOM, v.live ? 1 : 0.5, perf))),
    }[action];
    if (!act && !plotPickers.get(lead)?.(action)) return;
    e.preventDefault();
    act?.();
  }
  /**
   * Puts an empty plot beside each of plots `ids`, on side `dir` of it, as
   * one step that selects the new plots, to fill next, so redo selects them
   * again. From the keys, focus goes to the first.
   */
  function splitPlots(ids, dir, fromKeys = false) {
    const made = ids.map(() => newPlot('time', 'overlay', []));
    history.group(ids.length > 1 ? `Split ${plotsText(ids.length)}` : 'Split', () => {
      ids.forEach((at, i) => ws.addPlot(made[i], at, dir));
      selection.only(made.map((p) => p.id));
    });
    if (fromKeys) focusPlot(made[0].id);
  }

  function onKey(e) {
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') {
      e.preventDefault();
      paletteOpen = !paletteOpen;
      return;
    }
    // Escape, once a menu or a drag has had it, leaves the maximized view,
    // the selection kept; then deselects; with nothing selected it goes out
    // a level. It never clears cursors.
    if (e.key === 'Escape' && !e.defaultPrevented && !keepsKey(e) && (ws.maximized || ws.selected.size || selection.rows.size)) {
      e.preventDefault();
      if (ws.maximized) return void (ws.maximized = null);
      return selection.clear();
    }
    // Tab, the arrows, Enter, Escape and F6 move through the page's tree
    // (focus.js); a move that brings focus into another plot selects it.
    const from = plotAt(document.activeElement);
    if (navigate(e)) {
      const to = plotAt(document.activeElement);
      if (to && to !== from && e.key !== 'Escape') selection.arrived(to);
      return;
    }
    // What is typed into a field, a menu or dialog's own keys, and the keys
    // that press a button focused from the keyboard stay there (keys.js).
    if (keepsKey(e)) return;
    // Ctrl+Z takes the last edit back, and Ctrl+Y or Ctrl+Shift+Z does it again.
    if ((e.ctrlKey || e.metaKey) && !e.altKey && ['z', 'y'].includes(e.key.toLowerCase())) {
      e.preventDefault();
      const redo = e.key.toLowerCase() === 'y' || e.shiftKey;
      const label = redo ? history.redo() : history.undo();
      if (label) toast(`${redo ? 'Redo' : 'Undo'}: ${label}`);
      return refreshToolbar();
    }
    // Ctrl+C with no text selected copies the data of the selected plots, or of every plot.
    if ((e.ctrlKey || e.metaKey) && !e.shiftKey && e.key.toLowerCase() === 'c' && !String(window.getSelection() ?? '')) {
      e.preventDefault();
      return void copyData(selection.pageTargets('view')).catch((error) => toast.error(`Could not copy: ${error.message}`));
    }
    // Ctrl+A outside a field selects every plot of the tab, not the page's text.
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'a') {
      e.preventDefault();
      return selection.only(ws.shownIds(), null);
    }
    const action = actionFor('page', e);
    if (action === 'goToPlot') {
      // The plots as they show: only the maximized ones while some are.
      const id = ws.shownIds()[+e.key - 1];
      if (!id) return;
      e.preventDefault();
      selection.arrived(id);
      return focusPlot(id);
    }
    if (action === 'help') return void (keysOpen = true);
    if (action === 'stepTab') {
      if (ws.tabs.length < 2) return;
      e.preventDefault();
      return stepTab(e.key === ']' ? 1 : -1);
    }
    if (action !== 'pause' && action !== 'escape') {
      // Every other key is a plot's key, wherever focus is; among property
      // rows, which are what is picked there, none.
      if (!action && !e.defaultPrevented && !e.target.closest?.(ROWS)) plotKey(e);
      return;
    }
    if (action === 'escape') return leave();
    // Space pauses the selected plots, or every plot, or has them follow live again.
    e.preventDefault();
    const perf = performance.now(), vs = selection.pageTargets('view').map((id) => views.of(id));
    const pause = vs.some((v) => v.live);
    viewHistory.step(() => vs.forEach((v) => (pause ? v.pause(perf) : v.follow())));
  }
</script>

<!-- Nothing on the page drags natively: plots, signals and tabs move by
     pointer, and a native drag would only carry a picture of the page. -->
<svelte:window onkeydown={onKey} onkeyup={onKeyUp} onfocusin={remember} onpointerdowncapture={(e) => selection.press(e)} oncontextmenu={nativeMenu} ondragstart={(e) => e.preventDefault()} />
<canvas class="gl-layer" bind:this={glCanvas}></canvas>

{#snippet plotTile(id)}
  <!-- Its own plot, so an edit to another plot does not reach this one. -->
  {@const plot = ws.plots[id]}
  {#key id}
    {#if renderer && plot}
      <PlotCard
        {plot}
        {hub}
        view={views.of(id)}
        {viewHistory}
        targets={() => targetsOf(id)}
        {hover}
        {renderer}
        {registry}
        otherTabs={ws.tabs.filter((t) => t.id !== ws.activeId)}
        tabName={ws.tab.name}
        targetPlots={() => targetIdsOf(id).map((i) => ws.plots[i])}
        focused={ws.focusedId === id}
        selected={ws.selected.has(id)}
        onSelect={(shift, clicks) => selection.click(id, shift, clicks)}
        maximized={!!ws.maximized?.includes(id)}
        onFocus={() => selection.point(id)}
        onPress={() => selection.pressOn(id)}
        onRemove={() => closePlots(targetIdsOf(id))}
        onEach={(what, fn) => eachPlot(targetIdsOf(id), what, fn)}
        onKey={plotKey}
        onEdit={(what, patch) => history.group(what, () => ws.setPlot(id, patch))}
        onView={(patch) => viewHistory.step(() => targetIdsOf(id).forEach((i) => ws.plots[i]?.kind === ws.plots[id]?.kind && ws.setView(i, patch)))}
        targetIds={() => targetIdsOf(id)}
        onStartMove={(e) => drags.startMove(id, e)}
        onSplit={(dir) => splitPlots(targetIdsOf(id), dir)}
        onMaximize={() => (ws.maximized = ws.maximized?.includes(id) ? null : targetIdsOf(id))}
        onMoveToTab={(tabId) => {
          const ids = targetIdsOf(id);
          history.group(`Move ${plotsText(ids.length)} to ${ws.tabs.find((t) => t.id === tabId)?.name ?? 'a tab'}`, () => {
            for (const i of ids) ws.movePlotToTab(i, tabId);
          });
        }}
        onClearCursors={() => clearCursors(targetIdsOf(id))}
        onTraceDragStart={() => drags.traceDragStart()}
        onLandSignal={(item, items) => drags.landSignal(item, items ? id : null, items)}
        otherPlots={plotIds(ws.layout)
          .filter((p) => p !== id)
          .map((p) => ({ id: p, label: labelOf(p) }))}
        onMoveSignal={(key, to) => moveSignal(id, key, to)}
        onCheckCopy={() => copyCheck(targetIdsOf(id))}
        onExport={() => openExport(targetIdsOf(id))}
        onCopy={() => copyData(targetIdsOf(id)).catch((error) => toast.error(`Could not copy: ${error.message}`))}
      />
    {/if}
  {/key}
{/snippet}

{#snippet panelBody(id)}
  {#if id === 'devices'}
    <Devices session={hub.session} {unchecked} />
  {:else if id === 'signals'}
    <!-- The whole panel keeps the plot selection: its search, its list and its scrollbar. -->
    <div class="contents" data-keeps-selection>
      <SignalList session={hub.session} {drags} onAdd={addSignal} onNewPlot={newPlotWith} {addsTo} />
    </div>
  {:else}
    {#key hub.session}<Properties devices={checked} channels={PANELS[id].channels} panel={id} />{/key}
  {/if}
{/snippet}

{#snippet dockFrame(side)}
  <!-- The dock's frame, its rail too: it fills its pane, and its panels keep their open size while it slides. -->
  <aside
    class="dock-frame {side} flex min-h-0 flex-1 cursor-crosshair flex-col overflow-hidden rounded-lg border bg-card"
    class:rail={railed[side]}
    class:holding={docks[side].length > 0}
    class:awaiting={railed[side] && docks.dragging}
    class:opening={slide[side] === 'opening'}
    class:closing={slide[side] === 'closing'}
    class:pinned={contentSize[side] !== null}
    style:--content={contentSize[side] !== null ? `${contentSize[side]}px` : null}
    onpointerdown={(e) => drags.startMarquee(e)}
  >
    <Dock {docks} {side} content={panelBody} railed={railed[side]} />
  </aside>
{/snippet}

{#snippet dockHandle(side)}
  <Resizable.Handle
    disabled={!docks.shown(side)}
    data-dock-hidden={docks.shown(side) ? undefined : ''}
    aria-label="{side === 'left' ? 'Left' : 'Right'} dock width"
    title={prefs.hints ? 'Drag to resize; double-click resets' : undefined}
    class="mx-[3px] bg-transparent transition-colors after:w-3 hover:bg-primary/60 data-[direction=vertical]:mx-0 data-[direction=vertical]:my-[3px] data-[direction=vertical]:after:h-3"
    ondblclick={() => dockPanes[side]?.resize(DOCK.default)}
    onDraggingChange={(dragging) => (dragFrom[side] = dragging ? dockSize[side] : null)}
  />
{/snippet}

<!-- A tooltip opens on hover and on keyboard focus, not on the focus a
     closing popover or menu hands back to its trigger. -->
<Tooltip.Provider delayDuration={300} ignoreNonKeyboardFocus>
  <div class="app" class:moving={drags.moving}>
    <!-- One row: what acts on the plots, then the page's own settings. -->
    <header class="top-bar" data-focus="toolbar" tabindex="-1">
      <span class="brand">Twinleaf Hub</span>
      <button class="search" data-keeps-selection onclick={() => (paletteOpen = true)}><RiSearch />Commands<kbd data-hint>Ctrl K</kbd></button>
      <span class="divider"></span>
      <Tip text={live ? 'Pause every plot' : 'Every plot follows live'}>
        {#snippet children(tip)}
          <Button {...tip} size="xs" variant={live ? 'outline' : 'default'} onclick={toggleLive}>
            {#if live}<RiPause />Live{:else}<RiPlay />Follow live{/if}
          </Button>
        {/snippet}
      </Tip>
      <!-- The time window: a slider over the spans, a step apiece, each step
           about a doubling or more, with the span it stands at named. -->
      <Popover.Root>
        <Tip text="The time window the plots show">
          {#snippet children(tip)}
            <Popover.Trigger {...tip}>
              {#snippet child({ props })}<Button {...props} size="xs" variant="outline" class="window-button" aria-label="Time window" data-keeps-selection>{windowText(span)} s<RiChevron /></Button>{/snippet}
            </Popover.Trigger>
          {/snippet}
        </Tip>
        <Popover.Content class="axis-pop w-auto gap-0 p-1.5" align="start" sideOffset={BAR_OFFSET}>
          <div class="axis-row">
            <span>Window</span>
            <WindowSlider value={span} onchange={setSpan} onstart={() => viewHistory.begin()} onend={() => viewHistory.end()} widest={hub.liveSeconds()} />
          </div>
        </Popover.Content>
      </Popover.Root>
      <IconButton icon={RiEraser} label="Clear the cursors" data-keeps-selection onclick={() => clearCursors()} />
      <DropdownMenu.Root>
        <Tip text="Arrange this tab's plots">
          {#snippet children(tip)}
            <DropdownMenu.Trigger {...tip}>
              {#snippet child({ props })}<Button {...props} size="xs" variant="ghost"><RiGrid />Layout</Button>{/snippet}
            </DropdownMenu.Trigger>
          {/snippet}
        </Tip>
        <!-- Each arrangement beside a picture of it, as the plot menus lead with icons. -->
        <DropdownMenu.Content class="menu-icons w-max" align="end" sideOffset={BAR_OFFSET}>
          <!-- A check on the arrangement the tab is in now, if any. -->
          {@const now = ws.arrangement()}
          <DropdownMenu.Label>Arrange this tab<span class="menu-value">{plotsText(plotIds(ws.layout).length)}</span></DropdownMenu.Label>
          {#each ARRANGEMENTS as [name, label]}
            {@const Icon = ARRANGE_ICONS[name]}
            <DropdownMenu.Item onSelect={() => history.group(`Arrange as ${label}`, () => ws.arrange(name))}><Icon class="menu-icon" />{label}{#if name === now}<span class="menu-value" aria-label="The layout now"><RiCheck /></span>{/if}</DropdownMenu.Item>
          {/each}
          <DropdownMenu.Separator />
          <DropdownMenu.Item onSelect={() => history.group('Reset', () => ws.reset())}><RiReset class="menu-icon" />Reset to default</DropdownMenu.Item>
        </DropdownMenu.Content>
      </DropdownMenu.Root>
      <DropdownMenu.Root>
        <Tip text="Export or import the tabs">
          {#snippet children(tip)}
            <DropdownMenu.Trigger {...tip}>
              {#snippet child({ props })}<Button {...props} size="xs" variant="ghost" data-keeps-selection><RiDownload />File</Button>{/snippet}
            </DropdownMenu.Trigger>
          {/snippet}
        </Tip>
        <DropdownMenu.Content class="menu-icons w-max" align="end" sideOffset={BAR_OFFSET}>
          <DropdownMenu.Item onSelect={exportTabs}><RiDownload class="menu-icon" />Export every tab…<span class="menu-value">{counted(ws.tabs.length, 'tab')}</span></DropdownMenu.Item>
          <DropdownMenu.Item onSelect={() => importInput.click()}><RiUpload class="menu-icon" />Import tabs…</DropdownMenu.Item>
          <DropdownMenu.Separator />
          {@const exports = selection.pageTargets('view').length}
          <DropdownMenu.Item onSelect={() => openExport(selection.pageTargets('view'))}><RiExport class="menu-icon" />Export data…<span class="menu-value">{selection.plots.size ? `${exports} selected` : plotsText(exports)}</span></DropdownMenu.Item>
        </DropdownMenu.Content>
      </DropdownMenu.Root>
      <input type="file" accept="application/json,.json" hidden bind:this={importInput} onchange={importTabs} />
      {#if importError}<span class="text-xs text-destructive">{importError}</span>{/if}
      <Popover.Root>
        <Tip text="Display settings">
          {#snippet children(tip)}
            <Popover.Trigger {...tip}>
              {#snippet child({ props })}<Button {...props} size="icon-xs" variant="ghost" aria-label="Display settings"><RiSettings /></Button>{/snippet}
            </Popover.Trigger>
          {/snippet}
        </Tip>
        <!-- A grouped form: each section a heading over rows of a label and
             its control, the labels in one column and the controls in the
             other (plot.css, .settings-pop). -->
        <Popover.Content class="w-80 gap-0 p-0" align="end" sideOffset={BAR_OFFSET}>
          <div class="settings-pop">
            <section aria-labelledby="set-display">
              <h3 id="set-display">Display</h3>
              <div class="set-grid">
                <span>Theme</span>
                <!-- The theme as the icons every app shows it by: a sun, a moon, a screen for the system's. -->
                <div class="seg theme-seg justify-self-end" role="radiogroup" aria-label="Theme">
                  {#each THEMES as [value, label]}
                    {@const Icon = THEME_ICONS[value]}
                    <Tip text={label}>
                      {#snippet children(tip)}<button {...tip} role="radio" aria-checked={prefs.theme === value} aria-label={label} class:on={prefs.theme === value} onclick={() => prefs.setTheme(value)}><Icon /></button>{/snippet}
                    </Tip>
                  {/each}
                </div>
                <span>Trace width</span>
                <div class="set-control">
                  <Slider type="single" value={prefs.traceWidth} min={TRACE_WIDTH.min} max={TRACE_WIDTH.max} step={TRACE_WIDTH.step} onValueChange={(px) => prefs.setTraceWidth(px)} label="Trace width" />
                  <span class="set-value">{prefs.traceWidth} px</span>
                </div>
                <label for="set-hints">Shortcut hints</label>
                <Checkbox id="set-hints" class="justify-self-end" checked={prefs.hints} onCheckedChange={(on) => prefs.setHints(on)} />
              </div>
            </section>
            <section aria-labelledby="set-stream">
              <h3 id="set-stream">Streaming</h3>
              <div class="set-grid">
                <span>Delay</span>
                <div class="set-control">
                  <Slider type="single" value={delayMs} min={0} max={500} step={10} onValueChange={setDelay} label="Delay" />
                  <span class="set-value">{delayMs} ms</span>
                </div>
                <span>Decimation</span>
                <Select.Root type="single" value={String(hub.session.pps)} onValueChange={(v) => api.setPps(+v)}>
                  <Select.Trigger size="sm" class="w-full text-xs" aria-label="Decimation">{hub.session.pps} pts/s per trace</Select.Trigger>
                  <Select.Content>
                    {#each DECIMATIONS as p}<Select.Item value={String(p)} label={`${p} pts/s per trace`} />{/each}
                  </Select.Content>
                </Select.Root>
              </div>
            </section>
            {#if hub.session.memory}
              <section aria-labelledby="set-history">
                <h3 id="set-history">History</h3>
                <HistorySettings memory={hub.session.memory} />
              </section>
            {/if}
            <section aria-labelledby="set-renderer">
              <h3 id="set-renderer">Renderer</h3>
              <dl class="set-grid">
                <dt>Frames</dt><dd class="set-value">{hud.fps.toFixed(0)} fps · {hud.frameMs.toFixed(2)} ms CPU</dd>
                <dt>Drawn</dt><dd class="set-value">{hud.segments.toLocaleString()} segments · {hud.drawCalls} calls</dd>
                <dt>Received</dt><dd class="set-value">{hud.rxKBs.toFixed(0)} kB/s</dd>
                <dt>To the GPU</dt><dd class="set-value">{hud.uploadKBs.toFixed(1)} kB/s</dd>
              </dl>
            </section>
          </div>
        </Popover.Content>
      </Popover.Root>
    </header>

    {#key narrow.current}
    <div class="docked" class:narrow={narrow.current}>
    <Resizable.PaneGroup direction={narrow.current ? 'vertical' : 'horizontal'} class="layout" bind:ref={dockGroup} onLayoutChange={docksResized}>
      <Resizable.Pane id="dock-left" order={1} bind:this={dockPanes.left} defaultSize={startSize('left')} minSize={DOCK.min} maxSize={DOCK.max} collapsible collapsedSize={railPct.left} onResize={(size) => dockCollapsed('left', size)} class="flex min-h-0">
        {@render dockFrame('left')}
      </Resizable.Pane>
      {@render dockHandle('left')}
      <Resizable.Pane id="tiles" order={2} defaultSize={100 - startSize('left') - startSize('right')} class="flex min-h-0 min-w-0">
        <main class="flex min-h-0 min-w-0 flex-1 flex-col gap-1.5">
          <TabStrip {ws} {drags} onClose={closeTab} onReorder={(ids) => history.group('Move tab', () => ws.orderTabs(ids))} />
          <div class="tiles" data-focus="plots" tabindex="-1" class:has-max={ws.maximized} class:drop-target={drags.drop?.outer && !drags.drop.edge}>
            {#key ws.activeId}
              {#if ws.layout}
                <Tiles layout={ws.layout} leafContent={plotTile} drop={drags.drop} maximized={ws.maximized} lifted={drags.moving?.group} landed={drags.landed} onResize={(path, ratio) => history.group('Resize', () => ws.setRatio(path, ratio))} />
              {:else}
                <Tip text="Or drop a signal anywhere here">
                  {#snippet children(tip)}<Button {...tip} class="m-auto" size="sm" variant="outline" onclick={() => newPlotWith([])}><RiAdd />Add a plot</Button>{/snippet}
                </Tip>
              {/if}
            {/key}
          </div>
        </main>
      </Resizable.Pane>
      {@render dockHandle('right')}
      <Resizable.Pane id="dock-right" order={3} bind:this={dockPanes.right} defaultSize={startSize('right')} minSize={DOCK.min} maxSize={DOCK.max} collapsible collapsedSize={railPct.right} onResize={(size) => dockCollapsed('right', size)} class="flex min-h-0">
        {@render dockFrame('right')}
      </Resizable.Pane>
    </Resizable.PaneGroup>
    </div>
    {/key}

    <StatusBar {hub} {hud} onKeys={() => (keysOpen = true)} />
  </div>
</Tooltip.Provider>

<DragOverlay {drags} {ws} session={hub.session} />
<FocusRing />
<Toasts />
<KeysDialog bind:open={keysOpen} />
<ExportDialog bind:what={exporting} />
<CommandPalette bind:open={paletteOpen} session={hub.session} {ws} {act} />

<AlertDialog.Root open={!!closing} onOpenChange={(o) => !o && (closing = null)}>
  <AlertDialog.Content>
    <AlertDialog.Header>
      <AlertDialog.Title>Close “{closing?.name}”?</AlertDialog.Title>
      <AlertDialog.Description>
        {@const n = plotIds(closing?.layout ?? null).length}
        {n === 1 ? 'Its plot goes' : `Its ${n} plots go`} with it. Export first if you want to keep the layout.
      </AlertDialog.Description>
    </AlertDialog.Header>
    <AlertDialog.Footer>
      <AlertDialog.Cancel>Keep it</AlertDialog.Cancel>
      <AlertDialog.Action onclick={confirmClose}>Close tab</AlertDialog.Action>
    </AlertDialog.Footer>
  </AlertDialog.Content>
</AlertDialog.Root>
