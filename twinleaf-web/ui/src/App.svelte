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
  import { getContext, onDestroy, onMount, setContext, tick, untrack } from 'svelte';
  import { MediaQuery, SvelteSet } from 'svelte/reactivity';
  import { ViewHistory } from './lib/viewhistory.svelte.js';
  import { CursorEdits } from './lib/cursors.js';
  import { Drags } from './lib/drag.svelte.js';
  import { Renderer } from './lib/gl.js';
  import { plotIds } from './lib/layout.js';
  import { exportText, importText, newPlot, saveTabs } from './lib/tabs.js';
  import { ARRANGEMENTS, DECIMATIONS } from './lib/constants.js';
  import { claim } from './lib/keyrouter.js';
  import { commands as plotCommands } from './lib/commands.js';
  import CommandItem from './lib/CommandItem.svelte';
  import { escapeTyped, keepsKey, plotPickers } from './lib/keys.js';
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
  import { ROWS, SIGNAL } from './lib/selection.svelte.js';
  import SignalList, { matching } from './lib/SignalList.svelte';
  import TabStrip from './lib/TabStrip.svelte';
  import DragOverlay from './lib/DragOverlay.svelte';
  import FocusRing from './lib/FocusRing.svelte';
  import Toasts from './lib/Toasts.svelte';
  import { noticeOf, toast } from './lib/toast.svelte.js';
  import { DeviceWrites } from './lib/writes.svelte.js';
  import WriteNotice from './lib/WriteNotice.svelte';
  import { MAX_COPY_ROWS, blockTable, lostSeconds, writeTables } from './lib/copy.js';
  import { timeAxis } from './lib/plot.js';
  import { navigate, remember } from './lib/focus.js';
  import KeysDialog from './lib/KeysDialog.svelte';
  import ExportDialog from './lib/ExportDialog.svelte';
  import WindowField from './lib/WindowField.svelte';
  import { WINDOW_LIMITS } from './lib/window.js';
  import StatusBar from './lib/StatusBar.svelte';
  import HistorySettings from './lib/HistorySettings.svelte';
  import CommandPalette from './lib/CommandPalette.svelte';
  import IconButton from './lib/IconButton.svelte';
  import Tip from './lib/Tip.svelte';
  import { Button } from '$lib/components/ui/button/index.js';
  import { Input } from '$lib/components/ui/input/index.js';
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
  import * as clock from './lib/clock.js';
  import './lib/scrolling.js';

  // The hub, the tabs as the page edits them, their undo and redo, the
  // PlotViews on the page, which the frame loop draws, and each plot's view,
  // as main.js makes them: the panels reach the History to make a command
  // one step.
  const { hub, ws, history, registry, views } = getContext('page');
  setContext('history', history);
  views.onChange = viewChanged;
  // The plots' cursors, which are edits: placing, moving or removing any is one undo step (cursors.js).
  const cursors = new CursorEdits(ws, history);
  const viewHistory = new ViewHistory(ws, views, { notice: (text) => toast.warning(text), edit: (label, fn) => history.group(label, fn), cursors });
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
  // (writes.svelte.js). No device write is an undo step, Revert writes
  // only to the devices checked in the Devices panel, and while it is
  // offered Ctrl+Z takes back no edit made before the write.
  const writes = new DeviceWrites(hub, {
    open: () => writeNotice.open(),
    close: () => writeNotice.close(),
    report: (level, text, detail) => toast[level](text, [], detail),
  }, { checked: (route) => !unchecked.has(route), edits: () => history.undos });
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

  // The plot commands (commands.js), which every menu row, key, button and
  // the palette run, over the plots of the open tab and what writes them.
  const commands = plotCommands({
    targets: (on) => selection.targets(on),
    pageTargets: (scope) => selection.pageTargets(scope),
    signalTargets: (key) => selection.signalTargets(key),
    plotOf: (id) => ({ id, plot: ws.plots[id], view: views.of(id), pv: [...registry].find((p) => p.plotId === id) }),
    last: () => ws.focusedId,
    ws,
    selection,
    viewHistory,
    cursors,
    toast,
    edit: (ids, what, fn) => eachPlot(ids, what, fn),
    split: (ids, dir, fromKeys) => splitPlots(ids, dir, fromKeys),
    close: (ids, fromKeys) => closePlots(ids, fromKeys),
    newPlot: (keys) => newPlotWith(keys),
    copy: (plots) => copyData(plots),
    copyCheck: (plots) => copyCheck(plots),
    openExport: (plots) => openExport(plots),
    picker: (id, action) => !!plotPickers.get(id)?.(action),
  });
  setContext('commands', commands);
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
    // A plot's kind is what one is looking at too: switching it is a view step, which Back crosses.
    viewHistory.step(() =>
      history.group(ids.length > 1 ? `${what} (${plotsText(ids.length)})` : what, () => {
        for (const i of ids) {
          const patch = fn(ws.plots[i]);
          if (patch) ws.setPlot(i, patch);
        }
      }),
    );
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
  /** Reads the toolbar's window: what the first view it acts on can show of its window, which grows as a hub fills. */
  function readWindow() {
    const vs = chosen();
    if (vs.length) span = vs[0].windowShown(clock.now());
  }
  /** The widest window the first view the toolbar acts on can show now. */
  const widestWindow = () => chosen()[0]?.spans(clock.now())[1] ?? WINDOW_LIMITS[1];
  /** Shows the views' state in the toolbar, and keeps every paused view's points. */
  function refreshToolbar() {
    live = tabViews().every((v) => v.live);
    readWindow();
    // A paused time plot in a tab holds its points: a paused spectrum holds
    // the spectrum it shows, and a closed plot nothing.
    hub.hold(views.anyPaused((id) => plotAnywhere(id)?.kind === 'time'));
  }
  // Paused, every pan or zoom asks the server for the plot's view at full
  // resolution, read from its raw buffer, which reaches back as far as the
  // history kept. Live, the decimated stream is used.
  // Every move asks, and the session paces the reads (`Session.fetchWindow`).
  /**
   * The share of the view's span asked for either side of it, so a pan or a
   * zoom out stays within the window read until the next one lands.
   */
  const DETAIL_PAD = 1;
  const overviewKeys = new Map(); // plot id -> what its overview was read for
  function requestDetail(id) {
    const v = views.get(id);
    if (!v || v.live) {
      overviewKeys.delete(id);
      return hub.session.clearWindows(id);
    }
    requestOverview(id, v);
    const plot = plotAnywhere(id);
    if (!plot || plot.kind !== 'time') return;
    const [x0, x1] = v.range(hub.now(clock.now()));
    hub.session.fetchWindow(id, 'detail', tracesOf(plot), x0, x1, DETAIL_PAD * (x1 - x0), window.innerWidth * 2);
  }
  const tracesOf = (plot) => plot.items.map((i) => hub.session.byKey.get(i.key)).filter(Boolean);
  // Each view reads the oldest time its plot can show from what the hub holds of these.
  views.tracesOf = (id) => tracesOf(plotAnywhere(id) ?? { items: [] });
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
    hub.session.fetchWindow(id, 'overview', traces, lo, hi, 0, window.innerWidth * 2);
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
    clock.after(1000, () => URL.revokeObjectURL(url));
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
  /** The words every property row shown has in its name: one filter for every panel of properties. */
  let propertyFilter = $state('');
  /** Escape in the filter's field clears what is typed, before it does anything else. */
  const filterKey = (e) => escapeTyped(e, propertyFilter, '', () => (propertyFilter = ''));
  /** Whether dock `side` has a panel of properties, open or folded, and so holds the filter's field, in its top row. */
  const holdsFilter = (side) => docks[side].some((p) => PANELS[p.id].channels);
  /** What the Signals list's search holds: the signals it shows are those that match. */
  let signalQuery = $state('');
  // The rows the page shows, in page order, which a Shift pick runs along:
  // each open panel of rows or action buttons, and the Signals list, of the
  // docks shown, left then right, top to bottom.
  selection.rowOrder = () =>
    SIDES.filter((side) => docks.shown(side))
      .flatMap((side) => docks[side].filter((p) => p.open))
      .flatMap(({ id }) =>
        SIGNAL === `${id}:`
          ? matching(hub.session.signals, signalQuery).map((t) => SIGNAL + t.key)
          : PANELS[id].channels
            ? panelRows(checked, PANELS[id].channels, history.favorites, propertyFilter).map((r) => `${id}:${r.name}`)
            : [],
      );

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
  /**
   * The panes are laid out anew, left, tiles and right, a hidden dock's
   * collapsed to its rail. A dock's handle, moving its pane, sets its width;
   * anything else the pane library lays out, as it does from the panes'
   * first sizes when a rail's size changes, is put back to the docks'
   * widths, unless a dock is sliding to its own.
   */
  function docksResized(sizes) {
    for (const [side, size] of [['left', sizes[0]], ['right', sizes[2]]]) {
      if (byHandle(side)) {
        if (docks.shown(side) && size >= DOCK.min) keepDockSize(side, size);
        continue;
      }
      const target = docks.shown(side) ? dockSize[side] : railPct[side];
      if (settled && !slide[side] && Math.abs(size - target) >= 0.01) dockPanes[side]?.resize(target);
    }
  }
  /**
   * Whether dock `side`'s handle is moving its pane: a drag of it under way
   * in the pane group it began in, or the keys on it. A group going, as the
   * window's narrowness changes, lays its panes out from their first sizes,
   * which no handle moved.
   */
  const byHandle = (side) => dragFrom[side]?.narrow === narrow.current || (!!dockHandles[side] && document.activeElement === dockHandles[side]);
  /** Keeps `size` as dock `side`'s width, the one it shows at. */
  function keepDockSize(side, size) {
    dockSize[side] = size;
    try {
      localStorage.setItem(DOCK.keys[side], String(size));
    } catch {}
  }
  /** Each dock's handle, the element. */
  const dockHandles = $state({ left: null, right: null });
  /**
   * Each dock as a drag of its handle began, {width, hidden, narrow, ends}:
   * its width, which it opens to again when that drag collapsed it, and
   * which Escape puts back with whether it was hidden; the window's
   * narrowness, which the pane group it drags in is keyed on; and what ends
   * the drag's hold on Escape. Null while none is under way.
   */
  const dragFrom = { left: null, right: null };
  /**
   * While its handle moves its pane, dock `side` follows the pane: hidden
   * once the pane collapses to its rail, below the least width, to open
   * again at the width it had as a drag began; shown at the width the pane
   * opens to.
   */
  function dockResized(side, size) {
    if (!byHandle(side)) return;
    if (size < DOCK.min) {
      if (!docks.shown(side)) return;
      if (dragFrom[side] !== null) keepDockSize(side, dragFrom[side].width);
      docks.hide(side, true);
    } else if (!docks.shown(side) && docks[side].length) {
      keepDockSize(side, size);
      docks.hide(side, false);
    }
  }
  /**
   * A drag of dock `side`'s handle begins or ends, as the pane library says.
   * It ends where the pointer is let go, on the page or off it, or where it
   * is as the window loses focus, which then hears no release; Escape ends
   * it where it began. Every end goes through the pane library's release,
   * which takes its resize cursor off the page. A drag lasts no longer than
   * the pane group it drags in (`$effect.pre` below), and a press that
   * begins one ends any other of the same handle.
   */
  function dockDragging(side, dragging) {
    dragFrom[side]?.ends();
    dragFrom[side] = null;
    if (!dragging) return;
    const from = { width: dockSize[side], hidden: !docks.shown(side), narrow: narrow.current };
    const unclaim = claim('drag', { on: { cancel: () => (putBack(side, from), release(), from.ends()) } });
    const blur = () => (release(), from.ends());
    window.addEventListener('blur', blur);
    from.ends = () => {
      unclaim();
      window.removeEventListener('blur', blur);
      window.removeEventListener('pointerup', from.ends);
    };
    dragFrom[side] = from;
  }
  /**
   * Puts dock `side` back as a drag of its handle found it, `from`: at its
   * width, hidden or shown, in the panes there are now. With none, as the
   * page has gone, there is nothing to put back.
   */
  function putBack(side, from) {
    const pane = dockPanes[side];
    if (!pane) return;
    keepDockSize(side, from.width);
    pane.resize(from.hidden ? railPct[side] : from.width);
    docks.hide(side, from.hidden);
  }
  /** The pane library's release of a drag of a handle, as the pointer let go. */
  const release = () => window.dispatchEvent(new MouseEvent('mouseup'));
  // The pane group lives as long as the page and the window's narrowness
  // (the `{#key}` about it): before it goes, a drag of its handles ends
  // where it is, released while its handle still hears it, so no drag
  // outlives the panes it moves. Escape still puts the dock back until the
  // pointer that held the drag is let go, as during the drag.
  $effect.pre(() => {
    void narrow.current;
    return () => {
      const held = ['left', 'right'].map((side) => dragFrom[side]).filter(Boolean);
      dragFrom.left = dragFrom.right = null;
      if (held.length) release();
      for (const from of held) window.addEventListener('pointerup', from.ends);
    };
  });
  /** Puts dock `side` back to the width it starts at, showing it if hidden. */
  function resetDock(side) {
    keepDockSize(side, DOCK.default);
    if (docks.shown(side)) dockPanes[side]?.resize(DOCK.default);
    else if (docks[side].length) docks.hide(side, false);
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
  // midpoint, and opening, they fade in after it. The dock's button is
  // placed from the frame's outer edge, so it stays where it is throughout.
  let dockGroup = $state(null);
  /** The room the panes share, in px: the group less its handles. */
  let paneRoom = $state(0);
  /** A rail's width in px: one holding panels has its dock's button, 6 px in from its border each side; an empty one is a strip to drop on, wider while a panel is dragged. */
  const RAIL_PX = { panels: 34, empty: 8, awaiting: 56 };
  const railPx = (side) => (docks.dragging ? RAIL_PX.awaiting : docks[side].length ? RAIL_PX.panels : RAIL_PX.empty);
  /** A side's rail as a share of the panes' room, the size its pane collapses to. */
  const railShare = (side) => (paneRoom ? (railPx(side) / paneRoom) * 100 : 0);
  const railLeft = $derived(railShare('left')), railRight = $derived(railShare('right'));
  /**
   * Each side's rail share. Each is a number of its own, so a pane, which
   * lays the panes out anew when told the size it collapses to, is told only
   * when that size changes, not whenever a panel folds or moves.
   */
  const railPct = {
    get left() {
      return railLeft;
    },
    get right() {
      return railRight;
    },
  };
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
        clock.cancel(slideDone);
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
          slideDone = clock.after(ms + 50, () => {
            dockGroup?.classList.remove('sliding');
            railed = { left: !docks.shown('left'), right: !docks.shown('right') };
            slide = { left: null, right: null };
            contentSize = { left: null, right: null };
          });
        };
        clock.frame(() => clock.frame(start));
        clock.after(SLIDE_START_MS, start);
      }
      if (paneRoom) settled = true;
    });
  });

  function newPlotWith(keys) {
    history.group('New plot', () => ws.addPlot(newPlot('time', 'overlay', keys)));
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
    let frames = 0, cpu = 0, last = clock.now(), lastBytes = 0, upload = 0, raf;
    const loop = (perf) => {
      const start = clock.now();
      hub.session.tick(perf);
      renderer.begin();
      for (const pv of registry) pv.draw(perf);
      const st = renderer.stats;
      upload += st.uploadBytes;
      cpu += clock.now() - start;
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
        readWindow();
      }
      raf = clock.frame(loop);
    };
    raf = clock.frame(loop);
    // A hot reload unmounts the page without leaving it, so the socket, the
    // GPU context and the window reads go with the copy that made them.
    return () => {
      clock.cancel(raf);
      hub.close();
      renderer.dispose();
    };
  });

  /** Sets the window of the views the toolbar acts on: a window typed is a step of its own, the field's arrows and wheel one burst. */
  function setSpan(v, typed) {
    const vs = chosen(), set = () => vs.forEach((vw) => vw.setSpan(v));
    if (typed) viewHistory.step(set);
    else viewHistory.burst('window', vs, set);
  }
  /**
   * The toolbar's Live button: pauses every time plot of the tab while all
   * follow live, as one view step, or has them all follow live back at
   * their windows, as End does (ViewHistory `follow`).
   */
  function toggleLive() {
    const ids = ws.timeIds(), perf = clock.now();
    if (live) viewHistory.step(() => ids.forEach((id) => views.of(id).pause(perf)));
    else viewHistory.follow(ids, true);
    refreshToolbar();
  }
  function setDelay(ms) {
    delayMs = ms;
    views.setDelay(ms / 1000);
  }
  const act = { toggleDock: (side) => docks.hide(side, docks.shown(side)), dockShown: (side) => docks.shown(side), exportTabs, importTabs: () => importInput.click() };

  let paletteOpen = $state(false);
  let keysOpen = $state(false);
  // The export the dialog offers, or null: plots' signals, the time they
  // show and the time the history holds of them.
  let exporting = $state(null);
  /** Offers to export the signals of `targets`, plots as commands.js gives them. */
  function openExport(targets) {
    const session = hub.session, now = hub.now(clock.now());
    const plots = targets.filter((t) => t.plot.items.length);
    const traces = [...new Map(plots.flatMap(({ plot }) => plot.items.map((i) => session.byKey.get(i.key)).filter(Boolean)).map((t) => [t.key, t])).values()];
    if (!traces.length) return toast.warning('Nothing to export: the plots show no signals');
    const ranges = plots.filter((t) => t.plot.kind === 'time').map((t) => t.view.range(now));
    const shown = ranges.length ? [Math.min(...ranges.map((r) => r[0])), Math.max(...ranges.map((r) => r[1]))] : null;
    const oldest = Math.min(...traces.map((t) => session.memory?.oldest?.[t.meta.id] ?? Infinity));
    exporting = { hub: session.id, traces, shown, whole: [isFinite(oldest) ? oldest : -Infinity, now] };
  }

  /**
   * What Ctrl+C on `targets`, plots as commands.js gives them, would copy,
   * from the hub's count of the rows, as {state, rows, reason}: 'ok',
   * 'too-many' past what a paste holds, or 'partial' when history no longer
   * holds the window's start, the reason saying which.
   */
  async function copyCheck(targets) {
    const session = hub.session, now = hub.now(clock.now());
    let total = 0, lost = 0;
    for (const { plot, view } of targets) {
      if (!plot.items.length) continue;
      if (plot.kind === 'spectrum') {
        total += Math.max(0, ...plot.items.map((i) => session.byKey.get(i.key)?.spectrum?.f.length ?? 0));
        continue;
      }
      const [t0, t1] = view.range(now);
      const traces = plot.items.map((i) => session.byKey.get(i.key)).filter(Boolean);
      const replies = await Promise.all(traces.map((t) => api.rows({ hub: session.id, id: t.meta.id, t0, t1, limit: 0 })));
      total += replies.reduce((n, r) => n + r.count, 0);
      replies.forEach((r, i) => (lost = Math.max(lost, lostSeconds(t0, r.oldest, traces[i].meta.rate))));
    }
    if (total > MAX_COPY_ROWS) return { state: 'too-many', rows: total, reason: 'More than a paste holds: export instead' };
    if (lost) return { state: 'partial', rows: total, reason: `The first ${lost.toFixed(1)} s are no longer in history` };
    return { state: 'ok', rows: total };
  }

  /**
   * Copies the data of `targets`, plots as commands.js gives them, as
   * copy.js lays it out: a time plot's every raw sample in its visible
   * window, and a spectrum plot's spectra as it shows them, each signal's
   * density by frequency. Past MAX_COPY_ROWS it copies nothing and says how
   * many rows that would be; a window whose start the history no longer
   * holds is copied, and the notice says what is missing.
   */
  async function copyData(targets) {
    const session = hub.session, now = hub.now(clock.now());
    const plots = targets.filter((t) => t.plot.items.length);
    if (!plots.length) return toast.warning('Nothing to copy: the plots show no signals');
    const blocks = [];
    let total = 0, lost = 0;
    for (const { plot, view, pv } of plots) {
      if (plot.kind === 'spectrum') {
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
      const [t0, t1] = view.range(now);
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

  /** The plot `el` is in, by id, or null. */
  const plotAt = (el) => el?.closest?.('.leaf[data-plot]')?.dataset.plot ?? null;

  /**
   * Runs plot command `action` of key `e` (keymap.js, place 'plot') on the
   * plots it acts on (commands.js), and says whether it was taken: a command
   * with no plots to act on is not, and its key goes on.
   */
  function plotKey(action, e) {
    const how = { back: e.key === 'ArrowLeft' || e.key === 'ArrowDown', shift: e.shiftKey };
    return commands.run(action, { from: 'key', how });
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

  /**
   * Takes the last edit back, or with `redo` does it again. A plot's kind it
   * changes is a view step, as the command's is. Right after a device write
   * or action it may take nothing back, and a notice says why
   * (writes.svelte.js `undo`).
   */
  function undo(redo) {
    const edited = () => viewHistory.edited(() => (redo ? history.redo() : history.undo()));
    const { label } = redo ? { label: edited() } : writes.undo(history.undos.at(-1), edited);
    if (label) toast(`${redo ? 'Redo' : 'Undo'}: ${label}`);
    refreshToolbar();
  }

  // The page's keys, each a claimant of the key router, which holds their
  // order (keyrouter.js ORDER) and offers each the keys of its place in
  // keymap.js.
  const claimed = Object.entries({
    palette: { on: { palette: () => void (paletteOpen = !paletteOpen) } },
    // A key pressed in a plot is its plot command; but on a control inside
    // it, the arrows move between the controls, and what keeps its own keys
    // keeps them (keys.js).
    plot: {
      when(e, at) {
        const card = at?.closest?.('.plot-card');
        if (!card) return false;
        const inside = at !== card && !at.matches('.trace-zone');
        return !inside || (!e.key.startsWith('Arrow') && !keepsKey(e, at));
      },
      on: plotKey,
    },
    restore: { when: () => !!ws.maximized, on: { escape: () => void ws.restoreLayout() } },
    deselect: { when: () => ws.selected.size > 0 || selection.rows.size > 0 || selection.signals.size > 0, on: { escape: () => void selection.clear() } },
    // Tab, the arrows, Enter, Escape and F6 move through the page's tree
    // (focus.js); a move that brings focus into another plot selects it.
    navigation: {
      on(action, e, at) {
        const from = plotAt(document.activeElement);
        if (!navigate(e, at)) return false;
        const to = plotAt(document.activeElement);
        if (to && to !== from && e.key !== 'Escape') selection.arrived(to);
      },
    },
    page: {
      on: {
        undo: () => undo(false),
        redo: () => undo(true),
        // With no text selected: selected text is the browser's to copy.
        copyData: () => !String(window.getSelection() ?? '') && void commands.run('copyData', { from: 'key' }),
        // Every plot of the tab, not the page's text.
        selectAll: () => void selection.only(ws.shownIds(), null),
        goToPlot(e) {
          // The plots as they show: only the maximized ones while some are.
          const id = ws.shownIds()[+e.key - 1];
          if (!id) return false;
          selection.arrived(id);
          focusPlot(id);
        },
        help: () => void (keysOpen = true),
        stepTab: (e) => ws.tabs.length > 1 && void stepTab(e.key === ']' ? 1 : -1),
      },
    },
    // Among property rows, which are what is picked there, a plot's keys do nothing.
    plots: { when: (e, at) => !at?.closest?.(ROWS), on: plotKey },
    pause: { on: { pause: () => void commands.run('pause', { from: 'key' }) } },
  }).map(([id, claimant]) => claim(id, claimant));
  onDestroy(() => claimed.forEach((unclaim) => unclaim()));
</script>

<!-- Nothing on the page drags natively: plots, signals and tabs move by
     pointer, and a native drag would only carry a picture of the page. -->
<svelte:window onfocusin={remember} onpointerdowncapture={(e) => selection.press(e)} oncontextmenu={nativeMenu} ondragstart={(e) => e.preventDefault()} />
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
        {cursors}
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
        onFocus={() => selection.point(id)}
        onPress={() => selection.pressOn(id)}
        onEach={(what, fn) => eachPlot(targetIdsOf(id), what, fn)}
        onEdit={(what, patch) => history.group(what, () => ws.setPlot(id, patch))}
        onView={(patch) => viewHistory.step(() => targetIdsOf(id).forEach((i) => ws.plots[i]?.kind === ws.plots[id]?.kind && ws.setView(i, patch)))}
        targetIds={() => targetIdsOf(id)}
        onStartMove={(e) => drags.startMove(id, e)}
        onMoveToTab={(tabId) => {
          const ids = targetIdsOf(id);
          history.group(`Move ${plotsText(ids.length)} to ${ws.tabs.find((t) => t.id === tabId)?.name ?? 'a tab'}`, () => {
            for (const i of ids) ws.movePlotToTab(i, tabId);
          });
        }}
        onTraceDragStart={() => drags.traceDragStart()}
        onLandSignal={(item, items) => drags.landSignal(item, items ? id : null, items)}
        otherPlots={plotIds(ws.layout)
          .filter((p) => p !== id)
          .map((p) => ({ id: p, label: labelOf(p) }))}
        onMoveSignal={(key, to) => moveSignal(id, key, to)}
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
      <SignalList session={hub.session} {drags} bind:query={signalQuery} />
    </div>
  {:else}
    {#key hub.session}<Properties devices={checked} channels={PANELS[id].channels} panel={id} filter={propertyFilter} {drags} />{/key}
  {/if}
{/snippet}

{#snippet dockTop()}
  <Input class="h-7 min-w-0 flex-1 text-xs" type="search" bind:value={propertyFilter} placeholder="Filter properties" aria-label="Filter the properties of every panel by name" data-rows onkeydown={filterKey} />
{/snippet}

{#snippet dockFrame(side)}
  <!-- The dock's frame, its rail too: it fills its pane, and its panels keep their open size while it slides. It is no wider than its pane, cuts its panels off and does not scroll, so focus that lands in them moves nothing. -->
  <aside
    class="dock-frame {side} flex min-h-0 min-w-0 flex-1 cursor-crosshair flex-col overflow-clip rounded-lg border bg-card"
    class:rail={railed[side]}
    class:holding={docks[side].length > 0}
    class:awaiting={railed[side] && docks.dragging}
    class:opening={slide[side] === 'opening'}
    class:closing={slide[side] === 'closing'}
    class:pinned={contentSize[side] !== null}
    style:--content={contentSize[side] !== null ? `${contentSize[side]}px` : null}
    onpointerdown={(e) => drags.startMarquee(e)}
  >
    <Dock {docks} {side} content={panelBody} top={holdsFilter(side) ? dockTop : undefined} railed={railed[side]} carrying={drags.carrying} />
  </aside>
{/snippet}

{#snippet dockHandle(side)}
  <Resizable.Handle
    bind:ref={dockHandles[side]}
    disabled={!docks[side].length}
    data-dock-empty={docks[side].length ? undefined : ''}
    aria-label="{side === 'left' ? 'Left' : 'Right'} dock width"
    title={prefs.hints ? 'Drag to resize; double-click resets' : undefined}
    class="mx-[3px] bg-transparent transition-colors after:w-3 hover:bg-primary/60 data-[direction=vertical]:mx-0 data-[direction=vertical]:my-[3px] data-[direction=vertical]:after:h-3"
    ondblclick={() => resetDock(side)}
    onDraggingChange={(dragging) => dockDragging(side, dragging)}
  />
{/snippet}

<!-- A tooltip opens on hover and on keyboard focus, not on the focus a
     closing popover or menu hands back to its trigger. -->
<Tooltip.Provider delayDuration={300} ignoreNonKeyboardFocus>
  <div class="app" class:moving={drags.moving || drags.carrying}>
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
      <!-- The time window the plots show, typed or stepped. -->
      <WindowField value={span} onchange={setSpan} widest={widestWindow} />
      <IconButton icon={RiEraser} label="Clear the cursors" data-keeps-selection data-command="clearCursors" onclick={() => commands.run('clearCursors', { from: 'toolbar' })} />
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
          <CommandItem id="exportData" menu="dropdown" />
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
      <Resizable.Pane id="dock-left" order={1} bind:this={dockPanes.left} defaultSize={startSize('left')} minSize={DOCK.min} maxSize={DOCK.max} collapsible collapsedSize={railPct.left} onResize={(size) => dockResized('left', size)} class="flex min-h-0">
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
      <Resizable.Pane id="dock-right" order={3} bind:this={dockPanes.right} defaultSize={startSize('right')} minSize={DOCK.min} maxSize={DOCK.max} collapsible collapsedSize={railPct.right} onResize={(size) => dockResized('right', size)} class="flex min-h-0">
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
<CommandPalette bind:open={paletteOpen} session={hub.session} {ws} {act} devices={checked} />

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
