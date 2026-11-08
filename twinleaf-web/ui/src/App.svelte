<script>
  // The page: a toolbar, a sidebar of devices and signals beside the layout
  // tabs and their plots, and the dialogs. What it shows comes from the hub
  // (hub.svelte.js), what it lays out from the workspace
  // (workspace.svelte.js), and every plot is drawn on one WebGL canvas
  // behind the page (gl.js), each by its PlotView (plot.js).
  //
  // Each time plot has a View of its own (views.js). A gesture on a plot acts
  // on every selected plot when the plot is one of them, or else on it alone.
  // The toolbar and Space act on the highlighted plots: the selection, or
  // else the plot clicked last, or else, after a click on empty background,
  // every plot. Space pauses spectra too; the toolbar sets time plots only.
  import { onMount, tick, untrack } from 'svelte';
  import { MediaQuery, SvelteSet } from 'svelte/reactivity';
  import { SHADOW_ITEM_MARKER_PROPERTY_NAME as SHADOW } from 'svelte-dnd-action';
  import { Hub } from './lib/hub.svelte.js';
  import { Workspace } from './lib/workspace.svelte.js';
  import { Views } from './lib/views.js';
  import { Drags } from './lib/drag.svelte.js';
  import { Renderer } from './lib/gl.js';
  import { plotIds } from './lib/layout.js';
  import { chip, exportText, importText, loadTabs, newPlot, saveTabs } from './lib/tabs.js';
  import { ARRANGEMENTS, DECIMATIONS, SPANS } from './lib/constants.js';
  import { actionFor } from './lib/keymap.js';
  import { keepsKey, plotKeys } from './lib/keys.js';
  import { api } from './lib/protocol.js';
  import { THEMES, TRACE_WIDTH, prefs } from './lib/prefs.svelte.js';
  import RiSun from '@lucide/svelte/icons/sun';
  import RiMoon from '@lucide/svelte/icons/moon';
  import RiSystem from '@lucide/svelte/icons/monitor';
  import PlotCard from './lib/PlotCard.svelte';
  import Tile from './lib/Tile.svelte';
  import Devices from './lib/Devices.svelte';
  import Properties from './lib/Properties.svelte';
  import Dock from './lib/Dock.svelte';
  import { Docks, PANELS } from './lib/docks.svelte.js';
  import { byRoute } from './lib/properties.js';
  import SignalList from './lib/SignalList.svelte';
  import TabStrip from './lib/TabStrip.svelte';
  import DragOverlay from './lib/DragOverlay.svelte';
  import FocusRing from './lib/FocusRing.svelte';
  import Toasts from './lib/Toasts.svelte';
  import { toast } from './lib/toast.svelte.js';
  import { MAX_COPY_ROWS, blockTable, lostSeconds, writeTables } from './lib/copy.js';
  import { timeAxis } from './lib/plot.js';
  import { leave, navigate, remember } from './lib/focus.js';
  import KeysDialog from './lib/KeysDialog.svelte';
  import ExportDialog from './lib/ExportDialog.svelte';
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
  import { pressOutsideKeepsFocus } from './lib/menus.js';
  import RiChevron from '@lucide/svelte/icons/chevron-down';

  const hub = new Hub();
  const ws = new Workspace(loadTabs(localStorage));
  const views = new Views(hub, viewChanged);
  const drags = new Drags(ws, refreshToolbar);
  drags.axisDropAt = (plotId, x, y) => [...registry].find((pv) => pv.plotId === plotId)?.axisDropAt(x, y) ?? null;
  /** The PlotViews on the page, which the frame loop draws. */
  const registry = new Set();
  /** The time under the pointer, which every time plot marks. */
  const hover = { t: null };
  let renderer = $state();
  let glCanvas;

  $effect(() => saveTabs(localStorage, ws.tabs, ws.activeId));
  // A plot that leaves every tab takes its view with it, so a paused view
  // of a closed plot never holds points.
  $effect(() => {
    views.prune(new Set(ws.allPlotIds()));
    untrack(refreshToolbar);
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

  const targetsOf = (id) => (ws.selected.has(id) ? [...ws.selected] : [id]).map((i) => views.of(i));
  /** The views the toolbar acts on: the highlighted time plots', or every time plot's. */
  function chosen() {
    const ids = ws.highlighted().filter((id) => ws.plots[id]?.kind === 'time');
    return (ids.length ? ids : ws.timeIds()).map((id) => views.of(id));
  }
  /** The plot of `id` in any tab. */
  const plotAnywhere = (id) => ws.tabs.find((t) => t.plots[id])?.plots[id];
  function viewChanged(id) {
    requestDetail(id);
    refreshToolbar();
  }
  /**
   * A press on the page's empty background, outside the tiles, clears the
   * selection and the highlighted plot, as the docks' own background does.
   * A press on anything that takes input, or inside a popup, keeps them.
   */
  const KEEPS_SELECTION =
    '.tiles, button, a, input, textarea, select, label, [role=button], [role=option], [role=tab], [role=menuitem], [role=slider], [role=listbox], [role=grid], [role=combobox], [contenteditable], [data-slot$=-content], .dock-head';
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
  function pressOnBackground(e) {
    if (e.button !== 0 || e.shiftKey || !(e.target instanceof Element) || e.target.closest(KEEPS_SELECTION)) return;
    if (!ws.selected.size && ws.focusedId === null) return;
    ws.selected.clear();
    ws.focusedId = null;
    refreshToolbar();
  }

  /** Every time plot's view in the tab, which the toolbar's Live button acts on. */
  const tabViews = () => ws.timeIds().map((id) => views.of(id));
  // The top bar's menus and popovers: each opens 2 px below the bar, as a
  // plot's header menus open below the header (its 24 px buttons sit 5 px
  // above the 34 px bar's foot), and a press on another's button opens it.
  const BAR_OFFSET = 7;
  const layoutMenu = pressOutsideKeepsFocus(), fileMenu = pressOutsideKeepsFocus(), settingsMenu = pressOutsideKeepsFocus(), windowMenu = pressOutsideKeepsFocus();
  /** The step of SPANS nearest window `s`, by ratio, as a zoom leaves it between them. */
  const spanStep = (s) => SPANS.reduce((best, x, i) => (Math.abs(Math.log(x / s)) < Math.abs(Math.log(SPANS[best] / s)) ? i : best), 0);
  /** Each theme's icon. */
  const THEME_ICONS = { light: RiSun, dark: RiMoon, system: RiSystem };
  /** Each arrangement's picture in the Layout menu. */
  const ARRANGE_ICONS = { rows: RiRows, columns: RiColumns, grid: RiGridArrange, quadrants: RiQuadrants, focus: RiFocusArrange };
  /** Shows the views' state in the toolbar, and keeps every paused view's points. */
  function refreshToolbar() {
    const vs = chosen();
    live = tabViews().every((v) => v.live);
    if (vs.length) span = +vs[0].span.toFixed(3);
    // A paused spectrum holds the spectrum it shows, not the points.
    hub.hold(views.anyPaused((id) => plotAnywhere(id)?.kind !== 'spectrum'));
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

  /** A click on plot `id` selects it alone; with Shift it adds the plot to
   * the selection or takes it out. A click on one of several selected plots
   * narrows the selection to it once the double-click time has passed, so a
   * double-click still acts on them all. Esc, or a click on empty space,
   * clears the selection. */
  let narrowing;
  const DOUBLE_CLICK_MS = 300;
  function select(id, shift, clicks) {
    clearTimeout(narrowing);
    const selected = ws.selected;
    const only = () => {
      selected.clear();
      selected.add(id);
      refreshToolbar();
    };
    if (shift) selected.has(id) ? selected.delete(id) : selected.add(id);
    else if (!selected.has(id)) only();
    else if (selected.size > 1 && clicks === 1) narrowing = setTimeout(only, DOUBLE_CLICK_MS);
    refreshToolbar();
  }

  // Closing a tab with plots asks first: `closing` is the tab the dialog
  // asks about, and shows it. The dialog's Close button closes the tab and
  // the dialog; its Cancel and Esc close the dialog only.
  let closing = $state(null);
  function closeTab(t) {
    if (plotIds(t.layout).length) closing = t;
    else ws.closeTab(t);
  }
  function confirmClose() {
    ws.closeTab(closing);
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
      ws.addTabs(importText(await file.text()));
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
    const shown = [docks.shown('left') && 'left', 'tiles', docks.shown('right') && 'right'].filter(Boolean);
    try {
      shown.forEach((side, i) => side !== 'tiles' && localStorage.setItem(DOCK.keys[side], String((dockSize[side] = sizes[i]))));
    } catch {}
  }

  function newPlotWith(keys) {
    ws.addPlot(newPlot('time', 'overlay', keys));
  }
  /** Moves keyboard focus to plot `id`, once it is on the page. */
  async function focusPlot(id) {
    if (!ws.plots[id]) return;
    ws.focusedId = id;
    await tick();
    document.querySelector(`.leaf[data-plot="${id}"] .plot-card`)?.focus();
  }
  function closePlot(id) {
    ws.closePlot(id);
    hub.session.clearWindows(id);
  }
  /** Closes plot `id` from the keys, and gives focus to the plot after it, or else before it. */
  function closeFromKeys(id) {
    const order = plotIds(ws.layout), i = order.indexOf(id);
    closePlot(id);
    const next = order[i + 1] ?? order[i - 1];
    if (next) focusPlot(next);
    else ws.focusedId = null;
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
    // A hot reload unmounts the page without leaving it, so the socket and
    // the GPU context go with the copy that opened them.
    return () => {
      cancelAnimationFrame(raf);
      hub.close();
      renderer.dispose();
    };
  });

  function setSpan(v) {
    for (const vw of chosen()) {
      vw.span = +v;
      vw.notify();
    }
  }
  function followLive() {
    for (const vw of chosen()) vw.follow();
  }
  /**
   * The toolbar's Live button: pauses every time plot of the tab while all
   * follow live, or has them all follow live. Space does the same for the
   * highlighted plots alone.
   */
  function toggleLive() {
    const perf = performance.now();
    for (const vw of tabViews()) live ? vw.pause(perf) : vw.follow();
    refreshToolbar();
  }
  function setDelay(ms) {
    delayMs = ms;
    views.setDelay(ms / 1000);
  }
  /** Clears the cursors of plots `ids`, by default the selection's or every plot's, and says whether any were placed. */
  function clearCursors(ids = ws.selected.size ? [...ws.selected] : plotIds(ws.layout)) {
    const placed = (c) => c && (c.a !== null || c.b !== null);
    let had = false;
    for (const id of ids) {
      const vw = views.get(id);
      if (vw) (had ||= placed(vw.cursors)), (vw.cursors = { a: null, b: null });
    }
    for (const pv of registry) if (ids.includes(pv.plotId)) had = pv.clearCursors() || had;
    return had;
  }
  const act = { toggleDock: (side) => docks.hide(side, docks.shown(side)), dockShown: (side) => docks.shown(side), addToFocused: (key) => ws.addToFocused(key), newPlot: newPlotWith, followLive, clearCursors: () => clearCursors(), exportTabs, importTabs: () => importInput.click() };

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
    if (!traces.length) return toast('Nothing to export: the plots show no signals');
    const ranges = plots.filter(([, p]) => p.kind === 'time').map(([id]) => views.of(id).range(now));
    const shown = ranges.length ? [Math.min(...ranges.map((r) => r[0])), Math.max(...ranges.map((r) => r[1]))] : null;
    const oldest = Math.min(...traces.map((t) => session.memory?.oldest?.[t.meta.id] ?? Infinity));
    exporting = { hub: session.id, traces, shown, whole: [isFinite(oldest) ? oldest : now - 600, now] };
  }

  /**
   * What Ctrl+C on plots `ids` would copy, from the hub's count of the rows,
   * as {state, text}: 'ok', 'too-many' past what a paste holds, or
   * 'partial' when history no longer holds the window's start.
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
    if (total > MAX_COPY_ROWS) return { state: 'too-many', text: `${total.toLocaleString()} rows, more than a paste holds: export instead` };
    if (lost) return { state: 'partial', text: `The first ${lost.toFixed(1)} s are no longer in history` };
    return { state: 'ok', text: `${total.toLocaleString()} rows` };
  }

  async function copyData(ids) {
    const session = hub.session, now = hub.now(performance.now());
    const plots = ids.map((id) => [id, plotAnywhere(id)]).filter(([, p]) => p?.items.length);
    if (!plots.length) return toast('Nothing to copy: the plots show no signals');
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
      return toast(`${total.toLocaleString()} rows is more than a paste holds well (${MAX_COPY_ROWS.toLocaleString()}); narrow the window, or export it`);
    const rows = blocks.reduce((n, b) => n + b.rows.length, 0);
    await writeTables(blocks);
    const missing = lost ? `; the first ${lost.toFixed(1)} s of the window are no longer in history` : '';
    toast(`Copied ${rows.toLocaleString()} rows of ${blocks.length} plot${blocks.length === 1 ? '' : 's'}${missing}`);
  }

  /** Moves signal `key` from plot `from` to plot `to`, or to a new plot to its right with null. */
  function moveSignal(from, key, to) {
    const src = ws.plots[from];
    if (!src) return;
    src.items = src.items.filter((i) => i.key !== key);
    if (to) {
      const dst = ws.plots[to];
      if (dst && !dst.items.some((i) => i.key === key)) dst.items.push(chip(key));
    } else {
      const p = newPlot(src.kind, 'overlay', [key]);
      ws.addPlot(p, from, 'row');
    }
  }

  function onKey(e) {
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') {
      e.preventDefault();
      paletteOpen = !paletteOpen;
      return;
    }
    // Tab, the arrows, Enter, Escape and F6 move through the page's tree
    // (focus.js). A plot acts on its own Escape first.
    if (navigate(e, (n) => n.matches('[data-focus=plot]'))) return;
    // What is typed into a field, a menu or dialog's own keys, and the keys
    // that press a button focused from the keyboard stay there (keys.js).
    if (keepsKey(e)) return;
    // Ctrl+C with no text selected copies the highlighted plots' data.
    if ((e.ctrlKey || e.metaKey) && !e.shiftKey && e.key.toLowerCase() === 'c' && !String(window.getSelection() ?? '')) {
      e.preventDefault();
      const ids = ws.highlighted().length ? ws.highlighted() : ws.timeIds();
      return void copyData(ids).catch((error) => toast(`Could not copy: ${error.message}`));
    }
    // Ctrl+A outside a field selects every plot of the tab, not the page's text.
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'a') {
      e.preventDefault();
      ws.selected.clear();
      for (const id of plotIds(ws.layout)) ws.selected.add(id);
      return refreshToolbar();
    }
    const action = actionFor('page', e);
    if (action === 'goToPlot') {
      const id = plotIds(ws.layout)[+e.key - 1];
      if (!id) return;
      e.preventDefault();
      return focusPlot(id);
    }
    if (action === 'help') return void (keysOpen = true);
    if (action === 'stepTab') {
      if (ws.tabs.length < 2) return;
      e.preventDefault();
      return stepTab(e.key === ']' ? 1 : -1);
    }
    if (action !== 'pause' && action !== 'escape') {
      // Every other key acts on the highlighted plot, wherever focus is.
      if (!action && !e.defaultPrevented) plotKeys.get(ws.focusedId)?.(e);
      return;
    }
    if (action === 'escape') {
      if (ws.maximized) ws.maximized = null;
      else if (ws.selected.size) {
        ws.selected.clear();
        refreshToolbar();
      } else if (!clearCursors()) leave();
      return;
    }
    // Space pauses the highlighted plots, or has them follow live again.
    e.preventDefault();
    const perf = performance.now(), vs = ws.highlighted().map((id) => views.of(id));
    if (vs.some((v) => v.live)) for (const v of vs) v.pause(perf);
    else for (const v of vs) v.follow();
  }
</script>

<!-- Nothing on the page drags natively: plots, signals and tabs move by
     pointer, and a native drag would only carry a picture of the page. -->
<svelte:window onkeydown={onKey} onfocusin={remember} onpointerdown={pressOnBackground} oncontextmenu={nativeMenu} ondragstart={(e) => e.preventDefault()} />
<canvas class="gl-layer" bind:this={glCanvas}></canvas>

{#snippet plotTile(id)}
  {#key id}
    {#if renderer && ws.plots[id]}
      <PlotCard
        plot={ws.plots[id]}
        {hub}
        view={views.of(id)}
        targets={() => targetsOf(id)}
        {hover}
        {renderer}
        {registry}
        otherTabs={ws.tabs.filter((t) => t.id !== ws.activeId && !t[SHADOW])}
        focused={ws.focusedId === id}
        selected={ws.selected.has(id)}
        onSelect={(shift, clicks) => select(id, shift, clicks)}
        maximized={ws.maximized === id}
        onFocus={() => {
          ws.focusedId = id;
          refreshToolbar();
        }}
        onRemove={(keys) => (keys ? closeFromKeys(id) : closePlot(id))}
        onStartMove={(e) => drags.startMove(id, e)}
        onSplit={(dir, keys) => {
          const p = newPlot('time', 'overlay', []);
          ws.addPlot(p, id, dir);
          if (keys) focusPlot(p.id);
        }}
        onMaximize={() => (ws.maximized = ws.maximized === id ? null : id)}
        onMoveToTab={(tabId) => ws.movePlotToTab(id, tabId)}
        onClearCursors={() => clearCursors(ws.selected.has(id) ? [...ws.selected] : [id])}
        onTraceDragStart={() => drags.traceDragStart()}
        onTraceDrop={(key) => drags.dockTrace(key)}
        onEmptied={() => queueMicrotask(() => closePlot(id))}
        otherPlots={plotIds(ws.layout)
          .filter((p) => p !== id)
          .map((p) => ({ id: p, label: ws.plots[p]?.items.map((i) => i.key).join(', ') || 'Empty plot' }))}
        onMoveSignal={(key, to) => moveSignal(id, key, to)}
        onCheckCopy={() => copyCheck(ws.selected.has(id) ? [...ws.selected] : [id])}
        onExport={() => openExport(ws.selected.has(id) ? [...ws.selected] : [id])}
        onCopy={() => copyData(ws.selected.has(id) ? [...ws.selected] : [id]).catch((error) => toast(`Could not copy: ${error.message}`))}
      />
    {/if}
  {/key}
{/snippet}

{#snippet panelBody(id)}
  {#if id === 'devices'}
    <Devices session={hub.session} {unchecked} />
  {:else if id === 'signals'}
    <SignalList session={hub.session} {drags} onAdd={(key) => ws.addToFocused(key)} onNewPlot={newPlotWith} />
  {:else}
    {#key hub.session}<Properties devices={checked} channels={PANELS[id].channels} />{/key}
  {/if}
{/snippet}

{#snippet dockFrame(side)}
  <aside class="flex min-h-0 flex-1 cursor-crosshair flex-col overflow-hidden rounded-lg border bg-card p-1.5" onpointerdown={(e) => drags.startMarquee(e)}>
    <Dock {docks} {side} content={panelBody} />
  </aside>
{/snippet}

{#snippet dockHandle(side)}
  <Resizable.Handle
    aria-label="{side === 'left' ? 'Left' : 'Right'} dock width"
    title={prefs.hints ? 'Drag to resize; double-click resets' : undefined}
    class="mx-[3px] bg-transparent transition-colors after:w-3 hover:bg-primary/60 data-[direction=vertical]:mx-0 data-[direction=vertical]:my-[3px] data-[direction=vertical]:after:h-3"
    ondblclick={() => dockPanes[side]?.resize(DOCK.default)}
  />
{/snippet}

<!-- A tooltip opens on hover and on keyboard focus, not on the focus a
     closing popover or menu hands back to its trigger. -->
<Tooltip.Provider delayDuration={300} ignoreNonKeyboardFocus>
  <div class="app" class:moving={drags.moving}>
    <!-- One row: what acts on the plots, then the page's own settings. -->
    <header class="top-bar" data-focus="toolbar" tabindex="-1">
      <span class="brand">Twinleaf Hub</span>
      <button class="search" onclick={() => (paletteOpen = true)}><RiSearch />Commands{#if prefs.hints}<kbd>Ctrl K</kbd>{/if}</button>
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
              {#snippet child({ props })}<Button {...props} size="xs" variant="outline" class="window-button" aria-label="Time window">{span} s<RiChevron /></Button>{/snippet}
            </Popover.Trigger>
          {/snippet}
        </Tip>
        <Popover.Content class="window-pop w-72 gap-0 p-3" align="start" sideOffset={BAR_OFFSET} {...windowMenu}>
          <div class="window-head"><span>Window</span><span class="set-value">{span} s</span></div>
          <Slider type="single" value={spanStep(span)} min={0} max={SPANS.length - 1} step={1} onValueChange={(i) => setSpan(SPANS[i])} label="Time window" />
          <div class="window-ticks" aria-hidden="true">{#each SPANS as s}<span>{s}</span>{/each}</div>
        </Popover.Content>
      </Popover.Root>
      <IconButton icon={RiEraser} label="Clear the cursors" keys={['Esc']} onclick={() => clearCursors()} />
      <DropdownMenu.Root>
        <Tip text="Arrange this tab's plots">
          {#snippet children(tip)}
            <DropdownMenu.Trigger {...tip}>
              {#snippet child({ props })}<Button {...props} size="xs" variant="ghost"><RiGrid />Layout</Button>{/snippet}
            </DropdownMenu.Trigger>
          {/snippet}
        </Tip>
        <!-- Each arrangement beside a picture of it, as the plot menus lead with icons. -->
        <DropdownMenu.Content class="menu-icons w-max" align="end" sideOffset={BAR_OFFSET} {...layoutMenu}>
          <DropdownMenu.Label>Arrange this tab</DropdownMenu.Label>
          {#each ARRANGEMENTS as [name, label]}
            {@const Icon = ARRANGE_ICONS[name]}
            <DropdownMenu.Item onSelect={() => ws.arrange(name)}><Icon class="menu-icon" />{label}</DropdownMenu.Item>
          {/each}
          <DropdownMenu.Separator />
          <DropdownMenu.Item onSelect={() => ws.reset()}><RiReset class="menu-icon" />Reset to default</DropdownMenu.Item>
        </DropdownMenu.Content>
      </DropdownMenu.Root>
      <DropdownMenu.Root>
        <Tip text="Export or import the tabs">
          {#snippet children(tip)}
            <DropdownMenu.Trigger {...tip}>
              {#snippet child({ props })}<Button {...props} size="xs" variant="ghost"><RiDownload />File</Button>{/snippet}
            </DropdownMenu.Trigger>
          {/snippet}
        </Tip>
        <DropdownMenu.Content class="menu-icons w-max" align="end" sideOffset={BAR_OFFSET} {...fileMenu}>
          <DropdownMenu.Item onSelect={exportTabs}><RiDownload class="menu-icon" />Export every tab…</DropdownMenu.Item>
          <DropdownMenu.Item onSelect={() => importInput.click()}><RiUpload class="menu-icon" />Import tabs…</DropdownMenu.Item>
          <DropdownMenu.Separator />
          <DropdownMenu.Item onSelect={() => openExport(ws.selected.size ? [...ws.selected] : plotIds(ws.layout))}><RiExport class="menu-icon" />Export data…</DropdownMenu.Item>
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
        <Popover.Content class="w-80 gap-0 p-0" align="end" sideOffset={BAR_OFFSET} {...settingsMenu}>
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
    {#if !docks.shown('left')}<Dock {docks} side="left" content={panelBody} />{/if}
    <Resizable.PaneGroup direction={narrow.current ? 'vertical' : 'horizontal'} class="layout" onLayout={docksResized}>
      {#if docks.shown('left')}
        <Resizable.Pane id="dock-left" order={1} bind:this={dockPanes.left} defaultSize={dockSize.left} minSize={DOCK.min} maxSize={DOCK.max} class="flex min-h-0">
          {@render dockFrame('left')}
        </Resizable.Pane>
        {@render dockHandle('left')}
      {/if}
      <Resizable.Pane id="tiles" order={2} defaultSize={100 - (docks.shown('left') ? dockSize.left : 0) - (docks.shown('right') ? dockSize.right : 0)} class="flex min-h-0 min-w-0">
        <main class="flex min-h-0 min-w-0 flex-1 flex-col gap-1.5">
          <TabStrip {ws} {drags} onClose={closeTab} />
          <div class="tiles" data-focus="plots" tabindex="-1" class:has-max={ws.maximized} class:drop-target={drags.drop?.outer && !drags.drop.edge}>
            {#key ws.activeId}
              {#if ws.layout}
                <Tile node={ws.layout} leafContent={plotTile} drop={drags.drop} maximized={ws.maximized} lifted={drags.moving?.group} landed={drags.landed} />
              {:else}
                <Tip text="Or drop a signal anywhere here">
                  {#snippet children(tip)}<Button {...tip} class="m-auto" size="sm" variant="outline" onclick={() => newPlotWith([])}><RiAdd />Add a plot</Button>{/snippet}
                </Tip>
              {/if}
            {/key}
          </div>
        </main>
      </Resizable.Pane>
      {#if docks.shown('right')}
        {@render dockHandle('right')}
        <Resizable.Pane id="dock-right" order={3} bind:this={dockPanes.right} defaultSize={dockSize.right} minSize={DOCK.min} maxSize={DOCK.max} class="flex min-h-0">
          {@render dockFrame('right')}
        </Resizable.Pane>
      {/if}
    </Resizable.PaneGroup>
    {#if !docks.shown('right')}<Dock {docks} side="right" content={panelBody} />{/if}
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
