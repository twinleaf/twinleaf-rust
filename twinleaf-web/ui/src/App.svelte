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
  import { exportText, importText, loadTabs, newPlot, saveTabs } from './lib/tabs.js';
  import { ARRANGEMENTS, DECIMATIONS, SPANS } from './lib/constants.js';
  import { actionFor } from './lib/keymap.js';
  import { api } from './lib/protocol.js';
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
  import KeysDialog from './lib/KeysDialog.svelte';
  import StatusBar from './lib/StatusBar.svelte';
  import CommandPalette from './lib/CommandPalette.svelte';
  import IconButton from './lib/IconButton.svelte';
  import Tip from './lib/Tip.svelte';
  import { Button } from '$lib/components/ui/button/index.js';
  import * as Select from '$lib/components/ui/select/index.js';
  import { Slider } from '$lib/components/ui/slider/index.js';
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
  import RiUpload from '@lucide/svelte/icons/upload';

  const hub = new Hub();
  const ws = new Workspace(loadTabs(localStorage));
  const views = new Views(hub, viewChanged);
  const drags = new Drags(ws, refreshToolbar);
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
  /** Shows the chosen views' state in the toolbar, and keeps every paused view's points. */
  function refreshToolbar() {
    const vs = chosen();
    live = vs.every((v) => v.atNewest);
    if (vs.length) span = +vs[0].span.toFixed(3);
    // A paused spectrum holds the spectrum it shows, not the points.
    hub.hold(views.heldFrom((id) => plotAnywhere(id)?.kind !== 'spectrum'));
  }
  // Paused, every pan or zoom asks the server for the plot's view at full
  // resolution, read from its raw buffer. Live, the decimated stream is used.
  const DETAIL_DEBOUNCE_MS = 120;
  const detailTimers = new Map();
  function requestDetail(id) {
    clearTimeout(detailTimers.get(id));
    const v = views.get(id);
    const session = hub.session;
    if (!v || v.live) return session.clearDetail(id);
    const timer = setTimeout(() => {
      const plot = plotAnywhere(id);
      if (!plot || plot.kind !== 'time') return;
      const [x0, x1] = v.range(hub.now(performance.now()));
      const pad = (x1 - x0) * 0.5;
      const traces = plot.items.map((i) => session.byKey.get(i.key)).filter(Boolean);
      session.fetchDetail(id, traces, x0 - pad, x1 + pad, window.innerWidth * 2);
    }, DETAIL_DEBOUNCE_MS);
    detailTimers.set(id, timer);
  }

  /** Adds plot `id` to the selection or takes it out, with Shift, or else
   * clears the selection. A click on a selected plot clears it once the
   * double-click time has passed, so a double-click still acts on it all. */
  let deselect;
  const DOUBLE_CLICK_MS = 300;
  function select(id, shift, clicks) {
    clearTimeout(deselect);
    const selected = ws.selected;
    if (shift) selected.has(id) ? selected.delete(id) : selected.add(id);
    else if (!selected.has(id)) selected.clear();
    else if (clicks === 1) {
      deselect = setTimeout(() => {
        selected.clear();
        refreshToolbar();
      }, DOUBLE_CLICK_MS);
    }
    refreshToolbar();
  }

  // Closing a tab with plots asks first. `closing` shows the dialog, which
  // clears it as it closes, before its button's click runs, so the button
  // closes `asked`, which only this sets.
  let closing = $state(null);
  let asked = null;
  function closeTab(t) {
    if (plotIds(t.layout).length) closing = asked = t;
    else ws.closeTab(t);
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
    hub.session.clearDetail(id);
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
  /** Pauses the chosen plots while they follow live, as Space does, or has them follow live. */
  function toggleLive() {
    if (!live) return followLive();
    const perf = performance.now();
    for (const vw of chosen()) vw.pause(perf);
  }
  function setDelay(ms) {
    delayMs = ms;
    views.setDelay(ms / 1000);
  }
  /** Clears the cursors of `ids`' plots, or of the selection, or of every plot. */
  function clearCursors(ids = ws.selected.size ? [...ws.selected] : plotIds(ws.layout)) {
    for (const id of ids) {
      const vw = views.get(id);
      if (vw) vw.cursors = { a: null, b: null };
    }
    for (const pv of registry) if (ids.includes(pv.plotId)) pv.specCursors = { a: null, b: null };
  }
  const act = { addToFocused: (key) => ws.addToFocused(key), newPlot: newPlotWith, followLive, clearCursors: () => clearCursors(), exportTabs, importTabs: () => importInput.click() };

  let paletteOpen = $state(false);
  let keysOpen = $state(false);
  // Whether focus last moved by the keyboard: a control reached by Tab keeps
  // its keys, while one the pointer clicked hands Space and Escape to the plots.
  let keyFocus = false;
  function onKey(e) {
    if (e.key === 'Tab') keyFocus = true;
    if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') {
      e.preventDefault();
      paletteOpen = !paletteOpen;
      return;
    }
    // What is typed into a field, or a menu or dialog's own keys, stays there.
    if (e.target.closest?.('input, textarea, select, [contenteditable], [role=combobox][aria-expanded=true], [role=menu], [role=menuitem], [role=dialog], [data-slot=select-content]')) return;
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
    if (action !== 'pause' && action !== 'escape') return;
    const control = e.target.closest?.('button, [role=tab], [role=slider]');
    if (keyFocus && control) return;
    // A control the pointer clicked lets go of focus, so the key shows no ring on it.
    if (control) control.blur();
    if (action === 'escape') {
      if (ws.maximized) ws.maximized = null;
      else if (ws.selected.size) {
        ws.selected.clear();
        refreshToolbar();
      } else clearCursors();
      return;
    }
    // Space pauses the highlighted plots, or streams them again from where they are.
    e.preventDefault();
    const perf = performance.now(), vs = ws.highlighted().map((id) => views.of(id));
    if (vs.some((v) => v.live)) for (const v of vs) v.pause(perf);
    else for (const v of vs) v.resume(perf);
  }
</script>

<!-- Nothing on the page drags natively: plots, signals and tabs move by
     pointer, and a native drag would only carry a picture of the page. -->
<svelte:window onkeydown={onKey} onpointerdowncapture={() => (keyFocus = false)} ondragstart={(e) => e.preventDefault()} />
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
    title="Drag to resize; double-click resets"
    class="mx-[3px] bg-transparent transition-colors after:w-3 hover:bg-primary/60 data-[direction=vertical]:mx-0 data-[direction=vertical]:my-[3px] data-[direction=vertical]:after:h-3"
    ondblclick={() => dockPanes[side]?.resize(DOCK.default)}
  />
{/snippet}

<Tooltip.Provider delayDuration={300}>
  <div class="app" class:moving={drags.moving}>
    <!-- One row: what acts on the plots, then the page's own settings. -->
    <header class="top-bar">
      <span class="brand">Twinleaf Hub</span>
      <button class="search" onclick={() => (paletteOpen = true)}><RiSearch />Commands<kbd>Ctrl K</kbd></button>
      <span class="divider"></span>
      <Tip text={live ? 'Pause the highlighted plots, or every plot' : 'Follow live'} keys={live ? ['Space'] : ['End']}>
        {#snippet children(tip)}
          <Button {...tip} size="xs" variant={live ? 'outline' : 'default'} onclick={toggleLive}>
            {#if live}<RiPause />Live{:else}<RiPlay />Follow live{/if}
          </Button>
        {/snippet}
      </Tip>
      <Select.Root type="single" value={String(span)} onValueChange={setSpan}>
        <Tip text="The time window the plots show">
          {#snippet children(tip)}<Select.Trigger {...tip} size="sm" class="h-6 w-20 text-xs" aria-label="Time window">{span} s</Select.Trigger>{/snippet}
        </Tip>
        <Select.Content>
          {#each SPANS as s}<Select.Item value={String(s)} label={`${s} s`} />{/each}
        </Select.Content>
      </Select.Root>
      <IconButton icon={RiEraser} label="Clear the cursors" keys={['Esc']} onclick={() => clearCursors()} />
      <DropdownMenu.Root>
        <Tip text="Arrange this tab's plots">
          {#snippet children(tip)}
            <DropdownMenu.Trigger {...tip}>
              {#snippet child({ props })}<Button {...props} size="xs" variant="ghost"><RiGrid />Layout</Button>{/snippet}
            </DropdownMenu.Trigger>
          {/snippet}
        </Tip>
        <DropdownMenu.Content class="w-48">
          <DropdownMenu.Label>Arrange this tab</DropdownMenu.Label>
          {#each ARRANGEMENTS as [name, label]}
            <DropdownMenu.Item onSelect={() => ws.arrange(name)}>{label}</DropdownMenu.Item>
          {/each}
          <DropdownMenu.Separator />
          <DropdownMenu.Item onSelect={() => ws.reset()}>Reset to the default</DropdownMenu.Item>
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
        <DropdownMenu.Content class="w-56">
          <DropdownMenu.Item onSelect={exportTabs}><RiDownload />Export every tab…</DropdownMenu.Item>
          <DropdownMenu.Item onSelect={() => importInput.click()}><RiUpload />Import tabs…</DropdownMenu.Item>
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
        <Popover.Content class="w-80" align="end">
          <div class="settings-pop">
            <div class="title">Display</div>
            <label class="flex items-center gap-2 text-xs text-muted-foreground">
              Delay
              <Slider type="single" value={delayMs} min={0} max={500} step={10} onValueChange={setDelay} class="w-24" />
              <span class="w-16 tabular-nums text-foreground">{delayMs} ms</span>
            </label>
            <label class="flex items-center justify-between gap-2 text-xs text-muted-foreground">
              Decimation
              <Select.Root type="single" value={String(hub.session.pps)} onValueChange={(v) => api.setPps(+v)}>
                <Select.Trigger size="sm" class="w-40">{hub.session.pps} pts/s per trace</Select.Trigger>
                <Select.Content>
                  {#each DECIMATIONS as p}<Select.Item value={String(p)} label={`${p} pts/s per trace`} />{/each}
                </Select.Content>
              </Select.Root>
            </label>
            <div class="title">Renderer</div>
            <div class="hud">
              <span><b>{hud.fps.toFixed(0)}</b> fps</span>
              <span><b>{hud.frameMs.toFixed(2)}</b> ms CPU/frame</span>
              <span><b>{hud.segments.toLocaleString()}</b> segments</span>
              <span><b>{hud.drawCalls}</b> draws</span>
              <span><b>{hud.uploadKBs.toFixed(1)}</b> kB/s to GPU</span>
              <span><b>{hud.rxKBs.toFixed(0)}</b> kB/s in</span>
            </div>
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
          <div class="tiles" class:has-max={ws.maximized} class:drop-target={drags.drop?.outer && !drags.drop.edge}>
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
<KeysDialog bind:open={keysOpen} />
<CommandPalette bind:open={paletteOpen} session={hub.session} {ws} {act} />

<AlertDialog.Root open={!!closing} onOpenChange={(o) => !o && (closing = null)}>
  <AlertDialog.Content>
    <AlertDialog.Header>
      <AlertDialog.Title>Close “{closing?.name}”?</AlertDialog.Title>
      <AlertDialog.Description>
        Its {plotIds(closing?.layout ?? null).length} plots go with it. Export first if you want to keep the layout.
      </AlertDialog.Description>
    </AlertDialog.Header>
    <AlertDialog.Footer>
      <AlertDialog.Cancel>Keep it</AlertDialog.Cancel>
      <AlertDialog.Action onclick={() => asked && ws.closeTab(asked)}>Close tab</AlertDialog.Action>
    </AlertDialog.Footer>
  </AlertDialog.Content>
</AlertDialog.Root>
