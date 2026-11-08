<script>
  // One plot of a tab: its header of signals and actions, the plot body the
  // WebGL layer draws behind, and its menus. The drawing is plot.js's
  // PlotView; this is the plot's controls.
  import { onMount, tick } from 'svelte';
  import { dndzone, TRIGGERS, SHADOW_ITEM_MARKER_PROPERTY_NAME as SHADOW } from 'svelte-dnd-action';
  import * as ContextMenu from '$lib/components/ui/context-menu/index.js';
  import { ScrollArea } from '$lib/components/ui/scroll-area/index.js';
  import IconButton from './IconButton.svelte';
  import KindPicker from './KindPicker.svelte';
  import Tip from './Tip.svelte';
  import RiDraggable from '@lucide/svelte/icons/grip-vertical';
  import RiSplitH from '@lucide/svelte/icons/columns-2';
  import RiSplitV from '@lucide/svelte/icons/rows-2';
  import RiFullscreen from '@lucide/svelte/icons/maximize-2';
  import RiFullscreenExit from '@lucide/svelte/icons/minimize-2';
  import RiClose from '@lucide/svelte/icons/x';
  import RiPause from '@lucide/svelte/icons/pause';
  import RiPlay from '@lucide/svelte/icons/play';
  import RiLegend from '@lucide/svelte/icons/list';
  import { PlotView, colorOf, monitorOf } from './plot.js';
  import { DRAG_START, KINDS, MODES, SPANS, kindLabel } from './constants.js';
  import { actionFor, stepIn } from './keymap.js';
  import { chip } from './tabs.js';

  let {
    plot, hub, view, targets, hover, renderer, registry, focused, selected, maximized, otherTabs = [],
    onFocus, onSelect, onRemove, onStartMove, onSplit, onMaximize, onMoveToTab, onClearCursors,
    onTraceDragStart, onTraceDrop, onEmptied,
  } = $props();

  const session = $derived(hub.session);
  /** The signal `key`'s colour, units and rate, once the hub has its trace. */
  const colorFor = (key) => {
    const t = session.trace(key);
    return t ? colorOf(t) : 'var(--muted-foreground)';
  };
  const unitsOf = (key) => session.trace(key)?.meta.units ?? '';
  const shownItems = $derived(plot.items.filter((i) => !i[SHADOW]));

  // The context menu shows what was right-clicked: a y axis's bounds, the
  // time axis's window, or the plot's own items.
  let menuOpen = $state(false);
  let region = $state({ kind: 'plot' });
  let bounds = $state({ lo: '', hi: '' });
  let boundsError = $state('');
  const shown = (v) => String(+v.toPrecision(6));
  function contextAt(e) {
    region = pv?.regionAt(e.clientX, e.clientY) ?? { kind: 'plot' };
    boundsError = '';
    const b = region.kind === 'y' && region.keys.length ? pv.yBounds(region.keys[0]) : null;
    bounds = b ? { lo: shown(b[0]), hi: shown(b[1]) } : { lo: '', hi: '' };
  }
  /** Sets the bounds typed in, and says why it cannot when they do not make a range. */
  function applyBounds() {
    const lo = Number(bounds.lo), hi = Number(bounds.hi);
    if (!isFinite(lo) || !isFinite(hi) || bounds.lo.trim() === '' || bounds.hi.trim() === '') boundsError = 'Enter a number in each';
    else if (lo >= hi) boundsError = 'Max must be above min';
    else if (plot.kind === 'spectrum' && lo <= 0) boundsError = 'A log axis starts above zero';
    else {
      pv?.setYBounds(region.keys, lo, hi);
      menuOpen = false;
    }
  }
  function boundsKey(e) {
    // The menu reads keys to move between items; these are the field's own.
    e.stopPropagation();
    if (e.key === 'Enter') applyBounds();
    if (e.key === 'Escape') menuOpen = false;
  }
  const forTargets = (fn) => {
    for (const v of targets()) fn(v);
  };

  // The signals menu: every signal the plot does not show, filtered by a
  // substring of its name, and kept open so several can be added at once.
  // Focus stays in the filter, where the arrows move the highlight and Enter
  // adds the highlighted signal; the next one takes its place.
  let signalsOpen = $state(false);
  let signalFilter = $state('');
  let addActive = $state(0);
  const addable = $derived.by(() => {
    const has = new Set(plot.items.map((i) => i.key)), f = signalFilter.trim().toLowerCase();
    return session.signals.filter((t) => !has.has(t.key) && t.key.toLowerCase().includes(f));
  });
  function addSignal(key) {
    if (!plot.items.some((i) => i.key === key)) plot.items.push(chip(key));
  }
  function filterKey(e) {
    // Escape and Tab go on to the menu, which closes; every other key stays here.
    if (e.key === 'Escape' || e.key === 'Tab') return;
    e.stopPropagation();
    const to = { ArrowDown: addActive + 1, ArrowUp: addActive - 1, PageDown: addActive + 8, PageUp: addActive - 8 }[e.key];
    if (to !== undefined) addActive = Math.max(0, Math.min(addable.length - 1, to));
    else if (e.key === 'Enter' && addable[addActive]) {
      addSignal(addable[addActive].key);
      addActive = Math.max(0, Math.min(addActive, addable.length - 2));
    } else return;
    e.preventDefault();
    tick().then(() => document.getElementById(`add-${plot.id}-${addActive}`)?.scrollIntoView({ block: 'nearest' }));
  }

  // A click, not a drag, on the plot: with Shift it adds the plot to the
  // selection or takes it out. The capture sees the press before the header
  // or the plot takes it for a move or a pan.
  let pressedAt = null;
  function pressed(e) {
    pressedAt = [e.clientX, e.clientY];
    onFocus();
    // With Shift, a selected plot is picked up from anywhere on it, with the
    // rest of the selection. A plain drag still pans the selection, and a
    // Shift-click that does not move still toggles the plot.
    if (e.shiftKey && selected && e.button === 0 && !e.target.closest('button, input, [role=menuitem]')) {
      e.stopPropagation();
      onStartMove(e);
    }
  }
  function clicked(e) {
    if (!pressedAt || Math.hypot(e.clientX - pressedAt[0], e.clientY - pressedAt[1]) >= DRAG_START) return;
    if (e.target.closest('button, input, [role=menuitem]')) return;
    onSelect?.(e.shiftKey, e.detail);
  }

  let body = $state();
  let overlay = $state();
  let pv;

  // The readouts come from the plot view each frame; the page shows them a
  // few times a second, which is as fast as anyone reads.
  const LEGEND_MS = 66;
  let legend = $state({ rows: {}, cursors: null });

  // A plot too small for its header becomes a monitor, as plot.js's
  // `monitorOf` decides from the traces it draws.
  let size = $state({ width: Infinity, height: Infinity });
  const monitor = $derived(monitorOf(size, plot.mode, shownItems.filter((i) => session.trace(i.key)).length));

  /**
   * Marks a row of chips that runs past either edge, for the fade there, and
   * turns the wheel into a sideways scroll over it. The row is one line high
   * however many signals it holds.
   */
  function overflowEdges(node) {
    const mark = () => {
      node.classList.toggle('more-left', node.scrollLeft > 1);
      node.classList.toggle('more-right', node.scrollLeft + node.clientWidth < node.scrollWidth - 1);
    };
    const wheel = (e) => {
      if (node.scrollWidth <= node.clientWidth || Math.abs(e.deltaX) > Math.abs(e.deltaY)) return;
      e.preventDefault();
      node.scrollLeft += e.deltaY;
    };
    const sizing = new ResizeObserver(mark);
    sizing.observe(node);
    const chips = new MutationObserver(mark);
    chips.observe(node, { childList: true });
    node.addEventListener('scroll', mark, { passive: true });
    node.addEventListener('wheel', wheel, { passive: false });
    return {
      destroy() {
        sizing.disconnect();
        chips.disconnect();
        node.removeEventListener('scroll', mark);
        node.removeEventListener('wheel', wheel);
      },
    };
  }

  // Whether the plot's view streams, for its play and pause button.
  let streaming = $state(true);
  /** Pauses the plot and the plots linked to it, or streams them again from where they are. */
  function playPause() {
    const perf = performance.now();
    forTargets((v) => (view.live ? v.pause(perf) : v.resume(perf)));
  }

  onMount(() => {
    const poll = setInterval(() => pv?.legend && (legend = pv.legend), LEGEND_MS);
    pv = new PlotView({
      body,
      overlay,
      hub,
      view,
      targets,
      hoverTime: hover,
      plotId: plot.id,
      renderer,
      onFocus,
      config: () => ({ kind: plot.kind, mode: plot.mode, keys: plot.items.filter((i) => !i[SHADOW]).map((i) => i.key) }),
    });
    registry.add(pv);
    const sizing = new ResizeObserver(([entry]) => {
      const { width, height } = entry.contentRect;
      size = { width, height };
    });
    sizing.observe(card);
    streaming = view.live;
    const unwatch = view.onChange(() => (streaming = view.live));
    return () => {
      clearInterval(poll);
      unwatch();
      sizing.disconnect();
      registry.delete(pv);
    };
  });

  function consider(e) {
    if (e.detail.info.trigger === TRIGGERS.DRAG_STARTED) onTraceDragStart?.();
    plot.items = e.detail.items;
  }
  function finalize(e) {
    // A chip dropped on a plot's edge lands in no zone and returns here, then
    // goes to the new plot beside that edge.
    let items = e.detail.items;
    const dropped = items.find((i) => i.id === e.detail.info.id && !i[SHADOW]);
    const docked = dropped && onTraceDrop?.(dropped.key);
    if (docked) items = items.filter((i) => i !== dropped);
    // A chip from the signal list keeps the list's id until it gets its own here.
    else if (dropped?.id.startsWith('pal:')) items = items.map((i) => (i === dropped ? chip(i.key) : i));
    const seen = new Set();
    plot.items = items.filter((i) => !seen.has(i.key) && seen.add(i.key));
    // A plot whose last signal was dragged away, to another plot or a new
    // one, closes. One emptied with × stays, to be filled again.
    const movedOut = docked || e.detail.info.trigger === TRIGGERS.DROPPED_INTO_ANOTHER;
    if (movedOut && !plot.items.length) onEmptied?.();
  }
  // The plot body is a drop target of its own, which shows nothing: a signal
  // dropped there joins the header's chips, or a new plot when on an edge.
  let bodyItems = $state([]);
  function bodyConsider(e) {
    bodyItems = e.detail.items;
  }
  function bodyFinalize(e) {
    const keys = e.detail.items.filter((i) => !i[SHADOW]).map((i) => i.key);
    bodyItems = [];
    for (const key of keys) {
      if (onTraceDrop?.(key)) continue;
      addSignal(key);
    }
  }
  function remove(id) {
    plot.items = plot.items.filter((i) => i.id !== id);
  }
  /** Whether `e` happened on the header itself or the empty part of its chips. */
  const empty = (e) => e.target === e.currentTarget || e.target.classList?.contains('trace-zone');

  // The keyboard. The plot is one stop in the page's Tab order; nothing in
  // it takes a stop of its own. Keys pressed while it has focus act on it,
  // and Enter moves into its signals, where the arrows move between them.
  // Which key does what is keymap.js's.
  let card = $state();
  let zone = $state();
  let kindOpen = $state(false);
  let kindFromKeys = false;
  /** Opens the kind picker, which takes the keys until it closes. */
  function openKind() {
    kindFromKeys = true;
    kindOpen = true;
  }
  /** Gives focus back to the plot when a kind menu opened from the keys closes, after the closing key is done. */
  function kindClosed(open) {
    if (open || !kindFromKeys) return;
    kindFromKeys = false;
    setTimeout(() => card?.focus());
  }
  let active = $state(0); // the signal the arrows are on, while the signals have focus
  let zoneFocused = $state(false);
  let addFromKeys = false;
  /** Opens the add-a-signal menu under the plot's signals. */
  function openAdd() {
    const r = zone.getBoundingClientRect();
    addFromKeys = true;
    zone.dispatchEvent(new MouseEvent('contextmenu', { bubbles: true, cancelable: true, clientX: r.left + 8, clientY: r.bottom }));
  }
  /** Gives focus back to the plot when a menu opened from the keys closes. */
  function signalsClosed(open) {
    if (open || !addFromKeys) return;
    addFromKeys = false;
    queueMicrotask(() => card?.focus());
  }
  const PAN = 0.1, ZOOM = 1.25;
  /** What inside a plot keeps its own keys: fields, menus and buttons. */
  const OWN_KEYS = 'input, textarea, select, button, [contenteditable], [role=listbox], [role=menu], [role=combobox], [role=dialog]';
  function plotKey(e) {
    // Keys anywhere on the plot, its drawing and its signals included; a
    // field, menu or button inside it keeps its own.
    if (e.target !== card && e.target !== zone && e.target.closest?.(OWN_KEYS)) return;
    const perf = performance.now(), time = plot.kind === 'time';
    const back = e.key === 'ArrowLeft' || e.key === 'ArrowDown';
    const act = {
      enterSignals: () => (plot.items.length ? ((active = 0), zone.focus()) : openAdd()),
      addSignals: openAdd,
      kind: openKind,
      splitRight: () => onSplit('row', true),
      splitDown: () => onSplit('col', true),
      maximize: onMaximize,
      close: () => onRemove(true),
      fitY: () => pv?.autoscale(),
      legend: () => (plot.legend = !plot.legend),
      follow: () => forTargets((v) => v.follow()),
      pan: time && (() => forTargets((v) => v.pan((e.shiftKey ? 1 : PAN) * (back ? -1 : 1), perf))),
      zoom: time && (() => forTargets((v) => v.zoom(back ? ZOOM : 1 / ZOOM, v.live ? 1 : 0.5, perf))),
    }[actionFor('plot', e)];
    if (!act) return;
    e.preventDefault();
    act();
  }
  /** The keys of the plot's signals while they have focus; the rest go on to the plot. */
  function signalsKey(e) {
    if (e.target !== zone) return;
    const action = actionFor('signals', e);
    if (action === 'move') {
      active = stepIn(e, active, plot.items.length);
      document.getElementById(`chip-${plot.items[active]?.id}`)?.scrollIntoView({ block: 'nearest', inline: 'nearest' });
    } else if (action === 'remove') {
      remove(plot.items[active]?.id);
      active = Math.min(active, plot.items.length - 1);
      if (!plot.items.length) card.focus();
    } else if (action === 'leave') card.focus();
    else return;
    e.preventDefault();
    e.stopPropagation();
  }
  const description = $derived(`${kindLabel(plot.kind, plot.mode)} plot: ${plot.items.map((i) => i.key).join(', ') || 'no signals'}`);
</script>

<div
  class:monitor
  class="plot-card flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden border bg-transparent {selected ? 'selected border-primary' : focused ? 'border-ring/60' : 'border-border'}"
  bind:this={card}
  onpointerdowncapture={pressed}
  onclick={clicked}
  onfocusin={onFocus}
  onkeydown={plotKey}
  role="group"
  tabindex="0"
  aria-label={description}
>
  <!-- The header picks the plot up from anywhere empty, as a title bar does. -->
  <div
    class="plot-head flex h-7 shrink-0 cursor-grab items-center gap-1.5 border-b bg-card px-1.5"
    ondblclick={(e) => empty(e) && onMaximize()}
    onpointerdown={(e) => empty(e) && onStartMove(e)}
    role="toolbar"
    tabindex="-1"
  >
    <IconButton icon={RiDraggable} label="Drag onto a plot's edge to dock, its middle to swap, or a tab to move" class="grip cursor-grab touch-none text-muted-foreground" tabindex={-1} onpointerdown={onStartMove} />
    <KindPicker {plot} bind:open={kindOpen} onOpenChange={kindClosed} />
    <!-- The plot's signals, each with its value at the hover, or its newest
         while live. Right-clicking them adds or removes signals. -->
    <ContextMenu.Root bind:open={signalsOpen} onOpenChange={signalsClosed}>
      <ContextMenu.Trigger>
        {#snippet child({ props })}
          <div
            {...props}
            bind:this={zone}
            class="trace-zone"
            role="listbox"
            aria-label="Signals in this plot"
            aria-activedescendant={zoneFocused && plot.items[active] ? `chip-${plot.items[active].id}` : undefined}
            use:overflowEdges
            use:dndzone={{ items: plot.items, type: 'trace', flipDurationMs: 0, dropTargetStyle: {}, dropTargetClasses: [], delayTouchStart: true, zoneTabIndex: -1, zoneItemTabIndex: -1, autoAriaDisabled: true }}
            onconsider={consider}
            onfinalize={finalize}
            oncontextmenucapture={() => ((signalFilter = ''), (addActive = 0))}
            onkeydowncapture={signalsKey}
            onfocus={() => (zoneFocused = true)}
            onblur={() => (zoneFocused = false)}
          >
            {#each plot.items as item, i (item.id)}
              {@const r = legend.rows[item.key]}
              <div id="chip-{item.id}" class="trace-chip" class:ghost={item[SHADOW]} class:active={zoneFocused && i === active} role="option" aria-selected={zoneFocused && i === active} style="--c:{colorFor(item.key)}" title={r?.info}>
                <i></i>
                <span class="name">{item.key}</span>
                {#if unitsOf(item.key)}<span class="units">{unitsOf(item.key)}</span>{/if}
                <!-- Always there, and as wide as the longest value, so a value that comes and goes on hover never moves the header. -->
                <span class="val">{r?.value ?? ''}</span>
                <Tip text="Remove from this plot" keys={['Delete']}>
                  {#snippet children(tip)}<button {...tip} class="x" tabindex="-1" aria-label="Remove {item.key}" onclick={() => remove(item.id)}>×</button>{/snippet}
                </Tip>
              </div>
            {/each}
            {#if !plot.items.length}<span class="trace-empty">Drop signals here, or right-click to add</span>{/if}
          </div>
        {/snippet}
      </ContextMenu.Trigger>
      <ContextMenu.Content class="w-64">
        <div class="add-filter">
          <!-- svelte-ignore a11y_autofocus -->
          <input
            type="text"
            placeholder="Add a signal…"
            aria-label="Add a signal: type to filter"
            autofocus
            role="combobox"
            aria-expanded="true"
            aria-controls="add-{plot.id}"
            aria-activedescendant="add-{plot.id}-{addActive}"
            bind:value={signalFilter}
            oninput={() => (addActive = 0)}
            onkeydown={filterKey}
          />
        </div>
        <ScrollArea class="signal-list">
        <div id="add-{plot.id}" role="listbox" aria-label="Signals to add">
          {#each addable as t, i (t.key)}
            <!-- The filter takes the keys for the whole list. -->
            <!-- svelte-ignore a11y_click_events_have_key_events -->
            <div
              id="add-{plot.id}-{i}"
              class="signal-row"
              class:active={i === addActive}
              role="option"
              aria-selected={i === addActive}
              tabindex="-1"
              onpointermove={() => (addActive = i)}
              onpointerdown={(e) => e.preventDefault()}
              onclick={() => addSignal(t.key)}
            >
              <span class="signal-option" style="--c:{colorOf(t)}"><i></i>{t.key}<small>{t.meta.units}</small></span>
              <span class="rate">{t.meta.rate} Hz</span>
            </div>
          {:else}
            <div class="signal-none">{signalFilter ? 'No signal matches' : 'Every signal is shown'}</div>
          {/each}
        </div>
        </ScrollArea>
        {#if plot.items.length}
          <ContextMenu.Separator />
          <ContextMenu.Item variant="destructive" onSelect={() => (plot.items = [])}>Remove every signal</ContextMenu.Item>
        {/if}
      </ContextMenu.Content>
    </ContextMenu.Root>
    <div class="plot-actions flex shrink-0 items-center">
      <IconButton
        icon={RiLegend}
        label={plot.legend ? 'Unpin the legend' : 'Pin the legend in the corner'}
        keys={['L']}
        class={plot.legend ? 'text-primary' : ''}
        aria-pressed={!!plot.legend}
        tabindex={-1}
        onclick={() => (plot.legend = !plot.legend)}
      />
      <IconButton
        icon={streaming ? RiPause : RiPlay}
        label={streaming ? 'Pause' : 'Stream from here'}
        keys={['Space']}
        class={streaming ? '' : 'text-primary'}
        tabindex={-1}
        onclick={playPause}
      />
      <IconButton icon={RiSplitH} label="Split right" keys={['|']} class="split" tabindex={-1} onclick={() => onSplit('row')} />
      <IconButton icon={RiSplitV} label="Split down" keys={['−']} class="split" tabindex={-1} onclick={() => onSplit('col')} />
      <IconButton icon={maximized ? RiFullscreenExit : RiFullscreen} label={maximized ? 'Restore' : 'Maximize'} keys={maximized ? ['F', 'Esc'] : ['F']} class="maximize" tabindex={-1} onclick={onMaximize} />
      <IconButton icon={RiClose} label="Close plot" keys={['Delete']} tabindex={-1} onclick={() => onRemove()} />
    </div>
  </div>

  <!-- The placed cursors: where each is, and every signal's value at it. -->
  {#if legend.cursors}
    {@const c = legend.cursors}
    <!-- A scope's cursor readout: where A and B are, then each signal there. -->
    <div class="cursor-readout">
      <table>
        <thead>
          <tr>
            <th></th>
            <th class="a">A</th>
            <th class="b">B</th>
            <th>Δ = B − A</th>
            {#if c.rate}<th></th>{/if}
          </tr>
        </thead>
        <tbody>
          <tr class="position">
            <th><var>{c.x}</var></th>
            <td>{c.a}</td>
            <td>{c.b}</td>
            <td>{c.d}</td>
            {#if c.rate}<td class="rate"><span>1/Δ<var>t</var></span> {c.rate}</td>{/if}
          </tr>
          {#each shownItems as item (item.id)}
            {@const r = legend.rows[item.key]}
            <tr>
              <th style="--c:{colorFor(item.key)}"><i></i>{item.key}</th>
              <td>{r?.a ?? '—'}</td>
              <td>{r?.b ?? '—'}</td>
              <td>{r?.d ?? '—'}</td>
              {#if c.rate}<td></td>{/if}
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}

  <ContextMenu.Root bind:open={menuOpen}>
    <ContextMenu.Trigger>
      {#snippet child({ props })}
        <div {...props} class="plot-body" bind:this={body} oncontextmenucapture={contextAt}>
          <canvas bind:this={overlay}></canvas>
          <!-- The pinned legend: each signal and its value, one short row
               apiece in the corner, no taller than part of the plot. -->
          {#if plot.legend && plot.items.length}
            <div class="pinned-legend" role="list" aria-label="Legend">
              {#each shownItems as item (item.id)}
                <div class="row" role="listitem" style="--c:{colorFor(item.key)}" title={item.key}>
                  <i></i><span class="name">{item.key}</span><span class="val">{legend.rows[item.key]?.value ?? ''}</span><span class="units">{unitsOf(item.key)}</span>
                </div>
              {/each}
            </div>
          {/if}
          <div
            class="legend-zone"
            use:dndzone={{ items: bodyItems, type: 'trace', flipDurationMs: 0, dropTargetStyle: {}, dropTargetClasses: [], dropAnimationDisabled: true, zoneTabIndex: -1, zoneItemTabIndex: -1 }}
            onconsider={bodyConsider}
            onfinalize={bodyFinalize}
          >
            {#each bodyItems as item (item.id)}<div class="body-ghost"></div>{/each}
          </div>
        </div>
      {/snippet}
    </ContextMenu.Trigger>
    <ContextMenu.Content class="w-56">
      {#if region.kind === 'y'}
        <!-- A y axis: its bounds, typed in, in the units the axis shows. -->
        <ContextMenu.Label>Y axis{region.keys.length === 1 && plot.mode !== 'overlay' ? ` · ${region.keys[0]}` : ''}</ContextMenu.Label>
        <div class="bounds" role="group" aria-label="Y axis bounds">
          <label>Max <input type="text" inputmode="decimal" bind:value={bounds.hi} onkeydown={boundsKey} /></label>
          <label>Min <input type="text" inputmode="decimal" bind:value={bounds.lo} onkeydown={boundsKey} /></label>
          {#if boundsError}<span class="error">{boundsError}</span>{/if}
        </div>
        <ContextMenu.Item onSelect={applyBounds}>Set bounds<ContextMenu.Shortcut>Enter</ContextMenu.Shortcut></ContextMenu.Item>
        <ContextMenu.Item onSelect={() => pv?.fit(region.keys)}>Fit to data<ContextMenu.Shortcut>double-click</ContextMenu.Shortcut></ContextMenu.Item>
      {:else if region.kind === 'x' && plot.kind === 'time'}
        <!-- The time axis: the window, and following the stream. -->
        <ContextMenu.Label>Time axis</ContextMenu.Label>
        {#if view.live}
          <ContextMenu.Item onSelect={() => forTargets((v) => v.pause(performance.now()))}>Pause<ContextMenu.Shortcut>Space</ContextMenu.Shortcut></ContextMenu.Item>
        {:else}
          <ContextMenu.Item onSelect={() => forTargets((v) => v.resume(performance.now()))}>Stream from here<ContextMenu.Shortcut>Space</ContextMenu.Shortcut></ContextMenu.Item>
        {/if}
        {#if !view.atNewest}<ContextMenu.Item onSelect={() => forTargets((v) => v.follow())}>Follow live</ContextMenu.Item>{/if}
        <ContextMenu.Item onSelect={onClearCursors}>Clear cursors<ContextMenu.Shortcut>Esc</ContextMenu.Shortcut></ContextMenu.Item>
        <ContextMenu.Separator />
        <ContextMenu.Label>Window</ContextMenu.Label>
        <ContextMenu.RadioGroup value={String(view.span)} onValueChange={(s) => forTargets((v) => ((v.span = +s), v.notify()))}>
          {#each SPANS as s}<ContextMenu.RadioItem value={String(s)}>{s} s</ContextMenu.RadioItem>{/each}
        </ContextMenu.RadioGroup>
      {:else}
      <ContextMenu.Item onSelect={() => onSplit('row')}>Split right</ContextMenu.Item>
      <ContextMenu.Item onSelect={() => onSplit('col')}>Split down</ContextMenu.Item>
      <ContextMenu.Item onSelect={onMaximize}>{maximized ? 'Restore' : 'Maximize'}</ContextMenu.Item>
      <ContextMenu.Item onSelect={onClearCursors}>Clear cursors<ContextMenu.Shortcut>Esc</ContextMenu.Shortcut></ContextMenu.Item>
      <ContextMenu.Sub>
        <ContextMenu.SubTrigger disabled={!otherTabs.length}>Move to tab</ContextMenu.SubTrigger>
        <ContextMenu.SubContent>
          {#each otherTabs as t (t.id)}<ContextMenu.Item onSelect={() => onMoveToTab(t.id)}>{t.name}</ContextMenu.Item>{/each}
        </ContextMenu.SubContent>
      </ContextMenu.Sub>
      <ContextMenu.Separator />
      <ContextMenu.Label>Kind</ContextMenu.Label>
      <ContextMenu.RadioGroup value={plot.kind} onValueChange={(v) => (plot.kind = v)}>
        {#each KINDS as [kind, kl]}<ContextMenu.RadioItem value={kind}>{kl}</ContextMenu.RadioItem>{/each}
      </ContextMenu.RadioGroup>
      <ContextMenu.Label>Layout</ContextMenu.Label>
      <ContextMenu.RadioGroup value={plot.mode} onValueChange={(v) => (plot.mode = v)}>
        {#each MODES as [mode, ml]}<ContextMenu.RadioItem value={mode}>{ml}</ContextMenu.RadioItem>{/each}
      </ContextMenu.RadioGroup>
      <ContextMenu.Separator />
      <ContextMenu.Item variant="destructive" onSelect={onRemove}>Close plot</ContextMenu.Item>
      {/if}
    </ContextMenu.Content>
  </ContextMenu.Root>
</div>
