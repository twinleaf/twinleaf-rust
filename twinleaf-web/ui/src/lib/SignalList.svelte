<script>
  // Every signal the hub has, or those the search matches, to drag onto the
  // tiles or add to the highlighted plot.
  //
  // The search matches every word typed against the signal's key and units.
  // Its arrows move along the list, as the list's own do once it has focus.
  // Enter adds the signal to the focused plot, and Shift+Enter gives it a
  // new one. Dragging a signal out copies it, and nothing drops back in.
  import { untrack } from 'svelte';
  import { flip } from 'svelte/animate';
  import { dndzone, TRIGGERS, SHADOW_ITEM_MARKER_PROPERTY_NAME as SHADOW } from 'svelte-dnd-action';
  import { ScrollArea } from '$lib/components/ui/scroll-area/index.js';
  import { Input } from '$lib/components/ui/input/index.js';
  import { colorOf } from './plot.js';
  import * as ContextMenu from '$lib/components/ui/context-menu/index.js';
  import { claimMenu, releaseMenu } from './menus.js';
  import RiAdd from '@lucide/svelte/icons/plus';
  import RiNewPlot from '@lucide/svelte/icons/square-plus';
  import { actionFor, stepIn } from './keymap.js';
  import { uid } from './tabs.js';

  /** `drags` is the page's Drags; `onAdd(key)` adds to the focused plot, `onNewPlot(keys)` makes a plot. */
  let { session, drags, onAdd, onNewPlot } = $props();

  const FLIP_MS = 150;
  /** The keys the search keeps for its caret, though the list moves by them. */
  const CARET_KEYS = ['ArrowLeft', 'ArrowRight', 'Home', 'End'];
  let palette = $state([]);
  let dragging = false;
  let query = $state('');
  /** Whether signal `t` has every word of the search in its key or units. */
  const matches = (t, words) => words.every((w) => t.key.toLowerCase().includes(w) || (t.meta.units ?? '').toLowerCase() === w);
  const refresh = () => {
    const words = query.toLowerCase().split(/s+/).filter(Boolean);
    palette = session.signals.filter((t) => matches(t, words)).map((t) => ({ id: `pal:${t.key}`, key: t.key }));
    // Untracked, so the effect that refreshes follows the signals and the search alone.
    active = untrack(() => Math.min(active, Math.max(0, palette.length - 1)));
  };
  $effect(() => {
    session.signals;
    query;
    if (!dragging) refresh();
  });
  function consider(e) {
    const { trigger, id } = e.detail.info;
    if (trigger === TRIGGERS.DRAG_STARTED) {
      const idx = palette.findIndex((i) => i.id === id);
      const items = e.detail.items.filter((i) => !i[SHADOW]);
      items.splice(idx, 0, { ...palette[idx], id: uid('c') });
      palette = items;
      dragging = true;
      drags.traceDragStart();
    } else if (!dragging) {
      palette = e.detail.items;
    } else {
      palette = [...palette];
    }
  }
  function finalize(e) {
    dragging = false;
    // Where the drag ended is the page's to say, as the drag library can
    // count a quick drop as landing back here. A plot that took the signal
    // has already used the drop, so this opens a plot only for a drop on an
    // edge or an empty tab.
    drags.dockTrace(e.detail.info.id.slice('pal:'.length));
    refresh();
  }

  let menuOpen = $state(false);
  const closeMenu = () => (menuOpen = false);
  $effect(() => (menuOpen ? claimMenu(closeMenu) : releaseMenu(closeMenu)));
  let menuKey = $state(null); // the signal a right-click opened the menu for
  let active = $state(0);
  let focused = $state(false);
  function onkeydown(e) {
    // A key shows the row the arrows are on, however the list got focus.
    focused = true;
    const action = actionFor('list', e);
    const key = palette[active]?.key;
    if (action === 'move') active = stepIn(e, active, palette.length);
    else if (action === 'add' && key) onAdd(key);
    else if (action === 'newPlot' && key) onNewPlot([key]);
    else return;
    e.preventDefault();
    document.getElementById(`pal-${palette[active]?.id}`)?.scrollIntoView({ block: 'nearest' });
  }
  function searchKey(e) {
    if (e.key === 'Escape' && query) {
      query = '';
      e.preventDefault();
    } else if (!CARET_KEYS.includes(e.key)) onkeydown(e);
  }
</script>

<Input
  class="h-7 text-xs"
  type="search"
  bind:value={query}
  placeholder="Search {session.signals.length} signals"
  title="Drag a signal onto the plots, or click it to add it to the highlighted one"
  aria-label="Search signals: arrows move, Enter adds to the focused plot, Shift+Enter makes a new one"
  onkeydown={searchKey}
  onfocus={() => (focused = true)}
  onblur={() => (focused = false)}
/>
<!-- A signal's menu: where it goes. -->
<ContextMenu.Root bind:open={menuOpen}>
<ContextMenu.Trigger>
{#snippet child({ props })}
<ScrollArea class="palette-scroll">
  <div
    {...props}
    oncontextmenucapture={(e) => {
      menuKey = e.target.closest?.('.chip')?.dataset.key ?? null;
      if (!menuKey) e.preventDefault(), e.stopPropagation();
    }}
    class="palette"
    role="listbox"
    tabindex="0"
    data-focus-own
    aria-label="Signals: Enter adds to the focused plot, Shift+Enter makes a new one"
    aria-activedescendant={focused && palette[active] ? `pal-${palette[active].id}` : undefined}
    use:dndzone={{ items: palette, type: 'trace', flipDurationMs: FLIP_MS, dropFromOthersDisabled: true, dropTargetStyle: {}, delayTouchStart: true, zoneTabIndex: 0, zoneItemTabIndex: -1, autoAriaDisabled: true }}
    onconsider={consider}
    onfinalize={finalize}
    {onkeydown}
    onfocus={(e) => (focused = e.currentTarget.matches(':focus-visible'))}
    onblur={() => (focused = false)}
  >
    {#each palette as item, i (item.id)}
      {@const t = session.trace(item.key)}
      <!-- An option of the list, which takes its keys. -->
      <!-- svelte-ignore a11y_click_events_have_key_events -->
      <div
        id="pal-{item.id}"
        data-key={item.key}
        class="chip"
        class:active={focused && i === active}
        role="option"
        aria-selected={focused && i === active}
        tabindex="-1"
        style="--c: {t ? colorOf(t) : 'var(--muted-foreground)'}"
        animate:flip={{ duration: FLIP_MS }}
        onclick={() => onAdd(item.key)}
      >
        <i></i><span class="sig-key">{item.key}</span>{#if t?.meta.units}<span class="sig-units">{t.meta.units}</span>{/if}<small>{t?.meta.rate} Hz</small>
      </div>
    {/each}
  </div>
  {#if !palette.length}<div class="sig-empty">{session.signals.length ? 'No signal matches' : 'No signal yet'}</div>{/if}
</ScrollArea>
{/snippet}
</ContextMenu.Trigger>
<ContextMenu.Content class="menu-icons">
  {#if menuKey}
    <ContextMenu.Item onSelect={() => onAdd(menuKey)}><RiAdd class="menu-icon" />Add to plot<ContextMenu.Shortcut><kbd>Enter</kbd></ContextMenu.Shortcut></ContextMenu.Item>
    <ContextMenu.Item onSelect={() => onNewPlot([menuKey])}><RiNewPlot class="menu-icon" />New plot<ContextMenu.Shortcut><kbd>⇧</kbd><kbd>Enter</kbd></ContextMenu.Shortcut></ContextMenu.Item>
  {/if}
</ContextMenu.Content>
</ContextMenu.Root>
