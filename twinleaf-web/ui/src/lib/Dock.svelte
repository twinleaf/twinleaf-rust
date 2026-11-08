<script>
  // One dock, left or right: its panels in a column, each under a header
  // that drags it, within this dock or to the other, and folds it with its
  // chevron. A button at the dock's top right hides it. A dock that shows
  // no panels, as it has none or is hidden, is a rail along the page's
  // edge: a panel dropped on it joins the dock's panels, at the end, and
  // shows the dock, and a click on a hidden dock's rail shows it. Only
  // docks take panels, so none lands on the tiles.
  //
  // The header's menu moves a panel without dragging, and the header folds
  // it on Enter, so the keyboard reaches all of it.
  //
  // During a drag the panels shown are this dock's own copy, which the drag
  // library rearranges, and the drop hands them to `docks`. Where the
  // dragged panel would land shows its header alone, and the place is read
  // from the cursor, as the panel dragged is its header too.
  import { flip } from 'svelte/animate';
  import { dragHandleZone, dragHandle, TRIGGERS, SHADOW_ITEM_MARKER_PROPERTY_NAME as SHADOW } from 'svelte-dnd-action';
  import * as ContextMenu from '$lib/components/ui/context-menu/index.js';
  import RiGrip from '@lucide/svelte/icons/grip-vertical';
  import RiChevron from '@lucide/svelte/icons/chevron-right';
  import RiHideLeft from '@lucide/svelte/icons/panel-left-close';
  import RiShowLeft from '@lucide/svelte/icons/panel-left-open';
  import RiHideRight from '@lucide/svelte/icons/panel-right-close';
  import RiShowRight from '@lucide/svelte/icons/panel-right-open';
  import { PANELS } from './docks.svelte.js';
  import Tip from './Tip.svelte';

  /** `docks` is the page's Docks, `side` this dock's, and `content(id)` draws a panel's body. */
  let { docks, side, content } = $props();

  // Panels slide only while one is dragged, so a fold resizes them in place.
  const FLIP_MS = 150;
  let items = $state([]);
  let dragging = $state(false);
  const rail = $derived(!docks.shown(side));
  $effect(() => {
    const committed = rail ? [] : docks[side];
    if (!dragging) items = committed.map((p) => ({ ...p }));
  });
  const other = $derived(side === 'left' ? 'right' : 'left');
  const [HideIcon, ShowIcon] = side === 'left' ? [RiHideLeft, RiShowLeft] : [RiHideRight, RiShowRight];

  function consider(e) {
    if (e.detail.info.trigger === TRIGGERS.DRAG_STARTED) docks.dragging = true;
    dragging = true;
    items = e.detail.items;
  }
  function finalize(e) {
    dragging = false;
    docks.dragging = false;
    if (!rail) return docks.set(side, e.detail.items);
    // A rail holds none of its dock's panels, so what lands on it joins them.
    const kept = docks[side].filter((p) => !e.detail.items.some((q) => q.id === p.id));
    docks.set(side, [...kept, ...e.detail.items]);
    if (e.detail.items.length) docks.hide(side, false);
  }
</script>

<!-- The drag zone holds the panels alone, so the buttons sit beside it. -->
<!-- svelte-ignore a11y_click_events_have_key_events, a11y_no_static_element_interactions -->
<div
  class="dock-side {side}"
  class:rail
  class:hidden={rail && docks[side].length > 0}
  class:awaiting={rail && docks.dragging}
  title={rail ? (docks[side].length ? `Show the ${side} dock` : 'Drop a panel here') : undefined}
  onclick={() => rail && docks[side].length && docks.hide(side, false)}
>
  {#if rail}
    {#if docks[side].length}
      <Tip text="Show the {side} dock">
        {#snippet children(tip)}<button {...tip} class="dock-button" aria-label="Show the {side} dock"><ShowIcon /></button>{/snippet}
      </Tip>
    {/if}
  {:else}
    <Tip text="Hide the {side} dock">
      {#snippet children(tip)}
        <button {...tip} class="dock-button" aria-label="Hide the {side} dock" onclick={(e) => (e.stopPropagation(), docks.hide(side, true))}><HideIcon /></button>
      {/snippet}
    </Tip>
  {/if}
<div
  class="dock"
  role="list"
  aria-label="{side === 'left' ? 'Left' : 'Right'} dock"
  use:dragHandleZone={{ items, type: 'panel', flipDurationMs: FLIP_MS, dropTargetStyle: {}, dropTargetClasses: ['dock-target'], zoneTabIndex: -1, useCursorForDetection: true, morphDisabled: true }}
  onconsider={consider}
  onfinalize={finalize}
>
  {#each items as p, i (p.id)}
    <!-- The drag library's placeholder for a panel from the other dock has an id of its own. -->
    {@const panel = PANELS[p.id] ?? { title: '', shrinks: false }}
    <section class="dock-panel" class:shadow={p[SHADOW]} class:open={p.open} class:shrinks={panel.shrinks} role="listitem" aria-label={panel.title} animate:flip={{ duration: dragging ? FLIP_MS : 0 }}>
      <ContextMenu.Root>
        <ContextMenu.Trigger>
          {#snippet child({ props })}
            <header
              {...props}
              class="dock-head"
              use:dragHandle
              aria-label="{panel.title}: drag to move, Enter folds, the menu key moves"
              tabindex="0"
              ondblclick={() => docks.toggle(p.id)}
              onkeydown={(e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                  e.preventDefault();
                  docks.toggle(p.id);
                }
              }}
            >
              <button
                class="dock-fold"
                aria-label="{p.open ? 'Fold' : 'Unfold'} {panel.title}"
                aria-expanded={p.open}
                tabindex="-1"
                onpointerdown={(e) => e.stopPropagation()}
                onmousedown={(e) => e.stopPropagation()}
                onclick={() => docks.toggle(p.id)}
              >
                <RiChevron />
              </button>
              <span class="dock-title">{panel.title}</span>
              <RiGrip class="dock-grip" aria-hidden="true" />
            </header>
          {/snippet}
        </ContextMenu.Trigger>
        <ContextMenu.Content class="w-48">
          <ContextMenu.Item onSelect={() => docks.toggle(p.id)}>{p.open ? 'Fold' : 'Unfold'}</ContextMenu.Item>
          <ContextMenu.Item disabled={i === 0} onSelect={() => docks.step(p.id, -1)}>Move up</ContextMenu.Item>
          <ContextMenu.Item disabled={i === items.length - 1} onSelect={() => docks.step(p.id, 1)}>Move down</ContextMenu.Item>
          <ContextMenu.Item onSelect={() => docks.moveTo(p.id, other, docks[other].length)}>Move to the {other} dock</ContextMenu.Item>
          <ContextMenu.Separator />
          <ContextMenu.Item onSelect={() => docks.reset()}>Reset every panel</ContextMenu.Item>
        </ContextMenu.Content>
      </ContextMenu.Root>
      {#if p.open && !p[SHADOW]}
        <div class="dock-body">{@render content(p.id)}</div>
      {/if}
    </section>
  {/each}
</div>
</div>
