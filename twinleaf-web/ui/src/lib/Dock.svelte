<script>
  // One dock, left or right: its panels in a column, each under a header
  // that drags it, within this dock or to the other, and folds it with its
  // chevron. A button in the row of its first panel's header hides it, and
  // scrolls with that header, so it lies over no row. A dock that shows
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
  import { onDestroy, tick } from 'svelte';
  import { PAGE, claim } from './keyrouter.js';
  import { flip } from 'svelte/animate';
  import { dragHandleZone, dragHandle, TRIGGERS, SHADOW_ITEM_MARKER_PROPERTY_NAME as SHADOW } from 'svelte-dnd-action';
  import * as ContextMenu from '$lib/components/ui/context-menu/index.js';
  import RiGrip from '@lucide/svelte/icons/grip-vertical';
  import RiChevron from '@lucide/svelte/icons/chevron-right';
  import RiHideLeft from '@lucide/svelte/icons/panel-left-close';
  import RiShowLeft from '@lucide/svelte/icons/panel-left-open';
  import RiHideRight from '@lucide/svelte/icons/panel-right-close';
  import RiShowRight from '@lucide/svelte/icons/panel-right-open';
  import RiFold from '@lucide/svelte/icons/chevrons-down-up';
  import RiUp from '@lucide/svelte/icons/arrow-up';
  import RiDown from '@lucide/svelte/icons/arrow-down';
  import RiToLeft from '@lucide/svelte/icons/panel-left';
  import RiToRight from '@lucide/svelte/icons/panel-right';
  import RiReset from '@lucide/svelte/icons/rotate-ccw';
  import { PANELS } from './docks.svelte.js';
  import { enter, focusNode } from './focus.js';
  import { focusedByKeyboard } from './keys.js';
  import Tip from './Tip.svelte';
  import { counted } from './menus.js';

  /**
   * `docks` is the page's Docks, `side` this dock's, and `content(id)` draws
   * a panel's body. `railed` draws the rail in the panels' place: the page
   * says when, as a dock sliding shut keeps its panels until it has.
   */
  let { docks, side, content, railed } = $props();

  // Panels slide only while one is dragged, so a fold resizes them in place.
  const FLIP_MS = 150;
  /** How long to wait for a hidden dock's rail, which appears once the dock has slid shut, in ms. */
  const RAIL_WAIT_MS = 600;
  /**
   * Hides or shows the dock. The dock is drawn anew either way, so focus
   * that was in it moves to the new one: to its rail's show button once
   * the rail is there, into its panels once shown.
   */
  function setHidden(hidden) {
    const had = root?.contains(document.activeElement);
    docks.hide(side, hidden);
    if (!had) return;
    const until = performance.now() + RAIL_WAIT_MS;
    const land = () => {
      const now = document.querySelector(hidden ? `.dock-side.${side}.rail` : `.dock-side.${side}:not(.rail)`);
      if (!now) return void (performance.now() < until && requestAnimationFrame(land));
      const show = hidden && now.querySelector('.dock-button');
      if (show) focusNode(show);
      else if (!enter(now)) focusNode(now);
    };
    tick().then(land);
  }
  let root = $state(null);
  /** How far the pointer may move between press and release for a click on a header to fold it, not drag it, in px. */
  const CLICK_SLOP = 4;
  let pressedAt = null;
  /** Folds or unfolds panel `id` when a click on its header was a click, not the end of a drag. */
  function clickHead(e, id) {
    const still = pressedAt && Math.hypot(e.clientX - pressedAt.x, e.clientY - pressedAt.y) < CLICK_SLOP;
    pressedAt = null;
    if (still && e.button === 0) docks.toggle(id);
  }
  /** The panel whose header is `head`. */
  const panelOf = (head) => items.find((p) => `panel-${p.id}` === head.dataset.focusKey);
  /** What a header's key does only when the keyboard moved focus to the header, as keys.js has buttons keep the keys that press them; after a click the key is the page's, so Space pauses the plots. */
  const whenReached = (fn) => (e, head) => (focusedByKeyboard(head) ? fn(head, panelOf(head)) : PAGE);
  // A header's keys (keymap.js, place 'panel'): Space folds or unfolds, and
  // Enter goes into the contents, unfolding them first. Neither reaches the
  // drag library, whose listeners on the header and its panel would start a
  // keyboard drag of the panel.
  onDestroy(
    claim('panel', {
      when: (e, at) => at instanceof Element && at.matches('.dock-head') && !!root?.contains(at),
      on: {
        fold: whenReached((head, p) => void docks.toggle(p.id)),
        enter: whenReached((head, p) => {
          if (p.open) return void enter(head);
          docks.toggle(p.id);
          tick().then(() => enter(head));
        }),
      },
    }),
  );
  let items = $state([]);
  let dragging = $state(false);
  const rail = $derived(railed);
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

<!-- The drag zone holds the panels alone: the rail's button sits beside it, and the button that hides the dock in its first panel. -->
<!-- svelte-ignore a11y_click_events_have_key_events, a11y_no_static_element_interactions -->
<div
  class="dock-side {side}"
  class:rail
  class:hidden={rail && docks[side].length > 0}
  class:awaiting={rail && docks.dragging}
  title={rail ? (docks[side].length ? `Show the ${side} dock` : 'Drop a panel here') : undefined}
  data-focus="dock"
  tabindex="-1"
  bind:this={root}
  onclick={() => rail && docks[side].length && setHidden(false)}
>
  {#if rail && docks[side].length}
    <Tip text="Show the {side} dock">
      {#snippet children(tip)}<button {...tip} class="dock-button" aria-label="Show the {side} dock"><ShowIcon /></button>{/snippet}
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
    {@const panel = PANELS[p.id] ?? { title: '' }}
    <section class="dock-panel" class:shadow={p[SHADOW]} class:open={p.open} role="listitem" aria-label={panel.title} animate:flip={{ duration: dragging ? FLIP_MS : 0 }}>
      <ContextMenu.Root>
        <ContextMenu.Trigger>
          {#snippet child({ props })}
            <header
              {...props}
              class="dock-head"
              role="button"
              use:dragHandle
              aria-label="{panel.title}: click or Space folds, Enter goes in, drag to move, the menu key moves"
              aria-expanded={p.open}
              tabindex="0"
              data-focus="panel"
              data-focus-key="panel-{p.id}"
              onpointerdowncapture={(e) => {
                pressedAt = { x: e.clientX, y: e.clientY };
                // The drag library keeps a press from focusing the header, so a
                // press focuses it here, for Enter and Space to act on it next.
                e.currentTarget.focus({ preventScroll: true });
              }}
              onclick={(e) => clickHead(e, p.id)}
            >
              <span class="dock-fold" aria-hidden="true"><RiChevron /></span>
              <span class="dock-title">{panel.title}</span>
              <RiGrip class="dock-grip" aria-hidden="true" />
            </header>
          {/snippet}
        </ContextMenu.Trigger>
        <ContextMenu.Content class="menu-icons">
          <ContextMenu.Item onSelect={() => docks.toggle(p.id)}><RiFold class="menu-icon" />{p.open ? 'Fold' : 'Unfold'}</ContextMenu.Item>
          <ContextMenu.Item disabled={i === 0} onSelect={() => docks.step(p.id, -1)}><RiUp class="menu-icon" />Move up<span class="menu-value">{i + 1} of {items.length}</span></ContextMenu.Item>
          <ContextMenu.Item disabled={i === items.length - 1} onSelect={() => docks.step(p.id, 1)}><RiDown class="menu-icon" />Move down<span class="menu-value">{i + 1} of {items.length}</span></ContextMenu.Item>
          <ContextMenu.Item onSelect={() => docks.moveTo(p.id, other, docks[other].length)}>{#if other === 'left'}<RiToLeft class="menu-icon" />{:else}<RiToRight class="menu-icon" />{/if}Move to the {other} dock<span class="menu-value">{counted(docks[other].length, 'panel')}</span></ContextMenu.Item>
          <ContextMenu.Separator />
          <ContextMenu.Item onSelect={() => setHidden(true)}><HideIcon class="menu-icon" />Hide the {side} dock</ContextMenu.Item>
          <ContextMenu.Item onSelect={() => docks.reset()}><RiReset class="menu-icon" />Reset every panel</ContextMenu.Item>
        </ContextMenu.Content>
      </ContextMenu.Root>
      {#if i === 0 && !rail}
        <Tip text="Hide the {side} dock">
          {#snippet children(tip)}
            <!-- Out of the Tab order, which steps between the panels; the panels' menu and the commands hide the dock too. -->
            <button {...tip} class="dock-button" aria-label="Hide the {side} dock" data-focus-skip onclick={(e) => (e.stopPropagation(), setHidden(true))}><HideIcon /></button>
          {/snippet}
        </Tip>
      {/if}
      {#if p.open && !p[SHADOW]}
        <div class="dock-body" data-focus-group="panel-{p.id}">{@render content(p.id)}</div>
      {/if}
    </section>
  {/each}
</div>
</div>
