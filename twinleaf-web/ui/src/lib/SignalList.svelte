<script module>
  /** The signals of `signals` with every word of `query` in their key, or as their units. */
  export function matching(signals, query) {
    const words = query.toLowerCase().split(/\s+/).filter(Boolean);
    return signals.filter((t) => words.every((w) => t.key.toLowerCase().includes(w) || (t.meta.units ?? '').toLowerCase() === w));
  }
</script>

<script>
  // Every signal the hub has, or those the search matches, to pick, to drag
  // onto the tiles and to put on the selected plots.
  //
  // The search matches every word typed against the signal's key and units.
  // Signals are picked as the rows of every list are (selection.svelte.js):
  // a click picks one, Ctrl adds or drops one, Shift picks a run, and the
  // arrows pick the signal they move to, in the list or from its search,
  // extending with Shift. What puts signals on plots is a command
  // (commands.js), made on a signal from its menu, the list's keys or a
  // double-click, so it acts on the signals picked when that signal is one
  // of them. Dragging a signal out copies it, and the signals picked with
  // it, and nothing drops back in.
  import { getContext, onDestroy, tick } from 'svelte';
  import { flip } from 'svelte/animate';
  import { dndzone, TRIGGERS, SHADOW_ITEM_MARKER_PROPERTY_NAME as SHADOW } from 'svelte-dnd-action';
  import { ScrollArea } from '$lib/components/ui/scroll-area/index.js';
  import { Input } from '$lib/components/ui/input/index.js';
  import { colorOf } from './plot.js';
  import { colors } from './colors.svelte.js';
  import Swatch from './Swatch.svelte';
  import ColorPicker from './ColorPicker.svelte';
  import CommandItem from './CommandItem.svelte';
  import * as ContextMenu from '$lib/components/ui/context-menu/index.js';
  import RiPalette from '@lucide/svelte/icons/palette';
  import RiCopy from '@lucide/svelte/icons/clipboard-copy';
  import { COMMANDS } from './commands.js';
  import { stepIn } from './keymap.js';
  import { claim } from './keyrouter.js';
  import { SIGNAL } from './selection.svelte.js';
  import { uid } from './tabs.js';

  /** `drags` is the page's Drags, and `query` what the search holds, which the page reads for the signals shown. */
  let { session, drags, query = $bindable('') } = $props();
  const selection = getContext('selection');
  const commands = getContext('commands');

  const FLIP_MS = 150;
  /** The keys the search gives the list; every other key is the field's own. */
  const FROM_SEARCH = ['ArrowUp', 'ArrowDown', 'Enter'];
  /** The commands made on a signal, whose keys are the list's and whose rows its menu's. */
  const ON_SIGNAL = COMMANDS.filter((c) => c.place === 'list');
  let palette = $state([]);
  let dragging = false;
  const refresh = () => (palette = matching(session.signals, query).map((t) => ({ id: `pal:${t.key}`, key: t.key })));
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
      const key = palette[idx].key;
      palette = items;
      dragging = true;
      drags.traceDragStart(selection.signalTargets(key).filter((k) => k !== key));
    } else if (!dragging) {
      palette = e.detail.items;
    } else {
      palette = [...palette];
    }
  }
  function finalize(e) {
    dragging = false;
    // A plot that took the signal lands it; a drop the library counts as
    // back here, or in no zone, lands where the pointer was let go.
    const { id, trigger } = e.detail.info;
    if (trigger !== TRIGGERS.DROPPED_INTO_ANOTHER) drags.landSignal({ id, key: id.slice('pal:'.length) });
    refresh();
  }
  /** The signal lifted says how many signals go with it, when it is one of several picked. */
  function lifted(el, item) {
    const n = item ? selection.signalTargets(item.key).length : 1;
    if (n > 1) el.dataset.count = n;
  }

  let menuOpen = $state(false);
  let menuKey = $state(null); // the signal a right-click opened the menu for
  const picked = $derived(selection.signals);
  /** The row the arrows are on: the signal picked or moved to last, or the first while that is none the list shows. */
  const active = $derived(Math.max(0, palette.findIndex((i) => SIGNAL + i.key === selection.rowAt)));
  let focused = $state(false);
  /** Whether the search has focus, where Enter adds the signal the arrows are on. */
  let searching = $state(false);
  let search = $state(null);
  let list = $state(null);
  /** Shows the row the arrows are on, however the list got focus, and brings it into view. */
  function show() {
    focused = true;
    tick().then(() => document.getElementById(`pal-${palette[active]?.id}`)?.scrollIntoView({ block: 'nearest' }));
  }
  /** Picks row `i` as a click with `mods`' keys does (`Selection.pickRow`). */
  function pick(i, mods) {
    if (palette[i]) selection.pickRow(SIGNAL + palette[i].key, mods);
  }
  /** Runs command `id` on the signal the arrows are on, from the keys: the key is the list's whenever there is one. */
  function made(id) {
    const key = palette[active]?.key;
    if (!key) return false;
    commands.run(id, { signal: key, from: 'key' });
    show();
  }
  // The list's keys (keymap.js, place 'list'), pressed in it, or in its
  // search for those the search gives it.
  onDestroy(
    claim('list', {
      when: (e, at) => list?.contains(at) || (at === search && FROM_SEARCH.includes(e.key)),
      on: {
        move(e) {
          // The arrows pick the signal they move to; with Shift they pick the run to it.
          pick(stepIn(e, active, palette.length), { shiftKey: e.shiftKey });
          show();
        },
        pickAll: () => palette.length > 0 && void selection.pickRows(palette.map((i) => SIGNAL + i.key)),
        ...Object.fromEntries(ON_SIGNAL.map((c) => [c.id, () => made(c.id)])),
      },
    }),
  );
  /** Escape in the search clears what is typed, before it does anything else. */
  function searchKey(e) {
    if (e.key !== 'Escape' || !query) return;
    query = '';
    e.preventDefault();
  }
</script>

<Input
  class="h-7 text-xs"
  type="search"
  bind:ref={search}
  bind:value={query}
  placeholder="Search {session.signals.length} signals"
  title="Drag a signal onto the plots, or double-click it to add it to the selected ones"
  aria-label="Search signals: arrows move, Enter adds to the selected plots, Shift+Enter makes a new one"
  onkeydown={searchKey}
  onfocus={() => (focused = searching = true)}
  onblur={() => (focused = searching = false)}
/>
<!-- A signal's menu: where it and the signals picked with it go, its colour, its name. -->
<ContextMenu.Root bind:open={menuOpen}>
<ContextMenu.Trigger>
{#snippet child({ props })}
<ScrollArea class="palette-scroll" type="auto">
  <div
    {...props}
    oncontextmenucapture={(e) => {
      // A right-click on a signal not picked picks it alone, as a sheet does.
      menuKey = e.target.closest?.('.chip')?.dataset.key ?? null;
      if (menuKey) selection.rowMenu(SIGNAL + menuKey);
      else e.preventDefault(), e.stopPropagation();
    }}
    class="palette"
    role="listbox"
    aria-multiselectable="true"
    tabindex="0"
    data-focus-own
    aria-label="Signals: Enter adds to the selected plots, Shift+Enter makes a new one"
    aria-activedescendant={focused && palette[active] ? `pal-${palette[active].id}` : undefined}
    use:dndzone={{ items: palette, type: 'trace', flipDurationMs: FLIP_MS, dropFromOthersDisabled: true, dropTargetStyle: {}, delayTouchStart: true, zoneTabIndex: 0, zoneItemTabIndex: -1, autoAriaDisabled: true, transformDraggedElement: lifted }}
    onconsider={consider}
    onfinalize={finalize}
    bind:this={list}
    onfocus={(e) => (focused = e.currentTarget.matches(':focus-visible'))}
    onblur={() => (focused = false)}
  >
    {#each palette as item, i (item.id)}
      {@const t = session.trace(item.key)}
      <!-- An option of the list, which takes its keys, so it can hold the colour button. -->
      <!-- svelte-ignore a11y_click_events_have_key_events -->
      <div
        id="pal-{item.id}"
        data-key={item.key}
        class="chip"
        class:picked={picked.has(item.key)}
        class:chosen={searching && i === active}
        data-menu-for={menuOpen && item.key === menuKey ? '' : undefined}
        role="option"
        aria-selected={picked.has(item.key)}
        tabindex="-1"
        style="--c: {t ? colorOf(t) : 'var(--muted-foreground)'}"
        animate:flip={{ duration: FLIP_MS }}
        onclick={(e) => (pick(i, e), list?.focus())}
        ondblclick={() => commands.run('addToPlots', { signal: item.key, from: 'row' })}
      >
        <Swatch key={item.key} /><span class="sig-key">{item.key}</span>{#if t?.meta.units}<span class="sig-units">{t.meta.units}</span>{/if}<small>{t?.meta.rate} Hz</small>
      </div>
    {/each}
  </div>
  {#if !palette.length}<div class="sig-empty">{session.signals.length ? 'No signal matches' : 'No signal yet'}</div>{/if}
</ScrollArea>
{/snippet}
</ContextMenu.Trigger>
<ContextMenu.Content class="menu-icons">
  {#if menuKey}
    {#each ON_SIGNAL as c (c.id)}<CommandItem id={c.id} signal={menuKey} />{/each}
    <ContextMenu.Sub>
      <ContextMenu.SubTrigger><RiPalette class="menu-icon" />Colour<span class="menu-value"><i style="--c: {colorOf({ key: menuKey })}"></i>{#if colors.picked[menuKey] === undefined}Auto{/if}</span></ContextMenu.SubTrigger>
      <ContextMenu.SubContent class="p-1.5"><ColorPicker key={menuKey} ondone={() => (menuOpen = false)} /></ContextMenu.SubContent>
    </ContextMenu.Sub>
    <ContextMenu.Separator />
    <ContextMenu.Item onSelect={() => navigator.clipboard.writeText(menuKey)}><RiCopy class="menu-icon" />Copy name</ContextMenu.Item>
  {/if}
</ContextMenu.Content>
</ContextMenu.Root>
