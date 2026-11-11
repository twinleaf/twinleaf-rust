<script>
  // The layout tabs: drag to reorder, double-click or F2 to rename, and a
  // menu on right-click. The strip is one stop in the Tab order, on the open
  // tab, and the arrows move focus along it (keymap.js, `tabs`). A tab's
  // menu rows, its buttons and its keys run the tab commands (commands.js),
  // made on that tab.
  import { getContext, onDestroy, tick } from 'svelte';
  import { flip } from 'svelte/animate';
  import { dndzone } from 'svelte-dnd-action';
  import * as ContextMenu from '$lib/components/ui/context-menu/index.js';
  import IconButton from './IconButton.svelte';
  import CommandItem from './CommandItem.svelte';
  import RiCopy from '@lucide/svelte/icons/copy';
  import RiClose from '@lucide/svelte/icons/x';
  import RiAdd from '@lucide/svelte/icons/plus';
  import { plotIds } from './layout.js';
  import { claim } from './keyrouter.js';
  import { prefs } from './prefs.svelte.js';

  /** `onReorder(ids)` puts the tabs in the order of `ids`. */
  let { ws, drags, onReorder } = $props();
  const history = getContext('history');
  const commands = getContext('commands');
  /** Runs tab command `id`, made on tab `tab` from one of its buttons, or with no tab from the strip's. */
  const runOn = (id, tab = null) => commands.run(id, { tab, from: 'button' });

  const FLIP_MS = 150;
  let renaming = $state(null);
  /** Opens tab `id`'s name for editing, the name selected: what the Rename command has the page do. */
  export async function startRename(id) {
    renaming = id;
    await tick();
    document.querySelector(`[data-tab="${id}"] input`)?.select();
  }
  /** Ends a rename, focus going back to the tab. */
  async function endRename(id) {
    renaming = null;
    await tick();
    document.querySelector(`[data-tab="${id}"]`)?.focus();
  }
  function commitRename(t, name) {
    // Escape ends the rename first, so the blur that follows keeps nothing.
    if (renaming !== t.id) return;
    if (name.trim()) history.group('Rename tab', () => ws.renameTab(t.id, name.trim()));
    renaming = null;
  }
  let strip = $state(null);
  /** What the keys that move among the tabs do, by action. */
  const moves = {
    move(e, at) {
      const all = [...strip.querySelectorAll('[role=tab]')], i = all.indexOf(at);
      const to = { ArrowLeft: i - 1, ArrowRight: i + 1, Home: 0, End: all.length - 1 }[e.key];
      all[Math.max(0, Math.min(all.length - 1, to))].focus();
    },
    open: (e, at) => void ws.selectTab(at.dataset.tab),
  };
  // The keys of the tab with focus (keymap.js, place 'tabs'): one that
  // moves among the tabs, or else a tab command's, made on that tab. The
  // router takes them before the drag library's listener on the tab's slot
  // hears Space or Enter, which it would take for a keyboard drag; the tabs
  // move by pointer and by their keys here.
  onDestroy(
    claim('tabs', {
      when: (e, at) => at instanceof Element && at.matches('[role=tab]') && !!strip?.contains(at),
      on: (action, e, at) => (moves[action] ? moves[action](e, at) : void commands.run(action, { tab: at.dataset.tab, from: 'key' })),
    }),
  );
  // While a tab is dragged, the strip shows the drag library's order, its
  // placeholder among the tabs; the tabs themselves change once, at the drop.
  let dragged = $state.raw(null);
  const shown = $derived(dragged ?? ws.tabs);
  const consider = (e) => (dragged = e.detail.items);
  function finalize(e) {
    dragged = null;
    onReorder(e.detail.items.map((t) => t.id));
  }
</script>

<div class="flex cursor-crosshair flex-wrap items-center gap-1.5" role="group" aria-label="Layout tabs" data-focus="tabs" tabindex="-1" onpointerdown={(e) => drags.startMarquee(e)}>
  <div
    class="flex min-h-8 flex-wrap gap-1"
    onpointerdown={(e) => drags.startMarquee(e)}
    bind:this={strip}
    role="tablist"
    aria-label="Layouts"
    tabindex="-1"
    data-focus="tablist"
    data-focus-own
    use:dndzone={{ items: shown, type: 'tab', flipDurationMs: FLIP_MS, dropTargetStyle: {}, delayTouchStart: true, zoneTabIndex: -1, zoneItemTabIndex: -1, autoAriaDisabled: true }}
    onconsider={consider}
    onfinalize={finalize}
  >
    {#each shown as t (t.id)}
      <div class="tab-slot" animate:flip={{ duration: FLIP_MS }}>
        <ContextMenu.Root>
          <ContextMenu.Trigger>
            {#snippet child({ props })}
              <div
                {...props}
                class="tab"
                class:active={t.id === ws.activeId}
                class:target={drags.moving?.target?.tabId === t.id}
                data-tab={t.id}
                role="tab"
                tabindex={t.id === ws.activeId ? 0 : -1}
                aria-selected={t.id === ws.activeId}
                onclick={() => ws.selectTab(t.id)}
                ondblclick={() => runOn('renameTab', t.id)}
              >
                {#if renaming === t.id}
                  <!-- svelte-ignore a11y_autofocus -->
                  <input
                    class="w-32 rounded border border-input bg-background px-1 text-sm outline-none"
                    value={t.name}
                    autofocus
                    onclick={(e) => e.stopPropagation()}
                    onblur={(e) => commitRename(t, e.currentTarget.value)}
                    onkeydown={(e) => {
                      e.stopPropagation();
                      if (e.key === 'Enter') commitRename(t, e.currentTarget.value);
                      if (e.key === 'Enter' || e.key === 'Escape') endRename(t.id);
                    }}
                  />
                {:else}
                  <span title={prefs.hints ? 'Double-click to rename' : undefined}>{t.name}</span>
                  <span class="text-[11px] text-muted-foreground">{plotIds(t.layout).length}</span>
                {/if}
                <IconButton icon={RiCopy} label="Duplicate {t.name}" class="size-5" tabindex={-1} data-command="duplicateTab" onclick={(e) => (e.stopPropagation(), runOn('duplicateTab', t.id))} />
                <IconButton icon={RiClose} label="Close {t.name}" class="size-5" tabindex={-1} data-command="closeTab" onclick={(e) => (e.stopPropagation(), runOn('closeTab', t.id))} />
              </div>
            {/snippet}
          </ContextMenu.Trigger>
          <ContextMenu.Content class="menu-icons">
            <CommandItem id="renameTab" tab={t.id} />
            <CommandItem id="duplicateTab" tab={t.id} />
            <ContextMenu.Separator />
            <CommandItem id="closeTab" tab={t.id} />
          </ContextMenu.Content>
        </ContextMenu.Root>
      </div>
    {/each}
  </div>
  <IconButton icon={RiAdd} label="New layout" variant="outline" size="icon-sm" data-command="newTab" onclick={() => runOn('newTab')} />
</div>
