<script>
  // The layout tabs: drag to reorder, double-click or F2 to rename, and a
  // menu on right-click. The strip is one stop in the Tab order, on the open
  // tab, and the arrows move focus along it (keymap.js, `tabs`).
  import { tick } from 'svelte';
  import { flip } from 'svelte/animate';
  import { dndzone } from 'svelte-dnd-action';
  import * as ContextMenu from '$lib/components/ui/context-menu/index.js';
  import IconButton from './IconButton.svelte';
  import RiCopy from '@lucide/svelte/icons/copy';
  import RiClose from '@lucide/svelte/icons/x';
  import RiAdd from '@lucide/svelte/icons/plus';
  import { plotIds } from './layout.js';
  import { actionFor } from './keymap.js';

  /** `onClose(t)` closes tab `t`, asking first when it has plots. */
  let { ws, drags, onClose } = $props();

  const FLIP_MS = 150;
  let renaming = $state(null);
  /** Opens tab `id`'s name for editing, the name selected. */
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
    if (name.trim()) t.name = name.trim();
    renaming = null;
  }
  function tabKey(e, t) {
    if (e.target !== e.currentTarget) return;
    const all = [...e.currentTarget.closest('[role=tablist]').querySelectorAll('[role=tab]')];
    const i = all.indexOf(e.currentTarget);
    const action = actionFor('tabs', e);
    if (action === 'move') {
      const to = { ArrowLeft: i - 1, ArrowRight: i + 1, Home: 0, End: all.length - 1 }[e.key];
      all[Math.max(0, Math.min(all.length - 1, to))].focus();
    } else if (action === 'open') ws.selectTab(t.id);
    else if (action === 'rename') startRename(t.id);
    else if (action === 'close') onClose(t);
    else return;
    e.preventDefault();
    e.stopPropagation();
  }
  const reorder = (e) => (ws.tabs = e.detail.items);
</script>

<div class="flex cursor-crosshair flex-wrap items-center gap-1.5" role="group" aria-label="Layout tabs" data-focus="tabs" tabindex="-1" onpointerdown={(e) => drags.startMarquee(e)}>
  <div
    class="flex min-h-8 flex-wrap gap-1"
    onpointerdown={(e) => drags.startMarquee(e)}
    role="tablist"
    aria-label="Layouts"
    tabindex="-1"
    data-focus="tablist"
    data-focus-own
    use:dndzone={{ items: ws.tabs, type: 'tab', flipDurationMs: FLIP_MS, dropTargetStyle: {}, delayTouchStart: true, zoneTabIndex: -1, zoneItemTabIndex: -1, autoAriaDisabled: true }}
    onconsider={reorder}
    onfinalize={reorder}
  >
    {#each ws.tabs as t (t.id)}
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
                onkeydown={(e) => tabKey(e, t)}
                ondblclick={() => startRename(t.id)}
              >
                {#if renaming === t.id}
                  <!-- svelte-ignore a11y_autofocus -->
                  <input
                    class="w-32 rounded border border-ring bg-background px-1 text-sm outline-none"
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
                  <span title="Double-click to rename">{t.name}</span>
                  <span class="text-[11px] text-muted-foreground">{plotIds(t.layout).length}</span>
                {/if}
                <IconButton icon={RiCopy} label="Duplicate {t.name}" class="size-5" tabindex={-1} onclick={(e) => (e.stopPropagation(), ws.duplicateTab(t))} />
                <IconButton icon={RiClose} label="Close {t.name}" class="size-5" tabindex={-1} onclick={(e) => (e.stopPropagation(), onClose(t))} />
              </div>
            {/snippet}
          </ContextMenu.Trigger>
          <ContextMenu.Content>
            <ContextMenu.Item onSelect={() => startRename(t.id)}>Rename<ContextMenu.Shortcut><kbd>F2</kbd></ContextMenu.Shortcut></ContextMenu.Item>
            <ContextMenu.Item onSelect={() => ws.duplicateTab(t)}>Duplicate</ContextMenu.Item>
            <ContextMenu.Separator />
            <ContextMenu.Item variant="destructive" onSelect={() => onClose(t)}>Close</ContextMenu.Item>
          </ContextMenu.Content>
        </ContextMenu.Root>
      </div>
    {/each}
  </div>
  <IconButton icon={RiAdd} label="New layout" variant="outline" size="icon-sm" onclick={() => ws.addTab()} />
</div>
