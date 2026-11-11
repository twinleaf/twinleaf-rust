<script>
  // A command's row in a context menu or a dropdown menu, as its entry in
  // the command table reads (commands.js): its icon, label, value and key,
  // whether it can be chosen, and its destructive look; no row where the
  // entry says the menu has none. Made on plot `on`, or from the page with
  // none; on signal `signal`'s row; with `how` what the menu adds.
  import { getContext } from 'svelte';
  import * as ContextMenu from '$lib/components/ui/context-menu/index.js';
  import * as DropdownMenu from '$lib/components/ui/dropdown-menu/index.js';
  import RiWarn from '@lucide/svelte/icons/triangle-exclamation-point';

  let { id, on = null, signal = null, how = undefined, menu = 'context' } = $props();
  const commands = getContext('commands');
  const Menu = $derived(menu === 'dropdown' ? DropdownMenu : ContextMenu);
  const row = $derived(commands.row(id, { on, signal, how }));
  const value = $derived(typeof row.value === 'string' ? { text: row.value } : row.value);
</script>

<!-- One line, so no space comes between the row's parts. -->
{#if row.offered}<Menu.Item data-command={id} variant={row.destructive ? 'destructive' : undefined} disabled={row.disabled} onSelect={() => commands.run(id, { on, signal, how, from: 'menu' })}><row.icon class="menu-icon" />{row.label}{#if value}<span class="menu-value" class:warn={value.warn} title={value.title}>{#if value.warn}<RiWarn />{/if}{#if value.icon}<value.icon />{/if}{value.text}</span>{/if}{#if row.show}<Menu.Shortcut>{#each row.show as key}<kbd>{key}</kbd>{/each}</Menu.Shortcut>{/if}</Menu.Item>{/if}
