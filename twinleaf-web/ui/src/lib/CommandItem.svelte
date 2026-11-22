<script>
  // A command's row in a context menu or a dropdown menu, as its entry in
  // the command table reads (commands.js): its icon, label, value and key,
  // whether it can be chosen, and its destructive look; no row where the
  // entry says the menu has none. A command of choices has a row that opens
  // a submenu of them, the one its plot has marked. Made on plot `on`, or
  // from the page with none; on signal `signal`'s row; on tab `tab`; with
  // `how` what the menu adds. `onran(e)`, where given, hears the row
  // chosen, once the command has run.
  import { getContext } from 'svelte';
  import * as ContextMenu from '$lib/components/ui/context-menu/index.js';
  import * as DropdownMenu from '$lib/components/ui/dropdown-menu/index.js';
  import RiWarn from '@lucide/svelte/icons/triangle-exclamation-point';

  let { id, on = null, signal = null, tab = null, how = undefined, menu = 'context', onran = undefined } = $props();
  const commands = getContext('commands');
  const Menu = $derived(menu === 'dropdown' ? DropdownMenu : ContextMenu);
  const row = $derived(commands.row(id, { on, signal, tab, how }));
  const value = $derived(typeof row.value === 'string' ? { text: row.value } : row.value);
  /** Runs the command with `how`, from this row. */
  const run = (how) => commands.run(id, { on, signal, tab, how, from: 'menu' });
</script>

<!-- A row's value in its trailing column, when it has one. -->
{#snippet trailing()}{#if value}<span class="menu-value" class:warn={value.warn} title={value.title} aria-label={value.label}>{#if value.warn}<RiWarn />{/if}{#if value.icon}<value.icon />{/if}{value.text}</span>{/if}{/snippet}
<!-- One line each, so no space comes between a row's parts. -->
{#if row.offered && row.choices}
  <Menu.Sub>
    <Menu.SubTrigger data-command={id} disabled={row.disabled}><row.icon class="menu-icon" />{row.label}{@render trailing()}</Menu.SubTrigger>
    <Menu.SubContent>
      {#if row.chosen !== undefined}
        <Menu.RadioGroup value={row.chosen} onValueChange={(to) => run({ ...how, to })}>
          {#each row.choices as [to, name]}<Menu.RadioItem value={to}>{name}</Menu.RadioItem>{/each}
        </Menu.RadioGroup>
      {:else}
        {#each row.choices as [to, name] (to)}<Menu.Item onSelect={() => run({ ...how, to })}>{name}</Menu.Item>{/each}
      {/if}
    </Menu.SubContent>
  </Menu.Sub>
{:else if row.offered}<Menu.Item data-command={id} variant={row.destructive ? 'destructive' : undefined} disabled={row.disabled} onSelect={(e) => (run(how), onran?.(e))}><row.icon class="menu-icon" />{row.label}{@render trailing()}{#if row.show}<Menu.Shortcut>{#each row.show as key}<kbd>{key}</kbd>{/each}</Menu.Shortcut>{/if}</Menu.Item>{/if}
