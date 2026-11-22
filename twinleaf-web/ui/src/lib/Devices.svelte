<script>
  // The devices the hub's connection sees, each with a checkbox that says
  // whether the Properties panels show it. Every device starts checked, so
  // one device needs no clicks; `unchecked` holds the routes unchecked, one
  // set per hub, which the parent shares with the Properties panels.
  //
  // Under a device's name is one line, in every state, so a row is one
  // height and what is under it never moves: the firmware it runs, once
  // the hub has read it, and at the line's right what firmware.js
  // `rowState` says of it, a newer release, which a press on it offers to
  // install, or how the device stands to the catalog. An upgrade of it
  // takes the line in the version's place: its step, the percent uploaded
  // and a meter, which a press shows in the dialog. What the line has no
  // room for is cut short, the version first and then the words, and says
  // itself whole in its tooltip; an upgrade's number keeps its width, and
  // what is pressed stays in view at any width of the dock. The row's menu, the
  // panel's (Dock.svelte) and the buttons under the list run the firmware
  // commands (commands.js).
  import { getContext } from 'svelte';
  import { Button } from '$lib/components/ui/button/index.js';
  import { Checkbox } from '$lib/components/ui/checkbox/index.js';
  import { ScrollArea } from '$lib/components/ui/scroll-area/index.js';
  import * as ContextMenu from '$lib/components/ui/context-menu/index.js';
  import CommandItem from './CommandItem.svelte';
  import DevicePicker from './DevicePicker.svelte';
  import IconButton from './IconButton.svelte';
  import RiSearch from '@lucide/svelte/icons/search';
  import { byRoute } from './properties.js';
  import { hasUpdate, releaseText, rowState, versionShort, versionText } from './firmware.js';

  let { session, unchecked } = $props();
  const commands = getContext('commands');

  const devices = $derived([...session.devices.values()].sort(byRoute));
  /** The rows of the commands the panel's buttons run, as the table words them. */
  const check = $derived(commands.row('checkFirmware'));
  const all = $derived(commands.row('upgradeAll'));

  let pickerOpen = $state(false);
  /** What device `d`'s pressable item says in its tooltip: its words whole, as they may be cut short, and what a press does. */
  function offerTitle(d, state) {
    if (state.kind === 'progress') return `${state.text}${state.percent === null ? '' : ` ${state.percent}%`}: show the upgrade`;
    return state.kind === 'update' ? `${state.text}: upgrade firmware to ${releaseText(d.fw.catalog.releases[0])}…` : `${state.text}: show why`;
  }
  /** Opens the upgrade dialog of the device at `route`. */
  const upgrade = (route) => commands.run('upgradeFirmware', { how: { to: route }, from: 'button' });
</script>

<div class="devices">
  <ScrollArea class="device-list" type="auto">
    <div role="list" aria-label="Devices">
      {#each devices as d (d.route)}
        {@const state = rowState(d)}
        <ContextMenu.Root>
          <ContextMenu.Trigger>
            {#snippet child({ props })}
              <label {...props} class="device" role="listitem" title="{d.serial ?? ''} {d.firmware ?? ''}">
                <Checkbox aria-label="Show {d.route} in the panels" checked={!unchecked.has(d.route)} onCheckedChange={(on) => (on ? unchecked.delete(d.route) : unchecked.add(d.route))} />
                <i class="presence {d.presence}" aria-label={d.presence}></i>
                <span class="route">{d.route}</span>
                <span class="name">{d.name ?? '…'}</span>
                <span class="serial">{d.serial ?? ''}</span>
                <span class="firmware" data-firmware={state?.kind ?? 'none'}>
                  {#if state?.kind !== 'progress'}
                    <span class="version" title={d.fw?.installed ? `Runs firmware ${versionText(d.fw.installed)}` : undefined}>{d.fw?.installed ? versionShort(d.fw.installed) : '…'}</span>
                  {/if}
                  {#if state?.kind === 'note'}
                    <span class="said" title={state.text}>{state.text}</span>
                  {:else if state}
                    <!-- One button whatever it offers, so focus on it holds as an upgrade starts and ends. -->
                    <button class="offer {state.kind}" title={offerTitle(d, state)} onclick={() => upgrade(d.route)}>
                      {#if state.kind === 'progress'}<span class="word">{state.text}</span><span class="fit" data-room="100%"><span>{state.percent === null ? '' : `${state.percent}%`}</span></span><span class="meter"><span style="width: {state.percent ?? 0}%"></span></span>{:else}{state.text}{/if}
                    </button>
                  {/if}
                </span>
              </label>
            {/snippet}
          </ContextMenu.Trigger>
          <ContextMenu.Content class="menu-icons">
            <CommandItem id="upgradeFirmware" how={{ to: d.route }} />
            <CommandItem id="checkFirmware" />
          </ContextMenu.Content>
        </ContextMenu.Root>
      {:else}
        <div class="empty">No device yet</div>
      {/each}
    </div>
  </ScrollArea>
  {#if devices.filter(hasUpdate).length > 1}
    <Button variant="outline" size="xs" class="w-full" data-command="upgradeAll" onclick={() => commands.run('upgradeAll', { from: 'button' })}><all.icon />{all.label}</Button>
  {/if}
  <div class="device-buttons">
    <Button variant="outline" size="xs" class="min-w-0 flex-1" onclick={() => (pickerOpen = true)}><RiSearch />Find devices…</Button>
    <IconButton icon={check.icon} label={check.label} variant="outline" disabled={check.disabled} data-command="checkFirmware" onclick={() => commands.run('checkFirmware', { from: 'button' })} />
  </div>
</div>

<DevicePicker {session} bind:open={pickerOpen} />
