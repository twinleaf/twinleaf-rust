<script>
  // The devices the hub's connection sees, each with a checkbox that says
  // whether the Properties panels show it. Every device starts checked, so
  // one device needs no clicks; `unchecked` holds the routes unchecked, one
  // set per hub, which the parent shares with the Properties panels.
  import { Button } from '$lib/components/ui/button/index.js';
  import { Checkbox } from '$lib/components/ui/checkbox/index.js';
  import { ScrollArea } from '$lib/components/ui/scroll-area/index.js';
  import DevicePicker from './DevicePicker.svelte';
  import RiSearch from '@lucide/svelte/icons/search';
  import { byRoute } from './properties.js';

  let { session, unchecked } = $props();

  const devices = $derived([...session.devices.values()].sort(byRoute));

  let pickerOpen = $state(false);
</script>

<div class="devices">
  <ScrollArea class="device-list" type="auto">
    <div role="list" aria-label="Devices">
      {#each devices as d (d.route)}
        <label class="device" role="listitem" title="{d.serial ?? ''} {d.firmware ?? ''}">
          <Checkbox aria-label="Show {d.route} in the panels" checked={!unchecked.has(d.route)} onCheckedChange={(on) => (on ? unchecked.delete(d.route) : unchecked.add(d.route))} />
          <i class="presence {d.presence}" aria-label={d.presence}></i>
          <span class="route">{d.route}</span>
          <span class="name">{d.name ?? '…'}</span>
          <span class="serial">{d.serial ?? ''}</span>
        </label>
      {:else}
        <div class="empty">No device yet</div>
      {/each}
    </div>
  </ScrollArea>
  <Button variant="outline" size="xs" class="w-full" onclick={() => (pickerOpen = true)}><RiSearch />Find devices…</Button>
</div>

<DevicePicker {session} bind:open={pickerOpen} />
