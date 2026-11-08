<script>
  // The page's status along its foot: whether it reaches the hub, its
  // devices and how many are lost, what streams in, the raw history the hub
  // holds, and the frame rate. The keys and mouse gestures are a click away.
  import RiKeys from '@lucide/svelte/icons/keyboard';
  import RiWarn from '@lucide/svelte/icons/triangle-alert';
  import Tip from './Tip.svelte';
  import { prefs } from './prefs.svelte.js';
  import HistorySettings from './HistorySettings.svelte';
  import * as Popover from '$lib/components/ui/popover/index.js';
  import { bytesText as bytes, spanText as span } from './units.js';

  /** `hub` is the page's Hub, `hud` the frame stats, and `onKeys` opens the keys. */
  let { hub, hud, onKeys } = $props();

  const session = $derived(hub.session);
  const devices = $derived([...session.devices.values()]);
  const lost = $derived(devices.filter((d) => d.presence === 'lost'));
  const memory = $derived(session.memory);
  const full = $derived(memory?.budget ? Math.min(1, memory.bytes / memory.budget) : 0);
  const plural = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`;
</script>

<footer class="status-bar" data-focus="status" tabindex="-1">
  <span class="item" title={hub.connected ? 'The page reaches the hub' : 'The page lost the hub and tries again each second'}>
    <i class="dot" class:on={hub.connected}></i>{hub.connected ? 'Connected' : 'Reconnecting…'}
  </span>
  <span class="item">
    {plural(devices.length, 'device')}
    {#if lost.length}
      <span class="warn" title="Lost: {lost.map((d) => d.route).join(', ')}"><RiWarn />{lost.length} lost</span>
    {/if}
  </span>
  <span class="item">{plural(session.signals.length, 'signal')} · {hud.rxKBs.toFixed(0)} kB/s</span>
  {#if memory}
    <Popover.Root>
      <Tip text="Raw history: {bytes(memory.bytes)} of {bytes(memory.budget)}, at most {span(memory.maxAge)} of each stream">
        {#snippet children(tip)}
          <Popover.Trigger {...tip}>
            {#snippet child({ props })}
              <button {...props} class="item history">
                History {span(memory.seconds)}
                <span class="meter" aria-hidden="true"><span style="width: {full * 100}%"></span></span>
                {bytes(memory.bytes)}
              </button>
            {/snippet}
          </Popover.Trigger>
        {/snippet}
      </Tip>
      <Popover.Content class="w-72 p-3" align="start" side="top">
        <HistorySettings {memory} />
      </Popover.Content>
    </Popover.Root>
  {/if}
  <span class="spacer"></span>
  <span class="item" title="GPU: {hud.gpu || 'unknown'}">{hud.fps.toFixed(0)} fps</span>
  <Tip text="Every key and mouse gesture">
    {#snippet children(tip)}<button {...tip} class="item keys" onclick={onKeys}><RiKeys />Keys{#if prefs.hints}<kbd>?</kbd>{/if}</button>{/snippet}
  </Tip>
</footer>
