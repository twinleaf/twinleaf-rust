<script>
  // The page's status along its foot: whether it reaches the hub, its
  // devices and how many are lost, what streams in, what of it was lost in
  // the last minute, the raw history the hub holds, and the frame rate. The
  // keys and mouse gestures are a click away.
  //
  // The loss item shows only while the last minute lost something, or its
  // popover is open: each cause apart, the one that stands for the most
  // data first (loss.svelte.js `recent`), in words.js's `lossWords`. Its
  // popover counts what was lost since the page opened (LossCounts.svelte).
  import RiKeys from '@lucide/svelte/icons/keyboard';
  import RiWarn from '@lucide/svelte/icons/triangle-alert';
  import Tip from './Tip.svelte';
  import HistorySettings from './HistorySettings.svelte';
  import LossCounts from './LossCounts.svelte';
  import { LOSS_NONE_RECENT, LOSS_TIP, lossWords } from './words.js';
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
  const loss = $derived(hub.loss);
  let lossOpen = $state(false);
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
  <span class="item"><span>{plural(session.signals.length, 'signal')} · <span class="num">{hud.rxKBs.toFixed(0)}</span> kB/s</span></span>
  {#if loss.recent.length || lossOpen}
    <Popover.Root bind:open={lossOpen}>
      <Tip text={LOSS_TIP}>
        {#snippet children(tip)}
          <Popover.Trigger {...tip}>
            {#snippet child({ props })}
              <button {...props} class="item loss">
                <RiWarn />
                {#each loss.recent as cause, i (cause.kind)}
                  {#if i}<span aria-hidden="true">·</span>{/if}
                  <span class="cause" data-cause={cause.kind}>{#each lossWords(cause) as part}{#if part.num}<span class="num" style:min-width="{part.ch}ch">{part.num}</span>{:else}{part}{/if}{/each}</span>
                {:else}
                  <span class="cause">{LOSS_NONE_RECENT}</span>
                {/each}
              </button>
            {/snippet}
          </Popover.Trigger>
        {/snippet}
      </Tip>
      <Popover.Content class="w-96 p-3" align="start" side="top">
        <LossCounts {loss} devices={session.devices} />
      </Popover.Content>
    </Popover.Root>
  {/if}
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
    {#snippet children(tip)}<button {...tip} class="item keys" onclick={onKeys}><RiKeys />Keys<kbd data-hint>?</kbd></button>{/snippet}
  </Tip>
</footer>
