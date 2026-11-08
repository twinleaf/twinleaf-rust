<script>
  // The page's status along its foot: whether it reaches the hub, its
  // devices and how many are lost, what streams in, the raw history the hub
  // holds, and the frame rate. The keys and mouse gestures are a click away.
  import RiKeys from '@lucide/svelte/icons/keyboard';
  import RiWarn from '@lucide/svelte/icons/triangle-alert';
  import Tip from './Tip.svelte';

  /** `hub` is the page's Hub, `hud` the frame stats, and `onKeys` opens the keys. */
  let { hub, hud, onKeys } = $props();

  const session = $derived(hub.session);
  const devices = $derived([...session.devices.values()]);
  const lost = $derived(devices.filter((d) => d.presence === 'lost'));
  const memory = $derived(session.memory);
  const full = $derived(memory?.capacity ? Math.min(1, memory.bytes / memory.capacity) : 0);

  const bytes = (n) => (n >= 1e9 ? `${(n / 1e9).toFixed(2)} GB` : n >= 1e6 ? `${(n / 1e6).toFixed(1)} MB` : `${Math.round(n / 1e3)} kB`);
  const span = (s) => (s >= 120 ? `${Math.round(s / 60)} min` : `${Math.round(s)} s`);
  const plural = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`;
</script>

<footer class="status-bar">
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
    <span
      class="item"
      title="The hub holds {bytes(memory.bytes)} of raw rows, of {bytes(memory.capacity)} once every stream is full. The stream that keeps the least holds {span(memory.seconds)}."
    >
      History {span(memory.seconds)}
      <span class="meter" aria-hidden="true"><span style="width: {full * 100}%"></span></span>
      {bytes(memory.bytes)}
    </span>
  {/if}
  <span class="spacer"></span>
  <span class="item" title="GPU: {hud.gpu || 'unknown'}">{hud.fps.toFixed(0)} fps</span>
  <Tip text="Every key and mouse gesture">
    {#snippet children(tip)}<button {...tip} class="item keys" onclick={onKeys}><RiKeys />Keys<kbd>?</kbd></button>{/snippet}
  </Tip>
</footer>
