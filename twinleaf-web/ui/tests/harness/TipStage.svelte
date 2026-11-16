<script>
  // A button with a tooltip (Tip.svelte) reading `text`, fixed at `left` and
  // `top` of the window, inside the tooltips' provider as the page has it.
  import * as Tooltip from '$lib/components/ui/tooltip/index.js';
  import Tip from '../../src/lib/Tip.svelte';

  let { text, left = 0, top = 0 } = $props();
</script>

<Tooltip.Provider delayDuration={0}>
  <Tip {text}>
    {#snippet children(tip)}<button {...tip} class="tip-stage" aria-label="A button with a tooltip" style="position: fixed; left: {left}px; top: {top}px; width: 16px; height: 16px; z-index: 40"></button>{/snippet}
  </Tip>
</Tooltip.Provider>
