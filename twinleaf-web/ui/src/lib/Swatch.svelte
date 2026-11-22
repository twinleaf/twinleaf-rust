<script>
  // A signal's colour line, which opens its colour picker (ColorPicker.svelte).
  import { getContext } from 'svelte';
  import * as Popover from '$lib/components/ui/popover/index.js';
  import ColorPicker from './ColorPicker.svelte';
  import { NAMES } from './colors.svelte.js';

  /** `key` is the signal whose colour this shows and picks. */
  let { key } = $props();
  /** The Colors it is shown among: the page's, or within a plot, its host's. */
  const colors = getContext('colors');
  let open = $state(false);
  const current = $derived(colors.of(key));
</script>

<Popover.Root bind:open>
  <!-- The line's presses are its own, not the row's or the plot's around it. -->
  <Popover.Trigger>
    {#snippet child({ props })}
      <button {...props} class="swatch" tabindex="-1" aria-label="Colour of {key}: {typeof current === 'string' ? current : NAMES[current]}" title="Change the colour" onpointerdown={(e) => (e.stopPropagation(), props.onpointerdown?.(e))} onclick={(e) => (e.stopPropagation(), props.onclick?.(e))}><i></i></button>
    {/snippet}
  </Popover.Trigger>
  <Popover.Content class="w-auto gap-0 p-1.5" align="start">
    {#if open}<ColorPicker {key} ondone={() => (open = false)} />{/if}
  </Popover.Content>
</Popover.Root>
