<script>
  import { Button } from '$lib/components/ui/button/index.js';
  import Tip from './Tip.svelte';

  /** An icon button whose label shows as a tooltip, with its `keys`, and names it for screen readers. */
  let { icon: Icon, label, keys = [], onclick, onpointerdown, variant = 'ghost', size = 'icon-xs', class: className = '', ...rest } = $props();
</script>

<Tip text={label} {keys}>
  {#snippet children(props)}
    <Button {...props} {...rest} {variant} {size} class={className} aria-label={label} {onclick} {onpointerdown}>
      <Icon />
    </Button>
  {/snippet}
</Tip>
