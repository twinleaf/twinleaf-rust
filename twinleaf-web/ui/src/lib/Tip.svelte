<script>
  // A button's tooltip: what it does, and its keys when it has any. Every
  // button the label alone does not explain takes one, so it shows on hover
  // and on keyboard focus alike. Text, inputs, drag handles and other
  // passive things keep the browser's `title`, which stays out of the way
  // while one types or drags.
  //
  // `children(props)` draws the button with the trigger's props. A menu's
  // trigger takes them, and merges them with its own.
  import * as Tooltip from '$lib/components/ui/tooltip/index.js';

  let { text, keys = [], children } = $props();
</script>

<Tooltip.Root>
  <Tooltip.Trigger>
    {#snippet child({ props })}{@render children(props)}{/snippet}
  </Tooltip.Trigger>
  <Tooltip.Content>
    {text}{#each keys as key}<kbd data-slot="kbd">{key}</kbd>{/each}
  </Tooltip.Content>
</Tooltip.Root>
