<script>
  // The tiles of a Workspace's open tab, as the page lays them out, with an
  // empty box for each plot.
  import { setContext } from 'svelte';
  import Tile from '../../src/lib/Tile.svelte';

  let { ws, history } = $props();
  setContext('history', history);
</script>

{#snippet box(id)}<div class="flex-1" data-box={id}></div>{/snippet}

<div class="tiles" style="width: 900px; height: 600px">
  {#if ws.layout}
    <Tile node={ws.layout} leafContent={box} />
  {/if}
</div>
