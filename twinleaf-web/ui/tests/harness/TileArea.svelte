<script>
  // The tiles of a Workspace's open tab, as the page lays them out, with an
  // empty box for each plot.
  import Tiles from '../../src/lib/Tiles.svelte';

  let { ws, history } = $props();
</script>

{#snippet box(id)}<div class="flex-1" data-box={id}></div>{/snippet}

<div class="tiles" style="width: 900px; height: 600px">
  {#if ws.layout}
    <Tiles layout={ws.layout} leafContent={box} onResize={(path, ratio) => history.group('Resize', () => ws.setRatio(path, ratio))} />
  {/if}
</div>
