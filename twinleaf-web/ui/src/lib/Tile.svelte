<script>
  import Tile from './Tile.svelte';
  import * as Resizable from '$lib/components/ui/resizable/index.js';

  /** node: a layout node. leafContent: a snippet that renders one plot by id. */
  let { node, leafContent, drop, maximized, lifted, landed } = $props();
</script>

{#if node.type === 'leaf'}
  <div class="leaf" class:max={maximized === node.plotId} class:lifted={lifted?.has(node.plotId)} class:landed={landed?.has(node.plotId)} data-plot={node.plotId}>
    {@render leafContent(node.plotId)}
    {#if drop?.plotId === node.plotId}
      <div class="drop-hint {drop.edge}"></div>
    {/if}
  </div>
{:else}
  <!-- A split keeps its panes' sizes until the layout replaces it. -->
  {#key node.id}
    <Resizable.PaneGroup
      direction={node.dir === 'row' ? 'horizontal' : 'vertical'}
      onLayout={(sizes) => (node.ratio = sizes[0] / 100)}
      class="min-h-0 min-w-0 flex-1"
    >
      <Resizable.Pane defaultSize={node.ratio * 100} minSize={8} class="flex min-h-0 min-w-0">
        <Tile node={node.a} {leafContent} {drop} {maximized} {lifted} {landed} />
      </Resizable.Pane>
      <!-- Out of the Tab order, so Tab goes from plot to plot. -->
      <Resizable.Handle tabindex={-1} class="mx-[3px] bg-transparent transition-colors after:w-3 hover:bg-primary/60 data-[direction=vertical]:mx-0 data-[direction=vertical]:my-[3px] data-[direction=vertical]:after:h-3" />
      <Resizable.Pane defaultSize={(1 - node.ratio) * 100} minSize={8} class="flex min-h-0 min-w-0">
        <Tile node={node.b} {leafContent} {drop} {maximized} {lifted} {landed} />
      </Resizable.Pane>
    </Resizable.PaneGroup>
  {/key}
{/if}
