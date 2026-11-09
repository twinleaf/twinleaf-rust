<script>
  import { getContext, untrack } from 'svelte';
  import Tile from './Tile.svelte';
  import * as Resizable from '$lib/components/ui/resizable/index.js';

  /** node: a layout node. leafContent: a snippet that renders one plot by id. */
  let { node, leafContent, drop, maximized, lifted, landed } = $props();

  // A drag of the split's handle sets its ratio, as one undo step, and an
  // undo that puts the split back moves the panes there, which keep their own
  // sizes otherwise. Only a drag sets it: the sizes the panes settle on as
  // they mount are theirs, not an edit.
  const history = getContext('history');
  let group = $state();
  let dragging = false;
  function draggingChanged(now) {
    if (now === dragging) return;
    dragging = now;
    if (now) history?.begin('Resize');
    else history?.end();
  }
  $effect(() => {
    const want = node.ratio * 100;
    untrack(() => {
      const at = group?.getLayout?.()[0];
      if (at !== undefined && Math.abs(at - want) > 0.1) group.setLayout([want, 100 - want]);
    });
  });
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
      bind:this={group}
      direction={node.dir === 'row' ? 'horizontal' : 'vertical'}
      onLayoutChange={(sizes) => dragging && (node.ratio = sizes[0] / 100)}
      class="min-h-0 min-w-0 flex-1"
    >
      <Resizable.Pane defaultSize={node.ratio * 100} minSize={8} class="flex min-h-0 min-w-0">
        <Tile node={node.a} {leafContent} {drop} {maximized} {lifted} {landed} />
      </Resizable.Pane>
      <!-- Out of the Tab order, so Tab goes from plot to plot. -->
      <Resizable.Handle tabindex={-1} onDraggingChange={draggingChanged} class="mx-[3px] bg-transparent transition-colors after:w-3 hover:bg-primary/60 data-[direction=vertical]:mx-0 data-[direction=vertical]:my-[3px] data-[direction=vertical]:after:h-3" />
      <Resizable.Pane defaultSize={(1 - node.ratio) * 100} minSize={8} class="flex min-h-0 min-w-0">
        <Tile node={node.b} {leafContent} {drop} {maximized} {lifted} {landed} />
      </Resizable.Pane>
    </Resizable.PaneGroup>
  {/key}
{/if}
