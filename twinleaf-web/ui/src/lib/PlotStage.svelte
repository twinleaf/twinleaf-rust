<script>
  // A plot shown away from the page's tiles, bare: its body alone, with no
  // header and no border, on a canvas of its own the size of the stage, at
  // the pixel ratio asked for. It is the surface of the plot's host
  // (plothost.js), which it draws each frame, so `host.picture(id)` is the
  // stage's picture: `width` by `height` CSS px, `pixelRatio` device pixels
  // each.
  import { onMount, untrack } from 'svelte';
  import PlotCard from './PlotCard.svelte';
  import { boxFrame } from './gl.js';
  import * as clock from './clock.js';

  /** `host` is the PlotHost of plot `id`, which the stage shows; `width` and `height` are the stage's size in CSS px, and `pixelRatio` the device pixels its canvases have for each. */
  let { host, id, width, height, pixelRatio = 1 } = $props();
  const plot = $derived(host.plot(id));
  let box = $state(), canvas = $state();
  /** Whether the host has its surface, which the plot mounts once it has. */
  let surfaced = $state(false);

  onMount(() => {
    const release = host.surface(canvas, boxFrame(box, () => pixelRatio));
    surfaced = true;
    let frame;
    const loop = (perf) => {
      host.draw(perf);
      frame = clock.frame(loop);
    };
    frame = clock.frame(loop);
    return () => {
      clock.cancel(frame);
      release();
    };
  });
  // The plot reads its windows of the hub's history as finely as the stage
  // shows them: as it mounts, at each new width or pixel ratio, and again
  // when a new session, or a new decimation, drops every window.
  $effect(() => {
    void [width, pixelRatio, host.hub.session.detailsDropped];
    untrack(() => host.rereadWindows());
  });
</script>

<div class="plot-stage" bind:this={box} style="width: {width}px; height: {height}px">
  <canvas class="stage-gl" bind:this={canvas}></canvas>
  {#if surfaced && plot}<PlotCard {plot} {host} />{/if}
</div>
