<script>
  // A plot shown away from the page's tiles, bare: its body alone, with no
  // header and no border, on a canvas of its own, laid out `width` by
  // `height` CSS px: the size of its picture (PlotHost `picture`), which is
  // `pixelRatio` device pixels a CSS px, its windows of the hub's history
  // read as finely. With a `figure` (figure.js), the stage is that figure:
  // the plot takes the room its bands leave.
  //
  // With no `scale` the stage is on the page at its size, drawn at
  // `pixelRatio`, and takes the pointer as a plot does. With a `scale`, it
  // is shown `scale` times its size, as a preview of its picture: the plot
  // is laid out apart, out of sight and of the pointer's way, at its own
  // size, and each frame its picture, at `scale` times `screenRatio`, the
  // screen's device pixels a CSS px, is drawn on the canvas shown. So the
  // preview has the picture's layout, ticks and text, however small it is.
  import { onMount, untrack } from 'svelte';
  import PlotCard from './PlotCard.svelte';
  import { boxFrame } from './gl.js';
  import { bandsOf } from './figure.js';
  import * as clock from './clock.js';

  /** `host` is the PlotHost of plot `id`; `width`, `height`, `pixelRatio`, `figure`, `scale` and `screenRatio` as above. */
  let { host, id, width, height, pixelRatio = 1, figure = null, scale = null, screenRatio = 1 } = $props();
  const plot = $derived(host.plot(id));
  const bands = $derived(figure ? bandsOf(figure, width) : { top: 0, bottom: 0 });
  const apart = $derived(scale !== null);
  /** The device pixels a CSS px the plot is drawn at each frame: its picture's on the page, the preview's apart. */
  const drawRatio = () => (scale === null ? pixelRatio : scale * screenRatio);
  /** The look the host's colours are drawn in, for what the page lays over the plot. */
  const look = $derived(host.colors.dark === null ? '' : host.colors.dark ? 'dark' : 'light');
  let box = $state(), canvas = $state(), shown = $state();
  /** Whether the host has its surface, which the plot mounts once it has. */
  let surfaced = $state(false);

  onMount(() => {
    const release = host.surface(canvas, boxFrame(box, drawRatio), () => pixelRatio);
    surfaced = true;
    let frame;
    const loop = (perf) => {
      if (scale === null) host.draw(perf);
      else if (host.pv(id) && shown) host.picture(id, { ratio: drawRatio(), into: shown, figure });
      frame = clock.frame(loop);
    };
    frame = clock.frame(loop);
    return () => {
      clock.cancel(frame);
      release();
    };
  });
  // The plot reads its windows of the hub's history as finely as its
  // picture shows them: as it mounts, at each new width or pixel ratio, and
  // again when a new session, or a new decimation, drops every window.
  $effect(() => {
    void [width, pixelRatio, host.hub.session.detailsDropped];
    untrack(() => host.rereadWindows());
  });
</script>

<div class="plot-stage" class:apart style="width: {width * (scale ?? 1)}px; height: {height * (scale ?? 1)}px">
  <div class="stage-room">
    <div class="stage-box {look}" bind:this={box} style="width: {width}px; height: {height - bands.top - bands.bottom}px; margin-top: {apart ? 0 : bands.top}px">
      <canvas class="stage-gl" bind:this={canvas}></canvas>
      {#if surfaced && plot}<PlotCard {plot} {host} />{/if}
    </div>
  </div>
  {#if apart}<canvas class="stage-shown" bind:this={shown} style="width: {width * scale}px; height: {height * scale}px"></canvas>{/if}
</div>
