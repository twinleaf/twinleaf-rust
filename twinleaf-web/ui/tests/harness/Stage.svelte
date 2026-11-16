<script>
  // A plot on a stage of its own (PlotStage.svelte), fixed at `left` and
  // `top` over whatever page is mounted, in a dialog, which keeps its keys
  // from the page, inside what a page gives its components: the tooltips'
  // provider. `resize` gives the stage another size, pixel ratio, scale or
  // figure.
  import * as Tooltip from '$lib/components/ui/tooltip/index.js';
  import PlotStage from '../../src/lib/PlotStage.svelte';

  let { host, id, width, height, pixelRatio = 1, scale = null, screenRatio = 1, figure = null, left = 0, top = 0 } = $props();
  /** What `resize` gave in place of the props. */
  let given = $state({});
  const size = $derived({ width, height, pixelRatio, scale, figure, ...given });
  /** Gives the stage the fields of `to`, of {width, height, pixelRatio, scale, figure}. */
  export const resize = (to) => void (given = { ...given, ...to });
</script>

<Tooltip.Provider delayDuration={300} ignoreNonKeyboardFocus>
  <div class="test-stage" role="dialog" aria-label="A plot on a stage" style="position: fixed; left: {left}px; top: {top}px; z-index: 40">
    <PlotStage {host} {id} width={size.width} height={size.height} pixelRatio={size.pixelRatio} scale={size.scale} figure={size.figure} {screenRatio} />
  </div>
</Tooltip.Provider>
