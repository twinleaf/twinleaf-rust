<script>
  // The plots of a layout tree, each in a tile at the box layout.js `rects`
  // gives it, and the gap of every split, which a drag moves. The tiles are
  // one flat list keyed by plot, so a plot keeps its element, and all it
  // holds, however the tree around it changes. A tile's box is a fraction of
  // this box plus a count of gaps, so it follows the box as CSS lays it out.
  //
  // While a gap is dragged the tiles show the layout with that split at the
  // pointer's ratio; letting go gives the ratio to `onResize(path, ratio)`.
  import { GUTTER, gutters, plotIds, rects, setRatio } from './layout.js';
  import Separator from './Separator.svelte';

  /**
   * layout: the tree. leafContent: a snippet that renders one plot by id.
   * drop: where a dragged signal would go, {plotId, edge}. maximized: the
   * plot that fills the box. lifted, landed: the plots being moved, and
   * those that just landed, as Sets.
   */
  let { layout, leafContent, drop, maximized, lifted, landed, onResize } = $props();

  let box;
  /** The split being dragged and the ratio under the pointer, {path, ratio}, or null. */
  let live = $state(null);
  const shown = $derived(live ? setRatio(layout, live.path, live.ratio) : layout);

  const UNIT = { x: 0, y: 0, w: 1, h: 1 }, NONE = { x: 0, y: 0, w: 0, h: 0 };
  const num = (v) => +v.toFixed(6);
  /** CSS placing box `u` of the unit box plus `g` gaps. */
  const place = (u, g) =>
    `left: calc(${num(u.x * 100)}% + ${num(g.x)} * var(--gutter)); top: calc(${num(u.y * 100)}% + ${num(g.y)} * var(--gutter)); ` +
    `width: calc(${num(u.w * 100)}% + ${num(g.w)} * var(--gutter)); height: calc(${num(u.h * 100)}% + ${num(g.h)} * var(--gutter));`;
  const tiles = $derived.by(() => {
    const u = rects(shown, UNIT, 0), g = rects(shown, NONE, 1);
    return new Map([...u].map(([id, r]) => [id, place(r, g.get(id))]));
  });
  const gaps = $derived.by(() => {
    const u = gutters(shown, UNIT, 0), g = gutters(shown, NONE, 1);
    return u.map((gap, i) => ({ gap, style: place(gap, g[i]) }));
  });

  /** The gap at `path` in px of the page, the tiles as they are now. */
  function measure(path) {
    const r = box.getBoundingClientRect();
    return gutters(layout, { x: r.left, y: r.top, w: r.width, h: r.height }, GUTTER).find((g) => g.path === path);
  }
</script>

<div class="tile-box" bind:this={box} style="--gutter: {GUTTER}px">
  {#each plotIds(layout) as id (id)}
    <div class="leaf" class:max={maximized === id} class:lifted={lifted?.has(id)} class:landed={landed?.has(id)} data-plot={id} style={maximized === id ? '' : tiles.get(id)}>
      {@render leafContent(id)}
      {#if drop?.plotId === id}
        <div class="drop-hint {drop.edge}"></div>
      {/if}
    </div>
  {/each}
  {#each gaps as { gap, style } (gap.path)}
    <Separator
      {gap}
      {layout}
      {style}
      measure={() => measure(gap.path)}
      onMove={(ratio) => (live = ratio === null ? null : { path: gap.path, ratio })}
      onSet={(ratio) => onResize(gap.path, ratio)}
    />
  {/each}
</div>
