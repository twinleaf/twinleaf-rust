<script>
  // The gap between the two sides of a split, which a drag moves. While it is
  // dragged, `onMove(ratio)` gives the ratio under the pointer, for the tiles
  // to show; letting go gives it to `onSet(ratio)`, once. Escape, a
  // cancelled pointer, or a change to the layout it was dragged in, as an
  // undo, gives `onMove(null)` and sets nothing.
  import { onDestroy, untrack } from 'svelte';
  import { claim } from './keyrouter.js';

  /** The smallest share either side keeps. */
  const MIN_SHARE = 0.08;

  /**
   * gap: the split's gap as layout.js `gutters` gives it; measure(): the
   * same in px of the page, as the tiles are now. layout: the tree it is a
   * gap of. style: where it is drawn.
   */
  let { gap, layout, measure, style, onMove, onSet } = $props();

  let dragging = $state(false);
  let end = null;

  function down(e) {
    if (e.button !== 0) return;
    e.preventDefault();
    e.stopPropagation();
    const el = e.currentTarget;
    // A press made by script has no pointer to hold.
    if (e.isTrusted) el.setPointerCapture(e.pointerId);
    const g = measure(), row = g.dir === 'row';
    // The pointer keeps its place on the gap as the gap moves.
    const at = (ev) => (row ? ev.clientX : ev.clientY);
    const start = at(e), from = g.ratio;
    const room = row ? g.split.w - g.w : g.split.h - g.h;
    let ratio = from;
    dragging = true;
    const move = (ev) => {
      ratio = Math.min(1 - MIN_SHARE, Math.max(MIN_SHARE, from + (at(ev) - start) / room));
      onMove(ratio);
    };
    const unclaim = claim('drag', { on: { cancel: () => stop(false) } });
    const up = () => stop(true);
    const cancel = () => stop(false);
    const stop = (set) => {
      el.removeEventListener('pointermove', move);
      el.removeEventListener('pointerup', up);
      el.removeEventListener('pointercancel', cancel);
      unclaim();
      if (el.hasPointerCapture(e.pointerId)) el.releasePointerCapture(e.pointerId);
      dragging = false;
      end = null;
      onMove(null);
      if (set && ratio !== from) onSet(ratio);
    };
    el.addEventListener('pointermove', move);
    el.addEventListener('pointerup', up);
    el.addEventListener('pointercancel', cancel);
    end = () => stop(false);
  }

  $effect(() => {
    layout;
    untrack(() => end?.());
  });
  onDestroy(() => end?.());
</script>

<div
  class="separator"
  class:active={dragging}
  data-dir={gap.dir}
  role="separator"
  aria-orientation={gap.dir === 'row' ? 'vertical' : 'horizontal'}
  aria-valuenow={Math.round(gap.ratio * 100)}
  aria-valuemin={MIN_SHARE * 100}
  aria-valuemax={100 - MIN_SHARE * 100}
  {style}
  onpointerdown={down}
></div>
