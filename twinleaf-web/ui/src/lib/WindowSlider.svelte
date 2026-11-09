<script>
  // The time window's control, in the top bar's popover and a time axis's: a
  // slider along a log scale that settles into a preset's detent near it, and
  // the exact window, to read or type, at its end (window.js).
  import { SPANS } from './constants.js';
  import { nextPreset, positionOf, windowFrom, windowText, windowTyped } from './window.js';

  /** `value` is the window shown, in seconds, `onchange` sets one, and a typed one goes up to `widest`. */
  let { value, onchange, widest } = $props();

  let track = $state();
  // What the field holds while it is typed in, or null.
  let typed = $state(null);
  let dragging = false;

  const at = (e) => {
    const r = track.getBoundingClientRect();
    return windowFrom((e.clientX - r.left) / r.width, r.width);
  };
  function down(e) {
    if (e.button !== 0) return;
    dragging = true;
    track.setPointerCapture(e.pointerId);
    track.focus();
    onchange(at(e));
  }
  /** The arrows step from preset to preset, with Shift by a twentieth. */
  function key(e) {
    const dir = { ArrowRight: 1, ArrowUp: 1, ArrowLeft: -1, ArrowDown: -1 }[e.key];
    if (e.key === 'Home') onchange(SPANS[0]);
    else if (e.key === 'End') onchange(SPANS.at(-1));
    else if (dir) onchange(e.shiftKey ? +(value * 1.05 ** dir).toPrecision(2) : nextPreset(value, dir));
    else return;
    e.preventDefault();
  }
  function commit() {
    const w = typed === null ? null : windowTyped(typed, widest);
    if (w !== null) onchange(w);
    typed = null;
  }
</script>

<div class="window-slider">
  <div
    bind:this={track}
    class="window-track"
    role="slider"
    tabindex="0"
    aria-label="Time window"
    aria-valuemin={SPANS[0]}
    aria-valuemax={SPANS.at(-1)}
    aria-valuenow={value}
    aria-valuetext="{windowText(value)} seconds"
    style="--at: {positionOf(value) * 100}%"
    onpointerdown={down}
    onpointermove={(e) => dragging && onchange(at(e))}
    onpointerup={() => (dragging = false)}
    onpointercancel={() => (dragging = false)}
    onkeydown={key}
  >
    <span class="rail"><span class="fill"></span></span>
    {#each SPANS as s}<span class="detent" class:on={s === value} style="--p: {positionOf(s) * 100}%"><i></i>{s}</span>{/each}
    <span class="thumb"></span>
  </div>
  <label class="window-field">
    <input
      type="text"
      inputmode="decimal"
      aria-label="Time window, in seconds"
      value={typed ?? windowText(value)}
      oninput={(e) => (typed = e.currentTarget.value)}
      onkeydown={(e) => e.key === 'Enter' && (commit(), e.currentTarget.select())}
      onblur={commit}
    />s
  </label>
</div>
