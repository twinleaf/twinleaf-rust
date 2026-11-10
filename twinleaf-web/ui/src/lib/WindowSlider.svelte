<script>
  // The time window's control, in the top bar's popover and a time axis's: a
  // slider along a log scale that settles into a preset's detent near it, and
  // the exact window, to read or type, at its end (window.js).
  import { onDestroy } from 'svelte';
  import { SPANS } from './constants.js';
  import { claim } from './keyrouter.js';
  import { nextPreset, positionOf, windowFrom, windowText, windowTyped } from './window.js';

  /**
   * `value` is the window shown, in seconds, `onchange` sets one, and a
   * typed one goes up to `widest`. `onstart` and `onend` run as a drag
   * of the slider starts and ends, so what it sets on the way is one change.
   */
  let { value, onchange, widest, onstart = () => {}, onend = () => {} } = $props();

  let track = $state();
  /** The rail's span inside the track, which positions are fractions of. */
  let scale = $state();
  // What the field holds while it is typed in, or null.
  let typed = $state(null);
  let dragging = false;

  const at = (e) => {
    const r = scale.getBoundingClientRect();
    return windowFrom((e.clientX - r.left) / r.width, r.width);
  };
  function down(e) {
    if (e.button !== 0) return;
    // The slider owns the press: it neither starts nor extends a text selection, and it takes focus itself.
    e.preventDefault();
    dragging = true;
    onstart();
    track.setPointerCapture(e.pointerId);
    track.focus();
    onchange(at(e));
  }
  /** Ends a drag: its release, or the slider going, as its popover closes under it. */
  function release() {
    if (!dragging) return;
    dragging = false;
    onend();
  }
  onDestroy(release);
  // The slider's keys (keymap.js, place 'slider'): the arrows step from
  // preset to preset, with Shift by a twentieth, and Home and End go to the
  // narrowest and the widest.
  onDestroy(
    claim('slider', {
      when: (e, at) => !!track?.contains(at),
      on: {
        step(e) {
          const dir = { ArrowRight: 1, ArrowUp: 1, ArrowLeft: -1, ArrowDown: -1 }[e.key];
          onchange(e.shiftKey ? +(value * 1.05 ** dir).toPrecision(2) : nextPreset(value, dir));
        },
        narrowest: () => void onchange(SPANS[0]),
        widest: () => void onchange(SPANS.at(-1)),
      },
    }),
  );
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
    onpointerup={release}
    onpointercancel={release}
  >
    <span class="scale" bind:this={scale}>
      <span class="rail"><span class="fill"></span></span>
      {#each SPANS as s}<span class="detent" class:on={s === value} style="--p: {positionOf(s) * 100}%"><i></i>{s}</span>{/each}
      <span class="thumb"></span>
    </span>
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
