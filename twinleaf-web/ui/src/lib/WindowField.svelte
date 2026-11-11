<script>
  // The time window's field, in the top bar and a time axis's popover: the
  // span shown, in the unit that reads shortest, the unit inside the field's
  // outline, typed as window.js reads it, a number alone in the unit shown.
  // Enter sets what is typed and a blur sets it when it is a window; what is
  // refused leaves the window as it was and marks the field until it is
  // corrected or Escape puts back what was there, focus kept; with nothing
  // typed, Escape is the page's, as in any field (keys.js `escapeTyped`),
  // and goes out a level. The arrows (key router, place 'window') and the
  // wheel step along the wheel zoom's clock ladder (wheelzoom.js).
  import { onDestroy } from 'svelte';
  import { claim } from './keyrouter.js';
  import { escapeTyped } from './keys.js';
  import { LADDERS, LINE_PX, stepped } from './wheelzoom.js';
  import { windowParts, windowText, windowTyped } from './window.js';

  /**
   * `value` is the window shown, in seconds, and `widest()` the widest the
   * views can show now. `onchange(s, typed)` sets a window: `typed` for one
   * typed, else a step of the arrows or the wheel. Any other attribute is
   * the field's own, as the command it runs (`data-command`).
   */
  let { value, onchange, widest, ...own } = $props();

  let box = $state();
  // What the field holds while it is typed in, or null; why what was set from it was refused, or null, and the widest then.
  let typed = $state(null);
  let refused = $state(null);
  let most = $state(0);
  // The wheel's travel toward the next step, in px.
  let travel = 0;
  /** How far the wheel travels for a step, in px. */
  const WHEEL_STEP = 50;

  const parts = $derived(windowParts(value));
  // A number typed alone is in the unit shown; one typed with its unit shows none beside it.
  const unit = $derived(typed !== null && /[a-z]/i.test(typed) ? '' : parts[1]);
  const hint = $derived(refused === 'long' ? `At most ${windowText(most)}` : refused ? 'Not a window: type 90, 5m, 1.5 min or 2h' : undefined);

  /** Sets what is typed, when it is a window; else marks the field. Whether it was set. */
  function commit() {
    if (typed === null) return true;
    most = widest();
    const w = windowTyped(typed, most, parts[1]);
    refused = w.refused ?? null;
    if (refused) return false;
    typed = null;
    onchange(w.s, true);
    return true;
  }
  /** Puts back what was there, focus kept. */
  function revert() {
    typed = null;
    refused = null;
  }
  /** Steps the window one span along the clock ladder, longer for `dir` 1, no wider than the views can show. */
  function step(dir) {
    typed = null;
    refused = null;
    const s = Math.min(widest(), stepped(value, dir, LADDERS.clock));
    if (s !== value) onchange(s, false);
  }
  // The arrows (keymap.js, place 'window'): up a span, down a span.
  onDestroy(
    claim('window', {
      when: (e, at) => !!box?.contains(at),
      on: { step: (e) => step(e.key === 'ArrowUp' ? 1 : -1) },
    }),
  );
  function wheel(e) {
    e.preventDefault();
    travel += e.deltaY * (e.deltaMode === 1 ? LINE_PX : e.deltaMode === 2 ? 400 : 1);
    if (Math.abs(travel) < WHEEL_STEP) return;
    step(Math.sign(travel));
    travel = 0;
  }
</script>

<label class="window-field" class:invalid={!!refused} bind:this={box} onwheel={wheel} data-keeps-selection {...own}>
  <input
    type="text"
    autocomplete="off"
    spellcheck="false"
    aria-label="Time window"
    aria-invalid={!!refused}
    title={hint}
    value={typed ?? parts[0]}
    onfocus={(e) => e.currentTarget.select()}
    oninput={(e) => {
      typed = e.currentTarget.value;
      if (refused) refused = windowTyped(typed, widest(), parts[1]).refused ?? null;
    }}
    onkeydown={(e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        if (commit()) e.currentTarget.select();
      } else escapeTyped(e, typed ?? parts[0], parts[0], revert);
    }}
    onblur={commit}
  /><span class="unit">{unit}</span>
</label>
