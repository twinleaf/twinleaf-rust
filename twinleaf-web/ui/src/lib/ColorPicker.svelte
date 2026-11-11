<script>
  // A signal's colour, or several signals', as Office's colour menu lays it
  // out: Automatic first, a checkbox, ticked while the signal keeps the
  // colour it was given; the theme's eight colours in an even grid; then Custom…, which swaps the
  // grid for a picker of any colour (saturation and value, hue, hex, and the
  // screen's eyedropper where the browser has one) with Back to return. A
  // colour from the grid is a choice made, so it calls `ondone`; Automatic
  // and the custom picker apply as they change and leave it open.
  import { Checkbox } from '$lib/components/ui/checkbox/index.js';
  import RiPipette from '@lucide/svelte/icons/pipette';
  import RiBack from '@lucide/svelte/icons/chevron-left';
  import { colors, NAMES, hexToHsv, hsvToHex } from './colors.svelte.js';
  import { colorOf } from './plot.js';

  /** `key` is the signal whose colour this shows, and `keys` those whose colour it picks, `key` alone unless given; `ondone` hears a colour chosen from the grid. */
  let { key, keys = null, ondone } = $props();
  /** Picks colour `color` for each of `keys`, or with a function, the colour it gives for each. */
  const pickAll = (color) => (keys ?? [key]).forEach((k) => colors.pick(k, typeof color === 'function' ? color(k) : color));
  let editing = $state(false);
  let hsv = $state({ h: 0, s: 0, v: 0 });
  let hexText = $state('');
  const current = $derived(colors.of(key));
  const custom = $derived(typeof current === 'string');
  const automatic = $derived(colors.picked[key] === undefined);
  const canSample = typeof window !== 'undefined' && 'EyeDropper' in window;

  /** Ticked, each signal takes its given colour again; unticked, the colour last picked for it, or the one it shows. */
  function setAutomatic(on) {
    pickAll((k) => (on ? null : (colors.last[k] ?? colors.of(k))));
  }
  function choose(index) {
    pickAll(index);
    ondone?.();
  }
  /** Opens the custom picker on the signal's colour now. */
  function edit() {
    const hex = colorOf({ key });
    hsv = hexToHsv(hex);
    hexText = hex;
    editing = true;
  }
  function apply() {
    const hex = hsvToHex(hsv);
    hexText = hex;
    pickAll(hex);
  }
  /** Follows a drag on `e.currentTarget`, calling `at(x, y)` with the pointer's place across it, 0 to 1 each. */
  function drag(e, at) {
    const el = e.currentTarget, r = el.getBoundingClientRect();
    const move = (ev) => {
      at(Math.min(1, Math.max(0, (ev.clientX - r.left) / r.width)), Math.min(1, Math.max(0, (ev.clientY - r.top) / r.height)));
      apply();
    };
    el.setPointerCapture(e.pointerId);
    move(e);
    el.onpointermove = move;
    el.onpointerup = () => (el.onpointermove = el.onpointerup = null);
  }
  function typed() {
    const hex = hexText.trim().replace(/^#?/, '#').toLowerCase();
    if (!/^#[0-9a-f]{6}$/.test(hex)) return;
    hsv = hexToHsv(hex);
    pickAll(hex);
  }
  async function sample() {
    try {
      const { sRGBHex } = await new window.EyeDropper().open();
      hsv = hexToHsv(sRGBHex);
      apply();
    } catch {
      // Cancelled.
    }
  }
</script>

<div class="color-picker">
  {#if editing}
    <button class="picker-back" onclick={() => (editing = false)}><RiBack />Custom colour</button>
    <!-- svelte-ignore a11y_no_static_element_interactions -->
    <div class="picker-sv" style="--hue: {hsv.h}" onpointerdown={(e) => drag(e, (x, y) => (hsv = { ...hsv, s: x, v: 1 - y }))}>
      <span class="picker-thumb" style="left: {hsv.s * 100}%; top: {(1 - hsv.v) * 100}%; background: {hsvToHex(hsv)}"></span>
    </div>
    <!-- svelte-ignore a11y_no_static_element_interactions -->
    <div class="picker-hue" onpointerdown={(e) => drag(e, (x) => (hsv = { ...hsv, h: x * 360 }))}>
      <span class="picker-thumb" style="left: {(hsv.h / 360) * 100}%; top: 50%; background: hsl({hsv.h} 100% 50%)"></span>
    </div>
    <div class="picker-row">
      <input class="picker-hex" aria-label="Hex colour" spellcheck="false" bind:value={hexText} oninput={typed} />
      {#if canSample}
        <button class="picker-sample" aria-label="Pick a colour from the screen" title="Pick a colour from the screen" onclick={sample}><RiPipette /></button>
      {/if}
    </div>
  {:else}
    <label class="picker-auto"><Checkbox checked={automatic} onCheckedChange={setAutomatic} />Automatic</label>
    <div class="swatch-grid" role="radiogroup" aria-label="Colour of {key}">
      {#each NAMES as name, i}
        <button class="swatch-choice" role="radio" aria-checked={current === i} aria-label={name} title={name} style="--c: var(--trace-{i + 1})" onclick={() => choose(i)}></button>
      {/each}
    </div>
    <button class="picker-custom" aria-checked={custom} role="menuitemradio" onclick={edit}>
      <span class="picker-custom-chip" style={custom ? `--c: ${current}` : undefined}>{#if !custom}<RiPipette />{/if}</span>Custom…
    </button>
  {/if}
</div>
