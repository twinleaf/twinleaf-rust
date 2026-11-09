<script>
  // A plot's kind and layout: two independent choices, so two small
  // dropdowns whose triggers show the chosen one's icon, named in their
  // tooltips. Each item pairs its icon with its name, which teaches the icons. Layout applies to
  // spectra too: one shared axis, one axis with a y range per signal, or a
  // lane per signal.
  //
  // As in a menu bar, ← → move from one open dropdown to the other.
  import { tick } from 'svelte';
  import * as Select from '$lib/components/ui/select/index.js';
  import RiTime from '@lucide/svelte/icons/chart-line';
  import RiSpectrum from '@lucide/svelte/icons/audio-lines';
  import RiOverlay from '@lucide/svelte/icons/layers';
  import RiIndependent from '@lucide/svelte/icons/unfold-vertical';
  import RiStacked from '@lucide/svelte/icons/rows-3';
  import Tip from './Tip.svelte';
  import { KINDS, MODES } from './constants.js';

  /**
   * `plot` is the plot whose `kind` and `mode` these set. `open` is the kind
   * dropdown's, which the parent's K key opens, and `onOpenChange` hears
   * both dropdowns, so the parent can take focus back once neither is open.
   * `onKind(kind)` and `onMode(mode)` set them, by default on `plot` alone.
   */
  let { plot, open = $bindable(false), onOpenChange, onKind = (v) => (plot.kind = v), onMode = (v) => (plot.mode = v) } = $props();

  const ICONS = { time: RiTime, spectrum: RiSpectrum, overlay: RiOverlay, independent: RiIndependent, stacked: RiStacked };
  const nameOf = (list, value) => list.find(([v]) => v === value)?.[1] ?? value;
  let layoutOpen = $state(false);
  let kindTrigger = $state(null);
  let layoutTrigger = $state(null);
  // An open dropdown takes the keys on its trigger, however it opened: by
  // click, by K, or from the other one.
  $effect(() => {
    if (open) kindTrigger?.focus();
  });
  $effect(() => {
    if (layoutOpen) layoutTrigger?.focus();
  });

  /** Moves from one open dropdown to the other, as ← → do in a menu bar. */
  function across(e) {
    if (e.key !== 'ArrowLeft' && e.key !== 'ArrowRight') return;
    e.preventDefault();
    const toLayout = open;
    open = false;
    layoutOpen = false;
    tick().then(() => (toLayout ? (layoutOpen = true) : (open = true)));
  }
  // Heard once the move between dropdowns is done, so a move is not a close.
  const changed = () => setTimeout(() => onOpenChange?.(open || layoutOpen));
</script>

{#snippet items(list)}
  {#each list as [value, name]}
    {@const Icon = ICONS[value]}
    <Select.Item {value} label={name}><Icon class="text-muted-foreground" />{name}</Select.Item>
  {/each}
{/snippet}

<Select.Root type="single" value={plot.kind} onValueChange={onKind} bind:open onOpenChange={changed}>
  <Tip text="Kind: {nameOf(KINDS, plot.kind)}" keys={['K']}>
    {#snippet children(tip)}
      {@const Icon = ICONS[plot.kind]}
      <Select.Trigger {...tip} bind:ref={kindTrigger} size="sm" class="kind-select" tabindex={-1} aria-label="Plot kind: {nameOf(KINDS, plot.kind)}" onkeydown={(e) => open && across(e)}>
        <Icon />
      </Select.Trigger>
    {/snippet}
  </Tip>
  <Select.Content>{@render items(KINDS)}</Select.Content>
</Select.Root>
<Select.Root type="single" value={plot.mode} onValueChange={onMode} bind:open={layoutOpen} onOpenChange={changed}>
  <Tip text="Layout: {nameOf(MODES, plot.mode)}">
    {#snippet children(tip)}
      {@const Icon = ICONS[plot.mode]}
      <Select.Trigger {...tip} bind:ref={layoutTrigger} size="sm" class="kind-select" tabindex={-1} aria-label="Plot layout: {nameOf(MODES, plot.mode)}" onkeydown={(e) => layoutOpen && across(e)}>
        <Icon />
      </Select.Trigger>
    {/snippet}
  </Tip>
  <Select.Content>{@render items(MODES)}</Select.Content>
</Select.Root>
