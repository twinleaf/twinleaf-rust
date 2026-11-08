<script>
  // The raw history the hub keeps: a span of each stream's own time, or so
  // much memory across every stream, whichever comes first, as two rows of
  // a label and its control (plot.css, .set-grid). The caption names the
  // limit that comes first at the streams' current rate.
  //
  // The waterline is the memory held of the budget, and its tick is where
  // the span of time would fill it. A tick inside the bar means time comes
  // first, and the water stops there; none means memory does.
  import * as Select from '$lib/components/ui/select/index.js';
  import RiInfo from '@lucide/svelte/icons/info';
  import RiMinus from '@lucide/svelte/icons/minus';
  import RiPlus from '@lucide/svelte/icons/plus';
  import Tip from './Tip.svelte';
  import { api } from './protocol.js';
  import { HISTORY_BUDGETS, HISTORY_SPANS } from './constants.js';
  import { bytesEstimate, bytesText, spanText } from './units.js';

  /** `memory` is the session's: {bytes, budget, seconds, maxAge, rate}. */
  let { memory } = $props();

  const step = $derived(
    HISTORY_BUDGETS.reduce((best, b, i) => (Math.abs(b - memory.budget) < Math.abs(HISTORY_BUDGETS[best] - memory.budget) ? i : best), 0),
  );
  const lasts = $derived(memory.rate > 0 ? memory.budget / memory.rate : Infinity);
  const held = $derived(memory.budget ? Math.min(1, memory.bytes / memory.budget) : 0);
  /** Where the span of time fills the budget, as a share of it, past 1 when memory runs out first. */
  const tick = $derived(memory.budget && memory.rate > 0 ? (memory.rate * memory.maxAge) / memory.budget : 0);
  const info = $derived(
    memory.rate > 0
      ? `${bytesText(memory.rate)}/s coming in. Holding ${bytesText(memory.bytes)}, ${spanText(memory.seconds)} of each stream.`
      : `No stream is adding data. Holding ${bytesText(memory.bytes)}.`,
  );

  /** The limit that comes first at the current rate, whose value shows bright: `time`, `memory` or none. */
  const binding = $derived(tick >= 1 ? 'memory' : tick > 0 ? 'time' : null);

  const setBudget = (i) => api.setHistory(memory.maxAge, HISTORY_BUDGETS[i]);
</script>

<div class="history-set">
  <div class="set-grid">
    <span>Time limit</span>
    <Select.Root type="single" value={String(memory.maxAge)} onValueChange={(v) => api.setHistory(+v, memory.budget)}>
      <Select.Trigger size="sm" class="w-full text-xs {binding === 'time' ? 'text-foreground' : 'text-muted-foreground'}" aria-label="Time limit">{spanText(memory.maxAge)}</Select.Trigger>
      <Select.Content>
        {#each HISTORY_SPANS as s}<Select.Item value={String(s)} label={spanText(s)} />{/each}
      </Select.Content>
    </Select.Root>
    <span>Memory limit</span>
    <div class="stepper" role="group" aria-label="Memory limit">
      <button aria-label="Less memory" disabled={step === 0} onclick={() => setBudget(step - 1)}><RiMinus /></button>
      <span class:muted={binding !== 'memory'}>{bytesText(memory.budget)}</span>
      <button aria-label="More memory" disabled={step === HISTORY_BUDGETS.length - 1} onclick={() => setBudget(step + 1)}><RiPlus /></button>
    </div>
  </div>
  <div class="water">
    <div class="waterline" role="meter" aria-label="Memory held" aria-valuenow={memory.bytes} aria-valuemin={0} aria-valuemax={memory.budget}>
      <div class="fill" style="width: {held * 100}%"></div>
      {#if tick > 0 && tick < 1}<div class="tick" style="left: {tick * 100}%"></div>{/if}
    </div>
    <div class="water-notes">
      <div class="water-caption">
        {#if tick >= 1}
          <span>Whichever first: <b class="short">memory</b>, at {spanText(lasts)}</span>
        {:else if tick > 0}
          <span>Whichever first: <b>time</b>, at {bytesEstimate(memory.rate * memory.maxAge)}</span>
        {:else}
          <span>Whichever comes first</span>
        {/if}
        <Tip text={info}>
          {#snippet children(tip)}<button {...tip} class="info" aria-label={info}><RiInfo /></button>{/snippet}
        </Tip>
      </div>
      {#if memory.held}
        <!-- What holding the paused plots' capture costs: what the history would have dropped since. -->
        <div>Paused plots hold {bytesEstimate(memory.held.bytes)} more</div>
      {/if}
    </div>
  </div>
</div>
