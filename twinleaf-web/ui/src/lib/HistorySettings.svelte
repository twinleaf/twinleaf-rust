<script>
  // The raw history the hub keeps: a span of each stream's own time, or so
  // much memory across every stream, whichever comes first, as two rows of
  // a label and its control (plot.css, .set-grid). The caption names the
  // limit that comes first at the streams' current rate.
  //
  // The waterline is the memory held of the budget, and its tick is where
  // the span of time would fill it. A tick inside the bar means time comes
  // first, and the water stops there; none means memory does. While time
  // comes first, a hatched band runs on from the tick to where the live
  // history and a full paused one beside it would fill it, twice as far
  // (session.svelte.js `pausing`); past the bar's end it runs to the end,
  // and an arrow after the bar says it goes on. A line under the caption
  // says, in time, what a paused plot keeps.
  //
  // A lower limit that drops history the hub holds asks first, in place of
  // the waterline: what it drops of the live history and of what paused
  // plots hold (session.svelte.js `dropping`), with a button that drops it
  // and one that keeps the limit set, which has focus as it asks. Escape
  // while it asks keeps the limit and leaves the popover open; closing the
  // popover keeps it too, as the question goes with the component. Kept,
  // focus goes back to the control that asked. A higher limit, or a lower
  // one that drops nothing, is set at once.
  import * as Select from '$lib/components/ui/select/index.js';
  import { Button } from '$lib/components/ui/button/index.js';
  import RiArrow from '@lucide/svelte/icons/chevron-right';
  import RiInfo from '@lucide/svelte/icons/info';
  import RiMinus from '@lucide/svelte/icons/minus';
  import RiPlus from '@lucide/svelte/icons/plus';
  import Tip from './Tip.svelte';
  import { api } from './protocol.js';
  import { dropping, pausing } from './session.svelte.js';
  import { escapeFirst } from './overlay.svelte.js';
  import { HISTORY_BUDGETS, HISTORY_SPANS } from './constants.js';
  import { bytesEstimate, bytesText, spanText } from './units.js';
  import * as words from './words.js';

  /** `memory` is the session's: {bytes, budget, seconds, maxAge, rate, held}. */
  let { memory } = $props();

  /**
   * The least history, in seconds, whose drop a lower limit asks about:
   * less is within what the hub's last `memory` message is behind and what
   * its chunks round to.
   */
  const ASK_FROM = 1;

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
  /** What a paused plot keeps at the current rate and limits, or null while nothing streams. */
  const pause = $derived(pausing(memory));
  /** Where the paused band ends, as a share of the budget, past 1 when the budget holds less; 0 while memory comes first. */
  const band = $derived(binding === 'time' && pause ? pause.need / memory.budget : 0);

  /** The lower setting the question asks about, {maxAge, budget, limit}, `limit` the one lowered, `time` or `memory`; null while none is asked. */
  let asking = $state(null);
  /** What the question says: of what `asking` drops by the hub's last `memory` message, and the limit set now. */
  const ask = $derived(
    asking && words.lowerAsk(dropping(memory, asking.maxAge, asking.budget), asking.limit === 'time' ? spanText(memory.maxAge) : bytesText(memory.budget)),
  );

  /** Sets History to `maxAge` seconds within `budget` bytes, `limit` the one changed, or asks first when that drops history held. */
  function set(maxAge, budget, limit) {
    const raised = maxAge >= memory.maxAge && budget >= memory.budget;
    const drops = dropping(memory, maxAge, budget);
    if (!raised && (drops.live >= ASK_FROM || drops.paused >= ASK_FROM)) {
      asking = { maxAge, budget, limit };
      return;
    }
    asking = null;
    api.setHistory(maxAge, budget);
  }

  /** Sets the setting the question asks about. */
  function drop() {
    const { maxAge, budget } = asking;
    asking = null;
    api.setHistory(maxAge, budget);
  }

  // The controls that ask, and the question's Keep, which has focus while it asks.
  let timeTrigger = $state(null), lessButton = $state(null), keepButton = $state(null);
  $effect(() => keepButton?.focus());

  /** Keeps the limit set, and gives focus back to the control that asked. */
  function keep() {
    const from = asking.limit === 'time' ? timeTrigger : lessButton;
    asking = null;
    from?.focus();
  }
  // Escape while it asks keeps the limit, and leaves the popover open.
  escapeFirst(() => !!asking && (keep(), true));

  const setBudget = (i) => set(memory.maxAge, HISTORY_BUDGETS[i], 'memory');
</script>

<div class="history-set">
  <div class="set-grid">
    <span>Time limit</span>
    <!-- The list shows the limit set, not one a question keeps from being set. -->
    <Select.Root type="single" bind:value={() => String(memory.maxAge), (v) => set(+v, memory.budget, 'time')}>
      <Select.Trigger bind:ref={timeTrigger} size="sm" class="w-full text-xs {binding === 'time' ? 'text-foreground' : 'text-muted-foreground'}" aria-label="Time limit">{spanText(memory.maxAge)}</Select.Trigger>
      <Select.Content>
        {#each HISTORY_SPANS as s}<Select.Item value={String(s)} label={spanText(s)} />{/each}
      </Select.Content>
    </Select.Root>
    <span>Memory limit</span>
    <div class="stepper" role="group" aria-label="Memory limit">
      <button bind:this={lessButton} aria-label="Less memory" disabled={step === 0} onclick={() => setBudget(step - 1)}><RiMinus /></button>
      <span class:muted={binding !== 'memory'}>{bytesText(memory.budget)}</span>
      <button aria-label="More memory" disabled={step === HISTORY_BUDGETS.length - 1} onclick={() => setBudget(step + 1)}><RiPlus /></button>
    </div>
  </div>
  {#if ask}
    <div class="lower-ask" role="group" aria-label="Lower the limit">
      <div class="lower-lines" aria-live="polite">
        {#each ask.lines as line}<p>{line}</p>{/each}
      </div>
      <div class="lower-buttons">
        <Button bind:ref={keepButton} size="xs" variant="outline" onclick={keep}>{ask.keep}</Button>
        <Button size="xs" variant="destructive" onclick={drop}>{ask.drop}</Button>
      </div>
    </div>
  {:else}
    <div class="water">
      <div class="water-bar">
        <div
          class="waterline"
          role="meter"
          aria-label="Memory held"
          aria-valuenow={memory.bytes}
          aria-valuemin={0}
          aria-valuemax={memory.budget}
          aria-valuetext={words.waterText(memory.bytes, memory.budget, pause, binding === 'time')}
        >
          <div class="fill" style="width: {held * 100}%"></div>
          {#if band}<div class="paused" class:past={band > 1} style="left: {tick * 100}%; width: {(Math.min(1, band) - tick) * 100}%"></div>{/if}
          {#if tick > 0 && tick < 1}<div class="tick" style="left: {tick * 100}%"></div>{/if}
        </div>
        {#if band > 1}<RiArrow class="past-end" aria-hidden="true" />{/if}
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
        {#if pause && binding}<div class="paused-keeps">{words.pausedKeeps(pause, memory.maxAge)}</div>{/if}
        {#if memory.held}
          <!-- What holding the paused plots' capture costs: what the history would have dropped since. -->
          <div>Paused plots hold {bytesEstimate(memory.held.bytes)} more</div>
        {/if}
      </div>
    </div>
  {/if}
</div>
