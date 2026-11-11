<script>
  // The notice of the last device write or Revert (writes.svelte.js): what
  // happened, a line for each thing it left out, and Revert while it is
  // offered. The pointer on it keeps it open, and a press on it keeps the
  // selection, so Revert leaves the rows picked. After a Ctrl+Z that took
  // nothing back, it says to use Revert, and Revert is marked a moment. It
  // is drawn as the page's
  // other notices are, at its level.
  import RiCheck from '@lucide/svelte/icons/circle-check';
  import RiWarn from '@lucide/svelte/icons/triangle-exclamation-point';
  import RiError from '@lucide/svelte/icons/octagon-x';
  import * as words from './words.js';

  /** `writes` is the page's DeviceWrites. */
  let { writes } = $props();

  const notice = $derived(writes.notice);
  const off = $derived(notice?.revert ? writes.revertOff : null);
  const Icon = $derived({ success: RiCheck, warning: RiWarn, error: RiError }[notice?.level] ?? RiCheck);
</script>

{#if notice}
  <div class="write-notice" data-level={notice.level} data-keeps-selection role="status" onpointerenter={() => writes.hover(true)} onpointerleave={() => writes.hover(false)}>
    <Icon class="write-notice-icon" aria-hidden="true" />
    <div class="write-notice-body">
      <div class="write-notice-text">{notice.text}</div>
      {#each notice.detail as line, i (i)}<div class="write-notice-detail">{line}</div>{/each}
      {#if off}<div class="write-notice-detail">{words.revertOff(off.label, off.why)}</div>{/if}
      {#if writes.useRevert}<div class="write-notice-detail write-notice-undo">{writes.useRevert}</div>{/if}
    </div>
    {#if writes.canRevert}<button class="write-notice-revert" class:pointed={writes.pointed} onclick={() => writes.revert()}>{words.REVERT}</button>{/if}
  </div>
{/if}
