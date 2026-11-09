<script>
  // Exporting plots' signals to a file: every raw sample of them, the time
  // the plots show or the whole history, as CSV or HDF5. The hub writes the
  // file; the page only names it and saves it (exportfile.js).
  import * as Dialog from '$lib/components/ui/dialog/index.js';
  import { Button } from '$lib/components/ui/button/index.js';
  import { FORMATS, exportName, saveExport } from './exportfile.js';
  import { api } from './protocol.js';
  import { bytesText, spanText } from './units.js';
  import { toast } from './toast.svelte.js';

  /**
   * `what` is the export to offer, or null: {hub, traces, shown, whole},
   * `shown` the time [t0, t1] the plots show, null with none of time, and
   * `whole` the time the history may hold of the traces, from -Infinity
   * when the page has not heard where it begins.
   */
  let { what = $bindable(null) } = $props();

  let range = $state('shown');
  let format = $state('csv');
  const open = $derived(!!what);
  const chosen = $derived(what && (range === 'shown' && what.shown ? what.shown : what.whole));
  // What the file holds, as the hub counts it: {samples, seconds}, the
  // samples of every signal and the time from the oldest it holds; null
  // until the hub answers.
  let counted = $state(null);
  $effect(() => {
    counted = null;
    if (!chosen) return;
    const [t0, t1] = chosen, { hub, traces } = what;
    let current = true;
    Promise.all(traces.map((t) => api.rows({ hub, id: t.meta.id, t0, t1, limit: 0 }))).then(
      (replies) => {
        const oldest = Math.min(...replies.map((r) => r.oldest ?? Infinity));
        if (current) counted = { samples: replies.reduce((n, r) => n + r.count, 0), seconds: Math.max(0, t1 - Math.max(t0, oldest)) };
      },
      () => {},
    );
    return () => (current = false);
  });

  $effect(() => {
    if (what && !what.shown) range = 'whole';
  });

  /** Asks where to save, then saves; called by the click, as the save dialog needs. */
  function save() {
    const { hub, traces } = what;
    const [t0, t1] = chosen;
    const name = exportName(format);
    what = null;
    saveExport({ hub, ids: traces.map((t) => t.meta.id), t0, t1 }, format, name).then(
      (saved) => saved && toast.success(`Saved ${saved.name}${saved.bytes === null ? '' : `, ${bytesText(saved.bytes)}`}`),
      (error) => toast.error(`Could not export: ${error.message}`),
    );
  }
</script>

<Dialog.Root {open} onOpenChange={(o) => !o && (what = null)}>
  <Dialog.Content class="export-dialog sm:max-w-sm">
    <Dialog.Header>
      <Dialog.Title>Export data</Dialog.Title>
      <Dialog.Description>
        {#if what}{what.traces.length === 1 ? what.traces[0].key : `${what.traces.length} signals: ${what.traces.map((t) => t.key).join(', ')}`}{/if}
      </Dialog.Description>
    </Dialog.Header>
    <div class="axis-row" role="group" aria-label="Time to export">
      <span>Time</span>
      <div class="seg">
        <button class:on={range === 'shown'} aria-pressed={range === 'shown'} disabled={!what?.shown} onclick={() => (range = 'shown')}>Shown</button>
        <button class:on={range === 'whole'} aria-pressed={range === 'whole'} onclick={() => (range = 'whole')}>Whole history</button>
      </div>
    </div>
    <div class="axis-row" role="group" aria-label="File format">
      <span>Format</span>
      <div class="seg">
        {#each Object.entries(FORMATS) as [f, { description }]}
          <button class:on={format === f} aria-pressed={format === f} onclick={() => (format = f)}>{description.split(' ')[0]}</button>
        {/each}
      </div>
    </div>
    <p class="export-note">
      {#if !counted}Counting…{:else if counted.samples}{spanText(counted.seconds)}, {counted.samples.toLocaleString()} samples.{:else}No samples in this time.{/if}
      {format === 'csv' ? 'One time column, one column per signal.' : 'One table per stream, every column of it.'}
    </p>
    <Dialog.Footer>
      <Button size="sm" disabled={counted?.samples === 0} onclick={save}>Save…</Button>
    </Dialog.Footer>
  </Dialog.Content>
</Dialog.Root>
