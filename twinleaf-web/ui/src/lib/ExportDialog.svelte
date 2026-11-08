<script>
  // Exporting plots' signals to a file: every raw sample of them, the time
  // the plots show or the whole history, as CSV or HDF5. The hub writes the
  // file; the page only names it and saves it (exportfile.js).
  import * as Dialog from '$lib/components/ui/dialog/index.js';
  import { Button } from '$lib/components/ui/button/index.js';
  import { FORMATS, exportName, saveExport } from './exportfile.js';
  import { bytesText, spanText } from './units.js';
  import { toast } from './toast.svelte.js';

  /**
   * `what` is the export to offer, or null: {hub, traces, shown, whole},
   * `shown` the time [t0, t1] the plots show, null with none of time, and
   * `whole` the time the history holds of the traces.
   */
  let { what = $bindable(null) } = $props();

  let range = $state('shown');
  let format = $state('csv');
  const open = $derived(!!what);
  const chosen = $derived(what && (range === 'shown' && what.shown ? what.shown : what.whole));
  // About how many samples the file holds, at each signal's rate.
  const samples = $derived(what && chosen ? Math.round(what.traces.reduce((n, t) => n + t.meta.rate * (chosen[1] - chosen[0]), 0)) : 0);

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
      {#if chosen}{spanText(chosen[1] - chosen[0])}, about {samples.toLocaleString()} samples.{/if}
      {format === 'csv' ? 'One time column, one column per signal.' : 'One table per stream, every column of it.'}
    </p>
    <Dialog.Footer>
      <Button size="sm" onclick={save}>Save…</Button>
    </Dialog.Footer>
  </Dialog.Content>
</Dialog.Root>
