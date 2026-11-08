<script>
  // The Properties data table: a column per checked device and a row per
  // RPC name, merged and grouped as properties.js says, of the channels a
  // panel shows, with an edit bar under it for the row the arrows are on
  // when that row can be written or run. A cell's tooltip holds its whole
  // value, which the column may cut short.
  // A panel of actions alone is a row of buttons instead, each running its
  // action on every checked device that has it.
  //
  // A cell shows the device's announced value, or one read on request for an
  // entry no announcement keeps. A value written shows from the call until
  // the device announces anew, so an announcement older than the write never
  // steps the cell back. A row whose
  // devices' values differ, a known value beside an unknown one included,
  // is marked ≠. Writing sends one value to every checked device that has
  // the entry writable, each encoded by that device's own type, and shows
  // each device's outcome in its cell.
  //
  // Up to MAX_COLUMNS devices get a column each; more share one column,
  // which shows the value they agree on or how many values they differ in.
  // Each cell reads its own device's value, so one device's change redraws
  // only its cells. The caches here are of one hub, as the parent remounts
  // this for each session.
  import { tick } from 'svelte';
  import { SvelteMap } from 'svelte/reactivity';
  import { getCoreRowModel } from '@tanstack/table-core';
  import { createSvelteTable, FlexRender, renderSnippet } from '$lib/components/ui/data-table/index.js';
  import * as Table from '$lib/components/ui/table/index.js';
  import { ScrollArea } from '$lib/components/ui/scroll-area/index.js';
  import { Button } from '$lib/components/ui/button/index.js';
  import { Input } from '$lib/components/ui/input/index.js';
  import Tip from './Tip.svelte';
  import { api } from './protocol.js';
  import { sameValue } from './session.svelte.js';
  import { controlOf, deviceLabel, mergeRows, writersOf } from './properties.js';

  /**
   * `devices`: the checked Devices, a column each, in order. `channels`:
   * the RPC channels shown, as properties.js names them, every one by
   * default.
   */
  let { devices, channels = undefined } = $props();

  /** How long a cell shows that a write took, in ms. */
  const OK_MS = 1500;
  /** The rows PageUp and PageDown move by. */
  const PAGE = 10;
  /** The most devices that get a column each. */
  const MAX_COLUMNS = 8;

  const rows = $derived(mergeRows(devices, channels));
  const grouped = $derived(!channels || channels.length > 1);
  const buttons = $derived(channels?.length === 1 && channels[0] === 'action');

  // Values read on request, each device's outcome of the last call of a
  // name, and the values written, by route and name. A written value is
  // {value, over, inFlight}: `over` the announced value it replaces.
  const reads = new SvelteMap();
  const outcomes = new SvelteMap();
  const written = new SvelteMap();
  const key = (route, name) => `${route}\u0000${name}`;

  /** The value shown for `name` on device `d`, or undefined when none is known. */
  function valueOf(d, name) {
    const k = key(d.route, name);
    const announced = d.values.get(name);
    const w = written.get(k);
    if (w && (w.inFlight || sameValue(announced, w.over))) return w.value;
    return announced !== undefined ? announced : reads.get(k);
  }
  const show = (v) => (v === undefined ? '' : v === null ? '·' : String(v));
  /** Whether the devices that have `row` disagree, a known value beside an unknown one included. */
  function mixed(row) {
    const values = devices.filter((d) => row.entries.has(d.route)).map((d) => valueOf(d, row.name));
    return values.some((v) => !sameValue(v, values[0]));
  }
  /** The value every device that has `row` agrees on, or undefined. */
  const shared = (row) => (mixed(row) ? undefined : valueOf(devices.find((d) => row.entries.has(d.route)) ?? { route: '', values: new Map() }, row.name));

  /** The devices whose last call of `row` failed, or may have. */
  const failures = (row) => devices.filter((d) => ['error', 'refused', 'unknown'].includes(outcomes.get(key(d.route, row.name))?.state));
  const failureText = (row) => failures(row).map((d) => `${d.route}: ${outcomes.get(key(d.route, row.name))?.error ?? 'unknown'}`).join('; ');
  const busy = (row) => devices.some((d) => outcomes.get(key(d.route, row.name))?.state === 'busy');

  /** How many values the devices that have `row` differ in, an unknown one included. */
  function variety(row) {
    const shown = devices.filter((d) => row.entries.has(d.route)).map((d) => JSON.stringify(valueOf(d, row.name)) ?? 'unknown');
    return new Set(shown).size;
  }

  // The column definitions keep their identity while the devices do, as
  // the table rebuilds every column whenever it is handed new ones.
  const nameColumn = { id: 'name', header: 'Property', cell: ({ row }) => renderSnippet(nameCell, { row: row.original }) };
  const deviceColumns = new Map();
  const deviceColumn = (d) => {
    let column = deviceColumns.get(d);
    if (!column) {
      column = { id: d.route, header: () => deviceLabel(d), cell: ({ row }) => renderSnippet(valueCell, { row: row.original, device: d }) };
      deviceColumns.set(d, column);
    }
    return column;
  };
  const allColumn = { id: 'all', header: () => `${devices.length} devices`, cell: ({ row }) => renderSnippet(allCell, { row: row.original }) };
  const columns = $derived(devices.length <= MAX_COLUMNS ? [nameColumn, ...devices.map(deviceColumn)] : [nameColumn, allColumn]);

  const table = createSvelteTable({
    get data() {
      return rows;
    },
    get columns() {
      return columns;
    },
    getCoreRowModel: getCoreRowModel(),
    getRowId: (row) => row.name,
  });

  // The row the arrows are on, and the editor below the table.
  let active = $state(0);
  let draft = $state('');
  let editor = $state(null);
  let tableEl = $state(null);
  const current = $derived(rows[Math.min(active, rows.length - 1)]);
  let lastRow = null;
  $effect(() => {
    // A new row fills the editor with the value every device agrees on.
    const row = current;
    if (!row || row.name === lastRow) return;
    lastRow = row.name;
    draft = show(shared(row));
    readMissing(row);
  });

  /** Reads `row` from each device that has no value of it, as announcements keep only some entries. */
  function readMissing(row) {
    for (const d of devices) {
      if (!row.entries.get(d.route)?.readable || valueOf(d, row.name) !== undefined) continue;
      api
        .read(d.route, [row.name])
        .then((reply) => {
          const got = reply.values[row.name];
          if (got?.state === 'ok') reads.set(key(d.route, row.name), got.value);
        })
        .catch(() => {});
    }
  }

  /** Calls `row` on every device a write reaches, with `text`, and shows each device's outcome. */
  async function write(row, text) {
    const targets = writersOf(row, devices);
    if (!targets.length) return;
    for (const d of targets) {
      outcomes.set(key(d.route, row.name), { state: 'busy' });
      if (controlOf(row.rpc) !== 'action') written.set(key(d.route, row.name), { value: text, over: d.values.get(row.name), inFlight: true });
    }
    let results;
    try {
      results = (await api.call(row.name, text, targets.map((d) => d.route))).results;
    } catch (error) {
      results = targets.map((d) => ({ state: 'unknown', route: d.route, error: error.message }));
    }
    for (const result of results) {
      const k = key(result.route, row.name);
      const w = written.get(k);
      if (result.state !== 'ok') {
        // Nothing was written, or nobody knows what was: the cell shows what the device announces.
        written.delete(k);
        const error = result.state === 'skipped' ? 'Not sent, as another device refused the call' : result.error;
        outcomes.set(k, { state: result.state, error });
        continue;
      }
      if (w) written.set(k, { ...w, value: result.value ?? w.value, inFlight: false });
      else if (result.value !== null) reads.set(k, result.value);
      outcomes.set(k, { state: 'ok' });
      setTimeout(() => outcomes.get(k)?.state === 'ok' && outcomes.delete(k), OK_MS);
    }
  }

  function tableKey(e) {
    const n = rows.length;
    const to = { ArrowDown: active + 1, ArrowUp: active - 1, Home: 0, End: n - 1, PageDown: active + PAGE, PageUp: active - PAGE }[e.key];
    if (to !== undefined) {
      active = Math.max(0, Math.min(n - 1, to));
      tick().then(() => document.getElementById(`prop-${active}`)?.scrollIntoView({ block: 'nearest' }));
    } else if (e.key === 'Enter' && current) {
      const control = controlOf(current.rpc);
      if (control === 'action') write(current, '');
      else if (control === 'toggle') write(current, shared(current) ? 'false' : 'true');
      else editor?.focus();
    } else return;
    e.preventDefault();
    e.stopPropagation();
  }
  function editorKey(e) {
    if (e.key === 'Enter' && current) {
      e.preventDefault();
      write(current, draft.trim());
    } else if (e.key === 'Escape') {
      e.preventDefault();
      lastRow = null; // puts the shared value back
      draft = '';
      tableEl?.focus();
    }
  }
</script>

{#snippet nameCell({ row })}
  <span class="pname" title="{row.rpc.type ?? 'no type'} · {row.rpc.access.join('/') || 'action'}{row.rpc.persistent ? ' · persistent' : ''}">
    {row.name}{#if mixed(row)}<b class="mark" title="The devices differ">≠</b>{/if}{#if row.differs}<b class="mark warn" title="The devices declare it differently">!</b>{/if}
  </span>
{/snippet}

{#snippet allCell({ row })}
  {@const n = variety(row)}
  {@const failed = failures(row)}
  <span class="cell {busy(row) ? 'busy' : ''}" title={failureText(row)}>
    {#if controlOf(row.rpc) === 'action'}<span class="absent">action</span>
    {:else if n > 1}<span class="absent">{n} values</span>
    {:else}{show(shared(row))}{/if}
    {#if failed.length}<b class="mark bad">✕{failed.length}</b>{/if}
  </span>
{/snippet}

{#snippet valueCell({ row, device })}
  {@const entry = row.entries.get(device.route)}
  {@const out = outcomes.get(key(device.route, row.name))}
  <span class="cell {out?.state ?? ''}" title={out?.error ?? show(valueOf(device, row.name))}>
    {#if !entry}<span class="absent" title="This device has no {row.name}">—</span>
    {:else if controlOf(entry) === 'action'}<span class="absent">action</span>
    {:else}{show(valueOf(device, row.name))}{/if}
    {#if out?.state === 'unknown'}<b class="mark warn">?</b>{:else if out?.state === 'error' || out?.state === 'refused'}<b class="mark bad">✕</b>{:else if out?.state === 'skipped'}<b class="mark">–</b>{/if}
  </span>
{/snippet}

{#if !rows.length}
  <div class="props-empty">Nothing here on the checked devices</div>
{:else if buttons}
  <ScrollArea class="props">
    <div class="action-grid">
      {#each rows as row (row.name)}
        {@const reach = writersOf(row, devices)}
        {@const failed = failures(row)}
        <Tip text={failed.length ? failureText(row) : `Run on ${reach.length} device${reach.length === 1 ? '' : 's'}`}>
          {#snippet children(tip)}
            <Button
              {...tip}
              size="xs"
              variant="outline"
              class={busy(row) ? 'opacity-60' : failed.length ? 'text-destructive' : ''}
              disabled={!reach.length}
              onclick={() => write(row, '')}
            >
              {row.name}{#if failed.length}<b class="mark bad">✕{devices.length > 1 ? failed.length : ''}</b>{/if}
            </Button>
          {/snippet}
        </Tip>
      {/each}
    </div>
  </ScrollArea>
{:else}
<ScrollArea class="props">
  <Table.Root
    class="props-table"
    role="grid"
    data-focus-own
    tabindex={0}
    aria-label="Properties: arrows move, Enter edits or runs"
    aria-activedescendant={current ? `prop-${active}` : undefined}
    bind:ref={tableEl}
    onkeydown={tableKey}
  >
    <Table.Header>
      {#each table.getHeaderGroups() as group (group.id)}
        <Table.Row>
          {#each group.headers as header (header.id)}
            <Table.Head class={header.id === 'name' ? '' : 'text-right'}>
              <FlexRender content={header.column.columnDef.header} context={header.getContext()} />
            </Table.Head>
          {/each}
        </Table.Row>
      {/each}
    </Table.Header>
    <Table.Body>
      {#each table.getRowModel().rows as row, i (row.id)}
        {#if grouped && (i === 0 || row.original.group !== rows[i - 1]?.group)}
          <Table.Row class="group-row" aria-hidden="true">
            <Table.Cell colspan={columns.length}>{row.original.group}</Table.Cell>
          </Table.Row>
        {/if}
        <!-- svelte-ignore a11y_click_events_have_key_events -->
        <Table.Row
          id="prop-{i}"
          data-state={i === active ? 'selected' : undefined}
          aria-selected={i === active}
          onclick={() => ((active = i), tableEl?.focus())}
        >
          {#each row.getVisibleCells() as cell (cell.id)}
            <Table.Cell class={cell.column.id === 'name' ? '' : 'text-right'}>
              <FlexRender content={cell.column.columnDef.cell} context={cell.getContext()} />
            </Table.Cell>
          {/each}
        </Table.Row>
      {/each}
    </Table.Body>
  </Table.Root>
</ScrollArea>

{#if current && ['action', 'toggle', 'value'].includes(controlOf(current.rpc))}
  {@const control = controlOf(current.rpc)}
  {@const reach = writersOf(current, devices)}
  <div class="write-bar">
    <span class="wname">{current.name}</span>
    {#if control === 'action'}
      <Button size="xs" variant="outline" disabled={!reach.length} onclick={() => write(current, '')}>Run on {reach.length}</Button>
    {:else if control === 'toggle'}
      <Button size="xs" variant="outline" disabled={!reach.length} onclick={() => write(current, 'true')}>On</Button>
      <Button size="xs" variant="outline" disabled={!reach.length} onclick={() => write(current, 'false')}>Off</Button>
      <span class="reach">{reach.length} device{reach.length === 1 ? '' : 's'}</span>
    {:else if control === 'value'}
      <Input
        class="wvalue h-6 text-xs"
        bind:ref={editor}
        bind:value={draft}
        placeholder={mixed(current) ? '— mixed' : (current.rpc.type ?? '')}
        aria-label="New value of {current.name}"
        onkeydown={editorKey}
      />
      <Button size="xs" variant="outline" disabled={!reach.length || draft.trim() === ''} onclick={() => write(current, draft.trim())}>Write to {reach.length}</Button>
    {/if}
  </div>
{/if}
{/if}
