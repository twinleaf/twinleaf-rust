<script>
  // The Properties data table: a column per checked device and a row per
  // RPC name, merged and grouped as properties.js says, of the channels a
  // panel shows, with an edit bar under it for the rows picked, or the row
  // the arrows are on (editbar.js), under the panel the arrows are in. A
  // cell's tooltip holds its whole value, which the column may cut short.
  // A panel of actions alone is a row of buttons instead: a click runs its
  // action on every checked device that has it, and Ctrl or Shift picks it
  // as a row, each picked button numbered in the order it runs. Running
  // several actions asks first, listing them.
  //
  // A cell shows the device's value as the page's DeviceWrites has it
  // (writes.svelte.js): announced, read on request for an entry no
  // announcement keeps, or written and not yet announced anew. A row whose
  // devices' values differ, a known value beside an unknown one included,
  // is marked ≠. Writing sends one value to every checked device that has
  // the entry writable, each encoded by that device's own type, shows each
  // device's outcome in its cell, and a notice that offers Revert.
  //
  // Rows are picked as a sheet's are, to copy and to edit: a click picks
  // one, Shift the run to it from the row a pick started at, Ctrl or ⌘ adds
  // or drops one; Shift with the arrows extends, and Ctrl+A picks every row.
  // Ctrl+C, or Copy in the right-click menu, copies the picked rows, a
  // property and each device's value apiece, as a sheet pastes them. A star
  // pins a property to the Favorites panel, which shows every starred
  // property of the checked devices.
  //
  // Up to MAX_COLUMNS devices get a column each; more share one column,
  // which shows the value they agree on or how many values they differ in.
  // Each cell reads its own device's value, so one device's change redraws
  // only its cells.
  import { getContext, tick, untrack } from 'svelte';
  import { getCoreRowModel } from '@tanstack/table-core';
  import { createSvelteTable, FlexRender, renderSnippet } from '$lib/components/ui/data-table/index.js';
  import * as Table from '$lib/components/ui/table/index.js';
  import { ScrollArea } from '$lib/components/ui/scroll-area/index.js';
  import { Button } from '$lib/components/ui/button/index.js';
  import { Input } from '$lib/components/ui/input/index.js';
  import Tip from './Tip.svelte';
  import { controlOf, deviceLabel, isButtons, isFavorites, mergeRows, panelRows, sameShown, writersOf } from './properties.js';
  import { editBar, kindOf } from './editbar.js';
  import { counted } from './menus.js';
  import { writeTables } from './copy.js';
  import { toast } from './toast.svelte.js';
  import * as words from './words.js';
  import * as ContextMenu from '$lib/components/ui/context-menu/index.js';
  import * as AlertDialog from '$lib/components/ui/alert-dialog/index.js';
  import RiStar from '@lucide/svelte/icons/star';
  import RiCopy from '@lucide/svelte/icons/clipboard-copy';

  /**
   * `devices`: the checked Devices, a column each, in order. `channels`:
   * the RPC channels shown, as properties.js names them, every one by
   * default. `panel` names the panel, so its rows' picks are told from
   * another panel's. `filter` is the words every row shown has in its name.
   */
  let { devices, channels = undefined, panel = 'properties', filter = '' } = $props();

  /** The rows PageUp and PageDown move by. */
  const PAGE = 10;
  /** The most devices that get a column each. */
  const MAX_COLUMNS = 8;

  // The page's History, which makes each pin one step, and its stars.
  const history = getContext('history');
  const favorites = history.favorites;

  /** Whether this is the Favorites panel, of every starred property. */
  const starredOnly = $derived(isFavorites(channels));
  const rows = $derived(panelRows(devices, channels, favorites, filter));
  const grouped = $derived(!channels || channels.length > 1 || starredOnly);
  const buttons = $derived(isButtons(channels));

  // The page's writes to devices: each value shown, each device's outcome
  // of its last call, and the notices.
  const writes = getContext('writes');
  const valueOf = (d, name) => writes.valueOf(d, name);
  const outcomeOf = (d, name) => writes.outcomeOf(d.route, name);
  const show = (v) => (v === undefined ? '' : v === null ? '·' : String(v));
  /** Whether the devices that have `row` disagree, a known value beside an unknown one included. */
  function mixed(row) {
    const values = devices.filter((d) => row.entries.has(d.route)).map((d) => valueOf(d, row.name));
    return values.some((v) => !sameShown(v, values[0]));
  }
  /** The value every device that has `row` agrees on, or undefined. */
  const shared = (row) => (mixed(row) ? undefined : valueOf(devices.find((d) => row.entries.has(d.route)) ?? { route: '', values: new Map() }, row.name));

  /** The devices whose last call of `row` failed, or may have. */
  const failures = (row) => devices.filter((d) => ['error', 'refused', 'unknown'].includes(outcomeOf(d, row.name)?.state));
  const failureText = (row) => failures(row).map((d) => `${words.device(d)}: ${words.outcome(outcomeOf(d, row.name).state, outcomeOf(d, row.name).error)}`).join('; ');
  const busy = (row) => devices.some((d) => outcomeOf(d, row.name)?.state === 'busy');

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

  // The rows picked, the page's row pick (selection.svelte.js), which spans
  // panels, and the row the arrows are on, the page's one, when it is this
  // panel's, which is read when no announcement keeps it.
  const selection = getContext('selection');
  const prefix = `${panel}:`;
  /** The index of the row the arrows are on, or -1 when it is another panel's or none. */
  const active = $derived(rows.findIndex((r) => prefix + r.name === selection.rowAt));
  const current = $derived(active < 0 ? undefined : rows[active]);
  let tableEl = $state(null);
  $effect(() => {
    const row = current;
    if (row) untrack(() => writes.readMissing(row.name, devices.filter((d) => row.entries.get(d.route)?.readable)));
  });

  // The rows picked: this panel's by name, and every panel's, each property
  // once, in page order.
  const picked = $derived(new Set([...selection.rows].filter((k) => k.startsWith(prefix)).map((k) => k.slice(prefix.length))));
  const nameOf = (k) => k.slice(k.indexOf(':') + 1);
  /** The names of every picked row, each once, in page order. */
  const pickedNames = () => [...new Set((selection.rowOrder?.() ?? [...selection.rows]).filter((k) => selection.rows.has(k)).map(nameOf))];
  /** Picks row `i` as a click with `e`'s keys does (`Selection.pickRow`). */
  function pick(i, e) {
    const name = rows[i]?.name;
    if (!name) return;
    selection.pickRow(prefix + name, e);
  }

  // The edit bar: the rows picked, each property once, in the order they
  // were picked, or the row the arrows are on when none is. It shows under
  // the panel the arrows are in, and Enter edits its rows there; in another
  // panel's table, Enter does nothing.
  const byName = $derived(new Map(mergeRows(devices).map((r) => [r.name, r])));
  /** Every row picked, each property once, in the order picked. */
  const pickedRows = $derived([...new Set([...selection.rows].map(nameOf))].map((n) => byName.get(n)).filter(Boolean));
  const editing = $derived(!current ? [] : pickedRows.length ? pickedRows : [current]);
  const bar = $derived(editBar(editing, devices, valueOf));
  /** Whether the bar edits switches, and so writes true or false. */
  const switching = $derived(bar?.kind === 'switches' || bar?.control === 'toggle');
  let draft = $state('');
  let editor = $state(null);
  let lastEdit = null;
  $effect(() => {
    // New rows fill the field with the value every device of every row agrees on.
    const names = editing.map((r) => r.name).join('\n');
    if (names === lastEdit) return;
    lastEdit = names;
    draft = untrack(() => (bar?.kind === 'values' ? show(bar.shared) : bar?.kind === 'one' ? show(shared(editing[0])) : ''));
  });
  /** Writes `text` to every row of the bar, on the devices a write of each reaches. */
  const writeBar = (text) => writes.write(bar.rows.map((r) => ({ name: r.name, devices: writersOf(r, devices) })), text, switching);
  /** Runs actions `names` in order, each on every device that has it. */
  const runNames = (names) => writes.run(names, (n) => (byName.get(n) ? writersOf(byName.get(n), devices) : []));
  /** The actions waiting on the question running several asks, in order, or null. */
  let asking = $state(null);
  /** Runs the bar's actions: one at once, several once the person says so. */
  function runBar() {
    const names = bar.rows.map((r) => r.name);
    if (names.length > 1) asking = names;
    else runNames(names);
  }
  /** Enter on the bar's rows: runs them, turns switches, or goes to the field; on rows of several kinds it says so. */
  function enter() {
    if (!bar) return;
    if (bar.kind === 'kinds') toast(bar.hint);
    else if (bar.kind === 'actions' || bar.control === 'action') runBar();
    else if (switching) writeBar(bar.enter);
    else editor?.focus();
  }
  /** Whether the rows picked are several actions, which run in the order picked. */
  const runList = $derived(pickedRows.length > 1 && pickedRows.every((r) => kindOf(r) === 'action'));
  /** The number of action `name` among several picked to run, from 1, or null. */
  const runOrder = (name) => (runList ? pickedRows.findIndex((r) => r.name === name) + 1 || null : null);
  /** A click on the button of action `row`: it runs, picked alone; with Ctrl, ⌘ or Shift it is picked as a row is, and does not run. */
  function pillClick(row, e) {
    selection.pickRow(prefix + row.name, e);
    if (!(e.ctrlKey || e.metaKey || e.shiftKey)) runNames([row.name]);
  }
  // Pins act on every picked row, or the active one, as one undo step.
  /** The names a pin from the menu acts on: the picked rows', or the active row's. */
  const pinNames = () => (selection.rows.size ? pickedNames() : current ? [current.name] : []);
  /** Stars `names` when `on`, or takes their stars away, as one step. */
  function pin(names, on) {
    const changed = names.filter((n) => favorites.has(n) !== on);
    if (!changed.length) return;
    const what = `${on ? 'Add' : 'Remove'} ${changed.length > 1 ? `${changed.length} ` : ''}${on ? 'to' : 'from'} Favorites`;
    history.group(what, () => favorites.pin(changed, on));
  }
  /** A star clicked on row `name`: on a picked row it sets every picked row as this one turns, on another just this one. */
  function starClicked(name) {
    const on = !favorites.has(name);
    pin(picked.has(name) ? pickedNames() : [name], on);
  }
  /** The rows Copy copies: every picked row, each property once, in page order, or the active one. */
  function copied() {
    const byName = new Map(mergeRows(devices).map((r) => [r.name, r]));
    const chosen = pickedNames().map((n) => byName.get(n)).filter(Boolean);
    return chosen.length ? chosen : current ? [current] : [];
  }
  /** Copies `copied()`: a header of the devices, then each property and its values, as a sheet pastes them. */
  function copyRows() {
    const list = copied();
    if (!list.length) return;
    const each = devices.length <= MAX_COLUMNS;
    const header = ['Property', ...(each ? devices.map(deviceLabel) : [`${devices.length} devices`])];
    const body = list.map((r) => [r.name, ...(each ? devices.map((d) => (r.entries.has(d.route) ? show(valueOf(d, r.name)) : '')) : [show(shared(r))])]);
    writeTables([{ title: '', header, rows: body }]).then(
      () => toast.success(`Copied ${list.length} propert${list.length === 1 ? 'y' : 'ies'}`),
      (error) => toast.error(`Could not copy: ${error.message}`),
    );
  }

  function tableKey(e) {
    const n = rows.length;
    if ((e.ctrlKey || e.metaKey) && (e.key === 'c' || e.key === 'C')) {
      copyRows();
      e.preventDefault();
      e.stopPropagation();
      return;
    }
    if ((e.ctrlKey || e.metaKey) && (e.key === 'a' || e.key === 'A')) {
      selection.pickRows(rows.map((r) => prefix + r.name), current ? prefix + current.name : rows[0] ? prefix + rows[0].name : null);
      e.preventDefault();
      e.stopPropagation();
      return;
    }
    // From no row of this table, a move goes to its first row, or End to its last.
    const steps = { ArrowDown: 1, ArrowUp: -1, PageDown: PAGE, PageUp: -PAGE };
    const to = e.key === 'Home' ? 0 : e.key === 'End' ? n - 1 : e.key in steps ? (active < 0 ? 0 : active + steps[e.key]) : undefined;
    if (to !== undefined) {
      const i = Math.max(0, Math.min(n - 1, to));
      // The arrows pick the row they move to; with Shift they pick the run to it.
      pick(i, { shiftKey: e.shiftKey });
      tick().then(() => document.getElementById(`prop-${active}`)?.scrollIntoView({ block: 'nearest' }));
    } else if (e.key === 'Enter') enter();
    else return;
    e.preventDefault();
    e.stopPropagation();
  }
  function editorKey(e) {
    if (e.key === 'Enter' && bar && draft.trim() !== '') {
      e.preventDefault();
      writeBar(draft.trim());
    } else if (e.key === 'Escape') {
      e.preventDefault();
      lastEdit = null; // puts the shared value back
      draft = '';
      tableEl?.focus();
    }
  }
</script>

{#snippet nameCell({ row })}
  {@const starred = favorites.has(row.name)}
  <button class="star" class:on={starred} tabindex="-1" aria-pressed={starred} aria-label="{starred ? 'Remove' : 'Add'} {row.name} {starred ? 'from' : 'to'} Favorites" title={starred ? 'Remove from Favorites' : 'Add to Favorites'} onclick={(e) => (e.stopPropagation(), starClicked(row.name))}><RiStar /></button>
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
  {@const out = outcomeOf(device, row.name)}
  <span class="cell {out?.state ?? ''}" title={out && out.state !== 'busy' && out.state !== 'ok' ? words.outcome(out.state, out.error) : show(valueOf(device, row.name))}>
    {#if !entry}<span class="absent" title="This device has no {row.name}">—</span>
    {:else if controlOf(entry) === 'action'}<span class="absent">action</span>
    {:else}{show(valueOf(device, row.name))}{/if}
    {#if out?.state === 'unknown'}<b class="mark warn">?</b>{:else if out?.state === 'error' || out?.state === 'refused'}<b class="mark bad">✕</b>{:else if out?.state === 'skipped'}<b class="mark">–</b>{/if}
  </span>
{/snippet}

{#if !rows.length}
  <div class="props-empty">{filter.trim() ? 'No property matches' : starredOnly ? 'Star a property to add it here' : 'Nothing here on the checked devices'}</div>
{:else if buttons}
  <ScrollArea class="props">
    <div class="action-grid" data-rows>
      {#each rows as row (row.name)}
        {@const reach = writersOf(row, devices)}
        {@const failed = failures(row)}
        {@const order = runOrder(row.name)}
        <Tip text={failed.length ? failureText(row) : words.runOn(reach.length)}>
          {#snippet children(tip)}
            <Button
              {...tip}
              size="xs"
              variant="outline"
              class="{busy(row) ? 'opacity-60' : failed.length ? 'text-destructive' : ''} {picked.has(row.name) ? 'picked' : ''}"
              disabled={!reach.length}
              data-name={row.name}
              aria-pressed={picked.has(row.name)}
              onclick={(e) => pillClick(row, e)}
            >
              {#if order}<b class="run-order" aria-label="runs {order}">{order}</b>{/if}{row.name}{#if failed.length}<b class="mark bad">✕{devices.length > 1 ? failed.length : ''}</b>{/if}
            </Button>
          {/snippet}
        </Tip>
      {/each}
    </div>
  </ScrollArea>
{:else}
<ContextMenu.Root>
<ContextMenu.Trigger>
{#snippet child({ props })}
<div {...props} class="props-wrap" oncontextmenucapture={(e) => {
  // A right-click on a row not picked picks it alone, as a sheet does.
  const i = Number(e.target.closest?.('tr[id^=prop-]')?.id.slice(5));
  if (Number.isInteger(i) && rows[i]) selection.rowMenu(prefix + rows[i].name);
}}>
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
          class={picked.has(row.original.name) ? 'picked' : ''}
          aria-selected={picked.has(row.original.name)}
          onclick={(e) => (pick(i, e), tableEl?.focus())}
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
</div>
{/snippet}
</ContextMenu.Trigger>
<ContextMenu.Content class="menu-icons">
  <!-- What Copy copies (`copied`): every picked row, or the active row. -->
  {@const copies = copied()}
  {@const one = copies.length === 1 ? copies[0] : null}
  <ContextMenu.Item onSelect={copyRows}><RiCopy class="menu-icon" />Copy {copies.length > 1 ? `${copies.length} rows` : 'row'}{#if copies.length}<span class="menu-value">{one ? show(shared(one)) || 'Mixed' : `${counted(copies.length, 'row')} · ${counted(devices.filter((d) => copies.some((r) => r.entries.has(d.route))).length, 'device')}`}</span>{/if}<ContextMenu.Shortcut><kbd>Ctrl</kbd><kbd>C</kbd></ContextMenu.Shortcut></ContextMenu.Item>
  <!-- A pick some of whose rows are pinned offers both, each for the rows it changes. -->
  {@const names = pinNames()}
  {@const toPin = names.filter((n) => !favorites.has(n))}
  {@const toUnpin = names.filter((n) => favorites.has(n))}
  {@const count = (list) => (toPin.length && toUnpin.length) || list.length > 1 ? `${list.length} ` : ''}
  {#if toPin.length}
    <ContextMenu.Item onSelect={() => pin(toPin, true)}><RiStar class="menu-icon" />Add {count(toPin)}to Favorites<span class="menu-value">{favorites.names.size} starred</span></ContextMenu.Item>
  {/if}
  {#if toUnpin.length}
    <ContextMenu.Item onSelect={() => pin(toUnpin, false)}><RiStar class="menu-icon" />Remove {count(toUnpin)}from Favorites<span class="menu-value">{favorites.names.size} starred</span></ContextMenu.Item>
  {/if}
</ContextMenu.Content>
</ContextMenu.Root>

{/if}

{#if bar && !(buttons && bar.rows.length < 2)}
  {@const n = bar.reach.length}
  <!-- The bar acts on the rows picked, so a press in it keeps them, as one among the rows does. -->
  <div class="write-bar" class:several={bar.rows.length > 1} data-rows>
    <span class="wname" title={bar.detail ?? bar.label}>{bar.label}</span>
    {#if bar.kind === 'kinds'}
      <span class="reach">{bar.hint}</span>
    {:else if bar.kind === 'actions' || bar.control === 'action'}
      <Button size="xs" variant="outline" disabled={!n} onclick={runBar}>{bar.rows.length > 1 ? words.runSeveral(bar.rows.length) : words.runOn(n)}</Button>
    {:else if switching}
      <Button size="xs" variant="outline" disabled={!n} onclick={() => writeBar('true')}>{words.ON}</Button>
      <Button size="xs" variant="outline" disabled={!n} onclick={() => writeBar('false')}>{words.OFF}</Button>
      <span class="reach">{n} device{n === 1 ? '' : 's'}</span>
    {:else}
      {@const differ = bar.kind === 'values' ? bar.shared === undefined : mixed(bar.rows[0])}
      <Input
        class="wvalue h-6 text-xs"
        bind:ref={editor}
        bind:value={draft}
        placeholder={differ ? words.MIXED : (bar.rows[0].rpc.type ?? '')}
        aria-label="New value of {bar.label}"
        onkeydown={editorKey}
      />
      <Button size="xs" variant="outline" disabled={!n || draft.trim() === ''} onclick={() => writeBar(draft.trim())}>{words.writeTo(n)}</Button>
    {/if}
  </div>
{/if}

<AlertDialog.Root open={!!asking} onOpenChange={(open) => !open && (asking = null)}>
  <AlertDialog.Content>
    {@const ask = words.runAsk(asking ?? [], bar?.reach.length ?? 0)}
    <AlertDialog.Header>
      <AlertDialog.Title>{ask.title}</AlertDialog.Title>
      <AlertDialog.Description>{ask.text}</AlertDialog.Description>
    </AlertDialog.Header>
    <AlertDialog.Footer>
      <AlertDialog.Cancel>{ask.cancel}</AlertDialog.Cancel>
      <AlertDialog.Action onclick={() => (runNames(asking), (asking = null))}>{ask.run}</AlertDialog.Action>
    </AlertDialog.Footer>
  </AlertDialog.Content>
</AlertDialog.Root>
