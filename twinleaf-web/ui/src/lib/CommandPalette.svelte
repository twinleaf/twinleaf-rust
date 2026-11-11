<script>
  // The command palette, on Ctrl+K or ⌘K: add a signal, the commands the
  // command table lists here (commands.js), under its headings, those on
  // plots while they have plots to act on, and the properties of the
  // checked devices, by typing part of a name. A signal named here goes
  // onto the selected plots, or into a new one, alone, by the commands the
  // Signals list runs.
  //
  // A property shows its kind by a mark (read, setting, action) and its
  // value at the right as the Properties panels show it, ≠ where the
  // devices differ. Space or Tab locks it, and the field takes its
  // argument; Enter reads, writes or runs it as palette.js says, through
  // the page's DeviceWrites, as the Properties panels do, to the devices a
  // call of it reaches there: a write shows the same notice, with Revert,
  // and closes the palette; a read shows the value in place.
  import { getContext, tick, untrack } from 'svelte';
  import { computeCommandScore } from 'bits-ui';
  import * as Command from '$lib/components/ui/command/index.js';
  import { PALETTE } from './commands.js';
  import { escapeTyped } from './keys.js';
  import { mergeRows, sameShown, writersOf } from './properties.js';
  import { actsOf, argProblem, findProperties, searching, step } from './palette.js';
  import * as words from './words.js';
  import RiRead from '@lucide/svelte/icons/eye';
  import RiSetting from '@lucide/svelte/icons/sliders-horizontal';
  import RiAction from '@lucide/svelte/icons/play';
  import RiStar from '@lucide/svelte/icons/star';

  /** `session` holds the signals it names, and `devices` are the checked devices, whose properties it offers. */
  let { open = $bindable(false), session, devices = [] } = $props();
  const history = getContext('history');
  const commands = getContext('commands');
  const writes = getContext('writes');
  const favorites = history.favorites;

  /** Closes the palette, then runs `fn`. */
  function run(fn) {
    open = false;
    fn();
  }

  // What is typed, and the property locked (palette.js `step`), each time
  // it opens anew; the value of the row the list highlights; and the
  // answers of the last read of the property locked.
  let state = $state(searching());
  let highlight = $state('');
  let answers = $state(null);
  $effect(() => {
    open;
    untrack(() => ((state = searching()), (answers = null)));
  });

  /** The value of a property's row in the list, as its item names it. */
  const PROPERTY = 'property ';
  const rows = $derived(mergeRows(devices));
  const byName = $derived(new Map(rows.map((r) => [r.name, r])));
  const found = $derived(findProperties(rows, state.query, favorites));

  // The list shows what matches what is typed, filtered here and not by the
  // list, which would reorder the rows it holds under the page: a property
  // as palette.js finds it, any other row by the list's own score of its
  // value. The properties come first when the best of them scores higher
  // than any other row.
  /** The table's commands the palette lists, each a row (commands.js `offered`), as it opens. */
  const offered = $derived(open ? commands.offered() : []);
  const items = $derived({
    add: session.signals.map((t) => `add ${t.key}`),
    newPlot: session.signals.map((t) => `new plot ${t.key}`),
    ...Object.fromEntries(PALETTE.map((group) => [group, offered.filter((c) => c.group === group).map((c) => c.find)])),
  });
  const scoreOf = (value) => computeCommandScore(value, state.query);
  /** Whether the row of `value` shows for what is typed. */
  const keep = (value) => !state.query || scoreOf(value) > 0;
  const kept = $derived(Object.fromEntries(Object.entries(items).map(([group, values]) => [group, values.some(keep)])));
  const best = $derived(state.query ? Math.max(0, ...Object.values(items).flat().map(scoreOf)) : 0);
  const propertiesFirst = $derived(!!state.query && found.total > 0 && found.shown[0].score > best);
  const nothing = $derived(!found.total && !Object.values(kept).some(Boolean));
  const lockedRow = $derived(state.mode === 'locked' ? (byName.get(state.name) ?? null) : null);
  const highlighted = $derived(highlight.startsWith(PROPERTY) ? (byName.get(highlight.slice(PROPERTY.length)) ?? null) : null);
  const problem = $derived(lockedRow ? argProblem(lockedRow, devices, state.arg) : null);

  /** The devices a read of `row` reaches: those whose entry is read. */
  const readers = (row) => devices.filter((d) => row.entries.get(d.route)?.readable);
  const show = (v) => (v === undefined ? '' : v === null ? '·' : String(v));
  /** The values of `row` on the devices that have it, as their cells show them. */
  const valuesOf = (row) => devices.filter((d) => row.entries.has(d.route)).map((d) => writes.valueOf(d, row.name));
  /** Whether the devices that have `row` disagree, a known value beside an unknown one included. */
  const differ = (values) => values.some((v) => !sameShown(v, values[0]));

  /** Does what `r`, a step of palette.js, says: takes its state, calls what it names, and closes when it says to. */
  function apply(r) {
    const wasLocked = state.mode === 'locked' ? state.name : null;
    state = r.state;
    if (r.act) perform(r.act);
    if (r.close) open = false;
    else if (wasLocked && state.mode === 'search') {
      // Unlocked: the list comes back with the property highlighted.
      const value = PROPERTY + wasLocked;
      tick().then(tick).then(() => state.mode === 'search' && (highlight = value));
    }
  }
  /** Reads, writes or runs a property, as the Properties panels do. */
  function perform({ type, name, text }) {
    const row = byName.get(name);
    if (!row) return;
    if (type === 'write') return void writes.write([{ name, devices: writersOf(row, devices) }], text, actsOf(row).toggle);
    if (type === 'run') return void writes.run([name], () => writersOf(row, devices));
    answers = null;
    writes.read(name, readers(row)).then((got) => {
      if (state.mode === 'locked' && state.name === name && state.read) answers = got;
    });
  }
  /** Enter on the property `row` of the list, or on the one locked, `pick` the switch value highlighted. */
  const enter = (row, pick = null) => apply(step(state, 'Enter', { row, devices, pick }));

  /** The field's keys of its own: Space, Tab and Backspace lock and unlock a property, as palette.js says. */
  function fieldKey(e) {
    if (![' ', 'Tab', 'Backspace'].includes(e.key) || e.ctrlKey || e.metaKey || e.altKey || (e.key === 'Tab' && e.shiftKey)) return;
    const r = step(state, e.key, { row: lockedRow ?? highlighted, devices });
    if (!r.keep) return;
    e.preventDefault();
    apply(r);
  }
  /**
   * Escape over the palette: with an argument typed it clears it, as in any
   * field; then it unlocks the property; then, or once a read shows, it
   * closes the palette.
   */
  function escape(e) {
    if (!lockedRow) return;
    if (escapeTyped(e, state.arg, '', () => (state = { ...state, arg: '', read: false }))) return;
    const r = step(state, 'Escape', { row: lockedRow, devices });
    if (r.keep) e.preventDefault();
    apply(r);
  }
  /** What the field shows: the search, or the argument of the property locked. */
  const typed = () => (state.mode === 'locked' ? state.arg : state.query);
  /** Types `v` into the field: the search, or the argument, which hides a read shown. */
  const type = (v) => (state = state.mode === 'locked' ? { ...state, arg: v, read: false } : { ...state, query: v });

  /** What the field asks for while `row` is locked. */
  function placeholder(row) {
    const acts = actsOf(row);
    if (acts.action) return 'Enter runs it';
    if (acts.writable) return `Type a value (${row.rpc.type ?? 'no type'})${acts.readable ? ', or Enter reads it' : ''}`;
    return 'Enter reads it';
  }
</script>

{#snippet mark(row)}
  {@const kind = actsOf(row).mark}
  <span class="pal-mark" data-mark={kind} title={kind === 'action' ? 'Action' : kind === 'setting' ? 'Setting' : 'Read-only'}>
    {#if kind === 'action'}<RiAction />{:else if kind === 'setting'}<RiSetting />{:else}<RiRead />{/if}
  </span>
{/snippet}

{#snippet value(row)}
  {#if actsOf(row).action}<span class="pal-value muted">action</span>
  {:else}
    {@const values = valuesOf(row)}
    {#if differ(values)}<span class="pal-value" title={values.map(show).join(' · ')}><b class="mark">≠</b></span>
    {:else}<span class="pal-value" title={show(values[0])}>{show(values[0])}</span>{/if}
  {/if}
{/snippet}

{#snippet properties()}
  {#if found.total}
    <!-- The best matches only: the rest are counted, not drawn. -->
    <Command.Group value="properties" heading={found.total > found.shown.length ? `Properties · best ${found.shown.length} of ${found.total}` : 'Properties'}>
      {#each found.shown as f (f.row.name)}
        <Command.Item value={PROPERTY + f.row.name} data-property={f.row.name} onSelect={() => enter(f.row)}>
          {@render mark(f.row)}<span class="pal-name">{f.row.name}</span>{#if favorites.has(f.row.name)}<RiStar class="pal-star" aria-label="Starred" />{/if}{@render value(f.row)}
        </Command.Item>
      {/each}
    </Command.Group>
  {/if}
{/snippet}

<Command.Dialog bind:open bind:value={highlight} shouldFilter={false} onEscapeKeydown={escape}>
  <Command.Input
    placeholder={lockedRow ? placeholder(lockedRow) : 'Add a signal, switch tab, arrange, find a property…'}
    bind:value={typed, type}
    aria-invalid={problem ? 'true' : undefined}
    onkeydown={fieldKey}
  >
    {#snippet before()}
      {#if lockedRow}<span class="pal-chip" data-locked={lockedRow.name}>{lockedRow.name}</span>{/if}
    {/snippet}
  </Command.Input>
  <Command.List>
    {#if lockedRow}
      {@const acts = actsOf(lockedRow)}
      {@const writers = writersOf(lockedRow, devices).length}
      <Command.Group value="locked" heading="{lockedRow.name} · {lockedRow.rpc.type ?? 'no type'} · {acts.mark}">
        {#if acts.toggle && state.arg === '' && !state.read}
          {#each [[words.ON, 'true'], [words.OFF, 'false']] as [label, pick] (pick)}
            <Command.Item value="locked {pick}" data-pick={pick} onSelect={() => enter(lockedRow, pick)}>
              {@render mark(lockedRow)}<span class="pal-act">{label}</span><span class="pal-value muted">{words.reach(writers)}</span>
            </Command.Item>
          {/each}
        {:else}
          {@const what = acts.action ? words.runOn(writers) : state.arg !== '' || !acts.readable ? words.writeTo(writers) : words.readFrom(readers(lockedRow).length)}
          <Command.Item value="locked" onSelect={() => enter(lockedRow)}>
            {@render mark(lockedRow)}<span class="pal-act">{what}</span>{@render value(lockedRow)}
          </Command.Item>
        {/if}
      </Command.Group>
      {#if problem}<div class="pal-problem" role="alert">{problem}</div>{/if}
      {#if state.read}
        <div class="pal-read" role="status">
          {#if !answers}Reading…
          {:else}
            {@const ok = answers.filter((a) => a.state === 'ok')}
            {#if ok.length === answers.length && !differ(ok.map((a) => a.value))}{lockedRow.name} = {show(ok[0]?.value)}{answers.length > 1 ? ` · ${words.reach(answers.length)}` : ''}
            {:else}{#each answers as a (a.device.route)}<span class="pal-answer">{words.device(a.device)}: {a.state === 'ok' ? show(a.value) : words.outcome(a.state, a.error)}</span>{/each}{/if}
          {/if}
          <span class="muted"> · Enter closes</span>
        </div>
      {/if}
    {:else}
      {#if nothing}<div class="pal-empty" role="status">Nothing matches.</div>{/if}
      {#if propertiesFirst}{@render properties()}{/if}
      {#if kept.add}
        <Command.Group heading="Add to the selected plots">
          {#each session.signals as t (t.key)}
            {#if keep(`add ${t.key}`)}<Command.Item value={`add ${t.key}`} data-command="addToPlots" onSelect={() => run(() => commands.run('addToPlots', { signals: [t.key], from: 'palette' }))}>{t.key}</Command.Item>{/if}
          {/each}
        </Command.Group>
      {/if}
      {#if kept.newPlot}
        <Command.Group heading="New plot with">
          {#each session.signals as t (t.key)}
            {#if keep(`new plot ${t.key}`)}<Command.Item value={`new plot ${t.key}`} data-command="newPlot" onSelect={() => run(() => commands.run('newPlot', { signals: [t.key], from: 'palette' }))}>{t.key}</Command.Item>{/if}
          {/each}
        </Command.Group>
      {/if}
      <Command.Separator />
      {#each PALETTE as group (group)}
        {#if kept[group]}
          <Command.Group heading={group}>
            {#each offered.filter((c) => c.group === group) as c}
              {#if keep(c.find)}<Command.Item value={c.find} data-command={c.id} onSelect={() => run(() => commands.run(c.id, { from: 'palette', how: c.how }))}>{c.label}</Command.Item>{/if}
            {/each}
          </Command.Group>
        {/if}
      {/each}
      {#if !propertiesFirst}{@render properties()}{/if}
    {/if}
  </Command.List>
</Command.Dialog>

<style>
  .pal-chip {
    font-family: var(--font-data);
    font-size: 12px;
    color: var(--foreground);
    background: var(--muted);
    border-radius: 4px;
    padding: 1px 6px;
    white-space: nowrap;
    max-width: 14rem;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .pal-mark {
    display: inline-flex;
    color: var(--muted-foreground);
  }
  .pal-mark :global(svg) {
    width: 12px;
    height: 12px;
  }
  .pal-name {
    font-family: var(--font-data);
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .pal-act {
    white-space: nowrap;
  }
  :global(.pal-star) {
    width: 11px !important;
    height: 11px !important;
    color: var(--muted-foreground);
  }
  /* A row with a value at its right has no check mark after it. */
  :global([data-slot='command-item']:has(.pal-value) > svg:last-child) {
    display: none;
  }
  .pal-value {
    margin-left: auto;
    font-family: var(--font-data);
    max-width: 40%;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .muted {
    color: var(--muted-foreground);
  }
  .pal-empty {
    padding: 24px 0;
    text-align: center;
    font-size: 14px;
  }
  .pal-problem,
  .pal-read {
    padding: 4px 12px 8px;
    font-size: 12px;
  }
  .pal-problem {
    color: var(--destructive);
  }
  .pal-read {
    font-family: var(--font-data);
  }
  .pal-answer + .pal-answer::before {
    content: ' · ';
  }
</style>
