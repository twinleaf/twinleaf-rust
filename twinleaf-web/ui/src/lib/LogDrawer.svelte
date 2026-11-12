<script>
  // The log drawer, under the docks and the plots: the hub's log
  // (log.svelte.js), its newest line last.
  //
  // Its bar holds what chooses the lines shown: a toggle for each level
  // with how many lines the log holds of it, one that leaves only the lines
  // of lost data, the source, the proxy's own lines or those of one device,
  // and a filter of the words a line holds; then Follow and Copy. The
  // frame's button closes the drawer, as a dock's hides it.
  //
  // A row is a line, or several in a row that say the same, counted: the
  // time of day the hub took it, its level in the level's colour with a
  // strip of that colour at the row's edge, who said it and what it says.
  // The outage of a device's link is a divider after the line of its drop,
  // between the rows before it and those after, which says how long it
  // lasted once the link is back; it shows under any filter of levels and
  // words, with the lines of its device.
  //
  // While it follows, the newest row stays in view, and Follow is drawn as
  // the toolbar's Live is. Scrolling up stops that: Follow then is drawn
  // as the toolbar's Follow live is, counts the rows that came since, and
  // goes back to the newest; so does scrolling down to the end.
  //
  // A row's button, and Enter on the row the arrows are on (keymap.js,
  // place 'log'), puts X1 at the line's moment on every time plot of the
  // open tab (commands.js `x1At`). A line with no moment on the plots'
  // clock has no button to press.
  import { getContext, onDestroy, tick } from 'svelte';
  import { claim } from './keyrouter.js';
  import { escapeTyped } from './keys.js';
  import { LEVELS, matches } from './log.svelte.js';
  import { timeAxis } from './plot.js';
  import { byRoute } from './properties.js';
  import { toast } from './toast.svelte.js';
  import * as words from './words.js';
  import Tip from './Tip.svelte';
  import IconButton from './IconButton.svelte';
  import { Button } from '$lib/components/ui/button/index.js';
  import { Input } from '$lib/components/ui/input/index.js';
  import { ScrollArea } from '$lib/components/ui/scroll-area/index.js';
  import * as Select from '$lib/components/ui/select/index.js';
  import RiFollow from '@lucide/svelte/icons/arrow-down-to-line';
  import RiCopy from '@lucide/svelte/icons/clipboard-copy';

  /** `log` is the page's Log, and `session` the hub's, whose devices name the lines' sources. */
  let { log, session } = $props();
  const commands = getContext('commands');
  const hide = $derived(commands.row('showLog'));
  /** How a row's button reads: the command's label and picture. */
  const show = commands.row('x1At');

  /** The rows PageUp and PageDown move by. */
  const PAGE = 10;
  /** How near its end the list counts as scrolled to it, in px. */
  const END_SLOP = 4;
  /** The Select's value for every source, which the Log names ''. */
  const ALL = 'all';

  /** How the page names the device at `route`. */
  const named = (route) => words.device({ route, name: session.devices.get(route)?.name });
  const clockText = timeAxis('unix');
  const timeOf = (row) => clockText.readout(row.wall);
  const sourceOf = (row) => words.logSource(row, named);
  const messageOf = (row) => words.logMessage(row, named);
  /** What a row says, which the filter's words are looked for in. */
  const said = (row) => `${words.LEVEL_NAMES[row.level]} ${sourceOf(row)} ${messageOf(row)}`;

  const sources = $derived([
    [ALL, words.LOG_ALL],
    ['proxy', words.LOG_PROXY],
    ...[...new Set([...session.devices.keys(), ...(log.source && log.source !== 'proxy' ? [log.source] : [])])].sort((a, b) => byRoute({ route: a }, { route: b })).map((route) => [route, named(route)]),
  ]);
  const sourceName = $derived(sources.find(([value]) => value === (log.source || ALL))?.[1] ?? words.LOG_ALL);

  /** Whether row `row`, the drop of a link, has its outage's divider under the source chosen. */
  const cutShows = (row) => row.loss?.cause === 'link' && row.loss.seconds == null && (!log.source || log.source === 'proxy' || row.route === log.source);
  /** What the list draws, in order, each {row, line, cut}: whether the row itself shows, and whether its outage's divider does. */
  const items = $derived.by(() => {
    const filter = { levels: log.levels, lossOnly: log.lossOnly, source: log.source, query: log.query };
    return log.rows.map((row) => ({ row, line: matches(row, filter, said), cut: cutShows(row) })).filter((i) => i.line || i.cut);
  });
  const lines = $derived(items.filter((i) => i.line).map((i) => i.row));

  // Following: the list stays scrolled to its end as rows come.
  let viewport = $state(null);
  let following = $state(true);
  /** The number of the newest line as following stopped. */
  let stoppedAt = $state(0);
  const fresh = $derived(following ? 0 : lines.filter((row) => row.n > stoppedAt).length);
  /** Where the list was scrolled to as it was last heard. */
  let lastTop = 0;
  const toEnd = () => viewport && (viewport.scrollTop = lastTop = viewport.scrollHeight);
  $effect(() => {
    void items;
    if (following) toEnd();
  });
  $effect(() => {
    const el = viewport;
    if (!el) return;
    /** A scroll up stops the following; one to the end starts it again. */
    const scrolled = () => {
      const atEnd = el.scrollHeight - el.scrollTop - el.clientHeight <= END_SLOP;
      if (atEnd) following = true;
      else if (following && el.scrollTop < lastTop - 1) stop();
      lastTop = el.scrollTop;
    };
    el.addEventListener('scroll', scrolled, { passive: true });
    // The drawer resized keeps the newest row in view while it follows.
    const resized = new ResizeObserver(() => following && toEnd());
    resized.observe(el);
    return () => (el.removeEventListener('scroll', scrolled), resized.disconnect());
  });
  function stop() {
    stoppedAt = log.rows.at(-1)?.n ?? 0;
    following = false;
  }
  /** Follow's press: back to the newest row, or, while it follows, no further. */
  function toggleFollow() {
    if (following) return stop();
    following = true;
    toEnd();
  }

  /** The row the arrows are on, by id, or null. */
  let at = $state(null);
  const current = $derived(lines.find((row) => row.id === at) ?? null);
  let list = $state(null);
  /** Puts X1 at `row`'s moment on the tab's time plots, or says why it cannot. */
  function showOnPlots(row) {
    if (row.time === null) return void toast.warning(words.NO_MOMENT);
    if (!commands.run('x1At', { from: 'button', how: { t: row.time } })) toast.warning(words.NO_TIME_PLOT);
  }
  // The list's keys (keymap.js, place 'log'), pressed anywhere in it.
  onDestroy(
    claim('log', {
      when: (e, on) => !!list?.contains(on),
      on: {
        move(e) {
          const n = lines.length, i = lines.indexOf(current);
          if (!n) return;
          // From no row, a move starts at the newest; Home at the oldest.
          const steps = { ArrowDown: 1, ArrowUp: -1, PageDown: PAGE, PageUp: -PAGE };
          const to = e.key === 'Home' ? 0 : e.key === 'End' || i < 0 ? n - 1 : i + steps[e.key];
          at = lines[Math.max(0, Math.min(n - 1, to))].id;
          tick().then(() => document.getElementById(`log-row-${at}`)?.scrollIntoView({ block: 'nearest' }));
        },
        show: () => (current ? void showOnPlots(current) : false),
      },
    }),
  );

  /** Escape in the filter clears what is typed, before it does anything else. */
  const filterKey = (e) => escapeTyped(e, log.query, '', () => log.setQuery(''));

  /** Copies the rows shown, a line each as the list writes it, tabs between its parts. */
  function copy() {
    const text = items.flatMap(({ row, line, cut }) => [
      ...(line ? [[timeOf(row), words.LEVEL_NAMES[row.level].toUpperCase(), sourceOf(row), messageOf(row), ...(row.count > 1 ? [`×${row.count}`] : [])].join('\t')] : []),
      ...(cut ? [`— ${words.outageText(named(row.route), row.back)} —`] : []),
    ]);
    navigator.clipboard.writeText(`${text.join('\n')}\n`).then(
      () => toast.success(words.copiedLines(text.length)),
      (error) => toast.error(`Could not copy: ${error.message}`),
    );
  }
</script>

<!-- The frame a dock has, with its button; the bar is a dock's top row. -->
<aside class="dock-frame log-frame flex min-h-0 min-w-0 flex-1 flex-col overflow-clip rounded-lg border bg-card" data-focus="log" tabindex="-1" aria-label="Log">
  <div class="dock-top log-bar">
    <span class="log-title">Log</span>
    <div class="seg" role="group" aria-label="Levels shown">
      {#each LEVELS as level (level)}
        <button class:on={log.levels[level]} aria-pressed={log.levels[level]} data-level={level} onclick={() => log.toggleLevel(level)}>
          <i style:--c="var(--log-{level})"></i>{words.LEVEL_NAMES[level]}<span class="num">{words.countText(log.counts[level])}</span>
        </button>
      {/each}
    </div>
    <div class="seg">
      <button class:on={log.lossOnly} aria-pressed={log.lossOnly} data-filter="loss" onclick={() => log.onlyLoss(!log.lossOnly)}>
        {words.LOG_LOSS}<span class="num">{words.countText(log.counts.loss)}</span>
      </button>
    </div>
    <Select.Root type="single" value={log.source || ALL} onValueChange={(value) => log.setSource(value === ALL ? '' : value)}>
      <Select.Trigger size="sm" class="log-source text-xs" aria-label="Source">{sourceName}</Select.Trigger>
      <Select.Content>
        {#each sources as [value, label] (value)}<Select.Item {value} {label} />{/each}
      </Select.Content>
    </Select.Root>
    <Input class="log-filter h-6 flex-1 text-xs" type="search" placeholder="Filter" aria-label="Filter the lines by the words they hold" bind:value={() => log.query, (value) => log.setQuery(value)} onkeydown={filterKey} />
    <Button size="xs" variant={following ? 'outline' : 'default'} class="log-follow" aria-pressed={following} onclick={toggleFollow}>
      <RiFollow />{words.FOLLOW}<span class="num" style="--digits: 3">{fresh ? words.countText(fresh) : ''}</span>
    </Button>
    <IconButton icon={RiCopy} label={words.COPY_LINES} class="log-copy" onclick={copy} />
  </div>
  <ScrollArea class="log-rows" type="auto" bind:viewportRef={viewport}>
    <!-- svelte-ignore a11y_click_events_have_key_events -->
    <div class="log-list" role="listbox" tabindex="0" data-focus-own aria-label="Log lines: arrows move, Enter shows the line on the plots" aria-activedescendant={current ? `log-row-${current.id}` : undefined} bind:this={list}>
      {#each items as { row, line, cut } (row.id)}
        {#if line}
          <div class="log-row" id="log-row-{row.id}" role="option" tabindex="-1" data-level={row.level} data-loss={row.loss ? row.loss.cause : undefined} aria-selected={row.id === at} onclick={() => ((at = row.id), list?.focus())}>
            <span class="t">{timeOf(row)}</span>
            <span class="lv">{words.LEVEL_NAMES[row.level].toUpperCase()}</span>
            <span class="src" title={sourceOf(row)}>{sourceOf(row)}</span>
            <span class="msg" title={messageOf(row)}>{messageOf(row)}</span>
            <span class="n">{row.count > 1 ? `×${words.countText(row.count)}` : ''}</span>
            <button class="go" tabindex="-1" data-command="x1At" disabled={row.time === null} aria-label={show.label} title={row.time === null ? words.NO_MOMENT : show.label} onclick={(e) => (e.stopPropagation(), showOnPlots(row))}><show.icon /></button>
          </div>
        {/if}
        {#if cut}<div class="log-cut" role="separator" data-route={row.route}><span>{words.outageText(named(row.route), row.back)}</span></div>{/if}
      {:else}
        <div class="props-empty">{words.logEmpty(log.lossOnly, log.rows.length > 0)}</div>
      {/each}
    </div>
  </ScrollArea>
  <!-- The frame's button, in its corner: the last of the drawer's stops, after its bar and its rows. -->
  <Tip text={hide.label} keys={hide.show ?? []}>
    {#snippet children(tip)}
      <button {...tip} class="dock-button" aria-label={hide.label} data-command="showLog" onclick={() => commands.run('showLog', { from: 'button' })}><hide.icon /></button>
    {/snippet}
  </Tip>
</aside>
