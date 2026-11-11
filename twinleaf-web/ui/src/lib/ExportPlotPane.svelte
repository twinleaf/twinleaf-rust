<script>
  // The export inspector's Plot tab: what belongs to the one plot picked, a
  // paused copy of it (PlotHost `copy`). Its rows are the plot's own
  // settings and commands, run on the copy, so the figure shows them as the
  // plot would and the page's plot is left as it was: the layout, the
  // legend and where it sits, and one list of the signals, each with its
  // colour, its name in the legend and its unit, which shows its y axis
  // below the list: the axis's range, and its label, digits and notation,
  // the same settings as the axis's own popover. A stacked plot's signals
  // drag to reorder its lanes and its legend. The title is the image's.
  import { setContext } from 'svelte';
  import * as Popover from '$lib/components/ui/popover/index.js';
  import ColorPicker from './ColorPicker.svelte';
  import { colorOf } from './plot.js';
  import { MODES } from './constants.js';
  import { AXIS_SETTINGS } from './commands.js';
  import { axisFormat } from './units.js';

  /** `entry` is the plot exported: {copy, cid, title, yRanges}, its copy's host and plot id, its image's title, and each y axis's range as picked here. */
  let { entry = $bindable() } = $props();
  // svelte-ignore state_referenced_locally
  const { copy, cid } = entry;
  // The colour pickers and the commands here are the copy's.
  setContext('colors', copy.colors);
  setContext('commands', copy.commands);
  const plot = $derived(copy.plot(cid));
  const session = $derived(copy.hub.session);
  const stacked = $derived(plot.mode === 'stacked');
  /** The signal whose axis shows below the list. */
  let picked = $state(null);
  const pickedKey = $derived(plot.items.some((i) => i.key === picked) ? picked : (plot.items[0]?.key ?? null));
  /** The key of the y axis of signal `key`: an overlay's one shared axis, else the signal's own (plot.js `rangeKey`). */
  const axisOf = (key) => (plot.mode === 'overlay' ? 'shared' : key);
  const axis = $derived(pickedKey && axisOf(pickedKey));
  const axisSignals = $derived(plot.items.filter((i) => axisOf(i.key) === axis));
  const unitsOf = (key) => session.trace(key)?.meta.units ?? '';
  const set = $derived(axisFormat(plot.yFormat?.[axis]));
  /** Each value of axis setting `field` as a <select> holds it, and back. */
  const optionOf = (v) => (v === null ? 'auto' : String(v));
  const valueOf = (field, s) => (field === 'digits' ? (s === 'auto' ? null : Number(s)) : s);
  /** Sets axis setting `field` to `to` on the copy: the axis popover's command. */
  const setAxis = (field, to) => copy.commands.run({ label: 'yLabel', digits: 'yDigits', notation: 'yNotation' }[field], { on: cid, how: { key: axis, to } });

  /** The range picked for the axis: 'plot', as the plot had it, 'fit' or 'typed', with its [lo, hi]. */
  const range = $derived(entry.yRanges[axis] ?? { how: 'plot' });
  /** Sets the axis's range: as the plot had it, fitted to what it shows, or typed. */
  function setRange(how, lo, hi) {
    const pv = copy.pv(cid);
    if (!pv || !axis) return;
    const was = entry.yRanges[axis]?.was ?? pv.yBounds(axis);
    if (how === 'fit') pv.fit([axis]);
    else if (how === 'typed') {
      const [a, b] = [lo ?? pv.yBounds(axis)?.[0] ?? -1, hi ?? pv.yBounds(axis)?.[1] ?? 1];
      if (!(b > a)) return;
      pv.setYBounds([axis], a, b);
    } else if (was) pv.setYBounds([axis], ...was);
    const now = pv.yBounds(axis);
    entry.yRanges = { ...entry.yRanges, [axis]: { how, was, lo: now?.[0], hi: now?.[1] } };
  }
  const shown = (v) => (v === undefined || v === null ? '' : String(+v.toPrecision(6)));

  /** The legend's nine places, as [x, y, name]. */
  const PLACES = [0, 0.5, 1].flatMap((y) => [0, 0.5, 1].map((x) => [x, y, `${['Top', 'Middle', 'Bottom'][y * 2]} ${['left', 'centre', 'right'][x * 2]}`]));
  const legendAt = $derived(plot.legendAt ?? { x: 1, y: 0 });

  /** Renames signal `key` in the legend: `name`, or its key with none. */
  function rename(key, name) {
    const label = name.trim() && name.trim() !== key ? name.trim() : undefined;
    copy.edit(cid, 'Rename a signal', { items: plot.items.map((i) => (i.key === key ? { ...i, label } : i)) });
  }

  // A stacked plot's signals drag to reorder: the row dragged lands where
  // the line shows, before the row it is over when it came from below that
  // row, after it when from above.
  let dragged = $state(null), over = $state(null);
  /** The signal whose colour picker is open, or null. */
  let colorKey = $state(null);
  function drop(to) {
    if (dragged === null || dragged === to) return;
    const items = [...plot.items];
    items.splice(to, 0, ...items.splice(dragged, 1));
    copy.edit(cid, 'Reorder signals', { items });
  }
</script>

<div class="sec">
  <h3>Layout</h3>
  <span class="seg" role="group" aria-label="Layout">
    {#each MODES as [mode, name] (mode)}
      <button aria-pressed={plot.mode === mode} data-command="setLayout" onclick={() => copy.commands.run('setLayout', { on: cid, how: { to: mode } })}>{name.replace(' Y', '')}</button>
    {/each}
  </span>
</div>
<div class="sec">
  <h3>Legend</h3>
  <div class="r">
    <span class="chips"><button aria-pressed={!!plot.legend} data-command="legend" onclick={() => copy.commands.run('legend', { on: cid })}>Shown</button></span>
    <span class="c">
      {#if plot.legend}
        <span class="nine" role="group" aria-label="Legend place">
          {#each PLACES as [x, y, name] (name)}
            <button aria-pressed={legendAt.x === x && legendAt.y === y} aria-label={name} onclick={() => copy.edit(cid, 'Move the legend', { legendAt: { x, y } })}></button>
          {/each}
        </span>
      {/if}
    </span>
  </div>
</div>
<div class="sec">
  <h3>Title</h3>
  <input type="text" bind:value={entry.title} placeholder="None" aria-label="Title" />
</div>
<div class="sec">
  <h3>Signals</h3>
  {#each plot.items as item, i (item.id)}
    <div
      class="r sig"
      class:on={item.key === pickedKey}
      class:land={over === i && dragged !== null && dragged !== i}
      class:after={over === i && dragged !== null && i > dragged}
      role="listitem"
      draggable={stacked ? 'true' : undefined}
      data-key={item.key}
      ondragstart={(e) => stacked && ((dragged = i), e.dataTransfer?.setData('text/plain', item.key))}
      ondragover={(e) => dragged !== null && (e.preventDefault(), (over = i))}
      ondrop={(e) => (e.preventDefault(), drop(i), (dragged = over = null))}
      ondragend={() => (dragged = over = null)}
    >
      {#if stacked}<span class="grip" aria-hidden="true">⋮⋮</span>{:else}<span></span>{/if}
      <Popover.Root open={colorKey === item.key} onOpenChange={(o) => (colorKey = o ? item.key : null)}>
        <Popover.Trigger>
          {#snippet child({ props })}<button {...props} class="colour" aria-label="Colour of {item.key}" style="--c: {colorOf({ key: item.key }, copy.colors)}"><i></i></button>{/snippet}
        </Popover.Trigger>
        <Popover.Content class="w-auto gap-0 p-1.5" align="start">
          {#if colorKey === item.key}<ColorPicker key={item.key} ondone={() => (colorKey = null)} />{/if}
        </Popover.Content>
      </Popover.Root>
      <input type="text" value={item.label || item.key} aria-label="Name of {item.key}" onchange={(e) => rename(item.key, e.currentTarget.value)} />
      <button class="axis" aria-pressed={item.key === pickedKey} aria-label="Axis of {item.key}" onclick={() => (picked = item.key)}>{unitsOf(item.key) || '—'}</button>
    </div>
  {/each}
  {#if axis}
    <div class="axis-of">{axisSignals.length > 1 ? `Axis of ${axisSignals.length} signals` : 'Axis'}{unitsOf(pickedKey) ? ` · ${unitsOf(pickedKey)}` : ''}</div>
    <div class="r">
      <span>Range</span>
      <select aria-label="Range" value={range.how} onchange={(e) => setRange(e.currentTarget.value)}>
        <option value="plot">As on the plot</option>
        <option value="fit">Fitted</option>
        <option value="typed">Typed</option>
      </select>
    </div>
    {#if range.how === 'typed'}
      <div class="r">
        <span>From, to</span>
        <span class="pairin">
          <input type="number" step="any" aria-label="From" value={shown(range.lo)} onchange={(e) => setRange('typed', Number(e.currentTarget.value), range.hi)} />
          <input type="number" step="any" aria-label="To" value={shown(range.hi)} onchange={(e) => setRange('typed', range.lo, Number(e.currentTarget.value))} />
        </span>
      </div>
    {/if}
    {#each ['label', 'digits', 'notation'] as field (field)}
      <div class="r">
        <span>{AXIS_SETTINGS[field].what}</span>
        <select aria-label="Axis {field}" value={optionOf(set[field])} onchange={(e) => setAxis(field, valueOf(field, e.currentTarget.value))}>
          {#each AXIS_SETTINGS[field].names as [v, name] (v)}<option value={optionOf(v)}>{name}</option>{/each}
        </select>
      </div>
    {/each}
  {/if}
</div>
