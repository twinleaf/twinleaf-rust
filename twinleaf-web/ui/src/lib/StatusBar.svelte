<script>
  // The page's status along its foot: whether it reaches the hub, its
  // devices and how many are lost, what streams in, the raw history the hub
  // holds, what is missing of the data, and the frame rate. The log, and
  // the keys and mouse gestures, are a click away.
  //
  // The Log item, beside Keys, opens the log drawer and closes it
  // (commands.js `showLog`), and counts the warnings and errors that
  // arrived unseen (log.svelte.js), in the error's colour while one of
  // them is an error.
  //
  // The loss mark is there only while something is missing. While a
  // device's link is down, it says so and for how long, in the error's
  // colour: of the device longest down, counted from when the page heard
  // it lost. Else, while the last minute lost anything, it reads the share
  // of that minute's data that is missing and the cause that stands for
  // the most of it (loss.svelte.js `missing`, `recent`), in the warning's
  // colour. Its tooltip is one line of what the page counts lost since it
  // opened, and a press opens the log on the lines of lost data. It comes
  // after History, the last item before the bar's free room, and each
  // item of the bar has the room of the longest it says, so nothing moves
  // as the mark comes, goes or counts.
  import { getContext } from 'svelte';
  import RiKeys from '@lucide/svelte/icons/keyboard';
  import RiWarn from '@lucide/svelte/icons/triangle-alert';
  import RiDown from '@lucide/svelte/icons/unplug';
  import Tip from './Tip.svelte';
  import HistorySettings from './HistorySettings.svelte';
  import { LINK_DOWN, LOSS_CAUSE, device as deviceName, lossTip, shareText } from './words.js';
  import * as Popover from '$lib/components/ui/popover/index.js';
  import { bytesText as bytes, spanText as span } from './units.js';

  /** `hub` is the page's Hub, `hud` the frame stats, and `onKeys` opens the keys. */
  let { hub, hud, onKeys } = $props();

  const commands = getContext('commands');
  const session = $derived(hub.session);
  const devices = $derived([...session.devices.values()]);
  const lost = $derived(devices.filter((d) => d.presence === 'lost'));
  const memory = $derived(session.memory);
  const full = $derived(memory?.budget ? Math.min(1, memory.bytes / memory.budget) : 0);
  const loss = $derived(hub.loss);
  const log = $derived(hub.log);
  /** The longest a span of time reads as (units.js `spanText`), as the mark's room has it. */
  const LONGEST_SPAN = '119 min';
  /** Everything of a share the mark may say at its longest, a line each: all of the data, by each cause. */
  const SHARE_ROOM = Object.values(LOSS_CAUSE).map((cause) => `100% missing · ${cause}`).join('\n');
  const plural = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`;
  /** The most unseen lines the Log item counts out. */
  const MOST_UNSEEN = 999;
  const unseen = $derived(log.unseen.error + log.unseen.warn);
  const logRow = $derived(commands.row('showLog'));
  /** The device whose link has been down longest, or undefined while none is. */
  const down = $derived(lost.toSorted((a, b) => a.lostAt - b.lostAt)[0]);
  /** How long its link has been down, in seconds, as of the page's last tick. */
  const downFor = $derived(down ? Math.max(0, (loss.now - down.lostAt) / 1000) : 0);
</script>

<!-- What an item says is set from its start in the room of the longest it
     says, `room` (plot.css, .fit): the words close up to their numbers, the
     room left over comes after them, and what follows the item stays put. -->

<footer class="status-bar" data-focus="status" tabindex="-1">
  <span class="item" title={hub.connected ? 'The page reaches the hub' : 'The page lost the hub and tries again each second'}>
    <i class="dot" class:on={hub.connected}></i>{hub.connected ? 'Connected' : 'Reconnecting…'}
  </span>
  <span class="item">
    <span class="fit" data-room="00 devices"><span>{plural(devices.length, 'device')}</span></span>
    {#if lost.length}
      <span class="warn" title="Lost: {lost.map((d) => d.route).join(', ')}"><RiWarn /><span class="fit" data-room="00 lost"><span>{lost.length} lost</span></span></span>
    {/if}
  </span>
  <span class="item"><span class="fit" data-room="000 signals · 0000 kB/s"><span>{plural(session.signals.length, 'signal')} · <span class="num">{hud.rxKBs.toFixed(0)}</span> kB/s</span></span></span>
  {#if memory}
    <Popover.Root>
      <Tip text="Raw history: {bytes(memory.bytes)} of {bytes(memory.budget)}, at most {span(memory.maxAge)} of each stream">
        {#snippet children(tip)}
          <Popover.Trigger {...tip}>
            {#snippet child({ props })}
              <button {...props} class="item history">
                History {span(memory.seconds)}
                <span class="meter" aria-hidden="true"><span style="width: {full * 100}%"></span></span>
                {bytes(memory.bytes)}
              </button>
            {/snippet}
          </Popover.Trigger>
        {/snippet}
      </Tip>
      <Popover.Content class="w-72 p-3" align="start" side="top">
        <HistorySettings {memory} />
      </Popover.Content>
    </Popover.Root>
  {/if}
  {#if down || loss.missing !== null}
    <Tip text={lossTip(down ? deviceName(down) : null, loss.totals, loss.since)}>
      {#snippet children(tip)}
        <button {...tip} class="item loss" class:down={!!down} data-state={down ? 'down' : 'missing'} onclick={() => log.show(true)}>
          {#if down}
            <RiDown /><span class="fit" data-room="{LINK_DOWN} {LONGEST_SPAN}"><span>{LINK_DOWN} {span(downFor)}</span></span>
          {:else}
            <RiWarn /><span class="fit" data-room={SHARE_ROOM}><span>{shareText(loss.missing)} missing · {LOSS_CAUSE[loss.recent[0].kind]}</span></span>
          {/if}
        </button>
      {/snippet}
    </Tip>
  {/if}
  <span class="spacer"></span>
  <span class="item" title="GPU: {hud.gpu || 'unknown'}"><span class="fit" data-room="000 fps"><span>{hud.fps.toFixed(0)} fps</span></span></span>
  <Tip text={logRow.label} keys={logRow.show ?? []}>
    {#snippet children(tip)}
      <button {...tip} class="item log" class:on={log.open} aria-pressed={log.open} data-command="showLog" onclick={() => commands.run('showLog', { from: 'button' })}>
        <logRow.icon /><span class="fit" data-room="Log {MOST_UNSEEN}+"><span>Log <span class="num" class:error={log.unseen.error > 0} aria-label="{unseen} unseen warnings and errors">{unseen > MOST_UNSEEN ? `${MOST_UNSEEN}+` : unseen || ''}</span></span></span>
      </button>
    {/snippet}
  </Tip>
  <Tip text="Every key and mouse gesture">
    {#snippet children(tip)}<button {...tip} class="item keys" onclick={onKeys}><RiKeys />Keys<kbd data-hint>?</kbd></button>{/snippet}
  </Tip>
</footer>
