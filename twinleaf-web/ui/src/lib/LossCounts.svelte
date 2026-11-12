<script>
  // What the page counts lost since it opened, or since its counts were
  // reset, in the status bar's loss popover: a row for each device that
  // lost anything, with how often and for how long its link dropped and
  // how many samples it skipped and the hub dropped, then how often the
  // page fell behind the hub, and the button that resets them all. Each
  // heading's tooltip says what it counts.
  import { Button } from '$lib/components/ui/button/index.js';
  import { countText, device as deviceName, linkCount, LOSS_COUNTS, LOSS_NO_DEVICE, LOSS_RESET, lossSince } from './words.js';

  /** `loss` is the page's Losses, and `devices` its session's devices, by route. */
  let { loss, devices } = $props();

  const rows = $derived([...loss.devices].map(([route, counts]) => ({ route, label: deviceName({ route, name: devices.get(route)?.name }), ...counts })));
</script>

<div class="loss-counts">
  <div class="loss-head">
    <span>{lossSince(loss.resets > 0)}</span>
    <Button size="xs" variant="outline" onclick={() => loss.reset()}>{LOSS_RESET}</Button>
  </div>
  <table>
    <thead>
      <tr>
        <th>Device</th>
        {#each ['link', 'skipped', 'dropped'] as kind}<th title={LOSS_COUNTS[kind].tip}>{LOSS_COUNTS[kind].head}</th>{/each}
      </tr>
    </thead>
    <tbody>
      {#each rows as row (row.route)}
        <tr data-route={row.route}>
          <td>{row.label}</td>
          <td data-count="link">{linkCount(row.drops, row.seconds)}</td>
          <td data-count="skipped">{countText(row.skipped)}</td>
          <td data-count="dropped">{countText(row.dropped)}</td>
        </tr>
      {:else}
        <tr><td colspan="4" class="none">{LOSS_NO_DEVICE}</td></tr>
      {/each}
    </tbody>
  </table>
  <div class="loss-page" title={LOSS_COUNTS.behind.tip}>
    <span>{LOSS_COUNTS.behind.head}</span>
    <span data-count="behind">{loss.behind}×</span>
  </div>
</div>
