<script>
  // Upgrading a device's firmware, as `tio upgrade` does, by the hub, which
  // owns the device's connection: the device, the source (the newest
  // release published for it, another release, or a file from this
  // computer), and once started the upgrade's steps as the hub tells them.
  // The same dialog upgrades every device that has an update, one after
  // another, as `tio upgrade --all` does.
  //
  // The upgrade is the hub's, not the dialog's: what the dialog shows is the
  // device's `upgrade` state (session.svelte.js), so it shows the same
  // whenever it is opened, on any page, and closing it stops nothing. The
  // hub takes an upgrade back until its upload begins. Closing the dialog on
  // an upgrade that is done clears it at the hub, as read.
  //
  // Opening the dialog on a device whose catalog the hub has not checked,
  // or did not reach, asks the hub to check it.
  import { getContext, untrack } from 'svelte';
  import * as Dialog from '$lib/components/ui/dialog/index.js';
  import * as Select from '$lib/components/ui/select/index.js';
  import { Button } from '$lib/components/ui/button/index.js';
  import RiWait from '@lucide/svelte/icons/circle';
  import RiNow from '@lucide/svelte/icons/circle-dashed';
  import RiDone from '@lucide/svelte/icons/circle-check';
  import RiFailed from '@lucide/svelte/icons/octagon-x';
  import { byRoute } from './properties.js';
  import { MAX_IMAGE, cancellable, catalogNote, defaultSource, deviceText, failureText, hardwareText, hasUpdate, installedIs, percent, releaseText, releasesOf, rowState, running, statusOf, stepsOf, versionText } from './firmware.js';
  import { bytesText } from './units.js';
  import { toast } from './toast.svelte.js';

  /** `what` is the upgrade to show: {route}, that of the device there, {all}, that of every device with an update, or null. */
  let { what = $bindable(null) } = $props();
  const { hub } = getContext('page');

  const STEP_ICONS = { wait: RiWait, now: RiNow, done: RiDone, failed: RiFailed };
  /** The sources, by value: their names. */
  const SOURCES = [['latest', 'Latest'], ['release', 'Another'], ['file', 'File']];
  const WARNING = 'Streaming from the device stops during the upgrade, and it restarts afterwards. Keep it powered and connected.';

  const open = $derived(!!what);
  const device = $derived(what?.route !== undefined ? (hub.session.devices.get(what.route) ?? null) : null);
  const upgrade = $derived(device?.upgrade ?? null);
  const releases = $derived(releasesOf(device));
  const status = $derived(statusOf(device));

  // The devices one opening for all upgrades: those that had an update as it opened.
  let batch = $state.raw([]);
  const batched = $derived(batch.map((route) => hub.session.devices.get(route)).filter(Boolean));
  /** Whether the upgrades of an opening for all were asked for. */
  let batchStarted = $state(false);

  // The choices of one opening: the source picked, or null for the device's own, the release and the file.
  let picked = $state(null);
  let releaseFile = $state(null);
  let file = $state.raw(null);
  let fileInput = $state(null);
  const source = $derived(picked ?? (device ? defaultSource(device) : 'file'));
  /** The release Another installs: the one picked, or the newest after the latest that the device does not run. */
  const release = $derived(releases.find((r) => r.file === releaseFile) ?? releases.find((r, i) => i > 0 && !installedIs(device, r)) ?? releases[0] ?? null);
  /** Whether source `s` can be picked: the catalog lists a release for it. */
  const offered = (s) => s === 'file' || (releases.length > 0 && status !== 'development');
  /** Why the file picked cannot be sent, or null. */
  const fileRefused = $derived(!file ? null : file.size === 0 ? `${file.name} is empty.` : file.size > MAX_IMAGE ? `${file.name} is ${bytesText(file.size)}, more than the ${bytesText(MAX_IMAGE)} an upgrade takes.` : null);
  /** What the upgrade installs, as its button names it, or null while there is nothing to install. */
  const target = $derived(source === 'latest' ? (releases[0] ? releaseText(releases[0]) : null) : source === 'release' ? (release ? releaseText(release) : null) : file && !fileRefused ? file.name : null);

  $effect(() => {
    if (!what) return;
    untrack(() => {
      picked = null;
      releaseFile = null;
      file = null;
      batchStarted = false;
      batch = what.all ? [...hub.session.devices.values()].sort(byRoute).filter(hasUpdate).map((d) => d.route) : [];
      const state = device?.fw?.catalog.state;
      if (device && (state === 'unchecked' || state === 'unreachable')) hub.checkFirmware();
    });
  });

  /** Closes the dialog. An upgrade under way goes on; one that is done is cleared at the hub. */
  function close() {
    for (const d of device ? [device] : batched) if (d.upgrade?.state === 'done') hub.cancelUpgrade(d.route);
    what = null;
  }

  /** Says the page has no hub to ask. */
  const unreached = () => void toast.error('Not connected to the hub');

  /** Asks the hub for the upgrade the choices make. */
  async function start() {
    const route = device.route;
    if (source === 'latest') return void (hub.upgrade(route, { kind: 'latest' }) || unreached());
    if (source === 'release') return void (hub.upgrade(route, { kind: 'release', file: release.file }) || unreached());
    const bytes = await file.arrayBuffer();
    if (!hub.upgrade(route, { kind: 'file', name: file.name, size: bytes.byteLength }, bytes)) unreached();
  }

  /** Asks the hub to upgrade each device of the opening to its newest release, which it does one at a time, in this order. */
  function startAll() {
    batchStarted = true;
    for (const d of batched) if (hasUpdate(d) && !hub.upgrade(d.route, { kind: 'latest' })) return unreached();
  }

  /** Whether any upgrade of the opening for all is under way. */
  const batchRunning = $derived(batched.some((d) => running(d.upgrade)));
</script>

<Dialog.Root {open} onOpenChange={(o) => !o && close()}>
  <Dialog.Content class="firmware-dialog sm:max-w-md">
    <Dialog.Header>
      <Dialog.Title>{what?.all ? 'Upgrade firmware of all' : 'Upgrade firmware'}</Dialog.Title>
      {#if device}<Dialog.Description>{deviceText(device)}</Dialog.Description>{/if}
    </Dialog.Header>
    {#if device}
      <div class="set-grid facts">
        <span>Route</span><span class="fact">{device.route}</span>
        <span>Serial</span><span class="fact">{device.serial || device.fw?.installed?.serial || '–'}</span>
        <span>Hardware</span><span class="fact">{hardwareText(device.fw?.installed) ?? '–'}</span>
        <span>Installed</span><span class="fact" data-installed>{device.fw?.installed ? versionText(device.fw.installed) : '…'}</span>
      </div>
      {#if !upgrade}
        <div class="set-grid">
          <span>Source</span>
          <div class="axis-row fill">
            <div class="seg" role="group" aria-label="Source">
              {#each SOURCES as [value, name] (value)}
                <button class:on={source === value} aria-pressed={source === value} disabled={!offered(value)} onclick={() => (picked = value)}>{name}</button>
              {/each}
            </div>
          </div>
          {#if source === 'latest'}
            <span>Version</span><span class="fact" data-version>{releases[0] ? releaseText(releases[0]) : '–'}</span>
          {:else if source === 'release'}
            <span>Version</span>
            <Select.Root type="single" bind:value={() => release?.file ?? '', (v) => (releaseFile = v)}>
              <Select.Trigger size="sm" class="w-full font-mono text-xs" aria-label="Version">{release ? `${releaseText(release)}${installedIs(device, release) ? ' (installed)' : ''}` : '–'}</Select.Trigger>
              <Select.Content>
                {#each releases as r (r.file)}<Select.Item value={r.file} label="{releaseText(r)}{installedIs(device, r) ? ' (installed)' : ''}" />{/each}
              </Select.Content>
            </Select.Root>
          {:else}
            <span>File</span>
            <div class="set-control">
              <Button size="xs" variant="outline" onclick={() => fileInput.click()}>Choose file…</Button>
              <span class="fact file" title={file?.name}>{file ? file.name : 'A firmware image on this computer'}</span>
              {#if file}<span class="set-value">{bytesText(file.size)}</span>{/if}
              <input bind:this={fileInput} type="file" hidden aria-label="Firmware image" onchange={(e) => (file = e.currentTarget.files[0] ?? null)} />
            </div>
          {/if}
        </div>
        {#if device.fw?.catalog.state === 'checking'}<p class="export-note">Asking the catalog of published firmware…</p>{/if}
        {#if catalogNote(device)}<p class="export-note" data-catalog-note>{catalogNote(device)}</p>{/if}
        {#if fileRefused}<p class="export-note bad" role="alert">{fileRefused}</p>{/if}
        <p class="export-note">{WARNING}</p>
      {:else}
        <div class="fw-steps" role="list" aria-label="Steps" data-target={upgrade.target}>
          {#each stepsOf(upgrade) as step (step.id)}
            {@const Icon = STEP_ICONS[step.at]}
            <div class="fw-step {step.at}" role="listitem" data-step={step.id} data-at={step.at}>
              <Icon aria-hidden="true" />
              <span>{step.name}</span>
              {#if step.id === 'uploading' && step.at === 'now'}
                <div class="bar" role="progressbar" aria-label="Uploaded" aria-valuemin="0" aria-valuemax="100" aria-valuenow={percent(upgrade)}><i style="width: {percent(upgrade)}%"></i></div>
              {:else}<span></span>{/if}
              <span class="set-value">{step.value}</span>
            </div>
          {/each}
        </div>
        {#if upgrade.state === 'queued'}
          <p class="export-note">Waiting for another device's upgrade: the hub upgrades one at a time.</p>
        {:else if upgrade.state === 'done'}
          <p class="export-note done" role="status">{upgrade.installed ? `${deviceText(device)} now reports ${versionText(upgrade.installed)}.` : `Installed ${upgrade.target}. The device has not answered since it restarted.`}</p>
        {:else if upgrade.state === 'failed'}
          <p class="export-note bad" role="alert">{failureText(upgrade)}</p>
        {:else}
          <p class="export-note">Installing {upgrade.target}. {WARNING}</p>
        {/if}
      {/if}
    {:else if what?.all}
      <div class="fw-all" role="list" aria-label="Devices to upgrade">
        {#each batched as d (d.route)}
          {@const state = rowState(d)}
          <div class="row" role="listitem" data-route={d.route}>
            <span class="route">{d.route}</span>
            <span class="name">{d.name ?? '…'}</span>
            <span class="fact">{d.fw?.installed ? versionText(d.fw.installed) : '…'}</span>
            <span class="set-value">{#if d.upgrade?.state === 'done'}Done{:else if state?.kind === 'progress'}{state.text}{#if state.percent !== null} <span class="fit" data-room="100%"><span>{state.percent}%</span></span>{/if}{:else}{state?.text ?? ''}{/if}</span>
          </div>
        {:else}
          <p class="export-note">No device has a newer release published.</p>
        {/each}
      </div>
      <p class="export-note">Each device is upgraded to the newest release published for it, one after another. {WARNING}</p>
    {:else}
      <p class="export-note">That device is gone.</p>
    {/if}
    <Dialog.Footer class="min-w-0">
      {#if device && !upgrade}
        <Button size="sm" variant="ghost" onclick={close}>Cancel</Button>
        <!-- A long file name is cut short in the button, which keeps the dialog's width. -->
        <Button size="sm" class="min-w-0 shrink" disabled={!target} title={target ?? undefined} onclick={start}><span class="truncate">{target ? `Upgrade to ${target}` : 'Upgrade'}</span></Button>
      {:else if device && running(upgrade)}
        <Button size="sm" variant="ghost" disabled={!cancellable(upgrade)} title={cancellable(upgrade) ? undefined : 'The upload has begun: it cannot be cancelled'} onclick={() => hub.cancelUpgrade(device.route)}>Cancel upgrade</Button>
        <Button size="sm" variant="outline" onclick={close}>Close</Button>
      {:else if device && upgrade.state === 'failed'}
        <Button size="sm" variant="ghost" onclick={close}>Close</Button>
        {#if upgrade.step === 'check'}
          <Button size="sm" onclick={() => hub.cancelUpgrade(device.route)}>Choose another</Button>
        {:else}
          <Button size="sm" onclick={() => hub.retryUpgrade(device.route)}>Retry</Button>
        {/if}
      {:else if what?.all && !batchStarted}
        <Button size="sm" variant="ghost" onclick={close}>Cancel</Button>
        <Button size="sm" disabled={!batched.length} onclick={startAll}>Upgrade {batched.length} {batched.length === 1 ? 'device' : 'devices'}</Button>
      {:else}
        {#if what?.all && batchRunning}<span class="export-note foot">Closing stops nothing.</span>{/if}
        <Button size="sm" variant="outline" onclick={close}>Close</Button>
      {/if}
    </Dialog.Footer>
  </Dialog.Content>
</Dialog.Root>
