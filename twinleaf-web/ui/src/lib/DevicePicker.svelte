<script>
  // Finds the devices this host reaches, as `tio list` does, and opens the
  // ones checked as the hub's layout. The server gives them their routes,
  // one device at the root and several at /0, /1 and on in the order of
  // their URL and subtree (`layout_of` in routes.rs), whatever order they
  // were checked in. Opening replaces the hub, so the page takes the new
  // hub's snapshot.
  import * as Dialog from '$lib/components/ui/dialog/index.js';
  import { Button } from '$lib/components/ui/button/index.js';
  import { Checkbox } from '$lib/components/ui/checkbox/index.js';
  import { Input } from '$lib/components/ui/input/index.js';
  import { ScrollArea } from '$lib/components/ui/scroll-area/index.js';
  import { untrack } from 'svelte';
  import { api } from './protocol.js';

  let { session, open = $bindable(false) } = $props();

  let scanning = $state(false);
  let found = $state([]);
  let notices = $state([]);
  // Whether a scan browses the network too, remembered in this browser.
  // Outside tools set this key before a scan, so it is stable.
  const NETWORK_KEY = 'tl-picker-network';
  let network = $state(readNetwork());
  function readNetwork() {
    try {
      return localStorage.getItem(NETWORK_KEY) !== 'false';
    } catch {
      return true;
    }
  }
  $effect(() => {
    try {
      localStorage.setItem(NETWORK_KEY, String(network));
    } catch {}
  });
  // The subtrees checked, each {url, source}, by `pick(url, source)`.
  let picked = $state(new Map());
  let typed = $state('');
  let error = $state('');
  let opening = $state(false);

  // The newest scan; one that a newer scan superseded keeps nothing.
  let scans = 0;
  async function scan() {
    const mine = ++scans;
    scanning = true;
    error = '';
    try {
      const reply = await api.discover(network);
      if (mine !== scans) return;
      found = reply.devices;
      notices = reply.notices;
    } catch (e) {
      if (mine === scans) error = e.message;
    }
    if (mine === scans) scanning = false;
  }
  // Opening the dialog scans; changing what a scan covers does not.
  $effect(() => {
    if (open) untrack(scan);
  });

  const pick = (url, source) => JSON.stringify([url, source]);
  function toggle(url, source) {
    const next = new Map(picked), k = pick(url, source);
    next.has(k) ? next.delete(k) : next.set(k, { url, source });
    picked = next;
  }
  function addTyped() {
    const url = typed.trim();
    if (!url) return;
    if (!found.some((d) => d.url === url)) found = [...found, { url, interface: 'typed', name: null, routes: [] }];
    toggle(url, '/');
    typed = '';
  }

  /** Opens `mounts` as the hub's layout, or the proxy's default with none. */
  async function mount(mounts) {
    opening = true;
    error = '';
    try {
      await api.mount(mounts);
      open = false;
    } catch (e) {
      error = e.message;
    }
    opening = false;
  }
</script>

<Dialog.Root bind:open>
  <Dialog.Content class="max-h-[calc(100dvh-2rem)] sm:max-w-xl">
    <Dialog.Header><Dialog.Title>Devices</Dialog.Title></Dialog.Header>
    <div class="picker">
      <div class="now">
        Showing: {#if session.mounts}{session.mounts.map((m) => `${m.url}${m.source !== '/' ? ' ' + m.source : ''} at ${m.prefix}`).join(', ')}{:else}the proxy's default{/if}
      </div>
      <div class="bar">
        <Button size="xs" variant="outline" disabled={scanning} onclick={scan}>{scanning ? 'Looking…' : 'Look again'}</Button>
        <label class="flex items-center gap-1.5 text-xs"><Checkbox bind:checked={network} />Network (mDNS)</label>
      </div>
      {#each notices as n}<div class="notice">{n}</div>{/each}
      <ScrollArea class="found">
        <div role="list">
          {#each found as d (d.url)}
            <label class="row" role="listitem">
              <Checkbox checked={picked.has(pick(d.url, '/'))} onCheckedChange={() => toggle(d.url, '/')} />
              <span class="iface">{d.interface}</span>
              <span class="name">{d.name ?? '…'}</span>
              <span class="url">{d.url}</span>
            </label>
            {#each d.routes.filter((r) => r.route !== '/') as sub (sub.route)}
              <label class="row sub" role="listitem">
                <Checkbox checked={picked.has(pick(d.url, sub.route))} onCheckedChange={() => toggle(d.url, sub.route)} />
                <span class="iface">{sub.route}</span>
                <span class="name">{sub.name ?? '…'}</span>
              </label>
            {/each}
          {:else}
            <div class="notice">{scanning ? 'Looking for devices…' : 'No device found. Type a URL below.'}</div>
          {/each}
        </div>
      </ScrollArea>
      <div class="bar">
        <Input class="h-7 text-xs" placeholder="tcp://host or udp://127.0.0.1:7855" bind:value={typed} onkeydown={(e) => (e.stopPropagation(), e.key === 'Enter' && addTyped())} />
        <Button size="xs" variant="outline" onclick={addTyped}>Add</Button>
      </div>
      {#if error}<div class="notice bad">{error}</div>{/if}
    </div>
    <Dialog.Footer>
      <Button size="sm" variant="ghost" disabled={opening} onclick={() => mount([])}>Use the proxy's default</Button>
      <Button size="sm" disabled={opening || !picked.size} onclick={() => mount([...picked.values()])}>Open {picked.size || ''} {picked.size === 1 ? 'device' : 'devices'}</Button>
    </Dialog.Footer>
  </Dialog.Content>
</Dialog.Root>
