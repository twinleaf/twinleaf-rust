// What the Workspace test reads: effects over the open tab's layout and two
// of its plots, each through a derived as a tile reads its plot, counting
// their runs, and a change to what one plot's view shows.
import { flushSync } from 'svelte';
import { Workspace } from '../src/lib/workspace.svelte.js';
import { makeTab } from '../src/lib/tabs.js';
import { plotIds } from '../src/lib/layout.js';

export function probe() {
  const t = makeTab('A');
  const ws = new Workspace({ tabs: [t], activeId: t.id });
  const [a, b] = plotIds(ws.layout);
  const runs = { layout: 0, other: 0, changed: 0 };
  const dispose = $effect.root(() => {
    const other = $derived(ws.plots[b]);
    const changed = $derived(ws.plots[a]);
    $effect(() => {
      ws.layout;
      runs.layout++;
    });
    $effect(() => {
      other;
      runs.other++;
    });
    $effect(() => {
      changed;
      runs.changed++;
    });
  });
  flushSync();
  return { runs, dispose, change: () => ws.setView(a, { fRange: [1, 2] }) };
}
