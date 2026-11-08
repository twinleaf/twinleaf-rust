<script>
  // The command palette, on Ctrl+K or ⌘K: add a signal, open a tab, arrange
  // the tab, and the toolbar's actions, by typing part of a name.
  import * as Command from '$lib/components/ui/command/index.js';
  import { ARRANGEMENTS } from './constants.js';

  /** `act` holds the page's actions: addToFocused(key), newPlot(keys), followLive(), clearCursors(), exportTabs(), importTabs(). */
  let { open = $bindable(false), session, ws, act } = $props();

  /** Closes the palette, then runs `fn`. */
  function run(fn) {
    open = false;
    fn();
  }
</script>

<Command.Dialog bind:open>
  <Command.Input placeholder="Add a signal, switch tab, arrange…" />
  <Command.List>
    <Command.Empty>Nothing matches.</Command.Empty>
    <Command.Group heading="Add to the highlighted plot">
      {#each session.signals as t (t.key)}
        <Command.Item value={`add ${t.key}`} onSelect={() => run(() => act.addToFocused(t.key))}>{t.key}</Command.Item>
      {/each}
    </Command.Group>
    <Command.Group heading="New plot with">
      {#each session.signals as t (t.key)}
        <Command.Item value={`new plot ${t.key}`} onSelect={() => run(() => act.newPlot([t.key]))}>{t.key}</Command.Item>
      {/each}
    </Command.Group>
    <Command.Separator />
    <Command.Group heading="Tabs">
      {#each ws.tabs as t (t.id)}
        <Command.Item value={`tab ${t.name}`} onSelect={() => run(() => ws.selectTab(t.id))}>Go to {t.name}</Command.Item>
      {/each}
      <Command.Item onSelect={() => run(() => ws.addTab())}>New layout tab</Command.Item>
      <Command.Item onSelect={() => run(() => ws.duplicateTab(ws.tab))}>Duplicate this tab</Command.Item>
    </Command.Group>
    <Command.Group heading="Arrange">
      {#each ARRANGEMENTS as [name, label]}
        <Command.Item value={`arrange ${label}`} onSelect={() => run(() => ws.arrange(name))}>Arrange as {label}</Command.Item>
      {/each}
    </Command.Group>
    <Command.Group heading="View">
      <Command.Item onSelect={() => run(act.followLive)}>Follow live</Command.Item>
      <Command.Item onSelect={() => run(act.clearCursors)}>Clear cursors</Command.Item>
      <Command.Item onSelect={() => run(act.exportTabs)}>Export every tab</Command.Item>
      <Command.Item onSelect={() => run(act.importTabs)}>Import tabs</Command.Item>
    </Command.Group>
  </Command.List>
</Command.Dialog>
