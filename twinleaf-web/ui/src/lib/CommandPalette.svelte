<script>
  // The command palette, on Ctrl+K or ⌘K: add a signal, open a tab, arrange
  // the tab, the plot commands that have plots to act on now (commands.js),
  // and the toolbar's actions, by typing part of a name. A signal named
  // here goes onto the selected plots, or into a new one, alone, by the
  // commands the Signals list runs.
  import { getContext } from 'svelte';
  import * as Command from '$lib/components/ui/command/index.js';
  import { ARRANGEMENTS } from './constants.js';

  /** `act` holds the page's actions: toggleDock(side), dockShown(side), exportTabs(), importTabs(). */
  let { open = $bindable(false), session, ws, act } = $props();
  const history = getContext('history');
  const commands = getContext('commands');

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
    <Command.Group heading="Add to the selected plots">
      {#each session.signals as t (t.key)}
        <Command.Item value={`add ${t.key}`} data-command="addToPlots" onSelect={() => run(() => commands.run('addToPlots', { signals: [t.key], from: 'palette' }))}>{t.key}</Command.Item>
      {/each}
    </Command.Group>
    <Command.Group heading="New plot with">
      {#each session.signals as t (t.key)}
        <Command.Item value={`new plot ${t.key}`} data-command="newPlot" onSelect={() => run(() => commands.run('newPlot', { signals: [t.key], from: 'palette' }))}>{t.key}</Command.Item>
      {/each}
    </Command.Group>
    <Command.Separator />
    <Command.Group heading="Tabs">
      {#each ws.tabs as t (t.id)}
        <Command.Item value={`tab ${t.name}`} onSelect={() => run(() => ws.selectTab(t.id))}>Go to {t.name}</Command.Item>
      {/each}
      <Command.Item onSelect={() => run(() => history.group('New tab', () => ws.addTab()))}>New layout tab</Command.Item>
      <Command.Item onSelect={() => run(() => history.group('Duplicate tab', () => ws.duplicateTab(ws.tab)))}>Duplicate this tab</Command.Item>
    </Command.Group>
    <Command.Group heading="Docks">
      {#each ['left', 'right'] as side}
        <Command.Item value={`dock ${side}`} onSelect={() => run(() => act.toggleDock(side))}>{act.dockShown(side) ? 'Hide' : 'Show'} the {side} dock</Command.Item>
      {/each}
    </Command.Group>
    <Command.Group heading="Arrange">
      {#each ARRANGEMENTS as [name, label]}
        <Command.Item value={`arrange ${label}`} onSelect={() => run(() => history.group(`Arrange as ${label}`, () => ws.arrange(name)))}>Arrange as {label}</Command.Item>
      {/each}
    </Command.Group>
    <Command.Group heading="Plots">
      {#each commands.offered() as c (c.id)}
        <Command.Item value={`command ${c.id} ${c.label}`} onSelect={() => run(() => commands.run(c.id, { from: 'palette' }))}>{c.label}</Command.Item>
      {/each}
    </Command.Group>
    <Command.Group heading="View">
      <Command.Item onSelect={() => run(act.exportTabs)}>Export every tab</Command.Item>
      <Command.Item onSelect={() => run(act.importTabs)}>Import tabs</Command.Item>
    </Command.Group>
  </Command.List>
</Command.Dialog>
