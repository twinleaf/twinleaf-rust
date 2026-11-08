<script>
  // Every key, by where focus is, as keymap.js lists them.
  import * as Dialog from '$lib/components/ui/dialog/index.js';
  import { KEYMAP } from './keymap.js';

  let { open = $bindable(false) } = $props();
</script>

<Dialog.Root bind:open>
  <Dialog.Content class="max-h-[calc(100dvh-2rem)] overflow-y-auto sm:max-w-4xl">
    <Dialog.Header><Dialog.Title>Keys</Dialog.Title></Dialog.Header>
    <div class="keys">
      {#each KEYMAP as place (place.place)}
        <section>
          <h3>{place.title}</h3>
          {#each place.entries as entry}
            <div class="row"><span>{#each entry.show as k}<kbd>{k}</kbd>{/each}</span><span>{entry.what}</span></div>
          {/each}
        </section>
      {/each}
    </div>
  </Dialog.Content>
</Dialog.Root>
