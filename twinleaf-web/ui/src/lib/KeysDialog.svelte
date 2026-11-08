<script>
  // Every key and gesture, a section per place, as keymap.js's sheet lists
  // them: the keys as caps, where a gesture is made beside them, then what
  // it does.
  import * as Dialog from '$lib/components/ui/dialog/index.js';
  import { sheet } from './keymap.js';

  let { open = $bindable(false) } = $props();

  const sections = sheet();
</script>

<Dialog.Root bind:open>
  <Dialog.Content class="max-h-[calc(100dvh-2rem)] overflow-y-auto sm:max-w-4xl">
    <Dialog.Header><Dialog.Title>Keys</Dialog.Title></Dialog.Header>
    <div class="keys">
      {#each sections as section (section.title)}
        <section>
          <h3>{section.title}</h3>
          <dl>
            {#each section.rows as row (row.id)}
              <dt>{#each row.show as k}<kbd>{k}</kbd>{/each}{#if row.on}<span class="on">{row.on}</span>{/if}</dt>
              <dd>{row.what}</dd>
            {/each}
          </dl>
        </section>
      {/each}
    </div>
  </Dialog.Content>
</Dialog.Root>
