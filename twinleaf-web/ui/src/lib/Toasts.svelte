<script>
  // The page's notices (toast.svelte.js), stacked above the status bar.
  import { dismiss, toasts } from './toast.svelte.js';
</script>

<div class="toasts" role="status" aria-live="polite">
  {#each toasts as t (t.id)}
    <div class="toast">
      <span>{t.text}</span>
      {#each t.actions as a}
        <button onclick={() => (a.run(), dismiss(t.id))}>{a.label}</button>
      {/each}
    </div>
  {/each}
</div>
