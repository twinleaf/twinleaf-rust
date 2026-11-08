<script>
  // What a drag shows over the page: the compass's drop cells, the
  // selection box, and the ghost of a plot being moved, beside the pointer,
  // below and to the right so the cells stay in view.
  import { colorOf } from './plot.js';

  let { drags, ws, session } = $props();
</script>

{#if drags.compass}
  <div class="compass" aria-hidden="true">
    {#each drags.compass as c}
      <div class="compass-cell {c.edge}" class:outer={c.outer} class:active={c.active} style="left: {c.x}px; top: {c.y}px"></div>
      {#if c.active}<div class="compass-label" style="left: {c.lx}px; top: {c.ly}px">{c.label}</div>{/if}
    {/each}
  </div>
{/if}

{#if drags.marquee}
  {@const m = drags.marquee}
  <div class="marquee" style="left: {m.x}px; top: {m.y}px; width: {m.w}px; height: {m.h}px"></div>
{/if}

{#if drags.moving}
  {@const moving = drags.moving}
  {@const p = ws.plots[moving.plotId]}
  <div class="move-ghost" style="left: {moving.x + 18}px; top: {moving.y + 18}px">
    <div class="ghost-head">{moving.group.size > 1 ? `${moving.group.size} plots` : p?.kind === 'spectrum' ? 'Spectrum' : 'Time'}</div>
    <div class="ghost-body">
      {#each p?.items ?? [] as item (item.id)}
        {@const t = session.trace(item.key)}
        <span style="--c: {t ? colorOf(t) : 'var(--muted-foreground)'}"><i></i>{item.key}</span>
      {:else}
        <span>empty</span>
      {/each}
    </div>
    {#if moving.target}
      <div class="ghost-action">
        {#if moving.target.tabId}Move to “{ws.tabs.find((t) => t.id === moving.target.tabId)?.name}”{:else}{moving.target.label}{/if}
      </div>
    {/if}
  </div>
{/if}
