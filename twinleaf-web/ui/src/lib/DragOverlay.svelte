<script>
  // What a drag shows over the page: the compass's drop cells, where a
  // moved plot would land, the selection box, and the ghost of the plots
  // being moved, beside the pointer, below and to the right so the cells
  // stay in view. Several plots moved together show as a stack of cards,
  // one row apiece.
  import { colorOf } from './plot.js';

  let { drags, ws, session } = $props();
</script>

{#if drags.preview}
  {@const r = drags.preview}
  <div class="drop-preview" class:outer={r.outer} aria-hidden="true" style="left: {r.x}px; top: {r.y}px; width: {r.w}px; height: {r.h}px">
    {#if r.label}<span class="drop-label">{r.label}</span>{/if}
  </div>
{/if}

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
  {@const kind = (q) => (q?.kind === 'spectrum' ? 'Spectrum' : 'Time')}
  {@const swatch = (key) => { const t = session.trace(key); return t ? colorOf(t) : 'var(--muted-foreground)'; }}
  <div class="move-ghost" class:stack={moving.group.size > 1} style="left: {moving.x + 18}px; top: {moving.y + 18}px">
    {#if moving.group.size > 1}
      <div class="ghost-head">{moving.group.size} plots</div>
      <div class="ghost-body">
        {#each [...moving.group] as id (id)}
          {@const q = ws.plots[id]}
          <span class="ghost-plot"><b>{kind(q)}</b>{#each q?.items ?? [] as item (item.id)}<i style="--c: {swatch(item.key)}"></i>{:else} empty{/each}</span>
        {/each}
      </div>
    {:else}
      <div class="ghost-head">{kind(p)}</div>
      <div class="ghost-body">
        {#each p?.items ?? [] as item (item.id)}
          <span style="--c: {swatch(item.key)}"><i></i>{item.key}</span>
        {:else}
          <span>empty</span>
        {/each}
      </div>
    {/if}
    {#if moving.target}
      <div class="ghost-action">
        {#if moving.target.tabId}Move to “{ws.tabs.find((t) => t.id === moving.target.tabId)?.name}”{:else}{moving.target.label}{/if}
      </div>
    {/if}
  </div>
{/if}
