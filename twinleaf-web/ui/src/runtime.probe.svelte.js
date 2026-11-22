import { flushSync } from 'svelte';

// What the runtime test reads: an effect over a piece of state, and whether
// state wraps the object it was given.
export function probe() {
  const o = {};
  const wrapped = $state([o]);
  const state = $state({ n: 0 });
  const seen = [];
  const dispose = $effect.root(() => {
    $effect(() => {
      seen.push(state.n);
    });
  });
  flushSync();
  return { state, seen, dispose, proxied: !new Set([o]).has(wrapped[0]) };
}

