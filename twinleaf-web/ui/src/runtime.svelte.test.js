import { describe, expect, it } from 'vitest';
import { flushSync } from 'svelte';
import { probe } from './runtime.probe.svelte.js';

describe('the test runtime', () => {
  it("is Svelte's client runtime, so effects run and state is proxied", () => {
    const p = probe();
    expect(p.proxied).toBe(true);
    p.state.n = 1;
    flushSync();
    expect(p.seen).toEqual([0, 1]);
    p.dispose();
  });
});
