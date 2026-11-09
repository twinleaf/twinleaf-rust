import { afterEach, describe, expect, it, vi } from 'vitest';
import { Drags } from '../src/lib/drag.svelte.js';

afterEach(() => vi.useRealTimers());

describe('plots that land', () => {
  it('stay flashed for the whole flash, however soon after another they land', () => {
    vi.useFakeTimers();
    const drags = new Drags(null, () => {});
    drags.flash(['a']);
    vi.advanceTimersByTime(500);
    drags.flash(['b']);
    vi.advanceTimersByTime(500);
    expect(drags.landed).toEqual(new Set(['b']));
    vi.advanceTimersByTime(500);
    expect(drags.landed).toBe(null);
  });
});
