import { describe, expect, it } from 'vitest';
import { traceWidthOf } from './prefs.svelte.js';

describe('A trace width', () => {
  it('snaps to its steps and stays within its range', () => {
    expect(traceWidthOf(1.3)).toBe(1.25);
    expect(traceWidthOf(2.6)).toBe(2.5);
    expect(traceWidthOf(0.2)).toBe(1);
    expect(traceWidthOf(9)).toBe(3);
  });
});
