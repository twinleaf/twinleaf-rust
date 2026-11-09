import { describe, expect, it } from 'vitest';
import { nextPreset, positionOf, windowFrom, windowText, windowTyped } from '../src/lib/window.js';

describe('The window slider', () => {
  it('runs on a log scale from the shortest preset to the longest', () => {
    expect(positionOf(0.1)).toBe(0);
    expect(positionOf(120)).toBe(1);
    expect(positionOf(Math.sqrt(0.1 * 120))).toBeCloseTo(0.5);
    expect(positionOf(600)).toBe(1);
  });

  it('settles into a preset near it and takes any window between', () => {
    const width = 200;
    expect(windowFrom(positionOf(10) + 3 / width, width)).toBe(10);
    expect(windowFrom((positionOf(5) + positionOf(10)) / 2, width)).toBe(7.1);
    expect(windowFrom(-1, width)).toBe(0.1);
  });

  it('steps from preset to preset, from between them too', () => {
    expect(nextPreset(10, 1)).toBe(30);
    expect(nextPreset(7.4, 1)).toBe(10);
    expect(nextPreset(7.4, -1)).toBe(5);
    expect(nextPreset(60, 1)).toBe(120);
    expect(nextPreset(120, 1)).toBe(120);
  });

  it('writes and reads a window in seconds', () => {
    expect([windowText(30), windowText(7.408), windowText(0.25), windowText(0.002)]).toEqual(['30', '7.4', '0.25', '0.002']);
    expect(windowTyped('7.4 s')).toBe(7.4);
    expect(windowTyped('500')).toBe(120);
    expect(windowTyped('soon')).toBe(null);
  });
});
