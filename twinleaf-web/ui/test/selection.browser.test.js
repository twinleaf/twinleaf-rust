// What keeps the plots selected and what clears them, on the whole page, as
// the user's verdicts list it (round 2, Selection #1, #2, #16, #17). A
// press on each control of every kind the list names, with any plots
// selected, clears the selection or keeps it as the list says; a press on a
// plot's own control selects that plot first; keyboard focus selects; and
// Escape deselects, leaving a maximized plot as it is.
import { describe, expect, it } from 'vitest';
import { commands } from '@vitest/browser/context';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { frames, mountPage } from './browser/page.js';
import { plotIds } from '../src/lib/layout.js';

const KEYS = [['sine.sine'], ['aux.triangle'], ['sine.cosine']];

/** The controls round 2 #16 names, each a selector for its elements on the page and whether a press there keeps the plots selected. */
const LIST = [
  ['the Live button', () => [...document.querySelectorAll('button')].filter((b) => /^(Live|Paused|Pause)/.test(b.textContent.trim())), false],
  ['the Layout menu', () => [...document.querySelectorAll('button')].filter((b) => b.textContent.trim() === 'Layout'), false],
  ['display settings', () => [...document.querySelectorAll('button[aria-label="Display settings"]')], false],
  ['the tab strip', () => [...document.querySelectorAll('[role=tablist] [data-tab]')], false],
  ['dock headers', () => [...document.querySelectorAll('.dock-head')], false],
  ['the status bar', () => [...document.querySelectorAll('.status-bar')], false],
  ['the gaps between plots', () => [...document.querySelectorAll('.tiles .separator')], false],
  ['the plots', () => [...document.querySelectorAll('.tiles .leaf')], true],
  ['the window slider', () => [...document.querySelectorAll('button[aria-label="Time window"]')], true],
  ['the eraser', () => [...document.querySelectorAll('button[aria-label="Clear the cursors"]')], true],
  ['File', () => [...document.querySelectorAll('button')].filter((b) => b.textContent.trim() === 'File'), true],
  ['Commands', () => [...document.querySelectorAll('button.search')], true],
  ['the Signals list', () => [...document.querySelectorAll('.palette')], true],
];

/** A press and its release on `el`, as a click's are, so whatever it starts ends. */
function press(el, init = {}) {
  const at = { bubbles: true, cancelable: true, button: 0, pointerType: 'mouse', ...init };
  el.dispatchEvent(new PointerEvent('pointerdown', at));
  el.dispatchEvent(new PointerEvent('pointerup', at));
}

describe('a press', () => {
  it('on each control the list names keeps the plots selected or clears them, as it says', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      for (const [name, find, keeps] of LIST) {
        const els = find();
        expect(els.length, `${name} on the page`).toBeGreaterThan(0);
        for (const el of els) {
          const ids = plotIds(page.ws.layout);
          page.ws.selected.clear();
          for (const id of ids.slice(0, 2)) page.ws.selected.add(id);
          // A plot pressed is selected alone unless it was in the selection; the rest of the list keeps it as it was.
          const inPlot = el.closest('.leaf')?.dataset.plot;
          press(el);
          flushSync();
          if (!keeps) expect(page.ws.selected.size, `${name}: clears`).toBe(0);
          else if (!inPlot || ids.slice(0, 2).includes(inPlot)) expect([...page.ws.selected], `${name}: keeps`).toEqual(ids.slice(0, 2));
        }
      }
    } finally {
      page.dispose();
    }
  });

  it('with Shift, or the wheel anywhere, never clears the selection', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      const [a, b] = plotIds(page.ws.layout);
      for (const [, find] of LIST) {
        for (const el of find()) {
          page.ws.selected.clear();
          page.ws.selected.add(a);
          page.ws.selected.add(b);
          press(el, { shiftKey: true });
          el.dispatchEvent(new WheelEvent('wheel', { bubbles: true, deltaY: 40 }));
          flushSync();
          expect(page.ws.selected.has(a) && page.ws.selected.has(b)).toBe(true);
        }
      }
    } finally {
      page.dispose();
    }
  });

  it('on a plot’s own control selects that plot first, or keeps the selection it is in', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      const ids = plotIds(page.ws.layout);
      let checked = 0;
      fc.assert(
        fc.property(fc.subarray(ids), fc.nat(), fc.nat(), (picked, k, j) => {
          const id = ids[k % ids.length];
          const controls = [...document.querySelectorAll(`.leaf[data-plot="${id}"] button, .leaf[data-plot="${id}"] [role=combobox]`)];
          if (!controls.length) return;
          page.ws.selected.clear();
          for (const p of picked) page.ws.selected.add(p);
          press(controls[j % controls.length]);
          flushSync();
          expect([...page.ws.selected].sort()).toEqual(picked.includes(id) ? [...picked].sort() : [id]);
          expect(page.ws.focusedId).toBe(id);
          checked++;
        }),
      );
      expect(checked).toBeGreaterThan(20);
    } finally {
      page.dispose();
    }
  });
});

describe('Escape', () => {
  it('deselects everything, and leaves a maximized plot and the cursors as they are', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      const [a, b] = plotIds(page.ws.layout);
      page.ws.maximized = [a];
      page.ws.selected.add(a);
      page.ws.selected.add(b);
      page.ws.focusedId = a;
      flushSync();
      document.body.focus();
      await commands.key('Escape');
      flushSync();
      expect(page.ws.selected.size).toBe(0);
      expect(page.ws.focusedId).toBe(null);
      expect(page.ws.maximized).toEqual([a]);
    } finally {
      page.dispose();
    }
  });
});

describe('keyboard focus', () => {
  it('on a plot selects it alone', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      const [a, b, c] = plotIds(page.ws.layout);
      page.ws.selected.add(a);
      page.ws.selected.add(c);
      flushSync();
      await commands.key('2');
      flushSync();
      await frames();
      expect([...page.ws.selected]).toEqual([b]);
    } finally {
      page.dispose();
    }
  });

  it('is not needed for a plot key to act on the selection', async () => {
    const page = await mountPage(KEYS, 'columns');
    try {
      const ids = plotIds(page.ws.layout);
      document.body.focus();
      await commands.key('Control+a');
      flushSync();
      expect(page.ws.selected.size).toBe(ids.length);
      expect(page.ws.focusedId).toBe(null);
      await commands.key('l');
      flushSync();
      for (const id of ids) expect(page.ws.plots[id].legend, id).toBe(true);
    } finally {
      page.dispose();
    }
  });
});

describe('a signal clicked in the Signals list', () => {
  it('joins every selected plot that does not show it, and with none selected goes nowhere', async () => {
    const page = await mountPage(KEYS, 'columns', 0, ['vector.x', 'vector.y']);
    try {
      const [a, b, c] = plotIds(page.ws.layout);
      const click = async () => {
        document.querySelector('.palette [data-key="vector.x"]').click();
        flushSync();
        await frames();
      };
      const tabs = page.ws.tabs;
      await click();
      expect(page.ws.tabs).toBe(tabs);
      page.ws.selected.add(a);
      page.ws.selected.add(c);
      await click();
      for (const id of [a, c]) expect(page.ws.plots[id].items.map((i) => i.key)).toContain('vector.x');
      expect(page.ws.plots[b].items.map((i) => i.key)).not.toContain('vector.x');
      expect(page.history.undos.length).toBe(1);
    } finally {
      page.dispose();
    }
  });
});
