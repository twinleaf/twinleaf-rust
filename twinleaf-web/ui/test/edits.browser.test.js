// Every edit the page's controls make is one undo step, and undo puts the
// tabs back as they were. The page runs as in development, where an edit
// made outside an undo step throws, so a control that edits outside one
// fails here.
import { describe, expect, it } from 'vitest';
import { commands } from '@vitest/browser/context';
import { flushSync } from 'svelte';
import { centre, frames, mountPage, wait } from './browser/page.js';
import { plotIds } from '../src/lib/layout.js';

const KEYS = [['sine.sine', 'sine.cosine'], ['aux.triangle']];

/**
 * Runs `act` on a fresh page, after `prepare`, and checks it made one
 * step, named as `label` matches, that undo takes back.
 */
async function oneStep(label, act, prepare = () => {}) {
  const page = await mountPage(KEYS, 'columns', 1);
  prepare(page);
  flushSync();
  await frames();
  const steps = page.history.undos.length;
  const errors = [];
  const onError = (e) => errors.push(e.error?.stack ?? e.message ?? String(e.reason));
  window.addEventListener('error', onError);
  window.addEventListener('unhandledrejection', onError);
  try {
    const before = page.ws.tabs;
    await act(page);
    flushSync();
    await frames();
    expect(errors, JSON.stringify(errors)).toEqual([]);
    expect(page.history.undos.slice(steps).map((s) => s.label)).toEqual([expect.stringMatching(label)]);
    expect(page.ws.tabs).not.toBe(before);
    page.history.undo();
    flushSync();
    expect(page.ws.tabs).toBe(before);
  } finally {
    window.removeEventListener('error', onError);
    window.removeEventListener('unhandledrejection', onError);
    page.dispose();
  }
}

const plotEl = (page, i = 0) => document.querySelector(`.leaf[data-plot="${plotIds(page.ws.layout)[i]}"]`);

describe('a control that edits', () => {
  it('the legend key shows the legend of the selected plot', () =>
    oneStep(/legend/i, async (page) => {
      page.ws.selection.only([plotIds(page.ws.layout)[0]]);
      plotEl(page).querySelector('.plot-card').focus();
      await commands.key('l');
    }));

  it('a chip’s × removes its signal', () =>
    oneStep(/^Remove sine\.sine/, (page) => plotEl(page).querySelector('button[aria-label="Remove sine.sine"]').click()));

  it('a plot’s split button adds a plot beside it', () =>
    oneStep(/^Split/, async (page) => {
      await commands.mouse([['move', ...centre(plotEl(page))]]);
      plotEl(page).querySelector('button[aria-label^="Split"]').click();
    }));

  it('a tab’s duplicate button copies the tab', () =>
    oneStep(/^Duplicate tab/, () => document.querySelector('[role=tablist] [data-tab] button[aria-label^="Duplicate"]').click()));

  it('the new tab button adds a tab', () => oneStep(/^New tab/, () => document.querySelector('button[aria-label="New layout"]').click()));

  it('renaming a tab names it', () =>
    oneStep(/^Rename tab/, async () => {
      const tab = document.querySelector('[role=tablist] [data-tab]');
      tab.dispatchEvent(new MouseEvent('dblclick', { bubbles: true }));
      flushSync();
      await frames();
      const input = tab.querySelector('input');
      input.value = 'Renamed';
      input.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }));
    }));

  it('dragging the legend moves it', () =>
    oneStep(
      /^Move the legend/,
      async (page) => {
        const [x, y] = centre(plotEl(page).querySelector('.pinned-legend'));
        await commands.mouse([['move', x, y], ['down'], ['move', x - 40, y + 30], ['move', x - 80, y + 60], ['up']]);
        await wait(50);
      },
      (page) => page.history.group('Legend', () => page.ws.setPlot(plotIds(page.ws.layout)[0], { legend: true })),
    ));
});
