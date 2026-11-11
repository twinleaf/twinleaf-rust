// The History popover asks before a lower limit drops history the hub holds:
// - a lower limit that drops nothing the hub holds is set at once;
// - one that drops some asks in place, saying how much of the live history
//   and, while plots are paused, of what they hold; Keep leaves the limit
//   and sends the hub nothing, Drop sends the lower limit once; focus goes
//   to Keep as it asks;
// - a higher limit never asks;
// - Escape while it asks keeps the limit and leaves the popover open, and
//   a second Escape closes it.
// The hub here says what it holds by `memory` messages, and the page's
// /api/history requests are counted.
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { page as browser } from 'vitest/browser';
import { clickOn, mountPage, pressKey, settle, until } from './harness/page.js';

/** Where VITE_TL_SHOTS names a folder, relative to this file, the screenshots go there. */
const SHOTS = import.meta.env.VITE_TL_SHOTS;
const shot = async (name) => SHOTS && (await browser.screenshot({ path: `${SHOTS}/${name}.png` }));

/** A hub holding an hour of a 1600 bytes a second stream, its History an hour within 1 GB. */
const FULL = { type: 'memory', bytes: 3600 * 1600, budget: 1e9, seconds: 3600, max_age: 3600, rate: 1600, oldest: { 0: 0 } };

let page, sent;
beforeEach(async () => {
  page = await mountPage([['sine.sine']], 'grid', 0, ['sine.sine']);
  sent = [];
  const stubbed = window.fetch;
  window.fetch = (url, init) => {
    if (new URL(String(url), location.href).pathname !== '/api/history') return stubbed(url, init);
    sent.push(JSON.parse(init.body));
    return Promise.resolve(new Response('{}', { status: 200 }));
  };
});
afterEach(() => page.dispose());

/** The open History popover. */
const popover = () => document.querySelector('[data-slot=popover-content] .history-set');
/** The question it asks: its lines and its buttons' words, or null while it asks none. */
const question = () => {
  const ask = popover()?.querySelector('.lower-ask');
  return ask && { lines: [...ask.querySelectorAll('p')].map((p) => p.textContent), buttons: [...ask.querySelectorAll('button')].map((b) => b.textContent.trim()) };
};
/** The button of the question that says `text`. */
const button = (text) => [...popover().querySelectorAll('.lower-ask button')].find((b) => b.textContent.trim() === text);

/** Has the hub say `memory`, and opens the History popover in the status bar. */
async function open(memory) {
  page.hub.text(memory);
  await settle();
  await clickOn(document.querySelector('.status-bar .history'));
  expect(await until(popover), 'the popover opens').toBe(true);
}

/** Picks the time limit `label` from the popover's list, which shows `current` as the one set, when given. */
async function pickTime(label, current) {
  await clickOn(popover().querySelector('[aria-label="Time limit"]'));
  const option = () => [...document.querySelectorAll('[role=option]')].find((o) => o.textContent.trim() === label);
  expect(await until(option), `${label} is offered`).toBe(true);
  if (current) expect([...document.querySelectorAll('[role=option][aria-selected=true]')].map((o) => o.textContent.trim())).toEqual([current]);
  await clickOn(option());
  await until(() => !document.querySelector('[data-slot=select-content]'));
  await settle();
}

describe('a lower History limit', () => {
  it('is set at once when the hub holds less than it', async () => {
    await open({ ...FULL, bytes: 100 * 1600, seconds: 100 });
    await pickTime('30 min');
    expect(question()).toBe(null);
    expect(sent).toEqual([{ seconds: 1800, bytes: 1e9 }]);
  });

  it('asks first when it drops history held, and Keep sends nothing', async () => {
    await open(FULL);
    await pickTime('30 min');
    expect(question()).toEqual({ lines: ['This drops the oldest 30 min now held'], buttons: ['Keep 60 min', 'Drop 30 min'] });
    expect(sent).toEqual([]);
    await clickOn(button('Keep 60 min'));
    await settle();
    expect(question()).toBe(null);
    expect(popover().querySelector('[aria-label="Time limit"]').textContent.trim()).toBe('60 min');
    expect(sent).toEqual([]);
    // The list shows the limit kept, and the lower one picked again asks again.
    await pickTime('30 min', '60 min');
    expect(question()?.buttons).toEqual(['Keep 60 min', 'Drop 30 min']);
  });

  it('sets the lower limit once on Drop', async () => {
    await open(FULL);
    await pickTime('10 min');
    expect(question()?.lines).toEqual(['This drops the oldest 50 min now held']);
    await clickOn(button('Drop 50 min'));
    await settle();
    expect(question()).toBe(null);
    expect(sent).toEqual([{ seconds: 600, bytes: 1e9 }]);
  });

  it('says what paused plots lose of what they hold', async () => {
    // Paused 30 min ago: the paused copy holds an hour, half of it older than the live history.
    await open({ ...FULL, held: { bytes: 1800 * 1600, seconds: 3600 } });
    await pickTime('30 min');
    expect(question()).toEqual({
      lines: ['This drops the oldest 30 min now held', 'Paused plots lose the oldest 30 min they show'],
      buttons: ['Keep 60 min', 'Drop 30 min'],
    });
  });

  it('asks for a lower memory limit too, by the share of the history it drops', async () => {
    // Full by memory: an hour in 500 MB.
    await open({ ...FULL, bytes: 5e8, budget: 5e8, max_age: 86400, rate: 5e8 / 3600 });
    await clickOn(popover().querySelector('[aria-label="Less memory"]'));
    await settle();
    expect(question()).toEqual({ lines: ['This drops the oldest 30 min now held'], buttons: ['Keep 500.0 MB', 'Drop 30 min'] });
    expect(sent).toEqual([]);
    await clickOn(button('Drop 30 min'));
    await settle();
    expect(sent).toEqual([{ seconds: 86400, bytes: 2.5e8 }]);
  });

  it('is kept by Escape, which leaves the popover open; a second Escape closes it', async () => {
    await open(FULL);
    await pickTime('30 min');
    expect(question()).not.toBe(null);
    await pressKey('Escape');
    expect(await until(() => !popover(), 500), 'the popover stays').toBe(false);
    expect(question(), 'the question goes').toBe(null);
    expect(popover().querySelector('[aria-label="Time limit"]').textContent.trim(), 'the limit kept').toBe('60 min');
    expect(sent).toEqual([]);
    await pressKey('Escape');
    expect(await until(() => !popover()), 'the popover closes').toBe(true);
    expect(sent).toEqual([]);
    // A lower memory limit's question goes the same way.
    await open({ ...FULL, bytes: 5e8, budget: 5e8, max_age: 86400, rate: 5e8 / 3600 });
    await clickOn(popover().querySelector('[aria-label="Less memory"]'));
    await settle();
    expect(question()?.buttons).toEqual(['Keep 500.0 MB', 'Drop 30 min']);
    await pressKey('Escape');
    expect(await until(() => !popover(), 500)).toBe(false);
    expect(question()).toBe(null);
    expect(popover().querySelector('[aria-label="Memory limit"] span').textContent).toBe('500.0 MB');
    expect(sent).toEqual([]);
  });

  it('moves focus to Keep as it asks, whichever control asked', async () => {
    await open(FULL);
    await pickTime('30 min');
    expect(await until(() => document.activeElement === button('Keep 60 min')), 'Keep has focus after a pick from the list').toBe(true);
    await shot('history-1-asks');
    await pressKey('Escape');
    await settle();
    await shot('history-2-kept');
    expect(document.activeElement, 'kept, focus goes back to the list').toBe(popover().querySelector('[aria-label="Time limit"]'));
    await pressKey('Escape');
    expect(await until(() => !popover())).toBe(true);
    await open({ ...FULL, bytes: 5e8, budget: 5e8, max_age: 86400, rate: 5e8 / 3600 });
    await clickOn(popover().querySelector('[aria-label="Less memory"]'));
    expect(await until(() => document.activeElement === button('Keep 500.0 MB')), 'Keep has focus after the stepper').toBe(true);
  });
});

describe('a higher History limit', () => {
  it('is set at once, whatever the hub holds', async () => {
    await open({ ...FULL, held: { bytes: 1800 * 1600, seconds: 3600 } });
    await pickTime('4 h');
    expect(question()).toBe(null);
    expect(sent).toEqual([{ seconds: 14400, bytes: 1e9 }]);
    await clickOn(popover().querySelector('[aria-label="More memory"]'));
    await settle();
    expect(question()).toBe(null);
    expect(sent.at(-1)).toEqual({ seconds: 3600, bytes: 2e9 });
  });
});
