// The History popover asks before a lower limit drops history the hub holds:
// - a lower limit that drops nothing the hub holds is set at once;
// - one that drops some asks in place, saying how much of the live history
//   and, while plots are paused, of what they hold; Keep leaves the limit
//   and sends the hub nothing, Drop sends the lower limit once; focus goes
//   to Keep as it asks;
// - a higher limit never asks;
// - Escape while it asks keeps the limit and leaves the popover open, and
//   a second Escape closes it.
// Its waterline marks where the time limit fills the budget, and the band
// on from there a full paused history takes, pinned at the bar's end with
// an arrow past it; the line under it says what a paused plot keeps; and
// neither changes as the hub notes what it holds.
// The hub here says what it holds by `memory` messages, and the page's
// /api/history requests are counted.
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { page as browser } from 'vitest/browser';
import { clickOn, closeAll, frames, mountPage, pressKey, settle, until } from './harness/page.js';
import { pausing } from '../src/lib/session.svelte.js';
import { pausedKeeps } from '../src/lib/words.js';
import { prefs } from '../src/lib/prefs.svelte.js';

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

describe('the History waterline', () => {
  /** 760 MB an hour. */
  const RATE = 760e6 / 3600;
  /** The hub's `memory` in each case, and what the line under the caption says of it. */
  const CASES = {
    'time first, with room': [{ ...FULL, bytes: 380e6, rate: RATE / 2 }, 'A paused plot keeps its full 60 min'],
    'time first, without room': [{ ...FULL, bytes: 760e6, rate: RATE }, 'A paused plot keeps its full 60 min for ~19 min, then loses its oldest. All of it needs ~1.5 GB'],
    'memory first': [{ ...FULL, bytes: 1e9, seconds: 1e9 / RATE, max_age: 14400, rate: RATE }, 'A paused plot loses its oldest at once. Its full 4 h needs ~6.1 GB'],
  };
  const pauseOf = (m) => pausing({ rate: m.rate, maxAge: m.max_age, budget: m.budget });
  const part = (sel) => popover().querySelector(sel);
  /** Where on the bar, as a share of its width, `el`'s left and right edges are, and the middle of a tick. */
  const place = (el) => {
    const bar = part('.waterline').getBoundingClientRect(), r = el.getBoundingClientRect();
    return { left: (r.left - bar.left) / bar.width, right: (r.right - bar.left) / bar.width, middle: (r.left + r.width / 2 - bar.left) / bar.width, px: 1 / bar.width };
  };
  /** How the waterline and the line under it look: the line, the band's place, and whether the arrow shows. */
  const looks = () => JSON.stringify([part('.paused-keeps')?.textContent, part('.waterline .paused')?.getAttribute('style'), !!part('.past-end')]);

  it('marks where the live history and a full paused one beside it fill the budget, and says what a paused plot keeps', async () => {
    for (const [name, [memory, line]] of Object.entries(CASES)) {
      await closeAll();
      await open(memory);
      const pause = pauseOf(memory), timeFirst = pause.steady < memory.budget;
      expect(part('.paused-keeps').textContent, name).toBe(line);
      expect(line, name).toBe(pausedKeeps(pause, memory.max_age));
      const band = part('.waterline .paused'), tick = part('.waterline .tick'), meter = part('[role=meter]');
      if (!timeFirst) {
        // Memory first: neither mark, as the time limit's lies past the bar.
        expect([!!band, !!tick, !!part('.past-end')], name).toEqual([false, false, false]);
        expect(meter.getAttribute('aria-valuetext'), name).toBe('1.00 GB of 1.00 GB');
        continue;
      }
      const at = place(tick), span = place(band), px = at.px;
      expect(Math.abs(at.middle - pause.steady / memory.budget), `${name}: the tick`).toBeLessThanOrEqual(px);
      expect(Math.abs(span.left - pause.steady / memory.budget), `${name}: the band begins at the tick`).toBeLessThanOrEqual(px);
      expect(Math.abs(span.right - Math.min(1, pause.need / memory.budget)), `${name}: the band ends where the two fill the budget, or at the bar's end`).toBeLessThanOrEqual(px);
      const past = pause.need > memory.budget;
      const arrow = part('.past-end');
      expect(!!arrow, `${name}: an arrow says the band goes on past the bar`).toBe(past);
      if (past) expect(arrow.getBoundingClientRect().left, `${name}: after the bar`).toBeGreaterThanOrEqual(part('.waterline').getBoundingClientRect().right - 1);
      expect(meter.getAttribute('aria-valuetext'), name).toBe(
        past ? '760.0 MB of 1.00 GB. The time limit fills ~760 MB, and ~1.5 GB with a paused plot, past the limit' : '380.0 MB of 1.00 GB. The time limit fills ~380 MB, and ~760 MB with a paused plot',
      );
    }
  });

  it('stays as it is while the hub notes what it holds, and the paused plots’ share grows', async () => {
    const [memory] = CASES['time first, without room'];
    await open(memory);
    const seen = [];
    for (let s = 0; s < 6; s++) {
      // The live history rounds by its chunks, and a pause costs more each second.
      page.hub.text({ ...memory, bytes: memory.bytes + (s % 2 ? 262144 : -262144), held: { bytes: s * RATE, seconds: 3600 } });
      for (let f = 0; f < 10; f++) {
        await frames(1);
        if (seen.at(-1) !== looks()) seen.push(looks());
      }
    }
    expect(seen, 'what the waterline and its line showed, in order').toHaveLength(1);
    expect(part('.water-notes').textContent, 'what the paused plots hold says so beside it').toContain('Paused plots hold ~1.1 MB more');
  });

  it('is shot in each case, light and dark', async () => {
    try {
      for (const theme of ['light', 'dark']) {
        prefs.setTheme(theme);
        for (const [name, [memory]] of Object.entries(CASES)) {
          await closeAll();
          await open(memory);
          await settle(4);
          expect(part('.paused-keeps'), name).not.toBe(null);
          if (SHOTS) await browser.screenshot({ path: `${SHOTS}/history-water-${theme}-${name.replaceAll(/[ ,]+/g, '-')}.png`, element: popover().parentElement });
        }
      }
    } finally {
      prefs.setTheme('system');
    }
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
