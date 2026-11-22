// The peak commands on the whole page. Each, from each origin that offers
// it (the plot menu's peaks submenu, the readout's menu, the x axis's popover, its key and
// the command palette), puts the same X cursor on the same peak: X1 or X2
// on the nearest peak of the view, the cursor placed last on the next peak
// either way. Each is one undo step. Past the view's edge the view moves to
// show the peak, a step Back takes back, the cursor staying where it went.
// On several selected plots each of the lead plot's kind goes to a peak of
// its own signals, as one step. In the plot menu's peaks submenu P and
// Shift+P run the rows that show them. A time plot is paused as its X1 goes
// to a peak of it.
import { describe, expect, it } from 'vitest';
import { commands } from 'vitest/browser';
import { flushSync } from 'svelte';
import { centre, click, closeAll, editCursors, feed, leafOf, mountPage, pressKey, rightClick, settle, until, wait } from './harness/page.js';
import { MENUS, openMenu } from './harness/menus.js';
import * as clock from '../src/lib/clock.js';
import { xOf } from '../src/lib/cursors.js';
import { plotIds } from '../src/lib/layout.js';

const KEYS = [['sine.sine'], ['aux.triangle'], ['sine.sine'], ['aux.triangle']];
const KIND = ['spectrum', 'spectrum', 'time', 'time'];
/** Each signal's spectral peaks, in Hz: bins standing 20 dB over the rest. */
const PEAKS = { 'sine.sine': [8, 23, 41], 'aux.triangle': [12, 30] };

/** The page: two spectra, one of each signal, with the peaks PEAKS has, and two time plots of the sines `feed` gives. */
async function mounted() {
  const page = await mountPage(KEYS, 'grid', 0, Object.keys(PEAKS));
  feed(page.hub);
  const session = page.hub.session, perf = clock.now(), f = Float32Array.from({ length: 50 }, (_, i) => i + 1);
  session.apply({ kind: 'spectra', spectra: [...session.traces.values()].map((t) => ({ id: t.meta.id, f, a: f.map((v) => (PEAKS[t.key]?.includes(v) ? 0.5 : 0.05)), floor: 0.02 })) }, perf);
  session.tick(perf);
  const ids = plotIds(page.ws.layout);
  page.history.group('Prepare', () => ids.forEach((id, n) => page.ws.setPlot(id, { kind: KIND[n], readoutAt: { x: 1, y: 0 } })));
  await wait(300);
  await settle(6);
  return { page, ids };
}

/** X cursor `name` of plot `id` as the tab keeps it. */
const xAt = (page, id, name) => xOf(page.ws.plots[id].cursors, page.ws.plots[id].kind)[name];

/** Opens plot `on`'s menu and its peaks submenu, and returns the submenu. */
async function peaksMenu(page, on) {
  await MENUS['the plot menu'](page, on);
  const trigger = [...document.querySelectorAll('[data-slot=context-menu-sub-trigger]')].find((t) => t.textContent.trim().startsWith('X cursors to peaks'));
  if (!trigger) return null;
  await click(centre(trigger));
  await until(() => openMenu(true), 2000);
  return openMenu(true);
}

/** Runs peak command `id` on plot `on`, the plot alone selected, from `origin`, and says whether that origin offered it. */
async function runFrom(page, on, id, origin, key) {
  await closeAll();
  page.ws.selection.only([on]);
  flushSync();
  await settle();
  if (origin === 'its key') return void (await pressKey(key));
  if (origin === 'the palette') {
    await pressKey('Control+k');
    await settle();
    const item = document.querySelector(`[data-slot=command-item][data-command="${id}"]`);
    if (!item) return false;
    item.click();
    await settle();
    return until(() => !document.querySelector('[data-slot=dialog-overlay]'), 2000);
  }
  let el;
  if (origin === 'the plot menu') el = await peaksMenu(page, on);
  else if (origin === 'the readout’s menu') el = (await rightClick(centre(leafOf(on).querySelector('.cursor-readout'))), openMenu());
  else {
    const P = page.pv(on).geo.plot;
    await rightClick([P.x + P.w / 2, P.y + P.h + 8]);
    const open = () => document.querySelector('[data-slot=popover-content][data-state=open]');
    await until(open, 2000);
    el = open();
  }
  const control = el?.querySelector(`[data-command="${id}"]`);
  if (!control) return false;
  await click(centre(control));
}

const ORIGINS = ['the plot menu', 'the readout’s menu', 'the x axis', 'its key', 'the palette'];

describe('a peak command', () => {
  it('puts the same cursor on the same peak from every origin, as one undo step', async () => {
    const { page, ids } = await mounted();
    try {
      const on = ids[0], logX = page.pv(on).logX;
      // X2 at 35 Hz, so the readout shows; X1 not placed, so it goes from the middle of the view.
      const cases = [
        ['x1ToPeak', 'p', 'a', logX ? 8 : 23],
        ['x2ToPeak', 'Shift+P', 'b', 41],
        ['peakRight', '.', 'b', 41],
        ['peakLeft', ',', 'b', 23],
      ];
      for (const [id, key, name, want] of cases) {
        for (const origin of ORIGINS) {
          editCursors(page, on, () => ({ f: { a: null, b: 35 } }));
          await wait(200);
          await settle();
          const before = page.ws.plots[on].cursors, steps = page.history.undos.length;
          const offered = await runFrom(page, on, id, origin, key);
          expect(offered, `${id} offered by ${origin}`).not.toBe(false);
          await settle();
          expect(xAt(page, on, name), `${id} from ${origin}`).toBe(want);
          expect(page.history.undos.length - steps, `${id} from ${origin}: one step`).toBe(1);
          page.history.undo();
          flushSync();
          expect(page.ws.plots[on].cursors, `${id} from ${origin} undone`).toEqual(before);
        }
      }
    } finally {
      await closeAll();
      page.dispose();
    }
  }, 300_000);

  it('moves the view just enough to show a peak past its edge, as a step Back takes back, the cursor staying where it went', async () => {
    const { page, ids } = await mounted();
    try {
      const on = ids[0];
      page.ws.setView(on, { fRange: [5, 25] });
      editCursors(page, on, () => ({ f: { a: 23, b: null } }));
      page.ws.selection.only([on]);
      await wait(200);
      await settle();
      await pressKey('.');
      await settle();
      const shown = page.ws.plots[on].fRange;
      expect(xAt(page, on, 'a')).toBe(41);
      expect(shown[0] < 41 && 41 < shown[1], `41 Hz shown in ${shown}`).toBe(true);
      // As wide as it was, along the axis's scale.
      const width = (r) => (page.pv(on).logX ? Math.log10(r[1] / r[0]) : r[1] - r[0]);
      expect(width(shown)).toBeCloseTo(width([5, 25]), 6);
      // At the last peak there is nowhere further: the key does nothing.
      const steps = page.history.undos.length;
      await pressKey('.');
      await settle();
      expect([xAt(page, on, 'a'), page.history.undos.length, page.ws.plots[on].fRange]).toEqual([41, steps, shown]);
      leafOf(on).querySelector('button[aria-label="Back to the view before"]').click();
      await settle();
      expect(page.ws.plots[on].fRange).toEqual([5, 25]);
      expect(xAt(page, on, 'a')).toBe(41);
    } finally {
      page.dispose();
    }
  }, 120_000);

  it('on several selected plots moves each of the lead plot’s kind to a peak of its own, as one step, and leaves the others', async () => {
    const { page, ids } = await mounted();
    try {
      page.ws.selection.only([ids[0], ids[1], ids[2]], ids[0]);
      flushSync();
      await settle();
      const steps = page.history.undos.length;
      // No X1 placed: from the view's left edge to each spectrum's first peak.
      await pressKey('.');
      await settle();
      expect([xAt(page, ids[0], 'a'), xAt(page, ids[1], 'a'), xAt(page, ids[2], 'a')]).toEqual([8, 12, null]);
      expect(page.history.undos.length).toBe(steps + 1);
      expect(page.history.undos.at(-1).label).toBe('Next peak right (2 plots)');
    } finally {
      page.dispose();
    }
  }, 120_000);

  it('in the plot menu’s peaks submenu, runs X1 to peak by P and X2 to peak by Shift+P', async () => {
    const { page, ids } = await mounted();
    try {
      // From the middle of the view, on the axis's scale: 7.1 Hz on a log axis, 25.5 Hz on a linear one.
      const on = ids[1], want = page.pv(on).logX ? 12 : 30;
      for (const [key, name] of [['p', 'a'], ['Shift+P', 'b']]) {
        editCursors(page, on, () => undefined);
        page.ws.selection.only([on]);
        await settle();
        const sub = await peaksMenu(page, on);
        expect(sub, 'the peaks submenu opens').toBeTruthy();
        // The pointer onto the submenu, which takes focus there, as its rows' keys are heard where focus is.
        await commands.mouse([['move', ...centre(sub.querySelector('[role^=menuitem]'))]]);
        await settle();
        expect(document.activeElement?.closest('[data-slot=context-menu-sub-content]'), 'focus in the submenu').toBeTruthy();
        await pressKey(key);
        await settle();
        expect([xAt(page, on, 'a'), xAt(page, on, 'b')], key).toEqual(name === 'a' ? [want, null] : [null, want]);
        await closeAll();
      }
    } finally {
      page.dispose();
    }
  }, 120_000);

  it('pauses a time plot as it puts X1 on a peak of its signal', async () => {
    const { page, ids } = await mounted();
    try {
      const on = ids[2], view = page.views.get(on);
      page.ws.selection.only([on]);
      await settle();
      expect(view.live).toBe(true);
      await pressKey('p');
      await settle();
      expect(view.live).toBe(false);
      // The sine feed gives, sin(t + id), at its crest there.
      const x = xAt(page, on, 'a'), id = [...page.hub.session.traces.values()].find((t) => t.key === 'sine.sine').meta.id;
      expect(Math.sin(x + id)).toBeGreaterThan(0.99);
    } finally {
      page.dispose();
    }
  }, 120_000);
});
