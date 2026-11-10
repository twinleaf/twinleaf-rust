// Device writes on the whole page (round 2, Device writes #1 to #6): a
// write from the edit bar shows a notice that offers Revert; Ctrl+Z never
// writes to a device; Revert puts back the old value where the device
// still shows the value written and says which device it left; Revert goes
// when a device reconnects; a Revert answered late gives way to a write
// started after it; a device that refuses is named, with why, first; and
// an action reports with a notice of its own, with no Revert. The devices
// are the stub hub's, which answer as the server does. Every control is
// pressed with the real mouse, and the field typed into from the keyboard.
import { describe, expect, it } from 'vitest';
import { commands } from '@vitest/browser/context';
import { flushSync } from 'svelte';
import { clickOn, frames, mountPage, typeInto, wait } from './harness/page.js';

const RPCS = [
  { name: 'gain', channel: 'setting', value: 5 },
  { name: 'save', channel: 'action', type: null, access: [] },
];
const ROUTES = ['/0', '/1'];

const row = (name) => [...document.querySelectorAll('tr[id^=prop-]')].find((tr) => tr.querySelector('.pname')?.firstChild.textContent.trim() === name);
const notices = () => [...document.querySelectorAll('[data-sonner-toast]')].filter((t) => t.dataset.removed !== 'true');
const noticeText = () => notices().map((t) => t.textContent.replace(/\s+/g, ' ').trim()).join(' | ');
const button = (scope, text) => [...scope.querySelectorAll('button')].find((b) => b.textContent.trim() === text);
async function settle() {
  flushSync();
  await wait(20);
  await frames(3);
}
/** Clicks row `name`, types `text` into the edit bar and writes it. */
async function writeRow(name, text) {
  const tr = row(name);
  await clickOn(tr.querySelector('td:last-child'));
  await settle();
  await typeInto(document.querySelector(`input[aria-label="New value of ${name}"]`), text);
  await settle();
  await clickOn(button(document, 'Write to 2 devices'));
  await settle();
}

describe('a write to devices', () => {
  it('offers Revert, which Ctrl+Z is not: Revert puts back the old value where the device still shows the one written, and says which it left', async () => {
    const page = await mountPage([['sine.sine']], 'columns', 0, [], RPCS, { routes: ROUTES });
    try {
      await writeRow('gain', '10');
      expect(page.hub.calls).toEqual([{ name: 'gain', value: '10', routes: ROUTES }]);
      expect(noticeText()).toContain('Set gain to 10 (was 5) · 2 devices');
      expect(button(notices()[0], 'Revert')).toBeTruthy();
      document.activeElement?.blur();
      await commands.key('Control+z');
      await settle();
      expect(page.hub.calls.length, 'Ctrl+Z writes nothing').toBe(1);
      page.hub.announce('/1', { gain: 7 });
      await settle();
      await clickOn(button(notices()[0], 'Revert'));
      await settle();
      expect(page.hub.calls.slice(1)).toEqual([{ name: 'gain', value: '5', routes: ['/0'] }]);
      expect(noticeText()).toContain('Put gain back to 5');
      expect(noticeText()).toContain('test (/1) changed since; left at 7');
      expect(notices().some((t) => button(t, 'Revert'))).toBe(false);
    } finally {
      page.dispose();
    }
  });

  it('loses its Revert when a device it wrote reconnects, and says so', async () => {
    const page = await mountPage([['sine.sine']], 'columns', 0, [], RPCS, { routes: ROUTES });
    try {
      await writeRow('gain', '10');
      expect(button(notices()[0], 'Revert')).toBeTruthy();
      page.hub.presence('/0', 'lost');
      page.hub.presence('/0', 'alive');
      await settle();
      expect(notices().some((t) => button(t, 'Revert'))).toBe(false);
      expect(noticeText()).toContain('Revert is off: test (/0) reconnected');
    } finally {
      page.dispose();
    }
  });

  it('gives way to the write started after it when its Revert answers late', async () => {
    const page = await mountPage([['sine.sine']], 'columns', 0, [], RPCS, { routes: ROUTES });
    try {
      await writeRow('gain', '10');
      page.hub.slow = true;
      await clickOn(button(notices()[0], 'Revert'));
      await settle();
      await writeRow('gain', '7');
      expect(page.hub.calls.slice(1).map((c) => c.value)).toEqual(['5', '7']);
      // The Revert answers: its notice does not take the place of the write started after it, and no Revert shows.
      page.hub.release();
      await settle();
      expect(noticeText()).not.toContain('Put gain back');
      expect(notices().some((t) => button(t, 'Revert'))).toBe(false);
      page.hub.release();
      await settle();
      // The write of 7 started while the Revert was out, so the value before it was not known.
      expect(noticeText()).toContain('Set gain to 7 · 2 devices');
      expect(noticeText()).toContain('No Revert: old value unknown');
      expect(noticeText()).not.toContain('was');
    } finally {
      page.dispose();
    }
  });

  it('names the device that refused, and why, first, and says nothing was sent', async () => {
    const page = await mountPage([['sine.sine']], 'columns', 0, [], RPCS, { routes: ROUTES });
    try {
      page.hub.answer = (name, value, route) => (route === '/1' ? { state: 'refused', error: 'it has no such RPC' } : { state: 'ok', value: +value });
      await writeRow('gain', '10');
      expect(noticeText()).toContain('Could not set gain');
      expect(noticeText()).toContain('test (/1) refused: it has no such RPC');
      expect(noticeText()).toContain('Nothing was sent to the other device');
      expect(noticeText()).not.toMatch(/skipped|Not sent/);
      expect(notices().some((t) => button(t, 'Revert'))).toBe(false);
      // A device that did not answer may have taken the value: it is named, and has no Revert.
      page.hub.answer = (name, value, route) => (route === '/1' ? { state: 'unknown', error: 'timed out' } : { state: 'ok', value: +value });
      await writeRow('gain', '11');
      expect(noticeText()).toContain('Set gain to 11 · 1 of 2 devices');
      expect(noticeText()).toContain('test (/1) did not answer: timed out');
      expect(button(notices()[0], 'Revert')).toBeTruthy();
    } finally {
      page.dispose();
    }
  });
});

describe('an action', () => {
  it('runs at once and reports with a notice, with no Revert', async () => {
    const page = await mountPage([['sine.sine']], 'columns', 0, [], RPCS, { routes: ROUTES });
    try {
      await clickOn(button(document.querySelector('.action-grid'), 'save'));
      await settle();
      expect(page.hub.calls).toEqual([{ name: 'save', value: '', routes: ROUTES }]);
      expect(noticeText()).toContain('Ran save · 2 devices');
      expect(notices().some((t) => button(t, 'Revert'))).toBe(false);
    } finally {
      page.dispose();
    }
  });
});
