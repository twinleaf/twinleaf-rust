// Device properties in the command palette, on the whole page: a fragment
// typed finds a property, Space or Tab locks it, a value typed and Enter
// writes it to the checked devices through the same path as the edit bar,
// with the same notice and Revert; Enter with nothing typed reads, a
// setting as a read-only property, and the fresh value shows in place;
// an action runs only from a lock; a value a property does not take shows
// the edit bar's reason and writes nothing; devices that differ show ≠;
// the list draws only the best matches; and no key but Enter on a valid
// locked property calls a device. The devices are the stub hub's. A row
// of a command of the page or a tab, picked, does what it names: the
// arrangement, the tab or the dock of that row.
import { describe, expect, it } from 'vitest';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { page as browser } from 'vitest/browser';
import { clickOn, focusPage, frames, mountPage, pressKey, typeInto, until, wait } from './harness/page.js';
import { SHOWN } from '../src/lib/palette.js';

const ROUTES = ['/0', '/1'];
const FILL = Array.from({ length: 300 }, (_, i) => ({ name: `fill.n${i}`, channel: 'diagnostic', access: ['read'], value: i }));
const RPCS = [
  { name: 'gain', channel: 'setting', type: 'u8', value: 5 },
  { name: 'offset', channel: 'setting', value: [2, 3] },
  { name: 'enable', channel: 'setting', type: 'bool', value: true },
  { name: 'temp', channel: 'report', access: ['read'], value: 20 },
  { name: 'reboot', channel: 'action', type: null, access: [] },
  ...FILL,
];

/** Where VITE_TL_SHOTS names a folder, relative to this file, the screenshots go there. */
const SHOTS = import.meta.env.VITE_TL_SHOTS;
const shot = async (name) => SHOTS && (await browser.screenshot({ path: `${SHOTS}/${name}.png` }));

const input = () => document.querySelector('[data-slot=command-input]');
const dialog = () => document.querySelector('[data-slot=dialog-content]');
const chip = () => document.querySelector('.pal-chip')?.textContent.trim() ?? null;
const highlighted = () => document.querySelector('[data-slot=command-item][data-selected]');
const notices = () => [...document.querySelectorAll('[data-sonner-toast]')].filter((t) => t.dataset.removed !== 'true');
const noticeText = () => notices().map((t) => t.textContent.replace(/\s+/g, ' ').trim()).join(' | ');
const button = (scope, text) => [...scope.querySelectorAll('button')].find((b) => b.textContent.trim() === text);
async function settle() {
  flushSync();
  await wait(20);
  await frames(3);
}
/** Opens the palette from the page with Ctrl+K and waits for its field. */
async function openPalette() {
  await focusPage();
  await pressKey('Control+k');
  expect(await until(() => input() && document.activeElement === input(), 2000), 'the palette open, its field focused').toBe(true);
  await settle();
}
/** Types `text` into the field with focus, a key at a time. */
async function type(text) {
  for (const ch of text) await pressKey(ch);
  await settle();
}
const closed = () => until(() => !document.querySelector('[data-slot=dialog-overlay]'), 2000);

describe('a command in the command palette', () => {
  /** The palette's row that reads `text`, or null. */
  const rowOf = (text) => [...document.querySelectorAll('[data-slot=command-item]')].find((i) => i.textContent.trim() === text) ?? null;
  /** Opens the palette and picks the row that reads `text`, with the mouse. */
  async function pick(text) {
    await openPalette();
    expect(rowOf(text), `the row ${text}`).toBeTruthy();
    await clickOn(rowOf(text));
    expect(await closed(), `the palette closes on ${text}`).toBe(true);
    await settle();
  }

  it('of the page or a tab does what its row names: the arrangement, the tab and the dock it names, and no other', async () => {
    const page = await mountPage([['sine.sine'], ['sine.sine'], ['sine.sine']], 'rows', 1, ['sine.sine']);
    try {
      const [first, second] = page.ws.tabs;
      expect(page.ws.arrangement()).toBe('rows');
      await pick('Arrange as Columns');
      expect(page.ws.arrangement(), 'the arrangement named').toBe('columns');
      await pick('Hide the right dock');
      await openPalette();
      expect([!!rowOf('Show the right dock'), !!rowOf('Hide the left dock')], 'the right dock hidden, the left as it was').toEqual([true, true]);
      await pressKey('Escape');
      expect(await closed()).toBe(true);
      await pick('Show the right dock');
      await pick(`Go to ${second.name}`);
      expect(page.ws.activeId, 'the tab named').toBe(second.id);
      await pick(`Go to ${first.name}`);
      await pick('Duplicate this tab');
      expect(page.ws.tabs.map((t) => t.name), 'the open tab, copied after it').toEqual([first.name, `${first.name} copy`, second.name]);
      await pick('New layout tab');
      expect(page.ws.tabs.length, 'a tab more').toBe(4);
      expect(page.ws.tab, 'the new tab open, last').toBe(page.ws.tabs.at(-1));
    } finally {
      page.dispose();
    }
  }, 120_000);
});

describe('a device property in the command palette', () => {
  it('locks on Tab, writes the value typed on Enter to the checked devices with the edit bar’s notice, and Revert puts it back', async () => {
    const page = await mountPage([['sine.sine']], 'columns', 0, [], RPCS, { routes: ROUTES });
    try {
      await openPalette();
      await type('gai');
      expect(highlighted()?.dataset.property, 'the best match highlighted').toBe('gain');
      await shot('1-matching');
      await pressKey('Tab');
      await settle();
      expect(chip()).toBe('gain');
      expect(input().value, 'the field takes the argument').toBe('');
      expect(dialog().textContent, 'with no value, Enter reads').toContain('Read from 2 devices');
      await type('12');
      expect(dialog().textContent, 'the devices the write goes to').toContain('Write to 2 devices');
      await shot('2-locked-argument');
      expect(page.hub.calls, 'nothing written by typing or locking').toEqual([]);
      await pressKey('Enter');
      await settle();
      expect(page.hub.calls).toEqual([{ name: 'gain', value: '12', routes: ROUTES }]);
      expect(await closed(), 'the palette closes').toBe(true);
      const said = noticeText();
      expect(said).toContain('Set gain to 12 (was 5) · 2 devices');
      await shot('4-notice');
      await clickOn(button(notices()[0], 'Revert'));
      await settle();
      expect(page.hub.calls.slice(1)).toEqual([{ name: 'gain', value: '5', routes: ROUTES }]);
      // The edit bar's write of the same row says the same, its value apart.
      await clickOn([...document.querySelectorAll('tr[id^=prop-]')].find((tr) => tr.querySelector('.pname')?.firstChild.textContent.trim() === 'gain').querySelector('td:last-child'));
      await settle();
      await typeInto(document.querySelector('input[aria-label="New value of gain"]'), '12');
      await settle();
      await clickOn(button(document, 'Write to 2 devices'));
      await settle();
      expect(noticeText()).toBe(said);
    } finally {
      page.dispose();
    }
  });

  it('shows the edit bar’s reason for a value the property does not take, and writes nothing', async () => {
    const page = await mountPage([['sine.sine']], 'columns', 0, [], RPCS, { routes: ROUTES });
    try {
      await openPalette();
      await type('gain');
      await pressKey(' ');
      await settle();
      expect(chip()).toBe('gain');
      await type('300');
      const why = document.querySelector('.pal-problem')?.textContent;
      expect(why).toContain('300 is out of range: u8 takes 0 to 255');
      expect(input().getAttribute('aria-invalid')).toBe('true');
      await shot('3-invalid');
      await pressKey('Enter');
      await settle();
      expect(page.hub.calls).toEqual([]);
      expect(dialog(), 'the palette stays open').toBeTruthy();
      // Backspace three times empties the argument, a fourth unlocks.
      for (let i = 0; i < 4; i++) await pressKey('Backspace');
      await settle();
      expect(chip()).toBe(null);
      expect(input().value).toBe('gain');
      await pressKey('Escape');
      expect(await closed()).toBe(true);
      // The edit bar says the same for the same value.
      await clickOn([...document.querySelectorAll('tr[id^=prop-]')].find((tr) => tr.querySelector('.pname')?.firstChild.textContent.trim() === 'gain').querySelector('td:last-child'));
      await settle();
      await typeInto(document.querySelector('input[aria-label="New value of gain"]'), '300');
      await settle();
      expect(document.querySelector('.write-bar .why')?.textContent).toBe(why);
      expect(button(document, 'Write to 2 devices').disabled).toBe(true);
      await pressKey('Enter');
      await settle();
      expect(page.hub.calls).toEqual([]);
    } finally {
      page.dispose();
    }
  });

  it('reads a read-only property afresh on Enter and shows its value in place; a second Enter closes', async () => {
    const page = await mountPage([['sine.sine']], 'columns', 0, [], RPCS, { routes: ROUTES });
    const reads = [];
    page.hub.read = async (route, names) => (reads.push({ route, names }), { values: { temp: { state: 'ok', value: 42 } } });
    try {
      await openPalette();
      await type('temp');
      expect(highlighted()?.textContent).toContain('20');
      await pressKey('Enter');
      expect(await until(() => document.querySelector('.pal-read')?.textContent.includes('temp = 42'), 2000), 'the fresh value in place').toBe(true);
      expect(reads.map((r) => r.route).sort()).toEqual(ROUTES);
      expect(dialog(), 'the palette stays open').toBeTruthy();
      await pressKey('Enter');
      expect(await closed()).toBe(true);
      expect(page.hub.calls).toEqual([]);
    } finally {
      page.dispose();
    }
  });

  it('reads a setting afresh on Enter with nothing typed, highlighted or locked, and shows its value in place; a value typed then writes', async () => {
    const page = await mountPage([['sine.sine']], 'columns', 0, [], RPCS, { routes: ROUTES });
    const reads = [];
    page.hub.read = async (route, names) => (reads.push({ route, names }), { values: Object.fromEntries(names.map((n) => [n, { state: 'ok', value: n === 'enable' ? false : 42 }])) });
    try {
      await openPalette();
      await type('gain');
      await pressKey('Enter');
      expect(await until(() => document.querySelector('.pal-read')?.textContent.includes('gain = 42'), 2000), 'Enter on the setting highlighted reads it').toBe(true);
      expect(reads.map((r) => r.route).sort()).toEqual(ROUTES);
      expect(chip(), 'locked, to take a value').toBe('gain');
      await shot('5-setting-read');
      await type('12');
      expect(document.querySelector('.pal-read'), 'a value typed hides the read').toBe(null);
      await pressKey('Enter');
      await settle();
      expect(page.hub.calls).toEqual([{ name: 'gain', value: '12', routes: ROUTES }]);
      expect(await closed()).toBe(true);
      // Locked by Tab, Enter with nothing typed reads it too.
      page.hub.calls = [];
      reads.length = 0;
      await openPalette();
      await type('gain');
      await pressKey('Tab');
      await settle();
      expect(document.querySelector('.pal-read')).toBe(null);
      await pressKey('Enter');
      expect(await until(() => document.querySelector('.pal-read')?.textContent.includes('gain = 42'), 2000), 'Enter on the setting locked reads it').toBe(true);
      expect(reads.length).toBe(2);
      await pressKey('Enter');
      expect(await closed()).toBe(true);
      // A switch reads on Enter; Space then offers On and Off.
      await openPalette();
      await type('enable');
      await pressKey('Enter');
      expect(await until(() => document.querySelector('.pal-read')?.textContent.includes('enable = false'), 2000), 'Enter on a switch reads it').toBe(true);
      expect(dialog().querySelectorAll('[data-pick]').length, 'no On or Off while the read shows').toBe(0);
      await pressKey(' ');
      await settle();
      expect(document.querySelector('.pal-read'), 'Space turns to taking a value').toBe(null);
      expect([...dialog().querySelectorAll('[data-pick]')].map((i) => i.textContent.trim())).toEqual(['On2 devices', 'Off2 devices']);
      expect(page.hub.calls, 'nothing written by reading or locking').toEqual([]);
    } finally {
      page.dispose();
    }
  });

  it('runs an action only after a lock, and turns a switch to the value picked', async () => {
    const page = await mountPage([['sine.sine']], 'columns', 0, [], RPCS, { routes: ROUTES });
    try {
      await openPalette();
      await type('reboot');
      await pressKey('Enter');
      await settle();
      expect(page.hub.calls, 'Enter on an action not locked runs nothing').toEqual([]);
      expect(chip()).toBe('reboot');
      await pressKey('Enter');
      await settle();
      expect(page.hub.calls).toEqual([{ name: 'reboot', value: '', routes: ROUTES }]);
      expect(await closed()).toBe(true);
      expect(noticeText()).toContain('Ran reboot · 2 devices');
      page.hub.calls = [];
      await openPalette();
      await type('enable');
      await pressKey('Tab');
      await settle();
      expect([...dialog().querySelectorAll('[data-pick]')].map((i) => i.textContent.trim())).toEqual(['On2 devices', 'Off2 devices']);
      await pressKey('ArrowDown');
      await settle();
      await pressKey('Enter');
      await settle();
      expect(page.hub.calls).toEqual([{ name: 'enable', value: 'false', routes: ROUTES }]);
    } finally {
      page.dispose();
    }
  });

  it('marks ≠ where the devices differ, and draws only the best matches', async () => {
    const page = await mountPage([['sine.sine']], 'columns', 0, [], RPCS, { routes: ROUTES });
    try {
      await openPalette();
      await type('offs');
      expect(highlighted()?.dataset.property).toBe('offset');
      expect(highlighted().querySelector('.pal-value').textContent.trim()).toBe('≠');
      await pressKey('Control+a');
      await pressKey('Backspace');
      await type('fill');
      const items = document.querySelectorAll('[data-property]');
      expect(items.length).toBe(SHOWN);
      expect(dialog().textContent).toContain(`Properties · best ${SHOWN} of ${FILL.length}`);
      // fill.n1 comes before fill.n10: as early, as tight, shorter.
      await type('n1');
      expect([...document.querySelectorAll('[data-property]')].every((e) => e.dataset.property.startsWith('fill.n1')), 'only what matches is drawn').toBe(true);
      expect(highlighted()?.dataset.property).toBe('fill.n1');
    } finally {
      page.dispose();
    }
  });

  it('calls no device by any key but Enter on a locked property with a value it takes', async () => {
    const page = await mountPage([['sine.sine']], 'columns', 0, [], RPCS, { routes: ROUTES });
    const keys = ['g', 'a', 'r', 'e', '1', 'x', ' ', 'Tab', 'Backspace', 'Escape', 'ArrowDown', 'ArrowUp'];
    try {
      await fc.assert(
        fc.asyncProperty(fc.array(fc.constantFrom(...keys), { minLength: 1, maxLength: 12 }), async (seq) => {
          page.hub.calls = [];
          await openPalette();
          for (const k of seq) {
            if (!dialog()) break;
            await pressKey(k);
          }
          await settle();
          expect(page.hub.calls, seq.join(' ')).toEqual([]);
          for (let i = 0; i < 4 && dialog(); i++) await pressKey('Escape');
          expect(await closed(), 'Escape closes it at last').toBe(true);
        }),
        { examples: [[['g', 'a', 'Tab', '1', 'Escape', 'Escape']], [['r', 'e', ' ', ' ', 'Tab', 'ArrowDown']]] },
      );
    } finally {
      page.dispose();
    }
  }, 600_000);
});
