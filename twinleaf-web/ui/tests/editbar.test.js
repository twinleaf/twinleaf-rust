// The edit bar on several picked rows, on the whole page (round 2,
// Selection #8, #9 and #11): random picks across panels, of rows and
// action buttons of every kind, then a click or a key on the bar. Rows of
// different kinds show a short notice and write nothing. Several switches
// read "3 switches · Mixed"; On and Off set all of them, and Enter turns
// them all on, or all off when every one is on. Several values of one type
// write the typed value to each. Several actions are picked on their
// buttons with Ctrl or Shift, numbered in the order they run, and running
// them asks first, listing them. A press anywhere in the bar keeps the
// rows picked. Every control is pressed with the real mouse, and the field
// typed into from the keyboard. The devices are the stub hub's.
import { describe, expect, it } from 'vitest';
import { flushSync } from 'svelte';
import fc from 'fast-check';
import { clickOn, frames, mountPage, pressKey, typeInto, wait, withExamples } from './harness/page.js';
import { KEY as DOCKS_KEY, PANELS } from '../src/lib/docks.svelte.js';

const ROUTES = ['/0', '/1'];
const RPCS = [
  { name: 'gain', channel: 'setting', value: [1, 1] },
  { name: 'offset', channel: 'setting', value: [2, 3] },
  { name: 'level', channel: 'setting', type: 'u8', value: [4, 4] },
  { name: 'enable', channel: 'setting', type: 'bool', value: [true, false] },
  { name: 'sync', channel: 'setting', type: 'bool', value: [true, true] },
  { name: 'temp', channel: 'report', access: ['read'], value: [20, 21] },
  { name: 'reboot', channel: 'action', type: null, access: [] },
  { name: 'save', channel: 'action', type: null, access: [] },
  { name: 'zero', channel: 'action', type: null, access: [] },
  { name: 'arm', channel: null, type: 'bool', value: [false, false] },
  { name: 'mode', channel: null, type: 'u8', value: [4, 5] },
];
const DOCKS = {
  left: [{ id: 'devices', open: true }, { id: 'settings', open: true }],
  right: [{ id: 'favorites', open: true }, { id: 'reports', open: true }, { id: 'actions', open: true }, { id: 'other', open: true }],
  hidden: { left: false, right: false },
};

/** What the edit bar makes of a row, from its declaration alone: action, toggle, a value of its type, or display. */
function kindOf(name) {
  const r = RPCS.find((p) => p.name === name);
  const access = r.access ?? ['read', 'write'], type = r.type === undefined ? 'f32' : r.type;
  if (r.channel === 'action') return 'action';
  if (!access.includes('write')) return 'display';
  return type === 'bool' ? 'toggle' : `value:${type}`;
}

const panelOf = (el) => Object.entries(PANELS).find(([, p]) => p.title === el.closest('.dock-panel')?.getAttribute('aria-label'))?.[0];
/** Every row and action button the page shows, {key, el}, in page order. */
const shown = () =>
  [...document.querySelectorAll('.dock-panel tr[id^=prop-], .dock-panel .action-grid button[data-name]')].map((el) => ({
    key: `${panelOf(el)}:${el.dataset.name ?? el.querySelector('.pname').firstChild.textContent.trim()}`,
    el,
  }));
const bars = () => [...document.querySelectorAll('.write-bar')];
const button = (scope, text) => [...scope.querySelectorAll('button')].find((b) => b.textContent.trim() === text);
const notices = () => [...document.querySelectorAll('[data-sonner-toast]')].filter((t) => t.dataset.removed !== 'true').map((t) => t.textContent);
async function settle() {
  flushSync();
  await wait(10);
  await frames(2);
}
/** Waits until no question is open, as one closing still covers the page and takes its clicks. */
async function noQuestion() {
  for (let i = 0; i < 120 && document.querySelector('[role=alertdialog]'); i++) await frames();
  expect(document.querySelector('[role=alertdialog]'), 'the question closed').toBe(null);
}
/** Where a click on a row lands: its last cell, or the action button itself. */
const target = (el) => (el.matches('tr') ? el.querySelector('td:last-child') : el);

describe('the edit bar on the rows picked', () => {
  it('edits several rows of one kind together, asks before running several actions, and on rows of several kinds edits nothing', async () => {
    const page = await mountPage([['sine.sine']], 'columns', 0, [], RPCS, { routes: ROUTES, saved: { [DOCKS_KEY]: DOCKS } });
    page.history.group('Pin', () => page.history.favorites.pin(['save', 'mode'], true));
    const value = (route, name) => page.hub.session.devices.get(route).values.get(name);
    const seen = { kinds: 0, switches: 0, values: 0, actions: 0, asked: 0, cancelled: 0, enter: 0, mixed: 0 };
    try {
      await settle();
      await fc.assert(
        fc.asyncProperty(fc.array(fc.nat(), { minLength: 2, maxLength: 4 }), fc.constantFrom('click', 'key'), fc.boolean(), fc.nat(), async (at, how, yes, typed) => {
          const open = document.querySelector('[role=alertdialog]');
          if (open) await clickOn(button(open, 'Cancel'));
          await noQuestion();
          page.ws.selection.clear();
          await settle();
          const order = shown();
          for (const i of at) await clickOn(target(order[i % order.length].el), { ctrl: true });
          await settle();
          const keys = [...page.ws.selection.rows];
          const names = [...new Set(keys.map((k) => k.slice(k.indexOf(':') + 1)))];
          const kinds = [...new Set(names.map(kindOf))];
          const shownBars = bars();
          page.hub.calls = [];
          if (names.length < 2) return;
          expect(shownBars.length, 'one edit bar, under the panel picked in last').toBe(1);
          const bar = shownBars[0];
          // The panel of the row clicked last, which Ctrl may have just dropped.
          expect(panelOf(bar)).toBe(order[at.at(-1) % order.length].key.split(':')[0]);
          const grid = bar.closest('.dock-panel').querySelector('[role=grid]');
          if (kinds.length > 1) {
            seen.kinds++;
            expect(bar.textContent).toContain('Pick one kind to edit');
            expect(bar.querySelectorAll('button, input').length).toBe(0);
            if (grid) {
              grid.focus();
              await pressKey('Enter');
              await settle();
              expect(notices().some((t) => t.includes('Pick one kind to edit'))).toBe(true);
            }
            expect(page.hub.calls).toEqual([]);
            return;
          }
          const kind = kinds[0];
          if (kind === 'toggle') {
            seen.switches++;
            const all = names.flatMap((n) => ROUTES.map((r) => value(r, n)));
            const state = all.every((v) => v === true) ? 'On' : all.every((v) => v === false) ? 'Off' : 'Mixed';
            if (state === 'Mixed') seen.mixed++;
            expect(bar.textContent).toContain(`${names.length} switches · ${state}`);
            let to;
            if (how === 'key' && grid) {
              seen.enter++;
              grid.focus();
              await pressKey('Enter');
              to = state === 'On' ? 'false' : 'true';
            } else {
              to = yes ? 'true' : 'false';
              await clickOn(button(bar, yes ? 'On' : 'Off'));
            }
            await settle();
            expect(page.hub.calls).toEqual(names.map((name) => ({ name, value: to, routes: ROUTES })));
            expect([...page.ws.selection.rows], 'a press in the bar keeps the rows picked').toEqual(keys);
            for (const n of names) for (const r of ROUTES) expect(value(r, n)).toBe(to === 'true');
          } else if (kind.startsWith('value:')) {
            seen.values++;
            const type = kind.slice(6);
            expect(bar.textContent).toContain(`${names.length} values (${type})`);
            const input = bar.querySelector('input');
            const all = names.flatMap((n) => ROUTES.map((r) => value(r, n)));
            const same = all.every((v) => v === all[0]);
            expect(input.value).toBe(same ? String(all[0]) : '');
            if (!same) expect(input.placeholder).toBe('— mixed');
            const write = [...bar.querySelectorAll('button')].find((b) => b.textContent.startsWith('Write'));
            await typeInto(input, '');
            await settle();
            expect(write.disabled, 'Write waits for a value').toBe(true);
            const text = String(10 + (typed % 5));
            await typeInto(input, text);
            await settle();
            await clickOn(write);
            await settle();
            expect(page.hub.calls).toEqual(names.map((name) => ({ name, value: text, routes: ROUTES })));
            expect([...page.ws.selection.rows], 'a press in the bar keeps the rows picked').toEqual(keys);
          } else if (kind === 'action') {
            seen.actions++;
            expect(bar.textContent).toContain(`${names.length} actions`);
            // The buttons picked show the order they run in.
            for (const [i, n] of names.entries()) {
              const pill = document.querySelector(`.action-grid button[data-name="${n}"]`);
              if (pill) expect(pill.querySelector('.run-order')?.textContent).toBe(String(i + 1));
            }
            if (how === 'key' && grid) {
              grid.focus();
              await pressKey('Enter');
            } else await clickOn(button(bar, `Run ${names.length}…`));
            await settle();
            const dialog = document.querySelector('[role=alertdialog]');
            expect(dialog, 'running several actions asks first').toBeTruthy();
            expect(dialog.textContent).toContain(`Run ${names.length} actions?`);
            expect(dialog.textContent).toContain(`${names.join(', then ')} on 2 devices.`);
            expect(page.hub.calls).toEqual([]);
            seen.asked++;
            await clickOn(button(dialog, yes ? `Run ${names.length}` : 'Cancel'));
            await settle();
            await noQuestion();
            expect([...page.ws.selection.rows], 'the question keeps the rows picked').toEqual(keys);
            if (yes) expect(page.hub.calls).toEqual(names.map((name) => ({ name, value: '', routes: ROUTES })));
            else (seen.cancelled++, expect(page.hub.calls).toEqual([]));
          }
        }),
          // Each kind, whatever the seed: the rows run settings (gain, offset, level, enable, sync), favorites (save, mode), reports, actions and other.
          withExamples([
            [[0, 3], 'click', true, 0],
            [[3, 4], 'key', true, 0],
            [[0, 1], 'click', true, 1],
            [[5, 9, 10], 'click', true, 0],
            [[9, 10], 'click', false, 0],
            [[2, 6], 'key', true, 0],
            [[8, 5], 'key', true, 0],
          ]),
      );
      for (const [what, n] of Object.entries(seen)) expect(n, what).toBeGreaterThan(0);
    } finally {
      page.dispose();
    }
  }, 600_000);

  it('runs one action at its button at once, with no question', async () => {
    const page = await mountPage([['sine.sine']], 'columns', 0, [], RPCS, { routes: ROUTES, saved: { [DOCKS_KEY]: DOCKS } });
    try {
      await settle();
      const pill = document.querySelector('.action-grid button[data-name="save"]');
      await clickOn(pill);
      await settle();
      expect(page.hub.calls).toEqual([{ name: 'save', value: '', routes: ROUTES }]);
      expect(document.querySelector('[role=alertdialog]')).toBe(null);
      expect([...page.ws.selection.rows]).toEqual(['actions:save']);
    } finally {
      page.dispose();
    }
  });

  it('turns a switch the other way from what it shows on every Enter, a reply with no value or not', async () => {
    // A switch no announcement keeps, so it shows what was written to it.
    const rpcs = [{ name: 'latch', channel: 'setting', type: 'bool', announced: false }];
    const seen = { bare: 0, valued: 0, on: 0, off: 0 };
    await fc.assert(
      fc.asyncProperty(fc.array(fc.boolean(), { minLength: 1, maxLength: 6 }), async (replies) => {
        const page = await mountPage([['sine.sine']], 'columns', 0, [], rpcs, { routes: ROUTES, saved: { [DOCKS_KEY]: DOCKS } });
        try {
          await settle();
          const tr = document.querySelector('tr[id^=prop-]');
          await clickOn(target(tr));
          await settle();
          let shows = null;
          for (const bare of replies) {
            page.hub.answer = (name, value) => ({ state: 'ok', value: bare ? null : value === 'true' });
            page.hub.calls = [];
            tr.closest('[role=grid]').focus();
            await pressKey('Enter');
            await settle();
            const to = shows === true ? 'false' : 'true';
            expect(page.hub.calls).toEqual([{ name: 'latch', value: to, routes: ROUTES }]);
            shows = to === 'true';
            seen[bare ? 'bare' : 'valued']++;
            seen[shows ? 'on' : 'off']++;
          }
        } finally {
          page.dispose();
        }
      }),
      withExamples([[[true, true, true]], [[false, true, false]]]),
    );
    for (const [what, n] of Object.entries(seen)) expect(n, what).toBeGreaterThan(0);
  }, 600_000);

  it('does nothing on Enter in a table under which no bar shows', async () => {
    const page = await mountPage([['sine.sine']], 'columns', 0, [], RPCS, { routes: ROUTES, saved: { [DOCKS_KEY]: DOCKS } });
    try {
      await settle();
      const gain = shown().find((r) => r.key === 'settings:gain').el;
      await clickOn(target(gain));
      await settle();
      page.hub.calls = [];
      // The Other panel's table, whose first row is a switch, focused with no row picked there.
      const other = shown().find((r) => r.key === 'other:arm').el.closest('[role=grid]');
      other.focus();
      await pressKey('Enter');
      await settle();
      expect(page.hub.calls).toEqual([]);
      expect(bars().map(panelOf)).toEqual(['settings']);
    } finally {
      page.dispose();
    }
  });
});
