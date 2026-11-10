// The docks' panels are lists inside a list: a panel is as tall as what it
// shows while its dock has the room. Where the panels do not fit, a panel
// taller than a share of its dock's height gives way, down to that share,
// its list scrolling within it, and the dock scrolls the panels that still
// do not fit. So a panel past its share has only the room the others leave,
// and no list hides rows above empty dock. The subjects are found on the
// page: every dock, every panel in it and every list in a panel.
import fc from 'fast-check';
import { afterAll, describe, expect, it } from 'vitest';
import { page as browser, commands } from 'vitest/browser';
import { PANELS } from '../src/lib/docks.svelte.js';
import { centre, clickOn, mountPage, settle, wait, withExamples } from './harness/page.js';

const LIST = '[data-slot=scroll-area-viewport]';
const BAR = '[data-slot=scroll-area-scrollbar]';
const TITLES = Object.values(PANELS).map((p) => p.title);
const docks = () => [...document.querySelectorAll('.dock-side:not(.rail) > .dock')];
const titleOf = (panel) => panel.getAttribute('aria-label');
const panelsOf = (dock) => [...dock.querySelectorAll(':scope > .dock-panel')];
const openOf = (dock) => panelsOf(dock).filter((p) => p.classList.contains('open'));
const listsOf = (panel) => [...panel.querySelectorAll(LIST)];
const heightOf = (el) => el.getBoundingClientRect().height;
/** Whether the list of viewport `list` draws a scroll bar. */
const barOf = (list) => [...list.parentElement.querySelectorAll(`:scope > ${BAR}`)].some((bar) => heightOf(bar) > 0);
const rpcs = (n, channel, extra = {}) => Array.from({ length: n }, (_, i) => ({ name: `${channel ?? 'other'}.p${i}`, channel, value: i, ...extra }));
const signals = (n, from = 0) => Array.from({ length: n }, (_, i) => `s${from + i}.x`);
/** The RPC table of `counts` entries by channel, as `mountPage` takes it. */
const tableOf = ({ setting, report, action, other }) => [...rpcs(setting, 'setting'), ...rpcs(report, 'report', { access: ['read'] }), ...rpcs(action, 'action', { type: null, access: [] }), ...rpcs(other, null)];

/**
 * What of a panel's own decides its height, each kind counted apart, as an
 * empty list's notice is not as tall as a row.
 */
const OWN = { rows: 'tr, .chip, .device, [data-name]', notices: '.props-empty, .sig-empty, .empty', 'edit bars': '.write-bar', filters: 'input[aria-label^="Filter the properties"]' };

/** The share of its dock's height that `panel` gives way down to, as the stylesheet states it: '66.667%'. */
const shareOf = (panel) => panel.computedStyleMap().get('height').toString();
/** That share of the dock of `panel` in px. */
const sharePx = (panel) => (parseFloat(shareOf(panel)) / 100) * panel.parentElement.clientHeight;

/**
 * What each panel of every dock is, by title: its height in px, whether it
 * is open, whether it is past its share of the dock, in the room the
 * others leave, and what of its own decides its height, by OWN.
 */
const measured = () =>
  Object.fromEntries(
    docks().flatMap(panelsOf).map((panel) => [
      titleOf(panel),
      {
        height: heightOf(panel),
        open: panel.classList.contains('open'),
        grown: heightOf(panel) > sharePx(panel) + 1,
        own: Object.entries(OWN).map(([what, found]) => `${panel.querySelectorAll(found).length} ${what}`).join(', '),
      },
    ]),
  );

/**
 * Holds every dock, panel and list to the rule as the page stands; `said`
 * names the case. It counts in `seen` the docks and the lists it found
 * scrolling, and the panels past their share of a dock.
 */
function expectLists(said, seen) {
  const shares = new Set();
  for (const dock of docks()) {
    const room = dock.clientHeight;
    expect(getComputedStyle(dock).overflowY, `${said}: the dock scrolls what does not fit`).toMatch(/auto|scroll/);
    const all = panelsOf(dock);
    // Every panel is reached by scrolling the dock, and none lies over another.
    if (dock.scrollHeight > room) seen.docks++;
    dock.scrollTop = dock.scrollHeight;
    expect(all.at(-1).getBoundingClientRect().bottom, `${said}: the last panel's bottom, scrolled to`).toBeLessThanOrEqual(dock.getBoundingClientRect().bottom + 1);
    dock.scrollTop = 0;
    expect(all[0].getBoundingClientRect().top, `${said}: the first panel's top, scrolled to`).toBeGreaterThanOrEqual(dock.getBoundingClientRect().top - 1);
    all.forEach((panel, i) => i && expect(panel.getBoundingClientRect().top, `${said}: ${titleOf(panel)} starts below ${titleOf(all[i - 1])}`).toBeGreaterThanOrEqual(all[i - 1].getBoundingClientRect().bottom - 1));
    for (const panel of openOf(dock)) {
      const at = `${said}: ${titleOf(panel)}`;
      const share = shareOf(panel), height = heightOf(panel), grown = height > sharePx(panel) + 1;
      shares.add(share);
      expect(share, `${at} gives way down to a share of its dock's height`).toMatch(/%$/);
      // Past its share, a panel has only room the dock has: the dock does not scroll.
      if (grown) seen.grown++;
      expect(grown ? dock.scrollHeight - room : 0, `${at}, ${height} px of a dock of ${room} px: px the dock scrolls`).toBeLessThanOrEqual(1);
      // Nothing of the panel is cut off but the rows its lists scroll to.
      for (const box of [panel, panel.querySelector('.dock-body'), ...listsOf(panel).map((l) => l.parentElement)]) {
        expect(box.scrollHeight - box.clientHeight, `${at}: px cut off ${box.className.split(' ')[0] || box.dataset.slot}`).toBeLessThanOrEqual(1);
      }
      for (const list of listsOf(panel)) {
        const hidden = list.scrollHeight - list.clientHeight;
        expect(barOf(list), `${at}: a scroll bar, with ${hidden} px of rows out of view`).toBe(hidden > 0);
        if (hidden <= 0) continue;
        seen.lists++;
        expect(getComputedStyle(list).overflowY, `${at}: its list scrolls`).toMatch(/auto|scroll/);
        expect(height, `${at} hides rows only once it has given way to its share, ${sharePx(panel)} px`).toBeGreaterThanOrEqual(sharePx(panel) - 1);
      }
    }
    // The dock's room is used before a list hides rows.
    const empty = dock.getBoundingClientRect().top + room - all.at(-1).getBoundingClientRect().bottom;
    const cut = openOf(dock).filter((panel) => listsOf(panel).some((list) => list.scrollHeight > list.clientHeight));
    expect(empty > 1 ? cut.map(titleOf) : [], `${said}: the panels with rows out of view above ${empty} px of empty ${dock.getAttribute('aria-label')}`).toEqual([]);
  }
  expect([...shares].length, `${said}: one share for every panel, of ${[...shares]}`).toBeLessThanOrEqual(1);
}

afterAll(() => browser.viewport(1200, 900));

describe('a list in a dock', () => {
  it('whose rows fit does not scroll and draws no scroll bar', async () => {
    await browser.viewport(1200, 900);
    for (const routes of [['/'], ['/', '/0'], ['/', '/0', '/1']]) {
      const page = await mountPage([['sine.sine']], 'columns', 0, ['sine.sine', 'sine.cos'], [...rpcs(2, 'setting'), ...rpcs(2, 'report', { access: ['read'] })], { routes });
      try {
        await settle(6);
        const lists = docks().flatMap(openOf).flatMap((panel) => listsOf(panel).map((list) => [titleOf(panel), list]));
        expect(lists.map(([title]) => title)).toEqual(expect.arrayContaining(['Devices', 'Signals', 'Settings']));
        for (const [title, list] of lists) {
          // The pointer on a list, where a person sees its scroll bar.
          await commands.mouse([['move', ...centre(list)]]);
          await wait(100);
          await settle();
          const said = `${title} with ${routes.length} device${routes.length === 1 ? '' : 's'}`;
          expect(list.scrollHeight - list.clientHeight, `${said}: px of rows out of view`).toBe(0);
          expect(barOf(list), `${said}: a scroll bar`).toBe(false);
        }
      } finally {
        page.dispose();
      }
    }
  }, 120_000);
});

describe('a panel in a dock', () => {
  // A step folds or unfolds the panel titled `fold`, or gives `grow` more rows.
  const step = fc.oneof(
    fc.record({ fold: fc.constantFrom(...TITLES) }),
    fc.record({ grow: fc.constantFrom('signals', 'setting', 'report', 'action', 'other'), by: fc.integer({ min: 1, max: 12 }) }),
  );
  const cases = fc.record({
    height: fc.constantFrom(900, 760, 520, 340),
    rows: fc.record({ signals: fc.integer({ min: 1, max: 30 }), setting: fc.nat(14), report: fc.nat(14), action: fc.nat(10), other: fc.nat(8) }),
    steps: fc.array(step, { minLength: 1, maxLength: 6 }),
  });
  const EXAMPLES = [
    // Other unfolds below three panels that fill the dock.
    [{ height: 760, rows: { signals: 1, setting: 9, report: 11, action: 8, other: 6 }, steps: [{ fold: 'Other' }] }],
    // The signals outgrow the left dock, beside the devices.
    [{ height: 520, rows: { signals: 4, setting: 2, report: 2, action: 2, other: 0 }, steps: [{ grow: 'signals', by: 12 }, { grow: 'signals', by: 12 }, { fold: 'Devices' }, { fold: 'Devices' }] }],
    // Every panel open in a short window, then one above the rest gains rows.
    [{ height: 340, rows: { signals: 2, setting: 6, report: 6, action: 6, other: 6 }, steps: [{ fold: 'Diagnostics' }, { fold: 'Other' }, { grow: 'setting', by: 8 }, { fold: 'Settings' }] }],
    // A panel with nothing to show gets its first row, in place of its notice.
    [{ height: 900, rows: { signals: 1, setting: 0, report: 0, action: 0, other: 1 }, steps: [{ grow: 'action', by: 1 }, { grow: 'signals', by: 1 }] }],
    // The settings outgrow their share above shorter panels, one of which folds; then the reports outgrow theirs too.
    [{ height: 900, rows: { signals: 2, setting: 14, report: 2, action: 2, other: 0 }, steps: [{ grow: 'setting', by: 12 }, { grow: 'setting', by: 12 }, { fold: 'Reports' }, { fold: 'Reports' }, { grow: 'report', by: 12 }, { grow: 'report', by: 12 }, { grow: 'report', by: 12 }] }],
  ];

  it('keeps its height whatever the other panels do unless it is past its share, in the room they leave, scrolls its own list, and is reached by scrolling its dock', async () => {
    const seen = { docks: 0, lists: 0, grown: 0 };
    await fc.assert(
      fc.asyncProperty(cases, async ({ height, rows, steps }) => {
        await browser.viewport(1200, height);
        const counts = { ...rows };
        const page = await mountPage([['s0.x']], 'columns', 0, signals(counts.signals), tableOf(counts));
        const session = page.hub.session;
        try {
          await settle(6);
          expectLists('as mounted', seen);
          for (const [i, s] of steps.entries()) {
            const said = `after step ${i + 1}, ${JSON.stringify(s)}`;
            const before = measured();
            if (s.fold) await clickOn(document.querySelector(`.dock-panel[aria-label="${s.fold}"] .dock-title`));
            else if (s.grow === 'signals') {
              for (const key of signals(s.by, counts.signals)) session.text({ type: 'trace', trace: { id: session.traces.size, route: '/', stream: key.split('.')[0], column: 'x', units: 'V', rate: 100 } });
              counts.signals += s.by;
            } else {
              counts[s.grow] += s.by;
              session.text({ type: 'table', route: '/', rpcs: tableOf(counts).map(({ name, channel, type = 'f32', access = ['read', 'write'] }) => ({ name, channel, type, access, readable: access.includes('read') })) });
            }
            await settle(6);
            await wait(50);
            await settle();
            const after = measured();
            for (const [title, was] of Object.entries(before)) {
              const is = after[title];
              // A panel that the step left as it was: not the one folded, not one whose own rows changed,
              // and not one past its share, whose height is the room the others leave.
              if (title === s.fold || !was.open || !is.open || was.own !== is.own || was.grown || is.grown) continue;
              expect(is.height, `${said}: the height of ${title}, with ${is.own}`).toBe(was.height);
            }
            expectLists(said, seen);
          }
        } finally {
          page.dispose();
        }
      }),
      withExamples(EXAMPLES),
    );
    expect(seen.docks, 'docks seen scrolling').toBeGreaterThan(0);
    expect(seen.lists, 'lists seen scrolling').toBeGreaterThan(0);
    expect(seen.grown, 'panels seen past their share of a dock').toBeGreaterThan(0);
  }, 600_000);
});
