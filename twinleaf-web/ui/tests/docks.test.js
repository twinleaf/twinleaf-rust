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
import { SCROLL_REST_MS } from '../src/lib/scrolling.js';
import { centre, clickOn, glide, mountPage, pickItem, pressKey, rightClick, rested, settle, typeInto, until, wait, watchFrames, withExamples } from './harness/page.js';
import { lastKey } from '../src/lib/keyrouter.js';

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
const OWN = { rows: 'tr, .chip, .device, [data-name]', notices: '.props-empty, .sig-empty, .empty', 'edit bars': '.write-bar' };

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

// A dock has one button, which hides it and shows it again, at the same
// height whether the dock is shown or hidden; shown, at the dock's top
// right, the left dock's inner corner beside the tiles and the right dock's
// at the page's edge; hidden, on the rail. Shown, it is in the dock's top
// row beside the filter of its properties, its panels scrolling beneath;
// in a dock with no such filter there is no top row, and it shares the
// first panel header's row, clear of that header, whichever panel is
// first, open or folded. Each is one place however the dock is scrolled,
// its panels folded or moved. So a click at its place hides the dock, the
// next one at its place shows it, and nothing of the dock is ever under
// the button.
describe('the button that hides and shows a dock', () => {
  const SIDES = ['left', 'right'];
  const FIELD = 'input[aria-label^="Filter the properties"]';
  const frameOf = (side) => document.querySelector(`.dock-frame.${side}`);
  const buttonsOf = (side) => [...frameOf(side).querySelectorAll('button')].filter((b) => /^(Hide|Show) the \w+ dock$/.test(b.getAttribute('aria-label') ?? ''));
  /** Where the button of dock `side` is, in px, as text; or how many buttons the dock has, when not one. */
  const placeOf = (side) => {
    const all = buttonsOf(side);
    if (all.length !== 1) return `${all.length} buttons`;
    const { left, top, width, height } = all[0].getBoundingClientRect();
    return `${width} by ${height} px at ${left}, ${top}`;
  };
  const railed = (side) => frameOf(side).classList.contains('rail');
  /** Where VITE_TL_SHOTS names a folder, the page is saved there as `name`. */
  const SHOTS = import.meta.env.VITE_TL_SHOTS;
  const shot = async (name) => SHOTS && (await browser.screenshot({ path: `${SHOTS}/${name.replace(/[^\w-]+/g, '-')}.png` }));
  /** Whether dock `side` is at rest, as its rail when `hidden` and with its panels when not. */
  const rests = (side, hidden) => railed(side) === hidden && !document.querySelector('.layout.sliding') && !frameOf(side).matches('.opening, .closing');
  /**
   * What of dock `side`'s panels shows under its button: each element in
   * them the pointer hits at points across the button's box, the button out
   * of the way, so what the dock cuts away is not counted. By its panel and
   * its text.
   */
  function covered(side) {
    const button = buttonsOf(side)[0], b = button.getBoundingClientRect(), dock = frameOf(side).querySelector('.dock');
    const hits = new Set();
    button.style.visibility = 'hidden';
    try {
      for (let x = b.left + 0.5; x < b.right; x += 2)
        for (let y = b.top + 0.5; y < b.bottom; y += 2) {
          const el = document.elementFromPoint(x, y);
          if (el && el !== dock && dock.contains(el)) hits.add(`${titleOf(el.closest('.dock-panel'))}: ${el.tagName.toLowerCase()} "${el.textContent.trim().slice(0, 20)}"`);
        }
    } finally {
      button.style.visibility = '';
    }
    return [...hits];
  }
  /** The first panel's header of dock `side`. */
  const firstHead = (side) => frameOf(side).querySelector('.dock > .dock-panel .dock-head');
  /**
   * Holds the button of dock `side`, shown and its dock scrolled to its top,
   * to its row: with a filter, in the top row, the field ending short of it
   * and the panels below both; without, no top row, and the first panel's
   * header in its row, ending short of it, its grip too. Notes its place.
   */
  function inItsRow(side, said, note) {
    const frame = frameOf(side), dock = frame.querySelector('.dock'), field = frame.querySelector(FIELD);
    dock.scrollTop = 0;
    const b = buttonsOf(side)[0].getBoundingClientRect(), f = frame.getBoundingClientRect();
    expect(f.right - b.right, `${said}: px from its frame's right edge to its button`).toBeLessThanOrEqual(8);
    expect(!!frame.querySelector('.dock-top'), `${said}: a top row`).toBe(!!field);
    if (field) {
      const r = field.getBoundingClientRect();
      expect(b.bottom, `${said}: its button's bottom, above its panels`).toBeLessThanOrEqual(dock.getBoundingClientRect().top);
      expect(b.left - r.right, `${said}: px between its filter's field and its button`).toBeGreaterThanOrEqual(2);
      expect(r.top < b.bottom && r.bottom > b.top, `${said}: its filter's field and its button share a row`).toBe(true);
    } else {
      const head = firstHead(side), h = head.getBoundingClientRect(), grip = head.querySelector('.dock-grip').getBoundingClientRect();
      expect(Math.abs((h.top + h.bottom) / 2 - (b.top + b.bottom) / 2), `${said}: px between the middles of its button and its first header, ${titleOf(head.closest('.dock-panel'))}`).toBeLessThanOrEqual(1);
      expect(b.left - Math.max(h.right, grip.right), `${said}: px between its first header and its button`).toBeGreaterThanOrEqual(2);
    }
    expect(covered(side), `${said}: what is under its button`).toEqual([]);
    note('shown', said);
  }
  const all = (ids) => ids.map((id) => ({ id, open: true }));
  /** The arrangements mounted, by what they are: null the page's own, and one with the properties in the left dock. */
  const ARRANGED = {
    'the properties in the right dock': null,
    'the properties in the left dock': { left: all(['favorites', 'settings', 'reports', 'actions', 'diagnostics', 'other']), right: all(['devices', 'signals']) },
  };

  it('is at one height, shown or hidden; shown, at the top right, in the top row with a filter before it, or in the first header’s row with none; hidden, on the rail; at one place in each, however its dock is scrolled, folded or rearranged; and clicks at its place hide, show and hide the dock', async () => {
    await browser.viewport(1200, 520);
    // Where each dock's button was, shown and hidden, each place with where it was first seen; and its heights, every frame as it slid too.
    const places = { left: { shown: new Map(), hidden: new Map() }, right: { shown: new Map(), hidden: new Map() } };
    const heights = { left: new Map(), right: new Map() }, filtered = { left: new Set(), right: new Set() };
    const topOf = (side) => `${buttonsOf(side)[0]?.getBoundingClientRect().top} px`;
    let scrolled = 0;
    for (const [arranged, saved] of Object.entries(ARRANGED)) {
      const page = await mountPage([['s0.x']], 'columns', 0, signals(30), tableOf({ setting: 14, report: 14, action: 10, other: 4 }), { routes: ['/', '/0', '/1', '/2', '/3', '/4'], saved: saved ? { 'tl-docks-v1': saved } : {} });
      try {
        await settle(6);
        const once = (map, at, where) => map.has(at) || map.set(at, `${arranged}, ${where}`);
        const noteOf = (side) => (state, where) => (once(places[side][state], placeOf(side), where), once(heights[side], topOf(side), `${state}, ${where}`));
        if (!saved) {
          // A panel of properties moved into the left dock gives it a top row, and leaves it again: neither button moves.
          for (const [title, to] of [['Settings', 'left'], ['Settings', 'right']]) {
            const head = document.querySelector(`.dock-panel[aria-label=${title}] .dock-title`);
            head.scrollIntoView({ block: 'nearest' });
            await rightClick(centre(head));
            await pickItem(`Move to the ${to} dock`);
            await settle(6);
            expect(document.querySelector(`.dock-frame.${to} .dock-panel[aria-label=${title}]`), `${title} moved to the ${to} dock`).toBeTruthy();
            for (const side of SIDES) inItsRow(side, `the ${side} dock, ${arranged}, ${title} moved to the ${to}`, noteOf(side));
          }
        }
        for (const side of SIDES) {
          const said = `the ${side} dock, ${arranged}`, frame = frameOf(side), dock = frame.querySelector('.dock');
          const note = noteOf(side);
          filtered[side].add(!!frame.querySelector(FIELD));
          const button = buttonsOf(side)[0];
          expect(buttonsOf(side).length, `${said}: its buttons that hide or show it`).toBe(1);
          inItsRow(side, said, note);
          await shot(`${side}, ${arranged}`);
          // The first panel folded, and unfolded again.
          const first = () => frame.querySelector('.dock > .dock-panel');
          const was = titleOf(first());
          await clickOn(first().querySelector('.dock-title'));
          await settle(6);
          expect(first().classList.contains('open'), `${said}: ${was} folded`).toBe(false);
          inItsRow(side, `${said}, ${was} folded`, note);
          await shot(`${side}, ${arranged}, ${was} folded`);
          await clickOn(first().querySelector('.dock-title'));
          await settle(6);
          // Another panel moved to the top, by its menu.
          const second = frame.querySelectorAll('.dock > .dock-panel')[1];
          await rightClick(centre(second.querySelector('.dock-title')));
          await pickItem('Move up');
          await settle(6);
          await until(() => !document.querySelector('[data-slot=context-menu-content]'));
          expect(titleOf(first()), `${said}: the panel moved up is first`).toBe(titleOf(second));
          inItsRow(side, `${said}, ${titleOf(second)} moved to the top`, note);
          await shot(`${side}, ${arranged}, ${titleOf(second)} at the top`);
          const most = dock.scrollHeight - dock.clientHeight;
          if (most > 0) scrolled++;
          for (let top = 0; top <= most; top += 7) {
            dock.scrollTop = top;
            note('shown', `scrolled ${top} px of ${most}`);
            expect(covered(side), `${said}, scrolled ${top} px of ${most}: what is under its button`).toEqual([]);
            expect(button.contains(document.elementFromPoint(...centre(button))), `${said}, scrolled ${top} px of ${most}: its button is what the pointer hits`).toBe(true);
          }
          dock.scrollTop = most / 2;
          await shot(`${side}, ${arranged}, scrolled`);
          // Four clicks, each at the button's place, which leave the dock shown.
          const stop = watchFrames(() => topOf(side));
          for (const [n, hidden] of [true, false, true, false].entries()) {
            await commands.mouse([['move', ...centre(buttonsOf(side)[0])], ['down'], ['up']]);
            expect(await until(() => rests(side, hidden)), `${said}: ${hidden ? 'hidden' : 'shown'} after click ${n + 1} at its place`).toBe(true);
            expect(await rested(() => frameOf(side).getBoundingClientRect().width), `${said}: its frame comes to rest`).toBe(true);
            expect(buttonsOf(side).map((el) => el.getAttribute('aria-label')), `${said}, ${hidden ? 'hidden' : 'shown'}: what its button says it does`).toEqual([`${hidden ? 'Show' : 'Hide'} the ${side} dock`]);
            if (hidden) {
              const on = buttonsOf(side)[0].getBoundingClientRect(), rail = frameOf(side).getBoundingClientRect();
              expect([on.left >= rail.left, on.right <= rail.right], `${said}, hidden: its button within the rail`).toEqual([true, true]);
            }
            note(hidden ? 'hidden' : 'shown', hidden ? 'hidden' : 'shown again');
          }
          for (const at of stop()) once(heights[side], at, 'at some frame as it hid or showed');
        }
      } finally {
        page.dispose();
      }
    }
    expect(scrolled, 'docks seen scrolling').toBeGreaterThan(0);
    const listed = (map) => [...map].map(([at, where]) => `${at} (${where})`);
    for (const side of SIDES) {
      expect([...filtered[side]].sort(), `the ${side} dock was seen with a filter and without one`).toEqual([false, true]);
      for (const state of ['shown', 'hidden']) {
        const were = listed(places[side][state]);
        expect(were, `where the ${side} dock's button was, ${state}`).toEqual(were.slice(0, 1));
      }
      const were = listed(heights[side]);
      expect(were, `the heights the ${side} dock's button was at`).toEqual(were.slice(0, 1));
    }
  }, 240_000);
});

describe("the filter of a dock's properties", () => {
  const FIELD = 'input[aria-label^="Filter the properties"]';
  const PROPERTIES = Object.values(PANELS).filter((p) => p.channels).map((p) => p.title);
  const fieldsOf = (dock) => [...dock.parentElement.querySelectorAll(FIELD)];
  const placeOf = (el) => (({ left, top, width, height }) => ({ left, top, width, height }))(el.getBoundingClientRect());
  const panel = (title) => document.querySelector(`.dock-panel[aria-label="${title}"]`);
  const namesIn = (title) => [...panel(title).querySelectorAll('tr[id^=prop-] .pname, [data-name]')].map((el) => el.textContent.trim());

  it('is one field in each dock with a panel of properties, above its panels, where it stays as they fold, and filters every one', async () => {
    await browser.viewport(1200, 900);
    const page = await mountPage([['s0.x']], 'columns', 0, signals(3), tableOf({ setting: 4, report: 4, action: 3, other: 2 }));
    try {
      await settle(6);
      const [left, right] = docks();
      expect(docks().map((dock) => fieldsOf(dock).length), 'fields in the left dock, of no properties, and the right').toEqual([0, 1]);
      const field = fieldsOf(right)[0], at = placeOf(field);
      expect(right.contains(field), 'the field is among the panels, which scroll').toBe(false);
      expect(field.getBoundingClientRect().bottom, "the field's bottom, above the panels").toBeLessThanOrEqual(right.getBoundingClientRect().top);
      // Every panel of properties folds in turn, the open ones first, until none is open.
      const open = () => PROPERTIES.filter((title) => panel(title).classList.contains('open'));
      for (let title; (title = open()[0]); ) {
        await clickOn(panel(title).querySelector('.dock-title'));
        await settle(6);
        expect(fieldsOf(right), `with ${title} folded: the field`).toEqual([field]);
        expect(placeOf(field), `with ${title} folded: where the field is`).toEqual(at);
      }
      for (const title of ['Settings', 'Reports', 'Actions']) await clickOn(panel(title).querySelector('.dock-title'));
      await settle(6);
      expect(placeOf(field), 'with three panels unfolded: where the field is').toEqual(at);
      await typeInto(field, 'p1');
      await settle(6);
      expect(['Settings', 'Reports', 'Actions'].map(namesIn), 'the rows of each panel, filtered by p1').toEqual([['setting.p1'], ['report.p1'], ['action.p1']]);
      // A panel of properties moved to the other dock brings that dock a field of the same filter.
      await rightClick(centre(panel('Reports').querySelector('.dock-title')));
      await pickItem('Move to the left dock');
      await settle(6);
      expect(fieldsOf(left).map((f) => f.value), 'what the left dock filters by, holding Reports').toEqual(['p1']);
      expect(placeOf(field), 'with Reports in the other dock: where the field is').toEqual(at);
    } finally {
      page.dispose();
    }
  }, 60_000);
});

// What a person presses or reads in a dock, its top row too, found on the
// page: every control, every run of text, every scroll bar's thumb, and the focus ring
// around a header. No two of them overlap, however tall the window and
// wherever the dock is scrolled, and every scroll bar has one look at rest.
// A dock's scroll bars, its own and its lists', are faint at rest, where
// each still shows that there is more, and plain in use: with the pointer
// over the dock, while it or a list in it scrolls, and with the keys' focus
// in it. Only the colour changes, so nothing moves.
describe('what a dock shows', () => {
  const PRESSED = 'button, input, a[href], [role=button], [role=checkbox], [role=switch]';
  const THUMB = '[data-slot=scroll-area-thumb]';
  /** Box {l, t, r, b} of DOMRect `r`. */
  const box = (r) => ({ l: r.left, t: r.top, r: r.right, b: r.bottom });
  const meet = (a, b) => ({ l: Math.max(a.l, b.l), t: Math.max(a.t, b.t), r: Math.min(a.r, b.r), b: Math.min(a.b, b.b) });
  /** Whether box `a` has more than half a px each way. */
  const some = (a) => a.r - a.l > 0.5 && a.b - a.t > 0.5;
  /** The box inside `el` that its contents show in: its padding box, without its own scroll bar. */
  const inner = (el) => {
    const r = el.getBoundingClientRect();
    if (!el.clientWidth && !el.clientHeight) return box(r);
    return { l: r.left + el.clientLeft, t: r.top + el.clientTop, r: r.left + el.clientLeft + el.clientWidth, b: r.top + el.clientTop + el.clientHeight };
  };
  /**
   * What shows of `at`, the box of something in or of `el`: cut to every
   * box from `el` up to `dock` that clips or scrolls, and to below the
   * header of its own table where that header stays put as the rows scroll.
   */
  function shown(el, at, dock) {
    let out = at;
    for (let a = el; a && a !== dock.parentElement; a = a.parentElement) {
      const cs = getComputedStyle(a);
      if (cs.overflowX !== 'visible' || cs.overflowY !== 'visible') out = meet(out, inner(a));
    }
    const head = el.closest('table')?.querySelector('thead');
    if (head && !head.contains(el) && getComputedStyle(head.querySelector('th')).position === 'sticky') out = meet(out, { ...out, t: head.getBoundingClientRect().bottom });
    return out;
  }
  /** `dock`'s own scroll bar, which the browser draws: its `look`, as `lookOf` gives a list's, and the box of its thumb, `at`; null while the dock does not scroll. */
  function ownBar(dock) {
    const lane = dock.offsetWidth - dock.clientWidth - dock.clientLeft * 2, most = dock.scrollHeight - dock.clientHeight;
    if (most <= 0 || lane <= 0) return null;
    const cs = getComputedStyle(dock, '::-webkit-scrollbar-thumb'), px = (v) => parseFloat(v) || 0;
    // The bar's track runs the dock's height less the margins its styling gives it.
    const track = getComputedStyle(dock, '::-webkit-scrollbar-track'), span = dock.clientHeight - px(track.marginTop) - px(track.marginBottom);
    const r = dock.getBoundingClientRect(), left = r.left + dock.clientLeft + dock.clientWidth, top = r.top + dock.clientTop + px(track.marginTop);
    const width = lane - px(cs.borderLeftWidth) - px(cs.borderRightWidth);
    // The browser draws the thumb as styled only while the dock leaves the bar's width and colour to that styling.
    const own = getComputedStyle(dock), styled = own.scrollbarWidth === 'auto' && own.scrollbarColor === 'auto';
    if (!styled) return { look: { drawn: `as the browser's own, ${own.scrollbarWidth} and ${own.scrollbarColor}` }, at: { l: left, t: top, r: left + lane, b: top + span } };
    const length = Math.max((dock.clientHeight / dock.scrollHeight) * span, lane), y = top + (dock.scrollTop / most) * (span - length);
    return {
      look: { lane, width, inset: px(cs.borderLeftWidth), colour: cs.backgroundColor, round: px(cs.borderTopLeftRadius) >= width / 2 },
      at: { l: left + px(cs.borderLeftWidth), t: y + px(cs.borderTopWidth), r: left + lane - px(cs.borderRightWidth), b: y + length - px(cs.borderBottomWidth) },
    };
  }
  /** The look of the scroll bar whose thumb is element `thumb`: its lane's and its thumb's widths in px, how far the thumb is in from the lane's left, its colour, and whether its ends are round. */
  function lookOf(thumb) {
    const bar = thumb.parentElement.getBoundingClientRect(), t = thumb.getBoundingClientRect(), cs = getComputedStyle(thumb);
    return { lane: bar.width, width: t.width, inset: t.left - bar.left, colour: cs.backgroundColor, round: parseFloat(cs.borderTopLeftRadius) >= t.width / 2 };
  }
  /** Everything pressed or read in `dock` and in the top row above it that shows, each {what, el, at}. */
  function things(dock) {
    const out = [], whole = dock.parentElement;
    const add = (what, el, at) => some(at) && out.push({ what, el, at });
    for (const el of whole.querySelectorAll(PRESSED)) add(`${el.tagName.toLowerCase()} "${(el.getAttribute('aria-label') ?? el.textContent).trim().slice(0, 30)}"`, el, shown(el, box(el.getBoundingClientRect()), dock));
    const walk = document.createTreeWalker(whole, NodeFilter.SHOW_TEXT);
    for (let node; (node = walk.nextNode()); ) {
      if (!node.data.trim() || getComputedStyle(node.parentElement).visibility === 'hidden') continue;
      const range = document.createRange();
      range.selectNodeContents(node);
      add(`text "${node.data.trim().slice(0, 30)}"`, node.parentElement, shown(node.parentElement, box(range.getBoundingClientRect()), dock));
    }
    for (const thumb of dock.querySelectorAll(THUMB)) add(`the scroll bar of ${titleOf(thumb.closest('.dock-panel'))}`, thumb, shown(thumb, box(thumb.getBoundingClientRect()), dock));
    const own = ownBar(dock);
    if (own) add(`the scroll bar of the ${dock.getAttribute('aria-label')}`, null, own.at);
    return out;
  }
  const related = (a, b) => !!a.el && !!b.el && (a.el.contains(b.el) || b.el.contains(a.el));
  /**
   * Whether thing `t` shows at point `x`, `y`: a thing in a dock that cuts
   * part of itself away, as one with no top row cuts its button's corner,
   * only where the browser finds it; anything else wherever its box is.
   */
  const showsAt = (t, x, y) => {
    const dock = t.el?.closest('.dock');
    if (!dock || getComputedStyle(dock).clipPath === 'none') return true;
    return document.elementsFromPoint(x, y).some((e) => e === t.el || t.el.contains(e));
  };
  /** Whether things `a` and `b` overlap where both show: at some point of a grid over where their boxes meet. */
  function overlap(a, b) {
    const m = meet(a.at, b.at);
    if (!some(m)) return false;
    for (let i = 0; i < 5; i++)
      for (let j = 0; j < 5; j++) {
        const x = m.l + ((m.r - m.l) * (i + 0.5)) / 5, y = m.t + ((m.b - m.t) * (j + 0.5)) / 5;
        if (showsAt(a, x, y) && showsAt(b, x, y)) return true;
      }
    return false;
  }
  /** Each pair of `list` that overlaps, of things neither of which is inside the other, as text. */
  function overlaps(list) {
    const out = [];
    list.forEach((a, i) => list.slice(i + 1).forEach((b) => !related(a, b) && overlap(a, b) && out.push(`${a.what} and ${b.what}`)));
    return out;
  }

  /** Leaves every dock at rest: the pointer on the plots, no focus in a dock, and no scroll within the time one still counts. */
  async function atRest() {
    await commands.mouse([['move', ...centre(document.querySelector('.tiles'))]]);
    document.activeElement?.blur();
    await wait(SCROLL_REST_MS + 50);
    await settle();
  }
  /** How opaque CSS colour `colour` is, from 0 to 1. */
  const alphaOf = (colour) => (colour === 'transparent' ? 0 : +(colour.match(/[/,]\s*([\d.]+)\)$/)?.[1] ?? (/^rgba\(/.test(colour) ? colour.split(',')[3].replace(')', '') : 1)));
  /** Each scroll bar `dock` shows, its own and its lists', as {what, look}. */
  function barsOf(dock) {
    const own = ownBar(dock);
    const lists = [...dock.querySelectorAll(THUMB)].filter((thumb) => some(box(thumb.getBoundingClientRect()))).map((thumb) => ({ what: `the list of ${titleOf(thumb.closest('.dock-panel'))}`, look: lookOf(thumb) }));
    return [...(own ? [{ what: 'the dock', look: own.look }] : []), ...lists];
  }

  it('has every scroll bar faint at rest and plain in use: under the pointer, while the dock or a list in it scrolls, and with the keys’ focus in it; every bar of the dock alike, the other dock’s at rest, and nothing moves', async () => {
    await browser.viewport(1200, 520);
    const page = await mountPage([['s0.x']], 'columns', 0, signals(30), tableOf({ setting: 14, report: 14, action: 10, other: 6 }), { routes: ['/', '/0', '/1'] });
    const seen = { own: 0, lists: 0 };
    try {
      await settle(6);
      await clickOn(document.querySelector('.dock-panel[aria-label=Other] .dock-title'));
      await settle(6);
      /** Where everything a dock shows is: its bars' lanes and its rows. */
      const placed = (dock) => JSON.stringify([...dock.querySelectorAll(`${BAR}, ${LIST}, tr, .chip, .dock-head`)].map((el) => box(el.getBoundingClientRect())));
      for (const dock of docks()) {
        const side = dock.getAttribute('aria-label'), other = docks().find((d) => d !== dock);
        await atRest();
        const rest = barsOf(dock), at = placed(dock);
        expect(rest.length, `${side}: scroll bars`).toBeGreaterThan(0);
        if (rest.some((b) => b.what === 'the dock')) seen.own++;
        if (rest.some((b) => b.what !== 'the dock')) seen.lists++;
        const faint = rest[0].look;
        /** Holds every bar of the dock to one look, `plain` or faint, with its lane and thumb where they were. */
        const check = (when, plain) => {
          const bars = barsOf(dock);
          expect(bars.map((b) => b.what), `${side}, ${when}: its bars`).toEqual(rest.map((b) => b.what));
          for (const { what, look } of bars) {
            expect({ ...look, colour: 0 }, `${side}, ${when}: the lane and thumb of ${what}`).toEqual({ ...faint, colour: 0 });
            expect(look.colour, `${side}, ${when}: ${what} as the dock's other bars`).toBe(bars[0].look.colour);
          }
          const alpha = alphaOf(bars[0].look.colour);
          // Faint, and still a sign there is more; plain, at least twice as strong.
          if (plain) expect(alpha, `${side}, ${when}: plain`).toBeGreaterThanOrEqual(2 * alphaOf(faint.colour));
          else expect([look0(bars), alpha > 0.08 && alpha < 0.3], `${side}, ${when}: faint, ${bars[0].look.colour}`).toEqual([faint.colour, true]);
          expect(placed(dock), `${side}, ${when}: nothing moved`).toBe(at);
        };
        const look0 = (bars) => bars[0].look.colour;
        check('at rest', false);
        // The pointer over the dock: its bars are plain, the other dock's faint.
        await commands.mouse([['move', ...centre(dock)]]);
        await settle();
        check('under the pointer', true);
        for (const b of barsOf(other)) expect(alphaOf(b.look.colour), `${side} under the pointer: ${b.what} of the other dock`).toBeLessThan(0.3);
        await atRest();
        check('at rest again', false);
        // The dock scrolls, then a list in it: plain meanwhile and for a moment after, then faint.
        const scrollers = [dock, ...[...dock.querySelectorAll(LIST)].filter((l) => l.scrollHeight > l.clientHeight)].filter((el) => el.scrollHeight > el.clientHeight);
        for (const el of scrollers) {
          const was = el.scrollTop;
          el.scrollTop = was ? 0 : 1;
          await settle();
          el.scrollTop = was;
          await settle();
          check(`while ${el === dock ? 'it' : 'a list in it'} scrolls`, true);
          await wait(SCROLL_REST_MS / 2);
          check('half the rest time after a scroll', true);
          await atRest();
          check('once the scroll has rested', false);
        }
        // Focus from the keys on a header of the dock.
        await pressKey('Shift');
        dock.querySelector('.dock-head').focus({ preventScroll: true });
        await settle();
        await wait(SCROLL_REST_MS + 50);
        await settle();
        check('with the keys’ focus in it', true);
        expect(document.activeElement.closest('.dock'), 'focus in the dock').toBe(dock);
        dock.scrollTop = 0;
        await atRest();
      }
    } finally {
      page.dispose();
      await browser.viewport(1200, 900);
    }
    expect(Object.entries(seen).filter(([, n]) => n < 1).map(([what]) => what), 'not seen: a dock’s own bar, a list’s bar').toEqual([]);
  }, 180_000);

  it('overlaps no two things a person presses or reads, a focused header’s ring among them, at any window height and wherever it is scrolled; and gives every scroll bar one look at rest', async () => {
    const seen = { scrolled: 0, both: 0, rings: 0 }, looks = new Map();
    for (const height of [900, 520, 340]) {
      await browser.viewport(1200, height);
      const page = await mountPage([['s0.x']], 'columns', 0, signals(30), tableOf({ setting: 14, report: 14, action: 10, other: 6 }), { routes: ['/', '/0', '/1'] });
      try {
        await settle(6);
        // A row picked, so a panel shows its edit bar, and Other unfolded.
        await clickOn(document.querySelector('.dock-panel[aria-label=Settings] tr[id^=prop-] td'));
        await clickOn(document.querySelector('.dock-panel[aria-label=Other] .dock-title'));
        await settle(6);
        for (const dock of docks()) {
          const side = `${dock.getAttribute('aria-label')} in a window ${height} px tall`, most = Math.max(0, dock.scrollHeight - dock.clientHeight);
          if (most > 0) seen.scrolled++;
          for (const [where, top] of Object.entries({ top: 0, middle: most / 2, bottom: most })) {
            dock.scrollTop = top;
            await settle();
            const all = things(dock);
            expect(all.length, `${side}: things found`).toBeGreaterThan(8);
            expect(overlaps(all), `${side}, scrolled to its ${where}: what overlaps`).toEqual([]);
            // The looks are read at rest: a dock's bars are plain while it scrolls.
            await atRest();
            const own = ownBar(dock);
            // A dock that scrolls has a bar in a lane of its own, beside its panels and not over them.
            expect(most > 0 && !own, `${side}: its scroll bar is drawn over its panels`).toBe(false);
            if (own) looks.set(JSON.stringify(own.look), `the ${side}`);
            for (const thumb of dock.querySelectorAll(THUMB)) {
              if (!some(box(thumb.getBoundingClientRect()))) continue;
              looks.set(JSON.stringify(lookOf(thumb)), `${titleOf(thumb.closest('.dock-panel'))} in the ${side}`);
              if (own) seen.both++;
            }
          }
          // Each header with focus from the keys, its ring around it.
          for (const head of dock.querySelectorAll('.dock-head')) {
            await pressKey('Shift');
            head.focus();
            await settle();
            const ring = document.querySelector('.focus-ring');
            expect(ring, `${side}: the ring around ${head.textContent.trim()}`).not.toBe(null);
            const around = { what: `the ring around ${head.textContent.trim()}`, el: head, at: box(ring.getBoundingClientRect()) };
            expect(overlaps([around, ...things(dock)]).filter((pair) => pair.startsWith(around.what)), `${side}: what the ring overlaps`).toEqual([]);
            seen.rings++;
          }
          dock.scrollTop = 0;
        }
      } finally {
        page.dispose();
      }
    }
    expect(Object.entries(seen).filter(([, n]) => n < 1).map(([what]) => what), 'not seen: docks scrolled, a list’s bar beside its dock’s, rings').toEqual([]);
    expect([...looks.keys()].length, `one look for every scroll bar, of ${[...looks].map(([look, where]) => `${look} (${where})`).join('; ')}`).toBe(1);
  }, 180_000);
});

// The handle between a dock and the tiles sets the dock's width. A drag of
// it in past the dock's least width hides the dock, a drag of it out again
// shows the dock at the width dragged to, and the handle beside a hidden
// dock is dragged as any other. Escape puts back what the drag changed.
describe('a drag of the handle beside a dock', () => {
  const SIDES = ['left', 'right'];
  const frameOf = (side) => document.querySelector(`.dock-frame.${side}`);
  const paneOf = (side) => document.querySelector(`[data-pane-id="dock-${side}"]`);
  const handleOf = (side) => document.querySelector(`[data-pane-resizer][aria-label="${side === 'left' ? 'Left' : 'Right'} dock width"]`);
  /** Whether dock `side` is hidden, as its button says. */
  const hidden = (side) => !!frameOf(side).querySelector(`button[aria-label="Show the ${side} dock"]`);
  /** A cursor that resizes, which a drag of a handle shows over the whole page. */
  const RESIZE = /resize/;
  /** The cursors the page's root and body show, which no drag may leave behind. */
  const cursors = () => [document.documentElement, document.body].map((el) => getComputedStyle(el).cursor);
  /** How dock `side`'s frame stands, in px and by its classes. */
  const standOf = (side) => {
    const r = frameOf(side).getBoundingClientRect();
    return `${frameOf(side).className} at ${Math.round(r.left)}, ${Math.round(r.width)} px wide`;
  };
  /**
   * Whether `stand`, as `standOf` gives it, is `was` give or take a px: the
   * pane library gives each pane its share to three digits, so the panes a
   * drag resizes move the edge beside them by up to a px.
   */
  const near = (stand, was) => {
    const [a, b] = [stand, was].map((s) => s.match(/^(.*) at (-?\d+), (\d+) px wide$/));
    return a[1] === b[1] && Math.abs(a[2] - b[2]) <= 1 && Math.abs(a[3] - b[3]) <= 1;
  };
  /** Waits for a dock that hid or showed to come to rest. */
  const atRest = async () => {
    await settle(6);
    await until(() => !document.querySelector('.layout.sliding'));
    await rested(() => SIDES.map(standOf).join('; '));
  };

  /** The pane library's sizes, as shares of the panes' room: a dock's least and most width. */
  const LEAST = 12, MOST = 45;
  /** The localStorage keys the docks' widths and arrangement are kept under. */
  const KEPT = ['tl-side-size', 'tl-right-size', 'tl-docks-v1'];
  /** The panes' room and the pane group's width, in px: the library moves a handle by shares of the group, and sizes panes by shares of the room. */
  const room = () => [...document.querySelectorAll('.layout > [data-pane-id]')].reduce((n, el) => n + el.getBoundingClientRect().width, 0);
  const groupWidth = () => document.querySelector('.layout').getBoundingClientRect().width;
  const widthOf = (side) => paneOf(side).getBoundingClientRect().width;

  /**
   * A drag: the px the pointer moves out from the handle, the dock's way
   * (into the tiles), through each of `via`, and how it ends: let go there,
   * let go off the page, the window losing focus there and the pointer
   * moving on before it is let go, or Escape and then let go.
   */
  const aDrag = fc.record({ via: fc.array(fc.integer({ min: -450, max: 450 }), { minLength: 1, maxLength: 4 }), end: fc.constantFrom('up', 'up', 'outside', 'blur', 'escape') });
  const cases = fc.record({ side: fc.constantFrom(...SIDES), hiddenFirst: fc.boolean(), drags: fc.array(aDrag, { minLength: 1, maxLength: 3 }) });
  const EXAMPLES = [
    // As reported: in past the least width, which hides the dock, and out again.
    [{ side: 'left', hiddenFirst: false, drags: [{ via: [-250], end: 'up' }, { via: [260], end: 'up' }] }],
    // In past it and back out within one drag, then off the page.
    [{ side: 'right', hiddenFirst: false, drags: [{ via: [-300, 120], end: 'up' }, { via: [-200], end: 'outside' }] }],
    // Out of a hidden dock and Escape: it stays hidden.
    [{ side: 'left', hiddenFirst: true, drags: [{ via: [200], end: 'escape' }] }],
    // In past the least width and Escape: it stays shown at its width.
    [{ side: 'right', hiddenFirst: false, drags: [{ via: [-300, -320], end: 'escape' }, { via: [40], end: 'up' }] }],
    // The right dock hidden, the page loaded again: the left keeps its width.
    [{ side: 'right', hiddenFirst: true, drags: [{ via: [0], end: 'up' }] }],
    // The window loses focus in a drag out of a hidden dock: it ends there.
    [{ side: 'left', hiddenFirst: true, drags: [{ via: [150], end: 'blur' }] }],
    // In a little and back onto the press, where the pane library hears no move: the dock is as wide as the drag found it.
    [{ side: 'left', hiddenFirst: false, drags: [{ via: [-5, 0], end: 'up' }] }],
    // In past the least width, which hides it, and back onto the press: shown again at its width.
    [{ side: 'right', hiddenFirst: false, drags: [{ via: [-200, 0], end: 'up' }, { via: [60, 0], end: 'outside' }] }],
    // Out of a hidden dock, which shows it, and back onto the press: hidden again.
    [{ side: 'left', hiddenFirst: true, drags: [{ via: [200, 0], end: 'up' }] }],
  ];

  it('hides the dock exactly when let go inside its least width, else shows it at the width dragged to; Escape puts it back; it leaves no resize cursor, never moves the other dock, and its widths load again', async () => {
    await browser.viewport(1200, 900);
    const seen = { hid: 0, showed: 0, escaped: 0, outside: 0, blur: 0 };
    await fc.assert(
      fc.asyncProperty(cases, async ({ side, hiddenFirst, drags }) => {
        const other = SIDES.find((s) => s !== side), out = side === 'left' ? 1 : -1;
        let page = await mountPage([['s0.x']], 'columns', 0, signals(3), tableOf({ setting: 3, report: 3, action: 2, other: 2 }));
        try {
          await atRest();
          if (hiddenFirst) {
            await clickOn(frameOf(side).querySelector(`button[aria-label="Hide the ${side} dock"]`));
            await atRest();
          }
          for (const [n, { via, end }] of drags.entries()) {
            const said = `drag ${n + 1} of the ${side} handle, ${via.join(' then ')} px out, ending ${end}`;
            const was = { hidden: hidden(side), width: widthOf(side), kept: localStorage.getItem(side === 'left' ? KEPT[0] : KEPT[1]) };
            const r = room(), g = groupWidth(), rail = was.hidden ? was.width : 34;
            // The pane's share of the room where the pointer is let go. The pane library collapses an open pane below
            // halfway from its rail to its least width, and opens a collapsed one only at its least width.
            const share = (was.width / r) * 100 + ((via.at(-1) / g) * 100);
            const snap = was.hidden ? LEAST : ((rail / r) * 100 + LEAST) / 2;
            const collapses = share < snap;
            // A release too near where the pane snaps is left out: the pointer's px round.
            if (end !== 'escape' && Math.abs(share - snap) < 0.5) continue;
            const otherWas = standOf(other);
            const stop = watchFrames(() => standOf(other));
            const [x, y] = centre(handleOf(side));
            const steps = [['move', x, y], ['down']];
            let at = x;
            for (const d of via) {
              steps.push(...glide([at, y], [x + out * d, y], 4));
              at = x + out * d;
            }
            if (end === 'outside') steps.push(['move', at, -40], ['up']);
            await commands.mouse(steps);
            if (end === 'escape') {
              await pressKey('Escape');
              await commands.mouse([['move', at + out * 30, y], ['up']]);
            } else if (end === 'blur') {
              window.dispatchEvent(new FocusEvent('blur'));
              await commands.mouse([['move', at + out * 30, y], ['up']]);
            } else if (end === 'up') await commands.mouse([['up']]);
            await atRest();
            const frames = stop();
            expect(cursors(), `${said}: the page's cursors after it`).not.toEqual(expect.arrayContaining([expect.stringMatching(RESIZE)]));
            expect(document.querySelector('[data-pane-resizer][data-active=pointer]'), `${said}: a handle still dragged`).toBe(null);
            expect(frames.filter((s) => !near(s, otherWas)), `${said}: the ${other} dock's frame, at the frames it was not ${otherWas}`).toEqual([]);
            expect(near(standOf(other), otherWas), `${said}: the ${other} dock's frame, ${standOf(other)}, was ${otherWas}`).toBe(true);
            const keep = localStorage.getItem(side === 'left' ? KEPT[0] : KEPT[1]);
            if (end === 'escape') {
              seen.escaped++;
              expect(hidden(side), `${said}: hidden`).toBe(was.hidden);
              expect(Math.abs(widthOf(side) - was.width), `${said}: px from its width before`).toBeLessThan(1.5);
              expect(Number(keep ?? 22), `${said}: its width kept`).toBe(Number(was.kept ?? 22));
              continue;
            }
            if (end === 'outside' || end === 'blur') seen[end]++;
            expect(hidden(side), `${said}: hidden, let go at ${share.toFixed(1)}% of the room`).toBe(collapses);
            if (collapses) {
              seen.hid++;
              // Shown again, it opens to the width it had before: its own kept, or the width it was dragged from.
              expect(Number(keep ?? 22), `${said}: the width it opens to`).toBeCloseTo(was.hidden ? Number(was.kept ?? 22) : (was.width / r) * 100, 0);
            } else {
              seen.showed++;
              const want = (Math.min(MOST, Math.max(LEAST, share)) / 100) * r;
              expect(Math.abs(widthOf(side) - want), `${said}: px from the width dragged to, ${want.toFixed(1)} px`).toBeLessThan(2);
              expect(Number(keep ?? 22), `${said}: its width kept`).toBeCloseTo((widthOf(side) / r) * 100, 0);
            }
          }
          // The page loaded again: each dock as it was, hidden or at its width.
          const before = SIDES.map((s) => [hidden(s), Math.round(widthOf(s))]);
          const saved = Object.fromEntries(KEPT.filter((k) => localStorage.getItem(k) !== null).map((k) => [k, JSON.parse(localStorage.getItem(k))]));
          page.dispose();
          page = await mountPage([['s0.x']], 'columns', 0, signals(3), tableOf({ setting: 3, report: 3, action: 2, other: 2 }), { saved });
          await atRest();
          const after = SIDES.map((s) => [hidden(s), Math.round(widthOf(s))]);
          after.forEach(([h, w], i) => {
            expect(h, `loaded again: the ${SIDES[i]} dock hidden`).toBe(before[i][0]);
            expect(Math.abs(w - before[i][1]), `loaded again: px from the ${SIDES[i]} dock's width`).toBeLessThanOrEqual(1);
          });
        } finally {
          page.dispose();
        }
      }),
      withExamples(EXAMPLES),
    );
    expect(Object.entries(seen).filter(([, n]) => n < 1).map(([what]) => what), 'not seen').toEqual([]);
  }, 600_000);

  it('ends on Escape with its dock put back on a page mounted again under a drag held, and ends where it is as a narrowing window lays the panes out anew, Escape before the pointer is let go putting the dock back still; nothing thrown, and Escape is no drag’s once it is let go', async () => {
    const errors = [];
    const onError = (e) => errors.push(String(e.error?.message ?? e.message ?? e.reason));
    window.addEventListener('error', onError);
    window.addEventListener('unhandledrejection', onError);
    const mounted = () => mountPage([['s0.x']], 'columns', 0, signals(3), tableOf({ setting: 3, report: 3, action: 2, other: 2 }));
    /** Presses the left handle and drags it `by` px out, the button held. */
    const hold = async (by) => {
      const [x, y] = centre(handleOf('left'));
      await commands.mouse([['move', x, y], ['down'], ...glide([x, y], [x + by, y], 4)]);
      return [x + by, y];
    };
    let page = null;
    try {
      for (const how of ['mounted again', 'narrowed']) {
        await browser.viewport(1200, 900);
        page = await mounted();
        await atRest();
        if (how === 'mounted again') {
          // A drag held as the page goes, as a case that fails under a drag leaves it.
          await hold(60);
          page.dispose();
          await commands.mouse([['up']]);
          page = await mounted();
          await atRest();
        } else {
          // A drag let go first, so the width the drag begins at is not the one the panes were mounted at.
          await hold(-40);
          await commands.mouse([['up']]);
          await atRest();
        }
        const said = `the page ${how}`, was = { width: widthOf('left'), hidden: hidden('left'), kept: localStorage.getItem(KEPT[0]) };
        const at = await hold(80);
        await settle();
        const dragged = widthOf('left');
        expect(Math.abs(dragged - was.width), `${said}: the drag moved the dock`).toBeGreaterThan(20);
        if (how === 'narrowed') {
          // The panes are laid out anew, one above another, under the drag, and again side by side.
          await browser.viewport(700, 900);
          await atRest();
          await browser.viewport(1200, 900);
          await atRest();
          // The drag ended where it was.
          expect(Math.abs(widthOf('left') - dragged), `${said}: px from the width dragged to`).toBeLessThan(1.5);
        }
        await pressKey('Escape');
        await commands.mouse([['move', at[0] + 30, at[1]], ['up']]);
        await atRest();
        expect(errors, `${said}: nothing thrown`).toEqual([]);
        expect(hidden('left'), `${said}: hidden`).toBe(was.hidden);
        // Escape puts back what the drag moved: one under way, or one a narrowed window ended while the pointer still held it.
        expect(Math.abs(widthOf('left') - was.width), `${said}: px from its width before`).toBeLessThan(1.5);
        expect(Number(localStorage.getItem(KEPT[0]) ?? 22), `${said}: its width kept`).toBe(Number(was.kept ?? 22));
        expect(document.querySelector('[data-pane-resizer][data-active=pointer]'), `${said}: a handle still dragged`).toBe(null);
        expect(cursors(), `${said}: the page's cursors after it`).not.toEqual(expect.arrayContaining([expect.stringMatching(RESIZE)]));
        // The drag is over: Escape is no drag's.
        await pressKey('Escape');
        expect(lastKey().by, `${said}: Escape after it`).not.toBe('drag');
        page.dispose();
        page = null;
      }
    } finally {
      window.removeEventListener('error', onError);
      window.removeEventListener('unhandledrejection', onError);
      await commands.mouse([['up']]);
      page?.dispose();
      await browser.viewport(1200, 900);
    }
  }, 60_000);

  it('double-clicked, shows the dock at the width it starts at, hidden or shown', async () => {
    await browser.viewport(1200, 900);
    const page = await mountPage([['s0.x']], 'columns', 0, signals(3), tableOf({ setting: 3, report: 3, action: 2, other: 2 }));
    try {
      await atRest();
      const start = widthOf('left');
      const [x, y] = centre(handleOf('left'));
      await commands.mouse([['move', x, y], ['down'], ...glide([x, y], [x + 120, y], 4), ['up']]);
      await atRest();
      for (const hide of [false, true]) {
        if (hide) {
          await clickOn(frameOf('left').querySelector('button[aria-label="Hide the left dock"]'));
          await atRest();
        }
        const said = `double-clicked with the dock ${hide ? 'hidden' : 'shown'}`;
        expect(Math.abs(widthOf('left') - start), `${said}: px from its first width before`).toBeGreaterThan(20);
        await commands.mouse([['dblclick', ...centre(handleOf('left'))]]);
        await atRest();
        expect(hidden('left'), `${said}: hidden`).toBe(false);
        expect(Math.abs(widthOf('left') - start), `${said}: px from its first width`).toBeLessThan(1.5);
        expect(cursors(), `${said}: the page's cursors`).not.toEqual(expect.arrayContaining([expect.stringMatching(RESIZE)]));
      }
    } finally {
      page.dispose();
    }
  }, 60_000);
});

// A press on a header that lets go where it pressed, or within a couple of
// px, is a click, which folds the panel and does nothing else: no drag is
// under way, so neither dock's frame changes, whether a dock is shown or
// hidden, by its classes or by where it is at any frame.
describe('a click on a panel’s header', () => {
  const SIDES = ['left', 'right'];
  const frameOf = (side) => document.querySelector(`.dock-frame.${side}`);
  /** How the frame of dock `side` stands: its classes, its dock's, and where it is, in px. */
  const stateOf = (side) => {
    const frame = frameOf(side), r = frame.getBoundingClientRect();
    return `${frame.className} / ${frame.querySelector('.dock-side').className} / from ${Math.round(r.left)} px, ${Math.round(r.width)} px wide`;
  };
  /** How far the pointer moves between press and release, in px: not at all, and short of a drag along one axis and along both. */
  const MOVES = [[0, 0], [2, 0], [2, 2]];
  /** Where across a header's width a click lands, as a share of it: near its left end, its middle, near its right end. */
  const ACROSS = [0.04, 0.5, 0.96];

  it('folds the panel and changes neither dock’s frame at any frame, the other dock shown or hidden, from every header of each dock, anywhere across it', async () => {
    await browser.viewport(1200, 900);
    let clicks = 0;
    for (const hidden of [null, ...SIDES]) {
      const page = await mountPage([['s0.x']], 'columns', 0, signals(3), tableOf({ setting: 3, report: 3, action: 2, other: 2 }));
      try {
        await settle(6);
        if (hidden) {
          await clickOn(frameOf(hidden).querySelector('button[aria-label^="Hide the"]'));
          expect(await until(() => frameOf(hidden).classList.contains('rail')), `the ${hidden} dock is its rail`).toBe(true);
          await settle(6);
        }
        for (const side of SIDES.filter((s) => s !== hidden)) {
          const other = SIDES.find((s) => s !== side);
          const titles = [...frameOf(side).querySelectorAll('.dock-panel')].map(titleOf);
          expect(titles.length, `panels in the ${side} dock`).toBeGreaterThan(1);
          for (const [i, title] of titles.entries()) {
            for (const [k, across] of ACROSS.entries()) {
              const [dx, dy] = MOVES[(i + k) % MOVES.length];
              const panel = () => document.querySelector(`.dock-panel[aria-label="${title}"]`);
              const head = panel().querySelector('.dock-head');
              head.scrollIntoView({ block: 'nearest' });
              await settle();
              const r = head.getBoundingClientRect(), at = [r.left + across * r.width, r.top + r.height / 2], was = panel().classList.contains('open');
              const said = `a click on ${title}, ${Math.round(across * 100)}% across its header, moving ${dx}, ${dy} px, with the ${other} dock ${other === hidden ? 'hidden' : 'shown'}`;
              await commands.mouse([['move', ...at]]);
              await settle();
              const stand = () => `the ${side} dock: ${stateOf(side)}; the ${other} dock: ${stateOf(other)}`;
              expect(await rested(stand), `${said}: the docks are at rest before it`).toBe(true);
              const stop = watchFrames(stand);
              await commands.mouse([['down'], ['wait', 50], ['move', at[0] + dx, at[1] + dy], ['wait', 80], ['up'], ['wait', 80]]);
              await settle(6);
              const stood = stop();
              expect(stood, `${said}: how the docks' frames stood, frame by frame`).toEqual(stood.slice(0, 1));
              expect(panel().classList.contains('open'), `${said}: the panel is open`).toBe(!was);
              clicks++;
            }
          }
        }
      } finally {
        page.dispose();
      }
    }
    expect(clicks).toBeGreaterThan(40);
  }, 180_000);
});
