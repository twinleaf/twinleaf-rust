// The hub's log on the page: the Log item of the status bar, and the
// drawer it opens under the docks and the plots.
// - The Log item counts the warnings and errors that arrive while the
//   drawer is closed, and none once it is open.
// - The drawer lists the hub's lines, lines in a row that say the same as
//   one row that counts them; each level's toggle, the toggle of lost
//   data, the source and the filter show the rows they say, for any
//   setting of them, by a model of the lines the hub sent; and each toggle
//   counts the lines of its kind.
// - It follows the newest row until the list is scrolled up, counts what
//   came since, and goes back to the newest by Follow or by scrolling to
//   the end.
// - An outage of a device's link is a divider after the line of its drop,
//   which says how long it lasted once the link is back.
// - A row's button puts X1 at the line's moment on every time plot of the
//   tab, as one undo step.
// - The drawer is dragged taller and shorter, closed and opened again as
//   tall, and kept for the next visit.
// - The keys open it, reach each of its controls and its rows, show a row
//   on the plots, and close it.
// - At 900 by 600 nothing of it overlaps or is cut off, and it is drawn at
//   its neighbours' sizes.
// - While lines stream, no control of the drawer changes but its counts,
//   which only grow.
// The hub here is the stub, which sends lines and losses as the server
// does (harness/page.js `stubHub`).
import { afterAll, afterEach, describe, expect, it } from 'vitest';
import { page as browser, commands } from 'vitest/browser';
import fc from 'fast-check';
import { centre, clickOn, closeAll, feed, focusPage, frames, mountPage, pressKey, settle, typeInto, until, wait, withExamples } from './harness/page.js';
import { axisPopover } from './harness/menus.js';
import { plotIds } from '../src/lib/layout.js';
import { cursorOf } from '../src/lib/focus.js';
import { lastKey } from '../src/lib/keyrouter.js';
import { KEY, SIZE } from '../src/lib/log.svelte.js';
import { prefs } from '../src/lib/prefs.svelte.js';

/** Where VITE_TL_SHOTS names a folder, relative to this file, the screenshots go there. */
const SHOTS = import.meta.env.VITE_TL_SHOTS;
const shot = async (name) => SHOTS && (await browser.screenshot({ path: `${SHOTS}/${name}.png` }));

let page;
afterEach(() => (page?.dispose(), (page = null)));
afterAll(() => (prefs.setTheme('dark'), browser.viewport(1200, 900)));

const SIGNALS = ['sine.sine', 'aux.triangle'];
const DEVICE = [{ name: 'gain', channel: 'setting', value: 3 }, { name: 'offset', channel: 'setting', value: 0.5 }, { name: 'rate', channel: 'setting', value: 100 }];
/** How the page names each device of `twoDevices`, by route. */
const NAMES = { '/': 'VMR 0412', '/0': 'OMG 0077 (/0)' };
/**
 * A page of two time plots on a hub of two devices, VMR 0412 at the root
 * and OMG 0077 at /0, with 20 s of samples; `sent` lists each `log`
 * message its hub sends from then on.
 */
async function twoDevices(saved = {}) {
  const made = await mountPage([['sine.sine'], ['aux.triangle']], 'rows', 0, SIGNALS, DEVICE, { routes: ['/', '/0'], saved });
  made.hub.names.set('/', 'VMR 0412').set('/0', 'OMG 0077');
  made.hub.presence('/', 'alive');
  made.hub.presence('/0', 'alive');
  feed(made.hub);
  const sent = [], text = made.hub.text.bind(made.hub);
  made.hub.text = (msg) => (msg.type === 'log' && sent.push(msg), text(msg));
  await settle();
  return { ...made, sent };
}

const bar = () => document.querySelector('.status-bar');
const logItem = () => bar().querySelector('.log');
/** The Log item's count of what is unseen, as it reads. */
const unseen = () => logItem().querySelector('.num').textContent.trim();
const drawer = () => document.querySelector('.log-frame');
const list = () => drawer().querySelector('.log-list');
const viewport = () => drawer().querySelector('.log-rows [data-slot=scroll-area-viewport]');
const rowEls = () => [...drawer().querySelectorAll('.log-row')];
const cell = (row, name) => row.querySelector(`.${name}`).textContent.trim();
/** The rows the drawer shows, each as [level, source, message, count]. */
const rows = () => rowEls().map((row) => [cell(row, 'lv'), cell(row, 'src'), cell(row, 'msg'), cell(row, 'n')]);
/** The dividers it shows, by their words. */
const cuts = () => [...drawer().querySelectorAll('.log-cut')].map((cut) => cut.textContent.trim());
const levelToggle = (level) => drawer().querySelector(`.seg button[data-level=${level}]`);
const lossToggle = () => drawer().querySelector('[data-filter=loss]');
const sourceSelect = () => drawer().querySelector('.log-source');
const filterField = () => drawer().querySelector('.log-filter');
const followButton = () => drawer().querySelector('.log-follow');
const pressed = (button) => button.getAttribute('aria-pressed') === 'true';
const countOn = (button) => button.querySelector('.num').textContent.trim();
/** How far the list is from scrolled to its end, in px. */
const fromEnd = () => viewport().scrollHeight - viewport().scrollTop - viewport().clientHeight;

/** Opens the drawer by the status bar's Log item. */
async function open() {
  await clickOn(logItem());
  expect(await until(drawer), 'the drawer opens').toBe(true);
  await settle();
}

/** Lines of every level, a repeat, an outage that ended, samples lost in a row, and a line longer than a narrow drawer. */
function story(hub) {
  hub.line('info', 'Proxy hosted by tio proxy (pid 4242) at tcp://127.0.0.1:7855', { source: 'proxy', part: 'server', route: null });
  hub.line('info', 'Opening udp://192.168.1.40', { source: 'proxy', part: 'link', route: null });
  hub.line('info', 'Found VMR 0412, serial S1, firmware 1.4.2', { source: 'proxy' });
  hub.line('info', 'field lock acquired');
  for (let i = 0; i < 3; i++) hub.line('debug', 'pll phase error 0.0021 rad');
  hub.line('warn', 'field lock lost, reacquiring');
  hub.line('info', 'field lock acquired');
  hub.line('info', 'calibration loaded from flash', { route: '/0' });
  hub.line('warn', 'Sensor disconnected', { source: 'proxy', part: 'udp://192.168.1.41', route: null });
  hub.linkDown('/0');
  hub.line('error', 'heater current over limit, output held');
  hub.linkBack('/0', 11);
  hub.lose('/', 'sine', { skipped: 312 });
  hub.lose('/', 'sine', { hub: 1400 });
  hub.lose('/', 'sine', { hub: 700 });
  hub.line('warn', 'background diagnostic index settled at lucky number 7421, which is a long line that runs past the end of a narrow drawer');
  hub.line('debug', 'adc temperature 31.4 C', { route: '/0' });
}

// The model: what the drawer shows of the lines the hub sent, worked out
// here from the messages alone.

/** A count as the page writes it, to two figures from a thousand on. */
const counted = (n) => (n >= 1e3 ? `${+(n / 1e3).toPrecision(2)} k` : String(n));
/** What a line of a loss reads as. */
function lossWords({ cause, stream, samples, seconds }) {
  if (cause === 'link') return seconds === null ? 'link down' : `link back after ${seconds.toFixed(1)} s`;
  const n = `${counted(samples)} ${samples === 1 ? 'sample' : 'samples'}`;
  return cause === 'skipped' ? `${n} of ${stream} skipped` : `${cause} dropped ${n} of ${stream}`;
}
/** The rows of `lines`, the hub's `log` messages in order: lines in a row that say the same as one, a loss's samples summed. */
function modelRows(lines) {
  const out = [];
  for (const l of lines) {
    const same = l.loss?.cause === 'link' ? null : l.loss ? `${l.route} ${l.loss.cause} ${l.loss.stream}` : `${l.level} ${l.source} ${l.part} ${l.route} ${l.text}`;
    const last = out.at(-1);
    if (last && same !== null && last.same === same) {
      last.count++;
      if (l.loss) last.loss = { ...l.loss, samples: last.loss.samples + l.loss.samples };
    } else out.push({ ...l, same, count: 1 });
  }
  return out;
}
/** What a model row reads as: [level, source, message, count]. */
function modelRead(r) {
  const said = r.loss ? lossWords(r.loss) : r.text;
  return [r.level.toUpperCase(), r.source === 'proxy' ? r.part : NAMES[r.route], r.source === 'proxy' && r.route !== null ? `${NAMES[r.route]}: ${said}` : said, r.count > 1 ? `×${r.count}` : ''];
}
/** The rows of `lines` that show under `filter`, {levels, lossOnly, source, query}, as they read. */
function modelShown(lines, { levels, lossOnly, source, query }) {
  const words = query.toLowerCase().split(/\s+/).filter(Boolean);
  return modelRows(lines)
    .filter((r) => levels[r.level] && (!lossOnly || r.loss) && (!source || (source.startsWith('part:') ? r.part === source.slice(5) : r.route === source)))
    .map(modelRead)
    .filter((read) => words.every((w) => `${read[0]} ${read[1]} ${read[2]}`.toLowerCase().includes(w)));
}

/** Sets the drawer's controls to `filter`, each as a person does: a press on a toggle that differs, a choice of the source's list, and the filter typed. */
async function setFilter({ levels, lossOnly, source, query }) {
  for (const [level, on] of Object.entries(levels)) if (pressed(levelToggle(level)) !== on) await clickOn(levelToggle(level));
  if (pressed(lossToggle()) !== lossOnly) await clickOn(lossToggle());
  const name = source === '' ? 'All sources' : source.startsWith('part:') ? source.slice(5) : NAMES[source];
  if (sourceSelect().textContent.trim() !== name) {
    await clickOn(sourceSelect());
    const option = () => [...document.querySelectorAll('[role=option]')].find((o) => o.textContent.trim() === name);
    expect(await until(option), `the source ${name}`).toBe(true);
    await clickOn(option());
    expect(await until(() => !document.querySelector('[data-slot=select-content]')), 'the list closes').toBe(true);
  }
  if (filterField().value !== query) {
    await typeInto(filterField(), query);
  }
  await settle();
}

const EVERY = { error: true, warn: true, info: true, debug: true };
const START = { levels: { ...EVERY, debug: false }, lossOnly: false, source: '', query: '' };

describe('the Log item', () => {
  it('counts the warnings and errors that arrive while the drawer is closed, in the error’s colour once one is an error, and none once it is open', async () => {
    page = await twoDevices();
    expect([unseen(), pressed(logItem())]).toEqual(['', false]);
    page.hub.line('info', 'field lock acquired');
    page.hub.line('debug', 'pll phase error');
    await settle();
    expect(unseen()).toBe('');
    page.hub.line('warn', 'field lock lost');
    page.hub.lose('/', 'sine', { skipped: 3 });
    await settle();
    const number = logItem().querySelector('.num');
    expect([unseen(), number.classList.contains('error')]).toEqual(['2', false]);
    const [left, width] = [logItem().getBoundingClientRect().left, logItem().getBoundingClientRect().width];
    page.hub.linkDown('/0');
    await settle();
    expect([unseen(), number.classList.contains('error')]).toEqual(['3', true]);
    expect(getComputedStyle(number).color).not.toBe(getComputedStyle(logItem()).color);
    for (let i = 0; i < 200; i++) page.hub.line('warn', `warning ${i}`);
    await settle();
    expect(unseen()).toBe('203');
    expect([logItem().getBoundingClientRect().left, logItem().getBoundingClientRect().width], 'the item keeps its place and its width').toEqual([left, width]);
    await open();
    expect([unseen(), pressed(logItem())]).toEqual(['', true]);
    page.hub.line('error', 'heater current over limit');
    await settle();
    expect(unseen(), 'a line that arrives with the drawer open is seen').toBe('');
    await clickOn(logItem());
    expect(await until(() => !drawer()), 'the drawer closes').toBe(true);
    page.hub.line('warn', 'field lock lost');
    await settle();
    expect(unseen()).toBe('1');
  });
});

describe('the log drawer', () => {
  it('lists the hub’s lines, those in a row that say the same as one row with their count, and counts each level’s lines and those of lost data on its toggles', async () => {
    page = await twoDevices();
    story(page.hub);
    await open();
    expect(rows()).toEqual(modelShown(page.sent, START));
    expect(rows()).toContainEqual(['WARN', 'hub', 'VMR 0412: hub dropped 2.1 k samples of sine', '×2']);
    await clickOn(levelToggle('debug'));
    await settle();
    expect(rows()).toEqual(modelShown(page.sent, { ...START, levels: EVERY }));
    expect(rows()).toContainEqual(['DEBUG', 'VMR 0412', 'pll phase error 0.0021 rad', '×3']);
    const of = (level) => page.sent.filter((l) => l.level === level).length;
    expect(['error', 'warn', 'info', 'debug'].map((level) => countOn(levelToggle(level)))).toEqual(['error', 'warn', 'info', 'debug'].map((level) => String(of(level))));
    expect(countOn(lossToggle())).toBe(String(page.sent.filter((l) => l.loss).length));
    // A line that comes is counted at once, shown or not.
    await clickOn(levelToggle('debug'));
    page.hub.line('debug', 'adc temperature 31.5 C');
    await settle();
    expect(countOn(levelToggle('debug'))).toBe(String(of('debug')));
    expect(rows()).toEqual(modelShown(page.sent, START));
  });

  it('lists as sources the devices and the parts of the proxy its lines were said by, each under its heading, names each row by its source in a column of one width, and doubles no source in a row’s words', async () => {
    page = await twoDevices();
    story(page.hub);
    await open();
    await clickOn(levelToggle('debug'));
    await settle();
    // The sources the rows show, found on the page: the proxy's parts are those no device is named.
    const shown = new Set(rowEls().map((row) => cell(row, 'src')));
    const partsShown = [...shown].filter((name) => !Object.values(NAMES).includes(name)).sort();
    expect(partsShown).toEqual(['hub', 'link', 'server', 'udp://192.168.1.41']);
    await clickOn(sourceSelect());
    expect(await until(() => document.querySelector('[data-slot=select-content]')), 'the list opens').toBe(true);
    const listed = [...document.querySelectorAll('[data-slot=select-content] :is([role=option], [data-slot=select-label])')].map((el) => (el.matches('[role=option]') ? el.textContent.trim() : `# ${el.textContent.trim()}`));
    expect(listed).toEqual(['All sources', '# Devices', ...Object.values(NAMES), '# Proxy', ...partsShown]);
    await closeAll();
    for (const row of rowEls()) {
      const src = cell(row, 'src'), msg = cell(row, 'msg');
      expect(msg.startsWith(`${src}:`), `${src} | ${msg}`).toBe(false);
      expect(row.querySelector('.src').title).toBe(src);
    }
    // The column is as wide whatever it names, and stays so as a longer name comes.
    const widths = () => new Set(rowEls().map((row) => `${row.querySelector('.src').getBoundingClientRect().left} ${row.querySelector('.src').getBoundingClientRect().width} ${row.querySelector('.msg').getBoundingClientRect().left}`));
    const before = [...widths()];
    expect(before).toHaveLength(1);
    page.hub.line('info', 'a part with a long name', { source: 'proxy', part: 'tcp://a-very-long-host-name.example:7855', route: null });
    await settle();
    expect([...widths()]).toEqual(before);
    // A part no line was said by is not listed, and one comes to be listed with its first line.
    await clickOn(sourceSelect());
    await until(() => document.querySelector('[data-slot=select-content]'));
    expect([...document.querySelectorAll('[role=option]')].map((o) => o.textContent.trim())).toContain('tcp://a-very-long-host-name.example:7855');
    await closeAll();
  });

  it('shows the rows its toggles, its source and its filter say, for any setting of them', async () => {
    page = await twoDevices();
    story(page.hub);
    await open();
    const seen = { some: 0, none: 0 };
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          levels: fc.record({ error: fc.boolean(), warn: fc.boolean(), info: fc.boolean(), debug: fc.boolean() }),
          lossOnly: fc.boolean(),
          source: fc.constantFrom('', '', 'part:hub', 'part:server', 'part:link', 'part:udp://192.168.1.41', '/', '/0'),
          query: fc.constantFrom('', '', 'lock', 'LINK down', 'omg', 'sine dropped', 'warn', 'no such words'),
        }),
        async (filter) => {
          await closeAll();
          await setFilter(filter);
          const want = modelShown(page.sent, filter);
          expect(rows(), JSON.stringify(filter)).toEqual(want);
          seen[want.length ? 'some' : 'none']++;
          // With no row to show it says why, by whether only lost data shows.
          const empty = drawer().querySelector('.props-empty')?.textContent.trim() ?? null;
          const anyCut = cuts().length > 0;
          expect(empty).toBe(want.length || anyCut ? null : filter.lossOnly ? 'No data has been lost' : 'Nothing matches');
        },
      ),
      withExamples([
        [{ ...START, levels: { error: true, warn: false, info: false, debug: false } }],
        [{ ...START, levels: { error: false, warn: true, info: false, debug: false } }],
        [{ ...START, levels: { error: false, warn: false, info: true, debug: false } }],
        [{ ...START, levels: { error: false, warn: false, info: false, debug: true } }],
        [{ ...START, levels: EVERY, lossOnly: true }],
        [{ ...START, levels: EVERY, source: 'part:hub' }],
        [{ ...START, levels: EVERY, source: 'part:server' }],
        [{ ...START, levels: EVERY, source: 'part:link' }],
        [{ ...START, levels: EVERY, source: 'part:udp://192.168.1.41' }],
        [{ ...START, levels: EVERY, source: '/0' }],
        [{ ...START, levels: EVERY, source: '/' }],
        [{ ...START, levels: EVERY, query: 'lock' }],
        [{ ...START, levels: EVERY, query: 'no such words' }],
        [{ ...START, levels: { error: false, warn: false, info: false, debug: false }, lossOnly: true, source: '/' }],
      ]),
    );
    expect(seen.some).toBeGreaterThan(0);
    expect(seen.none).toBeGreaterThan(0);
  }, 240_000);

  it('follows the newest row until the list is scrolled up, counts what came since, and goes back to the newest by Follow or by scrolling to the end', async () => {
    page = await twoDevices();
    await open();
    for (let i = 0; i < 60; i++) page.hub.line('info', `line ${i}`);
    await settle();
    expect([pressed(followButton()), countOn(followButton())]).toEqual([true, '']);
    expect(fromEnd()).toBeLessThanOrEqual(1);
    const last = () => rowEls().at(-1).getBoundingClientRect(), box = () => viewport().getBoundingClientRect();
    expect(last().bottom).toBeLessThanOrEqual(box().bottom + 1);
    // A line that comes keeps the end in view.
    page.hub.line('info', 'one more');
    await settle();
    expect(fromEnd()).toBeLessThanOrEqual(1);
    expect(cell(rowEls().at(-1), 'msg')).toBe('one more');

    viewport().scrollTop -= 150;
    await settle();
    expect(pressed(followButton()), 'scrolled up, it follows no more').toBe(false);
    const top = viewport().scrollTop;
    for (let i = 0; i < 3; i++) page.hub.line('warn', `later ${i}`);
    // Lines the drawer does not show are not counted.
    page.hub.line('debug', 'hidden');
    await settle();
    expect([viewport().scrollTop, countOn(followButton())]).toEqual([top, '3']);
    expect(last().top, 'the newest row is below the list’s foot').toBeGreaterThanOrEqual(box().bottom);

    await clickOn(followButton());
    await settle();
    expect([pressed(followButton()), countOn(followButton())]).toEqual([true, '']);
    expect(fromEnd()).toBeLessThanOrEqual(1);
    expect(cell(rowEls().at(-1), 'msg')).toBe('later 2');

    // Follow pressed while it follows stops it where it is; scrolling to the end follows again.
    await clickOn(followButton());
    await settle();
    expect(pressed(followButton())).toBe(false);
    page.hub.line('info', 'unseen below');
    await settle();
    expect(countOn(followButton())).toBe('1');
    viewport().scrollTop = viewport().scrollHeight;
    await settle();
    expect([pressed(followButton()), countOn(followButton())]).toEqual([true, '']);
  });

  it('draws an outage of a device’s link as a divider after the line of its drop, which says how long it lasted once the link is back, under any toggle with the lines of its device', async () => {
    page = await twoDevices();
    await open();
    page.hub.line('info', 'before');
    page.hub.linkDown('/0');
    page.hub.line('info', 'meanwhile');
    await settle();
    expect(cuts()).toEqual(['link to OMG 0077 (/0) down']);
    /** What the list holds, rows by their message and dividers by their words, in order. */
    const order = () => [...list().children].map((el) => (el.classList.contains('log-cut') ? `— ${el.textContent.trim()}` : cell(el, 'msg')));
    expect(order()).toEqual(['before', 'OMG 0077 (/0): link down', '— link to OMG 0077 (/0) down', 'meanwhile']);
    page.hub.linkBack('/0', 11);
    await settle();
    expect(order()).toEqual(['before', 'OMG 0077 (/0): link down', '— link to OMG 0077 (/0) down 11.0 s', 'meanwhile', 'OMG 0077 (/0): link back after 11.0 s']);
    expect(getComputedStyle(drawer().querySelector('.log-cut')).color).toBe(getComputedStyle(rowEls()[1].querySelector('.lv')).color);
    // The divider stays with the line of the drop hidden, and goes with the lines of another device.
    await clickOn(levelToggle('error'));
    await settle();
    expect(order()).toEqual(['before', '— link to OMG 0077 (/0) down 11.0 s', 'meanwhile', 'OMG 0077 (/0): link back after 11.0 s']);
    await setFilter({ ...START, source: '/' });
    expect(order()).toEqual(['before', 'meanwhile']);
    await setFilter({ ...START, source: '/0' });
    expect(order()).toEqual(['OMG 0077 (/0): link down', '— link to OMG 0077 (/0) down 11.0 s', 'OMG 0077 (/0): link back after 11.0 s']);
  });

  it('puts X1 at a row’s moment on every time plot of the tab by the row’s button, as one step, and has no button to press for a line with no moment', async () => {
    page = await twoDevices();
    const ids = plotIds(page.ws.layout);
    await open();
    page.hub.line('warn', 'in view', { time: 14.5 });
    page.hub.line('warn', 'before the view', { time: 2.25 });
    page.hub.line('info', 'of no device', { source: 'proxy', part: 'server', route: null });
    await settle();
    const go = (i) => rowEls()[i].querySelector('.go');
    const x1 = () => ids.map((id) => page.ws.plots[id].cursors?.x?.a ?? null);
    const undos = page.history.undos.length;
    await clickOn(go(0));
    await settle();
    expect(x1()).toEqual([14.5, 14.5]);
    expect(ids.map((id) => page.views.of(id).live), 'each plot holds the moment').toEqual([false, false]);
    expect(page.history.undos.length).toBe(undos + 1);
    // A moment the plots do not show: each moves to show it.
    await clickOn(go(1));
    await settle();
    expect(x1()).toEqual([2.25, 2.25]);
    for (const id of ids) {
      const [lo, hi] = page.pv(id).xd;
      expect(lo <= 2.25 && 2.25 <= hi, `plot ${id} shows ${lo} to ${hi}`).toBe(true);
    }
    expect([go(0).title, go(0).getAttribute('aria-label')]).toEqual(['Show on plots', 'Show on plots']);
    expect([go(2).disabled, go(2).title]).toEqual([true, 'No samples of this device had arrived']);
    page.history.undo();
    page.history.undo();
    await settle();
    expect(x1()).toEqual([null, null]);
  });

  it('is dragged taller, closed by its button and opened again as tall, and is as it was left on the next visit', async () => {
    page = await twoDevices();
    await open();
    const height = () => drawer().getBoundingClientRect().height;
    const tiles = () => document.querySelector('.tiles').getBoundingClientRect();
    const start = height(), plots = tiles().height;
    expect(JSON.parse(localStorage.getItem(KEY))).toEqual({ open: true, size: SIZE.default });
    const handle = document.querySelector('.log-handle'), from = centre(handle);
    await commands.mouse([['move', ...from], ['down'], ['move', from[0], from[1] - 40], ['move', from[0], from[1] - 90], ['up']]);
    await settle();
    expect(Math.abs(height() - start - 90)).toBeLessThanOrEqual(2);
    expect(Math.abs(plots - tiles().height - 90), 'the plots give it the room').toBeLessThanOrEqual(2);
    const tall = height(), saved = JSON.parse(localStorage.getItem(KEY));
    expect(saved.open).toBe(true);
    expect(saved.size).toBeGreaterThan(SIZE.default);

    await clickOn(drawer().querySelector('.dock-button'));
    expect(await until(() => !drawer()), 'the drawer closes').toBe(true);
    await settle();
    expect(tiles().height, 'the plots have all the room again').toBeGreaterThan(plots);
    expect(JSON.parse(localStorage.getItem(KEY))).toEqual({ ...saved, open: false });
    await open();
    expect(Math.abs(height() - tall)).toBeLessThanOrEqual(1);

    // The next visit finds it open, as tall.
    page.dispose();
    page = await twoDevices({ [KEY]: saved });
    expect(await until(drawer), 'it loads open').toBe(true);
    await settle();
    expect(Math.abs(height() - tall)).toBeLessThanOrEqual(1);
    // A double-click on its edge puts its height back.
    await commands.mouse([['dblclick', ...centre(document.querySelector('.log-handle'))]]);
    await settle();
    expect(Math.abs(height() - start)).toBeLessThanOrEqual(1);
    expect(JSON.parse(localStorage.getItem(KEY))).toEqual({ open: true, size: SIZE.default });
  });

  it('is opened by its key, reached by F6 and Tab control by control, its rows by the arrows, shown on the plots by Enter, and closed by its key', async () => {
    page = await twoDevices();
    const ids = plotIds(page.ws.layout);
    page.hub.line('info', 'first', { time: 12 });
    page.hub.line('warn', 'second', { time: 15 });
    page.hub.line('info', 'third', { time: 16 });
    await focusPage();
    await pressKey('`');
    expect(await until(drawer), 'the key opens the drawer').toBe(true);
    expect(lastKey()).toMatchObject({ key: '`', by: 'page' });
    await settle();
    // F6 steps through the page's regions to it.
    let reached = false;
    for (let i = 0; i < 8 && !reached; i++) {
      await pressKey('F6');
      await settle(1);
      reached = !!document.activeElement?.closest('.log-frame');
    }
    expect(reached, 'F6 reaches the drawer').toBe(true);
    expect(document.activeElement, 'a region the keys were never in takes focus itself').toBe(drawer());
    // Enter goes in, and Tab goes around its controls, each once.
    await pressKey('Enter');
    await settle(1);
    expect(document.activeElement).toBe(levelToggle('error'));
    const stops = [document.activeElement];
    for (let i = 0; i < 16; i++) {
      await pressKey('Tab');
      await settle(1);
      const at = document.activeElement;
      expect(at?.closest('.log-frame'), 'Tab stays in the drawer').toBe(drawer());
      if (stops.includes(at)) break;
      stops.push(at);
    }
    const controls = [...['error', 'warn', 'info', 'debug'].map(levelToggle), lossToggle(), sourceSelect(), filterField(), followButton(), drawer().querySelector('.log-copy'), list(), drawer().querySelector('.dock-button')];
    expect(new Set(stops)).toEqual(new Set(controls));
    // A toggle the keys reached is pressed by Space.
    levelToggle('debug').focus();
    await pressKey('Tab');
    await pressKey('Shift+Tab');
    expect(document.activeElement).toBe(levelToggle('debug'));
    await pressKey(' ');
    await settle();
    expect(pressed(levelToggle('debug'))).toBe(true);
    // The arrows move through the rows from the newest, the ring on the row they are on; Enter shows it on the plots.
    while (document.activeElement !== list()) await pressKey('Tab');
    await pressKey('ArrowUp');
    await settle();
    const current = () => rowEls().filter((row) => row.getAttribute('aria-selected') === 'true').map((row) => cell(row, 'msg'));
    expect(current()).toEqual(['third']);
    await pressKey('ArrowUp');
    await settle();
    expect(current()).toEqual(['second']);
    expect(lastKey()).toMatchObject({ key: 'ArrowUp', by: 'log' });
    const ring = document.querySelector('.focus-ring').getBoundingClientRect(), row = rowEls()[1].getBoundingClientRect();
    expect(Math.abs(ring.top - row.top) <= 4 && Math.abs(ring.bottom - row.bottom) <= 4, 'the ring is around the row').toBe(true);
    await pressKey('Home');
    await settle();
    expect(current()).toEqual(['first']);
    await pressKey('ArrowDown');
    await pressKey('Enter');
    await settle();
    expect(ids.map((id) => page.ws.plots[id].cursors?.x?.a ?? null)).toEqual([15, 15]);
    // Escape goes out of the list to the drawer, and the key closes it: focus goes to the Log item.
    await pressKey('Escape');
    await settle(1);
    expect(cursorOf(document.activeElement)).toBe(drawer());
    await pressKey('`');
    expect(await until(() => !drawer()), 'the key closes the drawer').toBe(true);
    await settle();
    expect(document.activeElement).toBe(logItem());
  });

  it('at 900 by 600 cuts off and overlaps nothing, and is drawn at its neighbours’ sizes', async () => {
    await browser.viewport(900, 600);
    page = await twoDevices();
    story(page.hub);
    await open();
    await clickOn(levelToggle('debug'));
    await settle();
    const box = (el) => el.getBoundingClientRect();
    const inside = (el, outer) => box(el).left >= box(outer).left - 0.5 && box(el).right <= box(outer).right + 0.5 && box(el).top >= box(outer).top - 0.5 && box(el).bottom <= box(outer).bottom + 0.5;
    const apart = (a, b) => box(a).right <= box(b).left + 0.5 || box(b).right <= box(a).left + 0.5 || box(a).bottom <= box(b).top + 0.5 || box(b).bottom <= box(a).top + 0.5;
    const whole = (el) => el.scrollWidth <= el.clientWidth && el.scrollHeight <= el.clientHeight;
    // The bar's controls, found on the page: one row, each whole, inside the frame, clear of the others and of the frame's button.
    const topBar = drawer().querySelector('.log-bar');
    const controls = [...topBar.children, drawer().querySelector('.dock-button')];
    expect(controls.length).toBeGreaterThanOrEqual(7);
    for (const c of controls) {
      expect(inside(c, drawer()), `${c.className} inside the frame`).toBe(true);
      expect(whole(c), `${c.className} whole`).toBe(true);
      for (const d of controls) if (c !== d) expect(apart(c, d), `${c.className} clear of ${d.className}`).toBe(true);
    }
    expect(new Set([...topBar.children].map((c) => Math.round(box(c).top + box(c).height / 2))).size, 'the bar is one row').toBe(1);
    expect(box(filterField()).width).toBeGreaterThanOrEqual(80);
    // Each row's parts: inside it, clear of each other, and the time, the level and the count whole.
    expect(rowEls().length).toBeGreaterThan(3);
    for (const row of rowEls()) {
      const parts = [...row.children];
      for (const p of parts) {
        expect(inside(p, row), `${p.className} of ${cell(row, 'msg')}`).toBe(true);
        for (const q of parts) if (p !== q) expect(apart(p, q), `${p.className} and ${q.className} of ${cell(row, 'msg')}`).toBe(true);
      }
      for (const name of ['t', 'lv', 'n']) expect(row.querySelector(`.${name}`).scrollWidth, `${name} of ${cell(row, 'msg')}`).toBeLessThanOrEqual(row.querySelector(`.${name}`).clientWidth);
      // A message cut short is whole in its tooltip.
      expect(row.querySelector('.msg').title).toBe(cell(row, 'msg'));
    }
    expect(viewport().scrollWidth, 'the list does not scroll sideways').toBeLessThanOrEqual(viewport().clientWidth);
    // The drawer is above the status bar, under the docks and the tiles, and the status bar's items are clear of each other.
    expect(box(drawer()).bottom).toBeLessThanOrEqual(box(bar()).top);
    const frames = [...document.querySelectorAll('.dock-frame:not(.log-frame)')];
    for (const f of [...frames, document.querySelector('.tiles')]) expect(box(f).bottom).toBeLessThanOrEqual(box(drawer()).top);
    const items = [...bar().querySelectorAll('.item')];
    for (const a of items) {
      expect(inside(a, bar()), `${a.textContent.trim()} inside the bar`).toBe(true);
      for (const b of items) if (a !== b) expect(apart(a, b), `${a.textContent.trim()} clear of ${b.textContent.trim()}`).toBe(true);
    }

    // Its neighbours' sizes: a dock's frame, its edges and the gap to it; a toolbar's controls; a list's rows and type.
    const [left, right] = frames, look = (el, ...props) => props.map((p) => getComputedStyle(el)[p]);
    const FRAME = ['borderTopWidth', 'borderTopColor', 'borderTopLeftRadius', 'backgroundColor', 'paddingTop', 'paddingLeft'];
    expect(look(drawer(), ...FRAME)).toEqual(look(left, ...FRAME));
    expect([box(drawer()).left, box(drawer()).right]).toEqual([box(left).left, box(right).right]);
    const gap = box(document.querySelector('.tiles')).left - box(left).right;
    expect(box(drawer()).top - box(left).bottom, 'as far under the docks as the tiles are beside them').toBe(gap);
    const liveButton = [...document.querySelectorAll('.top-bar button')].find((b) => b.textContent.trim() === 'Live');
    const heights = [drawer().querySelector('.seg'), lossToggle().parentElement, sourceSelect(), filterField(), followButton(), drawer().querySelector('.log-copy')].map((el) => box(el).height);
    expect(heights).toEqual(heights.map(() => box(liveButton).height));
    expect(look(followButton(), 'fontSize', 'fontFamily', 'borderTopLeftRadius')).toEqual(look(liveButton, 'fontSize', 'fontFamily', 'borderTopLeftRadius'));
    const device = document.querySelector('.device'), dockHead = document.querySelector('.dock-head');
    expect(box(rowEls()[0]).height).toBe(box(device).height);
    expect(look(rowEls()[0], 'fontFamily', 'fontSize')).toEqual(look(device, 'fontFamily', 'fontSize'));
    expect(look(drawer().querySelector('.log-title'), 'fontFamily', 'fontSize', 'fontWeight', 'color')).toEqual(look(dockHead, 'fontFamily', 'fontSize', 'fontWeight', 'color'));
    const closeButton = drawer().querySelector('.dock-button'), dockButton = left.querySelector('.dock-button');
    expect([box(closeButton).width, box(closeButton).height, box(drawer()).right - box(closeButton).right, box(closeButton).top - box(drawer()).top]).toEqual([box(dockButton).width, box(dockButton).height, box(left).right - box(dockButton).right, box(dockButton).top - box(left).top]);
  });

  it('changes no control but its counts, which only grow, at any frame while lines stream', async () => {
    page = await twoDevices();
    await open();
    /** Each control of the drawer's bar, as it looks: where it is, its classes and what it says, its count apart. */
    const looks = () =>
      [...drawer().querySelector('.log-bar').children, drawer().querySelector('.dock-button')].flatMap((c) => (c.classList.contains('seg') ? [...c.children] : [c])).map((c) => {
        const r = c.getBoundingClientRect(), num = c.querySelector('.num');
        const words = [...c.childNodes].filter((n) => n !== num).map((n) => n.textContent.trim()).join(' ');
        return { look: `${r.left} ${r.top} ${r.width} ${r.height} ${c.className} ${c.getAttribute('aria-pressed')} ${words}`, count: num ? Number(num.textContent.trim() || 0) : null };
      });
    const step = fc.record({ level: fc.constantFrom('error', 'warn', 'info', 'debug'), lines: fc.integer({ min: 0, max: 3 }), loss: fc.boolean(), frames: fc.integer({ min: 1, max: 4 }) });
    let changed = 0;
    await fc.assert(
      fc.asyncProperty(fc.array(step, { minLength: 4, maxLength: 12 }), fc.boolean(), async (steps, scrolledUp) => {
        // Enough rows to scroll, then following or not.
        for (let i = 0; i < 30; i++) page.hub.line('info', `filler ${i} ${changed}`);
        await settle();
        viewport().scrollTop = scrolledUp ? 0 : viewport().scrollHeight;
        await settle();
        expect(pressed(followButton())).toBe(!scrolledUp);
        let before = looks();
        for (const s of steps) {
          for (let i = 0; i < s.lines; i++) page.hub.line(s.level, `streamed ${changed} ${i}`);
          if (s.loss) page.hub.lose('/', 'sine', { skipped: 2 });
          for (let f = 0; f < s.frames; f++) {
            await frames(1);
            const now = looks();
            expect(now.map((c) => c.look), `after ${JSON.stringify(s)}`).toEqual(before.map((c) => c.look));
            now.forEach((c, i) => c.count !== null && expect(c.count, c.look).toBeGreaterThanOrEqual(before[i].count));
            if (now.some((c, i) => c.count !== before[i].count)) changed++;
            before = now;
          }
        }
      }),
      withExamples([[[{ level: 'warn', lines: 3, loss: true, frames: 2 }, { level: 'debug', lines: 2, loss: false, frames: 1 }, { level: 'error', lines: 1, loss: false, frames: 3 }, { level: 'info', lines: 3, loss: true, frames: 1 }], true]]),
    );
    expect(changed, 'frames at which a count grew').toBeGreaterThan(0);
  }, 180_000);
});

describe('the log, to look at', () => {
  it('with every level, a repeat and an outage, light and dark, at 1280 by 800 and 900 by 600; filtered to lost data; and beside the Properties panels and the History popover', async () => {
    for (const theme of ['light', 'dark']) {
      for (const [w, h] of [[1280, 800], [900, 600]]) {
        prefs.setTheme(theme);
        await browser.viewport(w, h);
        page = await twoDevices();
        page.hub.text({ type: 'memory', bytes: 759e6, budget: 1e9, seconds: 600, max_age: 600, rate: 1.2e6, oldest: { 0: 0, 1: 0 } });
        story(page.hub);
        await open();
        await clickOn(levelToggle('debug'));
        await wait(1200);
        await settle();
        expect(rows().length).toBeGreaterThan(8);
        await shot(`page-${theme}-${w}x${h}`);
        // A row under the pointer, and the row the arrows are on.
        await commands.mouse([['move', ...centre(rowEls().at(-3))]]);
        list().focus();
        await pressKey('ArrowUp');
        await settle();
        await shot(`rows-hover-focus-${theme}-${w}x${h}`);
        await clickOn(lossToggle());
        await settle();
        expect(rows().every((r) => r[1] === 'hub')).toBe(true);
        await shot(`loss-only-${theme}-${w}x${h}`);
        await clickOn(lossToggle());
        await clickOn(sourceSelect());
        expect(await until(() => document.querySelector('[data-slot=select-content]')), 'the sources open').toBe(true);
        await settle();
        await shot(`sources-open-${theme}-${w}x${h}`);
        await closeAll();
        await clickOn(bar().querySelector('.history'));
        expect(await until(() => document.querySelector('[data-slot=popover-content]')), 'the History popover opens').toBe(true);
        await settle();
        await shot(`beside-properties-and-history-${theme}-${w}x${h}`);
        await closeAll();
        // The axis popover's strips beside the drawer's toggles, to compare the two.
        if (w === 1280) {
          await axisPopover(page, plotIds(page.ws.layout)[0], 'y');
          await settle();
          await shot(`toggles-beside-axis-popover-${theme}`);
          await closeAll();
        }
        page.dispose();
        page = null;
      }
    }
  }, 240_000);
});
