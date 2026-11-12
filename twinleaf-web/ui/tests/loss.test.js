// What the page lost, as the status bar and the plots say it:
// - with nothing lost, the status bar has no loss item;
// - each kind of loss the hub tells of reads as its own phrase beside the
//   data rate, the cause that stands for the most data first, and the
//   item keeps its width as its numbers grow;
// - its popover counts, by device, what was lost since the page opened,
//   and the page's own falling behind; Reset sets every count to nothing,
//   and the item goes once its popover closes;
// - a minute after the last loss the item goes, and the counts stay;
// - a snapshot sent in place of changes the page fell behind is a loss of
//   its own;
// - a plot marks a gap a sample wide with one dashed line, and a gap wide
//   enough to see with a band from the last point before it to the first
//   after.
// The hub here is the stub, which is sent each kind of `loss` as a hub's
// change.
import { afterEach, describe, expect, it } from 'vitest';
import { page as browser } from 'vitest/browser';
import { clickOn, closeAll, mountPage, settle, until, wait } from './harness/page.js';
import * as clock from '../src/lib/clock.js';
import { RECENT_MS } from '../src/lib/loss.svelte.js';
import { plotIds } from '../src/lib/layout.js';
import { LOSS_COUNTS, LOSS_NO_DEVICE, LOSS_NONE_RECENT, LOSS_RESET, lossSince, lossWords } from '../src/lib/words.js';

/** Where VITE_TL_SHOTS names a folder, relative to this file, the screenshots go there. */
const SHOTS = import.meta.env.VITE_TL_SHOTS;
const shot = async (name) => SHOTS && (await browser.screenshot({ path: `${SHOTS}/${name}.png` }));

let page;
afterEach(() => page?.dispose());

/** A page on a hub of two devices, each with a 100 Hz stream `sine`. */
const twoDevices = () => mountPage([['sine.sine']], 'grid', 0, ['sine.sine'], [], { routes: ['/', '/0'] });
/** Has the hub send `message` as its next change. */
async function say(message) {
  page.hub.text({ ...message, seq: page.hub.seq + 1 });
  await settle();
}
/** A `loss` of the device at `route`: its link's drops and seconds, and its stream sine's samples skipped, and dropped by the proxy and by the hub. */
const loss = (route, times, seconds, [skipped = 0, proxy = 0, hub = 0] = []) => ({ type: 'loss', route, link: { times, seconds }, streams: { sine: { skipped, link: 0, proxy, hub } } });

/** The status bar's loss item. */
const item = () => document.querySelector('.status-bar .loss');
/** What it says: each cause's phrase, in order, with the spaces of the numbers' fixed widths taken out. */
const said = () => [...item().querySelectorAll('.cause')].map((el) => el.textContent.replace(/\s+/g, ' ').trim());
/** The phrase of a cause as words.js writes it. */
const phrase = (cause) => lossWords(cause).map((part) => part.num ?? part).join('');
/** The open loss popover. */
const popover = () => document.querySelector('[data-slot=popover-content] .loss-counts');
/** Its rows: each device's label and counts, by route. */
const rows = () => Object.fromEntries([...popover().querySelectorAll('tr[data-route]')].map((tr) => [tr.dataset.route, [...tr.querySelectorAll('td')].map((td) => td.textContent.trim())]));

describe('the status bar', () => {
  it('says nothing of loss while nothing was lost', async () => {
    page = await twoDevices();
    expect(item()).toBe(null);
    // What a hub had lost before the page heard it, which its snapshot holds, is not the page's.
    page.hub.text(loss('/', 3, 40, [500, 20, 10]));
    await settle();
    expect(item()).toBe(null);
    await shot('loss-none');
  });

  it('says each cause of the last minute apart, the one that stands for the most data first', async () => {
    page = await twoDevices();
    await say(loss('/', 0, 0, [312]));
    expect(said()).toEqual(['312 samples skipped']);
    await shot('loss-skipped');
    // At 100 samples a second, 2100 dropped are 21 s of data, past the link's 11 s and the 3 s skipped.
    await say(loss('/', 0, 0, [312, 2000, 100]));
    await say(loss('/0', 4, 11));
    expect(said()).toEqual(['hub dropped 2.1 k', 'link dropped 4×, 11 s', '312 samples skipped']);
    expect(said()).toEqual(page.hub.loss.recent.map(phrase));
    expect([...item().querySelectorAll('.cause')].map((el) => el.dataset.cause)).toEqual(['dropped', 'link', 'skipped']);
    await shot('loss-three-causes');
  });

  it('keeps the loss item as wide while its numbers grow', async () => {
    page = await twoDevices();
    await say(loss('/', 1, 2, [3, 0, 40]));
    const before = item().getBoundingClientRect(), next = item().nextElementSibling.getBoundingClientRect().left;
    await say(loss('/', 9, 720, [999, 0, 120_000]));
    expect(said()).toEqual(['hub dropped 120 k', 'link dropped 9×, 12 min', '999 samples skipped']);
    expect(item().getBoundingClientRect().width).toBe(before.width);
    expect(item().nextElementSibling.getBoundingClientRect().left, 'what follows it stays put').toBe(next);
  });

  it('counts in its popover what was lost since the page opened, by device, and Reset counts from nothing', async () => {
    page = await twoDevices();
    await say(loss('/', 0, 0, [312, 2000, 100]));
    await say(loss('/0', 4, 11));
    // The page fell behind: the snapshot in its place holds the same hub, and its devices again.
    page.hub.text({ type: 'hub', id: page.hub.session.id, seq: page.hub.seq, behind: 1500 });
    await say({ type: 'routes', mounts: {}, routes: ['/', '/0'] });
    await say({ type: 'device', route: '/', name: 'test', serial: '/', firmware: '1', presence: 'alive' });
    await clickOn(item());
    expect(await until(popover), 'the popover opens').toBe(true);
    expect(popover().querySelector('.loss-head span').textContent).toBe(lossSince(false));
    expect(rows()).toEqual({ '/': ['test', '0×', '312', '2.1 k'], '/0': ['device (/0)', '4×, 11 s', '0', '0'] });
    expect(popover().querySelector('[data-count=behind]').textContent).toBe('1×');
    expect([...popover().querySelectorAll('th')].slice(1).map((th) => [th.textContent, th.title])).toEqual(['link', 'skipped', 'dropped'].map((kind) => [LOSS_COUNTS[kind].head, LOSS_COUNTS[kind].tip]));
    await shot('loss-popover');

    await clickOn([...popover().querySelectorAll('button')].find((b) => b.textContent.trim() === LOSS_RESET));
    await settle();
    expect(rows()).toEqual({});
    expect(popover().querySelector('td.none').textContent).toBe(LOSS_NO_DEVICE);
    expect(popover().querySelector('[data-count=behind]').textContent).toBe('0×');
    expect(popover().querySelector('.loss-head span').textContent).toBe(lossSince(true));
    expect(said(), 'the item stays under its open popover').toEqual([LOSS_NONE_RECENT]);
    await shot('loss-popover-reset');
    await closeAll();
    expect(await until(() => !item()), 'the item goes with its popover').toBe(true);
    // The counts go on from what the hub said last.
    await say(loss('/0', 5, 13));
    expect(said()).toEqual(['link dropped 1×, 2 s']);
  });

  it('drops the loss item a minute after the last loss, and keeps the counts', async () => {
    page = await twoDevices();
    await say(loss('/', 0, 0, [7]));
    expect(said()).toEqual(['7 samples skipped']);
    await wait(RECENT_MS - 2000);
    await settle();
    expect(said()).toEqual(['7 samples skipped']);
    await wait(3000);
    await settle();
    expect(item()).toBe(null);
    expect(page.hub.loss.devices.get('/')).toEqual({ drops: 0, seconds: 0, skipped: 7, dropped: 0 });
  });

  it('says so when the page fell behind the hub', async () => {
    page = await twoDevices();
    page.hub.text({ type: 'hub', id: page.hub.session.id, seq: page.hub.seq, behind: 2048 });
    await settle();
    expect(said()).toEqual(['page behind 1×']);
    await shot('loss-behind');
  });
});

describe('a plot', () => {
  it('marks a gap a sample wide with a line, and one wide enough to see with a band across it', async () => {
    page = await mountPage([['sine.sine']], 'grid', 0, ['sine.sine']);
    const session = page.hub.session, id = plotIds(page.ws.layout)[0];
    const points = (from, to, brk) => {
      const xs = Float64Array.from({ length: Math.round((to - from) * 100) }, (_, i) => from + i / 100);
      session.apply({ kind: 'points', chunks: [{ id: 0, level: 0, brk, xs, ys: Float32Array.from(xs, (x) => Math.sin(x)) }] }, clock.now());
    };
    // Three seconds lost from 13 s, as while a link is down, and one sample at 17 s.
    points(0, 13, false);
    points(16, 17, true);
    points(17.01, 20, true);
    session.tick(clock.now());
    await settle();
    const pv = page.pv(id), [lo, hi] = pv.xd, px = (seconds) => (seconds * pv.geo.plot.w) / (hi - lo);
    expect(lo, 'both gaps are in view').toBeLessThan(12.9);
    expect(pv.gaps.map((gap) => gap.band)).toEqual([true, false]);
    const [outage, skip] = pv.gaps;
    expect(Math.abs(outage.x1 - outage.x0 - px(3.01))).toBeLessThanOrEqual(1);
    expect(skip.x1 - skip.x0).toBe(0);
    expect(skip.x1 - outage.x1, 'a second on from the outage’s end').toBeGreaterThan(px(1) - 1);
    await shot('loss-plot-gaps');
  });
});
