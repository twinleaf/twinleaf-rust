// What the page lost, as the status bar and the plots say it:
// - with nothing lost, the status bar has no loss mark;
// - while the last minute lost anything, the mark reads the share of that
//   minute's data that is missing and the cause that stands for the most
//   of it, in the warning's colour, under the warning's icon;
// - while a device's link is down, it reads that and for how long, in the
//   error's colour, under an icon of its own;
// - its tooltip is one line of what the page counts lost since it opened;
// - a press on it opens the log on the lines of lost data;
// - a minute after the last loss the mark goes, and the loss stays in the
//   log and in the Log item's count of what is unseen;
// - a snapshot sent in place of changes the page fell behind is a loss of
//   its own;
// - nothing else in the status bar moves as the mark comes, counts and
//   goes, and each number in it keeps its width;
// - a plot marks a gap a sample wide with one dashed line, and a gap wide
//   enough to see with a band from the last point before it to the first
//   after.
// The hub here is the stub, which says each loss as the server does: the
// device's presence, the lines of the log and the `loss` counts.
import { afterEach, describe, expect, it } from 'vitest';
import { page as browser, commands } from 'vitest/browser';
import fc from 'fast-check';
import { centre, clickOn, mountPage, settle, until, wait, withExamples } from './harness/page.js';
import * as clock from '../src/lib/clock.js';
import { RECENT_MS } from '../src/lib/loss.svelte.js';
import { plotIds } from '../src/lib/layout.js';
import { LINK_DOWN, LOSS_CAUSE, shareText } from '../src/lib/words.js';

/** Where VITE_TL_SHOTS names a folder, relative to this file, the screenshots go there. */
const SHOTS = import.meta.env.VITE_TL_SHOTS;
const shot = async (name) => SHOTS && (await browser.screenshot({ path: `${SHOTS}/${name}.png` }));

let page;
afterEach(() => page?.dispose());

/** A page on a hub of two devices, VMR 0412 at the root, with a 100 Hz stream `sine`, and OMG 0077 at /0. */
async function twoDevices() {
  const made = await mountPage([['sine.sine']], 'grid', 0, ['sine.sine'], [{ name: 'gain', channel: 'setting', value: 1 }], { routes: ['/', '/0'] });
  made.hub.names.set('/', 'VMR 0412').set('/0', 'OMG 0077');
  made.hub.presence('/', 'alive');
  made.hub.presence('/0', 'alive');
  await settle();
  return made;
}

const bar = () => document.querySelector('.status-bar');
/** The status bar's loss mark. */
const mark = () => bar().querySelector('.loss');
/** What it says: its parts, which the bar sets a gap apart, a space apart. */
const said = () => [...mark().childNodes].map((node) => node.textContent.replace(/\s+/g, ' ').trim()).filter(Boolean).join(' ');
/** The icon it shows, by the name the icon set gives it. */
const icon = () => [...mark().querySelector('svg').classList].find((c) => c.startsWith('lucide-') && c !== 'lucide-icon').slice(7);
/** The colour CSS `value` computes to on the page. */
function colour(value) {
  const probe = document.body.appendChild(document.createElement('i'));
  probe.style.color = value;
  const computed = getComputedStyle(probe).color;
  probe.remove();
  return computed;
}
/** The tooltip showing, by its words, the keys it shows left out. */
const tip = () => [...(document.querySelector('[data-slot=tooltip-content]')?.childNodes ?? [])].filter((n) => n.nodeType === Node.TEXT_NODE).map((n) => n.textContent).join('').trim();
/** Rests the pointer on `el` until its tooltip shows, and gives its words. */
async function hover(el) {
  await commands.mouse([['move', ...centre(el).map((v) => v + 1)], ['move', ...centre(el)]]);
  expect(await until(() => tip() !== ''), 'the tooltip shows').toBe(true);
  return tip();
}
/** Takes the pointer off the status bar, and waits for the tooltip to go. */
async function unhover() {
  await commands.mouse([['move', 300, 300]]);
  await until(() => !document.querySelector('[data-slot=tooltip-content]'));
}

describe('the status bar', () => {
  it('says nothing of loss while nothing was lost', async () => {
    page = await twoDevices();
    expect(mark()).toBe(null);
    // What a hub had lost before the page heard it, which its snapshot holds, is not the page's.
    page.hub.text({ type: 'loss', route: '/', link: { times: 3, seconds: 40 }, streams: { sine: { skipped: 500, link: 0, proxy: 20, hub: 10 } } });
    await settle();
    expect(mark()).toBe(null);
    await shot('status-none');
  });

  it('reads the share of the last minute that is missing and its largest cause, in the warning’s colour, with what was lost since the page opened in its tooltip', async () => {
    page = await twoDevices();
    // 312 of 100 samples a second are 3.12 s of the minute.
    page.hub.lose('/', 'sine', { skipped: 312 });
    await settle();
    expect(said()).toBe(`${shareText(3.12 / 60)} missing · ${LOSS_CAUSE.skipped}`);
    expect(said()).toBe('5% missing · skipped');
    expect([mark().dataset.state, icon(), getComputedStyle(mark()).color]).toEqual(['missing', 'triangle-exclamation-point', colour('var(--cursor-a)')]);
    // 2100 dropped are 21 s more of the same stream: the larger cause.
    page.hub.lose('/', 'sine', { proxy: 2000, hub: 100 });
    await settle();
    expect(said()).toBe('40% missing · dropped');
    expect(await hover(mark())).toMatch(/^Since \d\d:\d\d: hub dropped 2\.1 k · 312 samples skipped\. Click for the log$/);
    await shot('status-missing');
    await unhover();
  });

  it('reads a link that is down now and for how long, in the error’s colour under its own icon, then what that outage lost of the minute', async () => {
    page = await twoDevices();
    page.hub.linkDown('/0');
    await settle();
    // The bar reads the time twice a second.
    await wait(1100);
    expect(said()).toBe(`${LINK_DOWN} 1 s`);
    expect([mark().dataset.state, icon(), getComputedStyle(mark()).color]).toEqual(['down', 'unplug', colour('var(--destructive)')]);
    const words = () => mark().querySelector('.fit');
    const narrow = words().getBoundingClientRect().width;
    await wait(2000);
    expect(said()).toBe(`${LINK_DOWN} 3 s`);
    expect(await hover(mark())).toMatch(/^The link to OMG 0077 \(\/0\) is down\. Since \d\d:\d\d: link dropped 1×, 0 s\. Click for the log$/);
    await shot('status-down');
    await unhover();
    // What it says has the room of the longest it says, so it is no wider then, and it starts at the room's start.
    expect(words().getBoundingClientRect().width).toBe(narrow);
    const longest = mark().appendChild(Object.assign(document.createElement('span'), { textContent: `${LINK_DOWN} 119 min` }));
    expect(longest.getBoundingClientRect().width).toBeLessThanOrEqual(narrow);
    longest.remove();
    expect(words().firstElementChild.getBoundingClientRect().left).toBe(words().getBoundingClientRect().left);
    // Back: the outage is what the last minute is missing.
    page.hub.linkBack('/0', 3);
    await settle();
    expect(said()).toBe('5% missing · link');
    expect(mark().dataset.state).toBe('missing');
  });

  it('opens the log on the lines of lost data when the mark is pressed', async () => {
    page = await twoDevices();
    page.hub.line('info', 'field lock acquired');
    page.hub.lose('/', 'sine', { skipped: 7 });
    page.hub.line('warn', 'field lock lost');
    await settle();
    expect(document.querySelector('.log-frame')).toBe(null);
    await clickOn(mark());
    expect(await until(() => document.querySelector('.log-frame')), 'the drawer opens').toBe(true);
    const rows = [...document.querySelectorAll('.log-frame .log-row .msg')].map((el) => el.textContent.trim());
    expect(rows).toEqual(['VMR 0412: 7 samples of sine skipped']);
    expect(document.querySelector('.log-frame [data-filter=loss]').getAttribute('aria-pressed')).toBe('true');
    await shot('drawer-loss-only');
  });

  it('drops the mark a minute after the last loss, and keeps the loss in the log and in what is unseen of it', async () => {
    page = await twoDevices();
    page.hub.lose('/', 'sine', { skipped: 7 });
    await settle();
    expect(said()).toBe('<1% missing · skipped');
    await wait(RECENT_MS - 2000);
    await settle();
    expect(said()).toBe('<1% missing · skipped');
    await wait(3000);
    await settle();
    expect(mark()).toBe(null);
    expect(bar().querySelector('.log .num').textContent.trim(), 'the warning is unseen still').toBe('1');
    expect(page.hub.log.rows.map((row) => row.loss)).toEqual([{ cause: 'skipped', stream: 'sine', samples: 7 }]);
  });

  it('says so when the page fell behind the hub', async () => {
    page = await twoDevices();
    // 2048 changes are 33 s of the minute.
    page.hub.text({ type: 'hub', id: page.hub.session.id, seq: page.hub.seq, behind: 2048 });
    await settle();
    expect(said()).toBe('55% missing · page');
    await shot('status-behind');
  });

  it('moves nothing else as the mark comes, counts and goes, whatever is lost and logged, keeps the mark as wide, and leaves no item a hole inside what it says', async () => {
    const seen = { came: 0, went: 0, counted: 0 };
    /** Every item of the bar but the mark, each as where it is and how wide. */
    const others = () => [...bar().querySelectorAll('.item:not(.loss)')].map((el) => [el.className, el.getBoundingClientRect().left, el.getBoundingClientRect().width].join(' '));
    const step = fc.oneof(
      fc.record({ t: fc.constant('lose'), cause: fc.constantFrom('skipped', 'proxy', 'hub'), n: fc.integer({ min: 1, max: 500_000 }) }),
      fc.record({ t: fc.constant('line'), level: fc.constantFrom('error', 'warn', 'info') }),
      fc.record({ t: fc.constant('behind'), n: fc.integer({ min: 1, max: 5000 }) }),
      fc.record({ t: fc.constant('wait'), ms: fc.constantFrom(700, 20_000, RECENT_MS + 1000) }),
    );
    await fc.assert(
      fc.asyncProperty(fc.array(step, { minLength: 1, maxLength: 8 }), async (steps) => {
        page?.dispose();
        page = await twoDevices();
        page.hub.text({ type: 'memory', bytes: 5e6, budget: 1e9, seconds: 20, max_age: 600, rate: 1e4, oldest: { 0: 0 } });
        // The frame rate has been read by now.
        await wait(1200);
        await settle();
        const still = others();
        let at = null, numbers = null;
        for (const s of steps) {
          const was = mark();
          if (s.t === 'lose') page.hub.lose('/', 'sine', { [s.cause]: s.n });
          else if (s.t === 'line') page.hub.line(s.level, 'a line');
          else if (s.t === 'behind') {
            page.hub.text({ type: 'hub', id: page.hub.session.id, seq: page.hub.seq, behind: s.n });
            page.hub.text({ type: 'routes', mounts: {}, routes: ['/', '/0'] });
            page.hub.presence('/', 'alive');
            page.hub.presence('/0', 'alive');
            page.hub.text({ type: 'trace', trace: { id: 0, route: '/', stream: 'sine', column: 'sine', units: 'V', rate: 100 } });
            page.hub.text({ type: 'memory', bytes: 5e6, budget: 1e9, seconds: 20, max_age: 600, rate: 1e4, oldest: { 0: 0 } });
          } else await wait(s.ms);
          await settle();
          expect(others(), `after ${JSON.stringify(s)}`).toEqual(still);
          // Every item's words start at the start of their room, the room left over after them.
          for (const fit of bar().querySelectorAll('.fit')) expect(fit.firstElementChild.getBoundingClientRect().left, fit.dataset.room).toBe(fit.getBoundingClientRect().left);
          const now = mark();
          if (now && !was) seen.came++;
          if (was && !now) seen.went++;
          if (!now) continue;
          // The mark stays where it first stood, after History, and its number keeps its width.
          const left = now.getBoundingClientRect().left, width = now.getBoundingClientRect().width;
          // No hole in what it says: its words start where their room does.
          const fit = now.querySelector('.fit');
          expect(fit.firstElementChild.getBoundingClientRect().left).toBe(fit.getBoundingClientRect().left);
          expect(left).toBe(now.previousElementSibling.getBoundingClientRect().right + parseFloat(getComputedStyle(bar()).columnGap));
          expect(now.previousElementSibling.classList.contains('history')).toBe(true);
          if (at !== null) expect([left, width]).toEqual([at, numbers]);
          [at, numbers] = [left, width];
          if (was) seen.counted++;
        }
      }),
      withExamples([
        [[{ t: 'lose', cause: 'skipped', n: 3 }, { t: 'lose', cause: 'hub', n: 120_000 }, { t: 'line', level: 'error' }, { t: 'wait', ms: RECENT_MS + 1000 }]],
        [[{ t: 'behind', n: 9 }, { t: 'lose', cause: 'proxy', n: 999 }, { t: 'wait', ms: 20_000 }, { t: 'behind', n: 4000 }]],
      ]),
    );
    for (const [what, n] of Object.entries(seen)) expect(n, what).toBeGreaterThan(0);
  }, 180_000);
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
