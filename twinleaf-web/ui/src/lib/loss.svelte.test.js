import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { CAUSES, Losses, RECENT_MS } from './loss.svelte.js';
import { Hub } from './hub.svelte.js';
import { lossTip, lossWords, shareText } from './words.js';

/** A `loss` message of the device at `route`: its link's drops and seconds, and its streams' counts. */
const loss = (route, times, seconds, streams = {}) => ({
  type: 'loss',
  route,
  link: { times, seconds },
  streams: Object.fromEntries(Object.entries(streams).map(([name, [skipped = 0, proxy = 0, hub = 0, link = 0]]) => [name, { skipped, link, proxy, hub }])),
});
/** Every stream at 1000 samples a second. */
const kHz = () => 1000;
/** A device's counts as the page keeps them. */
const counts = (losses, route = '/') => losses.devices.get(route) ?? null;

describe('what the page counts lost', () => {
  it('starts where a hub new to the page stood, and counts what each message adds', () => {
    const losses = new Losses();
    losses.snapshot('a', 0, 0);
    losses.apply(loss('/', 3, 20, { sine: [100, 5, 7] }), true, kHz, 0);
    expect(counts(losses), 'what the hub lost before the page heard it').toBe(null);
    expect(losses.recent).toEqual([]);
    expect([losses.missing, losses.totals]).toEqual([null, []]);

    losses.apply(loss('/', 4, 22.5, { sine: [130, 6, 9] }), false, kHz, 1000);
    expect(counts(losses)).toEqual({ drops: 1, seconds: 2.5, skipped: 30, dropped: 3 });
    losses.apply(loss('/', 4, 22.5, { sine: [130, 6, 9] }), false, kHz, 2000);
    expect(counts(losses), 'the same counts again add nothing').toEqual({ drops: 1, seconds: 2.5, skipped: 30, dropped: 3 });
  });

  it('counts from nothing a device whose first loss comes as a change, or in a later snapshot of the same hub', () => {
    const losses = new Losses();
    losses.snapshot('a', 0, 0);
    losses.apply(loss('/', 0, 0, { sine: [12] }), false, kHz, 0);
    expect(counts(losses)).toEqual({ drops: 0, seconds: 0, skipped: 12, dropped: 0 });
    // The page fell behind: the snapshot in place of what it missed holds the counts as they are now.
    losses.snapshot('a', 2000, 500);
    losses.apply(loss('/', 0, 0, { sine: [20] }), true, kHz, 500);
    losses.apply(loss('/1', 1, 3, {}), true, kHz, 500);
    expect(counts(losses)).toEqual({ drops: 0, seconds: 0, skipped: 20, dropped: 0 });
    expect(counts(losses, '/1')).toEqual({ drops: 1, seconds: 3, skipped: 0, dropped: 0 });
    expect(losses.behind).toBe(1);
  });

  it('keeps its counts across hubs, each counted from where the page first heard it', () => {
    const losses = new Losses();
    losses.snapshot('a', 0, 0);
    losses.apply(loss('/', 1, 2, { sine: [10] }), false, kHz, 0);
    losses.snapshot('b', 0, 100);
    losses.apply(loss('/', 7, 70, { sine: [700] }), true, kHz, 100);
    losses.apply(loss('/', 8, 71, { sine: [705] }), false, kHz, 200);
    expect(counts(losses)).toEqual({ drops: 2, seconds: 3, skipped: 15, dropped: 0 });
  });

  it('adds the proxy’s drops and the hub’s as dropped, and leaves out the rows a dropped link lost', () => {
    const losses = new Losses();
    losses.snapshot('a', 0, 0);
    losses.apply(loss('/', 1, 4, { sine: [1, 20, 300, 4000] }), false, kHz, 0);
    expect(counts(losses)).toEqual({ drops: 1, seconds: 4, skipped: 1, dropped: 320 });
  });

  it('totals every device’s counts and its own falling behind, each cause that lost anything, since the time of day it was made at', () => {
    const losses = new Losses(1234);
    expect(losses.since).toBe(1234);
    losses.snapshot('a', 0, 0);
    losses.apply(loss('/', 1, 4, { sine: [50, 2, 3] }), false, kHz, 0);
    losses.apply(loss('/1', 2, 6.5, { sine: [7] }), false, kHz, 0);
    expect(losses.totals).toEqual([
      { kind: 'link', n: 3, seconds: 10.5 },
      { kind: 'dropped', n: 5, seconds: 0 },
      { kind: 'skipped', n: 57, seconds: 0 },
    ]);
    losses.snapshot('a', 4000, 10);
    expect(losses.totals.at(-1)).toEqual({ kind: 'behind', n: 1, seconds: 0 });
    // The totals outlast the minute.
    losses.tick(10 + RECENT_MS);
    expect([losses.recent, losses.totals.map((cause) => cause.kind)]).toEqual([[], ['link', 'dropped', 'skipped', 'behind']]);
  });
});

describe('the share of the last minute that is missing', () => {
  it('is that of the device that lost the most: its link’s outage and its worst stream’s samples, with what the page fell behind, over the minute', () => {
    const losses = new Losses();
    losses.snapshot('a', 0, 0);
    expect(losses.missing).toBe(null);
    const rateOf = (stream) => ({ slow: 10, fast: 1000 })[stream];
    // 3 s of the slow stream and 1.5 s of the fast one: the slow stream's hole is the device's.
    losses.apply(loss('/', 0, 0, { slow: [30], fast: [1000, 500] }), false, rateOf, 0);
    expect(losses.missing).toBeCloseTo(3 / 60, 10);
    // A 6 s outage of the device's link is missing of its every stream.
    losses.apply(loss('/', 1, 6, { slow: [30], fast: [1000, 500] }), false, rateOf, 1000);
    expect(losses.missing).toBeCloseTo(9 / 60, 10);
    // Another device that lost less changes nothing; one that lost more is the share.
    losses.apply(loss('/1', 1, 8, {}), false, rateOf, 2000);
    expect(losses.missing).toBeCloseTo(9 / 60, 10);
    losses.apply(loss('/1', 1, 12, {}), false, rateOf, 3000);
    expect(losses.missing).toBeCloseTo(12 / 60, 10);
    // 125 changes behind are 2 s, missing of every device.
    losses.snapshot('a', 125, 4000);
    expect(losses.missing).toBeCloseTo(14 / 60, 10);
  });

  it('is never more than all of it, and nothing once the minute has passed', () => {
    const losses = new Losses();
    losses.snapshot('a', 0, 0);
    losses.apply(loss('/', 1, 500, {}), false, kHz, 0);
    expect(losses.missing).toBe(1);
    losses.tick(RECENT_MS - 1);
    expect([losses.missing, losses.now]).toEqual([1, RECENT_MS - 1]);
    losses.tick(RECENT_MS);
    expect([losses.missing, losses.now]).toEqual([null, RECENT_MS]);
  });
});

describe('what was lost in the last minute', () => {
  it('lists each cause once, summed, the one that stands for the most data first', () => {
    const losses = new Losses();
    losses.snapshot('a', 0, 0);
    // 0.3 s of samples skipped, 2.1 s dropped, an 11 s outage in four drops, and 16 ms of changes behind.
    losses.apply(loss('/', 0, 0, { sine: [300] }), false, kHz, 0);
    losses.apply(loss('/', 0, 0, { sine: [300, 0, 2100] }), false, kHz, 100);
    losses.apply(loss('/', 3, 8, { sine: [300, 0, 2100] }), false, kHz, 200);
    losses.apply(loss('/', 4, 11, { sine: [300, 0, 2100] }), false, kHz, 300);
    losses.snapshot('a', 1, 400);
    expect(losses.recent).toEqual([
      { kind: 'link', n: 4, seconds: 11 },
      { kind: 'dropped', n: 2100, seconds: 0 },
      { kind: 'skipped', n: 300, seconds: 0 },
      { kind: 'behind', n: 1, seconds: 0 },
    ]);
    expect(losses.recent.map((cause) => lossWords(cause).map((part) => part.num ?? part).join(''))).toEqual(['link dropped 4×, 11 s', 'hub dropped 2.1 k', '300 samples skipped', 'page behind 1×']);
  });

  it('weighs samples by their stream’s rate, and those of a stream the page shows none of last', () => {
    const losses = new Losses();
    losses.snapshot('a', 0, 0);
    const rateOf = (stream) => ({ slow: 10, fast: 10_000 })[stream];
    // 50 samples of a 10 Hz stream are 5 s, 5000 of a 10 kHz stream half a second.
    losses.apply(loss('/', 0, 0, { slow: [50], fast: [0, 5000], unseen: [0, 0, 0] }), false, rateOf, 0);
    expect(losses.recent.map((cause) => cause.kind)).toEqual(['skipped', 'dropped']);
    const others = new Losses();
    others.snapshot('a', 0, 0);
    others.apply(loss('/', 0, 0, { fast: [0, 5000] }), true, rateOf, 0);
    others.apply(loss('/', 0, 0, { fast: [0, 5001], unseen: [900_000] }), false, rateOf, 0);
    expect(others.recent, 'a stream of no known rate stands for no time').toEqual([
      { kind: 'dropped', n: 1, seconds: 0 },
      { kind: 'skipped', n: 900_000, seconds: 0 },
    ]);
  });

  it('forgets a loss a minute after it, and keeps it in the counts', () => {
    const losses = new Losses();
    losses.snapshot('a', 0, 0);
    losses.apply(loss('/', 0, 0, { sine: [5] }), false, kHz, 1000);
    losses.apply(loss('/', 1, 2, { sine: [5] }), false, kHz, 31_000);
    losses.tick(1000 + RECENT_MS - 1);
    expect(losses.recent.map((cause) => cause.kind)).toEqual(['link', 'skipped']);
    losses.tick(1000 + RECENT_MS);
    expect(losses.recent).toEqual([{ kind: 'link', n: 1, seconds: 2 }]);
    losses.tick(31_000 + RECENT_MS);
    expect(losses.recent).toEqual([]);
    expect(counts(losses)).toEqual({ drops: 1, seconds: 2, skipped: 5, dropped: 0 });
  });

  it('holds, of any losses, each cause at most once and no count but what the messages added', () => {
    const step = fc.record({ times: fc.nat(3), seconds: fc.nat(5), skipped: fc.nat(50), proxy: fc.nat(50), hub: fc.nat(50), behind: fc.nat(2) });
    fc.assert(
      fc.property(fc.array(step, { maxLength: 12 }), (steps) => {
        const losses = new Losses();
        losses.snapshot('a', 0, 0);
        const total = { times: 0, seconds: 0, skipped: 0, proxy: 0, hub: 0, behind: 0 };
        steps.forEach((s, i) => {
          for (const k of Object.keys(total)) total[k] += s[k];
          if (s.behind) losses.snapshot('a', s.behind, i);
          losses.apply(loss('/', total.times, total.seconds, { sine: [total.skipped, total.proxy, total.hub] }), false, kHz, i);
        });
        const kinds = losses.recent.map((cause) => cause.kind);
        expect(new Set(kinds).size).toBe(kinds.length);
        expect(kinds.every((kind) => CAUSES.includes(kind))).toBe(true);
        const of = (kind) => losses.recent.find((cause) => cause.kind === kind) ?? { n: 0, seconds: 0 };
        expect([of('link').n, of('link').seconds, of('skipped').n, of('dropped').n]).toEqual([total.times, total.seconds, total.skipped, total.proxy + total.hub]);
        expect(of('behind').n).toBe(steps.filter((s) => s.behind).length);
        expect(counts(losses) ?? { drops: 0, seconds: 0, skipped: 0, dropped: 0 }).toEqual({ drops: total.times, seconds: total.seconds, skipped: total.skipped, dropped: total.proxy + total.hub });
      }),
    );
  });
});

describe('the loss mark’s words', () => {
  it('read a share in whole percents, never as nothing, in no more characters than all of it', () => {
    const shares = [[0, '<1%'], [0.0049, '<1%'], [0.005, '1%'], [0.052, '5%'], [0.184, '18%'], [0.995, '100%'], [1, '100%']];
    expect(shares.map(([share]) => shareText(share))).toEqual(shares.map(([, text]) => text));
    fc.assert(fc.property(fc.double({ min: 0, max: 1, noNaN: true }), (share) => void expect(shareText(share)).toMatch(/^(<1|[1-9]\d?|100)%$/)));
  });

  it('say in one line what the page counts lost since it opened, after the link that is down now, and what a press does', () => {
    const since = new Date(2026, 0, 1, 14, 2, 30).getTime();
    const totals = [{ kind: 'link', n: 4, seconds: 12 }, { kind: 'dropped', n: 2100, seconds: 0 }, { kind: 'skipped', n: 1, seconds: 0 }, { kind: 'behind', n: 1, seconds: 0 }];
    expect(lossTip(null, totals, since)).toBe('Since 14:02: link dropped 4×, 12 s · hub dropped 2.1 k · 1 sample skipped · page behind 1×. Click for the log');
    expect(lossTip('VMR 0412', totals.slice(0, 1), since)).toBe('The link to VMR 0412 is down. Since 14:02: link dropped 4×, 12 s. Click for the log');
    // A link down since before the page heard its hub has dropped nothing the page counts.
    expect(lossTip('OMG 0077 (/0)', [], since)).toBe('The link to OMG 0077 (/0) is down. Click for the log');
    expect(lossTip(null, totals.slice(2, 3), null)).toBe('Lost: 1 sample skipped. Click for the log');
  });
});

describe('the hub’s messages', () => {
  it('reach the counts: a snapshot’s `behind`, and each `loss` with its streams’ rates', () => {
    const hub = new Hub();
    hub.text({ type: 'hub', id: 'a', seq: 0, behind: 0 });
    hub.text({ type: 'trace', seq: 1, trace: { id: 0, route: '/', stream: 'sine', column: 'sine', units: 'V', rate: 100 } });
    hub.text({ type: 'loss', seq: 2, ...loss('/', 0, 0, { sine: [100], status: [100] }) });
    expect(counts(hub.loss)).toEqual({ drops: 0, seconds: 0, skipped: 200, dropped: 0 });
    // A snapshot in place of 500 changes the page fell behind: 8 s of them, against the 1 s of samples.
    hub.text({ type: 'hub', id: 'a', seq: 9, behind: 500 });
    expect(hub.loss.behind).toBe(1);
    expect(hub.loss.recent.map((cause) => cause.kind)).toEqual(['behind', 'skipped']);
    expect(counts(hub.loss), 'the counts outlive the session').toEqual({ drops: 0, seconds: 0, skipped: 200, dropped: 0 });
  });
});
