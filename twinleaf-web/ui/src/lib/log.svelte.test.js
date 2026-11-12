import { describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { KEPT, KEY, LEVELS, Log, SIZE, matches } from './log.svelte.js';
import { Hub } from './hub.svelte.js';
import { lossLine, logMessage, logSource, outageText } from './words.js';

let n = 0;
/** A `log` message, numbered the next unless `n` is given. */
const line = (level, text, { route = '/', source = 'device', part = 'hub', time = 1, loss, n: number } = {}) => ({ type: 'log', n: number ?? ++n, wall: 1000 + n, time, level, source, ...(source === 'proxy' && { part }), route, text, ...(loss && { loss }) });
/** A line of a loss the proxy says of the device at `route`. */
const lost = (route, loss, level = 'warn') => line(level, 'as the hub words it', { route, source: 'proxy', loss });
/** A log on a hub new to it, the drawer closed. */
function fresh() {
  n = 0;
  const log = new Log();
  log.snapshot('a');
  return log;
}
/** What the rows say, each as [level, text, count]. */
const said = (log) => log.rows.map((r) => [r.level, r.text, r.count]);

describe('the log’s rows', () => {
  it('are the hub’s lines in order, lines in a row that say the same as one row that counts them and carries the newest’s times', () => {
    const log = fresh();
    log.take(line('info', 'up'), false);
    log.take(line('warn', 'lock lost'), false);
    log.take(line('warn', 'lock lost', { time: 7 }), false);
    log.take(line('warn', 'lock lost', { route: '/1' }), false);
    log.take(line('error', 'lock lost'), false);
    log.take(line('info', 'up'), false);
    // The same words from two parts of the proxy are two rows.
    log.take(line('info', 'up', { source: 'proxy', part: 'server', route: null }), false);
    log.take(line('info', 'up', { source: 'proxy', part: 'link', route: null }), false);
    log.take(line('info', 'up', { source: 'proxy', part: 'link', route: null }), false);
    expect(log.rows.slice(5).map((r) => [r.part, r.count])).toEqual([['server', 1], ['link', 2]]);
    expect(log.rows[0].part).toBe(null);
    log.rows = log.rows.slice(0, 5);
    expect(said(log)).toEqual([['info', 'up', 1], ['warn', 'lock lost', 2], ['warn', 'lock lost', 1], ['error', 'lock lost', 1], ['info', 'up', 1]]);
    expect(log.rows[1]).toMatchObject({ id: 2, n: 3, time: 7, wall: 1003, route: '/' });
    expect(log.counts).toEqual({ error: 1, warn: 3, info: 2, debug: 0, loss: 0 });
  });

  it('take a line once, by its number, whether it comes in a snapshot or as a change', () => {
    const log = fresh();
    const first = line('info', 'a'), second = line('info', 'b');
    log.take(first, true);
    log.take(second, false);
    // A snapshot of the same hub holds them again, and one the page missed.
    log.snapshot('a');
    log.take(first, true);
    log.take(second, true);
    log.take(line('info', 'c'), true);
    expect(said(log)).toEqual([['info', 'a', 1], ['info', 'b', 1], ['info', 'c', 1]]);
    // Another hub's lines take the place of these, numbered from 1 again.
    log.snapshot('b');
    expect(log.rows).toEqual([]);
    log.take(line('debug', 'of b', { n: 1 }), true);
    expect(said(log)).toEqual([['debug', 'of b', 1]]);
  });

  it('sum the samples of losses in a row of one cause of one stream of one device, and keep any other loss apart', () => {
    const log = fresh();
    log.take(lost('/', { cause: 'skipped', stream: 'sine', samples: 5 }), false);
    log.take(lost('/', { cause: 'skipped', stream: 'sine', samples: 7 }), false);
    log.take(lost('/', { cause: 'hub', stream: 'sine', samples: 1 }), false);
    log.take(lost('/', { cause: 'skipped', stream: 'aux', samples: 2 }), false);
    log.take(lost('/1', { cause: 'skipped', stream: 'aux', samples: 2 }), false);
    expect(log.rows.map((r) => [r.route, r.loss, r.count])).toEqual([
      ['/', { cause: 'skipped', stream: 'sine', samples: 12 }, 2],
      ['/', { cause: 'hub', stream: 'sine', samples: 1 }, 1],
      ['/', { cause: 'skipped', stream: 'aux', samples: 2 }, 1],
      ['/1', { cause: 'skipped', stream: 'aux', samples: 2 }, 1],
    ]);
    expect(log.counts.loss).toBe(5);
  });

  it('give the row of a link’s drop how long it was down once the line of its return comes, each drop its own', () => {
    const log = fresh();
    log.take(lost('/', { cause: 'link', seconds: null }, 'error'), false);
    log.take(lost('/1', { cause: 'link', seconds: null }, 'error'), false);
    expect(log.rows.map((r) => r.back)).toEqual([undefined, undefined]);
    log.take(lost('/1', { cause: 'link', seconds: 2.5 }, 'info'), false);
    log.take(lost('/1', { cause: 'link', seconds: null }, 'error'), false);
    expect(log.rows.map((r) => [r.route, r.loss.seconds, r.back])).toEqual([['/', null, undefined], ['/1', null, 2.5], ['/1', 2.5, undefined], ['/1', null, undefined]]);
    log.take(lost('/1', { cause: 'link', seconds: 0.4 }, 'info'), false);
    expect(log.rows[3].back).toBe(0.4);
    expect(log.rows[1].back, 'an outage that ended keeps its own length').toBe(2.5);
  });

  it('keep the newest KEPT, whatever came', () => {
    fc.assert(
      fc.property(fc.array(fc.tuple(fc.constantFrom(...LEVELS), fc.constantFrom('a', 'b', 'c')), { maxLength: 60 }), fc.integer({ min: KEPT - 5, max: KEPT + 40 }), (lines, more) => {
        const log = fresh();
        for (let i = 0; i < more; i++) log.take(line('debug', `filler ${i}`), false);
        for (const [level, text] of lines) log.take(line(level, text), false);
        expect(log.rows.length).toBeLessThanOrEqual(KEPT);
        // The newest line is the last row's, and every line taken is in the counts of the rows kept or went with a row dropped.
        const kept = log.rows.reduce((sum, r) => sum + r.count, 0);
        expect(log.rows.at(-1)?.n ?? 0).toBe(more + lines.length);
        expect(kept).toBeLessThanOrEqual(more + lines.length);
        expect(Object.values(log.counts).reduce((a, b) => a + b, 0) - log.counts.loss).toBe(kept);
        // No two rows in a row say the same.
        log.rows.forEach((r, i) => i > 0 && expect(r.key !== log.rows[i - 1].key || r.key === null).toBe(true));
      }),
    );
  });
});

describe('what of the log is unseen', () => {
  it('counts the warnings and errors that arrive while the drawer is closed, not those a hub held before the page heard it, and none once it opens', () => {
    const log = fresh();
    log.take(line('error', 'before the page'), true);
    log.take(line('warn', 'before the page too'), true);
    expect(log.unseen).toEqual({ error: 0, warn: 0 });
    log.take(line('warn', 'w'), false);
    log.take(line('warn', 'w'), false);
    log.take(line('info', 'i'), false);
    log.take(line('debug', 'd'), false);
    log.take(line('error', 'e'), false);
    expect(log.unseen).toEqual({ error: 1, warn: 2 });
    // A snapshot of the same hub, in place of changes the page fell behind, holds lines it missed.
    log.snapshot('a');
    log.take(line('error', 'missed'), true);
    expect(log.unseen).toEqual({ error: 2, warn: 2 });
    log.show();
    expect([log.open, log.unseen]).toEqual([true, { error: 0, warn: 0 }]);
    log.take(line('error', 'seen as it comes'), false);
    expect(log.unseen).toEqual({ error: 0, warn: 0 });
    log.hide();
    log.take(line('warn', 'after'), false);
    expect(log.unseen).toEqual({ error: 0, warn: 1 });
  });
});

describe('the drawer’s state', () => {
  it('opens on the lines of a loss alone or on every line, and is kept with its height', () => {
    const stored = new Map();
    const storage = { getItem: (k) => stored.get(k) ?? null, setItem: (k, v) => stored.set(k, v) };
    const log = new Log(storage);
    expect([log.open, log.size, log.lossOnly]).toEqual([false, SIZE.default, false]);
    log.show(true);
    expect([log.open, log.lossOnly]).toEqual([true, true]);
    log.resize(42);
    log.resize(SIZE.max + 1);
    expect(JSON.parse(stored.get(KEY))).toEqual({ open: true, size: 42 });
    const again = new Log(storage);
    expect([again.open, again.size, again.lossOnly]).toEqual([true, 42, false]);
    log.hide();
    log.show();
    expect([log.open, log.lossOnly]).toEqual([true, false]);
    log.hide();
    expect(new Log(storage).open).toBe(false);
    // What is stored is made whole.
    stored.set(KEY, '{"open":"yes","size":900}');
    expect([new Log(storage).open, new Log(storage).size]).toEqual([false, SIZE.default]);
    stored.set(KEY, 'not json');
    expect(new Log(storage).open).toBe(false);
  });
});

describe('a row shows', () => {
  const all = { levels: { error: true, warn: true, info: true, debug: true }, lossOnly: false, source: '', query: '' };
  const text = (row) => `${row.source} ${row.text}`;
  const row = (over = {}) => ({ level: 'info', source: 'device', route: '/', text: 'Field lock acquired', ...over });

  it('while its level is on, it is of a loss when only those show, it is of the source chosen, and it holds every word typed', () => {
    expect(matches(row(), all, text)).toBe(true);
    expect(matches(row(), { ...all, levels: { ...all.levels, info: false } }, text)).toBe(false);
    expect(matches(row(), { ...all, lossOnly: true }, text)).toBe(false);
    expect(matches(row({ loss: { cause: 'hub' } }), { ...all, lossOnly: true }, text)).toBe(true);
    // The lines one part of the proxy said, or those of a device, whoever said them.
    expect(matches(row(), { ...all, source: 'part:hub' }, text)).toBe(false);
    expect(matches(row({ source: 'proxy', part: 'hub' }), { ...all, source: 'part:hub' }, text)).toBe(true);
    expect(matches(row({ source: 'proxy', part: 'server' }), { ...all, source: 'part:hub' }, text)).toBe(false);
    expect(matches(row({ source: 'proxy', part: 'hub' }), { ...all, source: '/' }, text)).toBe(true);
    expect(matches(row(), { ...all, source: '/1' }, text)).toBe(false);
    expect(matches(row({ source: 'proxy', route: null }), { ...all, source: '/' }, text)).toBe(false);
    // Every word, in any case, anywhere in what the row says.
    expect(matches(row(), { ...all, query: '  LOCK field ' }, text)).toBe(true);
    expect(matches(row(), { ...all, query: 'lock lost' }, text)).toBe(false);
    expect(matches(row(), { ...all, query: 'device' }, text)).toBe(true);
  });
});

describe('the words of a row', () => {
  const named = (route) => ({ '/': 'VMR', '/0': 'OMG (/0)' })[route];
  it('are the line’s own, a loss in the page’s words, and the proxy’s line of a device after the device’s name', () => {
    expect(logSource({ source: 'device', route: '/0' }, named)).toBe('OMG (/0)');
    expect(logSource({ source: 'proxy', part: 'hub', route: '/0' }, named)).toBe('hub');
    expect(logSource({ source: 'proxy', part: 'server', route: null }, named)).toBe('server');
    expect(logMessage({ source: 'device', route: '/', text: 'lock lost' }, named)).toBe('lock lost');
    expect(logMessage({ source: 'proxy', route: '/', text: 'Found VMR' }, named)).toBe('VMR: Found VMR');
    expect(logMessage({ source: 'proxy', route: null, text: 'The log fell behind' }, named)).toBe('The log fell behind');
    expect(logMessage({ source: 'proxy', route: '/0', text: 'x', loss: { cause: 'hub', stream: 'sine', samples: 2100 } }, named)).toBe('OMG (/0): hub dropped 2.1 k samples of sine');
    expect(lossLine({ cause: 'link', seconds: null })).toBe('link down');
    expect(lossLine({ cause: 'link', seconds: 2.34 })).toBe('link back after 2.3 s');
    expect(lossLine({ cause: 'skipped', stream: 'aux', samples: 1 })).toBe('1 sample of aux skipped');
    expect(lossLine({ cause: 'proxy', stream: 'aux', samples: 12 })).toBe('proxy dropped 12 samples of aux');
    expect(outageText('VMR', undefined)).toBe('link to VMR down');
    expect(outageText('VMR', 11)).toBe('link to VMR down 11.0 s');
  });
});

describe('the hub’s messages', () => {
  it('reach the log: each `log`, in a snapshot or as a change, and a new hub’s in place of the last’s', () => {
    const hub = new Hub();
    hub.text({ type: 'hub', id: 'a', seq: 0, behind: 0 });
    hub.text(line('warn', 'held', { n: 1 }));
    hub.text({ ...line('error', 'new', { n: 2 }), seq: 1 });
    expect(said(hub.log)).toEqual([['warn', 'held', 1], ['error', 'new', 1]]);
    expect(hub.log.unseen).toEqual({ error: 1, warn: 0 });
    expect(hub.seq).toBe(1);
    hub.text({ type: 'hub', id: 'b', seq: 0, behind: 0 });
    expect([hub.log.rows, hub.log.unseen]).toEqual([[], { error: 0, warn: 0 }]);
  });
});
