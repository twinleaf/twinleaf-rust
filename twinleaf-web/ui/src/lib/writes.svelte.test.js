// DeviceWrites against a model of it (round 2, Device writes #2 to #6):
// random runs of writes of one or several rows to any devices, some
// refusing or failing, answered at once or late and in the order sent; of
// changes made elsewhere; of devices unchecked, disconnected, reconnected,
// and a new hub; of the pointer on and off the notice, time passing and the
// notice closed by hand; of Revert, answered at once or late; and of
// actions run. After every act the notice is the one of the write or
// Revert started last that has answered, it offers Revert as the rules
// say, and Revert calls exactly the checked devices that still show the
// value written, with their old values, and names the ones it left. The
// devices answer as the server does: a device that refuses refuses the
// whole call, and the others are skipped.
import { afterEach, describe, expect, it } from 'vitest';
import fc from 'fast-check';
import { Session } from './session.svelte.js';
import { DeviceWrites, NOTICE_MS, POINT_MS, REVERT_MS } from './writes.svelte.js';
import { History } from './history.svelte.js';
import { Workspace } from './workspace.svelte.js';
import { Favorites } from './favorites.svelte.js';
import { makeTab } from './tabs.js';
import { UNDO_ACTION, UNDO_UNKNOWN, USE_REVERT } from './words.js';
import * as clock from './clock.js';

/** The virtual clock a test runs on. */
let time = null;
/** Waits until what every promise resolved so far set off has run. */
const settled = () => new Promise((done) => setImmediate(done));
afterEach(() => time?.end());

const ROUTES = ['/0', '/1', '/2'];
/** The rows, each with the values written to it. `mode` is one no device announces. */
const ROWS = { gain: [1, 2, 3], level: [1, 2, 3], mode: [4, 5], enable: [true, false] };
const ANNOUNCED = ['gain', 'level', 'enable'];
const label = (route) => `dev (${route})`;
const typed = (text) => (text === 'true' ? true : text === 'false' ? false : +text);

/** A Session of the devices at ROUTES, each alive and announcing `values` by route. */
function session(values) {
  const s = new Session('test');
  s.text({ type: 'routes', mounts: {}, routes: ROUTES });
  for (const route of ROUTES) {
    s.text({ type: 'device', route, name: 'dev', serial: route, firmware: '1', presence: 'alive' });
    s.text({ type: 'settings', route, values: values[route] });
  }
  return s;
}

/**
 * A hub of `s` whose devices answer as the server does, in the order the
 * calls were sent: while `slow`, or while an earlier call waits, a call
 * waits for `release`. A call takes `refusing` and `failing` as they are
 * when it is sent; a device not alive as it answers does not answer.
 */
function fakeHub(s) {
  const hub = { session: s, calls: [], refusing: new Set(), failing: new Set(), slow: false, waiting: [] };
  const answer = (name, value, routes, refusing, failing) => {
    if (routes.some((r) => refusing.has(r))) return { sent: false, results: routes.map((route) => (refusing.has(route) ? { route, state: 'refused', error: 'no such RPC' } : { route, state: 'skipped' })) };
    const results = routes.map((route) => {
      const d = hub.session.devices.get(route);
      if (d.presence !== 'alive') return { route, state: 'unknown', error: 'timed out' };
      if (failing.has(route)) return { route, state: 'error', error: 'busy' };
      return { route, state: 'ok', value: value === '' ? null : typed(value) };
    });
    for (const r of results) {
      const d = hub.session.devices.get(r.route);
      if (r.state === 'ok' && ANNOUNCED.includes(name)) d.announce({ ...Object.fromEntries(d.values), [name]: r.value });
    }
    return { sent: true, results };
  };
  hub.call = (name, value, routes) => {
    hub.calls.push({ name, value, routes });
    const refusing = new Set(hub.refusing), failing = new Set(hub.failing);
    const reply = () => answer(name, value, routes, refusing, failing);
    if (!hub.slow && !hub.waiting.length) return Promise.resolve(reply());
    return new Promise((done) => hub.waiting.push(() => done(reply())));
  };
  hub.release = () => hub.waiting.shift()?.();
  hub.read = async () => ({ values: {} });
  return hub;
}

const subset = fc.subarray(ROUTES);
const act = fc.oneof(
  { weight: 5, arbitrary: fc.record({ t: fc.constant('write'), rows: fc.oneof(fc.constant(['enable']), fc.subarray(['gain', 'level', 'mode'], { minLength: 1 })), devices: fc.subarray(ROUTES, { minLength: 1 }), refuse: subset, fail: subset, v: fc.nat(), slow: fc.boolean() }) },
  { weight: 3, arbitrary: fc.record({ t: fc.constant('release') }) },
  { weight: 2, arbitrary: fc.record({ t: fc.constant('elsewhere'), route: fc.constantFrom(...ROUTES), row: fc.constantFrom(...ANNOUNCED), v: fc.nat() }) },
  fc.record({ t: fc.constant('uncheck'), route: fc.constantFrom(...ROUTES) }),
  fc.record({ t: fc.constant('presence'), route: fc.constantFrom(...ROUTES), how: fc.constantFrom('lost', 'reconnect') }),
  fc.record({ t: fc.constant('newHub') }),
  { weight: 3, arbitrary: fc.record({ t: fc.constant('revert'), slow: fc.boolean() }) },
  fc.record({ t: fc.constant('hover'), on: fc.boolean() }),
  { weight: 2, arbitrary: fc.record({ t: fc.constant('wait'), ms: fc.constantFrom(1_000, 4_000, 6_000, 11_000) }) },
  fc.record({ t: fc.constant('close') }),
  fc.record({ t: fc.constant('run'), devices: fc.subarray(ROUTES, { minLength: 1 }), refuse: subset, fail: subset }),
);
/** A write of gain to `devices`, which take it at once. */
const write = (devices, more = {}) => ({ t: 'write', rows: ['gain'], devices, refuse: [], fail: [], v: 1, slow: false, ...more });

describe('device writes, against a model of them', () => {
  it('show the notice of what was started last, offer Revert as the rules say, and Revert puts back exactly what is checked and still shows the value written', async () => {
    const seen = { offered: 0, several: 0, reverted: 0, changed: 0, unchecked: 0, unknownOld: 0, inFlightOld: 0, superseded: 0, lateRevert: 0, expired: 0, keptByHover: 0, offByPresence: 0, offByHub: 0, refused: 0, runRefused: 0 };
    await fc.assert(
      fc.asyncProperty(fc.array(act, { minLength: 1, maxLength: 25 }), async (acts) => {
        time = clock.virtual();
        const start = Object.fromEntries(ROUTES.map((r) => [r, { gain: 1, level: 1, enable: false }]));
        const hub = fakeHub(session(start));
        const checked = new Set(ROUTES);
        const reports = [];
        const w = new DeviceWrites(hub, { open() {}, close() {}, report: (level, text, detail) => reports.push({ level, text, detail }) }, { checked: (route) => checked.has(route) });
        // The model. What each device shows of a row: what it announces, or for `mode`, of this hub, the value it last took.
        let mode = new Map();
        const shows = (route, name) => (ANNOUNCED.includes(name) ? hub.session.devices.get(route).values.get(name) : mode.get(route));
        let notice = null, offer = null, hovered = false, latest = 0, ids = 0, hubs = 0;
        const epoch = new Map(ROUTES.map((r) => [r, 0]));
        /** The calls waiting, oldest first, each {op, name, routes, value, refusing, failing}: an op is a write or a Revert. */
        const waiting = [];
        const pending = (route, name) => waiting.some((c) => c.op.hub === hubs && c.name === name && c.routes.includes(route));
        /** Starts op `op` with `calls`, as the page sends them. */
        const begin = (op, calls) => {
          Object.assign(op, { id: ++ids, left: calls.length, hub: hubs, epochs: new Map(epoch) });
          latest = op.id;
          offer = null;
          for (const c of calls) waiting.push({ op, ...c });
        };
        /** What the model expects as op `op` has had every answer. */
        const finished = (op) => {
          if (op.id !== latest) return void seen.superseded++;
          if (op.t === 'revert') {
            notice = { left: NOTICE_MS, revert: false };
            return;
          }
          const entries = op.entries.filter((e) => e.ok && e.old !== undefined);
          offer = entries.length && op.hub === hubs ? { entries, epochs: op.epochs, hub: op.hub } : null;
          notice = { left: offer ? REVERT_MS : NOTICE_MS, revert: !!offer };
          if (offer) seen.offered++;
        };
        /** Answers the oldest call waiting, as the hub does, and what that finishes. */
        const answerOne = async () => {
          const c = waiting.shift();
          if (!c) return;
          hub.release();
          await settled();
          const refused = c.routes.some((r) => c.refusing.has(r));
          for (const route of c.routes) {
            const alive = hub.session.devices.get(route).presence === 'alive';
            const ok = !refused && alive && !c.failing.has(route);
            if (c.op.t === 'write') c.op.entries.find((x) => x.route === route && x.name === c.name).ok = ok;
            if (c.name === 'mode' && c.op.hub === hubs) {
              if (ok) mode.set(route, typed(c.value));
              else if (!refused && !alive) mode.delete(route);
            }
          }
          if (--c.op.left === 0) finished(c.op);
        };
        /** Answers every call waiting that the hub answers at once: those sent while no call waited and the hub was not slow. */
        const answerQuick = async (op, quick) => {
          while (quick && waiting.length && waiting.every((c) => c.op === op)) await answerOne();
        };
        const offerOff = () => !offer || offer.hub !== hubs || offer.entries.some((e) => offer.epochs.get(e.route) !== epoch.get(e.route));

        for (const a of acts) {
          if (a.t === 'write') {
            const toggle = a.rows[0] === 'enable';
            const value = String(toggle ? ROWS.enable[a.v % 2] : ROWS.gain[a.v % 3]);
            const edits = a.rows.map((name) => ({ name, devices: a.devices.map((r) => hub.session.devices.get(r)) }));
            // An old value is known when no call of it waits, and the device shows one.
            const entries = a.rows.flatMap((name) => a.devices.map((route) => ({ name, route, wrote: value, old: pending(route, name) ? undefined : shows(route, name), inFlight: pending(route, name) })));
            for (const e of entries) if (e.inFlight) seen.inFlightOld++;
            else if (e.old === undefined) seen.unknownOld++;
            if (a.rows.length > 1) seen.several++;
            if (a.refuse.some((r) => a.devices.includes(r))) seen.refused++;
            const quick = !a.slow && !waiting.length;
            hub.refusing = new Set(a.refuse);
            hub.failing = new Set(a.fail);
            hub.slow = a.slow;
            const op = { t: 'write', entries };
            begin(op, a.rows.map((name) => ({ name, routes: a.devices, value, refusing: hub.refusing, failing: hub.failing })));
            w.write(edits, value, toggle);
            hub.slow = false;
            hub.refusing = new Set();
            hub.failing = new Set();
            await answerQuick(op, quick);
          } else if (a.t === 'release') {
            await answerOne();
          } else if (a.t === 'elsewhere') {
            const d = hub.session.devices.get(a.route);
            d.announce({ ...Object.fromEntries(d.values), [a.row]: ROWS[a.row][a.v % ROWS[a.row].length] });
          } else if (a.t === 'uncheck') {
            if (checked.has(a.route)) checked.delete(a.route);
            else checked.add(a.route);
          } else if (a.t === 'presence') {
            const msg = (presence) => hub.session.text({ type: 'device', route: a.route, name: 'dev', serial: a.route, firmware: '1', presence });
            msg('lost');
            if (a.how === 'reconnect') msg('alive');
            epoch.set(a.route, epoch.get(a.route) + 1);
            if (offer?.entries.some((e) => e.route === a.route)) seen.offByPresence++;
          } else if (a.t === 'newHub') {
            hub.session = session(Object.fromEntries(ROUTES.map((r) => [r, Object.fromEntries(hub.session.devices.get(r).values)])));
            hubs++;
            mode = new Map();
            if (offer) seen.offByHub++;
          } else if (a.t === 'revert') {
            const can = !offerOff();
            const before = hub.calls.length;
            // What Revert puts back: the checked devices that show the value written.
            const back = can ? offer.entries.filter((e) => checked.has(e.route) && String(shows(e.route, e.name)) === String(e.wrote)) : [];
            const left = can ? offer.entries.filter((e) => !back.includes(e)) : [];
            const oneRow = can && new Set(offer.entries.map((e) => e.name)).size === 1;
            const quick = !a.slow && !waiting.length;
            hub.slow = a.slow;
            const done = w.revert();
            hub.slow = false;
            const sent = hub.calls.slice(before).flatMap((c) => c.routes.map((r) => `${c.name} ${r} ${c.value}`)).sort();
            expect(sent, 'Revert calls').toEqual(back.map((e) => `${e.name} ${e.route} ${String(e.old)}`).sort());
            if (can) {
              seen.reverted++;
              if (a.slow) seen.lateRevert++;
              const op = { t: 'revert' };
              // A call per row and old value, to every device that had it, as Revert sends them.
              const calls = new Map();
              for (const e of back) {
                const k = `${e.name} ${String(e.old)}`;
                if (!calls.has(k)) calls.set(k, { name: e.name, routes: [], value: String(e.old), refusing: new Set(), failing: new Set() });
                calls.get(k).routes.push(e.route);
              }
              begin(op, [...calls.values()]);
              if (!back.length) finished(op);
              await answerQuick(op, quick);
              await settled();
              if (op.left === 0) {
                await done;
                const detail = w.notice.detail.join('\n');
                // The devices it left, each with why: two named, or past two the first and how many more.
                for (const e of left.length > 2 ? left.slice(0, 1) : left) {
                  const why = checked.has(e.route) ? 'changed since' : 'is unchecked';
                  expect(detail).toContain(`${label(e.route)}${oneRow ? '' : ` ${e.name}`} ${why}`);
                  seen[why === 'changed since' ? 'changed' : 'unchecked']++;
                }
                if (left.length > 2) expect(detail).toContain(`and ${left.length - 1} more`);
              }
            }
          } else if (a.t === 'hover') {
            w.hover(a.on);
            hovered = a.on;
          } else if (a.t === 'wait') {
            time.advance(a.ms);
            if (notice && !hovered) {
              notice.left -= a.ms;
              if (notice.left <= 0) (notice = null), (offer = null), seen.expired++;
            } else if (notice && hovered && a.ms >= REVERT_MS) seen.keptByHover++;
          } else if (a.t === 'close') {
            w.closed();
            notice = null;
            offer = null;
            hovered = false;
          } else if (a.t === 'run') {
            if (waiting.length) continue;
            hub.refusing = new Set(a.refuse);
            hub.failing = new Set(a.fail);
            const before = reports.length, shown = w.notice;
            await w.run(['save'], () => a.devices.map((r) => hub.session.devices.get(r)));
            hub.refusing = new Set();
            hub.failing = new Set();
            expect(reports.length).toBe(before + 1);
            // A run reports apart from the write's notice, and leaves its Revert alone.
            expect(w.notice).toBe(shown);
            const r = reports.at(-1);
            const refusers = a.devices.filter((x) => a.refuse.includes(x));
            if (refusers.length) {
              seen.runRefused++;
              // The device that refused, and why, come first; the devices skipped are no failures.
              expect(r.level).toBe('error');
              expect(r.text).toBe('Could not run save');
              expect(r.detail[0].startsWith(`${label(refusers[0])} refused: no such RPC`), r.detail[0]).toBe(true);
            } else {
              const ok = a.devices.filter((x) => !a.fail.includes(x) && hub.session.devices.get(x).presence === 'alive');
              expect(r.level).toBe(!ok.length ? 'error' : ok.length < a.devices.length ? 'warning' : 'success');
              if (ok.length && ok.length < a.devices.length) expect(r.text).toBe(`save failed on ${a.devices.length - ok.length} of ${a.devices.length} devices`);
            }
          }
          await settled();
          expect(!!w.notice, JSON.stringify(a)).toBe(!!notice);
          if (notice) expect(w.notice.revert, JSON.stringify(a)).toBe(notice.revert);
          expect(w.canRevert, JSON.stringify(a)).toBe(!offerOff());
          if (w.notice?.revert && !w.canRevert && offer) expect(w.revertOff).not.toBe(null);
        }
        time.end();
      }),
      {
        // Each case counted below, whatever the seed.
        examples: [
          [[write(['/0', '/1']), { t: 'elsewhere', route: '/1', row: 'gain', v: 2 }, { t: 'uncheck', route: '/0' }, { t: 'revert', slow: false }]],
          [[write(['/0', '/1'], { rows: ['gain', 'mode'] }), write(['/0', '/1'], { rows: ['mode'] }), { t: 'wait', ms: 11_000 }]],
          [[write(['/0'], { slow: true }), write(['/0', '/1']), { t: 'release' }, { t: 'release' }]],
          [[write(['/0']), { t: 'revert', slow: true }, write(['/0'], { v: 2 }), { t: 'release' }, { t: 'release' }]],
          [[write(['/0']), { t: 'hover', on: true }, { t: 'wait', ms: 11_000 }, { t: 'hover', on: false }, { t: 'presence', route: '/0', how: 'reconnect' }]],
          [[write(['/2']), { t: 'newHub' }, { t: 'revert', slow: false }]],
          // A value set back elsewhere to what it was before the write, which Revert then leaves.
          [[write(['/0']), { t: 'elsewhere', route: '/0', row: 'gain', v: 0 }, { t: 'revert', slow: false }]],
          // A late Revert of two devices with one old value, sent as one call.
          [[write(['/1', '/2']), { t: 'revert', slow: true }, { t: 'release' }]],
          [[write(['/0', '/1'], { refuse: ['/1'] }), { t: 'run', devices: ['/0', '/1', '/2'], refuse: ['/2'], fail: [] }]],
        ],
      },
    );
    for (const [what, n] of Object.entries(seen)) expect(n, what).toBeGreaterThan(0);
  });

  it('closes the notice when its time is up, and only then', async () => {
    time = clock.virtual();
    const hub = fakeHub(session(Object.fromEntries(ROUTES.map((r) => [r, { gain: 1 }]))));
    let open = 0, close = 0;
    const w = new DeviceWrites(hub, { open: () => open++, close: () => close++, report: () => {} });
    await w.write([{ name: 'gain', devices: [hub.session.devices.get('/0')] }], '2');
    expect([open, close]).toEqual([1, 0]);
    expect(w.notice.text).toBe('Set gain to 2 (was 1)');
    w.hover(true);
    time.advance(REVERT_MS * 3);
    expect(w.canRevert).toBe(true);
    w.hover(false);
    time.advance(REVERT_MS - 1);
    expect(close).toBe(0);
    time.advance(1);
    expect([close, w.notice, w.canRevert]).toEqual([1, null, false]);
    w.closed();
    expect(close).toBe(1);
  });

  it('runs several actions in order, stops at the first that fails, and names the device that refused first', async () => {
    const hub = fakeHub(session(Object.fromEntries(ROUTES.map((r) => [r, {}]))));
    const reports = [];
    const w = new DeviceWrites(hub, { open() {}, close() {}, report: (level, text, detail) => reports.push({ level, text, detail }) });
    const all = () => ROUTES.map((r) => hub.session.devices.get(r));
    await w.run(['save', 'reboot', 'zero'], all);
    expect(hub.calls.map((c) => c.name)).toEqual(['save', 'reboot', 'zero']);
    expect(reports.at(-1)).toEqual({ level: 'success', text: 'Ran save, then reboot, then zero · 3 devices', detail: [] });
    hub.calls = [];
    hub.failing = new Set(['/1']);
    await w.run(['save', 'reboot', 'zero'], all);
    expect(hub.calls.map((c) => c.name)).toEqual(['save']);
    expect(reports.at(-1)).toEqual({ level: 'warning', text: 'save failed on 1 of 3 devices', detail: ['dev (/1): busy', 'Not run: reboot, zero'] });
    hub.failing = new Set();
    hub.refusing = new Set(['/2']);
    await w.run(['save', 'reboot'], all);
    expect(reports.at(-1)).toEqual({ level: 'error', text: 'Could not run save', detail: ['dev (/2) refused: no such RPC', 'Nothing was sent to the 2 other devices', 'Not run: reboot'] });
    expect(w.notice).toBe(null);
  });
});

// Ctrl+Z right after a write or an action (round 2, Device writes #1):
// random runs of edits of the page (a star turned), writes (of a row whose
// old value is known, which offer Revert, or of one whose old value is not,
// which offer none), runs of an action, time passing, Ctrl+Z, redo and
// Revert. The model numbers every act, and says each step on the undo stack
// was put there by the act that made it or brought it back by redo. Two
// holds compose: the last run's, for NOTICE_MS, or the last write's with no
// Revert, while its notice shows; and a write's while its Revert is
// offered, which a run leaves. A write or a Revert ends both. Ctrl+Z takes
// back the last step exactly when no hold it was put there before lasts,
// the first hold first; else it takes back nothing and says why: after a
// run a notice says an action cannot be undone, and the write's notice
// says that the old value was not known, or to use Revert, which it marks.
// It never does both, and the write's notice says so only while its hold
// lasts.
describe('Ctrl+Z after a write or an action', () => {
  const NAMES = ['a', 'b', 'c'];
  const step = fc.oneof(
    { weight: 3, arbitrary: fc.record({ t: fc.constant('edit'), name: fc.constantFrom(...NAMES) }) },
    { weight: 2, arbitrary: fc.record({ t: fc.constant('write'), known: fc.boolean() }) },
    fc.record({ t: fc.constant('run') }),
    { weight: 2, arbitrary: fc.record({ t: fc.constant('wait'), ms: fc.constantFrom(1_000, 4_000, NOTICE_MS - 1, NOTICE_MS + 1, REVERT_MS - 1, REVERT_MS + 1) }) },
    { weight: 4, arbitrary: fc.record({ t: fc.constant('undo') }) },
    fc.record({ t: fc.constant('redo') }),
    fc.record({ t: fc.constant('revert') }),
  );
  /** What the write's notice says of a held Ctrl+Z, by what holds. */
  const SAYS = { revert: USE_REVERT, unknown: UNDO_UNKNOWN, action: null };

  it('takes back an edit exactly when no hold it came before lasts, the run’s first and then Revert’s, and else says why, never both', async () => {
    const seen = { held: 0, heldAfterRun: 0, heldUnknown: 0, heldAction: 0, undone: 0, afterWrite: 0, expired: 0, empty: 0, redone: 0 };
    await fc.assert(
      fc.asyncProperty(fc.array(step, { minLength: 1, maxLength: 30 }), async (acts) => {
        time = clock.virtual();
        // gain is announced, so its old value is known; mode is not.
        const hub = fakeHub(session(Object.fromEntries(ROUTES.map((r) => [r, { gain: 1, level: 1, enable: false }]))));
        const tab = makeTab('A');
        const ws = new Workspace({ tabs: [tab], activeId: tab.id });
        const favorites = new Favorites(null);
        const history = new History(ws, favorites);
        const reports = [];
        const w = new DeviceWrites(hub, { open() {}, close() {}, report: (level, text) => reports.push({ level, text }) }, { edits: () => history.undos });
        // The model: the undo and redo stacks, each step {name, at}, `at` the act that put it on the undo stack; and the holds, each {at, why, until}, the act it holds for, why, and until when: the last run's or write's with no Revert, and the write's while its Revert is offered.
        let undos = [], redos = [], n = 0, first = null, rev = null, v = 1;
        const lasts = (h) => !!h && clock.now() < h.until;
        const stars = () => [...favorites.names].sort().join();
        for (const a of acts) {
          n++;
          if (a.t === 'edit') {
            history.group(`Star ${a.name}`, () => favorites.toggle(a.name));
            undos.push({ name: a.name, at: n });
            redos = [];
          } else if (a.t === 'write') {
            // A row no device announces and none was written to has no old value known.
            await w.write([{ name: a.known ? 'gain' : `mode${v}`, devices: [hub.session.devices.get('/0')] }], String(++v));
            await settled();
            rev = a.known ? { at: n, why: 'revert', until: clock.now() + REVERT_MS } : null;
            first = a.known ? null : { at: n, why: 'unknown', until: clock.now() + NOTICE_MS };
          } else if (a.t === 'run') {
            await w.run(['save'], () => [hub.session.devices.get('/0')]);
            await settled();
            first = { at: n, why: 'action', until: clock.now() + NOTICE_MS };
          } else if (a.t === 'wait') {
            time.advance(a.ms);
          } else if (a.t === 'revert') {
            const offered = lasts(rev);
            await w.revert();
            await settled();
            if (offered) (rev = null), (first = null);
          } else if (a.t === 'redo') {
            const s = redos.pop();
            history.redo();
            if (s) (undos.push({ ...s, at: n }), seen.redone++);
          } else {
            const top = undos.at(-1);
            const fits = (h) => lasts(h) && (!top || top.at < h.at);
            const last = fits(first) ? first : fits(rev) ? rev : null;
            const open = lasts(first) || lasts(rev);
            const takes = !last;
            const before = stars(), steps = history.undos.length, said = reports.length;
            const { label, held } = w.undo(history.undos.at(-1), () => history.undo());
            expect(!!held && label !== undefined, 'never both').toBe(false);
            if (takes) {
              expect([held, w.undoHeld, reports.length], `at ${n}: taken back, and nothing says anything of Ctrl+Z`).toEqual([undefined, null, said]);
              expect(label).toBe(top ? `Star ${top.name}` : null);
              if (top) {
                redos.push(undos.pop());
                expect(stars()).not.toBe(before);
                seen[open ? 'afterWrite' : first || rev ? 'expired' : 'undone']++;
              } else seen.empty++;
            } else {
              expect([held, w.undoHeld], `at ${n}: held, and the write's notice says why`).toEqual([true, SAYS[last.why]]);
              expect([stars(), history.undos.length], 'nothing taken back').toEqual([before, steps]);
              expect(reports.slice(said), 'a notice of its own after a run').toEqual(last.why === 'action' ? [{ level: 'warning', text: UNDO_ACTION }] : []);
              expect(w.pointed, 'Revert marked only while it holds for Revert').toBe(last.why === 'revert');
              time.advance(POINT_MS);
              expect(w.pointed, 'for a moment').toBe(false);
              seen[last.why === 'revert' ? (first?.why === 'action' && first.at > rev.at ? 'heldAfterRun' : 'held') : last.why === 'unknown' ? 'heldUnknown' : 'heldAction']++;
            }
          }
          expect(history.undos.map((s) => s.label), 'the undo stack as the model has it').toEqual(undos.map((s) => `Star ${s.name}`));
          // The write's notice says why only while its hold lasts.
          if (w.undoHeld) expect(lasts(w.undoHeld === USE_REVERT ? rev : first), `at ${n}: said only while it holds`).toBe(true);
        }
      }),
      {
        examples: [
          // Ctrl+Z with nothing to undo, then an edit taken back with nothing holding, redone, and taken back again.
          [[{ t: 'undo' }, { t: 'edit', name: 'a' }, { t: 'undo' }, { t: 'redo' }, { t: 'undo' }]],
          // An edit, a write, Ctrl+Z: held. Past the window: the edit is taken back.
          [[{ t: 'edit', name: 'a' }, { t: 'write', known: true }, { t: 'undo' }, { t: 'wait', ms: REVERT_MS + 1 }, { t: 'undo' }]],
          // An edit after the write is taken back; the one before it is not, until Revert is pressed.
          [[{ t: 'edit', name: 'a' }, { t: 'write', known: true }, { t: 'edit', name: 'b' }, { t: 'undo' }, { t: 'undo' }, { t: 'revert' }, { t: 'undo' }]],
          // A write with no Revert holds while its notice shows; a redo after a write is taken back.
          [[{ t: 'edit', name: 'a' }, { t: 'write', known: false }, { t: 'undo' }, { t: 'write', known: true }, { t: 'redo' }, { t: 'undo' }, { t: 'undo' }]],
          // A run after a write holds for the run, then, past NOTICE_MS, for the write's Revert still offered; past that, the edit is taken back.
          [[{ t: 'edit', name: 'a' }, { t: 'write', known: true }, { t: 'run' }, { t: 'undo' }, { t: 'wait', ms: NOTICE_MS + 1 }, { t: 'undo' }, { t: 'wait', ms: NOTICE_MS + 1 }, { t: 'undo' }]],
        ],
      },
    );
    for (const [what, k] of Object.entries(seen)) expect(k, what).toBeGreaterThan(0);
  });
});
