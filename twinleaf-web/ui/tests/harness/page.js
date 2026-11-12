// The whole page, mounted for a browser test with state of its own, and the
// gestures the tests make on it with the real mouse.
//
// The page's time is the test's. A page is mounted on a virtual clock
// (src/lib/clock.js) unless it asks for the real one: its time, its frames
// and its timers move only as `frames`, `settle`, `wait` and `until` step
// them, by the same amounts on any machine, so nothing the page times
// depends on how fast the test ran. The browser itself stays real: layout,
// its observers, CSS transitions and the timers inside libraries (menus and
// tooltips, the drag library, toasts). Each step of `frames` therefore runs
// inside one frame of the browser's own, where the page's frame callbacks
// run on a real page, and a test waits for what a library times with
// `until`, on what the page shows and not for a duration.
import { withY } from '../../src/lib/cursors.js';
import { flushSync, mount, unmount } from 'svelte';
import fc from 'fast-check';
import { expect, vi } from 'vitest';
import { commands, userEvent } from 'vitest/browser';
import { toast as notices } from 'svelte-sonner';
import App from '../../src/App.svelte';
import { Hub } from '../../src/lib/hub.svelte.js';
import { Workspace } from '../../src/lib/workspace.svelte.js';
import { History } from '../../src/lib/history.svelte.js';
import { Favorites } from '../../src/lib/favorites.svelte.js';
import { Views } from '../../src/lib/views.js';
import { PlotHost } from '../../src/lib/plothost.js';
import { colors } from '../../src/lib/colors.svelte.js';
import { presets } from '../../src/lib/layout.js';
import { emptyTab, joined, loadTabs, makeTab, newPlot } from '../../src/lib/tabs.js';
import { serveCaptures } from './captures.js';
import { api } from '../../src/lib/protocol.js';
import * as clock from '../../src/lib/clock.js';
import '../../src/app.css';
import '../../src/plot.css';

/** The virtual clock of the page mounted; null while none is, or while it is on the real clock. */
let virtual = null;

/** A task of the browser's own: every promise settled before it has run what waited on it, however long the chain. */
const task = () =>
  new Promise((done) => {
    const { port1, port2 } = new MessageChannel();
    port1.onmessage = () => (port1.close(), done());
    port2.postMessage(null);
  });

/** How long, in ms of real time, a wait of the harness waits for the browser's next frame before it fails. */
const FRAME_WAIT_MS = 10_000;

/**
 * One frame of the browser's own, `step` run in it where a page's frame
 * callbacks run, and then a task: what the frame's timers resolved, as a
 * hub's reply, has reached the page before the test goes on. It fails with
 * what `step` threw, or, when no frame comes within FRAME_WAIT_MS, with
 * what the page was like as it waited.
 */
const turn = (step) =>
  new Promise((done, fail) => {
    const late = setTimeout(() => fail(new Error(`no frame of the browser's in ${FRAME_WAIT_MS} ms: the page ${document.visibilityState}, ${document.hasFocus() ? 'focused' : 'unfocused'}`)), FRAME_WAIT_MS);
    requestAnimationFrame(() => {
      clearTimeout(late);
      try {
        step?.();
      } catch (error) {
        return fail(error);
      }
      task().then(done);
    });
  });

/**
 * Lets `n` frames pass. On the virtual clock each is one frame of the
 * browser's with one of the page's run inside it: the page's time moves on
 * by clock.js FRAME_MS, its timers due by then run, then its frame
 * callbacks. On the real clock, the browser's frames alone.
 */
export async function frames(n = 2) {
  for (let i = 0; i < n; i++) await turn(() => virtual?.nextFrame());
}

/**
 * Notes `read()` at every frame of the browser's, whatever clock the page
 * is on, until the function it returns is called, which gives each
 * different value noted, in order: what a person would have seen change.
 */
export function watchFrames(read) {
  const noted = [];
  let on = true;
  const note = () => {
    if (!on) return;
    const seen = read();
    if (noted.at(-1) !== seen) noted.push(seen);
    requestAnimationFrame(note);
  };
  note();
  return () => ((on = false), noted);
}

/** Waits until `read()` has been the same for `n` frames of the browser's running, as once a slide or a fade is over; false when it never is within `tries` frames. */
export async function rested(read, n = 6, tries = 600) {
  let last = read(), same = 0;
  for (let i = 0; i < tries && same < n; i++) {
    await turn();
    const seen = read();
    same = seen === last ? same + 1 : 0;
    last = seen;
  }
  return same >= n;
}

/**
 * Lets `ms` of the page's time pass. On the virtual clock the page lives
 * through them at once, frame by frame, its timers in their order; then the
 * browser has one frame of its own, with none of the page's, to lay out
 * and deliver what that set off. No real time is waited out, so this waits
 * for nothing a library or a CSS transition times: `until` does. On the
 * real clock it waits `ms`.
 */
export async function wait(ms) {
  if (!virtual) return new Promise((done) => setTimeout(done, ms));
  const whole = Math.floor(ms / clock.FRAME_MS);
  virtual.frames(whole);
  virtual.advance(ms - whole * clock.FRAME_MS);
  flushSync();
  await turn();
}

/** How long after a notice closes it stays among the notices the next page shows, in ms of real time: the notices' own timer. */
const NOTICE_REMOVAL_MS = 200;
/** When the last page disposed of closed its notices, by performance.now(); null when it showed none. */
let noticesClosed = null;
/** Waits in real time until the notices a disposed page closed have gone, so the page mounted next shows none of them. */
async function noticesGone() {
  if (noticesClosed === null) return;
  const left = noticesClosed + NOTICE_REMOVAL_MS - performance.now();
  noticesClosed = null;
  if (left > 0) await new Promise((done) => setTimeout(done, left + 1));
}

/** A value typed into a write as the stub's devices take it: a bool, a number, or the text. */
const typed = (text) => (text === 'true' ? true : text === 'false' ? false : text !== '' && !isNaN(+text) ? +text : text);

/**
 * A hub with no socket that has announced `signals`, keys as 'stream.column',
 * each a trace of route '/' in volts unless `units`, by key, names others,
 * and, with `rpcs`, a device at each of `routes` whose RPC table they
 * are, each {name, channel, value, type, access, announced}: type f32 and
 * access read and write unless given, `value` one for every device or an
 * array of one per route, and `announced` false for an entry no
 * announcement keeps, which a read answers nothing of.
 *
 * Its devices answer calls as the server does (`twinleaf-web/src/devices.rs`
 * `call`): `calls` lists each {name, value, routes}, and `answer(name,
 * value, route)` gives each device's outcome, by default ok with the value
 * as the device takes it, or {state, error} for error, unknown or refused.
 * A device that refuses refuses the whole call: nothing is sent, and every
 * other device is skipped. A device that took a value of an entry it
 * announces announces it. While `slow`, each reply waits for `release()`,
 * which answers the oldest call waiting. `announce(route, values)` has a
 * device announce new values, as another client's write would, and
 * `presence(route, state)` changes a device's presence. Each change it
 * sends is numbered the next after the last the hub applied.
 */
export function stubHub(signals = [], rpcs = [], routes = ['/'], units = {}) {
  const hub = new Hub();
  hub.close();
  hub.text({ type: 'hub', id: 'test', seq: 0 });
  signals.forEach((key, id) => {
    const [stream, column] = key.split('.');
    hub.text({ type: 'trace', seq: hub.seq + 1, trace: { id, route: '/', stream, column, units: units[key] ?? 'V', rate: 100 } });
  });
  const table = rpcs.map(({ name, channel, type = 'f32', access = ['read', 'write'] }) => ({ name, channel, type, access, readable: access.includes('read') }));
  const announced = new Set(rpcs.filter((p) => (p.access ?? ['read', 'write']).includes('read') && p.announced !== false).map((p) => p.name));
  const values = new Map(routes.map((route, i) => [route, Object.fromEntries(rpcs.filter((p) => announced.has(p.name)).map((p) => [p.name, (Array.isArray(p.value) ? p.value[i] : p.value) ?? 0]))]));
  hub.announce = (route, patch) => {
    Object.assign(values.get(route), patch);
    hub.text({ type: 'settings', seq: hub.seq + 1, route, values: { ...values.get(route) } });
  };
  hub.presence = (route, presence) => hub.text({ type: 'device', seq: hub.seq + 1, route, name: 'test', serial: route, firmware: '1', presence });
  if (rpcs.length) {
    hub.text({ type: 'routes', seq: hub.seq + 1, mounts: {}, routes });
    for (const route of routes) {
      hub.presence(route, 'alive');
      hub.text({ type: 'table', seq: hub.seq + 1, route, rpcs: table });
      hub.announce(route, {});
    }
  }
  hub.calls = [];
  hub.answer = (name, value) => ({ state: 'ok', value: value === '' ? null : typed(value) });
  hub.slow = false;
  const waiting = [];
  hub.release = () => waiting.shift()?.();
  /** The reply to a call: refused and skipped when any device refuses, else each device's answer. */
  const reply = (name, value, to) => {
    const answers = to.map((route) => ({ route, ...hub.answer(name, value, route) }));
    if (answers.some((r) => r.state === 'refused')) {
      return { sent: false, results: answers.map((r) => (r.state === 'refused' ? { route: r.route, state: 'refused', error: r.error } : { route: r.route, state: 'skipped' })) };
    }
    for (const r of answers) if (r.state === 'ok' && announced.has(name)) hub.announce(r.route, { [name]: typed(value) });
    return { sent: true, results: answers };
  };
  hub.call = (name, value, to) => {
    hub.calls.push({ name, value, routes: to });
    if (!hub.slow) return Promise.resolve(reply(name, value, to));
    return new Promise((done) => waiting.push(() => done(reply(name, value, to))));
  };
  hub.read = async (route, names) => ({ values: Object.fromEntries(names.filter((n) => announced.has(n)).map((n) => [n, { state: 'ok', value: values.get(route)?.[n] }])) });
  return hub;
}

/**
 * The page with a tab of plots showing `keys`, a list of signal keys per
 * plot, laid out as layout.js preset `preset`, open, `more` empty tabs
 * after it. Its hub has no socket and has announced the signals `signals`
 * in `units` and a device with `rpcs` at each of `routes`, as `stubHub`
 * takes them. localStorage holds `saved`, by key as JSON, and nothing
 * else, as it mounts; with `fromSaved`, its tabs are those `saved` keeps, as
 * a page loaded again reads them. What the page asks of /api is refused, as a hub that
 * has gone away refuses it, until the page is disposed of; but with
 * `rowsPerSecond`, /api/rows counts that many rows a second of each
 * signal's window, each count answered `rowsLatency` ms of the page's
 * time after it is asked, as a hub's reply takes. Its clock is virtual, stepped by `frames`, `settle`,
 * `wait` and `until`, until it is disposed of; with `realClock`, the
 * browser's own, as the smoke tests of real time mount it. It returns the
 * page's state, each plot's view and PlotView among it.
 */
export async function mountPage(keys = [[], [], [], []], preset = 'grid', more = 0, signals = [], rpcs = [], { routes = ['/'], saved = {}, units = {}, rowsPerSecond = null, rowsLatency = 0, fromSaved = false, realClock = false, captures = null } = {}) {
  virtual?.end();
  virtual = realClock ? null : clock.virtual();
  const mine = virtual;
  localStorage.clear();
  for (const [k, v] of Object.entries(saved)) localStorage.setItem(k, JSON.stringify(v));
  // A plot of captures' keys is a capture plot, any other a time plot (tabs.js `joined`).
  const ps = keys.map((k) => newPlot('time', 'overlay', [])).map((p, i) => ({ ...p, ...joined(p, keys[i]) }));
  const tab = makeTab('A', { plots: Object.fromEntries(ps.map((p) => [p.id, p])), layout: ps.length ? presets[preset](ps.map((p) => p.id)) : null });
  const ws = new Workspace(fromSaved ? loadTabs(localStorage) : { tabs: [tab, ...Array.from({ length: more }, (_, i) => emptyTab(`Layout ${i + 2}`))], activeId: tab.id });
  const history = new History(ws, new Favorites(null));
  const target = document.body.appendChild(document.createElement('div'));
  const hub = stubHub(signals, rpcs, routes, units);
  if (captures) serveCaptures(hub, captures);
  const registry = new Set();
  const views = new Views(hub);
  const fetched = window.fetch;
  const reply = (body, status = 200) => Promise.resolve(new Response(JSON.stringify(body), { status }));
  window.fetch = (url, init) => {
    const at = new URL(String(url), location.href);
    if (!at.pathname.startsWith('/api/')) return fetched(url, init);
    if (at.pathname === '/api/rows' && rowsPerSecond !== null) {
      const span = Number(at.searchParams.get('t1')) - Number(at.searchParams.get('t0'));
      const counted = () => reply({ count: Math.round(span * rowsPerSecond), oldest: 0, x: [], y: [] });
      return rowsLatency ? new Promise((done) => clock.after(rowsLatency, done)).then(counted) : counted();
    }
    return reply({ error: 'no hub in a test' }, 503);
  };
  await noticesGone();
  const plots = new PlotHost({ hub, ws, history, views, registry, colors });
  const app = mount(App, { target, context: new Map([['page', { hub, ws, history, views, plots }]]) });
  await frames();
  return {
    ws,
    history,
    hub,
    views,
    /** The page's PlotHost, which its plots reach the page through. */
    plots,
    /** The PlotView of plot `id` while it is on the page. */
    pv: (id) => [...registry].find((p) => p.plotId === id),
    dispose: () => {
      // Its notices close with it, while it still hears of their closing.
      if (document.querySelector('[data-sonner-toast]')) {
        notices.dismiss();
        flushSync();
        noticesClosed = performance.now();
      }
      unmount(app);
      target.remove();
      window.fetch = fetched;
      mine?.end();
      if (virtual === mine) virtual = null;
    },
  };
}

/**
 * Gives each of `hub`'s traces `seconds` of samples at 100 Hz up to now, a
 * sine of `amplitude`, and a spectrum of 50 bins from 1 Hz to 50 Hz, as
 * large, so each plot draws its axes, its legend's values and its cursors'
 * readout.
 */
export function feed(hub, seconds = 20, amplitude = 1) {
  const session = hub.session, perf = clock.now(), traces = [...session.traces.values()];
  for (const t of traces) {
    const xs = Float64Array.from({ length: seconds * 100 }, (_, i) => i / 100);
    const ys = Float32Array.from(xs, (x) => amplitude * Math.sin(x + t.meta.id));
    session.apply({ kind: 'points', chunks: [{ id: t.meta.id, level: 0, brk: false, xs, ys }] }, perf);
  }
  const f = Float32Array.from({ length: 50 }, (_, i) => i + 1);
  session.apply({ kind: 'spectra', spectra: traces.map((t) => ({ id: t.meta.id, f, a: f.map((v) => amplitude / v), floor: 0.02 * amplitude })) }, perf);
  session.tick(perf);
}

/** How often the server sends each trace's spectrum, and what it holds, in ms (twinleaf-web/src/hub.rs SPECTRUM_EVERY, MEMORY_EVERY). */
export const SPECTRUM_EVERY_MS = 100;
export const MEMORY_EVERY_MS = 1000;

/**
 * Lets `ms` of the page's time pass a frame at a time while `hub`, fed by
 * `feed`, streams as the server does: every frame each trace's new samples
 * at 100 Hz, every SPECTRUM_EVERY_MS the spectrum `feed` gave, and every
 * MEMORY_EVERY_MS what the hub holds, every sample from the first, each
 * arriving before the frame it is due in. After each frame it calls
 * `each(frame)`, the frames counted from 0: what that frame drew.
 */
export async function stream(hub, ms, each = () => {}) {
  const session = hub.session, traces = [...session.traces.values()], f = Float32Array.from({ length: 50 }, (_, i) => i + 1);
  const due = (every, from, to) => Math.floor(to / every) > Math.floor(from / every);
  for (let frame = 0; frame * clock.FRAME_MS < ms; frame++) {
    const perf = clock.now(), next = perf + clock.FRAME_MS;
    const first = Math.floor(session.latest * 100) + 1, last = Math.floor((session.latest + clock.FRAME_MS / 1000) * 100);
    const xs = Float64Array.from({ length: Math.max(0, last - first + 1) }, (_, i) => (first + i) / 100);
    if (xs.length) session.apply({ kind: 'points', chunks: traces.map((t) => ({ id: t.meta.id, level: 0, brk: false, xs, ys: Float32Array.from(xs, (x) => Math.sin(x + t.meta.id)) })) }, perf);
    if (due(SPECTRUM_EVERY_MS, perf, next)) session.apply({ kind: 'spectra', spectra: traces.map((t) => ({ id: t.meta.id, f, a: f.map((v) => 1 / v), floor: 0.02 })) }, perf);
    if (due(MEMORY_EVERY_MS, perf, next)) hub.text({ type: 'memory', bytes: 0, budget: 0, seconds: session.latest, max_age: 600, rate: 0, oldest: Object.fromEntries(traces.map((t) => [t.meta.id, 0])) });
    await frames(1);
    each(frame);
  }
}

/**
 * Gives plot `id` of `page`'s open tab the cursors `change(cursors)` makes
 * of those it has (cursors.js), as a step made before a case.
 */
export function editCursors(page, id, change) {
  page.history.group('Prepare', () => page.ws.setPlot(id, { cursors: change(page.ws.plots[id].cursors) }));
}
/** `cursors` with Y1 and Y2 of y axis `key` of a plot of `kind` at `a` and `b`. */
export const withYPair = (cursors, kind, key, a, b) => withY(withY(cursors, kind, key, 'a', a), kind, key, 'b', b);

export async function settle(n = 3) {
  flushSync();
  await frames(n);
  flushSync();
}
export const leafOf = (id) => document.querySelector(`.leaf[data-plot="${id}"]`);

/** Presses and lets go of the right button at `at`, as a right-click does. */
export async function rightClick(at) {
  await commands.mouse([['move', ...at], ['down', 'right'], ['up', 'right']]);
  await settle();
}
/** Clicks the left button at `at`. */
export async function click(at) {
  await commands.mouse([['move', ...at], ['down'], ['up']]);
  await settle();
}

/**
 * Presses and lets go of keyboard key `name` as Playwright names it, 'l',
 * 'Escape' or 'Control+z', any modifiers before the last `+` held through
 * it.
 */
export function pressKey(name) {
  const parts = name.length > 1 ? name.split('+') : [name];
  const mods = parts.slice(0, -1), last = parts.at(-1);
  // A character stands for itself, `[` and `{` written twice; a name is in braces.
  const key = last.length === 1 ? last.replace(/[[{]/, '$&$&') : `{${last}}`;
  return userEvent.keyboard(mods.map((m) => `{${m}>}`).join('') + key + mods.reverse().map((m) => `{/${m}}`).join(''));
}

/** What an open menu, dropdown, popover or list is. */
export const OPEN = '[data-slot$=-menu-content], [data-slot$=-menu-sub-content], [data-slot=popover-content], [data-slot=select-content]';
/** Closes whatever menu or popover is open, by Escape, and waits until it has gone. */
export async function closeAll() {
  for (let i = 0; i < 5 && document.querySelector(OPEN); i++) {
    const open = document.querySelectorAll(OPEN).length;
    await pressKey('Escape');
    await until(() => document.querySelectorAll(OPEN).length < open, 500);
  }
  expect(document.querySelector(OPEN), 'every menu closed').toBe(null);
}

/**
 * Clicks `el` with the real mouse at its centre, as a person does, its
 * press first: with Ctrl or Shift held when `ctrl` or `shift`, or with the
 * right button when `right`. It scrolls `el` into view first, and presses
 * once `el` has stopped moving and is what the pointer hits at its centre,
 * as a person presses what they see, not what is sliding or covered; it
 * throws when that does not come to be.
 */
export async function clickOn(el, { ctrl = false, shift = false, right = false } = {}) {
  el.scrollIntoView({ block: 'nearest', inline: 'nearest' });
  let at = centre(el);
  for (let i = 0; ; i++) {
    await frames(1);
    const now = centre(el);
    const hit = document.elementFromPoint(...now);
    if (now[0] === at[0] && now[1] === at[1] && el.contains(hit)) break;
    if (i > 120) throw new Error(`${el.outerHTML.slice(0, 80)} is not what the pointer hits: ${hit?.outerHTML.slice(0, 80)}`);
    at = now;
  }
  const held = [...(ctrl ? ['Control'] : []), ...(shift ? ['Shift'] : [])];
  const button = right ? ['right'] : [];
  await commands.mouse([['move', ...at], ...held.map((k) => ['hold', k]), ['down', ...button], ['up', ...button], ...held.map((k) => ['let go', k])]);
}

/** Types `text` into field `el` from the keyboard, in place of what it held, after a click in it. */
export async function typeInto(el, text) {
  await clickOn(el);
  await pressKey('Control+a');
  await pressKey('Backspace');
  for (const ch of text) await pressKey(ch);
}

export const centre = (el) => {
  const r = el.getBoundingClientRect();
  return [r.left + r.width / 2, r.top + r.height / 2];
};

/**
 * Presses at `from`, moves through each point of `via` and on to `to`, a few
 * steps a leg, resting `rest` ms at each point so the drag library sees it,
 * and lets go.
 */
export async function drag(from, to, via = [], rest = 0) {
  const path = [['move', ...from], ['down'], ['move', from[0] + 8, from[1] + 8]];
  let at = from;
  for (const p of [...via, to]) {
    for (let k = 1; k <= 4; k++) path.push(['move', at[0] + ((p[0] - at[0]) * k) / 4, at[1] + ((p[1] - at[1]) * k) / 4]);
    if (rest) path.push(['wait', rest]);
    at = p;
  }
  path.push(['up']);
  await commands.mouse(path);
}

/** The moves from `from` to `to` in `n` steps, for `commands.mouse`. */
export const glide = (from, to, n = 4) => Array.from({ length: n }, (_, k) => ['move', from[0] + ((to[0] - from[0]) * (k + 1)) / n, from[1] + ((to[1] - from[1]) * (k + 1)) / n]);

/**
 * What `fc.assert` takes to try each of `examples` and then its random
 * cases: the examples count toward a property's cases, so they come on top.
 */
export const withExamples = (examples) => ({ examples, numRuns: examples.length + fc.readConfigureGlobal().numRuns });

/**
 * Steps a frame at a time until `ready()` holds, or the frames of `ms` have
 * passed; says whether it holds. It counts frames and reads no clock, so a
 * slow machine has as many steps as a fast one.
 */
export async function until(ready, ms = 5000) {
  for (let left = Math.ceil(ms / clock.FRAME_MS); ; left--) {
    flushSync();
    if (ready()) return true;
    if (left <= 0) return false;
    await frames(1);
  }
}

/**
 * Closes every notice on the page and waits until they have gone. The
 * notices stack up over the plots, an undo's and a redo's among them, stay
 * while the pointer rests on them, and take every press on them: a press
 * meant for a plot comes after this.
 */
export async function clearNotices() {
  if (!document.querySelector('[data-sonner-toast]')) return;
  notices.dismiss();
  expect(await until(() => !document.querySelector('[data-sonner-toast]'), 3000), 'the notices gone').toBe(true);
}

/** Waits until no drag of the drag library is under way, as one lets go and its drop lands. */
export const dropped = () => until(() => !document.getElementById('dnd-action-dragged-el'));

/** The plots whose tiles show, by id, in page order: hidden ones, as behind maximized plots, left out. */
export const showing = () => [...document.querySelectorAll('.tiles .leaf[data-plot]')].filter((l) => getComputedStyle(l).visibility !== 'hidden').map((l) => l.dataset.plot);

/** A click on the status bar, which gives the page the keys and deselects, as the first thing a person does. */
export async function focusPage() {
  await commands.mouse([['move', ...centre(document.querySelector('.status-bar'))], ['down'], ['up']]);
  flushSync();
}

/** Right-clicks plot `id`'s body, which opens its menu, and waits for it. */
export async function plotMenu(id) {
  await commands.mouse([['move', ...centre(leafOf(id).querySelector('.plot-body'))], ['down', 'right'], ['up', 'right']]);
  await until(() => document.querySelector('[data-slot=context-menu-content][data-state=open]'));
}

/** Picks the menu item whose text starts with each of `labels` in turn, a submenu's trigger then an item in it, each once it shows. */
export async function pickItem(...labels) {
  for (const label of labels) {
    const item = () => [...document.querySelectorAll('[role^=menuitem]')].find((m) => m.textContent.trim().startsWith(label));
    await until(item, 2000);
    item()?.click();
    flushSync();
    await frames();
  }
}

/** Empty dock or panel space, where a box starts: a dock's frame, a dock below its panels, a panel's body, a list's space below its items. */
export const EMPTY = '.dock-frame, .dock-side, .dock, .dock-body, .palette, [data-slot=scroll-area-viewport]';

/** How far, in px, empty space reaches around a point `emptySpots` gives: a row beside it may still grow by less, as its font loads. */
const EMPTY_MARGIN = 4;

/** Points in the docks where a press lands on empty space, with empty space EMPTY_MARGIN px each way within the same frame, each [x, y]. */
export function emptySpots() {
  const out = [];
  for (const frame of document.querySelectorAll('.dock-frame')) {
    const r = frame.getBoundingClientRect();
    const empty = (x, y) => {
      const el = document.elementFromPoint(Math.min(Math.max(x, r.left + 1), r.right - 1), Math.min(Math.max(y, r.top + 1), r.bottom - 1));
      return !!el && frame.contains(el) && el.matches(EMPTY);
    };
    const m = EMPTY_MARGIN;
    for (let y = r.top + 4; y < r.bottom - 2; y += 6) {
      for (const x of [r.left + 3, r.left + r.width / 2, r.right - 3]) {
        if (empty(x, y) && empty(x, y - m) && empty(x, y + m) && empty(x - m, y) && empty(x + m, y)) out.push([x, y]);
      }
    }
  }
  return out;
}

/** A point on plot `leaf`'s header where a press picks the plot up: the header's own empty space or its chip row's. */
export function headerSpot(leaf) {
  const head = leaf.querySelector('.plot-head'), r = head.getBoundingClientRect(), y = r.top + r.height / 2;
  for (let x = r.right - 4; x > r.left; x -= 4) {
    const el = document.elementFromPoint(x, y);
    if (el === head || el?.classList.contains('trace-zone')) return [x, y];
  }
  return null;
}

/** `steps` for `commands.mouse` with keyboard keys `keys` held through them, as Playwright names them. */
export const holding = (keys, steps) => [...keys.map((k) => ['hold', k]), ...steps, ...keys.map((k) => ['let go', k])];

/**
 * Keeps the page from the hub's /api, which a test has none of: a window
 * read waits for ever, and a count is `rows(query)`, none by default.
 * Undone by `vi.restoreAllMocks()`.
 */
export function quietApi(rows = () => Promise.resolve({ count: 0, oldest: null, x: [], y: [] })) {
  vi.spyOn(api, 'window').mockImplementation(() => new Promise(() => {}));
  vi.spyOn(api, 'rows').mockImplementation(rows);
}

/** The rows a second of each signal a test hub holds, as `stubHub` announces its traces. */
const RATE = 100;
/** The seconds each level of a hub's decimation keeps of a history long enough, finest first: each four times the last. */
const LEVEL_SECONDS = Array.from({ length: 6 }, (_, k) => 120 * 4 ** k);
/** How much further back than the hub's raw history the page's coarsest points reach, as a share of it. */
const COARSE_REACH = 0.07;
/** The seconds a hub keeps of each stream beyond its History setting: twinleaf-web/src/hub.rs HISTORY_HEADROOM. */
export const HISTORY_HEADROOM = 2;

/**
 * Has `page`'s hub hold `seconds` of each signal, under a History setting
 * of `history` seconds, as a hub that has run for hours does, without an
 * hour of samples being fed. A hub that has filled its history holds the
 * setting and HISTORY_HEADROOM more, which `seconds` is unless given.
 * - The page's clock stands at `now`, so a pause holds the same time in
 *   every run, and the hub's newest sample is `behind` seconds after it:
 *   how far the page's clock trails the hub.
 * - The page has each trace's points at every level of the decimation,
 *   each level a quarter of the rate of the one before over four times the
 *   span, 12 000 points a level at most, the coarse ones of a hub that has
 *   dropped samples reaching COARSE_REACH further back than its raw history.
 * - A `memory` message says the hub holds each signal from `seconds` before
 *   its newest sample, and, with `dropped`, as a hub that has filled its
 *   history has, that it held older samples before.
 * - /api/window answers any range as the server does (twinleaf-web/src/hub.rs
 *   `window`): the rows it holds within it, decimated to at most twice
 *   `width` points, at least 64; and /api/rows counts them. A window read
 *   answers `delay` ms of the page's time after it is asked, at once with
 *   none, and one aborted meanwhile throws as an aborted request does.
 * Returns {now, oldest, asked, read}: `oldest` the oldest time held;
 * `asked` each window read so far, as {id, t0, t1, width, points, at,
 * answered, aborted, crossed}, `at` and `answered` the page's time it was
 * asked and answered, null while it is not, and `crossed` whether an abort
 * came once its reply was due: on the virtual clock a frame's timers and
 * its callbacks run together, so a reply due in a frame lands after that
 * frame's moves, where a browser lands it before; and `read(t0, t1, width)` the window a
 * read is answered with, as {ratio, x}. The answers are undone by
 * `vi.restoreAllMocks()`.
 */
export function holdHistory(page, { history, seconds = history + HISTORY_HEADROOM, now = 100_000, behind = 0, dropped = seconds >= history, delay = 0 } = {}) {
  const hub = page.hub, session = hub.session, newest = now + behind, oldest = newest - seconds, asked = [];
  session.now = () => now;
  const keeps = LEVEL_SECONDS.map((s) => Math.min(s, history * (1 + COARSE_REACH)));
  hub.text({ type: 'config', pps: RATE, levels: keeps });
  for (const t of session.traces.values()) {
    t.clear();
    keeps.forEach((keep, level) => {
      const rate = RATE / 4 ** level, n = Math.floor(Math.min(keep, seconds * (dropped ? 1 + COARSE_REACH : 1)) * rate);
      const xs = Float64Array.from({ length: n }, (_, i) => newest - (n - 1 - i) / rate);
      session.apply({ kind: 'points', chunks: [{ id: t.meta.id, level, brk: false, xs, ys: Float32Array.from(xs, (x) => Math.sin(x / 7 + t.meta.id)) }] });
    });
  }
  const ids = [...session.traces.keys()];
  const memory = (at) => hub.text({ type: 'memory', bytes: 0, budget: 0, seconds: Math.min(seconds, history), max_age: history, rate: 0, oldest: Object.fromEntries(ids.map((id) => [id, at])) });
  // What it held first is what this history says, whatever a history stated before did.
  session.firstHeld = {};
  if (dropped) memory(oldest - 1);
  memory(oldest);
  /** The times of the rows held within [t0, t1], each `ratio` rows after the last. */
  const rows = (t0, t1, ratio = 1) => {
    const a = Math.ceil(Math.max(t0, oldest) * RATE), b = Math.floor(Math.min(t1, newest) * RATE);
    return { count: Math.max(0, b - a + 1), x: (n) => Float64Array.from({ length: n }, (_, i) => (a + i * ratio) / RATE) };
  };
  const read = (t0, t1, width) => {
    const raw = rows(t0, t1).count, ratio = Math.max(1, Math.ceil(raw / Math.max(64, 2 * width)));
    return { ratio, x: rows(t0, t1, ratio).x(Math.ceil(raw / ratio)) };
  };
  vi.spyOn(api, 'window').mockImplementation(async ({ id, t0, t1, width, signal }) => {
    const { ratio, x } = read(t0, t1, width);
    const ask = { id, t0, t1, width, points: x.length, at: clock.now(), answered: null, aborted: false };
    asked.push(ask);
    if (delay > 0) await new Promise((done) => (clock.after(delay, done), signal?.addEventListener('abort', done)));
    if (signal?.aborted) {
      ask.aborted = true;
      ask.crossed = clock.now() >= ask.at + delay;
      throw signal.reason;
    }
    ask.answered = clock.now();
    return { t0, t1, ratio, chunks: x.length ? [{ x, y: Float32Array.from(x, (v) => Math.sin(v / 7 + id)) }] : [] };
  });
  vi.spyOn(api, 'rows').mockImplementation(async ({ t0, t1 }) => ({ count: rows(t0, t1).count, oldest, x: [], y: [] }));
  return { now, oldest, asked, read };
}

/**
 * Turns the wheel over plot `id`'s plot area by `deltaY` px, `n` times, at
 * fraction `at` across it, on the whole pixel nearest, with Shift held when
 * `shift`, and returns the fraction across that pixel is at. The turns are
 * dispatched one after another with no frame between, so what they zoom to
 * depends on neither the browser's input queue nor the machine's speed. A
 * time zoom eases on in the frames after (`wheelRest`).
 */
export function turnWheel(page, id, deltaY, { n = 1, at = 0.5, shift = false } = {}) {
  const P = page.pv(id).geo.plot, canvas = leafOf(id).querySelector('.plot-body canvas'), x = Math.round(P.x + at * P.w);
  for (let i = 0; i < n; i++) {
    canvas.dispatchEvent(new WheelEvent('wheel', { bubbles: true, cancelable: true, deltaY, clientX: x, clientY: Math.round(P.y + P.h / 2), shiftKey: shift }));
  }
  return (x - P.x) / P.w;
}

/** Whether plot `id`'s wheel is easing the time span of a view. */
export const easing = (page, id) => !!page.pv(id).timeZoom?.eases.size;

/** Steps frames until plot `id`'s wheel has eased its views to rest, and gives the frames that took. */
export async function wheelRest(page, id) {
  let n = 0;
  for (; easing(page, id) && n < 600; n++) await frames(1);
  flushSync();
  return n;
}

/** Turns the wheel as `turnWheel` does, then waits for its zoom to rest (`wheelRest`). */
export async function turnWheelToRest(page, id, deltaY, opts) {
  const at = turnWheel(page, id, deltaY, opts);
  await wheelRest(page, id);
  return at;
}
