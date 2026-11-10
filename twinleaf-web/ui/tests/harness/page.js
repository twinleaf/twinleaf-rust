// The whole page, mounted for a browser test with state of its own, and the
// gestures the tests make on it with the real mouse.
import { flushSync, mount, unmount } from 'svelte';
import { expect, vi } from 'vitest';
import { commands } from '@vitest/browser/context';
import App from '../../src/App.svelte';
import { Hub } from '../../src/lib/hub.svelte.js';
import { Workspace } from '../../src/lib/workspace.svelte.js';
import { History } from '../../src/lib/history.svelte.js';
import { Favorites } from '../../src/lib/favorites.svelte.js';
import { Views } from '../../src/lib/views.js';
import { presets } from '../../src/lib/layout.js';
import { emptyTab, makeTab, newPlot } from '../../src/lib/tabs.js';
import { api } from '../../src/lib/protocol.js';
import '../../src/app.css';
import '../../src/plot.css';

export const wait = (ms) => new Promise((done) => setTimeout(done, ms));
export const frames = (n = 2) =>
  new Promise((done) => {
    const next = (k) => (k ? requestAnimationFrame(() => next(k - 1)) : done());
    next(n);
  });

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
 * `presence(route, state)` changes a device's presence.
 */
export function stubHub(signals = [], rpcs = [], routes = ['/'], units = {}) {
  const hub = new Hub();
  hub.close();
  let seq = 0;
  hub.text({ type: 'hub', id: 'test', seq });
  signals.forEach((key, id) => {
    const [stream, column] = key.split('.');
    hub.text({ type: 'trace', seq: ++seq, trace: { id, route: '/', stream, column, units: units[key] ?? 'V', rate: 100 } });
  });
  const table = rpcs.map(({ name, channel, type = 'f32', access = ['read', 'write'] }) => ({ name, channel, type, access, readable: access.includes('read') }));
  const announced = new Set(rpcs.filter((p) => (p.access ?? ['read', 'write']).includes('read') && p.announced !== false).map((p) => p.name));
  const values = new Map(routes.map((route, i) => [route, Object.fromEntries(rpcs.filter((p) => announced.has(p.name)).map((p) => [p.name, (Array.isArray(p.value) ? p.value[i] : p.value) ?? 0]))]));
  hub.announce = (route, patch) => {
    Object.assign(values.get(route), patch);
    hub.text({ type: 'settings', seq: ++seq, route, values: { ...values.get(route) } });
  };
  hub.presence = (route, presence) => hub.text({ type: 'device', seq: ++seq, route, name: 'test', serial: route, firmware: '1', presence });
  if (rpcs.length) {
    hub.text({ type: 'routes', seq: ++seq, mounts: {}, routes });
    for (const route of routes) {
      hub.presence(route, 'alive');
      hub.text({ type: 'table', seq: ++seq, route, rpcs: table });
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
 * else, as it mounts. What the page asks of /api is refused, as a hub that
 * has gone away refuses it, until the page is disposed of; but with
 * `rowsPerSecond`, /api/rows counts that many rows a second of each
 * signal's window. It returns the page's state, each plot's view and
 * PlotView among it.
 */
export async function mountPage(keys = [[], [], [], []], preset = 'grid', more = 0, signals = [], rpcs = [], { routes = ['/'], saved = {}, units = {}, rowsPerSecond = null } = {}) {
  localStorage.clear();
  for (const [k, v] of Object.entries(saved)) localStorage.setItem(k, JSON.stringify(v));
  const ps = keys.map((k) => newPlot('time', 'overlay', k));
  const tab = makeTab('A', { plots: Object.fromEntries(ps.map((p) => [p.id, p])), layout: ps.length ? presets[preset](ps.map((p) => p.id)) : null });
  const ws = new Workspace({ tabs: [tab, ...Array.from({ length: more }, (_, i) => emptyTab(`Layout ${i + 2}`))], activeId: tab.id });
  const history = new History(ws, new Favorites(null));
  const target = document.body.appendChild(document.createElement('div'));
  const hub = stubHub(signals, rpcs, routes, units);
  const registry = new Set();
  const views = new Views(hub);
  const fetched = window.fetch;
  const reply = (body, status = 200) => Promise.resolve(new Response(JSON.stringify(body), { status }));
  window.fetch = (url, init) => {
    const at = new URL(String(url), location.href);
    if (!at.pathname.startsWith('/api/')) return fetched(url, init);
    if (at.pathname === '/api/rows' && rowsPerSecond !== null) {
      const span = Number(at.searchParams.get('t1')) - Number(at.searchParams.get('t0'));
      return reply({ count: Math.round(span * rowsPerSecond), oldest: 0, x: [], y: [] });
    }
    return reply({ error: 'no hub in a test' }, 503);
  };
  const app = mount(App, { target, context: new Map([['page', { hub, ws, history, registry, views }]]) });
  await frames();
  return {
    ws,
    history,
    hub,
    views,
    /** The PlotView of plot `id` while it is on the page. */
    pv: (id) => [...registry].find((p) => p.plotId === id),
    dispose: () => {
      unmount(app);
      target.remove();
      window.fetch = fetched;
    },
  };
}

/**
 * Gives each of `hub`'s traces `seconds` of samples at 100 Hz up to now, and
 * a spectrum of 50 bins from 1 Hz to 50 Hz, so each plot draws its axes, its
 * legend's values and its cursors' readout.
 */
export function feed(hub, seconds = 20) {
  const session = hub.session, perf = performance.now(), traces = [...session.traces.values()];
  for (const t of traces) {
    const xs = Float64Array.from({ length: seconds * 100 }, (_, i) => i / 100);
    const ys = Float32Array.from(xs, (x) => Math.sin(x + t.meta.id));
    session.apply({ kind: 'points', chunks: [{ id: t.meta.id, level: 0, brk: false, xs, ys }] }, perf);
  }
  const f = Float32Array.from({ length: 50 }, (_, i) => i + 1);
  session.apply({ kind: 'spectra', spectra: traces.map((t) => ({ id: t.meta.id, f, a: f.map((v) => 1 / v), floor: 0.02 })) }, perf);
  session.tick(perf);
}

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

/** What an open menu, dropdown, popover or list is. */
export const OPEN = '[data-slot$=-menu-content], [data-slot$=-menu-sub-content], [data-slot=popover-content], [data-slot=select-content]';
/** Closes whatever menu or popover is open, by Escape, and waits until it has gone. */
export async function closeAll() {
  for (let i = 0; i < 5 && document.querySelector(OPEN); i++) {
    await commands.key('Escape');
    await wait(150);
    flushSync();
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
  await commands.key('Control+a');
  await commands.key('Backspace');
  for (const ch of text) await commands.key(ch);
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

/** Waits, a frame at a time, until `ready()` holds or `ms` have passed; says whether it holds. */
export async function until(ready, ms = 5000) {
  const end = performance.now() + ms;
  for (;;) {
    flushSync();
    if (ready()) return true;
    if (performance.now() > end) return false;
    await frames(1);
  }
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

/** Points in the docks where a press lands on empty space, each [x, y]. */
export function emptySpots() {
  const out = [];
  for (const frame of document.querySelectorAll('.dock-frame')) {
    const r = frame.getBoundingClientRect();
    for (let y = r.top + 4; y < r.bottom - 2; y += 6) {
      for (const x of [r.left + 3, r.left + r.width / 2, r.right - 3]) {
        const el = document.elementFromPoint(x, y);
        if (el && frame.contains(el) && el.matches(EMPTY)) out.push([x, y]);
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
