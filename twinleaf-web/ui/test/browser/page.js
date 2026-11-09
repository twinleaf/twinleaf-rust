// The whole page, mounted for a browser test with state of its own, and the
// gestures the tests make on it with the real mouse.
import { mount, unmount } from 'svelte';
import { commands } from '@vitest/browser/context';
import App from '../../src/App.svelte';
import { Hub } from '../../src/lib/hub.svelte.js';
import { Workspace } from '../../src/lib/workspace.svelte.js';
import { History } from '../../src/lib/history.svelte.js';
import { Favorites } from '../../src/lib/favorites.svelte.js';
import { presets } from '../../src/lib/layout.js';
import { emptyTab, makeTab, newPlot } from '../../src/lib/tabs.js';
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
 * each a trace of route '/' in volts, and, with `rpcs`, a device at each of
 * `routes` whose RPC table they are, each {name, channel, value, type,
 * access}: type f32 and access read and write unless given, and `value`
 * one for every device or an array of one per route.
 *
 * Its devices answer calls: `calls` lists each {name, value, routes}, and
 * `answer(name, value, route)` gives each device's outcome, by default ok
 * with the value as the device takes it. A device that took a value of an
 * entry it reads announces it. `announce(route, values)` has a device
 * announce new values, as another client's write would, and
 * `presence(route, state)` changes a device's presence.
 */
export function stubHub(signals = [], rpcs = [], routes = ['/']) {
  const hub = new Hub();
  hub.close();
  let seq = 0;
  hub.text({ type: 'hub', id: 'test', seq });
  signals.forEach((key, id) => {
    const [stream, column] = key.split('.');
    hub.text({ type: 'trace', seq: ++seq, trace: { id, route: '/', stream, column, units: 'V', rate: 100 } });
  });
  const table = rpcs.map(({ name, channel, type = 'f32', access = ['read', 'write'] }) => ({ name, channel, type, access }));
  const read = new Set(table.filter((r) => r.access.includes('read')).map((r) => r.name));
  const values = new Map(routes.map((route, i) => [route, Object.fromEntries(rpcs.filter((p) => read.has(p.name)).map((p) => [p.name, (Array.isArray(p.value) ? p.value[i] : p.value) ?? 0]))]));
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
  hub.call = async (name, value, to) => {
    hub.calls.push({ name, value, routes: to });
    const results = await Promise.all(to.map(async (route) => ({ route, ...(await hub.answer(name, value, route)) })));
    for (const r of results) if (r.state === 'ok' && read.has(name)) hub.announce(r.route, { [name]: r.value });
    return { sent: to.length, results };
  };
  hub.read = async (route, names) => ({ values: Object.fromEntries(names.map((n) => [n, { state: 'ok', value: values.get(route)?.[n] }])) });
  return hub;
}

/**
 * The page with a tab of plots showing `keys`, a list of signal keys per
 * plot, laid out as layout.js preset `preset`, open, `more` empty tabs
 * after it. Its hub has no socket and has announced the
 * signals `signals` and a device with `rpcs` at each of `routes`, as
 * `stubHub` takes them. localStorage holds `saved`, by key as JSON, and
 * nothing else, as it mounts.
 */
export async function mountPage(keys = [[], [], [], []], preset = 'grid', more = 0, signals = [], rpcs = [], { routes = ['/'], saved = {} } = {}) {
  localStorage.clear();
  for (const [k, v] of Object.entries(saved)) localStorage.setItem(k, JSON.stringify(v));
  const ps = keys.map((k) => newPlot('time', 'overlay', k));
  const tab = makeTab('A', { plots: Object.fromEntries(ps.map((p) => [p.id, p])), layout: ps.length ? presets[preset](ps.map((p) => p.id)) : null });
  const ws = new Workspace({ tabs: [tab, ...Array.from({ length: more }, (_, i) => emptyTab(`Layout ${i + 2}`))], activeId: tab.id });
  const history = new History(ws, new Favorites(null));
  const target = document.body.appendChild(document.createElement('div'));
  const hub = stubHub(signals, rpcs, routes);
  const app = mount(App, { target, context: new Map([['page', { hub, ws, history }]]) });
  await frames();
  return {
    ws,
    history,
    hub,
    dispose: () => {
      unmount(app);
      target.remove();
    },
  };
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
