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

/**
 * A hub with no socket that has announced `signals`, keys as 'stream.column',
 * each a trace of route '/' in volts.
 */
export function stubHub(signals = []) {
  const hub = new Hub();
  hub.close();
  hub.text({ type: 'hub', id: 'test', seq: 0 });
  signals.forEach((key, id) => {
    const [stream, column] = key.split('.');
    hub.text({ type: 'trace', seq: id + 1, trace: { id, route: '/', stream, column, units: 'V', rate: 100 } });
  });
  return hub;
}

/**
 * The page with a tab of plots showing `keys`, a list of signal keys per
 * plot, laid out as layout.js preset `preset`, open, `more` empty tabs
 * after it, and nothing saved. Its hub has no socket and has announced the
 * signals `signals`.
 */
export async function mountPage(keys = [[], [], [], []], preset = 'grid', more = 0, signals = []) {
  localStorage.clear();
  const ps = keys.map((k) => newPlot('time', 'overlay', k));
  const tab = makeTab('A', { plots: Object.fromEntries(ps.map((p) => [p.id, p])), layout: ps.length ? presets[preset](ps.map((p) => p.id)) : null });
  const ws = new Workspace({ tabs: [tab, ...Array.from({ length: more }, (_, i) => emptyTab(`Layout ${i + 2}`))], activeId: tab.id });
  const history = new History(ws, new Favorites(null));
  const target = document.body.appendChild(document.createElement('div'));
  const hub = stubHub(signals);
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
