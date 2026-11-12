// Device captures on the page: the Captures of each device under its
// signals in the Signals list, put on plots by the list's own gestures; a
// capture plot, a slice along the capture's own x, drawn by the path a
// spectrum is; Trigger and Auto, which ask the hub for what the plots show
// and no more; where a capture stands, said on the plot; and the overlay
// and the average of its last shots.
//
// The hub is the harness's (harness/captures.js `serveCaptures`), which
// lists captures, takes asks and sends frames as the hub does, the frames
// byte for byte (src/lib/protocol.test.js).
import { afterEach, describe, expect, it, vi } from 'vitest';
import { commands, page as browser } from 'vitest/browser';
import { flushSync } from 'svelte';
import { centre, clickOn, closeAll, dropped, frames, leafOf, mountPage, pickItem, pressKey, rightClick, settle, typeInto, until, wait } from './harness/page.js';
import { fid, keyOf } from './harness/captures.js';
import { plotIds } from '../src/lib/layout.js';
import { APART, SAVE_KEY } from '../src/lib/tabs.js';
import { SHOT_COUNTS } from '../src/lib/shots.js';
import { CAPTURE_DELAY } from '../src/lib/constants.js';

/** Where VITE_TL_SHOTS names a folder, relative to this file, the screenshots go there. */
const SHOTS = import.meta.env.VITE_TL_SHOTS;
const shoot = async (name, element) => SHOTS && (await browser.screenshot({ path: `${SHOTS}/${name}.png`, element }));

const SIGNALS = ['sine.sin', 'sine.cos'];
/** An FID capture, 1500 16-bit samples 0.5 µs apart, named by its metadata; and a phase capture the device has not named, 32-bit floats 1 µs apart. */
const FID = { rpc: 'fid.capture', label: { name: 'FID', units: 'V', x_name: 'Time', x_units: 's' }, shot: fid() };
const PHASE = { rpc: 'phase.capture', label: null, shot: (number) => ({ type: 0x42, calibration: 1, x0: 0, dx: 1e-6, elements: Float32Array.from({ length: 750 }, (_, i) => 0.01 * i * number) }) };
const K = { fid: keyOf('fid.capture'), phase: keyOf('phase.capture') };
/** How long the test hub takes a capture, in ms of the page's time, and a wait that outlasts it. */
const TAKEN = 150;
/** The FID's calibration as the frame carries it, a 32-bit float. */
const CAL = Math.fround(1e-4);

const mount = (keys, opts = {}) => mountPage(keys, 'rows', 0, SIGNALS, [], { captures: [FID, PHASE], ...opts });
const rows = () => [...document.querySelectorAll('.palette .chip[data-key]')];
const row = (key) => rows().find((r) => r.dataset.key === key);
const button = (id, command) => leafOf(id).querySelector(`.plot-head [data-command=${command}]`);
const stateOf = (id) => [...leafOf(id).querySelectorAll('.capture-state div')].map((d) => d.textContent.trim()).join('; ');
const notices = () => [...document.querySelectorAll('[data-sonner-toast]')].filter((t) => t.dataset.removed !== 'true').map((t) => t.textContent);
/** The capture messages the page sent the hub, each as "type rpc" and a repeat's delay. */
const posted = (page) => page.hub.captures.posted.filter((m) => ['capture', 'repeat', 'stop'].includes(m.type)).map((m) => `${m.type} ${m.rpc}${m.type === 'repeat' ? ` ${m.delay}` : ''}`);
/** Triggers a capture on plot `id` by its button, and waits until the hub has sent it. */
async function trigger(page, id) {
  await clickOn(button(id, 'captureNow'));
  await wait(TAKEN);
  await settle();
}
/** The slice plot `id` draws of its first capture. */
const slice = (page, id) => page.pv(id).drawn?.plans[0]?.s;

afterEach(() => vi.restoreAllMocks());

describe('the Signals list', () => {
  it('lists a device’s captures under its signals, headed Captures, each by its metadata’s name and units, or its RPC’s name, and its search finds them', async () => {
    const page = await mount([[]]);
    try {
      expect(rows().map((r) => r.dataset.key)).toEqual(['sine.cos', 'sine.sin', K.fid, K.phase]);
      const [, , first, second] = rows();
      expect([first.querySelector('.sig-key').textContent, first.querySelector('.sig-units').textContent]).toEqual(['FID', 'V']);
      expect([second.querySelector('.sig-key').textContent, second.querySelector('.sig-units')]).toEqual(['phase.capture', null]);
      // The first capture is headed, in room of its own above it, clear of the signal before.
      expect(rows().map((r) => getComputedStyle(r, '::before').content)).toEqual(['none', 'none', '"Captures"', 'none']);
      const gap = (a, b) => b.getBoundingClientRect().top - a.getBoundingClientRect().bottom;
      expect(gap(rows()[1], first)).toBeGreaterThan(gap(rows()[0], rows()[1]) + 12);
      await shoot('list', document.querySelector('.palette'));
      // The search matches a capture by the name it shows, and a signal as before.
      await typeInto(document.querySelector('.dock-panel input[type=search]'), 'fid');
      await settle();
      expect(rows().map((r) => r.dataset.key)).toEqual([K.fid]);
      expect(getComputedStyle(rows()[0], '::before').content).toBe('"Captures"');
      await typeInto(document.querySelector('.dock-panel input[type=search]'), 'cos');
      await settle();
      expect(rows().map((r) => r.dataset.key)).toEqual(['sine.cos']);
    } finally {
      page.dispose();
    }
  });

  for (const how of ['double-click', 'Enter', 'menu', 'drag'])
    it(`puts a capture on a plot by ${how}, which makes an empty plot a capture plot`, async () => {
      const page = await mount([[]]);
      try {
        const [id] = plotIds(page.ws.layout), el = row(K.fid);
        page.ws.selection.only([id]);
        flushSync();
        if (how === 'double-click') await commands.mouse([['dblclick', ...centre(el.querySelector('.sig-key'))]]);
        else if (how === 'Enter') (await clickOn(el.querySelector('.sig-key')), page.ws.selection.only([id]), await pressKey('Enter'));
        else if (how === 'menu') (await rightClick(centre(el.querySelector('.sig-key'))), await pickItem('Add to plot'));
        else {
          const from = centre(el.querySelector('.sig-key')), to = centre(leafOf(id).querySelector('.plot-body'));
          await commands.mouse([['move', ...from], ['down'], ['move', from[0] + 8, from[1] + 8], ...[1, 2, 3, 4].map((k) => ['move', from[0] + ((to[0] - from[0]) * k) / 4, from[1] + ((to[1] - from[1]) * k) / 4]), ['up']]);
          await dropped();
        }
        await settle();
        const plot = Object.values(page.ws.plots).find((p) => p.items.length);
        expect(plot, 'a plot shows it').toBeTruthy();
        expect([plot.kind, plot.items.map((i) => i.key)]).toEqual(['capture', [K.fid]]);
        // Its chip names it as the list does, and its header has Trigger and Auto where a time plot pauses.
        const leaf = leafOf(plot.id);
        expect(leaf.querySelector('.trace-chip .name').textContent).toBe('FID');
        expect([!!button(plot.id, 'captureNow'), !!button(plot.id, 'captureAuto'), !!button(plot.id, 'pause')]).toEqual([true, true, false]);
        // Showing it asks for nothing: no capture is taken until one is asked for.
        expect(posted(page)).toEqual([]);
      } finally {
        page.dispose();
      }
    });

  it('is in the command palette too, by its name, and the edits of a capture are named by it', async () => {
    const page = await mount([[]]);
    try {
      const [id] = plotIds(page.ws.layout), label = () => page.history.undos.at(-1)?.label;
      page.ws.selection.only([id]);
      await pressKey('Control+k');
      await settle();
      const rows = (command) => [...document.querySelectorAll(`[data-slot=command-item][data-command=${command}]`)].map((r) => r.textContent.trim());
      expect(await until(() => rows('addToPlots').length)).toBe(true);
      // The captures after the signals, each by the name the list gives it, to add or to make a plot of.
      expect([rows('addToPlots'), rows('newPlot')]).toEqual([['sine.cos', 'sine.sin', 'FID', 'phase.capture'], ['sine.cos', 'sine.sin', 'FID', 'phase.capture']]);
      [...document.querySelectorAll('[data-slot=command-item][data-command=addToPlots]')].find((r) => r.textContent.trim() === 'FID').click();
      await settle();
      expect([page.ws.plots[id].kind, page.ws.plots[id].items.map((i) => i.key), label()]).toEqual(['capture', [K.fid], 'Add FID']);
      leafOf(id).querySelector('.trace-chip .x').click();
      await settle();
      expect([page.ws.plots[id].items, label()]).toEqual([[], 'Remove FID']);
    } finally {
      page.dispose();
    }
  });

  it('keeps captures and signals in plots apart: a plot of one takes none of the other, and says why', async () => {
    const page = await mount([['sine.sin'], [K.fid], []]);
    try {
      const [time, capture, empty] = plotIds(page.ws.layout);
      const run = (on, key) => page.plots.commands.run('addToPlots', { on, signals: [key], from: 'menu' });
      run(time, K.fid);
      run(capture, 'sine.cos');
      await settle();
      expect([page.ws.plots[time].items.map((i) => i.key), page.ws.plots[capture].items.map((i) => i.key)]).toEqual([['sine.sin'], [K.fid]]);
      expect(notices().filter((n) => n.includes(APART)).length).toBeGreaterThan(0);
      expect(page.history.undos.length, 'no edit made').toBe(0);
      // Its own sort it takes: a second capture, and an empty plot takes a signal as a time plot.
      run(capture, K.phase);
      run(empty, 'sine.cos');
      expect([page.ws.plots[capture].items.map((i) => i.key), page.ws.plots[empty].kind]).toEqual([[K.fid, K.phase], 'time']);
      // A capture dragged onto the time plot lands nowhere, and the plot's signals are as they were.
      const from = centre(row(K.fid).querySelector('.sig-key')), to = centre(leafOf(time).querySelector('.plot-body'));
      await commands.mouse([['move', ...from], ['down'], ['move', from[0] + 8, from[1] + 8], ...[1, 2, 3, 4].map((k) => ['move', from[0] + ((to[0] - from[0]) * k) / 4, from[1] + ((to[1] - from[1]) * k) / 4]), ['up']]);
      await dropped();
      await settle();
      expect([page.ws.plots[time].kind, page.ws.plots[time].items.map((i) => i.key)]).toEqual(['time', ['sine.sin']]);
      // A capture plot may be no other kind, and a time plot no capture plot.
      expect(page.plots.commands.row('setKind', { on: capture }).choices).toEqual([['capture', 'Capture']]);
      expect(page.plots.commands.row('setKind', { on: time }).choices.map(([k]) => k)).toEqual(['time', 'spectrum']);
      page.plots.commands.run('setKind', { on: time, how: { to: 'capture' } });
      page.plots.commands.run('setKind', { on: capture, how: { to: 'time' } });
      expect([page.ws.plots[time].kind, page.ws.plots[capture].kind]).toEqual(['time', 'capture']);
    } finally {
      page.dispose();
    }
  });
});

describe('a capture plot', () => {
  it('draws a capture along its own x, in the capture’s x units, its values calibrated, replaced by the next', async () => {
    const page = await mount([[K.fid], [K.phase]]);
    try {
      const [id, other] = plotIds(page.ws.layout), pv = page.pv(id);
      expect(page.pv(id).xd ?? null, 'nothing drawn before a capture').toBe(null);
      await trigger(page, id);
      const first = FID.shot(1), s = slice(page, id);
      // Its values are the device's counts times the calibration, and its x from 0 to 749.5 µs.
      expect(s.number).toBe(1);
      expect([...s.a]).toEqual([...first.elements].map((v) => Math.fround(v * CAL)));
      expect([s.f[0], s.f.at(-1)]).toEqual([0, Math.fround(1499 * 5e-7)]);
      expect(pv.xd).toEqual([0, Math.fround(1499 * 5e-7)]);
      // The x axis reads in µs, not seconds of a clock; the y axis in volts.
      const xTicks = pv.tickLabels.filter((b) => b.axis === undefined).map((b) => b.text);
      expect(xTicks.length).toBeGreaterThan(2);
      for (const text of xTicks) expect(text).toMatch(/^(0 s|\d+ µs)$/);
      expect(leafOf(id).querySelector('.trace-chip .units').textContent).toBe('V');
      expect(pv.axisUnits('shared')).toBe('V');
      // The other plot, of the capture no one asked for, has none.
      expect(slice(page, other) ?? null).toBe(null);
      // The next capture takes its place.
      await trigger(page, id);
      expect(slice(page, id).number).toBe(2);
      expect([...slice(page, id).a]).toEqual([...FID.shot(2).elements].map((v) => Math.fround(v * CAL)));
      // A capture the device has not named reads in its own numbers: 32-bit floats as they are, not copied.
      await trigger(page, other);
      const phase = page.hub.session.byKey.get(K.phase).shots.at(-1);
      expect(slice(page, other).a).toBe(phase.y);
      expect([...phase.y.slice(0, 3)]).toEqual([0, Math.fround(0.01), Math.fround(0.02)]);
      await shoot('fid', leafOf(id));
    } finally {
      page.dispose();
    }
  });

  it('asks for one capture on Trigger, and on Auto for captures again and again at its delay, until Auto is off or no plot shows it; two plots of one capture share one ask', async () => {
    const page = await mount([[K.fid], [K.fid]]);
    try {
      const [a, b] = plotIds(page.ws.layout), cmds = page.plots.commands, asks = () => page.hub.captures.asks('fid.capture');
      expect(posted(page)).toEqual([]);
      await trigger(page, a);
      expect(posted(page)).toEqual(['capture fid.capture']);
      // Both plots show the one capture the hub sent.
      expect([slice(page, a).number, slice(page, b).number]).toEqual([1, 1]);
      // Auto: the delay it has until set, and its button pressed.
      await clickOn(button(a, 'captureAuto'));
      await settle();
      expect([page.ws.plots[a].auto, button(a, 'captureAuto').getAttribute('aria-pressed'), button(b, 'captureAuto').getAttribute('aria-pressed')]).toEqual([true, 'true', 'false']);
      expect(posted(page).at(-1)).toBe(`repeat fid.capture ${CAPTURE_DELAY}`);
      // The other plot on Auto too asks nothing more; at a shorter delay, the hub is asked for that one.
      const sent = posted(page).length;
      await clickOn(button(b, 'captureAuto'));
      await settle();
      expect(posted(page).length).toBe(sent);
      cmds.run('captureDelay', { on: b, how: { to: 0.2 } });
      await settle();
      expect(posted(page).slice(sent)).toEqual(['repeat fid.capture 0.2']);
      // Captures come again and again.
      const before = page.hub.captures.number('fid.capture');
      await wait(1500);
      await settle();
      expect(page.hub.captures.number('fid.capture')).toBeGreaterThan(before + 2);
      expect(slice(page, a).number).toBe(page.hub.captures.number('fid.capture'));
      // The plot at the shorter delay closed: the other's delay, and no stop.
      cmds.run('close', { on: b });
      await settle();
      expect(posted(page).at(-1)).toBe(`repeat fid.capture ${CAPTURE_DELAY}`);
      expect(asks().repeat).toBe(CAPTURE_DELAY);
      // Auto off: the ask is taken back, and no more come.
      await clickOn(button(a, 'captureAuto'));
      await settle();
      expect([posted(page).at(-1), asks()]).toEqual(['stop fid.capture', { once: false, repeat: null }]);
      await wait(TAKEN);
      const rested = page.hub.captures.number('fid.capture');
      await wait(2000);
      expect(page.hub.captures.number('fid.capture')).toBe(rested);
      // A capture asked for and not yet taken is taken back as the last plot that shows it closes.
      await clickOn(button(a, 'captureNow'));
      cmds.run('close', { on: a });
      await settle();
      expect([posted(page).at(-1), asks()]).toEqual(['stop fid.capture', { once: false, repeat: null }]);
      await wait(1000);
      expect(page.hub.captures.number('fid.capture')).toBe(rested);
    } finally {
      page.dispose();
    }
  });

  it('runs Trigger and Auto from the keys, on the capture plots selected, and offers them on no other plot', async () => {
    const page = await mount([[K.fid], ['sine.sin']]);
    try {
      const [capture, time] = plotIds(page.ws.layout);
      await clickOn(leafOf(capture).querySelector('.plot-body'));
      await pressKey('t');
      await settle();
      expect(posted(page)).toEqual(['capture fid.capture']);
      await pressKey('r');
      await settle();
      expect([page.ws.plots[capture].auto, posted(page).at(-1)]).toEqual([true, `repeat fid.capture ${CAPTURE_DELAY}`]);
      await pressKey('r');
      await settle();
      expect([page.ws.plots[capture].auto, posted(page).at(-1)]).toEqual([false, 'stop fid.capture']);
      // A time plot has neither a button nor a row for them, and its Pause is no capture plot's.
      expect([button(time, 'captureNow'), button(time, 'captureAuto'), button(capture, 'pause')]).toEqual([null, null, null]);
      for (const id of ['captureNow', 'captureAuto', 'captureShots', 'captureDelay']) expect([id, page.plots.commands.row(id, { on: time }).offered, page.plots.commands.row(id, { on: capture }).offered]).toEqual([id, false, true]);
      // Space pauses the time plot, and leaves the capture plot as it is.
      page.ws.selection.only([capture, time]);
      await pressKey(' ');
      await settle();
      expect([page.views.of(time).live, page.views.of(capture).live]).toEqual([false, true]);
    } finally {
      page.dispose();
    }
  });

  it('says where its capture stands, plainly: capturing, reading with how far, busy, timed out, an error with its text, the device gone; and nothing at rest', async () => {
    const page = await mount([[K.fid]]);
    try {
      const [id] = plotIds(page.ws.layout), hub = page.hub.captures;
      expect(stateOf(id)).toBe('');
      // A capture under way says so once it has been a moment, not at once.
      hub.latency = 2000;
      await clickOn(button(id, 'captureNow'));
      await settle();
      expect(stateOf(id), 'under way for a few frames').toBe('');
      await wait(400);
      await settle();
      expect(stateOf(id)).toBe('capturing…');
      hub.reading('fid.capture', 100, 400);
      await settle();
      expect(stateOf(id)).toBe('reading 25%');
      await wait(2000);
      await settle();
      expect(stateOf(id), 'at rest, with its capture').toBe('');
      hub.latency = 100;
      for (const [failing, said] of [['busy', 'busy'], ['timeout', 'timed out'], [{ error: 'the device did not start the capture' }, 'error: the device did not start the capture']]) {
        hub.failing['fid.capture'] = failing;
        await trigger(page, id);
        expect(stateOf(id)).toBe(said);
        expect(leafOf(id).querySelector('.capture-state .failed'), `${said} stands out`).not.toBe(null);
      }
      await shoot('state-error', leafOf(id));
      // The state is on the plot, within it, and takes no press.
      const box = leafOf(id).querySelector('.capture-state').getBoundingClientRect(), P = page.pv(id).geo.plot;
      expect(box.left >= P.x && box.right <= P.x + P.w && box.bottom <= P.y + P.h && box.top >= P.y).toBe(true);
      expect(getComputedStyle(leafOf(id).querySelector('.capture-state')).pointerEvents).toBe('none');
      // The next that succeeds clears it.
      await trigger(page, id);
      expect(stateOf(id)).toBe('');
      // On Auto, of a device that captures at once, the plot says nothing, frame after frame: no word comes and goes with each capture.
      await clickOn(button(id, 'captureAuto'));
      await settle();
      const said = new Set(), taken = hub.number('fid.capture');
      for (let n = 0; n < 150; n++) (await frames(1), said.add(stateOf(id)));
      expect([[...said], hub.number('fid.capture') > taken + 2]).toEqual([[''], true]);
      // The device lost, with Auto on; and back.
      hub.lose();
      await settle();
      expect(stateOf(id)).toBe('device gone');
      hub.find();
      await settle();
      await wait(TAKEN);
      await settle();
      expect(stateOf(id)).toBe('');
    } finally {
      page.dispose();
    }
  });

  it('keeps Auto with its tab: a page loaded again asks at once; and asks again when a lost device is back', async () => {
    const first = await mount([[K.fid]]);
    let saved;
    try {
      const [id] = plotIds(first.ws.layout);
      first.plots.commands.run('captureDelay', { on: id, how: { to: 0.2 } });
      await clickOn(button(id, 'captureAuto'));
      await settle();
      saved = JSON.parse(localStorage.getItem(SAVE_KEY));
      expect(Object.values(saved.tabs[0].plots)[0]).toMatchObject({ kind: 'capture', auto: true, autoDelay: 0.2 });
    } finally {
      first.dispose();
    }
    const page = await mount([], { saved: { [SAVE_KEY]: saved }, fromSaved: true });
    try {
      const [id] = plotIds(page.ws.layout), hub = page.hub.captures;
      await settle();
      expect(posted(page)).toEqual(['repeat fid.capture 0.2']);
      expect(button(id, 'captureAuto').getAttribute('aria-pressed')).toBe('true');
      await wait(1000);
      await settle();
      expect(slice(page, id).number).toBeGreaterThan(1);
      // The device goes: the hub drops the ask, and the page asks nothing of a device that is gone.
      hub.lose();
      await wait(1000);
      await settle();
      expect([posted(page), hub.asks('fid.capture').repeat]).toEqual([['repeat fid.capture 0.2'], null]);
      const had = hub.number('fid.capture');
      // Back: the page asks again, unprompted, and captures come.
      hub.find();
      await settle();
      await wait(1000);
      await settle();
      expect(posted(page)).toEqual(['repeat fid.capture 0.2', 'repeat fid.capture 0.2']);
      expect(hub.number('fid.capture')).toBeGreaterThan(had);
      expect(slice(page, id).number).toBe(hub.number('fid.capture'));
    } finally {
      page.dispose();
    }
  });

  it('overlays its last shots, the older the fainter, or averages them, and says how many: an average of shots alike, begun again by one that differs', async () => {
    // Shots 1 to 6 of 1500 values, then of 1000.
    const changing = { ...FID, shot: (number) => fid(number > 6 ? 1000 : 1500)(number) };
    const page = await mountPage([[K.fid]], 'rows', 0, SIGNALS, [], { captures: [changing, PHASE] });
    try {
      const [id] = plotIds(page.ws.layout), cmds = page.plots.commands, note = () => leafOf(id).querySelector('.capture-state .note')?.textContent ?? '';
      const set = async (shots, count) => (cmds.run('captureShots', { on: id, how: { to: shots } }), cmds.run('captureCount', { on: id, how: { to: count } }), await settle(), await wait(100), await settle());
      expect(cmds.row('captureCount', { on: id }).choices.map(([n]) => n)).toEqual(SHOT_COUNTS);
      for (let n = 0; n < 3; n++) await trigger(page, id);
      expect([slice(page, id).under, note()]).toEqual([[], '']);
      // Overlay of 4: three shots so far, then four, and no more than four.
      await set('overlay', 4);
      expect([slice(page, id).under.map((u) => u.shot.number), note()]).toEqual([[1, 2], 'last 3 of 4']);
      for (let n = 0; n < 2; n++) await trigger(page, id);
      const s = slice(page, id);
      expect([s.number, s.under.map((u) => u.shot.number), note()]).toEqual([5, [2, 3, 4], 'last 4 of 4']);
      expect(s.under.map((u) => u.alpha)).toEqual([...s.under.map((u) => u.alpha)].sort((x, y) => x - y));
      expect(s.under[0].alpha).toBeLessThan(s.under.at(-1).alpha);
      // Each shot is drawn: a line a shot, the newest last.
      const calls = page.plots.renderer.stats.drawCalls;
      expect(calls).toBeGreaterThanOrEqual(4);
      await shoot('overlay', leafOf(id));
      // Average of 4: the mean of shots 2 to 5, value by value.
      await set('average', 4);
      const shots = page.hub.session.byKey.get(K.fid).shots, mean = (of) => Float32Array.from(of[0].y, (_, i) => of.reduce((sum, x) => sum + x.y[i], 0) / of.length);
      expect(note()).toBe('avg 4 of 4');
      expect([...slice(page, id).a]).toEqual([...mean(shots.slice(-4))]);
      expect(slice(page, id).under).toEqual([]);
      await shoot('average', leafOf(id));
      // A shot of another length starts the mean again: of one, then of two.
      await trigger(page, id);
      await trigger(page, id);
      expect([shots.at(-1).n, note()]).toEqual([1000, 'avg 1 of 4']);
      expect(slice(page, id).a).toBe(shots.at(-1).y);
      await trigger(page, id);
      expect(note()).toBe('avg 2 of 4');
      expect([...slice(page, id).a]).toEqual([...mean(shots.slice(-2))]);
      // The settings are the plot's, kept with its tab, each an edit to undo.
      expect(page.ws.plots[id]).toMatchObject({ shots: 'average', shotCount: 4 });
      expect(page.history.undos.length).toBeGreaterThanOrEqual(3);
    } finally {
      page.dispose();
    }
  });

  it('takes cursors, peaks, its axes’ settings, a copy of its data and the export dialog as a spectrum does', async () => {
    const written = [];
    const page = await mount([[K.fid]]);
    try {
      vi.spyOn(navigator.clipboard, 'write').mockImplementation(async (items) => void written.push(items));
      const [id] = plotIds(page.ws.layout), cmds = page.plots.commands, pv = page.pv(id);
      await trigger(page, id);
      // X cursors, in the capture's x, a pair in view; the readout says so in µs.
      cmds.run('addXCursors', { on: id });
      await settle();
      await wait(100);
      const { a, b } = page.ws.plots[id].cursors.c;
      expect(a > pv.xd[0] && b > a && b < pv.xd[1]).toBe(true);
      expect(page.views.of(id).live, 'a capture plot is not paused by a cursor').toBe(true);
      const readout = () => [...leafOf(id).querySelectorAll('.cursor-readout tr')].map((tr) => tr.textContent.replace(/\s+/g, ' ').trim());
      expect(await until(() => readout().length > 0)).toBe(true);
      expect(readout().join(' | ')).toMatch(/µs/);
      // X1 to the peak nearest it: a point of the capture higher than those about it.
      expect(cmds.row('x1ToPeak', { on: id }).disabled).toBe(false);
      cmds.run('x1ToPeak', { on: id });
      await settle();
      const s = slice(page, id), at = page.ws.plots[id].cursors.c.a, i = s.f.indexOf(at);
      expect(i, 'on a point of the capture').toBeGreaterThan(0);
      expect(Math.abs(s.a[i]) >= Math.abs(s.a[i - 1]) && Math.abs(s.a[i]) >= Math.abs(s.a[i + 1])).toBe(true);
      // The x axis's popover: the capture's x by name and unit, its range, digits and notation, and no scale.
      const P = pv.geo.plot;
      await rightClick([P.x + P.w / 2, P.y + P.h + 14]);
      const pop = () => document.querySelector('[data-slot=popover-content]');
      expect(await until(() => pop())).toBe(true);
      expect(pop().querySelector('.axis-head').textContent.replace(/\s+/g, ' ').trim()).toBe('Times');
      const labels = [...pop().querySelectorAll('[role=group]')].map((g) => g.getAttribute('aria-label'));
      expect(labels).toEqual(expect.arrayContaining(['X range', 'X cursors', 'Peaks', 'Digits', 'Notation']));
      expect(labels).not.toContain('Frequency scale');
      await shoot('x-popover', pop());
      await typeInto(pop().querySelector('input[aria-label="Lowest x shown"]'), '0.0001');
      await typeInto(pop().querySelector('input[aria-label="Highest x shown"]'), '0.0003');
      await pressKey('Enter');
      await settle();
      expect([page.ws.plots[id].fRange, page.pv(id).xd]).toEqual([[0.0001, 0.0003], [0.0001, 0.0003]]);
      await closeAll();
      // Notation Exp writes its ticks so.
      cmds.run('xNotation', { on: id, how: { to: 'exp' } });
      await settle();
      await wait(100);
      for (const text of page.pv(id).tickLabels.filter((t) => t.axis === undefined).map((t) => t.text)) expect(text).toMatch(/^\d(\.\d+)?e-\d+ s$/);
      cmds.run('xNotation', { on: id, how: { to: 'auto' } });
      cmds.run('fRange', { on: id, how: { range: null } });
      await settle();
      await wait(100);
      expect(page.pv(id).xd).toEqual([0, Math.fround(1499 * 5e-7)]);
      // The y axis's popover: a range, its unit among those of volts, and no scale.
      await rightClick([P.x - 12, P.y + P.h / 2]);
      expect(await until(() => pop())).toBe(true);
      const yLabels = [...pop().querySelectorAll('[role=group]')].map((g) => g.getAttribute('aria-label'));
      expect(yLabels).toEqual(expect.arrayContaining(['Y axis range', 'Y cursors', 'Unit']));
      expect(yLabels).not.toContain('Density scale');
      await closeAll();
      // The unit fixed at mV: the axis's numbers and the chip's unit follow.
      cmds.run('yUnit', { on: id, how: { key: 'shared', to: -3 } });
      await settle();
      await wait(100);
      expect(leafOf(id).querySelector('.trace-chip .units').textContent).toBe('mV');
      // Copy data: the capture as the plot shows it, its x in the capture's own name and units.
      cmds.run('copyData', { on: id });
      expect(await until(() => written.length === 1)).toBe(true);
      const text = await (await written[0][0].getType('text/plain')).text(), lines = text.split('\n');
      expect(lines.some((l) => l.includes('Capture of FID'))).toBe(true);
      expect(lines.find((l) => l.startsWith('Time (s)'))).toBe('Time (s)\tFID (V)');
      expect(lines.filter((l) => /^[-\d.e]+\t[-\d.e]+$/.test(l)).length).toBe(1500);
      // The export dialog opens on it: its picture at the side, named as the plot names it, and a copy that draws the capture as the plot shows it.
      cmds.run('exportData', { on: id });
      const dialog = () => document.querySelector('[role=dialog]');
      expect(await until(dialog)).toBe(true);
      expect(await until(() => dialog().querySelector('.thumb img'))).toBe(true);
      expect(dialog().querySelector('.thumb small').textContent).toBe('FID');
      const copied = () => [...page.plots.registry].length && document.querySelector('[role=dialog] .fig canvas');
      expect(await until(copied)).toBe(true);
      await wait(500);
      await settle(6);
      // A capture is in no data file, which the dialog says, and its image is one.
      expect(dialog().querySelector('.will tbody').textContent).toMatch(/No data file of a capture/);
      expect(dialog().querySelector('button.go').disabled).toBe(true);
      await shoot('export', dialog());
      await clickOn([...dialog().querySelectorAll('button')].find((b) => b.textContent.trim() === 'Image'));
      await wait(500);
      await settle(6);
      expect(dialog().querySelector('.will tbody').textContent).toMatch(/1 signal/);
      expect(dialog().querySelector('button.go').disabled).toBe(false);
      await shoot('export-image', dialog());
    } finally {
      page.dispose();
    }
  });
});
