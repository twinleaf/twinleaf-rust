// A plot away from the workspace: a paused copy of one of the page's plots
// (PlotHost `copy`), shown on a stage of its own (PlotStage.svelte) at a
// size and a pixel ratio asked for.
// - The copy's picture (PlotHost `picture`) is the plot's own at the same
//   size and stretch of time, pixel for pixel within SAME.
// - What is done to the copy, from its menus or by its commands, changes
//   the copy alone: the page's plot, its undo steps, its view steps, its
//   colours and the tabs it keeps stay as they were, and the page's plot
//   goes on following live.
// - A copy does not follow live, and its menu has no row of a command that
//   needs a workspace or a live plot (commands.js HOSTED). A key pressed in
//   it is not the page's: it runs no command on the page's plots.
// - A picture is the stage's size times its pixel ratio, at 1 and at 2,
//   and holds the traces whenever it is asked for.
// - A paused copy reads its own windows of the hub's history, as finely as
//   its stage shows them, and has the hub hold its history while it lasts.
// - Taking the stage down and ending the copy leaves nothing of them: no
//   picker, no PlotView, no GL context, no window read, no hold on the
//   hub, no listener on its view, no frame drawn.
// - Shown at a scale, as a preview, a stage gives the picture its size and
//   pixel ratio ask for, the same as at its own size, and shows that
//   picture's layout: the same ticks and labels, finer or coarser.
// - A picture holds what the page lays over the plot, its legend, where it
//   shows, and never the hover's line, tip or values; with a figure, the
//   figure's bands about the plot on its background, in the look its
//   colours are drawn in. A size past the browser's largest canvas is
//   refused, with why. A copy drawn small is never a monitor.
import { afterEach, describe, expect, it, vi } from 'vitest';
import { commands, page as browser } from 'vitest/browser';
import { flushSync, mount, unmount } from 'svelte';
import Stage from './harness/Stage.svelte';
import { centre, closeAll, feed, focusPage, frames, holdHistory, leafOf, mountPage, pickItem, pressKey, quietApi, settle, until, wait } from './harness/page.js';
import { plotIds } from '../src/lib/layout.js';
import { COMMANDS, HOSTED } from '../src/lib/commands.js';
import { SAVE_KEY } from '../src/lib/tabs.js';
import { monitorOf, theme } from '../src/lib/plot.js';
import { bandsOf } from '../src/lib/figure.js';
import { prefs } from '../src/lib/prefs.svelte.js';
import { colors } from '../src/lib/colors.svelte.js';

/** Where VITE_TL_SHOTS names a folder, relative to this file, the screenshots go there. */
const SHOTS = import.meta.env.VITE_TL_SHOTS;
const shot = async (name) => SHOTS && (await browser.screenshot({ path: `${SHOTS}/${name}.png` }));

const SIGNALS = ['sine.sine', 'aux.triangle'];
/** How far apart, of 255, a channel of a pixel and of a colour may be and the pixel still be of that colour. */
const CHANNEL_TOLERANCE = 24;
/** How far apart, of 255, a channel of a pixel of a plot's picture and of its copy's may be: no pixel of the two differs by more. */
const SAME = 2;

let page, staged = [];
afterEach(() => {
  for (const s of staged.splice(0)) s.end();
  vi.restoreAllMocks();
  page?.dispose();
  page = null;
});

/** Shows plot `host.id` of copy `host` on a stage `width` by `height` px at `pixelRatio`, at (`left`, `top`) of the page, with the `scale`, `screenRatio` and `figure` of `more` (PlotStage.svelte); `resize(to)` gives it another size, pixel ratio, scale or figure, and `end()` takes it down. */
async function stage(host, { width, height, pixelRatio = 1, left = 8, top = 8, ...more }) {
  const target = document.body.appendChild(document.createElement('div'));
  const app = mount(Stage, { target, props: { host, id: host.id, width, height, pixelRatio, left, top, ...more } });
  const s = { el: null, ended: false, resize: (to) => app.resize(to), end: () => !s.ended && ((s.ended = true), unmount(app), target.remove()) };
  staged.push(s);
  await settle(6);
  s.el = target.querySelector('.plot-stage');
  return s;
}

/** The pixels of canvas `c`, as {width, height, data}. */
const pixels = (c) => c.getContext('2d').getImageData(0, 0, c.width, c.height);
/** How many pixels of `a` and `b`, pictures of one size, differ by more than SAME in a channel. */
function differing(a, b) {
  const pa = pixels(a).data, pb = pixels(b).data;
  let n = 0;
  for (let i = 0; i < pa.length; i += 4) if (Math.abs(pa[i] - pb[i]) > SAME || Math.abs(pa[i + 1] - pb[i + 1]) > SAME || Math.abs(pa[i + 2] - pb[i + 2]) > SAME || Math.abs(pa[i + 3] - pb[i + 3]) > SAME) n++;
  return n;
}
/** How many pixels of picture `c` are within CHANNEL_TOLERANCE of `rgba`, a colour as GL has it, 0 to 1 a channel. */
function coloured(c, rgba) {
  const p = pixels(c).data, [r, g, b] = rgba.map((v) => Math.round(v * 255));
  let n = 0;
  for (let i = 0; i < p.length; i += 4) if (Math.abs(p[i] - r) <= CHANNEL_TOLERANCE && Math.abs(p[i + 1] - g) <= CHANNEL_TOLERANCE && Math.abs(p[i + 2] - b) <= CHANNEL_TOLERANCE) n++;
  return n;
}
/** The colour signal `key` is drawn in among `from`, a Colors, as GL has it. */
const rgbaOf = (from, key) => theme().tracesRgba[from.of(key)];
/** Pictures `a` and `b` side by side on one canvas, shown at the foot of the page for a screenshot; the function returned takes it away. */
function sideBySide(a, b) {
  const c = document.createElement('canvas');
  c.width = a.width + b.width + 12;
  c.height = Math.max(a.height, b.height);
  const ctx = c.getContext('2d');
  ctx.fillStyle = '#f0f';
  ctx.fillRect(a.width, 0, 12, c.height);
  ctx.drawImage(a, 0, 0);
  ctx.drawImage(b, a.width + 12, 0);
  c.style.cssText = 'position: fixed; left: 0; top: 0; z-index: 100';
  document.body.appendChild(c);
  return () => c.remove();
}
/** What the page keeps and holds of its plots, to compare before and after: the tabs as kept, the undo steps, whether Back can go, the colours as kept, and plot `id`'s settings. */
const pageState = (id) => ({
  tabs: localStorage.getItem(SAVE_KEY),
  undos: page.history.undos.length,
  redos: page.history.redos.length,
  back: page.plots.viewHistory.canBack,
  forward: page.plots.viewHistory.canForward,
  colors: JSON.stringify([colors.given, colors.picked, colors.last]),
  kept: localStorage.getItem('twinleaf-colors'),
  plot: JSON.stringify(page.ws.plots[id]),
  selected: [...page.ws.selected].join(),
});
/** The body of plot `id` of the page, its box in client px. */
const bodyOf = (id) => leafOf(id).querySelector('.plot-body').getBoundingClientRect();
/** The rows of the menus open, by command. */
const rows = () => [...document.querySelectorAll('[role^=menuitem][data-command]')].map((m) => m.dataset.command);
/** Right-clicks the plot area of the stage's plot, which opens its menu, and waits for it. */
async function stageMenu(host) {
  const P = host.pv(host.id).geo.plot;
  await commands.mouse([['move', P.x + P.w * 0.6, P.y + P.h * 0.6], ['down', 'right'], ['up', 'right']]);
  expect(await until(() => document.querySelector('[data-slot=context-menu-content][data-state=open]')), 'the copy’s menu open').toBe(true);
}

describe('a paused copy of a plot, on a stage of its own', () => {
  it('draws the plot’s own picture, takes its commands alone while the plot goes on live, and leaves nothing behind', async () => {
    page = await mountPage([SIGNALS], 'grid', 0, SIGNALS);
    quietApi();
    feed(page.hub, 30);
    await settle();
    const [id] = plotIds(page.ws.layout), plots = page.plots;
    // The plot paused, so it and its copy show one stretch of time; the pointer off the plots, so neither draws a hover.
    // The stage is the size of the plot's body, at the same fraction of a pixel on the page, as both draw on the page's pixel grid.
    plots.commands.run('pause', { on: id });
    await focusPage();
    await settle(6);
    const box = bodyOf(id), ratio = window.devicePixelRatio || 1;
    const copy = plots.copy(id);
    const s = await stage(copy, { width: box.width, height: box.height, pixelRatio: ratio, left: 8 + (box.left % 1), top: 40 + (box.top % 1) });
    const cv = copy.view(copy.id), stretch = cv.range(page.hub.now(0));
    expect(cv.live, 'the copy paused').toBe(false);
    expect(stretch, 'the copy’s stretch of time').toEqual(plots.view(id).range(page.hub.now(0)));
    expect(s.el.querySelector('.plot-body').getBoundingClientRect().width, 'the copy’s body the stage’s width').toBe(box.width);
    expect(s.el.querySelector('.plot-body').getBoundingClientRect().height, 'the copy’s body the stage’s height').toBe(box.height);

    // Its picture is the plot's.
    const original = plots.picture(id), first = copy.picture(copy.id);
    expect([first.width, first.height], 'the pictures’ sizes').toEqual([original.width, original.height]);
    expect([first.width, first.height]).toEqual([Math.round(box.width * ratio), Math.round(box.height * ratio)]);
    for (const key of SIGNALS) expect(coloured(first, rgbaOf(colors, key)), `${key} drawn in the copy’s picture`).toBeGreaterThan(200);
    const remove = sideBySide(original, first);
    await frames(2);
    await shot('copy-original-and-copy');
    remove();
    expect(differing(original, first), `pixels of ${first.width * first.height} that differ between the plot’s picture and its copy’s`).toBe(0);

    // The plot follows live again and goes on, the copy holding its stretch.
    plots.commands.run('follow', { on: id });
    await settle();
    const live = pageState(id), x1 = page.pv(id).drawn.x1, drawn = [copy.pv(copy.id).drawn.x0, copy.pv(copy.id).drawn.x1];
    await wait(500);
    await settle();
    expect(plots.view(id).live, 'the plot follows live').toBe(true);
    expect(page.pv(id).drawn.x1, 'the plot’s newest time moves on').toBeGreaterThan(x1 + 0.3);
    expect(cv.range(page.hub.now(0)), 'the copy’s stretch stays').toEqual(stretch);
    expect([copy.pv(copy.id).drawn.x0, copy.pv(copy.id).drawn.x1], 'the copy draws the stretch it drew').toEqual(drawn);
    expect(copy.commands.run('follow', { on: copy.id }), 'Follow live on the copy is not taken').toBe(false);
    expect(copy.commands.run('pause', { on: copy.id }), 'Pause on the copy is not taken').toBe(false);
    expect(cv.live).toBe(false);

    // Its menu: no row of a command that needs what the copy's host has not, and no group for where a plot is on the page.
    await stageMenu(copy);
    const shown = rows(), unhosted = COMMANDS.filter((c) => c.needs?.some((n) => HOSTED.includes(n))).map((c) => c.id);
    expect(shown.length, 'the copy’s menu has command rows').toBeGreaterThan(2);
    expect(shown.filter((c) => unhosted.includes(c)), 'rows of commands the copy’s host cannot run').toEqual([]);
    const labels = [...document.querySelectorAll('[data-slot=context-menu-label]')].map((l) => l.textContent.trim());
    expect(labels, 'the menu’s groups').toEqual(['Cursors', 'Show']);
    await shot('copy-menu');
    // Layout, from the copy's menu, changes the copy.
    await pickItem('Layout', 'Stacked');
    await closeAll();
    await settle();
    expect(copy.plot(copy.id).mode, 'the copy stacked').toBe('stacked');
    expect(copy.history.undos.at(-1)?.label, 'a step of the copy’s own undo').toBe('Layout Stacked');
    expect(copy.pv(copy.id).geo.lanes.length, 'the copy draws a lane a signal').toBe(SIGNALS.length);
    expect(page.pv(id).geo.lanes.length, 'the plot draws one lane').toBe(1);

    // The legend, by its command, and a signal's colour, from the legend's swatch.
    copy.commands.run('legend', { on: copy.id });
    await settle();
    const swatch = s.el.querySelector(`.pinned-legend .row[data-key="${SIGNALS[0]}"] .swatch`);
    expect(swatch, 'the copy’s legend shows its signals').toBeTruthy();
    await commands.mouse([['move', ...centre(swatch)], ['down'], ['up']]);
    expect(await until(() => document.querySelector('.swatch-choice')), 'the colour picker open').toBe(true);
    const was = colors.of(SIGNALS[0]), to = [1, 2, 3].find((c) => c !== was && c !== colors.of(SIGNALS[1]));
    await commands.mouse([['move', ...centre(document.querySelectorAll('.swatch-choice')[to])], ['down'], ['up']]);
    await closeAll();
    await focusPage();
    await settle(6);
    expect(copy.colors.of(SIGNALS[0]), 'the colour picked on the copy').toBe(to);
    expect(colors.of(SIGNALS[0]), 'the page’s colour of the signal').toBe(was);
    // Cursors, and a y axis's digits, by their commands.
    copy.commands.run('addXCursors', { on: copy.id });
    const axis = copy.pv(copy.id).yAxes[0].key;
    copy.commands.run('yDigits', { on: copy.id, how: { key: axis, to: 4 } });
    await settle(6);
    expect(copy.plot(copy.id).cursors?.x, 'X cursors on the copy').toBeTruthy();
    expect(copy.plot(copy.id).yFormat, 'the copy’s y axis settings').toEqual({ [axis]: { digits: 4 } });

    // A key pressed in the copy is not the page's: L, with the page's plot selected, leaves its legend as it was.
    page.ws.selection.only([id]);
    flushSync();
    const card = s.el.querySelector('.plot-card');
    card.focus();
    await pressKey('l');
    await settle();
    expect(document.activeElement, 'focus in the copy').toBe(card);
    expect(page.ws.plots[id].legend, 'the page’s plot’s legend after L pressed in the copy').toBe(false);
    page.ws.selection.clear();
    await focusPage();
    await settle(6);

    // The pictures show it: the copy in the new colour, the plot in its own.
    const second = copy.picture(copy.id), plotNow = plots.picture(id);
    const removeSecond = sideBySide(plotNow, second);
    await frames(2);
    await shot('copy-after-commands');
    removeSecond();
    expect(coloured(second, theme().tracesRgba[to]), 'the copy’s picture has the new colour').toBeGreaterThan(200);
    expect(coloured(second, theme().tracesRgba[was]), 'the copy’s picture has none of the old colour').toBeLessThan(20);
    expect(coloured(plotNow, theme().tracesRgba[to]), 'the plot’s picture has none of the new colour').toBeLessThan(20);
    expect(coloured(plotNow, theme().tracesRgba[was]), 'the plot’s picture keeps its colour').toBeGreaterThan(200);
    // The page is as it was since its plot went live: its plot, its undo steps, its view steps, its colours and the tabs it keeps.
    expect(pageState(id), 'the page after the copy’s commands').toEqual(live);
    expect(plots.view(id).live, 'the plot still follows live').toBe(true);

    // Taken down and ended, nothing of the copy is left.
    const gl = copy.renderer.gl, draws = vi.spyOn(copy, 'draw');
    expect(copy.pickers.size, 'its picker while shown').toBe(1);
    expect(copy.registry.size, 'its PlotView while shown').toBe(1);
    expect(page.hub.holders.has(copy), 'its hold on the hub’s history while it lasts').toBe(true);
    s.end();
    copy.dispose();
    flushSync();
    await frames(4);
    expect(copy.pickers.size, 'pickers left').toBe(0);
    expect(copy.registry.size, 'PlotViews left').toBe(0);
    expect(copy.renderer, 'its surface').toBe(null);
    expect(gl.isContextLost(), 'its GL context given back').toBe(true);
    expect(draws, 'frames drawn after').not.toHaveBeenCalled();
    expect(page.hub.holders.has(copy), 'its hold on the hub’s history').toBe(false);
    expect(page.hub.session.reads.has(copy.id) || page.hub.session.windows.has(copy.id), 'window reads left').toBe(false);
    expect(cv.listeners.size, 'listeners on its view but its Views’ own').toBe(1);
    expect(document.querySelector('.plot-stage, .test-stage canvas'), 'its elements').toBe(null);
    expect(plots.pickers.size, 'the page’s pickers, one a plot').toBe(plotIds(page.ws.layout).length);
    expect(pageState(id), 'the page after the copy has gone').toEqual(live);
  });

  it('gives its picture at the size and pixel ratio asked for, whenever it is asked', async () => {
    page = await mountPage([SIGNALS], 'grid', 0, SIGNALS);
    quietApi();
    feed(page.hub, 30);
    await settle();
    const [id] = plotIds(page.ws.layout), size = { width: 420, height: 300 }, pictures = {};
    // The plot paused, so each copy of it shows one stretch of time.
    page.plots.commands.run('pause', { on: id });
    for (const pixelRatio of [1, 2]) {
      const copy = page.plots.copy(id);
      const s = await stage(copy, { ...size, pixelRatio });
      // Asked for between frames, long after the last one was drawn.
      await wait(300);
      const picture = (pictures[pixelRatio] = copy.picture(copy.id));
      expect(picture instanceof HTMLCanvasElement, 'a canvas').toBe(true);
      expect([picture.width, picture.height], `the picture at ${pixelRatio}`).toEqual([size.width * pixelRatio, size.height * pixelRatio]);
      expect(copy.renderer.canvas.width, `the stage’s canvas at ${pixelRatio}`).toBe(size.width * pixelRatio);
      for (const key of SIGNALS) expect(coloured(picture, rgbaOf(colors, key)), `${key} in the picture at ${pixelRatio}`).toBeGreaterThan(150 * pixelRatio);
      // The y axis's numbers, which the overlay draws left of the plot area, where no trace is: pixels there that are not the background.
      const pv = copy.pv(copy.id), gutter = Math.floor((pv.geo.plot.x - pv.drawnOn.x) * pixelRatio) - 1;
      const p = picture.getContext('2d').getImageData(0, 0, gutter, picture.height).data, bg = theme().bgRgba.map((v) => Math.round(v * 255));
      let text = 0;
      for (let i = 0; i < p.length; i += 4) if (Math.abs(p[i] - bg[0]) > CHANNEL_TOLERANCE || Math.abs(p[i + 1] - bg[1]) > CHANNEL_TOLERANCE || Math.abs(p[i + 2] - bg[2]) > CHANNEL_TOLERANCE) text++;
      expect(text, `pixels of the y axis’s numbers at ${pixelRatio}`).toBeGreaterThan(40 * pixelRatio);
      s.end();
      copy.dispose();
    }
    // The page's own surface is the window's still.
    expect(page.plots.renderer.dpr, 'the page’s pixel ratio').toBe(window.devicePixelRatio || 1);
    expect(page.plots.renderer.canvas.width, 'the page’s canvas').toBe(Math.round(window.innerWidth * (window.devicePixelRatio || 1)));
    // At 2 it is the picture at 1, twice as fine: halved, the two are alike.
    const half = document.createElement('canvas');
    half.width = size.width;
    half.height = size.height;
    half.getContext('2d').drawImage(pictures[2], 0, 0, size.width, size.height);
    const a = pixels(pictures[1]).data, b = pixels(half).data;
    let sum = 0;
    for (let i = 0; i < a.length; i++) sum += Math.abs(a[i] - b[i]);
    const remove = sideBySide(pictures[1], pictures[2]);
    await frames(2);
    await shot('copy-ratio-1-and-2');
    remove();
    expect(sum / a.length, 'the mean difference of a channel, of 255, between the picture at 1 and the picture at 2 halved').toBeLessThan(3);
  });

  it('reads its own windows of the hub’s history, as finely as its stage shows them, and holds the history while it lasts', async () => {
    page = await mountPage([[SIGNALS[0]]], 'grid', 0, SIGNALS);
    const held = holdHistory(page, { history: 600 });
    await settle();
    const [id] = plotIds(page.ws.layout), plots = page.plots, session = page.hub.session, trace = session.byKey.get(SIGNALS[0]);
    // Paused five minutes back, where only the hub's history has every sample.
    const [t0, t1] = [held.now - 300, held.now - 290];
    plots.viewHistory.step(() => plots.view(id).show(t0, t1, 0));
    await settle(6);
    expect(session.windowOf(id, 'detail', trace), 'the plot’s window read').toBeTruthy();
    const copy = plots.copy(id), size = { width: 500, height: 320 };
    const asked = held.asked.length;
    const s = await stage(copy, { ...size, pixelRatio: 3 });
    expect(await until(() => session.windowOf(copy.id, 'detail', trace)), 'the copy’s own window read').toBe(true);
    await settle(6);
    expect(copy.pv(copy.id).drawn.plans[0].fromWindow, 'the copy draws from its window').toBe(true);
    /** The points the newest read of a window that holds the copy's stretch asked for. */
    const points = () => held.asked.slice(asked).filter((r) => r.t0 <= t0 && r.t1 >= t1).at(-1).width;
    expect(points(), 'the points its window read asks for: one a device pixel').toBe(size.width * 3);
    // At another pixel ratio, and at another width, the stage's picture is that size, and the window is read again for it.
    s.resize({ pixelRatio: 1 });
    await settle(6);
    expect(points(), 'at a pixel ratio of 1: two a CSS px').toBe(size.width * 2);
    expect(copy.picture(copy.id).width, 'the picture at a pixel ratio of 1').toBe(size.width);
    s.resize({ width: 640, pixelRatio: 4 });
    await settle(6);
    expect(points(), 'at 640 px and a pixel ratio of 4').toBe(640 * 4);
    expect([copy.picture(copy.id).width, copy.picture(copy.id).height], 'the picture at 640 px and a pixel ratio of 4').toEqual([640 * 4, size.height * 4]);
    expect(copy.pv(copy.id).drawn.plans[0].fromWindow, 'the copy still draws from its window').toBe(true);
    // The plot goes live: the hub's history is held for the copy still.
    plots.commands.run('follow', { on: id });
    await settle();
    expect(page.hub.holding, 'the history held while the copy lasts').toBe(true);
    expect(session.windowOf(id, 'detail', trace), 'the live plot’s window').toBeUndefined();
    expect(session.windowOf(copy.id, 'detail', trace), 'the copy’s window kept').toBeTruthy();
    s.end();
    copy.dispose();
    await settle();
    expect(page.hub.holding, 'the history let go once the copy has gone').toBe(false);
    expect(session.reads.has(copy.id) || session.windows.has(copy.id), 'the copy’s windows and reads').toBe(false);
  });
});

describe('a picture of a paused copy', () => {
  /** The page with one plot of SIGNALS, fed and paused, and a copy of it. */
  async function copied() {
    page = await mountPage([SIGNALS], 'grid', 0, SIGNALS);
    quietApi();
    feed(page.hub, 30);
    await settle();
    const [id] = plotIds(page.ws.layout);
    page.plots.commands.run('pause', { on: id });
    await focusPage();
    await settle(6);
    return { id, copy: page.plots.copy(id) };
  }
  /** The tick labels plot `pv` drew last, each its text and place across and down its body, in CSS px. */
  const ticks = (pv) => pv.tickLabels.map((t) => `${t.text}@${Math.round(t.x - pv.drawnOn.x)},${Math.round(t.y - pv.drawnOn.y)}`);
  afterEach(() => copies.splice(0).forEach((c) => c.dispose()));
  const copies = [];

  it('is the size and the layout asked for, however small the stage shows it', async () => {
    const { copy } = await copied(), size = { width: 640, height: 360, pixelRatio: 2 };
    copies.push(copy);
    const whole = await stage(copy, size);
    const full = copy.picture(copy.id), fullTicks = ticks(copy.pv(copy.id));
    whole.end();
    await settle(4);
    // Shown at a quarter, on a screen of one device pixel a CSS px.
    const shown = await stage(copy, { ...size, scale: 1 / 4, screenRatio: 1 });
    const el = shown.el.querySelector('.stage-shown'), r = el.getBoundingClientRect();
    expect([r.width, r.height], 'the stage shown at a quarter').toEqual([160, 90]);
    expect([el.width, el.height], 'its canvas at a quarter').toEqual([160, 90]);
    expect(ticks(copy.pv(copy.id)), 'the ticks and labels of the stage shown at a quarter').toEqual(fullTicks);
    expect(shown.el.querySelector('.plot-body').getBoundingClientRect().width, 'its plot laid out at its own size').toBe(640);
    const picture = copy.picture(copy.id);
    expect([picture.width, picture.height], 'the picture at its size and ratio').toEqual([1280, 720]);
    expect(differing(picture, full), 'pixels that differ from the stage’s at its own size').toBe(0);
    // The canvas shown is the picture at a quarter.
    const remove = sideBySide(el, picture);
    await frames(2);
    await shot('copy-shown-at-a-quarter');
    remove();
    const a = pixels(el), small = document.createElement('canvas');
    [small.width, small.height] = [el.width, el.height];
    small.getContext('2d').drawImage(picture, 0, 0, el.width, el.height);
    const b = pixels(small);
    let sum = 0;
    for (let i = 0; i < a.data.length; i++) sum += Math.abs(a.data[i] - b.data[i]);
    expect(sum / a.data.length, 'the mean difference of a channel, of 255, between the canvas shown and the picture scaled to it').toBeLessThan(6);
  });

  it('holds the legend where it shows, and never the hover’s line, tip or values', async () => {
    const { id, copy } = await copied();
    copies.push(copy);
    // On the page: the pointer over the plot draws its hover; its picture is the one with the pointer away.
    const still = page.plots.picture(id), body = leafOf(id).querySelector('.plot-body').getBoundingClientRect();
    await commands.mouse([['move', body.left + body.width * 0.5, body.top + body.height * 0.4]]);
    await settle(6);
    expect(page.pv(id).hover, 'the pointer over the plot').toBeTruthy();
    expect(differing(page.plots.picture(id), still), 'pixels of the picture the hover changes').toBe(0);
    expect(page.pv(id).hover, 'the hover kept for the next frame').toBeTruthy();
    await focusPage();
    // The copy's legend, by its command: in the picture, where the stage shows it.
    const s = await stage(copy, { width: 500, height: 300, pixelRatio: 1 });
    const bare = copy.picture(copy.id);
    copy.commands.run('legend', { on: copy.id });
    await settle(6);
    const legend = s.el.querySelector('.pinned-legend').getBoundingClientRect(), at = copy.pv(copy.id).drawnOn;
    expect(s.el.querySelector('.pinned-legend .val'), 'a bare legend has no values').toBe(null);
    const withLegend = copy.picture(copy.id), pa = pixels(withLegend).data, pb = pixels(bare).data;
    let inside = 0, outside = 0;
    for (let y = 0; y < withLegend.height; y++)
      for (let x = 0; x < withLegend.width; x++) {
        const i = (y * withLegend.width + x) * 4, changed = Math.abs(pa[i] - pb[i]) + Math.abs(pa[i + 1] - pb[i + 1]) + Math.abs(pa[i + 2] - pb[i + 2]) > 3 * SAME;
        const over = x >= legend.left - at.x - 1 && x <= legend.right - at.x + 1 && y >= legend.top - at.y - 1 && y <= legend.bottom - at.y + 1;
        if (changed) over ? inside++ : outside++;
      }
    expect(inside, 'pixels the legend changes within its box').toBeGreaterThan((legend.width * legend.height) / 4);
    expect(outside, 'pixels it changes outside its box').toBe(0);
  });

  it('with a figure, is the figure’s bands about the plot, in the look its colours are drawn in', async () => {
    const { copy } = await copied();
    copies.push(copy);
    const figure = { title: 'A title', xLabel: 'Time', caption: { left: 'a device', right: 'a time' } };
    const s = await stage(copy, { width: 480, height: 320, pixelRatio: 2, figure });
    const plain = s.el.querySelector('.plot-body').getBoundingClientRect(), bands = bandsOf(figure, 480);
    expect(plain.height, 'the plot takes the room the bands leave').toBeCloseTo(320 - bands.top - bands.bottom, 6);
    const picture = copy.picture(copy.id, { figure });
    expect([picture.width, picture.height], 'the figure’s size at its ratio').toEqual([960, 640]);
    const p = picture.getContext('2d').getImageData(0, 0, 960, Math.round(bands.top * 2)).data, bg = theme().bgRgba.map((v) => Math.round(v * 255));
    let text = 0;
    for (let i = 0; i < p.length; i += 4) if (Math.abs(p[i] - bg[0]) > CHANNEL_TOLERANCE || Math.abs(p[i + 1] - bg[1]) > CHANNEL_TOLERANCE) text++;
    expect(text, 'the title’s pixels in its band').toBeGreaterThan(100);
    expect(p[3], 'the band on the plot’s background, opaque').toBe(255);
    // In the other look: the background and the traces in its colours.
    copy.colors.dark = !prefs.dark;
    await settle(6);
    const other = copy.picture(copy.id, { figure }), look = theme(!prefs.dark);
    const corner = other.getContext('2d').getImageData(1, 1, 1, 1).data;
    expect([...corner.slice(0, 3)], 'the other look’s background').toEqual(look.bgRgba.slice(0, 3).map((v) => Math.round(v * 255)));
    for (const key of SIGNALS) expect(coloured(other, look.tracesRgba[copy.colors.of(key)]), `${key} in the other look`).toBeGreaterThan(150);
    expect(s.el.querySelector('.stage-box').classList.contains(prefs.dark ? 'light' : 'dark'), 'what the page lays over it in that look').toBe(true);
    const remove = sideBySide(picture, other);
    await frames(2);
    await shot('copy-figure-both-looks');
    remove();
  });

  it('is refused past the browser’s largest canvas, and a copy drawn small is never a monitor', async () => {
    const { copy } = await copied();
    copies.push(copy);
    const s = await stage(copy, { width: 300, height: 140, pixelRatio: 1 });
    expect(copy.pictureRefused(copy.id, { width: 300, height: 140, ratio: 1 }), 'its own size').toBe(null);
    expect(copy.pictureRefused(copy.id, { width: 100_000, height: 140, ratio: 1 }), 'too wide').toBe('Too large for this browser');
    expect(copy.pictureRefused(copy.id, { width: 20_000, height: 20_000, ratio: 1 }), 'too many pixels').toBe('Too large for this browser');
    // 300 by 140 is a monitor's size on the page, where its header floats over the plot.
    expect(monitorOf({ width: 300, height: 140 }, 'overlay', SIGNALS.length), 'a monitor’s size').toBe(true);
    expect(s.el.querySelector('.plot-card').classList.contains('monitor'), 'the copy a monitor').toBe(false);
    expect(copy.pv(copy.id).geo.head, 'a header the plot makes room for').toBe(-Infinity);
  });
});
