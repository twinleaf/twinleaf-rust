// The export dialog (ExportDialog.svelte), driven as a person drives it:
// opened from a plot's menu, its controls pressed with the mouse and the
// keys, its files caught where the browser's save window would take them.
// - It opens on the plots the menu's command acts on: the plot, or every
//   selected plot, a thumbnail each, each ticked; an unticked plot is in no
//   file.
// - Each choice of time gives the stretch it names, to the figure and to
//   the data asked of the hub.
// - Each row of the inspector changes the figure and the saved image, and
//   nothing on the page: its plots, its undo steps, its view steps, its
//   colours and the tabs it keeps.
// - A stacked plot's signals drag to reorder its legend and its lanes.
// - An image file is width × height × resolution pixels, and is the figure
//   shown, finer: scaled to the figure's size the two are alike.
// - Data asks the hub what the export always asked for the same choices.
// - Save asks where at the press, then shows a bar while the files are
//   made, however long that takes, and writes them whole into the place
//   picked with no second press; a window closed with nothing picked makes
//   nothing, and Cancel or a failure leaves the place picked as it was.
//   Where the browser has no save window, each file downloads.
// - Escape closes it, leaving nothing of its copies; focus goes back to the
//   plot. It fits a 900 × 600 window, scrolling inside itself, and every
//   control is reached by Tab.
// - Copy image, among the plot's header actions while the pointer is on
//   the plot, and on its menu, puts the plot's picture on the clipboard.
import { afterEach, describe, expect, it, vi } from 'vitest';
import { commands, page as browser, userEvent } from 'vitest/browser';
import fc from 'fast-check';
import { flushSync } from 'svelte';
import { centre, clickOn, closeAll, focusPage, frames, holdHistory, leafOf, mountPage, pickItem, plotMenu, pressKey, settle, until, wait } from './harness/page.js';
import * as clock from '../src/lib/clock.js';
import { plotIds } from '../src/lib/layout.js';
import { SAVE_KEY } from '../src/lib/tabs.js';
import { colors } from '../src/lib/colors.svelte.js';
import { prefs } from '../src/lib/prefs.svelte.js';
import { exportUrl } from '../src/lib/exportfile.js';
import { theme as themeOf } from '../src/lib/plot.js';

/** Where VITE_TL_SHOTS names a folder, relative to this file, the screenshots go there. */
const SHOTS = import.meta.env.VITE_TL_SHOTS;
const shot = async (name) => SHOTS && (await browser.screenshot({ path: `${SHOTS}/${name}.png` }));

const SIGNALS = ['sine.sine', 'aux.triangle', 'aux.saw'];
/** The mean difference of a channel, of 255, the figure shown and its file scaled to it may have. */
const LIKENESS = 6;
/** How far, of 255, a thumbnail's pixel may be from a trace's colour and be of it; and the least share of a thumbnail's pixels in its traces' colours. */
const THUMB_TOLERANCE = 40, THUMB_SHARE = 0.02;

let page, restore = [];
afterEach(async () => {
  for (const r of restore.splice(0)) r();
  vi.restoreAllMocks();
  page?.dispose();
  page = null;
  await browser.viewport(1200, 900);
});

/** The page with `plots` plots, a list of signal keys each, side by side, its hub holding ten minutes of each, every plot paused so it shows one stretch. */
async function exportPage(plots = [SIGNALS.slice(0, 2)], options = {}) {
  page = await mountPage(plots, 'columns', 0, SIGNALS, [], options);
  holdHistory(page, { history: 600 });
  await settle();
  for (const id of plotIds(page.ws.layout)) page.plots.commands.run('pause', { on: id });
  await focusPage();
  await settle(6);
  return plotIds(page.ws.layout);
}

/** Gives plot `id` of the page X cursors 2 s and 4 s into what it shows, as a step made before a case, so it has a readout. */
function withCursors(id) {
  const [t0] = page.plots.view(id).range(page.hub.now(0));
  page.history.group('Prepare', () => page.ws.setPlot(id, { cursors: { x: { a: t0 + 2, b: t0 + 4 } } }));
  flushSync();
}
const dialog = () => document.querySelector('.export-dialog[data-state=open]');
/** The dialog's control that reads `text`, or is labelled it. */
const control = (text) => [...dialog().querySelectorAll('button, input, select')].find((b) => b.getAttribute('aria-label') === text || b.textContent.trim() === text) ?? null;
/** Presses the dialog's control `text`. */
const press = async (text) => {
  const el = control(text);
  expect(el, `the control ${text}`).toBeTruthy();
  await clickOn(el);
  await settle();
};
/** Picks `value` in the dialog's select labelled `label`. */
const choose = async (label, value) => {
  await userEvent.selectOptions(control(label), value);
  await settle();
};
/** Types `text` into the dialog's field labelled `label`, in place of what it held, and leaves it. */
const fill = async (label, text) => {
  await userEvent.fill(control(label), String(text));
  control(label).dispatchEvent(new Event('change', { bubbles: true }));
  await settle();
};

/** Has the page's copies of its plots noted as each is made (PlotHost `copy`), in order. */
function watchCopies() {
  const made = [], copy = page.plots.copy.bind(page.plots);
  vi.spyOn(page.plots, 'copy').mockImplementation((id) => {
    const c = copy(id);
    made.push(c);
    return c;
  });
  return made;
}

/** Opens the dialog from plot `ids[0]`'s menu with `ids` selected, or that plot alone, and waits for its figure. */
async function openExport(ids) {
  if (ids.length > 1) {
    page.ws.selection.only(ids);
    flushSync();
  }
  await plotMenu(ids[0]);
  await pickItem('Export');
  const opened = await until(() => dialog()?.querySelector('.stage-shown'));
  if (!opened) await shot('debug-open');
  expect(opened, 'the dialog and its figure').toBe(true);
  await settle(8);
}
/** Waits until the figure shows the picked plot drawn: its copy's plot on the stage, its windows read. */
const drawn = (copy) => until(() => copy.pv(copy.id)?.drawn && !page.hub.session.reading(copy.id)).then(() => settle(4));

/** How long after a press, in ms, a browser opens its save windows (Chromium's transient activation), on the page's clock here. */
const ACTIVATION_MS = 5000;
/**
 * Has the browser's save and folder windows hand over what is saved, in
 * place of a file, and returns it: each {name, blob}, as each file is
 * written whole. As a browser does, a window opens only within
 * ACTIVATION_MS of the page's time of a press, and refuses later. Beside
 * it: `picks`, each window opened, with `note()` as it opened; `made`, the
 * names of the files the windows and the folders hold, an empty one from
 * the moment its save window picks it, as Chromium makes it, and gone once
 * its handle removes it; and `answer`, 'pick' or 'close', which the windows
 * do.
 */
function catchSaves({ note = () => null } = {}) {
  const saved = [], kept = [window.showSaveFilePicker, window.showDirectoryPicker];
  saved.picks = [];
  saved.made = new Set();
  saved.answer = 'pick';
  let pressed = -Infinity;
  const onPress = () => (pressed = clock.now());
  document.addEventListener('pointerdown', onPress, true);
  document.addEventListener('keydown', onPress, true);
  const opened = (kind) => {
    if (clock.now() - pressed > ACTIVATION_MS) throw new DOMException('Must be handling a user gesture to show a file picker.', 'SecurityError');
    saved.picks.push({ kind, noted: note() });
    if (saved.answer === 'close') throw new DOMException('The user aborted a request.', 'AbortError');
  };
  const file = (name) => ({
    name,
    createWritable: async () => ({ write: async (blob) => void (saved.made.add(name), saved.push({ name, blob })), close: async () => {} }),
    remove: async () => void saved.made.delete(name),
  });
  window.showSaveFilePicker = vi.fn(async ({ suggestedName }) => {
    opened('file');
    saved.made.add(suggestedName);
    return file(suggestedName);
  });
  window.showDirectoryPicker = vi.fn(async () => (opened('folder'), { getFileHandle: async (name) => file(name) }));
  restore.push(() => {
    [window.showSaveFilePicker, window.showDirectoryPicker] = kept;
    document.removeEventListener('pointerdown', onPress, true);
    document.removeEventListener('keydown', onPress, true);
  });
  return saved;
}
/** `n` files, as a message counts them. */
const filesOf = (n) => (n === 1 ? '1 file' : `${n} files`);
/** Has the page's asks of the hub for an export answered with `body`, noted as each comes: their queries. */
function catchExports(body = () => new Response('time (s),sine.sine (V)\n1,2\n')) {
  const asked = [], fetched = window.fetch;
  window.fetch = (url, init) => {
    const at = new URL(String(url), location.href);
    if (at.pathname !== '/api/export') return fetched(url, init);
    asked.push(Object.fromEntries(at.searchParams));
    return Promise.resolve(body(init));
  };
  restore.push(() => (window.fetch = fetched));
  return asked;
}
/** Presses Save and waits until `n` files are saved and the dialog has closed. */
async function saveAll(saved, n = 1) {
  const before = saved.length;
  await press(control('Save image…') ? 'Save image…' : 'Save data…');
  expect(await until(() => saved.length >= before + n, 20000), `${filesOf(n)} saved`).toBe(true);
  expect(await until(() => !document.querySelector('.export-dialog')), 'the dialog closed once saved').toBe(true);
  await settle();
  return saved.slice(before);
}
/** The pixels of `blob`, an image file, as a canvas. */
async function decoded(blob) {
  const bmp = await createImageBitmap(blob), c = document.createElement('canvas');
  [c.width, c.height] = [bmp.width, bmp.height];
  c.getContext('2d').drawImage(bmp, 0, 0);
  return c;
}
/** The figure shown, as {width, height, data}. */
const preview = () => {
  const c = dialog().querySelector('.stage-shown');
  return c.getContext('2d').getImageData(0, 0, c.width, c.height);
};
/** The mean difference of a channel, of 255, between images `a` and `b`, {data} of one size. */
const meanDiff = (a, b) => {
  let sum = 0;
  for (let i = 0; i < a.data.length; i++) sum += Math.abs(a.data[i] - b.data[i]);
  return sum / a.data.length;
};
/** `c`, a canvas, scaled to `w` by `h`, as {width, height, data}. */
function scaledTo(c, w, h) {
  const out = document.createElement('canvas');
  [out.width, out.height] = [w, h];
  const ctx = out.getContext('2d');
  ctx.imageSmoothingQuality = 'high';
  ctx.drawImage(c, 0, 0, w, h);
  return ctx.getImageData(0, 0, w, h);
}
/** What the page keeps and holds of its plots, to compare before and after. */
const pageState = () => ({
  tabs: localStorage.getItem(SAVE_KEY),
  undos: page.history.undos.length,
  redos: page.history.redos.length,
  back: page.plots.viewHistory.canBack,
  colors: JSON.stringify([colors.given, colors.picked, colors.last]),
  plots: JSON.stringify(page.ws.plots),
  views: JSON.stringify(plotIds(page.ws.layout).map((id) => page.plots.view(id).range(page.hub.now(0)))),
});

describe('the export dialog', { timeout: 120_000 }, () => {
  it('opens on the plot its menu is on, or on every selected plot, each ticked, and an unticked plot is in no file', async () => {
    const ids = await exportPage([SIGNALS.slice(0, 2), [SIGNALS[2]], [SIGNALS[0]]]);
    await openExport([ids[1]]);
    expect(dialog().querySelectorAll('.thumb').length, 'a thumbnail for the one plot').toBe(1);
    expect(control('Saved as'), 'no choice of one file or several for one plot').toBe(null);
    await shot('export-one-data');
    await pressKey('Escape');
    expect(await until(() => !document.querySelector('.export-dialog')), 'closed').toBe(true);

    await openExport([ids[0], ids[2]]);
    const thumbs = dialog().querySelectorAll('.thumb');
    expect(thumbs.length, 'a thumbnail a selected plot').toBe(2);
    expect([...thumbs].map((t) => t.querySelector('input').checked), 'each ticked').toEqual([true, true]);
    expect([...thumbs].map((t) => t.querySelector('small').textContent), 'each named by its signals').toEqual(['sine.sine, aux.triangle', 'sine.sine']);
    expect(dialog().querySelectorAll('.will tbody tr').length, 'a file each').toBe(2);
    await shot('export-several-data');
    await clickOn(thumbs[0].querySelector('input'));
    await settle();
    const rows = [...dialog().querySelectorAll('.will tbody tr')];
    expect(rows.length, 'one file once a plot is unticked').toBe(1);
    expect(rows[0].querySelector('td.c').textContent, 'the ticked plot’s file').toBe('sine.sine');
    await press('Image');
    expect(control('Plot 1'), 'the Plot tab names the plot picked').toBeTruthy();
    await clickOn(thumbs[1].querySelector('.pick'));
    await settle();
    expect(control('Plot 2'), 'picking another plot names it').toBeTruthy();
    await shot('export-several-image');
  });

  it('shows and exports the stretch each choice of time names', async () => {
    const [id] = await exportPage();
    // X cursors two seconds apart in what the plot shows.
    const [t0, t1] = page.plots.view(id).range(page.hub.now(0));
    page.history.group('Prepare', () => page.ws.setPlot(id, { cursors: { x: { a: t0 + 2, b: t0 + 4 } } }));
    await settle();
    const made = watchCopies(), asked = catchExports();
    const saved = catchSaves();
    const oldest = page.hub.session.memory.oldest[0], now = page.hub.now(0);
    // Each choice: the stretch the figure shows, and the data asked for. All captured shows all the plot can show, and asks for all the hub holds until the dialog opened.
    const want = {
      shows: { figure: [t0, t1], data: [t0, t1] },
      all: { figure: page.plots.view(id).limits(0), data: [oldest, now] },
      cursors: { figure: [t0 + 2, t0 + 4], data: [t0 + 2, t0 + 4] },
    };
    for (const [range, { figure, data }] of Object.entries(want)) {
      await openExport([id]);
      const copy = made.at(-1);
      await choose('Time', range);
      await drawn(copy);
      const shown = copy.view(copy.id).range(now);
      expect(shown[0], `${range}: the figure from`).toBeCloseTo(figure[0], 6);
      expect(shown[1], `${range}: the figure to`).toBeCloseTo(figure[1], 6);
      const pv = copy.pv(copy.id), px = (figure[1] - figure[0]) / (pv.geo.plot.w * copy.renderer.dpr);
      expect(Math.abs(pv.drawn.x0 - figure[0]), `${range}: drawn from, within 2 of its pixels`).toBeLessThan(2 * px);
      await saveAll(saved);
      expect(asked.length, `${range}: asked of the hub`).toBeGreaterThan(0);
      expect(await until(() => !document.querySelector('.export-dialog')), `${range}: closed`).toBe(true);
      expect([+asked.at(-1).t0, +asked.at(-1).t1], `${range}: the data asked of the hub`).toEqual(data);
    }
  });

  it('changes the figure and the saved image by each row of its inspector, and nothing on the page', async () => {
    const [id] = await exportPage();
    const made = watchCopies(), saved = catchSaves();
    const key = SIGNALS[0];
    // Each row: what it needs first, and the change, each made through the dialog.
    const legend = async () => (await press('Plot 1'), await press('Legend shown'));
    const ROWS = [
      { row: 'Background', change: async () => press(control('Dark').getAttribute('aria-pressed') === 'true' ? 'White' : 'Dark') },
      { row: 'Grid', change: () => press('Grid') },
      { row: 'Date and time', change: () => press('Date and time') },
      { row: 'Device', change: () => press('Device') },
      { row: 'X axis shows', change: () => choose('X axis shows', 'secs') },
      { row: 'X label', change: () => fill('X label', 'Seconds of it') },
      { row: 'Title', first: () => press('Plot 1'), change: () => fill('Title', 'A title') },
      { row: 'Layout Stacked', first: () => press('Plot 1'), change: () => press('Stacked') },
      { row: 'Layout Independent', first: () => press('Plot 1'), change: () => press('Independent') },
      { row: 'Legend', first: () => press('Plot 1'), change: () => press('Legend shown') },
      { row: 'Legend place', first: legend, change: () => press('Bottom left') },
      { row: 'Name', first: legend, change: () => fill(`Name of ${key}`, 'Field') },
      { row: 'Colour', first: () => press('Plot 1'), change: async () => (await press(`Colour of ${key}`), await until(() => document.querySelector('.swatch-choice')), await clickOn(document.querySelectorAll('.swatch-choice')[5]), await until(() => !document.querySelector('[data-slot=popover-content]'))) },
      { row: 'Axis digits', first: () => press('Plot 1'), change: () => choose('Axis digits', '5') },
      { row: 'Axis notation', first: () => press('Plot 1'), change: () => choose('Axis notation', 'exp') },
      { row: 'Axis label', first: () => press('Plot 1'), change: () => choose('Axis label', 'name') },
      { row: 'Axis range', first: () => press('Plot 1'), change: async () => (await choose('Range', 'typed'), await fill('From', -5), await fill('To', 5)) },
    ];
    for (const { row, first, change } of ROWS) {
      const was = pageState();
      // Before the change, as saved.
      await openExport([id]);
      await press('Image');
      await first?.();
      await drawn(made.at(-1));
      const [before] = await saveAll(saved);
      // The change, shown and saved.
      await openExport([id]);
      await press('Image');
      await first?.();
      await drawn(made.at(-1));
      const shownBefore = preview();
      await change();
      await drawn(made.at(-1));
      const shownAfter = preview();
      if (row === 'Layout Stacked') await shot('export-layout-stacked');
      if (row === 'Legend place') await shot('export-legend-place');
      const [after] = await saveAll(saved);
      expect(meanDiff(shownBefore, shownAfter), `${row}: the figure changed`).toBeGreaterThan(0.02);
      const [a, b] = [await decoded(before.blob), await decoded(after.blob)];
      const same = a.width === b.width && a.height === b.height;
      expect(!same || meanDiff(scaledTo(a, a.width, a.height), scaledTo(b, a.width, a.height)) > 0.02, `${row}: the saved image changed`).toBe(true);
      expect(pageState(), `${row}: the page as it was`).toEqual(was);
    }
  });

  it('fixes the unit of an axis in a known unit, in the figure, the saved image and its range fields, and nothing on the page', async () => {
    const [id] = await exportPage([[SIGNALS[0]]], { units: { [SIGNALS[0]]: 'nT' } });
    const made = watchCopies(), saved = catchSaves(), was = pageState();
    await openExport([id]);
    await press('Image');
    await press('Plot 1');
    const copy = made.at(-1);
    await drawn(copy);
    const [before] = await saveAll(saved);
    await openExport([id]);
    await press('Image');
    await press('Plot 1');
    await drawn(made.at(-1));
    const shownBefore = preview(), c = made.at(-1), pv = () => c.pv(c.id);
    expect([...control('Axis unit').options].map((o) => o.textContent), 'the units offered').toEqual(['Auto', 'fT', 'pT', 'nT', 'µT', 'mT', 'T']);
    await choose('Axis unit', '-12');
    await drawn(c);
    expect(c.plot(c.id).yFormat, 'the copy’s axis in pT').toEqual({ shared: { unit: -12 } });
    // The pane's signal row and axis, and the figure's legend, name pT; the page's plot and Signals list nT.
    if (control('Legend shown').getAttribute('aria-pressed') !== 'true') await press('Legend shown');
    await drawn(c);
    const unitsIn = (root) => [...root.querySelectorAll('[data-unit-of]')].map((el) => el.textContent.trim());
    expect(new Set(unitsIn(dialog().querySelector('.stage-box'))), 'the figure’s legend').toEqual(new Set(['pT']));
    expect(unitsIn(dialog()).filter((t) => t !== 'pT'), 'the dialog names pT alone').toEqual([]);
    expect(unitsIn(dialog()).length, 'the pane and the figure').toBeGreaterThanOrEqual(3);
    expect(unitsIn(leafOf(id)), 'the page’s plot').toEqual(['nT']);
    expect([...document.querySelectorAll('.sig-units')].map((el) => el.textContent.trim()).sort(), 'the Signals list').toEqual(['V', 'V', 'nT']);
    expect(pv().legend.cursors ?? null, 'no cursors here').toBe(null);
    const ticks = pv().tickLabels.filter((l) => l.axis === 'shared').map((l) => Number(l.text.replace('−', '-')));
    expect(Math.max(...ticks.map(Math.abs)), 'its ticks in pT').toBeGreaterThan(100);
    expect(meanDiff(shownBefore, preview()), 'the figure changed').toBeGreaterThan(0.02);
    // A range typed in pT is that range in nT, and reads back in pT.
    await choose('Range', 'typed');
    await fill('From', -500);
    await fill('To', 500);
    await drawn(c);
    expect(pv().yBounds('shared'), 'the range typed in pT').toEqual([-0.5, 0.5]);
    expect([control('From').value, control('To').value], 'read back in pT').toEqual(['-500', '500']);
    const [after] = await saveAll(saved);
    const [a, b] = [await decoded(before.blob), await decoded(after.blob)];
    expect(meanDiff(scaledTo(a, a.width, a.height), scaledTo(b, a.width, a.height)), 'the saved image changed').toBeGreaterThan(0.02);
    expect(pageState(), 'the page as it was').toEqual(was);
    // An axis in a unit the page does not know has no unit to fix.
    page.dispose();
    const [other] = await exportPage([[SIGNALS[0]]], { units: { [SIGNALS[0]]: 'psi' } });
    await openExport([other]);
    await press('Image');
    await press('Plot 1');
    expect(control('Axis unit'), 'no Unit for psi').toBe(null);
  });

  it('reorders a stacked plot’s legend and lanes as its signals are dragged', async () => {
    const [id] = await exportPage([SIGNALS]);
    const made = watchCopies();
    await openExport([id]);
    const copy = made.at(-1);
    await press('Image');
    await press('Plot 1');
    await press('Stacked');
    await press('Legend shown');
    await drawn(copy);
    const rows = () => [...dialog().querySelectorAll('.r.sig')];
    expect(rows().every((r) => r.querySelector('.grip')), 'each signal has a grip').toBe(true);
    // The last row dragged onto the first.
    const data = new DataTransfer(), [from, to] = [rows()[2], rows()[0]];
    from.dispatchEvent(new DragEvent('dragstart', { bubbles: true, dataTransfer: data }));
    to.dispatchEvent(new DragEvent('dragover', { bubbles: true, cancelable: true, dataTransfer: data }));
    flushSync();
    expect(to.classList.contains('land'), 'a line where it lands').toBe(true);
    to.dispatchEvent(new DragEvent('drop', { bubbles: true, cancelable: true, dataTransfer: data }));
    from.dispatchEvent(new DragEvent('dragend', { bubbles: true, dataTransfer: data }));
    await drawn(copy);
    const order = [SIGNALS[2], SIGNALS[0], SIGNALS[1]];
    expect(rows().map((r) => r.dataset.key), 'the list').toEqual(order);
    expect(copy.plot(copy.id).items.map((i) => i.key), 'the copy’s signals').toEqual(order);
    const stage = dialog().querySelector('.stage-box');
    expect([...stage.querySelectorAll('.pinned-legend .row')].map((r) => r.dataset.key), 'the legend').toEqual(order);
    const lanes = copy.pv(copy.id).geo.lanes, axes = copy.pv(copy.id).yAxes;
    expect(lanes.map((l) => axes.find((a) => a.lane === l)?.key), 'the lanes, top to bottom').toEqual(order);
    expect(page.ws.plots[id].items.map((i) => i.key), 'the page’s plot').toEqual(SIGNALS);
    await shot('export-reordered');
    // An overlay's signals have no grips and do not drag.
    await press('Overlay');
    expect(rows().some((r) => r.querySelector('.grip') || r.draggable), 'an overlay’s rows').toBe(false);
  });

  it('drags the legend on the figure as on the plot, to the corner it snaps to', async () => {
    const [id] = await exportPage();
    const made = watchCopies();
    await openExport([id]);
    const copy = made.at(-1);
    await press('Image');
    await press('Plot 1');
    await press('Legend shown');
    await drawn(copy);
    expect(await until(() => dialog().querySelector('[data-handle=legend]')), 'a handle on the figure’s legend').toBe(true);
    const handle = dialog().querySelector('[data-handle=legend]'), shown = dialog().querySelector('.stage-shown').getBoundingClientRect();
    const from = centre(handle), h = handle.getBoundingClientRect();
    // To the figure's bottom left, a little short of it: it snaps there.
    const to = [shown.left + h.width / 2 + 2, shown.bottom - h.height / 2 - 30];
    await commands.mouse([['move', ...from], ['down'], ...[1, 2, 3, 4, 5, 6].map((k) => ['move', from[0] + ((to[0] - from[0]) * k) / 6, from[1] + ((to[1] - from[1]) * k) / 6]), ['up']]);
    await drawn(copy);
    expect(copy.plot(copy.id).legendAt, 'the copy’s legend at the bottom left').toEqual({ x: 0, y: 1 });
    expect(copy.history.undos.at(-1)?.label, 'the plot’s own edit').toBe('Move the legend');
    expect(page.ws.plots[id].legendAt, 'the page’s plot’s legend').toBeUndefined();
    expect(control('Bottom left').getAttribute('aria-pressed'), 'the place picked among the nine').toBe('true');
    await shot('export-legend-dragged');
  });

  it('drags the cursors’ readout on the figure as on the plot, to the corner it is let go past', async () => {
    const [id] = await exportPage();
    withCursors(id);
    const made = watchCopies();
    await openExport([id]);
    const copy = made.at(-1);
    await press('Image');
    await press('Plot 1');
    await drawn(copy);
    expect(await until(() => dialog().querySelector('[data-handle=readout]')), 'a handle on the figure’s readout').toBe(true);
    const handle = dialog().querySelector('[data-handle=readout]'), shown = dialog().querySelector('.stage-shown').getBoundingClientRect();
    const from = centre(handle), to = [shown.right + 20, shown.bottom + 20];
    await commands.mouse([['move', ...from], ['down'], ...[1, 2, 3, 4, 5, 6].map((k) => ['move', from[0] + ((to[0] - from[0]) * k) / 6, from[1] + ((to[1] - from[1]) * k) / 6]), ['up']]);
    await drawn(copy);
    expect(copy.plot(copy.id).readoutAt, 'the copy’s readout at the bottom right').toEqual({ x: 1, y: 1 });
    expect(copy.history.undos.at(-1)?.label, 'the plot’s own edit').toBe('Move the readout');
    expect(page.ws.plots[id].readoutAt, 'the page’s plot’s readout').toBeUndefined();
    expect(control('Cursors bottom right').getAttribute('aria-pressed'), 'the place picked among the nine').toBe('true');
    await shot('export-readout-dragged');
  });

  it('shows the cursors’ readout or not, at each of its places, in the figure and the saved image, and nothing on the page', async () => {
    const [id] = await exportPage();
    withCursors(id);
    const made = watchCopies(), saved = catchSaves();
    const ROWS = [
      { row: 'Cursors shown', change: () => press('Cursors shown') },
      ...['top centre', 'top right', 'middle left', 'middle centre', 'middle right', 'bottom left', 'bottom centre', 'bottom right'].map((p) => ({ row: `Cursors ${p}`, change: () => press(`Cursors ${p}`) })),
    ];
    for (const { row, change } of ROWS) {
      const was = pageState();
      await openExport([id]);
      await press('Image');
      await press('Plot 1');
      await drawn(made.at(-1));
      const [before] = await saveAll(saved);
      await openExport([id]);
      await press('Image');
      await press('Plot 1');
      await drawn(made.at(-1));
      const shownBefore = preview();
      await change();
      await drawn(made.at(-1));
      const shownAfter = preview();
      if (row === 'Cursors bottom centre') await shot('export-readout-place');
      const [after] = await saveAll(saved);
      expect(meanDiff(shownBefore, shownAfter), `${row}: the figure changed`).toBeGreaterThan(0.02);
      const [a, b] = [await decoded(before.blob), await decoded(after.blob)];
      expect(meanDiff(scaledTo(a, a.width, a.height), scaledTo(b, a.width, a.height)), `${row}: the saved image changed`).toBeGreaterThan(0.02);
      expect(pageState(), `${row}: the page as it was`).toEqual(was);
    }
  });

  it('never puts the legend and the readout over each other by their pickers', async () => {
    const [id] = await exportPage();
    withCursors(id);
    const made = watchCopies();
    await openExport([id]);
    const copy = made.at(-1);
    await press('Image');
    await press('Plot 1');
    await press('Legend shown');
    await drawn(copy);
    const NAMES = ['top left', 'top centre', 'top right', 'middle left', 'middle centre', 'middle right', 'bottom left', 'bottom centre', 'bottom right'];
    const box = (sel) => dialog().querySelector(`.stage-box ${sel}`).getBoundingClientRect();
    const place = fc.constantFrom(...NAMES);
    // The same place for both, picked in either order, among the cases.
    const examples = [['top left', 'top left', true], ['top right', 'top right', false], ['middle centre', 'middle centre', true]];
    await fc.assert(
      fc.asyncProperty(place, place, fc.boolean(), async (legend, readout, legendLast) => {
        const picks = legendLast ? [`Cursors ${readout}`, legend[0].toUpperCase() + legend.slice(1)] : [legend[0].toUpperCase() + legend.slice(1), `Cursors ${readout}`];
        for (const p of picks) await press(p);
        await drawn(copy);
        const [l, r] = [box('.pinned-legend'), box('.cursor-readout')];
        const over = l.left < r.right && r.left < l.right && l.top < r.bottom && r.top < l.bottom;
        expect(over, `legend ${legend}, readout ${readout}, ${legendLast ? 'the legend' : 'the readout'} picked last: one over the other`).toBe(false);
        // The one picked last is where it was picked.
        const last = legendLast ? copy.plot(copy.id).legendAt : copy.plot(copy.id).readoutAt, name = legendLast ? legend : readout;
        const [y, x] = name.split(' ');
        expect(last, `${name} picked last, where it was picked`).toEqual({ x: { left: 0, centre: 0.5, right: 1 }[x], y: { top: 0, middle: 0.5, bottom: 1 }[y] });
      }),
      { examples, numRuns: examples.length + fc.readConfigureGlobal().numRuns },
    );
    expect(page.ws.plots[id].legendAt ?? null, 'the page’s legend').toBe(null);
    await shot('export-legend-and-readout');
    // Side by side in one row at the inspector's narrowest, in a 900 by 600 window, each column within it.
    await browser.viewport(900, 600);
    await settle(8);
    const insp = dialog().querySelector('.insp').getBoundingClientRect(), cols = [...dialog().querySelectorAll('.overlay')].map((c) => c.getBoundingClientRect());
    expect(cols.length, 'the legend and the cursors').toBe(2);
    expect(Math.abs(cols[0].top - cols[1].top), 'in one row').toBeLessThan(1);
    for (const [i, c] of cols.entries()) expect(c.left >= insp.left && c.right <= insp.right, `column ${i + 1} within the inspector`).toBe(true);
    for (const c of dialog().querySelectorAll('.overlay')) {
      expect(c.scrollWidth, 'nothing cut in a column').toBeLessThanOrEqual(c.clientWidth);
      const [chip, nine] = [c.querySelector('.chips').getBoundingClientRect(), c.querySelector('.nine').getBoundingClientRect()];
      expect(nine.top < chip.bottom && chip.top < nine.bottom, 'Shown and the places on one line, so the row is no taller than the legend’s alone').toBe(true);
    }
    await shot('export-legend-and-readout-900x600');
  });

  it('saves an image of width × height × resolution pixels, the figure shown, finer', async () => {
    const [id] = await exportPage();
    const made = watchCopies(), saved = catchSaves();
    const sizes = fc.oneof(
      fc.record({ unit: fc.constant('px'), w: fc.integer({ min: 120, max: 1400 }), h: fc.integer({ min: 120, max: 900 }), dpi: fc.constant(300) }),
      fc.record({ unit: fc.constant('in'), w: fc.integer({ min: 2, max: 6 }), h: fc.integer({ min: 2, max: 5 }), dpi: fc.constantFrom(96, 150, 300) }),
      fc.record({ unit: fc.constant('cm'), w: fc.integer({ min: 4, max: 15 }), h: fc.integer({ min: 4, max: 12 }), dpi: fc.constantFrom(96, 150, 300) }),
    );
    const examples = [[{ unit: 'px', w: 640, h: 300, dpi: 300 }], [{ unit: 'in', w: 4, h: 3, dpi: 150 }], [{ unit: 'cm', w: 6, h: 4, dpi: 300 }]];
    let worst = 0;
    await fc.assert(
      fc.asyncProperty(sizes, async ({ unit, w, h, dpi }) => {
        await openExport([id]);
        await press('Image');
        await choose('Shape', 'custom');
        await press(unit);
        if (unit !== 'px') await fill('Resolution', dpi);
        await fill('Width', w);
        await fill('Height', h);
        await drawn(made.at(-1));
        const shown = preview();
        const [file] = await saveAll(saved);
        const img = await decoded(file.blob), per = { px: 1, in: 96, cm: 96 / 2.54 }[unit], ratio = unit === 'px' ? 1 : dpi / 96;
        expect([img.width, img.height], `${w} × ${h} ${unit} at ${dpi} dpi`).toEqual([Math.round(w * per * ratio), Math.round(h * per * ratio)]);
        const diff = meanDiff(shown, scaledTo(img, shown.width, shown.height));
        worst = Math.max(worst, diff);
        expect(diff, `the file scaled to the figure, ${w} × ${h} ${unit}`).toBeLessThan(LIKENESS);
      }),
      { examples, numRuns: examples.length + 3 },
    );
    expect(worst, 'the worst mean difference of a channel, of 255, of the files scaled to their figures').toBeLessThan(LIKENESS);
  });

  it('asks the hub for the data the export always asked for the same choices', async () => {
    const ids = await exportPage([SIGNALS.slice(0, 2), [SIGNALS[0], SIGNALS[2]]]);
    // The second plot shows an earlier, shorter stretch, so the time the plots span is neither's alone.
    const [a0] = page.plots.view(ids[0]).range(page.hub.now(0));
    page.plots.viewHistory.step(() => page.plots.view(ids[1]).show(a0 - 30, a0 - 25, 0));
    await settle(4);
    const asked = catchExports(), saved = catchSaves();
    const session = page.hub.session, now = page.hub.now(0);
    /** The request as the export made it before this dialog, for plots `targets` and a time of 'shown' or 'whole'. */
    const before = (targets, time) => {
      const traces = [...new Map(targets.flatMap((i) => page.ws.plots[i].items.map((it) => session.byKey.get(it.key))).map((t) => [t.key, t])).values()];
      const ranges = targets.filter((i) => page.ws.plots[i].kind === 'time').map((i) => page.plots.view(i).range(now));
      const shown = ranges.length ? [Math.min(...ranges.map((r) => r[0])), Math.max(...ranges.map((r) => r[1]))] : null;
      const oldest = Math.min(...traces.map((t) => session.memory?.oldest?.[t.meta.id] ?? Infinity));
      const [t0, t1] = time === 'shown' && shown ? shown : [isFinite(oldest) ? oldest : -Infinity, now];
      return { hub: session.id, ids: traces.map((t) => t.meta.id), t0, t1 };
    };
    const query = (request, format) => Object.fromEntries(new URL(exportUrl(request, format, 'x'), location.href).searchParams);
    const CASES = [
      { targets: [ids[0]], time: 'shown', range: 'shows', format: 'csv' },
      { targets: [ids[0]], time: 'whole', range: 'all', format: 'h5' },
      { targets: ids, time: 'shown', range: 'shows', format: 'csv', one: true },
      { targets: ids, time: 'whole', range: 'all', format: 'csv', one: true },
    ];
    for (const { targets, time, range, format, one } of CASES) {
      await openExport(targets);
      await choose('Time', range);
      await choose('Format', format);
      if (one) await choose('Saved as', 'one');
      await saveAll(saved);
      const got = asked.at(-1), want = query(before(targets, time), format);
      delete got.name;
      delete want.name;
      expect(got, `${targets.length} plots, ${time}, ${format}`).toEqual(want);
    }
    // A file each: each plot's signals over its own stretch.
    await openExport(ids);
    await choose('Saved as', 'each');
    const n = asked.length;
    await saveAll(saved, 2);
    expect(asked.slice(n).map((q) => q.ids), 'a file a plot').toEqual(ids.map((i) => page.ws.plots[i].items.map((it) => String(session.byKey.get(it.key).meta.id)).join(',')));
  });

  /** Has the hub answer each export ask with a body fed by hand: `feeds`, each ask's stream controller, and `asked`, the queries. */
  function slowExports() {
    const feeds = [];
    const asked = catchExports((init) => {
      let feed;
      const body = new ReadableStream({ start: (c) => (feed = c) });
      feeds.push(feed);
      init?.signal?.addEventListener('abort', () => feed.error(new DOMException('aborted', 'AbortError')));
      return new Response(body);
    });
    return { asked, feeds };
  }
  const bar = () => dialog()?.querySelector('[role=progressbar]') ?? null;
  const notices = () => [...document.querySelectorAll('[data-sonner-toast]')].map((t) => t.textContent.trim());

  it('asks where at the press, then shows a bar while the file is made, longer than a press lets a browser ask, and saves it with no second press', async () => {
    const [id] = await exportPage();
    let asked = [];
    const saved = catchSaves({ note: () => asked.length });
    const slow = slowExports();
    asked = slow.asked;
    await openExport([id]);
    await press('Save data…');
    expect(saved.picks, 'the save window, at the press, before the hub was asked').toEqual([{ kind: 'file', noted: 0 }]);
    expect(await until(() => slow.asked.length === 1 && slow.feeds[0]), 'asked of the hub').toBe(true);
    expect(bar(), 'the bar').toBeTruthy();
    slow.feeds[0].enqueue(new Uint8Array(200_000));
    await settle();
    expect(await until(() => +bar()?.getAttribute('aria-valuenow') > 0), 'the bar moves as bytes come').toBe(true);
    expect(bar().parentElement.textContent, 'the bytes so far').toContain('kB');
    expect(saved.length, 'nothing written before the file is whole').toBe(0);
    await shot('export-progress');
    // Longer than a press lets the browser open a save window.
    await wait(ACTIVATION_MS + 1500);
    expect(bar(), 'still making').toBeTruthy();
    slow.feeds[0].enqueue(new TextEncoder().encode('time,v\n1,2\n'));
    slow.feeds[0].close();
    expect(await until(() => saved.length === 1), 'saved with no second press').toBe(true);
    expect(saved.picks.length, 'one save window').toBe(1);
    expect((await saved[0].blob.arrayBuffer()).byteLength, 'the file, whole').toBe(200_000 + 11);
    expect(await until(() => !document.querySelector('.export-dialog')), 'closed').toBe(true);
    expect(await until(() => notices().some((t) => t.startsWith(`Saved ${saved[0].name}, `))), `a notice naming the file, among ${JSON.stringify(notices())}`).toBe(true);
  });

  it('makes nothing when the save window closes with nothing picked, and leaves no file when the making stops or fails', async () => {
    const ids = await exportPage([SIGNALS.slice(0, 2), [SIGNALS[2]]]);
    const saved = catchSaves(), { asked, feeds } = slowExports();
    await openExport([ids[0]]);
    // Closed with nothing picked: nothing made, the dialog as it was.
    saved.answer = 'close';
    await press('Save data…');
    await settle(4);
    expect(saved.picks.length, 'the save window opened').toBe(1);
    expect(asked.length, 'nothing asked of the hub').toBe(0);
    expect(bar(), 'no bar').toBe(null);
    expect(control('Save data…'), 'Save as it was').toBeTruthy();
    expect(notices(), 'no notice').toEqual([]);
    // Picked, then Cancel while it is made: the file picked goes, nothing written.
    saved.answer = 'pick';
    await press('Save data…');
    expect(await until(() => asked.length === 1 && feeds[0]), 'asked of the hub').toBe(true);
    expect([...saved.made], 'the file the save window made, empty').toHaveLength(1);
    feeds[0].enqueue(new Uint8Array(1000));
    await settle();
    await press('Cancel');
    expect(await until(() => saved.made.size === 0), `no file left behind: ${[...saved.made]}`).toBe(true);
    expect(saved.length, 'nothing written').toBe(0);
    expect(bar(), 'no bar once cancelled').toBe(null);
    expect(dialog(), 'the dialog stays').toBeTruthy();
    // Picked, then the hub fails partway: the file picked goes, nothing written, and the failure is told.
    await press('Save data…');
    expect(await until(() => asked.length === 2 && feeds[1]), 'asked again').toBe(true);
    feeds[1].enqueue(new Uint8Array(1000));
    await settle();
    feeds[1].error(new Error('the hub went away'));
    expect(await until(() => saved.made.size === 0 && !bar()), 'no file left behind').toBe(true);
    expect(saved.length, 'nothing written').toBe(0);
    expect(await until(() => notices().some((t) => t.includes('the hub went away'))), 'the failure told').toBe(true);
    await pressKey('Escape');
    expect(await until(() => !document.querySelector('.export-dialog')), 'closed').toBe(true);
    // Several files into a folder, the making stopped: no file in it.
    await openExport(ids);
    await press('Save data…');
    expect(saved.picks.at(-1).kind, 'the folder window').toBe('folder');
    expect(await until(() => asked.length === 3 && feeds[2]), 'asked of the hub').toBe(true);
    await press('Cancel');
    await settle(4);
    expect([...saved.made], 'nothing in the folder').toEqual([]);
    // Several files saved: told as files.
    feeds.length = 0;
    const done = saveAll(saved, 2);
    for (let i = 0; i < 2; i++) {
      expect(await until(() => feeds[i]), `file ${i + 1} asked`).toBe(true);
      feeds[i].enqueue(new TextEncoder().encode('t,v\n'));
      feeds[i].close();
    }
    await done;
    expect(await until(() => notices().some((t) => t.startsWith('Saved 2 files, '))), `a notice of 2 files, among ${JSON.stringify(notices())}`).toBe(true);
  });

  it('downloads each file where the browser has no save window', async () => {
    const ids = await exportPage([SIGNALS.slice(0, 2), [SIGNALS[2]]]);
    const kept = [window.showSaveFilePicker, window.showDirectoryPicker];
    [window.showSaveFilePicker, window.showDirectoryPicker] = [undefined, undefined];
    restore.push(() => ([window.showSaveFilePicker, window.showDirectoryPicker] = kept));
    const downloads = [];
    vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(function () {
      downloads.push(this.download);
    });
    catchExports();
    await openExport(ids);
    await press('Save data…');
    expect(await until(() => downloads.length === 2), 'a download a file').toBe(true);
    expect(await until(() => !document.querySelector('.export-dialog')), 'closed').toBe(true);
    expect(downloads.every((n) => /^twinleaf-\d{8}-\d{6}-.+\.csv$/.test(n)), `each named: ${downloads}`).toBe(true);
  });

  it('closes at Escape, leaving nothing of its copies, with focus back on the plot', async () => {
    const [id] = await exportPage();
    const made = watchCopies();
    await openExport([id]);
    const copy = made.at(-1);
    await drawn(copy);
    expect(page.hub.holders.has(copy), 'the copy holds the history').toBe(true);
    await pressKey('Escape');
    expect(await until(() => !document.querySelector('.export-dialog')), 'closed').toBe(true);
    await settle(4);
    expect(page.hub.holders.has(copy), 'its hold let go').toBe(false);
    expect(copy.renderer, 'its surface').toBe(null);
    expect(leafOf(id).contains(document.activeElement), `focus on the plot, not ${document.activeElement?.outerHTML.slice(0, 60)}`).toBe(true);
  });

  it('fits a 900 × 600 window, scrolling inside itself, and reaches every control by Tab', async () => {
    await browser.viewport(900, 600);
    const ids = await exportPage([SIGNALS.slice(0, 2), [SIGNALS[2]]]);
    await openExport(ids);
    for (const [tab, pane] of [['Data', null], ['Image', 'All'], ['Image', 'Plot 1']]) {
      await press(tab);
      if (pane) await press(pane);
      await settle(4);
      const r = dialog().getBoundingClientRect();
      expect(r.left >= 0 && r.top >= 0 && r.right <= 900 && r.bottom <= 600, `${tab} ${pane}: within the window, ${JSON.stringify(r)}`).toBe(true);
      expect(document.scrollingElement.scrollHeight, 'the page does not scroll').toBeLessThanOrEqual(600);
      await shot(`export-900x600-${tab}-${pane ?? ''}`.toLowerCase().replace(/\s/g, ''));
      if (pane) {
        const insp = dialog().querySelector('.insp');
        expect(insp.scrollHeight > insp.clientHeight && getComputedStyle(insp).overflowY === 'auto', `${tab} ${pane}: the inspector scrolls`).toBe(true);
      }
      // Tab through: each control is reached.
      const controls = [...dialog().querySelectorAll('button:not(:disabled), input:not(:disabled), select:not(:disabled)')].filter((el) => el.checkVisibility() && !el.closest('.plot-stage'));
      const reached = new Set();
      control(tab).focus();
      for (let i = 0; i < controls.length + 4; i++) {
        await pressKey('Tab');
        reached.add(document.activeElement);
      }
      expect(controls.filter((el) => !reached.has(el)).map((el) => el.getAttribute('aria-label') ?? el.textContent.trim()), `${tab} ${pane}: controls Tab never reached`).toEqual([]);
    }
  });

  it('copies the plot’s picture from its button, shown while the pointer is on the plot, and from its menu', async () => {
    const [id] = await exportPage();
    const written = [];
    vi.spyOn(navigator.clipboard, 'write').mockImplementation(async (items) => void written.push(items));
    const body = leafOf(id).querySelector('.plot-body'), button = () => leafOf(id).querySelector('.plot-actions > .copy-image');
    // The pointer last crossed the plot on its way to the status bar: its fade out ends.
    expect(await until(() => getComputedStyle(button()).opacity === '0'), 'hidden while the pointer is away').toBe(true);
    expect(getComputedStyle(button()).pointerEvents, 'taking no press while hidden').toBe('none');
    await commands.mouse([['move', ...centre(body)]]);
    expect(await until(() => getComputedStyle(button()).opacity === '1'), 'shown while the pointer is over the plot').toBe(true);
    await shot('copy-image-button');
    await clickOn(button());
    expect(await until(() => written.length === 1), 'copied from the button').toBe(true);
    const [item] = written[0];
    expect(item.types, 'as a PNG').toEqual(['image/png']);
    const img = await decoded(await item.getType('image/png')), want = page.plots.picture(id);
    expect([img.width, img.height], 'the plot’s picture').toEqual([want.width, want.height]);
    await focusPage();
    await plotMenu(id);
    await pickItem('Copy image');
    expect(await until(() => written.length === 2), 'copied from the menu').toBe(true);
  });

  it('shows each plot’s thumbnail with its traces plain to see, light and dark', async () => {
    const ids = await exportPage([SIGNALS.slice(0, 2), [SIGNALS[2]]]);
    const shares = [];
    for (const theme of ['light', 'dark']) {
      prefs.setTheme(theme);
      restore.push(() => prefs.setTheme('system'));
      await settle(6);
      await openExport(ids);
      const look = themeOf(theme === 'dark');
      for (const [i, img] of [...dialog().querySelectorAll('.thumb img')].entries()) {
        await until(() => img.complete && img.naturalWidth > 0);
        const c = document.createElement('canvas');
        [c.width, c.height] = [img.naturalWidth, img.naturalHeight];
        c.getContext('2d').drawImage(img, 0, 0);
        const p = c.getContext('2d').getImageData(0, 0, c.width, c.height).data;
        const keys = page.ws.plots[ids[i]].items.map((it) => it.key), traces = keys.map((k) => look.tracesRgba[colors.of(k)].map((v) => Math.round(v * 255)));
        let hit = 0;
        for (let j = 0; j < p.length; j += 4) if (traces.some(([r, g, b]) => Math.abs(p[j] - r) <= THUMB_TOLERANCE && Math.abs(p[j + 1] - g) <= THUMB_TOLERANCE && Math.abs(p[j + 2] - b) <= THUMB_TOLERANCE)) hit++;
        shares.push(`${theme} ${i + 1}: ${((100 * hit) / (c.width * c.height)).toFixed(2)}%`);
        expect.soft(hit / (c.width * c.height), `${theme}, plot ${i + 1}: the share of its pixels in its traces' colours`).toBeGreaterThan(THUMB_SHARE);
      }
      await shot(`export-thumbs-${theme}`);
      await pressKey('Escape');
      await until(() => !document.querySelector('.export-dialog'));
    }
  });

  it('looks like the agreed design, light and dark', async () => {
    const ids = await exportPage([SIGNALS, [SIGNALS[2]]]);
    for (const theme of ['light', 'dark']) {
      prefs.setTheme(theme);
      restore.push(() => prefs.setTheme('system'));
      await settle(4);
      await openExport(ids);
      await shot(`export-${theme}-data`);
      await press('Image');
      await shot(`export-${theme}-image-all`);
      await press('Plot 1');
      for (const layout of ['Overlay', 'Independent', 'Stacked']) {
        await press(layout);
        await settle(8);
        await shot(`export-${theme}-image-${layout.toLowerCase()}`);
      }
      await press('Legend shown');
      await press(`Axis of ${SIGNALS[1]}`);
      await settle(8);
      await shot(`export-${theme}-image-plot-axis`);
      await pressKey('Escape');
      await until(() => !document.querySelector('.export-dialog'));
    }
  });
});
