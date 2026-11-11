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
// - Save shows a bar while the files are made, Cancel stops it, and a file
//   made after the browser stops asking where waits for Save file….
// - Escape closes it, leaving nothing of its copies; focus goes back to the
//   plot. It fits a 900 × 600 window, scrolling inside itself, and every
//   control is reached by Tab.
// - Copy image, among the plot's header actions while the pointer is on
//   the plot, and on its menu, puts the plot's picture on the clipboard.
import { afterEach, describe, expect, it, vi } from 'vitest';
import { commands, page as browser, userEvent } from 'vitest/browser';
import fc from 'fast-check';
import { flushSync } from 'svelte';
import { centre, clickOn, closeAll, focusPage, frames, holdHistory, leafOf, mountPage, pickItem, plotMenu, pressKey, settle, until } from './harness/page.js';
import { plotIds } from '../src/lib/layout.js';
import { SAVE_KEY } from '../src/lib/tabs.js';
import { colors } from '../src/lib/colors.svelte.js';
import { prefs } from '../src/lib/prefs.svelte.js';
import { exportUrl } from '../src/lib/exportfile.js';

/** Where VITE_TL_SHOTS names a folder, relative to this file, the screenshots go there. */
const SHOTS = import.meta.env.VITE_TL_SHOTS;
const shot = async (name) => SHOTS && (await browser.screenshot({ path: `${SHOTS}/${name}.png` }));

const SIGNALS = ['sine.sine', 'aux.triangle', 'aux.saw'];
/** The mean difference of a channel, of 255, the figure shown and its file scaled to it may have. */
const LIKENESS = 6;

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

/** Has the browser's save and folder windows hand over what is saved, in place of a file, and returns it: each {name, blob}. */
function catchSaves() {
  const saved = [], kept = [window.showSaveFilePicker, window.showDirectoryPicker];
  const file = (name) => ({ name, createWritable: async () => ({ write: async (blob) => void saved.push({ name, blob }), close: async () => {} }) });
  window.showSaveFilePicker = vi.fn(async ({ suggestedName }) => file(suggestedName));
  window.showDirectoryPicker = vi.fn(async () => ({ getFileHandle: async (name) => file(name) }));
  restore.push(() => ([window.showSaveFilePicker, window.showDirectoryPicker] = kept));
  return saved;
}
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
  expect(await until(() => saved.length >= before + n, 20000), `${n} files saved`).toBe(true);
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
    const legend = async () => (await press('Plot 1'), await press('Shown'));
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
      { row: 'Legend', first: () => press('Plot 1'), change: () => press('Shown') },
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

  it('reorders a stacked plot’s legend and lanes as its signals are dragged', async () => {
    const [id] = await exportPage([SIGNALS]);
    const made = watchCopies();
    await openExport([id]);
    const copy = made.at(-1);
    await press('Image');
    await press('Plot 1');
    await press('Stacked');
    await press('Shown');
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
    await press('Shown');
    await drawn(copy);
    expect(await until(() => dialog().querySelector('.legend-handle')), 'a handle on the figure’s legend').toBe(true);
    const handle = dialog().querySelector('.legend-handle'), shown = dialog().querySelector('.stage-shown').getBoundingClientRect();
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

  it('shows a bar while the file is made, stops at Cancel, and waits for Save file… once the browser stops asking where', async () => {
    const [id] = await exportPage();
    const saved = catchSaves();
    let feed = null;
    const asked = catchExports((init) => {
      const body = new ReadableStream({ start: (c) => (feed = c) });
      init?.signal?.addEventListener('abort', () => feed.error(new DOMException('aborted', 'AbortError')));
      return new Response(body);
    });
    await openExport([id]);
    await press('Save data…');
    expect(await until(() => asked.length === 1 && feed), 'asked of the hub').toBe(true);
    const bar = () => dialog().querySelector('[role=progressbar]');
    expect(bar(), 'the bar').toBeTruthy();
    feed.enqueue(new Uint8Array(200_000));
    await settle();
    expect(await until(() => +bar()?.getAttribute('aria-valuenow') > 0), 'the bar moves as bytes come').toBe(true);
    expect(bar().parentElement.textContent, 'the bytes so far').toContain('kB');
    await shot('export-progress');
    await press('Cancel');
    expect(bar(), 'no bar once cancelled').toBe(null);
    expect(dialog(), 'the dialog stays').toBeTruthy();
    expect(control('Save data…'), 'Save again').toBeTruthy();
    expect(window.showSaveFilePicker, 'no save window').not.toHaveBeenCalled();

    // Made after the browser has stopped asking where: Save file… asks.
    Object.defineProperty(navigator, 'userActivation', { value: { isActive: false }, configurable: true });
    restore.push(() => delete navigator.userActivation);
    await press('Save data…');
    expect(await until(() => asked.length === 2 && feed), 'asked again').toBe(true);
    feed.enqueue(new TextEncoder().encode('time,v\n1,2\n'));
    feed.close();
    expect(await until(() => control('Save file…')), 'Save file…').toBe(true);
    expect(window.showSaveFilePicker, 'no save window before it').not.toHaveBeenCalled();
    await shot('export-save-file');
    await press('Save file…');
    expect(await until(() => saved.length === 1), 'saved').toBe(true);
    expect(await saved[0].blob.text(), 'the file').toBe('time,v\n1,2\n');
    expect(await until(() => !document.querySelector('.export-dialog')), 'closed').toBe(true);
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
      await press('Shown');
      await press(`Axis of ${SIGNALS[1]}`);
      await settle(8);
      await shot(`export-${theme}-image-plot-axis`);
      await pressKey('Escape');
      await until(() => !document.querySelector('.export-dialog'));
    }
  });
});
