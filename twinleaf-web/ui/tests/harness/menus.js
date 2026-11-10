// The page's menus, each opened as a person opens it, on a page set up so
// every one of them has its rows: plots with signals, a legend, cursors,
// another tab and properties.
import { expect } from 'vitest';
import { commands } from '@vitest/browser/context';
import { centre, click, editCursors, feed, leafOf, mountPage, rightClick, settle, wait, withYPair } from './page.js';
import { plotIds } from '../../src/lib/layout.js';

export { OPEN, click, closeAll, leafOf, rightClick, settle } from './page.js';
export const SIGNALS = ['sine.sine', 'aux.triangle'];
/** The newest menu open, or the newest submenu with `sub`. */
export const openMenu = (sub = false) => [...document.querySelectorAll(sub ? '[data-slot$=-menu-sub-content]' : '[data-slot$=-menu-content]')].at(-1) ?? null;
/** The row of the open menus whose text starts with `label`. */
export const row = (label) => [...document.querySelectorAll('[role^=menuitem], [data-slot$=-menu-label]')].find((m) => m.textContent.trim().startsWith(label)) ?? null;

/**
 * The page: three plots in rows, the first two showing both signals and the
 * third one of them; the first independent, with the legend, its X and Y
 * cursors placed; another tab; and two properties of a device.
 */
export async function menuPage() {
  const page = await mountPage([SIGNALS, SIGNALS, [SIGNALS[0]]], 'rows', 1, SIGNALS, [
    { name: 'gain', channel: 'setting', value: 3 },
    { name: 'offset', channel: 'setting', value: 0.5 },
  ]);
  feed(page.hub);
  const ids = plotIds(page.ws.layout);
  page.history.group('Prepare', () => page.ws.setPlot(ids[0], { mode: 'independent', legend: true }));
  await settle();
  const pv = page.pv(ids[0]);
  editCursors(page, ids[0], (c) => withYPair({ ...c, x: { a: 14, b: 16 } }, 'time', pv.yAxes[0].key, 0.1, 0.5));
  // The legend and the readout show what a frame drew a few times a second.
  await wait(200);
  await settle();
  return { ...page, ids };
}

/** A spot of plot `id`'s plot area, in client px. */
export function plotArea(page, id) {
  const P = page.pv(id)?.geo?.plot;
  expect(P, `plot ${id} drawn`).toBeTruthy();
  // Its lower right, clear of the readout and the legend in their corners.
  return [P.x + P.w * 0.75, P.y + P.h * 0.75];
}

/** Each menu of the page, `open(page, id)` opening it on plot `id` where it is a plot's, and returning it. */
export const MENUS = {
  'the plot menu': async (page, id) => (await rightClick(plotArea(page, id)), openMenu()),
  'a chip’s menu': async (page, id) => (await rightClick(centre(leafOf(id).querySelector('.trace-chip .name'))), openMenu()),
  'a legend row’s menu': async (page) => (await rightClick(centre(leafOf(page.ids[0]).querySelector('.pinned-legend .row'))), openMenu()),
  'the readout’s menu': async (page) => (await rightClick(centre(leafOf(page.ids[0]).querySelector('.cursor-readout'))), openMenu()),
  'the add-signal menu': async (page, id) => {
    await commands.mouse([['move', ...centre(leafOf(id))]]);
    await click(centre(leafOf(id).querySelector('button[aria-label="Add a signal"]')));
    return openMenu();
  },
  'the Signals list’s menu': async () => (await rightClick(centre(document.querySelector('.palette .chip'))), openMenu()),
  'a tab’s menu': async () => (await rightClick(centre(document.querySelector('[role=tab]'))), openMenu()),
  'a dock panel’s menu': async () => (await rightClick(centre(document.querySelector('.dock-head'))), openMenu()),
  'the properties menu': async () => (await rightClick(centre(document.querySelector('tr[id^=prop-]'))), openMenu()),
  'the Layout dropdown': async () => (await click(centre([...document.querySelectorAll('button')].find((b) => b.textContent.trim() === 'Layout'))), openMenu()),
  'the File dropdown': async () => (await click(centre([...document.querySelectorAll('button')].find((b) => b.textContent.trim() === 'File'))), openMenu()),
  'the Kind submenu': async (page, id) => (await MENUS['the plot menu'](page, id), await click(centre(row('Kind'))), openMenu(true)),
  'the Move to tab submenu': async (page, id) => (await MENUS['the plot menu'](page, id), await click(centre(row('Move to tab'))), openMenu(true)),
  'the Move to submenu': async (page, id) => (await MENUS['a chip’s menu'](page, id), await click(centre(row('Move to'))), openMenu(true)),
  'the Axis submenu': async (page) => (await MENUS['a chip’s menu'](page, page.ids[0]), await click(centre(row('Axis'))), openMenu(true)),
};
