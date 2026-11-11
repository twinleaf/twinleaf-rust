// The page's one focus ring, on the whole page: every thing that can take
// focus, found on the page and in each menu, popover and dialog it opens,
// shows the same ring or none, and draws none of its own.
// - The ring is FocusRing.svelte's: 2 px wide, in the ring colour, 3 px
//   outside what it surrounds, its corners that thing's own made 3 px
//   larger; cut only by the window's edge or a box that scrolls.
// - Focus from the keyboard shows it around the element, or around the
//   field's box for a field inside one.
// - A list on the page that keeps focus on itself shows it around its
//   current item.
// - While a menu is open, it shows around the item the menu was opened
//   for, where that is one of a list's items, and nowhere else.
// - What shows none (NONE) says why.
// - Nothing changes its own outline, shadow or border for focus.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { flushSync } from 'svelte';
import { MENUS, closeAll, menuPage } from './harness/menus.js';
import { clickOn, focusPage, frames, pressKey, quietApi, settle, until } from './harness/page.js';

beforeEach(() => quietApi());
afterEach(() => vi.restoreAllMocks());

/** What can take focus, by the keys or a press. */
const FOCUSABLE = 'button, input, select, textarea, a[href], [tabindex]';
/** What shows no ring when it has focus, and why. */
const NONE = {
  '.plot-card.selected': 'a selected plot’s border marks it, and focus selects a plot',
  '.plot-body': 'the plot is the stop the keys reach; its drawing takes focus only to open its menu',
  '[role=option]': 'its list shows it as the current item',
  'input[role=combobox], [data-slot=command-input]': 'a filter field: the list it chooses from highlights its row',
  '[role=menu], [role=menu] *': 'a menu highlights its rows',
  '[role=dialog], [role=alertdialog], [data-slot$=-content]': 'a layer’s own box, which holds focus until something in it takes it',
};
/** What a ring goes around in place of the field inside it. */
const BOX = '[data-slot=input-group], .window-field';
const OUT = 3, WIDTH = '2px';

/** Every element of `root` that can take focus and shows. */
function places(root = document) {
  return [...root.querySelectorAll(FOCUSABLE)].filter((el) => !el.disabled && !el.closest('[inert], [aria-hidden=true], [data-sonner-toaster]') && el.getClientRects().length > 0 && getComputedStyle(el).visibility !== 'hidden');
}
const named = (el) => `${el.tagName.toLowerCase()}${el.className && typeof el.className === 'string' ? `.${el.className.trim().split(/\s+/)[0]}` : ''}[${(el.getAttribute('aria-label') ?? el.getAttribute('role') ?? el.textContent.trim()).slice(0, 24)}]`;

/** The ring colour, as the browser computes it. */
function ringColour() {
  const probe = document.body.appendChild(document.createElement('div'));
  probe.style.borderTop = '1px solid var(--ring)';
  const colour = getComputedStyle(probe).borderTopColor;
  probe.remove();
  return colour;
}
/** What `el` draws of its own that could stand for focus: its outline, its shadow and its border’s colour. */
function own(el) {
  const cs = getComputedStyle(el);
  return { outline: cs.outlineStyle !== 'none' && parseFloat(cs.outlineWidth) > 0 ? `${cs.outlineWidth} ${cs.outlineStyle}` : 'none', shadow: cs.boxShadow, border: cs.borderTopColor };
}
/** The rings on the page, each {rect, width, colour, radius}. */
const rings = () =>
  [...document.querySelectorAll('.focus-ring')].map((el) => {
    const cs = getComputedStyle(el);
    return { rect: el.getBoundingClientRect(), width: cs.borderTopWidth, colour: cs.borderTopColor, radius: parseFloat(cs.borderTopLeftRadius) };
  });
/** The sides of `el` the window’s edge or a box that scrolls or clips it cuts a ring on. */
function cut(el) {
  const t = el.getBoundingClientRect();
  const sides = { left: t.left < OUT, top: t.top < OUT, right: innerWidth - t.right < OUT, bottom: innerHeight - t.bottom < OUT };
  for (let a = el.parentElement; a && a !== document.body; a = a.parentElement) {
    const cs = getComputedStyle(a);
    if (cs.overflowX === 'visible' && cs.overflowY === 'visible') continue;
    const c = a.getBoundingClientRect();
    if (t.left < c.left) sides.left = true;
    if (t.top < c.top) sides.top = true;
    if (t.right > c.right) sides.right = true;
    if (t.bottom > c.bottom) sides.bottom = true;
  }
  return sides;
}
/** Checks that the page shows one ring, the one look, around `target`. */
function expectRing(target, said, colour) {
  const all = rings();
  expect(all.length, `${said}: one ring`).toBe(1);
  const [r] = all, t = target.getBoundingClientRect(), sides = cut(target);
  expect([r.width, r.colour], `${said}: its width and colour`).toEqual([WIDTH, colour]);
  // A pill's corners are a radius too large to add to exactly: both are rounder than either box.
  const corner = parseFloat(getComputedStyle(target).borderTopLeftRadius) || 0, round = Math.max(t.width, t.height);
  if (corner > round) expect(r.radius, `${said}: its corners`).toBeGreaterThan(round);
  else expect(r.radius, `${said}: its corners`).toBeCloseTo(corner + OUT, 1);
  const d = { left: t.left - r.rect.left, top: t.top - r.rect.top, right: r.rect.right - t.right, bottom: r.rect.bottom - t.bottom };
  for (const [side, by] of Object.entries(d)) {
    if (sides[side]) expect(by, `${said}: its ${side}, cut`).toBeLessThanOrEqual(OUT + 0.05);
    else expect(by, `${said}: its ${side}`).toBeCloseTo(OUT, 1);
  }
}

/**
 * Gives each focusable thing of `root` focus from the keyboard, and checks
 * what it shows. Returns how many showed the ring, and how many none.
 */
async function walk(root, where, colour) {
  let ringed = 0, bare = 0;
  for (const el of places(root)) {
    if (!el.isConnected || !places(root).includes(el)) continue;
    document.activeElement?.blur?.();
    await frames(1);
    const rest = own(el), said = `${named(el)} in ${where}`;
    // A key pressed first, so the focus that follows is the keyboard's.
    await pressKey('Shift');
    el.focus();
    flushSync();
    await frames(2);
    if (document.activeElement !== el) continue;
    // Nothing of its own: no outline, and no shadow or border that focus changes; a plot's border is its selection's.
    const now = own(el);
    expect(now.outline, `${said}: an outline of its own`).toBe('none');
    expect(now.shadow, `${said}: a shadow of its own`).toBe(rest.shadow);
    if (!el.matches('.plot-card')) expect(now.border, `${said}: a border of its own`).toBe(rest.border);
    const current = el.getAttribute('aria-activedescendant');
    const why = Object.keys(NONE).find((sel) => el.matches(sel));
    if (why) {
      expect(rings().length, `${said} shows no ring: ${NONE[why]}`).toBe(0);
      bare++;
    } else {
      expectRing(current !== null ? document.getElementById(current) : (el.closest(BOX) ?? el), said, colour);
      ringed++;
    }
  }
  return { ringed, bare };
}

describe('the focus ring', () => {
  it('is the one ring on every focusable thing of the page, its menus, its popovers and its dialogs, or none where the rule says none', async () => {
    const page = await menuPage();
    const colour = ringColour(), seen = {};
    try {
      await focusPage();
      seen.page = await walk(document, 'the page', colour);
      // Each popover a control of the page opens, found by its trigger: one of each kind.
      const kinds = new Map([...document.querySelectorAll('[aria-haspopup=dialog]')].filter((t) => places().includes(t)).map((t) => [(t.getAttribute('aria-label') ?? t.textContent).trim().split(/\s+/).slice(0, 2).join(' '), t]));
      expect(kinds.size).toBeGreaterThan(1);
      seen.popovers = { ringed: 0, bare: 0 };
      for (const trigger of kinds.values()) {
        await focusPage();
        await clickOn(trigger);
        if (!(await until(() => document.querySelector('[data-slot=popover-content][data-state=open]'), 1500))) continue;
        await settle();
        const got = await walk(document.querySelector('[data-slot=popover-content][data-state=open]'), `the popover of ${named(trigger)}`, colour);
        seen.popovers.ringed += got.ringed;
        seen.popovers.bare += got.bare;
        await closeAll();
      }
      // The dialogs the keys open.
      seen.dialogs = { ringed: 0, bare: 0 };
      for (const key of ['Control+k', '?']) {
        await focusPage();
        await pressKey(key);
        await until(() => document.querySelector('[role=dialog]'), 1500);
        await settle();
        const got = await walk(document.querySelector('[role=dialog]'), `the dialog ${key} opens`, colour);
        seen.dialogs.ringed += got.ringed;
        seen.dialogs.bare += got.bare;
        await pressKey('Escape');
        await until(() => !document.querySelector('[role=dialog]'), 1500);
      }
      // Each menu: its rows are highlighted, and the ring is around the item it was opened for, if a list marks one.
      seen.menus = { ringed: 0, bare: 0 };
      for (const [name, open] of Object.entries(MENUS)) {
        await closeAll();
        await focusPage();
        const menu = await open(page, page.ids[0]);
        expect(menu, `${name} opens`).toBeTruthy();
        await settle();
        const item = document.querySelector('[data-menu-for]');
        for (const row of places(menu)) {
          const rest = own(row);
          row.focus();
          flushSync();
          await frames(2);
          if (document.activeElement !== row) continue;
          expect(own(row), `a row of ${name}: nothing of its own`).toEqual(rest);
          if (item) expectRing(item, `the item ${name} is open for`, colour);
          else expect(rings().length, `${name}, open for no item of a list, shows no ring`).toBe(0);
          item ? seen.menus.ringed++ : seen.menus.bare++;
        }
        expect(!!item, `${name} marks the item it is open for`).toBe(['a chip’s menu', 'the Signals list’s menu', 'the Move to submenu', 'the Axis submenu'].includes(name));
      }
      await closeAll();
    } finally {
      page.dispose();
    }
    // Most of the page shows the ring, and each kind of place was walked.
    expect(seen.page.ringed).toBeGreaterThan(40);
    expect(seen.page.ringed).toBeGreaterThan(seen.page.bare * 2);
    expect(seen.popovers.ringed).toBeGreaterThan(3);
    expect(seen.dialogs.ringed + seen.dialogs.bare).toBeGreaterThan(1);
    expect(seen.menus.ringed).toBeGreaterThan(3);
    expect(seen.menus.bare).toBeGreaterThan(10);
  }, 600_000);

  it('is around the current item of each list on the page that keeps focus on itself, as the arrows move it', async () => {
    const page = await menuPage();
    const colour = ringColour();
    let lists = 0;
    try {
      await focusPage();
      // Found on the page: what moves among its own items and names the one it is on.
      for (const list of places().filter((el) => el.matches('[data-focus-own]') && !el.matches('[role=tablist]'))) {
        await pressKey('Shift');
        list.focus();
        for (const key of ['ArrowDown', 'ArrowRight', 'ArrowDown']) {
          await pressKey(key);
          flushSync();
          await frames(2);
          const current = document.getElementById(list.getAttribute('aria-activedescendant') ?? '');
          if (!current) continue;
          expect(own(current).outline, `${named(current)}: an outline of its own`).toBe('none');
          expectRing(current, `the current item of ${named(list)} after ${key}`, colour);
          lists++;
        }
      }
    } finally {
      page.dispose();
    }
    expect(lists).toBeGreaterThan(5);
  }, 600_000);
});
