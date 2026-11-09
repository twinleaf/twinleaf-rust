// Every menu of the page has one look (round 1 "Menu look", round 2 Menus):
// rows that lead with an icon in one frame and type; a group named by an
// uppercase caption with a rule after it, no line before it; a trailing
// column that carries the row's value, in fixed-width digits and at least a
// column wide, or while the hints show its shortcut in the value's place;
// and one right edge for the shortcuts, the values and the arrows.
import { describe, expect, it } from 'vitest';
import { MENUS, closeAll, menuPage } from './browser/menus.js';
import { prefs } from '../src/lib/prefs.svelte.js';

/** Whether `el` shows: it and every element it is in are displayed. */
const shows = (el) => !!el && el.getClientRects().length > 0 && getComputedStyle(el).visibility !== 'hidden';
/** The rows of menu `menu`, its own and not a submenu's. */
const rowsOf = (menu) => [...menu.querySelectorAll('[role^=menuitem]')].filter((r) => r.closest('[data-slot$=-content]') === menu);
/** What a row ends with: its value, its shortcut or a submenu's arrow, whichever shows last in it. */
function trailing(r) {
  const last = [...r.children].filter(shows).at(-1);
  return last && (last.matches('.menu-value, [data-slot$=-shortcut]') || (last.tagName === 'svg' && last.matches('.lucide-chevron-right'))) ? last : null;
}

/** The look of menu `menu` that every menu shares, as a string to compare. */
function look(menu) {
  const s = getComputedStyle(menu);
  const items = rowsOf(menu).map((r) => getComputedStyle(r));
  const one = (list) => [...new Set(list)].join('|');
  return JSON.stringify({
    frame: [s.borderRadius, s.boxShadow, s.padding, s.backgroundColor],
    type: one(items.map((i) => `${i.fontSize}/${i.lineHeight}`)),
    room: one(items.map((i) => `${i.paddingTop} ${i.paddingRight} ${i.paddingBottom}`)),
  });
}

describe('every menu', () => {
  it('has the one look, its groups under a caption with a rule, its trailing column on one right edge, as the hints switch says', async () => {
    const page = await menuPage();
    const looks = new Map();
    let edges = 0, values = 0, shortcuts = 0;
    try {
      for (const hints of [false, true]) {
        prefs.setHints(hints);
        expect(document.documentElement.dataset.hints).toBe(hints ? 'on' : 'off');
        for (const [name, open] of Object.entries(MENUS)) {
          await closeAll();
          const menu = await open(page, page.ids[0]);
          expect(menu, `${name} opens`).toBeTruthy();
          const sub = menu.matches('[data-slot$=-sub-content]');
          looks.set(name, look(menu));
          // A top menu's rows lead with an icon, a submenu's lists choices.
          if (!sub) {
            expect(menu.classList.contains('menu-icons'), `${name} leads with icons`).toBe(true);
            for (const r of rowsOf(menu)) expect(r.querySelector(':scope > svg.menu-icon, :scope > .menu-icon'), `${name}: ${r.textContent.trim()} has its icon`).toBeTruthy();
          }
          // A group's caption: uppercase, a rule after it, and no line before it.
          for (const label of menu.querySelectorAll('[data-slot$=-menu-label]')) {
            const s = getComputedStyle(label), rule = getComputedStyle(label, '::after');
            expect(s.textTransform, `${name}: ${label.textContent}`).toBe('uppercase');
            expect(rule.content !== 'none' && parseFloat(rule.height) === 1 && parseFloat(rule.width) > 8, `${name}: ${label.textContent} has its rule`).toBe(true);
            expect(label.previousElementSibling?.matches('[data-slot$=-separator]') ?? false, `${name}: no line before ${label.textContent}`).toBe(false);
          }
          // The hints: shortcuts show only with them, and then in their value's place; a warning, and why a row cannot be chosen, keep their place.
          for (const r of rowsOf(menu)) {
            const value = r.querySelector('.menu-value'), shortcut = r.querySelector('[data-slot$=-shortcut]');
            const keeps = !!value && (value.classList.contains('warn') || r.hasAttribute('data-disabled'));
            if (shortcut) expect(shows(shortcut), `${name}: ${r.textContent.trim()}'s shortcut`).toBe(hints && !keeps);
            if (value) expect(shows(value), `${name}: ${r.textContent.trim()}'s value`).toBe(!hints || !shortcut || keeps);
            if (value && shows(value)) {
              const s = getComputedStyle(value);
              expect(s.fontVariantNumeric, `${name}: ${r.textContent.trim()}'s digits`).toContain('tabular-nums');
              expect(parseFloat(s.minWidth), `${name}: ${r.textContent.trim()}'s column`).toBeGreaterThanOrEqual(60);
              values++;
            }
            if (shortcut && shows(shortcut)) shortcuts++;
          }
          // One right edge: whatever a row ends with ends where the row's content does.
          const ends = [...rowsOf(menu), ...menu.querySelectorAll('[data-slot$=-menu-label]')].map((r) => {
            const t = r.matches('[data-slot$=-menu-label]') ? r.querySelector('.menu-value') : trailing(r);
            return t && [r, t];
          }).filter(Boolean);
          for (const [r, t] of ends) {
            const edge = r.getBoundingClientRect().right - parseFloat(getComputedStyle(r).paddingRight);
            expect(Math.abs(t.getBoundingClientRect().right - edge), `${name}: ${r.textContent.trim()} ends on the edge`).toBeLessThanOrEqual(1);
            edges++;
          }
        }
        await closeAll();
      }
      // One frame, type and room for every menu, its submenus too.
      const [first, ...rest] = [...looks];
      for (const [name, l] of rest) expect(l, `${name} looks as ${first[0]} does`).toBe(first[1]);
      expect(edges, 'rows measured').toBeGreaterThan(25);
      expect(values, 'values seen').toBeGreaterThan(3);
      expect(shortcuts, 'shortcuts seen').toBeGreaterThan(3);
    } finally {
      prefs.setHints(true);
      page.dispose();
    }
  }, 120_000);
});
