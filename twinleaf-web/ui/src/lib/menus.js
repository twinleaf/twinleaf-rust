// What the menus' rows say in their trailing column, of what each row's
// command acts on: a count over the plots it acts on, a setting they share,
// or 'Mixed' where they differ.

/** `n` things, named `one`, or `many` when they are not one. */
export const counted = (n, one, many = `${one}s`) => `${n.toLocaleString()} ${n === 1 ? one : many}`;

/** The one value of `values`, 'Mixed' when they differ, or null when there are none. */
export const common = (values) => (!values.length ? null : values.every((v) => v === values[0]) ? values[0] : 'Mixed');

/** What a row's `<kbd>` says for a key that is not its own name. */
const KBD = { '⇧': 'shift', ctrl: 'control' };
/** What a menu is on the page, a submenu too. */
const MENU = '[data-slot$=-menu-content], [data-slot$=-menu-sub-content]';
/**
 * Key `e`, pressed on `at` in an open menu, before the menu hears it: a key
 * one of the menu's rows shows runs that row, so the key does with the menu
 * open what it does with it closed, and this says whether one ran. A
 * submenu inside a menu comes after it. Enter stays the highlighted row's
 * while one is highlighted, and a field in the menu keeps what is typed in
 * it.
 */
export function runShownKey(e, at) {
  if (!(at instanceof Element) || at.closest('input, textarea') || e.altKey || e.metaKey) return false;
  const menus = [];
  for (let m = at.closest(MENU); m; m = m.parentElement?.closest(MENU)) menus.unshift(m);
  const key = e.key.toLowerCase(), named = key.length > 1;
  for (const menu of menus) {
    if (e.key === 'Enter' && menu.querySelector('[data-highlighted]')) continue;
    const rows = [...menu.querySelectorAll('[role^=menuitem]')]
      .map((row) => [row, [...row.querySelectorAll('kbd')].map((k) => k.textContent.trim().toLowerCase()).map((k) => KBD[k] ?? k)])
      .filter(([, keys]) => keys.length && keys.at(-1) === key && keys.includes('control') === e.ctrlKey);
    // A row takes the Shift it shows. A character's row that shows none takes it with Shift too, as a keyboard types some characters with Shift, unless another row shows it with Shift.
    const [row] = rows.find(([, keys]) => keys.includes('shift') === e.shiftKey) ?? (named ? null : rows.find(([, keys]) => !keys.includes('shift'))) ?? [];
    if (row && !row.hasAttribute('data-disabled')) {
      row.click();
      return true;
    }
  }
  return false;
}
