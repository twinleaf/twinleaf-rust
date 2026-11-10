// What the menus' rows say in their trailing column, of what each row's
// command acts on: a count over the plots it acts on, a setting they share,
// or 'Mixed' where they differ.

/** `n` things, named `one`, or `many` when they are not one. */
export const counted = (n, one, many = `${one}s`) => `${n.toLocaleString()} ${n === 1 ? one : many}`;

/** The one value of `values`, 'Mixed' when they differ, or null when there are none. */
export const common = (values) => (!values.length ? null : values.every((v) => v === values[0]) ? values[0] : 'Mixed');

/** What a row's `<kbd>` says for a key that is not its own name. */
const KBD = { '⇧': 'shift', ctrl: 'control' };
/**
 * A menu's keydown, seen before the menu's own: a key one of its rows shows
 * runs that row, so the key does with the menu open what it does with it
 * closed. Enter stays the highlighted row's while one is highlighted, and a
 * field in the menu keeps what is typed in it.
 */
export function runShownKey(e) {
  if (e.target.closest?.('input, textarea') || e.altKey || e.metaKey) return;
  if (e.key === 'Enter' && e.currentTarget.querySelector('[data-highlighted]')) return;
  const key = e.key.toLowerCase(), named = key.length > 1;
  for (const row of e.currentTarget.querySelectorAll('[role^=menuitem]:not([data-disabled])')) {
    const keys = [...row.querySelectorAll('kbd')].map((k) => k.textContent.trim().toLowerCase()).map((k) => KBD[k] ?? k);
    if (!keys.length || keys.at(-1) !== key) continue;
    // A key with a name takes the Shift its row shows; a character is typed however its keyboard types it.
    if (named && keys.includes('shift') !== e.shiftKey) continue;
    if (keys.includes('control') !== e.ctrlKey) continue;
    e.preventDefault();
    e.stopPropagation();
    row.click();
    return;
  }
}
