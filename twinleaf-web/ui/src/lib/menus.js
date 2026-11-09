// What the menus' rows say in their trailing column, of what each row's
// command acts on: a count over the plots it acts on, a setting they share,
// or 'Mixed' where they differ. And one menu or popover open at a time, as a
// desktop's context menus are: opening one closes whichever was open, on any
// part of the page.

/** `n` things, named `one`, or `many` when they are not one. */
export const counted = (n, one, many = `${one}s`) => `${n.toLocaleString()} ${n === 1 ? one : many}`;

/** The one value of `values`, 'Mixed' when they differ, or null when there are none. */
export const common = (values) => (!values.length ? null : values.every((v) => v === values[0]) ? values[0] : 'Mixed');

let closeOpen = null;

/** Notes that the menu `close` closes is open, closing any other first. */
export function claimMenu(close) {
  if (closeOpen && closeOpen !== close) closeOpen();
  closeOpen = close;
}

/** Notes that the menu `close` closes has closed. */
export function releaseMenu(close) {
  if (closeOpen === close) closeOpen = null;
}

/**
 * Props for a menu's or popover's content: a press outside it, which closes
 * it, leaves focus where the press put it instead of handing it back to the
 * menu's button, so a press on another menu's button opens that menu at
 * once. Closing it from the keys still hands focus back.
 */
export function pressOutsideKeepsFocus() {
  let pressed = false;
  return {
    onInteractOutside: () => (pressed = true),
    onCloseAutoFocus: (e) => {
      if (pressed) e.preventDefault();
      pressed = false;
    },
  };
}
