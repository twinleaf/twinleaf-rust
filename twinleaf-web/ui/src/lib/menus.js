// One menu or popover open at a time, as a desktop's context menus are:
// opening one closes whichever was open, on any part of the page.

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
