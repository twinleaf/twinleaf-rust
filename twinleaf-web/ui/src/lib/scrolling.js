// What scrolls now, for the look of its scroll bars (plot.css): a dock's
// frame is marked `data-scrolling` while the dock or a list in it scrolls,
// by the wheel, a drag of the bar or the keys, and for SCROLL_REST_MS after
// its last move.
import * as clock from './clock.js';

/** How long after its last scroll a dock still counts as scrolling, in ms. */
export const SCROLL_REST_MS = 800;
/** What is marked while something in it scrolls. */
const HOST = '.dock-frame';
/** The timer that ends each host's scrolling. */
const resting = new WeakMap();

/** A scroll of `e`'s target, heard on its way down, as a scroll does not bubble. */
function scrolled(e) {
  const host = e.target instanceof Element ? e.target.closest(HOST) : null;
  if (!host) return;
  host.dataset.scrolling = '';
  clock.cancel(resting.get(host));
  resting.set(host, clock.after(SCROLL_REST_MS, () => delete host.dataset.scrolling));
}
if (typeof window !== 'undefined') window.addEventListener('scroll', scrolled, true);
