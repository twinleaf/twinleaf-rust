// A plot's cursors: X1 and X2 along its x axis and Y1 and Y2 along each y
// axis. They are part of the tab (tabs.js), kept on the plot as `cursors`:
//
//   {x: {a, b}, f: {a, b}, c: {a, b}, y: {<kind>: {<axis key>: {a, b}}}}
//
// `x` is a time plot's X1 and X2, in device time, `f` a spectrum's, in Hz,
// and `c` a capture plot's, in its captures' x units;
// `y` holds Y1 and Y2 of each y axis, by the plot's kind and the axis's
// range key (plot.js `rangeKey`), in the units the axis shows. `a` is
// cursor 1 and `b` cursor 2, a number or null when not placed. A part with
// nothing placed is left out, and a plot with no cursor has no `cursors`,
// so two plots with the same cursors read the same. Each kind keeps its
// own, so a plot switched to the other kind and back finds them.
//
// A plot whose view follows live has no `x`, whatever its kind: time X
// cursors mark moments, which a live plot scrolls away. Placing one pauses
// the plot; going live, by any route, takes them away, by an edit made with
// it where a person went live; Back, or an undo, into the paused view brings
// back the pause and the cursors it had (viewhistory.svelte.js keeps this);
// and a tab read from storage or a file has none, but on a time plot kept
// paused, which loads paused (tabs.js), and loses them once loaded if the
// hub no longer holds what it shows (`unloaded`). Y cursors, a spectrum's
// `f` and a capture plot's `c` stay as they are.
//
// The values are never changed in place: each change here gives a new one.
// CursorEdits is the one writer of a plot's `cursors`: every change is an
// edit, one undo step, however many plots it changes. A drag shows what it
// moves as a draft over the plots it acts on, and writes once, when it
// ends.

import { counted } from './menus.js';

const NONE = Object.freeze({ a: null, b: null });
/** The field of X1 and X2 on a plot of each kind. */
const X_FIELDS = { time: 'x', spectrum: 'f', capture: 'c' };
const KINDS = Object.keys(X_FIELDS);
const xField = (kind) => X_FIELDS[kind] ?? 'x';
const some = (pair) => pair.a !== null || pair.b !== null;

/** X1 and X2, as {a, b}, of `cursors` on a plot of `kind`. */
export const xOf = (cursors, kind) => cursors?.[xField(kind)] ?? NONE;
/** Y1 and Y2, as {a, b}, of `cursors` on y axis `key` of a plot of `kind`. */
export const yOf = (cursors, kind, key) => cursors?.y?.[kind]?.[key] ?? NONE;

/** `cursors` with every part that has nothing placed left out, or undefined when that is all of it. */
function tidy(cursors) {
  const out = {};
  for (const f of Object.values(X_FIELDS)) if (cursors[f] && some(cursors[f])) out[f] = cursors[f];
  const y = {};
  for (const kind of KINDS) {
    const axes = Object.entries(cursors.y?.[kind] ?? {}).filter(([, pair]) => some(pair));
    if (axes.length) y[kind] = Object.fromEntries(axes);
  }
  if (Object.keys(y).length) out.y = y;
  return Object.keys(out).length ? out : undefined;
}

/** `cursors` with X cursor `name` ('a' or 'b') of a plot of `kind` at `value`, or removed with null. */
export const withX = (cursors, kind, name, value) => tidy({ ...cursors, [xField(kind)]: { ...xOf(cursors, kind), [name]: value } });
/** `cursors` with Y cursor `name` of y axis `key` of a plot of `kind` at `value`, or removed with null. */
export const withY = (cursors, kind, key, name, value) =>
  tidy({ ...cursors, y: { ...cursors?.y, [kind]: { ...cursors?.y?.[kind], [key]: { ...yOf(cursors, kind, key), [name]: value } } } });
/** `cursors` without X1 and X2 of a plot of `kind`. */
export const withoutX = (cursors, kind) => tidy({ ...cursors, [xField(kind)]: undefined });
/** `cursors` with X1 and X2 of a plot of `kind` as `pair`, {a, b}. */
export const withXPair = (cursors, kind, pair) => tidy({ ...cursors, [xField(kind)]: pair });
/** Whether `cursors` hold a time X cursor: one of a time plot's, or kept by a spectrum for when it is one again. */
export const hasTimeX = (cursors) => some(xOf(cursors, 'time'));
/** `cursors` without the Y cursors of y axis `key` of a plot of `kind`, or with no key, of its every y axis. */
export function withoutY(cursors, kind, key) {
  const { [key]: _, ...rest } = cursors?.y?.[kind] ?? {};
  return tidy({ ...cursors, y: { ...cursors?.y, [kind]: key === undefined ? undefined : rest } });
}
/** How many of `cursors` a plot of `kind` has placed: its X1 and X2, and Y1 and Y2 of each y axis of either kind. */
export function placedIn(cursors, kind) {
  const count = (pair) => (pair.a !== null) + (pair.b !== null);
  return count(xOf(cursors, kind)) + KINDS.flatMap((k) => Object.values(cursors?.y?.[k] ?? {})).reduce((n, pair) => n + count(pair), 0);
}

/** The cursors saved or imported JSON `v` describes: what of it holds together, or undefined for none. */
export function cursorsFromJson(v) {
  const object = (o) => !!o && typeof o === 'object' && !Array.isArray(o);
  if (!object(v)) return undefined;
  const at = (n) => (typeof n === 'number' && isFinite(n) ? n : null);
  const pair = (p) => (object(p) ? { a: at(p.a), b: at(p.b) } : NONE);
  const y = {};
  for (const kind of KINDS) if (object(v.y?.[kind])) y[kind] = Object.fromEntries(Object.entries(v.y[kind]).map(([key, p]) => [key, pair(p)]));
  return tidy({ ...Object.fromEntries(Object.values(X_FIELDS).map((f) => [f, pair(v[f])])), y });
}

export class CursorEdits {
  /** What a drag under way shows in place of the tab's cursors, by plot id. */
  drafts = new Map();
  /** The X cursor an edit here placed or moved last on each plot, 'a' or 'b', by plot id; X1 where an edit placed or moved both. */
  #movedX = new Map();

  /** `ws` is the page's Workspace and `history` its History. */
  constructor(ws, history) {
    this.ws = ws;
    this.history = history;
  }

  /** The cursors the tab keeps of plot `id` of the open tab. */
  saved(id) {
    return this.ws.plots[id]?.cursors;
  }

  /** The cursors plot `id` of the open tab shows: a drag's draft of them while it moves one, else the tab's. */
  get(id) {
    return this.drafts.has(id) ? this.drafts.get(id) : this.saved(id);
  }

  /** The X cursor of plot `id` placed or moved last, 'a' or 'b', where it is still placed; else the one placed, X1 first; else 'a'. */
  lastX(id) {
    const kind = this.ws.plots[id]?.kind, pair = xOf(this.get(id), kind), moved = this.#movedX.get(id);
    return moved && pair[moved] !== null ? moved : pair.a === null && pair.b !== null ? 'b' : 'a';
  }

  /** Shows `cursors` on plot `id` until the drag that moves them ends. */
  draft(id, cursors) {
    this.drafts.set(id, cursors);
  }

  /** Ends a drag: writes what it drafted as one step named `what`, and says whether that changed anything. */
  commit(what) {
    const drafts = new Map(this.drafts);
    this.drafts.clear();
    return this.set(what, drafts);
  }

  /** Ends a drag given up: the plots show the tab's cursors again. */
  cancel() {
    this.drafts.clear();
  }

  /**
   * Gives each plot of `byId`, a Map of plot id to cursors, those cursors,
   * as one step named `what`, with how many plots it changed when several.
   * Says whether it changed any.
   */
  set(what, byId) {
    const changed = [...byId].filter(([id, c]) => this.ws.plots[id] && JSON.stringify(c ?? null) !== JSON.stringify(this.ws.plots[id].cursors ?? null));
    if (!changed.length) return false;
    for (const [id, c] of changed) {
      const kind = this.ws.plots[id].kind, was = xOf(this.ws.plots[id].cursors, kind), now = xOf(c, kind);
      const moved = now.a !== null && now.a !== was.a ? 'a' : now.b !== null && now.b !== was.b ? 'b' : null;
      if (moved) this.#movedX.set(id, moved);
    }
    this.history.group(changed.length > 1 ? `${what} (${counted(changed.length, 'plot')})` : what, () => {
      for (const [id, c] of changed) this.ws.setPlot(id, { cursors: c });
    });
    return true;
  }

  /** Removes every cursor of plots `ids`, as one step, and says whether any were placed. */
  clear(ids) {
    return this.set('Clear cursors', new Map(ids.map((id) => [id, undefined])));
  }

  /** Plots `ids` follow live: their time X cursors, which would scroll away, are removed, as one step. */
  followed(ids) {
    return this.set('Follow live', new Map(ids.map((id) => [id, withoutX(this.ws.plots[id]?.cursors, 'time')])));
  }

  /**
   * Plots `ids`, of any tab, loaded paused at a view the hub no longer
   * holds, follow live: their time X cursors go as the tabs end loading,
   * which is no edit, from the tabs and from every step undo and redo keep
   * (History `amend`), so neither brings them back.
   */
  unloaded(ids) {
    const gone = new Set(ids), off = (p) => (gone.has(p.id) && p.cursors ? { ...p, cursors: withoutX(p.cursors, 'time') } : p);
    this.history.amend((tabs) => tabs.map((t) => (Object.keys(t.plots).some((id) => gone.has(id)) ? { ...t, plots: Object.fromEntries(Object.entries(t.plots).map(([id, p]) => [id, off(p)])) } : t)));
  }
}
