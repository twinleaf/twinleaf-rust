// The docks beside the plots: a left and a right column of panels, each
// open or folded. A panel moves between and within the docks, by its
// header or its menu, and never onto the tiles, which hold plots alone. A
// dock hides whole, to give the tiles its width, and keeps its panels.
//
// Where the panels are is one record, a place for each panel by its id: its
// side, its rank among that side's panels, and whether it is open. A panel
// has one place, so it is in one dock, and a dock's panels are read from
// the places: no dock keeps a list of its own that could disagree. A move
// gives one panel a new place; nothing writes a dock's panels as a list.
//
// While a panel is dragged the arrangement stands: nothing moves, folds,
// hides or shows until the panel is let go, and the drop is the next
// write. So a dock is, for the whole of a drag, what it was as it began.
//
// The arrangement is kept under KEY, as each side's panels in order. A
// saved one keeps the panels this page knows, once each, and a panel it
// lacks joins its default dock, so a page that adds a panel shows it to
// someone who arranged the old ones.

/** The localStorage key of the arrangement. */
export const KEY = 'tl-docks-v1';

/**
 * Every panel, by id. `channels` names the RPC channels a Properties panel
 * shows (null for entries with none). A panel is as tall as what it shows
 * while its dock has the room. Where the panels do not fit, a panel taller
 * than the share of the dock's height that plot.css gives every panel gives
 * way, down to that share, its list scrolling inside it, and the dock
 * scrolls the panels that still do not fit. So a list hides rows only where
 * its dock has no room left, and only a panel past its share, of which a
 * dock has one at most, changes height with the other panels.
 */
export const PANELS = {
  devices: { title: 'Devices' },
  signals: { title: 'Signals' },
  settings: { title: 'Settings', channels: ['setting'] },
  reports: { title: 'Reports', channels: ['report'] },
  actions: { title: 'Actions', channels: ['action'] },
  diagnostics: { title: 'Diagnostics', channels: ['diagnostic'] },
  other: { title: 'Other', channels: [null] },
  favorites: { title: 'Favorites', channels: ['favorite'] },
};

export const SIDES = ['left', 'right'];

/** Where each panel starts: its side, in order, and whether it is open. */
export const DEFAULT = {
  left: [
    { id: 'devices', open: true },
    { id: 'signals', open: true },
  ],
  right: [
    { id: 'favorites', open: true },
    { id: 'settings', open: true },
    { id: 'reports', open: true },
    { id: 'actions', open: true },
    { id: 'diagnostics', open: false },
    { id: 'other', open: false },
  ],
};

/**
 * The arrangement `saved` holds, made whole, as {places, hidden}: `places`
 * a place {side, rank, open} for every panel by id, and `hidden` whether
 * each side is hidden. A panel `saved` names more than once is where it
 * names it first, one it does not know is left out, and one it lacks is
 * last on its default side.
 */
export function normalize(saved) {
  const places = {};
  let rank = 0;
  const place = (side, p) => {
    if (PANELS[p?.id] && !places[p.id]) places[p.id] = { side, rank: rank++, open: p.open !== false };
  };
  for (const side of SIDES) for (const p of Array.isArray(saved?.[side]) ? saved[side] : []) place(side, p);
  for (const side of SIDES) for (const p of DEFAULT[side]) place(side, p);
  return { places, hidden: { left: saved?.hidden?.left === true, right: saved?.hidden?.right === true } };
}

/** The panels `places` has on `side`, each {id, open}, in order. */
export function panelsOn(places, side) {
  return Object.keys(places)
    .filter((id) => places[id].side === side)
    .sort((a, b) => places[a].rank - places[b].rank)
    .map((id) => ({ id, open: places[id].open }));
}

/** `places` with panel `id` at `index` of `side`'s panels, the others of that side around it in the order they had. */
export function move(places, id, side, index) {
  if (!places[id]) return places;
  const ids = panelsOn(places, side).map((p) => p.id).filter((other) => other !== id);
  ids.splice(Math.max(0, Math.min(index, ids.length)), 0, id);
  const out = { ...places, [id]: { ...places[id], side } };
  ids.forEach((other, rank) => (out[other] = { ...out[other], rank }));
  return out;
}

export class Docks {
  /** Where every panel is, by id: {side, rank, open}. */
  #places = $state.raw({});
  /** Whether each dock is hidden, by side. */
  #hidden = $state.raw({ left: false, right: false });
  #dragging = $state(false);
  /** The left dock's panels, each {id, open}, in order. */
  left = $derived(panelsOn(this.#places, 'left'));
  /** The right dock's panels, each {id, open}, in order. */
  right = $derived(panelsOn(this.#places, 'right'));

  /** `storage` is localStorage, or anything with its getItem and setItem. */
  constructor(storage) {
    this.storage = storage;
    let saved = null;
    try {
      saved = JSON.parse(storage?.getItem(KEY) ?? 'null');
    } catch {}
    this.#arrange(normalize(saved), false);
  }

  /** Whether each dock is hidden, by side. */
  get hidden() {
    return this.#hidden;
  }

  /** Whether a panel is being dragged: each dock then shows where it would go, an empty one too. */
  get dragging() {
    return this.#dragging;
  }

  /**
   * The one write of the arrangement: every panel's place, `places`, and
   * whether each dock is hidden, `hidden`, each as it was unless given. It
   * saves the arrangement unless `save` is false, as for one just read.
   * While a panel is dragged it does nothing.
   */
  #arrange({ places = this.#places, hidden = this.#hidden }, save = true) {
    if (this.#dragging) return;
    this.#places = places;
    this.#hidden = hidden;
    if (!save) return;
    try {
      this.storage?.setItem(KEY, JSON.stringify({ left: this.left, right: this.right, hidden }));
    } catch {}
  }

  /** Whether `side` shows its panels: it has some, and is not hidden. */
  shown(side) {
    return this[side].length > 0 && !this.#hidden[side];
  }

  /** Hides `side`, or shows it, by `hidden`. */
  hide(side, hidden) {
    this.#arrange({ hidden: { ...this.#hidden, [side]: hidden } });
  }

  /** The side panel `id` is on. */
  sideOf(id) {
    return this.#places[id]?.side;
  }

  /** Moves panel `id` to `index` of `side`'s panels, and shows that dock. */
  moveTo(id, side, index) {
    this.#arrange({ places: move(this.#places, id, side, index), hidden: { ...this.#hidden, [side]: false } });
  }

  /** Moves panel `id` by `step` places within its side. */
  step(id, step) {
    const side = this.sideOf(id);
    this.moveTo(id, side, this[side].findIndex((p) => p.id === id) + step);
  }

  /** Folds panel `id`, or unfolds it. */
  toggle(id) {
    const at = this.#places[id];
    this.#arrange({ places: { ...this.#places, [id]: { ...at, open: !at.open } } });
  }

  /** Puts every panel back where it starts, and shows both docks. */
  reset() {
    this.#arrange(normalize(null));
  }

  /** A drag of a panel begins: the arrangement stands until it ends. */
  startDrag() {
    this.#dragging = true;
  }

  /** The drag ends with panel `id` let go at `index` of `side`'s panels: it moves there, and that dock shows. */
  drop(id, side, index) {
    this.#dragging = false;
    this.moveTo(id, side, index);
  }

  /** The drag ends with nothing more to move. */
  endDrag() {
    this.#dragging = false;
  }
}
