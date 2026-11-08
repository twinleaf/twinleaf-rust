// The docks beside the plots: a left and a right column of panels, each
// open or folded. A panel moves between and within the docks, by its
// header or its menu, and never onto the tiles, which hold plots alone. A
// dock hides whole, to give the tiles its width, and keeps its panels.
//
// The arrangement is kept under KEY. A saved one keeps the panels this page
// knows, once each, and a panel it lacks joins its default dock, so a page
// that adds a panel shows it to someone who arranged the old ones.

/** The localStorage key of the arrangement. */
export const KEY = 'tl-docks-v1';

/**
 * Every panel, by id. `channels` names the RPC channels a Properties panel
 * shows (null for entries with none). A panel is as tall as what it shows;
 * one that `shrinks` gives up height when the dock runs short and scrolls
 * inside, and the rest keep theirs.
 */
export const PANELS = {
  devices: { title: 'Devices', shrinks: false },
  signals: { title: 'Signals', shrinks: true },
  settings: { title: 'Settings', shrinks: true, channels: ['setting'] },
  reports: { title: 'Reports', shrinks: true, channels: ['report'] },
  actions: { title: 'Actions', shrinks: false, channels: ['action'] },
  diagnostics: { title: 'Diagnostics', shrinks: true, channels: ['diagnostic'] },
  other: { title: 'Other', shrinks: true, channels: [null] },
};

export const SIDES = ['left', 'right'];

/** Where each panel starts: its side, in order, and whether it is open. */
export const DEFAULT = {
  left: [
    { id: 'devices', open: true },
    { id: 'signals', open: true },
  ],
  right: [
    { id: 'settings', open: true },
    { id: 'reports', open: true },
    { id: 'actions', open: true },
    { id: 'diagnostics', open: false },
    { id: 'other', open: false },
  ],
};

/**
 * The arrangement `saved` holds, made whole: unknown and repeated panels
 * dropped, and each missing one appended to its default side.
 */
export function normalize(saved) {
  const seen = new Set();
  const out = { left: [], right: [], hidden: { left: saved?.hidden?.left === true, right: saved?.hidden?.right === true } };
  for (const side of SIDES) {
    for (const p of Array.isArray(saved?.[side]) ? saved[side] : []) {
      if (!PANELS[p?.id] || seen.has(p.id)) continue;
      seen.add(p.id);
      out[side].push({ id: p.id, open: p.open !== false });
    }
  }
  for (const side of SIDES) for (const p of DEFAULT[side]) if (!seen.has(p.id)) out[side].push({ ...p });
  return out;
}

/** `docks` with panel `id` moved to `index` of `side`, the rest in order. */
export function move(docks, id, side, index) {
  const panel = SIDES.flatMap((s) => docks[s]).find((p) => p.id === id);
  if (!panel) return docks;
  const out = { ...docks, left: docks.left.filter((p) => p.id !== id), right: docks.right.filter((p) => p.id !== id) };
  out[side].splice(Math.max(0, Math.min(index, out[side].length)), 0, panel);
  return out;
}

export class Docks {
  left = $state([]);
  right = $state([]);
  /** Whether each dock is hidden, by side. */
  hidden = $state({ left: false, right: false });
  /** Whether a panel is being dragged, so an empty dock shows where it would go. */
  dragging = $state(false);

  /** `storage` is localStorage, or anything with its getItem and setItem. */
  constructor(storage) {
    this.storage = storage;
    let saved = null;
    try {
      saved = JSON.parse(storage?.getItem(KEY) ?? 'null');
    } catch {}
    Object.assign(this, normalize(saved));
  }

  save() {
    try {
      this.storage?.setItem(KEY, JSON.stringify({ left: this.left, right: this.right, hidden: this.hidden }));
    } catch {}
  }

  /** Whether `side` shows its panels: it has some, and is not hidden. */
  shown(side) {
    return this[side].length > 0 && !this.hidden[side];
  }

  /** Hides `side`, or shows it, by `hidden`. */
  hide(side, hidden) {
    this.hidden = { ...this.hidden, [side]: hidden };
    this.save();
  }

  /** The side panel `id` is on. */
  sideOf(id) {
    return SIDES.find((s) => this[s].some((p) => p.id === id));
  }

  /** Replaces the panels of `side` with `panels`, as a drop leaves them. */
  set(side, panels) {
    this[side] = panels.map(({ id, open }) => ({ id, open }));
    this.save();
  }

  moveTo(id, side, index) {
    Object.assign(this, move({ left: this.left, right: this.right, hidden: this.hidden }, id, side, index));
    this.hidden = { ...this.hidden, [side]: false };
    this.save();
  }

  /** Moves panel `id` by `step` places within its side. */
  step(id, step) {
    const side = this.sideOf(id);
    this.moveTo(id, side, this[side].findIndex((p) => p.id === id) + step);
  }

  toggle(id) {
    const side = this.sideOf(id);
    this[side] = this[side].map((p) => (p.id === id ? { ...p, open: !p.open } : p));
    this.save();
  }

  /** Puts every panel back where it starts. */
  reset() {
    Object.assign(this, normalize(null));
    this.save();
  }
}
