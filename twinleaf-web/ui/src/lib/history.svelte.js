// Undo and redo for what a person edits on the page: the tabs, their plots
// and layout, and the starred properties. A step is a snapshot before and
// after, with the plots selected at each, so undo puts the selection back
// as it was.
//
// A command that changes several things, as closing every selected plot,
// runs inside `group`, and a drag runs between `begin` and `end`, so each
// is one step. Changes made outside any group are steps of their own, and
// those that come within MERGE_MS of the step before join it, so a change
// the page makes a moment after a command lands in that command's step.
//
// What a view shows, as a spectrum's zoom, is not an edit: it is left out
// of the snapshot and kept as it is when a step is undone.

import { SHADOW_ITEM_MARKER_PROPERTY_NAME as SHADOW } from 'svelte-dnd-action';

/** How soon after a step a change made outside any group joins it, in ms. */
export const MERGE_MS = 400;
/** How many steps undo keeps. */
const DEPTH = 200;

/** Plot fields that are what a view shows, not what was edited. */
const VIEW_FIELDS = new Set(['fRange']);
/**
 * The tabs as a snapshot: view fields left out, the placeholders a drag
 * shows where a tab or signal would land left out, and split ratios to a
 * thousandth, so a pane's own rounding is no edit.
 */
function snapshotTabs(tabs) {
  return JSON.stringify(tabs, (k, v) => {
    if (VIEW_FIELDS.has(k)) return undefined;
    if (k === 'ratio' && typeof v === 'number') return Math.round(v * 1000) / 1000;
    if (Array.isArray(v)) return v.filter((x) => !x?.[SHADOW]);
    return v;
  });
}

export class History {
  /** The steps undo goes back through, oldest first, each {label, before, after, selBefore, selAfter, at}. */
  undos = $state([]);
  /** The steps redo goes forward through, the next last. */
  redos = $state([]);
  #depth = 0;
  #open = null;
  #last;

  /**
   * `ws` is the page's Workspace and `favorites` its Favorites. `now`
   * gives the time in ms, for joining changes.
   */
  constructor(ws, favorites, now = () => performance.now()) {
    this.ws = ws;
    this.favorites = favorites;
    this.now = now;
    this.#last = this.#read();
  }

  /** What undo would take back, or null. */
  get undoLabel() {
    return this.undos.at(-1)?.label ?? null;
  }
  /** What redo would do again, or null. */
  get redoLabel() {
    return this.redos.at(-1)?.label ?? null;
  }

  /** Opens a step named `label`; changes until the matching `end` are part of it. Steps opened inside one are part of the outer. */
  begin(label) {
    if (this.#depth++ === 0) this.#open = { label, before: this.#last, selBefore: this.#selection() };
  }

  /** Closes the step `begin` opened, and keeps it if anything changed. */
  end() {
    if (this.#depth === 0) return;
    if (--this.#depth) return;
    const open = this.#open;
    this.#open = null;
    this.#commit(open.label, open.before, open.selBefore, false);
  }

  /** Runs `fn` as one step named `label`, and returns what it returns. */
  group(label, fn) {
    this.begin(label);
    try {
      return fn();
    } finally {
      this.end();
    }
  }

  /** Takes in a change made outside any group: the page calls this whenever the tabs or favorites change. */
  observe() {
    if (this.#depth) return;
    const now = this.#read();
    if (now.key === this.#last.key) return;
    const top = this.undos.at(-1);
    if (top && !this.redos.length && this.now() - top.at < MERGE_MS) {
      top.after = now;
      top.selAfter = this.#selection();
      top.at = this.now();
      this.#last = now;
      return;
    }
    this.#commit('Edit', this.#last, this.#selection(), true);
  }

  /** Takes back the last step, and returns its label, or null when there is none. */
  undo() {
    return this.#step(this.undos, this.redos, 'before', 'selBefore');
  }

  /** Does again the step undo took back last, and returns its label, or null when there is none. */
  redo() {
    return this.#step(this.redos, this.undos, 'after', 'selAfter');
  }

  #step(from, to, side, sel) {
    if (this.#depth) return null;
    const e = from.pop();
    if (!e) return null;
    this.#write(e[side], e[sel]);
    this.#last = this.#read();
    e.at = -Infinity;
    to.push(e);
    return e.label;
  }

  #commit(label, before, selBefore, observed) {
    const after = this.#read();
    if (after.key === before.key) return;
    this.undos.push({ label, before, after, selBefore, selAfter: this.#selection(), at: this.now(), observed });
    if (this.undos.length > DEPTH) this.undos.shift();
    this.redos = [];
    this.#last = after;
  }

  /** The snapshot of what steps change: `key` compares them, `tabs` restores them. */
  #read() {
    const tabs = snapshotTabs(this.ws.tabs);
    const favorites = JSON.stringify([...this.favorites.names].sort());
    return { key: tabs + favorites, tabs, favorites, activeId: this.ws.activeId };
  }

  #selection() {
    return { tab: this.ws.activeId, selected: [...this.ws.selected], focusedId: this.ws.focusedId };
  }

  /** Puts snapshot `s` back, keeps each plot's view fields as they are now, and opens the tab of `sel` with its plots selected. */
  #write(s, sel) {
    if (snapshotTabs(this.ws.tabs) !== s.tabs) {
      const views = new Map(this.ws.tabs.flatMap((t) => Object.values(t.plots)).map((p) => [p.id, p]));
      const tabs = JSON.parse(s.tabs);
      for (const t of tabs) {
        for (const p of Object.values(t.plots)) {
          const was = views.get(p.id);
          if (was) for (const f of VIEW_FIELDS) if (f in was) p[f] = was[f];
        }
      }
      this.ws.tabs = tabs;
    }
    if (JSON.stringify([...this.favorites.names].sort()) !== s.favorites) this.favorites.set(JSON.parse(s.favorites));
    const tab = this.ws.tabs.some((t) => t.id === sel.tab) ? sel.tab : this.ws.tabs[0].id;
    if (tab !== this.ws.activeId) this.ws.selectTab(tab);
    this.ws.selected.clear();
    for (const id of sel.selected) if (this.ws.plots[id]) this.ws.selected.add(id);
    this.ws.focusedId = this.ws.plots[sel.focusedId] ? sel.focusedId : null;
  }
}
