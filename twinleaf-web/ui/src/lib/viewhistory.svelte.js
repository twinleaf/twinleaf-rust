// Back and Forward through what the plots of a tab have shown, from the
// buttons on each plot. A step holds, for each plot one gesture moved, what
// it showed before and after: its View `state` (live or paused and where,
// its span and window, its cursors, the y ranges it holds and the spectra a
// paused spectrum holds) and its view fields (tabs.js VIEW_FIELDS, as a
// spectrum's frequencies). Back shows the before of every plot of the
// step still in the tab, so the plots one gesture moved go back together;
// Forward shows the after.
//
// Back goes to the newest step that moves a plot shown, past steps of
// plots closed, moved to another tab or hidden behind a maximized plot,
// which stay where they are until their plots show again; so Back and
// Forward can be pressed just when a press changes what shows. A view
// shown live follows the newest samples again. One shown paused holds
// where it was, ending no later than the newest samples; a time plot's
// whose start the hub has dropped since starts at the oldest time it
// holds, and a notice says so. A plot an undo brings back shows its view
// by the same rule (`returned`).
//
// Each tab has its own steps, which go when the tab is gone for good
// (`prune`). A drag, from its press to its release (`begin`, `end`), is one
// step, and so is a command (`step`), whatever they pass through on the
// way. Wheel turns and keys (`burst`) are one step while each comes less
// than IDLE_MS after the last and makes the same kind of change to the same
// plots. A gesture that moves no view, or a burst that ends where it began,
// is no step and leaves Forward as it was; a step made inside another
// gesture is part of that one.
//
// The edit history (history.svelte.js) is apart: an undo never moves a
// view, and Back never edits.

import { SvelteMap } from 'svelte/reactivity';
import { sameView } from './plot.js';
import { VIEW_FIELDS } from './tabs.js';

/** How long, in ms, wheel turns or keys rest before the next is a step of its own. */
export const IDLE_MS = 500;
/** How many steps Back keeps for each tab. */
const DEPTH = 100;
/** The notice for a view whose start the hub has dropped. */
export const GONE = 'That moment is gone: showing the oldest data held';

/** The entries of map `m` for `ids`. */
const only = (m, ids) => new Map(ids.map((id) => [id, m.get(id)]));
/** Whether plot states `a` and `b`, as `#read` gives them, show the same. */
const same = (a, b) => sameView(a.view, b.view) && JSON.stringify(a.fields) === JSON.stringify(b.fields);

/** One tab's steps, each {before, after}: Maps of plot id to {view, fields}. */
class Steps {
  /** The steps Back goes through, oldest first. */
  backs = $state.raw([]);
  /** The steps Forward goes through, the next last. */
  forwards = $state.raw([]);
}

export class ViewHistory {
  #tabs = new SvelteMap(); // tab id -> Steps
  #held = null; // the gesture under way, from `begin` to `end`: {tab, before}
  #burst = null; // the burst open to more: {tab, kind, targets, at, before, step, forwards}, its step, if any, the newest
  #bursting = false; // whether a burst's change is running, so a step inside it is part of it

  /**
   * `ws` is the page's Workspace and `views` its Views. `dropped(id)` is
   * the time before which the hub has dropped what it held of plot `id`'s
   * signals, or null when it has dropped none; `notice(text)` tells the
   * person; `now()` is the page time, in ms.
   */
  constructor(ws, views, { dropped = () => null, notice = () => {}, now = () => performance.now() } = {}) {
    Object.assign(this, { ws, views, dropped, notice, now });
  }

  /** Whether Back would change what the open tab shows. */
  get canBack() {
    return this.#next('backs') >= 0;
  }
  /** Whether Forward would change what the open tab shows. */
  get canForward() {
    return this.#next('forwards') >= 0;
  }

  /** Runs `fn` as one step, and returns what it returns; inside a drag or a burst, as part of that. */
  step(fn) {
    if (this.#held || this.#bursting) return fn();
    this.begin();
    try {
      return fn();
    } finally {
      this.end();
    }
  }

  /**
   * Runs `fn`, a wheel turn or a key that makes a change of `kind` to the
   * views `targets`, and returns what it returns. It joins the burst before
   * it when that one was of the same kind on the same targets, less than
   * IDLE_MS ago, with no other step made or gone through since; else it
   * begins one. A burst's step holds what its turns moved from where they
   * were as it began, and is none while that is nothing.
   */
  burst(kind, targets, fn) {
    if (this.#held || this.#bursting) return fn();
    const tab = this.ws.activeId, at = this.now(), of = this.#idsOf(targets).join();
    const steps = this.#of(tab), b = this.#burst;
    if (!(b && b.tab === tab && b.kind === kind && b.targets === of && at - b.at < IDLE_MS))
      this.#burst = { tab, kind, targets: of, before: this.#read(this.#ids()), step: null, forwards: steps.forwards };
    const burst = this.#burst;
    burst.at = at;
    this.#bursting = true;
    let out;
    try {
      out = fn();
    } finally {
      this.#bursting = false;
    }
    const ids = [...burst.before.keys()], after = this.#read(ids);
    const moved = ids.filter((id) => after.has(id) && !same(burst.before.get(id), after.get(id)));
    const under = burst.step ? steps.backs.slice(0, -1) : steps.backs;
    burst.step = moved.length ? { before: only(burst.before, moved), after: only(after, moved) } : null;
    steps.backs = burst.step ? [...under.slice(-(DEPTH - 1)), burst.step] : under;
    steps.forwards = burst.step ? [] : burst.forwards;
    return out;
  }

  /** Starts a gesture held down, as a drag, which `end` makes one step. */
  begin() {
    this.end();
    this.#burst = null;
    this.#held = { tab: this.ws.activeId, before: this.#read(this.#ids()) };
  }

  /** Ends the gesture `begin` started, a step if it moved any view. */
  end() {
    const h = this.#held;
    if (!h) return;
    this.#held = null;
    const ids = [...h.before.keys()], after = this.#read(ids);
    const moved = ids.filter((id) => after.has(id) && !same(h.before.get(id), after.get(id)));
    if (moved.length) this.#push(h.tab, { before: only(h.before, moved), after: only(after, moved) });
  }

  /** Shows what the newest step that moves a plot shown in the open tab moved, as it was before; says whether there was one. */
  back() {
    return this.#go('backs', 'forwards', 'before');
  }

  /** Shows again what the step Back went back past last moved, of those that move a plot shown; says whether there was one. */
  forward() {
    return this.#go('forwards', 'backs', 'after');
  }

  /**
   * Plots `ids` are in the tabs again, as an undo brings back a plot closed:
   * each of the open tab's shows its view again by the rule Back shows one
   * by, as the hub, holding nothing for a plot that is gone, may have dropped
   * where it starts.
   */
  returned(ids) {
    this.#show(this.#read(ids.filter((id) => this.ws.plots[id])));
  }

  /** Lets go of the steps of every tab not in `tabs`, a Set of tab ids: those the tabs or an undo or redo hold. */
  prune(tabs) {
    for (const tab of [...this.#tabs.keys()]) if (!tabs.has(tab)) this.#tabs.delete(tab);
  }

  /** Whether it keeps steps of tab `tab`. */
  holds(tab) {
    return this.#tabs.has(tab);
  }

  /** The index in the open tab's `list` of the newest step that moves a plot shown, or -1. */
  #next(list) {
    const steps = this.#tabs.get(this.ws.activeId);
    if (!steps) return -1;
    const shown = new Set(this.ws.shownIds());
    return steps[list].findLastIndex((s) => [...s.before.keys()].some((id) => shown.has(id) && this.views.get(id)));
  }

  /** Moves the open tab's step `#next(from)` to `to`, and shows its plots' `side`. */
  #go(from, to, side) {
    this.end();
    this.#burst = null;
    const i = this.#next(from);
    if (i < 0) return false;
    const steps = this.#of(this.ws.activeId), step = steps[from][i];
    steps[from] = steps[from].toSpliced(i, 1);
    steps[to] = [...steps[to], step];
    this.#show(step[side]);
    return true;
  }

  /** Shows `states`, by plot id, of the plots in the open tab, with the notice when the hub dropped where one starts. */
  #show(states) {
    let lost = false;
    for (const [id, s] of states) if (this.ws.plots[id] && this.views.get(id)) lost = this.#put(id, s) || lost;
    if (lost) this.notice(GONE);
  }

  /**
   * Shows plot `id` of the open tab as `s` has it. A paused time plot's
   * view whose start the hub has dropped moves later to start at the oldest
   * time it holds; says whether it moved.
   */
  #put(id, s) {
    let view = s.view, lost = false;
    const dropped = this.ws.plots[id].kind === 'time' && !view.live ? this.dropped(id) : null;
    if (dropped !== null && view.right - view.span < dropped) {
      const right = Math.max(view.right, dropped + view.span);
      lost = right > view.right;
      view = { ...view, right, pausedAt: Math.max(view.pausedAt ?? right, right) };
    }
    this.views.get(id).restore(view, this.now());
    const plot = this.ws.plots[id], patch = {};
    for (const f of VIEW_FIELDS) if (JSON.stringify(plot[f] ?? null) !== JSON.stringify(s.fields[f])) patch[f] = s.fields[f];
    if (Object.keys(patch).length) this.ws.setView(id, patch);
    return lost;
  }

  #push(tab, step) {
    const steps = this.#of(tab);
    steps.backs = [...steps.backs.slice(-(DEPTH - 1)), step];
    steps.forwards = [];
  }

  #of(tab) {
    let steps = this.#tabs.get(tab);
    if (!steps) this.#tabs.set(tab, (steps = new Steps()));
    return steps;
  }

  /** The open tab's plots, by id, in one order. */
  #ids() {
    return Object.keys(this.ws.plots ?? {}).sort();
  }

  /** The ids of the plots whose views are `targets`, in one order. */
  #idsOf(targets) {
    return [...this.views.map].filter(([, v]) => targets.includes(v)).map(([id]) => id).sort();
  }

  /** What plots `ids` that have a view show, by id: {view, fields}, its View state and its view fields. */
  #read(ids) {
    const out = new Map();
    for (const id of ids) {
      const v = this.views.get(id), plot = this.ws.tabs.find((t) => t.plots[id])?.plots[id];
      if (v) out.set(id, { view: v.state(), fields: Object.fromEntries([...VIEW_FIELDS].map((f) => [f, plot?.[f] ?? null])) });
    }
    return out;
  }
}
