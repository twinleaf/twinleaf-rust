<script>
  // One plot of a tab: its header of signals and actions, the plot body the
  // WebGL layer draws behind, and its menus. The drawing is plot.js's
  // PlotView; this is the plot's controls.
  import { getContext, onDestroy, onMount, tick } from 'svelte';
  import { dndzone, TRIGGERS, SHADOW_ITEM_MARKER_PROPERTY_NAME as SHADOW } from 'svelte-dnd-action';
  import * as ContextMenu from '$lib/components/ui/context-menu/index.js';
  import * as Popover from '$lib/components/ui/popover/index.js';
  import ColorPicker from './ColorPicker.svelte';
  import RiToLeft from '@lucide/svelte/icons/arrow-left';
  import RiToRight from '@lucide/svelte/icons/arrow-right';
  import RiRemoveAxis from '@lucide/svelte/icons/trash-2';
  import RiFit from '@lucide/svelte/icons/unfold-vertical';
  import RiFitX from '@lucide/svelte/icons/unfold-horizontal';
  import { common, counted } from './menus.js';
  import RiSpan from '@lucide/svelte/icons/ruler-dimension-line';
  import RiShowOnly from '@lucide/svelte/icons/eye';
  import RiHide from '@lucide/svelte/icons/eye-off';
  import RiPalette from '@lucide/svelte/icons/palette';
  import RiAxis from '@lucide/svelte/icons/arrow-right-left';
  import RiMove from '@lucide/svelte/icons/square-arrow-out-up-right';
  import RiTrash from '@lucide/svelte/icons/trash-2';
  import RiCopy from '@lucide/svelte/icons/clipboard-copy';
  import RiEraser from '@lucide/svelte/icons/eraser';
  import RiTab from '@lucide/svelte/icons/app-window';
  import RiKind from '@lucide/svelte/icons/chart-line';
  import RiLayout from '@lucide/svelte/icons/layers';
  import { ScrollArea } from '$lib/components/ui/scroll-area/index.js';
  import IconButton from './IconButton.svelte';
  import CommandItem from './CommandItem.svelte';
  import { axesLike, commandOf, xSpan } from './commands.js';
  import KindPicker from './KindPicker.svelte';
  import { plotPickers } from './keys.js';
  import { claim } from './keyrouter.js';
  import Tip from './Tip.svelte';
  import RiDraggable from '@lucide/svelte/icons/grip-vertical';
  import RiBack from '@lucide/svelte/icons/arrow-left';
  import RiForward from '@lucide/svelte/icons/arrow-right';
  import RiClose from '@lucide/svelte/icons/x';
  import RiPause from '@lucide/svelte/icons/pause';
  import RiPlay from '@lucide/svelte/icons/play';
  import RiAddSignal from '@lucide/svelte/icons/plus';
  import RiLegend from '@lucide/svelte/icons/list';
  import RiXCursors from '@lucide/svelte/icons/separator-vertical';
  import RiYCursors from '@lucide/svelte/icons/separator-horizontal';
  import RiZoomTo from '@lucide/svelte/icons/zoom-in';
  import { PlotView, axisSlots, colorOf, cursorText, monitorOf } from './plot.js';
  import { colors } from './colors.svelte.js';
  import Swatch from './Swatch.svelte';
  import { DRAG_START, KINDS, MODES, kindLabel } from './constants.js';
  import { writeTables } from './copy.js';
  import { toast } from './toast.svelte.js';
  import { stepIn } from './keymap.js';
  import { chip } from './tabs.js';
  import WindowSlider from './WindowSlider.svelte';
  import * as clock from './clock.js';

  let {
    plot, hub, view, targets, hover, renderer, registry, focused, selected, otherTabs = [], tabName = '',
    targetPlots = () => [plot],
    onFocus, onPress, onSelect, onStartMove, onMoveToTab,
    onTraceDragStart, onLandSignal, onEdit, onView, otherPlots = [], onMoveSignal,
    onEach, targetIds = () => [plot.id], viewHistory, cursors,
  } = $props();
  /** The plot commands (commands.js): this plot's rows, buttons and keys run them, made on this plot. */
  const commands = getContext('commands');
  /** Runs plot command `id` made on this plot from its button, `how` as the command takes it. */
  const runHere = (id, how) => commands.run(id, { on: plot.id, from: 'button', how });
  /** How Maximize reads on this plot, for its button. */
  const maximizeRow = $derived(commands.row('maximize', { on: plot.id }));

  /** Runs `fn(pv)` on the PlotView of each plot a command here acts on. */
  const eachPv = (fn) => {
    const ids = targetIds();
    for (const p of registry) if (ids.includes(p.plotId)) fn(p);
  };
  /** Runs `fn`, a command here that moves the views, as one view step, however many plots it moves. */
  const asStep = (fn) => viewHistory.step(fn);
  /**
   * Gives each plot a cursor command here acts on the cursors `fn(pv)`
   * returns for it, as one edit named `what`: `each` runs over those
   * plots, `eachPv` by default, or `eachAxis` of an axis.
   */
  const editCursors = (what, fn, each = eachPv) => {
    const byId = new Map();
    each((p, axis) => byId.set(p.plotId, fn(p, axis)));
    cursors.set(what, byId);
  };
  const clearXCursors = () => editCursors('Clear X cursors', (p) => p.xCursorsCleared());
  /** Removes the Y cursors of the axis of each plot that matches this plot's y axis `key`, or with no key, of every axis of each plot. */
  const clearYCursors = (key) => editCursors('Clear Y cursors', (p, axis) => p.yCursorsCleared(axis), key === undefined ? eachPv : (fn) => eachAxis(key, fn));
  /**
   * Runs `fn(pv, axis)` for a command made on this plot's y axis `key`, on
   * the PlotView of each plot the command acts on, `axis` the key of that
   * plot's y axis in the same units as this one (commands.js `axesLike`).
   */
  const eachAxis = (key, fn) => axesLike(pv, targetPvs(), key).forEach(([p, axis]) => fn(p, axis));
  /** Sets y scale `scale` on each plot a command on this plot's y axis `key` acts on: those with an axis in its units. */
  function setYScale(key, scale) {
    const ids = new Set();
    eachAxis(key, (p) => ids.add(p.plotId));
    onEach(scale === 'log' ? 'Log density scale' : 'Linear density scale', (p) => (ids.has(p.id) ? { yScale: scale } : null));
  }
  // What the menus' rows say of the plots their commands act on (menus.js):
  // a count over them, a setting they share, or how many they are where a
  // row names plots. Each is read as its menu opens.
  /** The PlotViews of the plots a command here acts on. */
  const targetPvs = () => {
    const ids = targetIds();
    return [...registry].filter((p) => ids.includes(p.plotId));
  };
  /** The first X cursor placed on the plots a command here acts on that have one, named to tell it from its pair, or from the view's width. */
  function xFirst() {
    return common(targetPvs().flatMap((p) => {
      const { a, b } = p.markers(), at = a ?? b;
      if (at === null) return [];
      const between = a !== null && b !== null ? Math.abs(b - a) : p.xd ? p.xd[1] - p.xd[0] : 0;
      return [`${a !== null ? 'X1' : 'X2'} ${cursorText(p.xKind()).at(at, between)}`];
    }));
  }
  /** The name list `list` (KINDS, MODES) gives value `v`. */
  const nameIn = (list, v) => list.find((x) => x[0] === v)?.[1] ?? v;
  /** Sets `field` to `value` on each plot a command here acts on of this plot's kind, as a frequency axis's scale; `what` names the step. */
  const setEach = (what, field, value) => onEach(what, (p) => (p.kind === plot.kind ? { [field]: value } : null));

  const session = $derived(hub.session);
  /** The signal `key`'s colour, units and rate, once the hub has its trace. */
  const colorFor = (key) => {
    const t = session.trace(key);
    return t ? colorOf(t) : 'var(--muted-foreground)';
  };
  const unitsOf = (key) => session.trace(key)?.meta.units ?? '';
  /** An independent plot's axes, {left, right}, each the signal keys from the plot outward. */
  const slots = $derived(plot.mode === 'independent' ? axisSlots(drawnItems().map((i) => i.key), plot.axisSides) : null);
  /** Which side plot `p`'s axis of signal `key` is on, as a menu names it: Left, Right or None. */
  const sideOf = (p, key) => {
    const s = axisSlots(p.items.filter((i) => !i.hidden).map((i) => i.key), p.axisSides);
    return s.left.includes(key) ? 'Left' : s.right.includes(key) ? 'Right' : 'None';
  };
  /** Whether signal `key` has no axis of its own on an independent plot. */
  const axisless = (key) => !!slots && !slots.left.includes(key) && !slots.right.includes(key);
  /** Puts signal `key`'s axis on `side`, 'left' or 'right', or hides it with 'none', on each independent plot a command here acts on that shows it. */
  function setSide(key, side) {
    const shows = (p) => p.mode === 'independent' && p.items.some((i) => i.key === key);
    onEach(side === 'none' ? `Hide ${key}'s axis` : `${key}'s axis on the ${side}`, (p) => (shows(p) ? { axisSides: { ...p.axisSides, [key]: side } } : null));
  }

  // A right-click shows what it was on: the time axis's window, or the
  // plot's own items, in a menu; a y axis's settings, which take typing, in
  // a popover at the pointer, as menus hold no fields.
  let menuOpen = $state(false);
  let region = $state({ kind: 'plot' });
  let axisOpen = $state(false);
  let xAxisOpen = $state(false);
  let readoutOpen = $state(false);
  let axisAt = $state({ x: 0, y: 0 });
  let axisAnchor = $state(null);
  let bounds = $state({ lo: '', hi: '' });
  let boundsError = $state('');
  const shown = (v) => String(+v.toPrecision(6));
  /** The one signal the axis right-clicked scales, whose colour its popover sets, or null for a shared axis. */
  const axisKey = $derived(region.kind === 'y' && region.keys.length === 1 && region.keys[0] !== 'shared' ? region.keys[0] : null);
  // What a copy of the plot would hold, asked of the hub at each right-click
  // that opens or moves the menu, since the window may have changed while it
  // was shut or open. The right-click sets the placeholder itself, so the
  // menu's first frame already has its line.
  let copyCheck = $state(null);
  let copyAsk = $state(0);
  /** What Copy data says while the hub counts the rows. */
  const COUNTING = { state: 'ok', rows: null };
  function recountCopy() {
    if (plot.items.length) copyCheck = COUNTING;
    copyAsk++;
  }
  /** How long the view must rest, while the menu is open, before the count is asked again, in ms. */
  const RECOUNT_MS = 200;
  $effect(() => {
    copyAsk;
    if (!plot.items.length) return void (copyCheck = null);
    // A closing menu keeps its line while it fades, so it does not change size on the way out.
    if (!menuOpen) return;
    let current = true, asked = 0, timer;
    const ask = () => {
      const n = ++asked;
      commands.check('copyData', { on: plot.id }).then((check) => current && n === asked && (copyCheck = check), () => current && n === asked && (copyCheck = null));
    };
    copyCheck = COUNTING;
    ask();
    // A wheel over a plot zooms it with the menu still open: the count
    // follows once the views of every plot a copy would hold rest, keeping
    // the last one shown meanwhile.
    const offs = targets().map((v) =>
      v.onChange(() => {
        clock.cancel(timer);
        timer = clock.after(RECOUNT_MS, ask);
      }),
    );
    return () => {
      current = false;
      clock.cancel(timer);
      offs.forEach((off) => off());
    };
  });
  function contextAt(e) {
    // The cursors' readout has a menu of its own.
    if (e.target.closest?.('.cursor-readout')) return;
    region = pv?.regionAt(e.clientX, e.clientY) ?? { kind: 'plot' };
    if (region.kind !== 'y' && region.kind !== 'x') return;
    // The axis's popover, in place of the plot's menu.
    e.preventDefault();
    e.stopPropagation();
    const r = body.getBoundingClientRect();
    axisAt = { x: e.clientX - r.left, y: e.clientY - r.top };
    if (region.kind === 'x') {
      fBoundsError = '';
      const xd = plot.kind === 'spectrum' ? pv?.xd : null;
      fBounds = xd ? { lo: shown(xd[0]), hi: shown(xd[1]) } : { lo: '', hi: '' };
      fBoundsShown = { ...fBounds };
      return void (xAxisOpen = true);
    }
    boundsError = '';
    const b = region.keys.length ? pv.yBounds(region.keys[0]) : null;
    bounds = b ? { lo: shown(b[0]), hi: shown(b[1]) } : { lo: '', hi: '' };
    boundsShown = { ...bounds };
    axisOpen = true;
  }
  /** Puts the cursors' readout on the clipboard, as the table shows it. */
  function copyReadout() {
    const c = legend.cursors;
    if (!c) return;
    const rows = [];
    if (c.x) {
      rows.push([`X ${c.x.v}`, c.x.a, c.x.b, c.x.d, c.x.rate ? `1/Δt ${c.x.rate}` : '']);
      for (const item of plot.items) {
        const r = legend.rows[item.key];
        rows.push([item.key, r?.a ?? '—', r?.b ?? '—', r?.d ?? '—', '']);
      }
    }
    for (const y of c.ys) rows.push([`Y ${y.name}`, y.a, y.b, y.d, y.ratio]);
    const blocks = [{ title: `Cursors · ${plot.items.map((i) => i.key).join(', ')}`, header: ['', '1', '2', 'Δ = 2 − 1', ''], rows }];
    writeTables(blocks).then(
      () => toast.success('Copied the cursor readout'),
      (error) => toast.error(`Could not copy: ${error.message}`),
    );
  }
  /**
   * What the popovers' fields showed as they opened, or last set: bounds
   * left as shown set nothing, so a field that loses focus as its popover
   * closes leaves every plot as it was.
   */
  let boundsShown = { lo: '', hi: '' }, fBoundsShown = { lo: '', hi: '' };
  const edited = (b, was) => b.lo !== was.lo || b.hi !== was.hi;
  /** Sets the bounds typed in, and says why it cannot when they do not make a range. Returns whether they hold. */
  function applyBounds() {
    if (!edited(bounds, boundsShown)) return true;
    const lo = Number(bounds.lo), hi = Number(bounds.hi);
    if (!isFinite(lo) || !isFinite(hi) || bounds.lo.trim() === '' || bounds.hi.trim() === '') boundsError = 'Enter a number in each';
    else if (lo >= hi) boundsError = 'Max must be above min';
    else if (plot.kind === 'spectrum' && plot.yScale !== 'linear' && lo <= 0) boundsError = 'A log axis starts above zero';
    else {
      boundsError = '';
      // A log axis takes no bound at or below zero, and is left as it is.
      asStep(() => eachAxis(region.keys[0], (p, axis) => !(p.logY && lo <= 0) && p.setYBounds([axis], lo, hi)));
      boundsShown = { ...bounds };
      return true;
    }
    return false;
  }
  // A spectrum's frequency range, typed in its x axis's popover.
  let fBounds = $state({ lo: '', hi: '' });
  let fBoundsError = $state('');
  /** Sets the frequencies typed in, and says why it cannot when they do not make a range. Returns whether they hold. */
  function applyFBounds() {
    if (!edited(fBounds, fBoundsShown)) return true;
    const lo = Number(fBounds.lo), hi = Number(fBounds.hi);
    if (!isFinite(lo) || !isFinite(hi) || fBounds.lo.trim() === '' || fBounds.hi.trim() === '') fBoundsError = 'Enter a number in each';
    else if (lo >= hi) fBoundsError = 'Max must be above min';
    else if (plot.xScale !== 'linear' && lo <= 0) fBoundsError = 'A log axis starts above zero';
    else {
      fBoundsError = '';
      onView({ fRange: [lo, hi] });
      fBoundsShown = { ...fBounds };
      return true;
    }
    return false;
  }
  /** Enter sets the bounds and closes; leaving a field sets them and stays. */
  function boundsKey(e) {
    if (e.key === 'Enter' && applyBounds()) axisOpen = false;
  }

  /** Runs `fn(view)` on the view of each plot a command here acts on, as one view step. */
  const forTargets = (fn) => viewHistory.step(() => targets().forEach(fn));

  // The signals menu: every signal the plot does not show, filtered by a
  // substring of its name, and kept open so several can be added at once.
  // Focus stays in the filter, where the arrows move the highlight and Enter
  // adds the highlighted signal; the next one takes its place.
  let signalsOpen = $state(false);
  let signalFilter = $state('');
  let addActive = $state(0);
  const addable = $derived.by(() => {
    const has = new Set(plot.items.map((i) => i.key)), f = signalFilter.trim().toLowerCase();
    return session.signals.filter((t) => !has.has(t.key) && t.key.toLowerCase().includes(f));
  });
  /** Adds signal `key` to each plot a command here acts on that does not show it. */
  function addSignal(key) {
    onEach(`Add ${key}`, (p) => (p.items.some((i) => i.key === key) ? null : { items: [...p.items, chip(key)] }));
  }
  function filterKey(e) {
    // Escape and Tab go on to the menu, which closes; every other key stays here.
    if (e.key === 'Escape' || e.key === 'Tab') return;
    e.stopPropagation();
    const to = { ArrowDown: addActive + 1, ArrowUp: addActive - 1, PageDown: addActive + 8, PageUp: addActive - 8 }[e.key];
    if (to !== undefined) addActive = Math.max(0, Math.min(addable.length - 1, to));
    else if (e.key === 'Enter' && addable[addActive]) {
      addSignal(addable[addActive].key);
      addActive = Math.max(0, Math.min(addActive, addable.length - 2));
    } else return;
    e.preventDefault();
    tick().then(() => document.getElementById(`add-${plot.id}-${addActive}`)?.scrollIntoView({ block: 'nearest' }));
  }

  // A click, not a drag, on the plot selects it; with Shift it adds the plot
  // to the selection or takes it out, and does nothing else: no pan, and no
  // pick-up from the header. The capture sees the press before the header
  // or the plot takes it for a move or a pan. A menu opened anywhere on the
  // plot, by the right button or the keys, selects it first as a press on
  // its controls does, so the menu acts on what is selected.
  let pressedAt = null;
  /** The plot's header and controls, a press on which selects the plot first: its grip and its header start the same move. */
  const CONTROLS = '.plot-head, button, [role=button], [role=combobox], input';
  function pressed(e) {
    pressedAt = [e.clientX, e.clientY];
    if (e.button === 0 && !e.shiftKey && e.target.closest?.(CONTROLS)) onPress?.();
    else onFocus();
  }
  function clicked(e) {
    if (!pressedAt || Math.hypot(e.clientX - pressedAt[0], e.clientY - pressedAt[1]) >= DRAG_START) return;
    if (e.target.closest('button, input, [role=menuitem]')) return;
    onSelect?.(e.shiftKey, e.detail);
  }

  let body = $state();
  let overlay = $state();
  let pv;

  // The readouts come from the plot view each frame; the page shows them a
  // few times a second, which is as fast as anyone reads.
  const LEGEND_MS = 66;
  let legend = $state({ rows: {}, cursors: null });

  // A plot too small for its header becomes a monitor, as plot.js's
  // `monitorOf` decides from the traces it draws.
  let size = $state({ width: Infinity, height: Infinity });
  const monitor = $derived(monitorOf(size, plot.mode, plot.items.filter((i) => session.trace(i.key)).length));

  /**
   * Marks a row of chips that runs past either edge, for the fade there, and
   * turns the wheel into a sideways scroll over it. The row is one line high
   * however many signals it holds.
   */
  function overflowEdges(node) {
    const mark = () => {
      node.classList.toggle('more-left', node.scrollLeft > 1);
      node.classList.toggle('more-right', node.scrollLeft + node.clientWidth < node.scrollWidth - 1);
    };
    const wheel = (e) => {
      if (node.scrollWidth <= node.clientWidth || Math.abs(e.deltaX) > Math.abs(e.deltaY)) return;
      e.preventDefault();
      node.scrollLeft += e.deltaY;
    };
    const sizing = new ResizeObserver(mark);
    sizing.observe(node);
    const chips = new MutationObserver(mark);
    chips.observe(node, { childList: true });
    node.addEventListener('scroll', mark, { passive: true });
    node.addEventListener('wheel', wheel, { passive: false });
    return {
      destroy() {
        sizing.disconnect();
        chips.disconnect();
        node.removeEventListener('scroll', mark);
        node.removeEventListener('wheel', wheel);
      },
    };
  }

  // Whether the plot's view streams, for its play and pause button, and its window, for the time axis's slider.
  let streaming = $state(true);
  let windowSpan = $state(10);

  onMount(() => {
    const poll = clock.every(LEGEND_MS, () => pv?.legend && (legend = pv.legend));
    pv = new PlotView({
      body,
      overlay,
      hub,
      view,
      targets,
      targetIds,
      cursors,
      onFollow: () => runHere('follow'),
      hoverTime: hover,
      plotId: plot.id,
      renderer,
      onFocus,
      onFrequencies: (lo, hi) => onView({ fRange: [lo, hi] }),
      onFitY: (key) => asStep(() => eachAxis(key, (p, axis) => p.fit([axis]))),
      viewHistory,
      config: () => ({ kind: plot.kind, mode: plot.mode, sides: plot.axisSides, keys: drawnItems().map((i) => i.key), floorsHidden: drawnItems().filter((i) => i.floorHidden).map((i) => i.key), xLog: plot.xScale !== 'linear', yLog: plot.yScale !== 'linear', fRange: plot.fRange ?? null }),
    });
    registry.add(pv);
    plotPickers.set(plot.id, openPicker);
    const sizing = new ResizeObserver(([entry]) => {
      const { width, height } = entry.contentRect;
      size = { width, height };
    });
    sizing.observe(card);
    streaming = view.live;
    windowSpan = view.window;
    const unwatch = view.onChange(() => ((streaming = view.live), (windowSpan = view.window)));
    return () => {
      pv.letGo();
      clock.cancel(poll);
      unwatch();
      sizing.disconnect();
      registry.delete(pv);
      if (plotPickers.get(plot.id) === openPicker) plotPickers.delete(plot.id);
    };
  });

  // While a chip is dragged over the header, the chips it shows, the drag
  // library's shadow among them; the plot's own stay as they are until the
  // drop, which the page lands as one step. dragOrigin: this plot's chips as
  // they were when one of them was picked up.
  let chips = $state.raw(null);
  let dragOrigin = $state.raw(null);
  const zoneItems = $derived(chips ?? plot.items);
  /** The signals the plot draws and gives axes to. */
  const drawnItems = () => plot.items.filter((i) => !i.hidden);
  // A chip being dragged is the drag library's shadow in the row it is over,
  // drawn as the chip faded and outlined at the place it would land, the
  // other chips trading places with it as the pointer crosses them. In its
  // own row the shadow takes the chip's own room, so the row keeps its width
  // and only its order changes. While the pointer is anywhere else, a shadow
  // of the row's own stands where the chip was, so the row keeps its chips in
  // place until the chip lands on another plot. That one has an id of its
  // own, so the chip coming back gets a new element, not the one the library
  // hid. Where the library adds its shadow beside that one, the row keeps the
  // library's.
  const HOME = 'home:'; // the id prefix of that shadow
  function consider(e) {
    const { trigger, id } = e.detail.info;
    if (trigger === TRIGGERS.DRAG_STARTED) {
      dragOrigin = [...plot.items];
      onTraceDragStart?.();
    }
    const items = e.detail.items;
    const shadows = items.filter((i) => i[SHADOW]);
    const shadow = shadows.find((i) => !i.id.startsWith(HOME)) ?? shadows[0];
    // A row the chip only passed over shows the plot's own chips again.
    if (!shadow) chips = dragOrigin ? dragOrigin.map((i) => (i.id === id ? { ...i, id: HOME + i.id, [SHADOW]: true } : i)) : null;
    else chips = items.filter((i) => !i[SHADOW] || i === shadow);
  }
  // A pointer drop ends in one finalize that is not DROPPED_INTO_ANOTHER:
  // the zone the chip landed in, or the one it came from when it landed in
  // none. That one lands it, with the chips as dropped when it landed among
  // them; the zone it left only lets go of what it showed.
  function finalize(e) {
    const { trigger, id } = e.detail.info;
    const items = e.detail.items.filter((i) => !i[SHADOW]);
    chips = null;
    dragOrigin = null;
    if (trigger === TRIGGERS.DROPPED_INTO_ANOTHER) return;
    const dropped = items.find((i) => i.id === id);
    if (dropped) onLandSignal?.(dropped, trigger === TRIGGERS.DROPPED_INTO_ZONE ? items : null);
  }
  // The plot body is a drop target of its own, which shows nothing: a signal
  // dropped there joins the header's chips, or a new plot when on an edge.
  let bodyItems = $state([]);
  function bodyConsider(e) {
    bodyItems = e.detail.items;
  }
  function bodyFinalize(e) {
    const dropped = e.detail.items.filter((i) => !i[SHADOW]);
    bodyItems = [];
    for (const item of dropped) onLandSignal?.(item, [...plot.items, item]);
  }
  function remove(id) {
    const item = plot.items.find((i) => i.id === id);
    if (item) onEdit(`Remove ${item.key}`, { items: plot.items.filter((i) => i !== item) });
  }
  // A signal the plot keeps but does not draw, as a legend hides a series.
  let menuChip = $state(null); // the signal a right-click on its chip opened the menu for
  let legendMenuOpen = $state(false);
  let legendKey = $state(null); // the signal a right-click on its legend row opened the legend's menu for
  const toggleHidden = (key) => onEdit(`${plot.items.find((i) => i.key === key)?.hidden ? 'Show' : 'Hide'} ${key}`, { items: plot.items.map((i) => (i.key === key ? { ...i, hidden: !i.hidden } : i)) });
  // Whether a spectrum draws the signal's noise floor line, which its chip's eye turns on and off.
  const toggleFloor = (key) => onEdit(`${plot.items.find((i) => i.key === key)?.floorHidden ? 'Show' : 'Hide'} ${key}'s noise floor`, { items: plot.items.map((i) => (i.key === key ? { ...i, floorHidden: !i.floorHidden } : i)) });
  /** The units of what the plot shows of signal `key`: a spectrum's are per √Hz. */
  const unitsShown = (key) => unitsOf(key) && (plot.kind === 'spectrum' ? `${unitsOf(key)}/√Hz` : unitsOf(key));
  const showOnly = (key) => onEdit(`Show only ${key}`, { items: plot.items.map((i) => ({ ...i, hidden: i.key !== key })) });
  const showAll = () => onEdit('Show every signal', { items: plot.items.map((i) => ({ ...i, hidden: false })) });
  /** Whether `e` happened on the header itself or the empty part of its chips. */
  const empty = (e) => e.target === e.currentTarget || e.target.classList?.contains('trace-zone');

  // The keyboard. The plot is one stop in the page's Tab order; nothing in
  // it takes a stop of its own. Keys pressed while it has focus act on it,
  // and Enter moves into its signals, where the arrows move between them.
  // Which key does what is keymap.js's.
  let card = $state();
  let zone = $state();
  let kindOpen = $state(false);
  let kindFromKeys = false;
  /** Opens the kind picker, which takes the keys until it closes. */
  function openKind() {
    kindFromKeys = true;
    kindOpen = true;
  }
  /** Gives focus back to the plot when a kind menu opened from the keys closes, after the closing key is done. */
  function kindClosed(open) {
    if (open || !kindFromKeys) return;
    kindFromKeys = false;
    clock.after(0, () => card?.focus());
  }
  let active = $state(0); // the signal the arrows are on, while the signals have focus
  /**
   * Makes signal `key` the one the signals' keys act on, the signals
   * focused: what a right-click on its chip or its legend row does, so the
   * keys its menu shows act on it once the menu has closed. Returns `key`.
   */
  function makeCurrent(key) {
    const at = plot.items.findIndex((i) => i.key === key);
    if (at < 0) return key;
    active = at;
    zoneFocused = true;
    zone?.focus();
    return key;
  }
  let zoneFocused = $state(false);
  let addFromKeys = false;
  /** Opens the add-a-signal menu under the plot's signals. */
  /**
   * Opens the add-signal menu under `below`, the signals' row by default, as
   * a right-click on the row does. From the keys, closing it gives focus back
   * to the plot.
   */
  function openAdd(below = zone, fromKeys = true) {
    const r = below.getBoundingClientRect();
    addFromKeys = fromKeys;
    zone.dispatchEvent(new MouseEvent('contextmenu', { bubbles: true, cancelable: true, clientX: r.left + 4, clientY: r.bottom }));
  }
  let addInput = $state(null);
  /** Removes every signal and keeps the menu open on its filter, as adding one does, to add others. */
  function removeAll(e) {
    e.preventDefault();
    onEach('Remove every signal', () => ({ items: [] }));
    tick().then(() => addInput?.focus());
  }
  /** Gives focus back to the plot when a menu opened from the keys closes. */
  function signalsClosed(open) {
    if (open || !addFromKeys) return;
    addFromKeys = false;
    queueMicrotask(() => card?.focus());
  }

  /** Where a legend sits before it is dragged: the top right corner. */
  const LEGEND_AT = { x: 1, y: 0 };
  /** Where the cursors' readout sits before it is dragged: the top left corner. */
  const READOUT_AT = { x: 0, y: 0 };
  /** The space between the readout and the plot's edge, in px, as plot.css keeps it. */
  const READOUT_INSET = 4;
  /**
   * The space between the legend and the plot area's edges, in px, as
   * plot.css keeps it: its top clears the LIVE tag, and a monitor's header
   * while it shows.
   */
  const LEGEND_INSET = { side: 8, top: 22, monitorTop: 32 };
  /** How near, in px, a dragged legend or readout comes to an edge or the middle before it snaps there. */
  const LEGEND_SNAP = 12;
  /** The legend or readout being dragged, {place, at}: shown where the pointer has it, and kept once let go. */
  let overlayDrag = $state.raw(null);
  /** Where the legend or readout `place` sits: `plot[place]`, or where a drag has it, or else `fallback`. */
  const atOf = (place, fallback) => (overlayDrag?.place === place ? overlayDrag.at : plot[place]) ?? fallback;
  /**
   * Drags the legend or the cursors' readout within the plot, keeping where
   * it sits in `plot[place]`, as one edit when it is let go. That is a share
   * of the room around it, 0 to 1 across and down, so it stays in its place,
   * a corner or between, as the plot resizes. Across and down, it snaps to
   * either edge and the middle: the nine places of a corner, an edge's
   * middle or the centre.
   */
  function dragOverlay(e, place) {
    // Its drag is its own, not the plot's pan.
    e.stopPropagation();
    if (e.button !== 0 || e.target.closest('.x, .swatch')) return;
    e.preventDefault();
    // A menu opened on it stays where it opened, so picking it up closes that.
    readoutOpen = false;
    legendMenuOpen = false;
    menuOpen = false;
    const own = e.currentTarget.getBoundingClientRect();
    const [dx, dy] = [e.clientX - own.left, e.clientY - own.top];
    // The room it moves in, in client px: the legend's within the plot area, the readout's within the body.
    const b = body.getBoundingClientRect(), P = pv.geo?.plot;
    const room = place === 'legendAt' && P
      ? { l: P.x + LEGEND_INSET.side, t: Math.max(P.y + LEGEND_INSET.top, b.top + (monitor ? LEGEND_INSET.monitorTop : 0)), r: P.x + P.w - LEGEND_INSET.side, b: P.y + P.h - LEGEND_INSET.side }
      : { l: b.left + READOUT_INSET, t: b.top + READOUT_INSET, r: b.right - READOUT_INSET, b: b.bottom - READOUT_INSET };
    const share = (offset, room) => {
      if (room <= 0) return 0;
      const at = Math.min(1, Math.max(0, offset / room));
      return [0, 0.5, 1].find((a) => Math.abs(a - at) * room < LEGEND_SNAP) ?? at;
    };
    // The pointer is held to it, so the drag ends where it was let go, or
    // with nothing kept when the pointer is lost or Escape is pressed.
    const el = e.currentTarget, pointer = e.pointerId;
    // A press made by script has no pointer to hold.
    if (e.isTrusted) el.setPointerCapture(pointer);
    const move = (ev) => {
      const at = { x: share(ev.clientX - dx - room.l, room.r - room.l - own.width), y: share(ev.clientY - dy - room.t, room.b - room.t - own.height) };
      overlayDrag = { place, at };
    };
    const end = (keep) => {
      el.removeEventListener('pointermove', move);
      el.removeEventListener('pointerup', up);
      el.removeEventListener('lostpointercapture', lost);
      unclaim();
      if (el.hasPointerCapture(pointer)) el.releasePointerCapture(pointer);
      endOverlayDrag = null;
      const d = overlayDrag;
      overlayDrag = null;
      if (d && keep) onEdit(place === 'legendAt' ? 'Move the legend' : 'Move the readout', { [place]: d.at });
    };
    const up = () => end(true);
    const lost = () => end(false);
    const unclaim = claim('drag', { on: { cancel: () => end(false) } });
    el.addEventListener('pointermove', move);
    el.addEventListener('pointerup', up);
    el.addEventListener('lostpointercapture', lost);
    endOverlayDrag = () => end(false);
  }
  /** Ends a legend or readout drag with nothing kept: the plot going away mid-drag. */
  let endOverlayDrag = null;
  onDestroy(() => endOverlayDrag?.());
  /** Opens this plot's picker for keymap.js plot action `action`, as the page asks when the keys act on this plot; says whether it has one. */
  function openPicker(action) {
    const open = { enterSignals: () => (plot.items.length ? ((active = 0), zone.focus()) : openAdd()), addSignals: () => openAdd(), kind: openKind }[action];
    open?.();
    return !!open;
  }
  // The keys of the plot's signals while they have focus (keymap.js, place
  // 'signals'); the rest go on to the plot. A key of theirs shows the
  // signal the arrows are on, however the row got focus.
  onDestroy(
    claim('signals', {
      when: (e, at) => at === zone,
      on: {
        move(e) {
          zoneFocused = true;
          active = stepIn(e, active, plot.items.length);
          document.getElementById(`chip-${plot.items[active]?.id}`)?.scrollIntoView({ block: 'nearest', inline: 'nearest' });
        },
        remove() {
          zoneFocused = true;
          remove(plot.items[active]?.id);
          active = Math.min(active, plot.items.length - 1);
          if (!plot.items.length) card.focus();
        },
        leave: () => card.focus(),
      },
    }),
  );
  const description = $derived(`${kindLabel(plot.kind, plot.mode)} plot: ${plot.items.map((i) => i.key).join(', ') || 'no signals'}`);
</script>

<!-- A row's value in its trailing column, when it has one. -->
{#snippet value(text)}{#if text}<span class="menu-value">{text}</span>{/if}{/snippet}
<!-- The distance between X1 and X2, after a ruler, when a pair is placed. -->
{#snippet span()}{@const d = xSpan(targetPvs())}{#if d}<span class="menu-value"><RiSpan />{d}</span>{/if}{/snippet}
<!-- A signal's own menu, on its chip and on its row of the legend: what
     it shows, how it looks, where it goes. `close` closes the menu it is in. -->
{#snippet signalMenu(key, close)}
  {@const item = plot.items.find((i) => i.key === key)}
  {@const side = slots ? (slots.left.includes(key) ? 'left' : slots.right.includes(key) ? 'right' : 'none') : null}
  {@const hides = plot.items.filter((i) => i.key !== key && !i.hidden).length}
  <!-- Two rows: this signal alone or every signal, whichever it is not showing; and this signal hidden or shown. -->
  {#if !hides && !item?.hidden && plot.items.some((i) => i.hidden)}
    <ContextMenu.Item onSelect={showAll}><RiShowOnly class="menu-icon" />Show all</ContextMenu.Item>
  {:else}
    <ContextMenu.Item onSelect={() => showOnly(key)}><RiShowOnly class="menu-icon" />Show only this{@render value(hides ? `Hides ${hides}` : null)}</ContextMenu.Item>
  {/if}
  <ContextMenu.Item onSelect={() => toggleHidden(key)}>{#if item?.hidden}<RiShowOnly class="menu-icon" />Show{:else}<RiHide class="menu-icon" />Hide{/if}</ContextMenu.Item>
  <ContextMenu.Separator />
  <ContextMenu.Sub>
    <ContextMenu.SubTrigger><RiPalette class="menu-icon" />Colour<span class="menu-value"><i style="--c: {colorFor(key)}"></i>{#if colors.picked[key] === undefined}Auto{/if}</span></ContextMenu.SubTrigger>
    <ContextMenu.SubContent class="p-1.5"><ColorPicker {key} ondone={close} /></ContextMenu.SubContent>
  </ContextMenu.Sub>
  {#if side}
    <ContextMenu.Sub>
      <ContextMenu.SubTrigger><RiAxis class="menu-icon" />Axis{@render value(common(targetPlots().filter((p) => p.mode === 'independent' && p.items.some((i) => i.key === key)).map((p) => sideOf(p, key))))}</ContextMenu.SubTrigger>
      <ContextMenu.SubContent>
        <ContextMenu.RadioGroup value={side} onValueChange={(v) => setSide(key, v)}>
          <ContextMenu.RadioItem value="left">Left</ContextMenu.RadioItem>
          <ContextMenu.RadioItem value="right">Right</ContextMenu.RadioItem>
          <ContextMenu.RadioItem value="none">None</ContextMenu.RadioItem>
        </ContextMenu.RadioGroup>
      </ContextMenu.SubContent>
    </ContextMenu.Sub>
  {/if}
  <ContextMenu.Sub>
    <ContextMenu.SubTrigger><RiMove class="menu-icon" />Move to{@render value(counted(otherPlots.length, 'plot'))}</ContextMenu.SubTrigger>
    <ContextMenu.SubContent class="max-w-72">
      <ContextMenu.Item onSelect={() => onMoveSignal?.(key, null)}>New plot</ContextMenu.Item>
      {#if otherPlots.length}<ContextMenu.Separator />{/if}
      {#each otherPlots as p (p.id)}<ContextMenu.Item onSelect={() => onMoveSignal?.(key, p.id)}><span class="truncate">{p.label}</span></ContextMenu.Item>{/each}
    </ContextMenu.SubContent>
  </ContextMenu.Sub>
  <ContextMenu.Separator />
  <ContextMenu.Item variant="destructive" onSelect={() => item && remove(item.id)}><RiTrash class="menu-icon" />Remove<ContextMenu.Shortcut><kbd>Delete</kbd></ContextMenu.Shortcut></ContextMenu.Item>
{/snippet}
<div
  class:monitor
  class="plot-card flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden border bg-transparent {selected ? 'selected border-primary' : 'border-border'}"
  bind:this={card}
  onpointerdowncapture={pressed}
  oncontextmenucapture={() => onPress?.()}
  onclick={clicked}
  onfocusin={onFocus}
  role="group"
  tabindex="0"
  data-focus="plot"
  data-focus-arrows
  aria-label={description}
>
  <!-- The header picks the plot up from anywhere empty, as a title bar does. -->
  <div
    class="plot-head flex h-7 shrink-0 cursor-grab items-center gap-1.5 border-b bg-card px-1.5"
    ondblclick={(e) => empty(e) && runHere('maximize')}
    onpointerdown={(e) => !e.shiftKey && empty(e) && onStartMove(e)}
    role="toolbar"
    tabindex="-1"
  >
    <IconButton icon={RiDraggable} label="Drag onto a plot's edge to dock, its middle to swap, or a tab to move" class="grip cursor-grab touch-none text-muted-foreground" tabindex={-1} data-focus-skip onpointerdown={(e) => !e.shiftKey && onStartMove(e)} />
    <KindPicker {plot} bind:open={kindOpen} onOpenChange={kindClosed} onKind={(v) => onEach(`Kind ${nameIn(KINDS, v)}`, () => ({ kind: v }))} onMode={(v) => onEach(`Layout ${nameIn(MODES, v)}`, () => ({ mode: v }))} />
    <!-- The plot's signals, each with its value at the hover, or its newest
         while live, unless the legend shows the values. Right-clicking them
         adds or removes signals. -->
    <ContextMenu.Root bind:open={signalsOpen} onOpenChange={signalsClosed}>
      <ContextMenu.Trigger>
        {#snippet child({ props })}
          <div
            {...props}
            bind:this={zone}
            class="trace-zone"
            role="listbox"
            data-focus="signals"
            data-focus-own
            aria-label="Signals in this plot"
            aria-activedescendant={zoneFocused && plot.items[active] ? `chip-${plot.items[active].id}` : undefined}
            use:overflowEdges
            use:dndzone={{ items: zoneItems, type: 'trace', flipDurationMs: 0, dropTargetStyle: {}, dropTargetClasses: [], delayTouchStart: true, zoneTabIndex: -1, zoneItemTabIndex: -1, autoAriaDisabled: true, useCursorForDetection: true, morphDisabled: true }}
            onconsider={consider}
            onfinalize={finalize}
            oncontextmenucapture={(e) => ((signalFilter = ''), (addActive = 0), (menuChip = makeCurrent(e.target.closest?.('.trace-chip')?.dataset.key ?? null)))}
            onfocus={(e) => (zoneFocused = e.currentTarget.matches(':focus-visible'))}
            onblur={() => (zoneFocused = false)}
          >
            {#each zoneItems as item, i (item.id)}
              {@const r = legend.rows[item.key]}
              <div id="chip-{item.id}" data-key={item.key} class:hidden-signal={item.hidden} class="trace-chip" class:ghost={item[SHADOW]} data-menu-for={signalsOpen && menuChip === item.key ? '' : undefined} class:unscaled={axisless(item.key)} role="option" aria-selected={zoneFocused && i === active} style="--c:{colorFor(item.key)}" title={axisless(item.key) ? 'No axis of its own: drag it beside the plot\'s left or right edge to give it one' : r?.info}>
                <Swatch key={item.key} />
                <span class="name">{item.key}</span>
                {#if unitsShown(item.key)}<span class="units">{unitsShown(item.key)}</span>{/if}
                {#if plot.kind === 'spectrum'}
                  <!-- A spectrum's chip shows its white noise floor, always,
                       the hover's values being in the legend and the tip; its
                       eye turns the floor's line on the plot on and off. -->
                  <span class="val floor" title={r?.floor === '—' ? 'No flat noise floor in this spectrum yet' : `White noise floor: ${r?.floor ?? ''} ${unitsShown(item.key)}`}>
                    <button class="eye" tabindex="-1" aria-pressed={!item.floorHidden} aria-label="{item.floorHidden ? 'Show' : 'Hide'} the noise floor of {item.key}" onclick={() => toggleFloor(item.key)}>
                      {#if item.floorHidden}<RiHide />{:else}<RiShowOnly />{/if}
                    </button>{r?.floor ?? ''}
                  </span>
                {:else if !plot.legend}
                  <!-- Always there, and as wide as the longest value, so a value that comes and goes on hover never moves the header. The legend, when shown, holds the values instead. -->
                  <span class="val">{r?.value ?? ''}</span>
                {/if}
                <button class="x" tabindex="-1" aria-label="Remove {item.key}" onclick={() => remove(item.id)}>×</button>
              </div>
            {/each}
            {#if !plot.items.length}<span class="trace-empty">Drop signals here, or right-click to add</span>{/if}
          </div>
        {/snippet}
      </ContextMenu.Trigger>
      <ContextMenu.Content class={menuChip ? 'menu-icons' : 'menu-icons w-64'}>
        {#if menuChip}
          {@render signalMenu(menuChip, () => (signalsOpen = false))}
        {:else}
        <div class="add-filter">
          <!-- svelte-ignore a11y_autofocus -->
          <input
            type="text"
            placeholder="Add a signal…"
            aria-label="Add a signal: type to filter"
            autofocus
            role="combobox"
            aria-expanded="true"
            aria-controls="add-{plot.id}"
            aria-activedescendant="add-{plot.id}-{addActive}"
            bind:this={addInput}
            bind:value={signalFilter}
            oninput={() => (addActive = 0)}
            onkeydown={filterKey}
          />
        </div>
        <ScrollArea class="signal-list">
        <div id="add-{plot.id}" role="listbox" aria-label="Signals to add">
          {#each addable as t, i (t.key)}
            <!-- The filter takes the keys for the whole list. -->
            <!-- svelte-ignore a11y_click_events_have_key_events -->
            <div
              id="add-{plot.id}-{i}"
              class="signal-row"
              class:active={i === addActive}
              role="option"
              aria-selected={i === addActive}
              tabindex="-1"
              onpointermove={() => (addActive = i)}
              onpointerdown={(e) => e.preventDefault()}
              onclick={() => addSignal(t.key)}
            >
              <span class="signal-option" style="--c:{colorOf(t)}"><i></i>{t.key}<small>{t.meta.units}</small></span>
              <span class="rate">{t.meta.rate} Hz</span>
            </div>
          {:else}
            <div class="signal-none">{signalFilter ? 'No signal matches' : 'Every signal is shown'}</div>
          {/each}
        </div>
        </ScrollArea>
        {#if plot.items.length}
          <ContextMenu.Separator />
          <ContextMenu.Item variant="destructive" onSelect={removeAll}><RiTrash class="menu-icon" />Remove every signal{@render value(counted(targetPlots().reduce((n, p) => n + p.items.length, 0), 'signal'))}</ContextMenu.Item>
        {/if}
        {/if}
      </ContextMenu.Content>
    </ContextMenu.Root>
    <IconButton icon={RiAddSignal} label="Add a signal" keys={commandOf('addSignals').show} class="add-signal" tabindex={-1} onclick={(e) => openAdd(e.currentTarget, false)} />
    <div class="plot-actions flex shrink-0 items-center">
      <IconButton
        icon={RiLegend}
        label={plot.legend ? 'Hide the legend' : 'Show the legend in the plot'}
        keys={commandOf('legend').show}
        class={plot.legend ? 'text-primary' : ''}
        aria-pressed={!!plot.legend}
        tabindex={-1}
        data-command="legend"
        onclick={() => runHere('legend')}
      />
      <IconButton
        icon={streaming ? RiPause : RiPlay}
        label={streaming ? 'Pause' : 'Follow live'}
        keys={commandOf('pause').show}
        class={streaming ? '' : 'text-primary'}
        tabindex={-1}
        data-command="pause"
        onclick={() => runHere('pause')}
      />
      <!-- Back and Forward through the views of the tab's plots (viewhistory.svelte.js). -->
      <IconButton icon={RiBack} label="Back to the view before" class="view-step" disabled={!viewHistory.canBack} tabindex={-1} onclick={() => viewHistory.back()} />
      <IconButton icon={RiForward} label="Forward to the view after" class="view-step" disabled={!viewHistory.canForward} tabindex={-1} onclick={() => viewHistory.forward()} />
      <IconButton icon={maximizeRow.icon} label={maximizeRow.label} keys={maximizeRow.show} class="maximize" tabindex={-1} data-command="maximize" onclick={() => runHere('maximize')} />
      <IconButton icon={RiClose} label="Close plot" keys={commandOf('close').show} tabindex={-1} data-command="close" onclick={() => runHere('close')} />
    </div>
  </div>

  <!-- The x axis's settings, as a y axis's: a time axis's window and
       stream, a frequency axis's scale, and its cursors. -->
  <Popover.Root bind:open={xAxisOpen}>
    <Popover.Content customAnchor={axisAnchor} side="top" align="start" class="axis-pop w-auto gap-0 p-1.5">
      <div class="axis-head">
        {plot.kind === 'time' ? 'Time' : 'Frequency'}<span class="units">{plot.kind === 'time' ? 's' : 'Hz'}</span>
        {#if plot.kind === 'time'}
          <span class="axis-tools">
            <Tip text={view.live ? 'Pause' : 'Follow live'} keys={commandOf('pause').show}>
              {#snippet children(tip)}<button {...tip} class="icon-btn" aria-label={view.live ? 'Pause' : 'Follow live'} data-command="pause" onclick={() => runHere('pause')}>{#if view.live}<RiPause />{:else}<RiPlay />{/if}</button>{/snippet}
            </Tip>
          </span>
        {/if}
      </div>
      {#if plot.kind === 'time'}
        <div class="axis-row">
          <span>Window</span>
          <WindowSlider value={windowSpan} onchange={(s) => viewHistory.burst('window', targets(), () => forTargets((v) => v.setSpan(s)))} onstart={() => viewHistory.begin()} onend={() => viewHistory.end()} widest={hub.liveSeconds()} />
        </div>
      {:else}
        <!-- The range as a phrase, low to high, in Hz, with the fit to every bin beside it. -->
        <div class="axis-range" role="group" aria-label="Frequency range">
          <input aria-label="Lowest frequency shown" type="text" inputmode="decimal" bind:value={fBounds.lo} onkeydown={(e) => e.key === 'Enter' && applyFBounds() && (xAxisOpen = false)} onblur={applyFBounds} />
          <span>to</span>
          <input aria-label="Highest frequency shown" type="text" inputmode="decimal" bind:value={fBounds.hi} onkeydown={(e) => e.key === 'Enter' && applyFBounds() && (xAxisOpen = false)} onblur={applyFBounds} />
          <Tip text="Every frequency">
            {#snippet children(tip)}<button {...tip} class="icon-btn" aria-label="Show every frequency" onclick={() => (onView({ fRange: null }), (xAxisOpen = false))}><RiFitX /></button>{/snippet}
          </Tip>
        </div>
        {#if fBoundsError}<div class="axis-error">{fBoundsError}</div>{/if}
        <div class="axis-row" role="group" aria-label="Frequency scale">
          <span>Scale</span>
          <div class="seg">
            <button class:on={plot.xScale !== 'linear'} aria-pressed={plot.xScale !== 'linear'} onclick={() => setEach('Log frequency scale', 'xScale', 'log')}>Log</button>
            <button class:on={plot.xScale === 'linear'} aria-pressed={plot.xScale === 'linear'} onclick={() => setEach('Linear frequency scale', 'xScale', 'linear')}>Linear</button>
          </div>
        </div>
      {/if}
      <div class="axis-row" role="group" aria-label="X cursors">
        <span>Cursors</span>
        <Tip text="Add X1 and X2">
          {#snippet children(tip)}<button {...tip} class="icon-btn" aria-label="Add X cursors" data-command="addXCursors" onclick={() => (runHere('addXCursors'), (xAxisOpen = false))}><RiXCursors /></button>{/snippet}
        </Tip>
        <Tip text="Zoom to X1–X2" keys={['Ctrl', 'drag']}>
          {#snippet children(tip)}<button {...tip} class="icon-btn" aria-label="Zoom to the X cursors" onclick={() => (pv?.zoomToCursors(), (xAxisOpen = false))}><RiZoomTo /></button>{/snippet}
        </Tip>
        <Tip text="Clear the X cursors">
          {#snippet children(tip)}<button {...tip} class="icon-btn" aria-label="Clear the X cursors" onclick={() => (clearXCursors(), (xAxisOpen = false))}><RiEraser /></button>{/snippet}
        </Tip>
      </div>
    </Popover.Content>
  </Popover.Root>

  <!-- A y axis's settings: the signal it scales, its bounds, a fit, and the
       signal's colour when the axis is one signal's. -->
  <Popover.Root bind:open={axisOpen}>
    <Popover.Content customAnchor={axisAnchor} side="right" align="start" class="axis-pop w-auto gap-0 p-1.5">
      <div class="axis-head">
        {#if axisKey}<i style="--c: {colorFor(axisKey)}"></i>{axisKey}{#if unitsOf(axisKey)}<span class="units">{unitsOf(axisKey)}</span>{/if}{:else}Y axis{/if}
        {#if axisKey && plot.mode === 'independent'}
          {@const other = region.side === 'left' ? 'right' : 'left'}
          <span class="axis-tools">
            <Tip text="Move this axis to the {other}">
              {#snippet children(tip)}<button {...tip} class="icon-btn" aria-label="Move this axis to the {other}" onclick={() => (setSide(axisKey, other), (axisOpen = false))}>{#if other === 'left'}<RiToLeft />{:else}<RiToRight />{/if}</button>{/snippet}
            </Tip>
            <Tip text="Remove this axis; the signal keeps its own scale">
              {#snippet children(tip)}<button {...tip} class="icon-btn" aria-label="Remove this axis" onclick={() => (setSide(axisKey, 'none'), (axisOpen = false))}><RiRemoveAxis /></button>{/snippet}
            </Tip>
          </span>
        {/if}
      </div>
      <!-- The range as a phrase, low to high, with the fit beside it. -->
      <div class="axis-range" role="group" aria-label="Y axis range">
        <input aria-label="Lowest value shown" type="text" inputmode="decimal" bind:value={bounds.lo} onkeydown={boundsKey} onblur={applyBounds} />
        <span>to</span>
        <input aria-label="Highest value shown" type="text" inputmode="decimal" bind:value={bounds.hi} onkeydown={boundsKey} onblur={applyBounds} />
        <Tip text="Fit to the data" keys={['double-click']}>
          {#snippet children(tip)}<button {...tip} class="icon-btn" aria-label="Fit to the data" onclick={() => (asStep(() => eachAxis(region.keys[0], (p, axis) => p.fit([axis]))), (axisOpen = false))}><RiFit /></button>{/snippet}
        </Tip>
      </div>
      {#if plot.kind === 'spectrum'}
        <div class="axis-row" role="group" aria-label="Density scale">
          <span>Scale</span>
          <div class="seg">
            <button class:on={plot.yScale !== 'linear'} aria-pressed={plot.yScale !== 'linear'} onclick={() => setYScale(region.keys[0], 'log')}>Log</button>
            <button class:on={plot.yScale === 'linear'} aria-pressed={plot.yScale === 'linear'} onclick={() => setYScale(region.keys[0], 'linear')}>Linear</button>
          </div>
        </div>
      {/if}
      <!-- This axis's Y cursors, which a pull up off the x axis also draws out. -->
      <div class="axis-row" role="group" aria-label="Y cursors">
        <span>Cursors</span>
        <Tip text="Add Y1 and Y2 on this axis">
          {#snippet children(tip)}<button {...tip} class="icon-btn" aria-label="Add Y cursors on this axis" data-command="addYCursors" onclick={() => (runHere('addYCursors', region.keys[0]), (axisOpen = false))}><RiYCursors /></button>{/snippet}
        </Tip>
        <Tip text="Clear this axis's cursors">
          {#snippet children(tip)}<button {...tip} class="icon-btn" aria-label="Clear this axis's cursors" onclick={() => (clearYCursors(region.keys[0]), (axisOpen = false))}><RiEraser /></button>{/snippet}
        </Tip>
      </div>
      {#if boundsError}<div class="axis-error">{boundsError}</div>{/if}
      {#if axisKey}
        <div class="axis-sep"></div>
        <ColorPicker key={axisKey} ondone={() => (axisOpen = false)} />
      {/if}
    </Popover.Content>
  </Popover.Root>
  <ContextMenu.Root bind:open={menuOpen}>
    <ContextMenu.Trigger>
      {#snippet child({ props })}
        <div {...props} class="plot-body" bind:this={body} oncontextmenucapture={(e) => (recountCopy(), contextAt(e))}>
          <canvas bind:this={overlay}></canvas>
          <div class="axis-anchor" bind:this={axisAnchor} style="left: {axisAt.x}px; top: {axisAt.y}px"></div>
          <!-- The legend: each signal and its value, one short row apiece,
               no taller than part of the plot, where it was dragged to. A
               row's menu is its chip's. -->
          {#if plot.legend && plot.items.length}
            {@const at = atOf('legendAt', LEGEND_AT)}
            <div class="legend-layer" oncontextmenu={(e) => e.stopPropagation()}>
              <ContextMenu.Root bind:open={legendMenuOpen}>
                <ContextMenu.Trigger>
                  {#snippet child({ props })}
                    <div {...props} class="pinned-legend" data-focus-skip role="list" aria-label="Legend: drag to move" style="--x: {at.x}; --y: {at.y}" onpointerdowncapture={(e) => dragOverlay(e, 'legendAt')} oncontextmenucapture={(e) => (legendKey = makeCurrent(e.target.closest?.('.row')?.dataset.key ?? null))}>
                      {#each plot.items as item (item.id)}
                        <div class="row" role="listitem" data-key={item.key} style="--c:{colorFor(item.key)}" title={item.key}>
                          <Swatch key={item.key} /><span class="name">{item.key}</span><span class="val">{legend.rows[item.key]?.value ?? ''}</span><span class="units">{unitsShown(item.key)}</span>
                          <Tip text="Remove from this plot" keys={['Delete']}>
                            {#snippet children(tip)}<button {...tip} class="x" tabindex="-1" aria-label="Remove {item.key}" onclick={() => remove(item.id)}>×</button>{/snippet}
                          </Tip>
                        </div>
                      {/each}
                    </div>
                  {/snippet}
                </ContextMenu.Trigger>
                <ContextMenu.Content class="menu-icons">
                  {#if legendKey}
                    {@render signalMenu(legendKey, () => (legendMenuOpen = false))}
                  {:else}
                    <CommandItem id="legend" on={plot.id} />
                  {/if}
                </ContextMenu.Content>
              </ContextMenu.Root>
            </div>
          {/if}
          <!-- The cursors' readout, a scope's: where X1 and X2 are, then each
               signal there; then each y axis's Y1 and Y2. Column 1 is X1 or
               Y1, 2 is X2 or Y2. It floats in the plot where it was dragged
               to, as the legend does, taking none of the plot's height, and
               its menu copies it or clears the cursors. -->
          {#if legend.cursors}
            {@const c = legend.cursors}
            {@const extra = !!c.x?.rate || c.ys.some((y) => y.ratio)}
            {@const at = atOf('readoutAt', READOUT_AT)}
            <div class="readout-layer" oncontextmenu={(e) => e.stopPropagation()}>
              <ContextMenu.Root bind:open={readoutOpen}>
                <ContextMenu.Trigger>
                  {#snippet child({ props })}
                    <div {...props} class="cursor-readout" data-focus-skip role="group" aria-label="Cursor readout: drag to move" style="--x: {at.x}; --y: {at.y}" onpointerdowncapture={(e) => dragOverlay(e, 'readoutAt')}>
                      <table>
                        <thead>
                          <tr>
                            <th></th>
                            <th class="a">1</th>
                            <th class="b">2</th>
                            <th>Δ = 2 − 1</th>
                            {#if extra}<th></th>{/if}
                          </tr>
                        </thead>
                        <tbody>
                          {#if c.x}
                            <tr class="position">
                              <th>X <var>{c.x.v}</var></th>
                              <td>{c.x.a}</td>
                              <td>{c.x.b}</td>
                              <td>{c.x.d}</td>
                              {#if extra}<td class="rate">{#if c.x.rate}<span>1/Δ<var>t</var></span> {c.x.rate}{/if}</td>{/if}
                            </tr>
                            {#each plot.items as item (item.id)}
                              {@const r = legend.rows[item.key]}
                              <tr>
                                <th style="--c:{colorFor(item.key)}"><i></i>{item.key}</th>
                                <td>{r?.a ?? '—'}</td>
                                <td>{r?.b ?? '—'}</td>
                                <td>{r?.d ?? '—'}</td>
                                {#if extra}<td></td>{/if}
                              </tr>
                            {/each}
                          {/if}
                          {#each c.ys as y (y.key)}
                            <tr class="position">
                              <th style={y.color ? `--c:${y.color}` : ''}>Y {#if y.color}<i></i>{/if}{y.name}</th>
                              <td>{y.a}</td>
                              <td>{y.b}</td>
                              <td>{y.d}</td>
                              {#if extra}<td class="rate">{y.ratio}</td>{/if}
                            </tr>
                          {/each}
                        </tbody>
                      </table>
                    </div>
                  {/snippet}
                </ContextMenu.Trigger>
                <ContextMenu.Content class="menu-icons">
                  <ContextMenu.Item onSelect={copyReadout}><RiCopy class="menu-icon" />Copy table{@render value(counted((c.x ? 1 + plot.items.length : 0) + c.ys.length, 'row'))}</ContextMenu.Item>
                  {#if c.x && c.x.a !== '—' && c.x.b !== '—'}<ContextMenu.Item onSelect={() => pv?.zoomToCursors()}><RiZoomTo class="menu-icon" />Zoom to X1–X2{@render span()}</ContextMenu.Item>{/if}
                  <ContextMenu.Separator />
                  {#if c.x}<ContextMenu.Item onSelect={clearXCursors}><RiXCursors class="menu-icon" />Clear X cursors{@render value(xFirst())}</ContextMenu.Item>{/if}
                  {#if c.ys.length}<ContextMenu.Item onSelect={() => clearYCursors()}><RiYCursors class="menu-icon" />Clear Y cursors{@render value(counted(targetPvs().reduce((n, p) => n + p.yCursorAxes(), 0), 'axis', 'axes'))}</ContextMenu.Item>{/if}
                  <ContextMenu.Item data-command="clearCursors" onSelect={() => commands.run('clearCursors', { on: plot.id })}><RiEraser class="menu-icon" />Clear all cursors{@render value(commands.row('clearCursors', { on: plot.id }).value)}</ContextMenu.Item>
                </ContextMenu.Content>
              </ContextMenu.Root>
            </div>
          {/if}
          <div
            class="legend-zone"
            use:dndzone={{ items: bodyItems, type: 'trace', flipDurationMs: 0, dropTargetStyle: {}, dropTargetClasses: [], dropAnimationDisabled: true, morphDisabled: true, zoneTabIndex: -1, zoneItemTabIndex: -1 }}
            onconsider={bodyConsider}
            onfinalize={bodyFinalize}
          >
            {#each bodyItems as item (item.id)}<div class="body-ghost"></div>{/each}
          </div>
        </div>
      {/snippet}
    </ContextMenu.Trigger>
    <ContextMenu.Content class="menu-icons">
      <!-- Four groups, each under its name, the most asked for first: the
           plot's data, its cursors, how it shows its signals, and where it
           is; then closing it. Copy data says first when a copy would hold
           too much or come back short, from the hub's count of the rows. -->
      {#if plot.items.length}
        <ContextMenu.Label>Data</ContextMenu.Label>
        <CommandItem id="copyData" on={plot.id} how={copyCheck} />
        <CommandItem id="exportData" on={plot.id} />
      {/if}
      <ContextMenu.Label>Cursors</ContextMenu.Label>
      <!-- Cursors are dragged out of the axes; these place a pair in view, Add Y cursors on the axis under the right-click. -->
      <CommandItem id="addXCursors" on={plot.id} />
      <CommandItem id="addYCursors" on={plot.id} how={region.keys?.[0]} />
      <CommandItem id="clearCursors" on={plot.id} />
      <ContextMenu.Label>Show</ContextMenu.Label>
      <ContextMenu.Sub>
        <ContextMenu.SubTrigger><RiKind class="menu-icon" />Kind{@render value(common(targetPlots().map((p) => nameIn(KINDS, p.kind))))}</ContextMenu.SubTrigger>
        <ContextMenu.SubContent>
          <ContextMenu.RadioGroup value={plot.kind} onValueChange={(v) => onEach(`Kind ${nameIn(KINDS, v)}`, () => ({ kind: v }))}>
            {#each KINDS as [kind, kl]}<ContextMenu.RadioItem value={kind}>{kl}</ContextMenu.RadioItem>{/each}
          </ContextMenu.RadioGroup>
        </ContextMenu.SubContent>
      </ContextMenu.Sub>
      <ContextMenu.Sub>
        <ContextMenu.SubTrigger><RiLayout class="menu-icon" />Layout{@render value(common(targetPlots().map((p) => nameIn(MODES, p.mode))))}</ContextMenu.SubTrigger>
        <ContextMenu.SubContent>
          <ContextMenu.RadioGroup value={plot.mode} onValueChange={(v) => onEach(`Layout ${nameIn(MODES, v)}`, () => ({ mode: v }))}>
            {#each MODES as [mode, ml]}<ContextMenu.RadioItem value={mode}>{ml}</ContextMenu.RadioItem>{/each}
          </ContextMenu.RadioGroup>
        </ContextMenu.SubContent>
      </ContextMenu.Sub>
      <ContextMenu.Label>Arrange</ContextMenu.Label>
      <CommandItem id="splitRight" on={plot.id} />
      <CommandItem id="splitDown" on={plot.id} />
      <CommandItem id="maximize" on={plot.id} />
      <ContextMenu.Sub>
        <ContextMenu.SubTrigger disabled={!otherTabs.length}><RiTab class="menu-icon" />Move to tab{@render value(tabName)}</ContextMenu.SubTrigger>
        <ContextMenu.SubContent>
          {#each otherTabs as t (t.id)}<ContextMenu.Item onSelect={() => onMoveToTab(t.id)}>{t.name}</ContextMenu.Item>{/each}
        </ContextMenu.SubContent>
      </ContextMenu.Sub>
      <ContextMenu.Separator />
      <CommandItem id="close" on={plot.id} />
    </ContextMenu.Content>
  </ContextMenu.Root>
</div>
