// A y axis's own label, digits and notation, and a spectrum's x axis's
// digits and notation, as its popover sets them (commands.js yLabel,
// yDigits, yNotation, xDigits, xNotation; units.js numberText). Each
// setting is the axis's own and writes its tick numbers, its cursor tags
// and the cursors' readout alike, as one undo step, kept with the tab; at
// Auto the plot is drawn as one with no settings. The label runs up the
// axis's outer edge, in room the gutter makes for it, clear of its numbers.
import { describe, expect, it } from 'vitest';
import { commands, page as browser } from 'vitest/browser';
import { centre, closeAll, editCursors, feed, leafOf, mountPage, rightClick, settle, wait, withYPair } from './harness/page.js';
import * as clock from '../src/lib/clock.js';
import { plotIds } from '../src/lib/layout.js';
import { SAVE_KEY } from '../src/lib/tabs.js';

const SIGNALS = ['sine.sine', 'aux.triangle'];
const MODES = ['overlay', 'independent', 'stacked'];
/** Where VITE_TL_SHOTS names a folder, relative to this file, the screenshots go there. */
const SHOTS = import.meta.env.VITE_TL_SHOTS;
/** The width, in px, the gutter of a labelled axis widens by: the axes' 11 px type and 2 px either side. */
const LABEL_W = 15;
/** Whether boxes `a` and `b`, each {x, y, w, h}, overlap. */
const overlap = (a, b) => a.x < b.x + b.w && b.x < a.x + a.w && a.y < b.y + b.h && b.y < a.y + a.h;

/**
 * A page of one paused plot of `keys` in `mode` and of `kind`, with
 * `fields` set on it, fed, and Y1 and Y2 placed on each y axis and X1 and
 * X2 in view; and the plot's id and PlotView. With `saved` its tabs are
 * those `saved` keeps, as a page loaded again reads them.
 */
async function plotPage(mode = 'overlay', kind = 'time', fields = {}, saved = null) {
  const page = await mountPage(saved ? [] : [SIGNALS], 'grid', 0, SIGNALS, [], saved ? { saved: { [SAVE_KEY]: saved }, fromSaved: true } : {});
  feed(page.hub);
  const id = plotIds(page.ws.layout)[0];
  if (!saved) {
    page.history.group('Prepare', () => page.ws.setPlot(id, { mode, kind, ...fields }));
    page.views.get(id).pause(clock.now());
  }
  await wait(300);
  await settle();
  const pv = page.pv(id), spectrum = kind === 'spectrum';
  if (!saved) editCursors(page, id, (c) => pv.yAxes.reduce((c, a) => withYPair(c, kind, a.key, spectrum ? 0.05 : 0.1234, spectrum ? 0.2 : 0.5678), spectrum ? { ...c, f: { a: 3.21, b: 7.65 } } : { ...c, x: { a: 14, b: 16 } }));
  await drawn(pv);
  return { page, id, pv };
}
/** Lets plot `pv` draw what it holds now, its readout read anew. */
async function drawn(pv) {
  await wait(300);
  await settle();
  pv.draw(clock.now());
}

/** Where y axis `key` of PlotView `pv` is, in client px: the middle of its band and of its lane. */
function axisSpot(pv, key) {
  const a = pv.yAxes.find((a) => a.key === key);
  return [(a.x0 + a.x1) / 2, a.lane.y + a.lane.h / 2];
}
/** Sets axis setting `group` to the value named `name` from the popover of what is at `at`, as a person does, and closes it. */
async function choose(at, group, name) {
  await rightClick(at);
  const el = document.querySelector(`[data-slot=popover-content] [role=group][aria-label="${group}"] button[aria-label="${name}"]`);
  expect(el, `the popover's ${group} ${name}`).toBeTruthy();
  await commands.mouse([['move', ...centre(el)], ['down'], ['up']]);
  await settle();
  await closeAll();
}

/**
 * What plot `pv` drew last of y axis `key` and wrote in the readout: its
 * tick numbers, its Y cursors' value tags, its readout row's Y1, Y2 and Δ,
 * and the cells of the signals it scales at X1 and X2 and between them.
 * The readout's rows are checked against the page's table.
 */
function yTexts(pv, key) {
  const axis = pv.yAxes.find((a) => a.key === key);
  const ticks = pv.tickLabels.filter((l) => l.axis === key).map((l) => l.text);
  const tags = pv.pressable.filter((p) => p.part === 'tag' && p.kind === 'y' && p.key === key && !p.edge && !/^Y[12]$/.test(p.text)).map((p) => p.text);
  const row = pv.legend.cursors?.ys.find((r) => r.key === key);
  const cells = axis.plans.flatMap((p) => ['a', 'b', 'd'].map((c) => pv.legend.rows[p.t.key][c]));
  const table = pv.body.querySelector('.cursor-readout')?.textContent ?? '';
  for (const t of [...row ? [row.a, row.b, row.d] : [], ...cells]) expect(table, `the readout shows ${t}`).toContain(t);
  return { ticks, tags, readout: row ? [row.a, row.b, row.d] : [], cells };
}
/** What a spectrum's x axis drew last and wrote in the readout: its tick numbers, its X cursors' value tags, and the readout's X1, X2 and Δ. */
function xTexts(pv) {
  const x = pv.legend.cursors?.x;
  return {
    ticks: pv.tickLabels.filter((l) => l.axis === undefined).map((l) => l.text),
    tags: pv.pressable.filter((p) => p.part === 'tag' && p.kind === 'x' && !p.edge && !/^X[12]$/.test(p.text)).map((p) => p.text),
    readout: x ? [x.a, x.b, x.d] : [],
  };
}

/** Value text `s` read: whether it has an exponent or an SI prefix, and its significant figures, `nonzero` without the trailing zeros of a whole number. */
function figuresOf(s) {
  const t = s.replace(/\s*(?:[fpnµmkMG]?(?:V|Hz)(?:\/√Hz)?)$/, '');
  const prefixed = /\d\s*[fpnµmkMG](?:V|Hz)/.test(s);
  expect(t, `${s} is a number`).toMatch(/^[-−]?\d+(\.\d+)?(e[+-]\d+)?$/);
  const digits = t.replace(/^[-−]/, '').replace(/e.*$/, '').replace('.', '').replace(/^0+/, '');
  return { exp: /e[+-]\d+$/.test(t), prefixed, whole: !/[.e]/.test(t), figures: digits.length, nonzero: digits.replace(/0+$/, '').length };
}
/** Whether value text `s` has `d` significant figures: exactly, or, a whole number, with as many or more before its trailing zeros. */
const hasFigures = (s, d) => {
  const f = figuresOf(s);
  return f.whole ? f.nonzero <= d && f.figures >= d : f.figures === d;
};

/**
 * Checks that what an axis wrote, `got` (yTexts or xTexts), reads in `set`
 * alike, its ticks, tags and readout: an exponent with Exp, none and no
 * SI prefix with Plain; and to Digits' figures, the ticks to as many or
 * more, so no two read alike. At Auto it is `auto`, what it wrote with no
 * settings; with Auto digits the tags and readout keep Auto's.
 */
function agrees(got, set, auto, where) {
  if (set.digits === null && set.notation === 'auto') return void expect(got, where).toEqual(auto);
  const values = [...got.tags, ...got.readout, ...(got.cells ?? [])];
  expect(values.length && got.ticks.length, `${where}: values and ticks drawn`).toBeTruthy();
  for (const s of [...values, ...got.ticks]) {
    const f = figuresOf(s);
    if (set.notation === 'exp') expect(f.exp, `${where}: ${s} has an exponent`).toBe(true);
    if (set.notation === 'plain') expect(f.exp || f.prefixed, `${where}: ${s} is plain`).toBe(false);
  }
  expect(new Set(got.ticks).size, `${where}: ticks ${got.ticks} read apart`).toBe(got.ticks.length);
  for (const s of values) expect(hasFigures(s, set.digits ?? 4), `${where}: ${s} to ${set.digits ?? 4} figures`).toBe(true);
  // An exponent's figures are its own; plain numbers share the decimals of the largest. Zero has no figures to count.
  if (set.digits && set.notation === 'exp') for (const s of got.ticks.filter((s) => parseFloat(s) !== 0)) expect(figuresOf(s).figures, `${where}: tick ${s} to ${set.digits} figures or more`).toBeGreaterThanOrEqual(set.digits);
  if (set.digits && set.notation !== 'exp') expect(Math.max(...got.ticks.map((s) => figuresOf(s).figures)), `${where}: the largest of ${got.ticks} to ${set.digits} figures or more`).toBeGreaterThanOrEqual(set.digits);
}

/** The settings each case sets, one after another, each as [popover row, button, the axis's settings after it]. */
const STEPS = [
  ['Digits', '2', { digits: 2, notation: 'auto' }],
  ['Digits', '3', { digits: 3, notation: 'auto' }],
  ['Notation', 'Exponent', { digits: 3, notation: 'exp' }],
  ['Digits', '5', { digits: 5, notation: 'exp' }],
  ['Notation', 'Plain', { digits: 5, notation: 'plain' }],
  ['Digits', '4', { digits: 4, notation: 'plain' }],
  ['Digits', 'Auto', { digits: null, notation: 'plain' }],
  ['Notation', 'Exponent', { digits: null, notation: 'exp' }],
  ['Notation', 'Auto', { digits: null, notation: 'auto' }],
];

describe('a y axis’s digits and notation', () => {
  for (const mode of MODES)
    it(`(${mode}) write its tick numbers, cursor tags and readout alike, each one undo step, and leave its other axes as they were`, async () => {
      const { page, id, pv } = await plotPage(mode);
      try {
        const keys = pv.yAxes.map((a) => a.key), [key, ...others] = keys;
        const auto = Object.fromEntries(keys.map((k) => [k, yTexts(pv, k)]));
        for (const [group, name, set] of STEPS) {
          const where = `${mode}, ${key}, ${group} ${name}`, steps = page.history.undos.length;
          await choose(axisSpot(pv, key), group, name);
          expect(page.history.undos.length - steps, `${where}: one undo step`).toBe(1);
          expect({ digits: null, notation: 'auto', ...page.ws.plots[id].yFormat?.[key] }, where).toEqual(set);
          await drawn(pv);
          const got = yTexts(pv, key);
          agrees(got, set, auto[key], where);
          for (const k of others) expect(yTexts(pv, k), `${where}: ${k} as it was`).toEqual(auto[k]);
          // Undo puts the axis back as it was, redo sets it again.
          const before = page.history.undos.length;
          page.history.undo();
          await drawn(pv);
          expect(page.history.undos.length, `${where}: undone`).toBe(before - 1);
          page.history.redo();
          await drawn(pv);
          expect(yTexts(pv, key), `${where}: done again`).toEqual(got);
        }
        // Back at Auto every axis is drawn as it was with no settings.
        for (const k of keys) expect(yTexts(pv, k), `${mode}, ${k} back at Auto`).toEqual(auto[k]);
      } finally {
        page.dispose();
      }
    }, 120_000);

  it('on a spectrum, write its numbers with an exponent at Auto, and its frequencies as its x axis sets them', async () => {
    const { page, id, pv } = await plotPage('overlay', 'spectrum');
    try {
      const autoY = yTexts(pv, 'shared'), autoX = xTexts(pv);
      for (const s of [...autoY.tags, ...autoY.readout]) expect(figuresOf(s).exp, `${s} on Auto`).toBe(true);
      await choose(axisSpot(pv, 'shared'), 'Digits', '2');
      await drawn(pv);
      agrees(yTexts(pv, 'shared'), { digits: 2, notation: 'auto' }, autoY, 'the spectrum, Digits 2');
      // Its frequencies as they were: the gutter, which its numbers widen or narrow, may give the x axis room for another tick.
      const { ticks: _, ...x } = xTexts(pv), { ticks: __, ...wasX } = autoX;
      expect(x, 'its frequencies as they were').toEqual(wasX);
      const P = pv.geo.plot, xAxis = [P.x + P.w / 2, P.y + P.h + 8];
      for (const [group, name, set] of [['Digits', '3', { digits: 3, notation: 'auto' }], ['Notation', 'Plain', { digits: 3, notation: 'plain' }], ['Notation', 'Exponent', { digits: 3, notation: 'exp' }], ['Digits', 'Auto', { digits: null, notation: 'exp' }]]) {
        const where = `the spectrum's frequencies, ${group} ${name}`, steps = page.history.undos.length;
        await choose(xAxis, group, name);
        expect(page.history.undos.length - steps, `${where}: one undo step`).toBe(1);
        expect({ digits: null, notation: 'auto', ...page.ws.plots[id].xFormat }, where).toEqual(set);
        await drawn(pv);
        const got = xTexts(pv);
        expect(got.tags.length && got.readout.length && got.ticks.length, `${where}: drawn`).toBeTruthy();
        for (const s of [...got.tags, ...got.readout, ...got.ticks]) {
          if (set.notation === 'exp') expect(figuresOf(s).exp, `${where}: ${s}`).toBe(true);
          if (set.notation === 'plain') expect(figuresOf(s).exp || figuresOf(s).prefixed, `${where}: ${s}`).toBe(false);
        }
        for (const s of [...got.tags, ...got.readout]) expect(hasFigures(s, set.digits ?? 4), `${where}: ${s}`).toBe(true);
        expect(new Set(got.ticks).size, `${where}: ${got.ticks}`).toBe(got.ticks.length);
      }
      // Its y axis kept what was set on it.
      agrees(yTexts(pv, 'shared'), { digits: 2, notation: 'auto' }, autoY, 'the spectrum, Digits 2, after its frequencies');
    } finally {
      page.dispose();
    }
  }, 120_000);
});

/** Settings at Auto, written out, which a plot draws as one with none. */
const AT_AUTO = { label: 'off', digits: null, notation: 'auto' };

describe('a plot whose axes are at Auto', () => {
  it('is drawn pixel for pixel as one with no axis settings, on every layout and kind', async () => {
    for (const kind of ['time', 'spectrum'])
      for (const mode of MODES) {
        const pictures = [];
        for (const fields of [{}, { yFormat: Object.fromEntries(['shared', ...SIGNALS].map((k) => [k, AT_AUTO])), xFormat: AT_AUTO }]) {
          const { page, id, pv } = await plotPage(mode, kind, fields);
          try {
            pictures.push([pv.overlay.toDataURL(), yTexts(pv, pv.yAxes[0].key), pv.geo.widths]);
            expect(leafOf(id)).toBeTruthy();
          } finally {
            page.dispose();
          }
        }
        expect(pictures[1][1], `${kind}, ${mode}: what it writes`).toEqual(pictures[0][1]);
        expect(pictures[1][2], `${kind}, ${mode}: its gutters`).toEqual(pictures[0][2]);
        expect(pictures[1][0] === pictures[0][0], `${kind}, ${mode}: its picture`).toBe(true);
      }
  }, 120_000);
});

describe('a y axis’s label', () => {
  /** The label drawn on axis `key` of `pv` last, or undefined. */
  const labelOf = (pv, key) => pv.labelBoxes.find((b) => b.axis === key);
  /** Checks the labels `pv` drew: each in its axis's band, at its outer edge, outside its numbers, within its lane below the header and above the resting tabs, clear of every number, tag and tab. */
  function placed(pv, where) {
    for (const b of pv.labelBoxes) {
      const a = pv.yAxes.find((a) => a.key === b.axis), body = pv.body.getBoundingClientRect();
      expect(b.x >= a.x0 - 0.01 && b.x + b.w <= a.x1 + 0.01, `${where}: ${b.text} in its axis's band ${a.x0} to ${a.x1}`).toBe(true);
      expect(b.x >= body.left && b.x + b.w <= body.right, `${where}: ${b.text} in the body`).toBe(true);
      expect(b.y >= a.lane.y && b.y + b.h <= a.lane.y + a.lane.h, `${where}: ${b.text}, ${b.y} to ${b.y + b.h}, in its lane ${a.lane.y} + ${a.lane.h}`).toBe(true);
      for (const t of pv.tickLabels) expect(overlap(b, t), `${where}: ${b.text} clear of the number ${t.text}`).toBe(false);
      for (const p of pv.pressable.filter((p) => p.part === 'tag' || p.part === 'tab')) expect(overlap(b, p), `${where}: ${b.text} clear of ${p.text}`).toBe(false);
      for (const o of pv.labelBoxes) if (o !== b) expect(overlap(b, o), `${where}: ${b.text} clear of ${o.text}`).toBe(false);
    }
  }

  for (const mode of MODES)
    it(`(${mode}) names the axis's units, or its signals with their units, along its outer edge in room the gutter makes for it`, async () => {
      const { page, id, pv } = await plotPage(mode);
      try {
        const keys = pv.yAxes.map((a) => a.key), [key] = keys;
        const width = () => pv.geo.widths[pv.yAxes[0].side][pv.yAxes[0].slot];
        const bare = width();
        const names = pv.yAxes[0].plans.map((p) => p.t.key).join(', ');
        for (const [name, want] of [['Units', 'V'], ['Name and units', `${names} (V)`], ['Off', null]]) {
          const where = `${mode}, ${key}, Label ${name}`, steps = page.history.undos.length;
          await choose(axisSpot(pv, key), 'Label', name);
          expect(page.history.undos.length - steps, `${where}: one undo step`).toBe(1);
          await drawn(pv);
          expect(labelOf(pv, key)?.text ?? null, where).toBe(want);
          // The gutter widens by the label's column, its numbers as they were.
          expect(width(), `${where}: the gutter`).toBe(bare + (want ? LABEL_W : 0));
          placed(pv, where);
          for (const k of keys.slice(1)) expect(labelOf(pv, k), `${where}: ${k} has none`).toBeUndefined();
          if (SHOTS) await browser.screenshot({ path: `${SHOTS}/label-${mode}-${name.replace(/ /g, '-')}.png`, element: leafOf(id) });
        }
        page.history.undo();
        await drawn(pv);
        expect(labelOf(pv, key)?.text, `${mode}: undone, the name again`).toBe(`${names} (V)`);
      } finally {
        page.dispose();
      }
    }, 120_000);

  it('names only the units of an axis too short for its names, and is not drawn on one too short for those', async () => {
    const { page, id, pv } = await plotPage('stacked', 'time', { yFormat: Object.fromEntries(SIGNALS.map((k) => [k, { label: 'name' }])) });
    const seen = new Set();
    try {
      const leaf = leafOf(id);
      for (let h = 700; h >= 120; h -= 4) {
        leaf.style.height = `${h}px`;
        pv.draw(clock.now());
        pv.draw(clock.now());
        const where = `two lanes in a tile ${h} px high`;
        placed(pv, where);
        for (const a of pv.yAxes) {
          const text = labelOf(pv, a.key)?.text ?? null;
          expect([`${a.key} (V)`, 'V', null], `${where}: ${a.key}`).toContain(text);
          seen.add(text === null ? 'none' : text === 'V' ? 'units' : 'name');
        }
        // A gutter widened for no label is not: it widens only while a label is drawn.
        expect(pv.geo.widths.left[0] > 24 + LABEL_W - 1 || !pv.labelBoxes.length, where).toBe(true);
      }
      expect([...seen].sort()).toEqual(['name', 'none', 'units']);
    } finally {
      page.dispose();
    }
  }, 120_000);

  it('looks as it should, with each notation, on each layout', async () => {
    if (!SHOTS) return;
    for (const mode of MODES)
      for (const notation of ['auto', 'plain', 'exp']) {
        const yFormat = Object.fromEntries(['shared', ...SIGNALS].map((k) => [k, { label: 'name', notation, digits: notation === 'auto' ? null : 3 }]));
        const { page, id } = await plotPage(mode, 'time', { yFormat });
        try {
          await browser.screenshot({ path: `${SHOTS}/notation-${mode}-${notation}.png`, element: leafOf(id) });
        } finally {
          page.dispose();
        }
      }
    const { page, id, pv } = await plotPage('overlay', 'spectrum', { yFormat: { shared: { label: 'units', digits: 3 } }, xFormat: { notation: 'plain', digits: 3 } });
    try {
      await browser.screenshot({ path: `${SHOTS}/spectrum.png`, element: leafOf(id) });
      // Each axis's popover, its rows of settings among the others.
      const P = pv.geo.plot;
      for (const [name, at] of [['y', axisSpot(pv, 'shared')], ['x', [P.x + P.w / 2, P.y + P.h + 8]]]) {
        await rightClick(at);
        await browser.screenshot({ path: `${SHOTS}/popover-${name}.png`, element: document.querySelector('[data-slot=popover-content]') });
        await closeAll();
      }
    } finally {
      page.dispose();
    }
  }, 120_000);
});

describe('an axis’s settings', () => {
  it('are saved with its tab, and draw the plot as they did when the page is loaded again', async () => {
    const first = await plotPage('independent', 'time');
    let saved, drawnBefore, settings;
    try {
      const { pv, id } = first, [a, b] = pv.yAxes.map((x) => x.key);
      await choose(axisSpot(pv, a), 'Label', 'Name and units');
      await choose(axisSpot(pv, a), 'Digits', '3');
      await choose(axisSpot(pv, b), 'Notation', 'Exponent');
      await drawn(pv);
      settings = first.page.ws.plots[id].yFormat;
      expect(settings).toEqual({ [a]: { label: 'name', digits: 3 }, [b]: { notation: 'exp' } });
      drawnBefore = [yTexts(pv, a).ticks, yTexts(pv, b).ticks, pv.labelBoxes.map((l) => l.text)];
      saved = JSON.parse(localStorage.getItem(SAVE_KEY));
    } finally {
      first.page.dispose();
    }
    const again = await plotPage('independent', 'time', {}, saved);
    try {
      const { pv, id } = again, [a, b] = pv.yAxes.map((x) => x.key);
      expect(again.page.ws.plots[id].yFormat).toEqual(settings);
      expect([yTexts(pv, a).ticks, yTexts(pv, b).ticks, pv.labelBoxes.map((l) => l.text)]).toEqual(drawnBefore);
    } finally {
      again.page.dispose();
    }
  }, 120_000);
});
