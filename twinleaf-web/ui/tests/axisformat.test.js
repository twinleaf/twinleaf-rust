// A y axis's own label, digits, notation and unit, and a spectrum's x
// axis's digits and notation, as its popover sets them (commands.js yLabel,
// yDigits, yNotation, yUnit, xDigits, xNotation; units.js numberText).
// Each setting is the axis's own and writes its tick numbers, its cursor
// tags and the cursors' readout alike, as one undo step, kept with the tab;
// at Auto the plot is drawn as one with no settings. The popover's sample
// line writes a value of the axis as the plot does, and steps its digits.
// The label runs up the axis's outer edge, in room the gutter makes for it,
// clear of its numbers.
import { describe, expect, it } from 'vitest';
import { commands, page as browser } from 'vitest/browser';
import { centre, click, closeAll, editCursors, feed, leafOf, mountPage, pressKey, rightClick, settle, typeInto, wait, withYPair } from './harness/page.js';
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
 * those `saved` keeps, as a page loaded again reads them. Its signals are
 * `signals`, in volts unless `units` names others by key, of size
 * `amplitude`, which the Y cursors' places scale with; with `plots` it has
 * that many such plots, in rows, as `ids` and `pvs`.
 */
async function plotPage(mode = 'overlay', kind = 'time', fields = {}, saved = null, { signals = SIGNALS, units = {}, amplitude = 1, plots = 1 } = {}) {
  const page = await mountPage(saved ? [] : Array.from({ length: plots }, () => signals), plots > 1 ? 'rows' : 'grid', 0, signals, [], saved ? { saved: { [SAVE_KEY]: saved }, fromSaved: true, units } : { units });
  feed(page.hub, 20, amplitude);
  const ids = plotIds(page.ws.layout), [id] = ids;
  if (!saved) {
    page.history.group('Prepare', () => ids.forEach((p) => page.ws.setPlot(p, { mode, kind, ...fields })));
    for (const p of ids) page.views.get(p).pause(clock.now());
  }
  await wait(300);
  await settle();
  const pvs = ids.map((p) => page.pv(p)), [pv] = pvs, spectrum = kind === 'spectrum';
  if (!saved)
    for (const p of ids) editCursors(page, p, (c) => page.pv(p).yAxes.reduce((c, a) => withYPair(c, kind, a.key, amplitude * (spectrum ? 0.05 : 0.1234), amplitude * (spectrum ? 0.2 : 0.5678)), spectrum ? { ...c, f: { a: 3.21, b: 7.65 } } : { ...c, x: { a: 14, b: 16 } }));
  for (const p of pvs) await drawn(p);
  return { page, id, pv, ids, pvs };
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
/** What the sample line's word says at each digits, fewest first, as its − and + step through them. */
const STEP_WORDS = ['auto', '2 fig', '3 fig', '4 fig', '5 fig'];
/** The open popover's sample line: its value, its word, and its − and +. */
function sampleLine() {
  const line = document.querySelector('[data-slot=popover-content] .axis-sample');
  expect(line, 'the popover’s sample line').toBeTruthy();
  return { text: line.querySelector('output').textContent, word: line.querySelector('.axis-step span').textContent, fewer: line.querySelector('button[aria-label="Fewer digits"]'), more: line.querySelector('button[aria-label="More digits"]') };
}
/**
 * Sets axis setting `group` to the value named `name` from the popover of
 * what is at `at`, as a person does, and closes it: Digits by pressing the
 * sample line's − or + until its word names `name`, the others by their
 * button. Returns how many presses it took, each one undo step.
 */
async function choose(at, group, name) {
  await rightClick(at);
  let presses = 0;
  if (group === 'Digits') {
    const want = STEP_WORDS[name === 'Auto' ? 0 : Number(name) - 1];
    for (let { word, fewer, more } = sampleLine(); word !== want && presses < STEP_WORDS.length; { word, fewer, more } = sampleLine()) {
      await click(centre(STEP_WORDS.indexOf(want) > STEP_WORDS.indexOf(word) ? more : fewer));
      presses++;
    }
    expect(sampleLine().word, `the popover's Digits ${name}`).toBe(want);
  } else if (group === 'Unit') {
    const trigger = document.querySelector('[data-slot=popover-content] [data-command=yUnit]');
    expect(trigger, "the popover's Unit").toBeTruthy();
    await click(centre(trigger));
    const option = [...document.querySelectorAll('[data-slot=select-content] [role=option]')].find((o) => o.textContent.trim() === name);
    expect(option, `the popover's Unit ${name}`).toBeTruthy();
    await click(centre(option));
    presses = 1;
  } else {
    const el = document.querySelector(`[data-slot=popover-content] [role=group][aria-label="${group}"] button[aria-label="${name}"]`);
    expect(el, `the popover's ${group} ${name}`).toBeTruthy();
    await commands.mouse([['move', ...centre(el)], ['down'], ['up']]);
    presses = 1;
  }
  await settle();
  await closeAll();
  return presses;
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
          const presses = await choose(axisSpot(pv, key), group, name);
          expect(page.history.undos.length - steps, `${where}: an undo step a press`).toBe(presses);
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
        const presses = await choose(xAxis, group, name);
        expect(page.history.undos.length - steps, `${where}: an undo step a press`).toBe(presses);
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
const AT_AUTO = { label: 'off', digits: null, notation: 'auto', unit: null };

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
    // A y axis's popover in a known unit and in one the page does not know, in each notation; and a plot of nT at pT sizes, at Auto and in pT.
    for (const [name, opts] of [['nT', NT], ['psi', PSI]])
      for (const notation of ['auto', 'plain', 'exp']) {
        const { page, pv } = await plotPage('overlay', 'time', { yFormat: { shared: { label: 'units', notation } } }, null, opts);
        try {
          await rightClick(axisSpot(pv, 'shared'));
          await browser.screenshot({ path: `${SHOTS}/popover-y-${name}-${notation}.png`, element: document.querySelector('[data-slot=popover-content]') });
          await closeAll();
        } finally {
          page.dispose();
        }
      }
    for (const [name, unit] of [['auto', undefined], ['pT', -12]]) {
      const { page, id } = await plotPage('overlay', 'time', { legend: true, yFormat: { shared: { label: 'units', unit } } }, null, NT);
      try {
        await browser.screenshot({ path: `${SHOTS}/nT-${name}.png`, element: leafOf(id) });
      } finally {
        page.dispose();
      }
    }
  }, 120_000);
});

/** A signal in nT, whose values here are of pT size, and one in a unit the page does not know. */
const NT = { signals: ['mag.x'], units: { 'mag.x': 'nT' }, amplitude: 0.005 };
const PSI = { signals: ['press.p'], units: { 'press.p': 'psi' } };
/** The open popover's range fields, as numbers. */
const rangeFields = () => [...document.querySelectorAll('[data-slot=popover-content] [data-command=yRange] input')].map((i) => Number(i.value));
/** The value of range `r` the sample line writes: its middle, or of a range across zero its end further from zero. */
const middle = ([lo, hi]) => (lo < 0 && hi > 0 ? (-lo > hi ? lo : hi) : (lo + hi) / 2);
/** Whether `a` and `b` agree to `rel` of the larger. */
const near = (a, b, rel) => Math.abs(a - b) <= rel * Math.max(Math.abs(a), Math.abs(b));

describe('an axis’s sample line', () => {
  for (const [kind, opts] of [['time', {}], ['spectrum', {}], ['time', NT]])
    it(`(${kind}${opts.units ? ', nT' : ''}) writes a value of the axis as the plot writes it, in each notation, digits and unit, and − and + stop at either end`, async () => {
      const { page, pv } = await plotPage('overlay', kind, {}, null, opts);
      try {
        const key = 'shared', at = axisSpot(pv, key);
        for (const unit of opts.units ? ['Auto', 'pT'] : ['Auto'])
          for (const notation of ['Auto', 'Plain', 'Exponent']) {
            if (opts.units) await choose(at, 'Unit', unit);
            await choose(at, 'Notation', notation);
            await rightClick(at);
            // From Auto, − does nothing; then + steps through every digits, and at 5 does nothing.
            for (let i = 0; i < 6 && sampleLine().word !== 'auto'; i++) await click(centre(sampleLine().fewer));
            for (const [n, word] of STEP_WORDS.entries()) {
              const where = `${kind}, unit ${unit}, ${notation}, ${word}`, line = sampleLine();
              expect(line.word, where).toBe(word);
              await drawn(pv);
              // The sample is the middle of the range, written as the plot writes the axis's values: its Y cursors' readout is that writer's.
              expect(line.text, `${where}: the sample`).toBe(pv.yValueText(key)(middle(pv.yBounds(key))));
              const row = pv.legend.cursors.ys.find((r) => r.key === key), y = pv.yPair(key);
              expect(row.a, `${where}: Y1 as the same writer writes it`).toBe(pv.yValueText(key)(y.a));
              if (unit !== 'Auto') expect(line.text, `${where}: in ${unit}`).toMatch(new RegExp(` ${unit}$`));
              expect(line.fewer.disabled, `${where}: − at the fewest`).toBe(n === 0);
              expect(line.more.disabled, `${where}: + at the most`).toBe(n === STEP_WORDS.length - 1);
              const steps = page.history.undos.length;
              if (n === 0) {
                await click(centre(line.fewer));
                expect([sampleLine().word, page.history.undos.length], `${where}: − stops at Auto`).toEqual(['auto', steps]);
              }
              await click(centre(line.more));
              expect(page.history.undos.length - steps, `${where}: +`).toBe(n === STEP_WORDS.length - 1 ? 0 : 1);
            }
            expect(sampleLine().word, `${kind}: + stops at 5`).toBe('5 fig');
            await closeAll();
          }
      } finally {
        page.dispose();
      }
    }, 120_000);
});

describe('a y axis’s unit', () => {
  /** What plot `pv` wrote of axis `key`, as yTexts, and the pointer's tag on it, the hover's tip, its legend's value at the hover and its label. */
  function texts(pv, key) {
    return { ...yTexts(pv, key), pointer: pv.pressable.find((p) => p.part === 'pointer' && p.key === key)?.value ?? null, tip: pv.pressable.find((p) => p.part === 'tip')?.value ?? null, legend: pv.legend.rows[pv.yAxes.find((a) => a.key === key).plans[0].t.key].value, label: pv.labelBoxes.find((b) => b.axis === key)?.text ?? null };
  }

  it('of a known unit writes the ticks, tags, readout, legend and label in the unit chosen, and the range fields read in it, each one undo step', async () => {
    const { page, id, pv } = await plotPage('overlay', 'time', { yFormat: { shared: { label: 'units' } } }, null, NT);
    try {
      const key = 'shared', at = axisSpot(pv, key);
      const hover = async () => {
        await drawn(pv);
        // At the middle of the time shown, which the gutter's width, as the ticks' text widens it, moves on the page.
        const P = pv.geo.plot;
        leafOf(id).querySelector('.plot-body canvas').dispatchEvent(new PointerEvent('pointermove', { bubbles: true, clientX: P.x + P.w / 2, clientY: P.y + P.h * 0.8 }));
        pv.draw(clock.now());
      };
      await hover();
      const auto = texts(pv, key);
      await rightClick(at);
      const autoFields = rangeFields();
      // At Auto the values take their prefix by their size, the ticks plain numbers in nT.
      expect(document.querySelector('[data-slot=popover-content] [data-command=yUnit]').textContent.trim()).toBe('Auto');
      await closeAll();
      expect(auto.label).toBe('nT');
      for (const s of [...auto.tags, ...auto.readout]) expect(s, 'a value at Auto').toMatch(/ [fp]T$/);
      expect([auto.ticks.length > 0, auto.pointer, auto.tip, auto.legend], 'ticks, a pointer tag, a tip and a legend value drawn').toEqual([true, expect.any(String), expect.any(String), expect.any(String)]);
      expect(auto.tip, 'the tip at Auto').toMatch(/ [fp]T$/);
      for (const [name, e] of [['pT', -12], ['fT', -15], ['µT', -6], ['nT', -9], ['T', 0]]) {
        const where = `Unit ${name}`, steps = page.history.undos.length;
        await choose(at, 'Unit', name);
        expect(page.history.undos.length - steps, `${where}: one undo step`).toBe(1);
        expect(page.ws.plots[id].yFormat, where).toEqual({ [key]: { label: 'units', unit: e } });
        await hover();
        const got = texts(pv, key), by = 10 ** (-9 - e);
        expect(got.label, `${where}: the label`).toBe(name);
        for (const s of [...got.tags, ...got.readout, ...got.cells.filter((c) => c !== '—')]) expect(s, `${where}: a value`).toMatch(new RegExp(`^-?[\\d.]+(e[+-]\\d+)? ${name}$`));
        expect(got.tip, `${where}: the tip`).toMatch(new RegExp(`^-?[\\d.]+(e[+-]\\d+)?( … -?[\\d.]+(e[+-]\\d+)?)? ${name}$`));
        // The numbers written with no unit are those at Auto taken to it.
        expect(got.ticks.length, `${where}: as many ticks`).toBe(auto.ticks.length);
        got.ticks.forEach((t, i) => expect(near(Number(t), Number(auto.ticks[i].replace("−", "-")) * by, 1e-9), `${where}: tick ${t} is ${auto.ticks[i]} nT`).toBe(true));
        expect(near(Number(got.pointer), Number(auto.pointer) * by, 1e-3), `${where}: the pointer's ${got.pointer} is ${auto.pointer} nT`).toBe(true);
        expect(near(Number(got.legend), Number(auto.legend) * by, 0.05), `${where}: the legend's ${got.legend} is ${auto.legend} nT`).toBe(true);
        await rightClick(at);
        rangeFields().forEach((v, i) => expect(near(v, autoFields[i] * by, 1e-5), `${where}: range field ${v} is ${autoFields[i]} nT`).toBe(true));
        expect(document.querySelector('[data-slot=popover-content] [data-command=yUnit]').textContent.trim(), where).toBe(name);
        await closeAll();
      }
      // Undo puts the unit before back, and Auto draws the axis as it was.
      page.history.undo();
      expect(page.ws.plots[id].yFormat).toEqual({ [key]: { label: 'units', unit: -9 } });
      await choose(at, 'Unit', 'Auto');
      expect(page.ws.plots[id].yFormat).toEqual({ [key]: { label: 'units' } });
      await hover();
      expect(texts(pv, key)).toEqual(auto);
    } finally {
      page.dispose();
    }
  }, 120_000);

  it('takes a range typed in the unit it fixes as the same range typed in the reported unit', async () => {
    const { page, pv } = await plotPage('overlay', 'time', {}, null, NT);
    try {
      const key = 'shared', at = axisSpot(pv, key);
      /** Types `lo` and `hi` into the range fields and presses Enter. */
      const typeRange = async (lo, hi) => {
        await rightClick(at);
        const [a, b] = document.querySelectorAll('[data-slot=popover-content] [data-command=yRange] input');
        await typeInto(a, lo);
        await typeInto(b, hi);
        await pressKey('Enter');
        await settle();
        await closeAll();
      };
      for (const [typed, inNT] of [[['-3', '4'], ['-0.003', '0.004']], [['-2.5', '7.25'], ['-0.0025', '0.00725']], [['0.001', '0.002'], ['0.000001', '0.000002']]]) {
        await choose(at, 'Unit', 'Auto');
        await typeRange(...inNT);
        const want = pv.yBounds(key);
        expect(want, `${inNT} typed in nT`).toEqual(inNT.map(Number));
        await typeRange('-1', '1');
        await choose(at, 'Unit', 'pT');
        await typeRange(...typed);
        expect(pv.yBounds(key), `${typed} typed in pT`).toEqual(want);
        // The fields read the range back as it was typed.
        await rightClick(at);
        expect(rangeFields(), `${typed} read back`).toEqual(typed.map(Number));
        await closeAll();
      }
    } finally {
      page.dispose();
    }
  }, 120_000);

  it('is named in the heading of a signal’s axis popover, in place of the signal’s own', async () => {
    const { page, pv } = await plotPage('independent', 'time', {}, null, NT);
    const at = axisSpot(pv, 'mag.x'), heading = () => document.querySelector('[data-slot=popover-content] .axis-head .units')?.textContent;
    try {
      for (const [name, want] of [['Auto', 'nT'], ['pT', 'pT'], ['µT', 'µT'], ['Auto', 'nT']]) {
        await choose(at, 'Unit', name);
        await rightClick(at);
        expect(heading(), `Unit ${name}`).toBe(want);
        await closeAll();
      }
    } finally {
      page.dispose();
    }
  }, 120_000);

  it('is set on the axis in its units of every selected plot, as one undo step', async () => {
    const { page, ids, pvs } = await plotPage('overlay', 'time', {}, null, { ...NT, plots: 3 });
    try {
      page.ws.selection.only([ids[0], ids[2]]);
      await settle();
      const steps = page.history.undos.length;
      await choose(axisSpot(pvs[0], 'shared'), 'Unit', 'pT');
      expect(page.history.undos.length - steps, 'one undo step').toBe(1);
      expect(ids.map((p) => page.ws.plots[p].yFormat ?? null)).toEqual([{ shared: { unit: -12 } }, null, { shared: { unit: -12 } }]);
      for (const p of pvs) await drawn(p);
      expect(pvs.map((p) => yTexts(p, 'shared').readout.every((s) => s.endsWith(' pT')))).toEqual([true, false, true]);
      page.history.undo();
      expect(ids.map((p) => page.ws.plots[p].yFormat ?? null)).toEqual([null, null, null]);
    } finally {
      page.dispose();
    }
  }, 120_000);

  it('of a unit the page does not know is not offered, and its values are written in it as reported', async () => {
    for (const [opts, offered] of [[PSI, false], [{}, true], [NT, true]]) {
      const { page, pv } = await plotPage('overlay', 'time', {}, null, opts);
      try {
        await rightClick(axisSpot(pv, 'shared'));
        const where = opts.units ? Object.values(opts.units)[0] : 'V';
        expect(!!document.querySelector('[data-slot=popover-content] [data-command=yUnit]'), `${where}: a Unit choice`).toBe(offered);
        await closeAll();
        // Y1 and Y2 at 0.1234 and 0.5678, to four figures with no prefix.
        if (!offered) expect(yTexts(pv, 'shared').readout.slice(0, 2), `${where}: as reported`).toEqual(['0.1234 psi', '0.5678 psi']);
      } finally {
        page.dispose();
      }
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
      await choose(axisSpot(pv, b), 'Unit', 'mV');
      await drawn(pv);
      settings = first.page.ws.plots[id].yFormat;
      expect(settings).toEqual({ [a]: { label: 'name', digits: 3 }, [b]: { notation: 'exp', unit: -3 } });
      drawnBefore = [yTexts(pv, a).ticks, yTexts(pv, b), pv.labelBoxes.map((l) => l.text)];
      for (const s of drawnBefore[1].readout) expect(s, 'in mV').toMatch(/ mV$/);
      saved = JSON.parse(localStorage.getItem(SAVE_KEY));
    } finally {
      first.page.dispose();
    }
    const again = await plotPage('independent', 'time', {}, saved);
    try {
      const { pv, id } = again, [a, b] = pv.yAxes.map((x) => x.key);
      expect(again.page.ws.plots[id].yFormat).toEqual(settings);
      expect([yTexts(pv, a).ticks, yTexts(pv, b), pv.labelBoxes.map((l) => l.text)]).toEqual(drawnBefore);
    } finally {
      again.page.dispose();
    }
  }, 120_000);
});
