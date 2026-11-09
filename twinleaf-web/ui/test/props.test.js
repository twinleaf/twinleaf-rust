// No component assigns to a prop it was given, or to a property reached
// through it, except through `$bindable`. Such an assignment makes Svelte read
// that prop through an override the component can set, so the component can
// stop following what its parent passes, and gives the parent's state a
// second writer. Changes go up through a callback or a method instead.
//
// What this sees is what the compiler marks: assignments, `++`, `--` and
// `delete` through the prop's own name. A method that mutates (`push`), an
// alias, or a function the prop is passed to goes unseen.
import { describe, expect, it } from 'vitest';
import { compile } from 'svelte/compiler';
import fs from 'node:fs';
import path from 'node:path';

/** Svelte's PROPS_IS_UPDATED and PROPS_IS_BINDABLE flags (svelte/src/constants.js). */
const UPDATED = 1 << 2, BINDABLE = 1 << 3;

/** The writes the page still makes, as `file prop`, each with what it writes. */
const PENDING = new Map([
  ['lib/Dock.svelte docks', 'whether a panel is being dragged'],
  ['lib/KindPicker.svelte plot', "the plot's kind and mode"],
  ['lib/PlotCard.svelte plot', "the plot's signals, axes, scale and frequency range"],
]);

const SRC = path.resolve('src');
const components = (dir) =>
  fs.readdirSync(dir, { withFileTypes: true }).flatMap((e) => {
    const p = path.join(dir, e.name);
    return e.isDirectory() ? components(p) : e.name.endsWith('.svelte') ? [p] : [];
  });

/** The props component `source` writes to without `$bindable`. */
function written(source, filename) {
  const { code } = compile(source, { filename, generate: 'client' }).js;
  return [...code.matchAll(/\$\.prop\(\$\$props, '(\w+)', (\d+)/g)].filter(([, , f]) => +f & UPDATED && !(+f & BINDABLE)).map(([, prop]) => prop);
}

describe('props', () => {
  const found = components(SRC).flatMap((file) => written(fs.readFileSync(file, 'utf8'), file).map((prop) => `${path.relative(SRC, file).replaceAll('\\', '/')} ${prop}`));

  it('are found when written, and not when only read or bound', () => {
    const c = (script) => `<script>${script}</script>`;
    expect(written(c('let { node } = $props(); const f = () => (node.ratio = 1);'), 'a.svelte')).toEqual(['node']);
    expect(written(c('let { node } = $props(); const f = () => node.ratio;'), 'b.svelte')).toEqual([]);
    expect(written(c('let { node = $bindable() } = $props(); const f = () => (node = 1);'), 'c.svelte')).toEqual([]);
  });
  it('are written only through $bindable, or by a write the page still makes', () => {
    expect(found.filter((w) => !PENDING.has(w))).toEqual([]);
  });
  it('the page still writes are still written, so the list shrinks as each goes', () => {
    expect([...PENDING.keys()].filter((w) => !found.includes(w))).toEqual([]);
  });
});
