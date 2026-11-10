// What the code's structure keeps true, checked by reading the source: each
// piece of state has one writer.
//
// No component assigns to a prop it was given, or to a property reached
// through it, except through `$bindable`. Such an assignment makes Svelte read
// that prop through an override the component can set, so the component can
// stop following what its parent passes, and gives the parent's state a
// second writer. Changes go up through a callback or a method instead.
// What this sees is what the compiler marks: assignments, `++`, `--` and
// `delete` through the prop's own name. A method that mutates (`push`), an
// alias, or a function the prop is passed to goes unseen.
//
// State that one module owns is written nowhere else (OWNED): a line outside
// the owner that matches its `writes` fails.
import { describe, expect, it } from 'vitest';
import { compile } from 'svelte/compiler';
import fs from 'node:fs';
import path from 'node:path';

/** Svelte's PROPS_IS_UPDATED and PROPS_IS_BINDABLE flags (svelte/src/constants.js). */
const UPDATED = 1 << 2, BINDABLE = 1 << 3;

/** The writes the page still makes, as `file prop`, each with what it writes. */
const PENDING = new Map([
  ['lib/Dock.svelte docks', 'whether a panel is being dragged'],
]);

/** State one module owns: `writes` matches a line that changes it. */
const OWNED = [
  {
    what: 'the selection of plots and rows',
    owner: 'lib/selection.svelte.js',
    writes: /\.selected\.(add|delete|clear)\(|\bfocusedId\s*=[^=]|\.selection\.(plots\.(add|delete|clear)\(|last\s*=[^=]|rows\s*=[^=])/,
  },
];

const SRC = path.resolve('src');
/** Every source file under `dir` but the tests. */
const sources = (dir) =>
  fs.readdirSync(dir, { withFileTypes: true }).flatMap((e) => {
    const p = path.join(dir, e.name);
    return e.isDirectory() ? sources(p) : /\.(js|svelte)$/.test(e.name) && !e.name.endsWith('.test.js') ? [p] : [];
  });
const rel = (file) => path.relative(SRC, file).replaceAll('\\', '/');

/** The props component `source` writes to without `$bindable`. */
function written(source, filename) {
  const { code } = compile(source, { filename, generate: 'client' }).js;
  return [...code.matchAll(/\$\.prop\(\$\$props, '(\w+)', (\d+)/g)].filter(([, , f]) => +f & UPDATED && !(+f & BINDABLE)).map(([, prop]) => prop);
}

describe('props', () => {
  const found = sources(SRC)
    .filter((file) => file.endsWith('.svelte'))
    .flatMap((file) => written(fs.readFileSync(file, 'utf8'), file).map((prop) => `${rel(file)} ${prop}`));

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

describe.each(OWNED)('$what', ({ owner, writes }) => {
  it('is written only by its owner', () => {
    const found = sources(SRC)
      .filter((file) => rel(file) !== owner)
      .flatMap((file) => fs.readFileSync(file, 'utf8').split('\n').map((line, i) => [file, i + 1, line]))
      .filter(([, , line]) => writes.test(line) && !/^\s*(\/\/|\*)/.test(line))
      .map(([file, n]) => `${rel(file)}:${n}`);
    expect(found).toEqual([]);
  });
});
