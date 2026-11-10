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
//
// A menu row that shows a key is a command's, drawn by CommandItem from the
// command table (commands.js), so its key is the key that runs it: no other
// component writes a `<kbd>` into a menu row, but the rows of commands the
// table does not hold yet (OWN_KEYS).
//
// The key router (keyrouter.js) is the one listener for keys: no other
// module listens for a key on the window or an element, and a component
// hears keys only on a text field, for the field's own editing.
import { describe, expect, it } from 'vitest';
import { compile, parse } from 'svelte/compiler';
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
  {
    what: 'a plot’s cursors',
    owner: 'lib/cursors.js',
    writes: /setPlot\([^)]*\bcursors\b|\.cursors\s*=[^=]|\.cursors\.[\w.[\]]+\s*=[^=]/,
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

/** The menu rows that still write their own keys, as `file keys`, each with the row. */
const OWN_KEYS = new Map([
  ['lib/PlotCard.svelte Delete', 'a signal’s Remove'],
  ['lib/SignalList.svelte Enter', 'the Signals list’s Add to plot'],
  ['lib/SignalList.svelte ⇧ Enter', 'the Signals list’s New plot'],
  ['lib/TabStrip.svelte F2', 'a tab’s Rename'],
  ['lib/Properties.svelte Ctrl C', 'the property rows’ Copy'],
]);

describe('a menu row’s key', () => {
  const found = sources(SRC)
    .filter((file) => file.endsWith('.svelte') && rel(file) !== 'lib/CommandItem.svelte' && !rel(file).startsWith('lib/components/ui/'))
    .flatMap((file) => fs.readFileSync(file, 'utf8').split('\n').map((line) => [rel(file), line]))
    .filter(([, line]) => /\.Item\b.*<kbd>/.test(line))
    .map(([file, line]) => `${file} ${[...line.matchAll(/<kbd>([^<]*)<\/kbd>/g)].map((m) => m[1]).join(' ')}`);

  it('is drawn by CommandItem from the command table, or by a row of a command the table does not hold yet', () => {
    expect(found.filter((row) => !OWN_KEYS.has(row))).toEqual([]);
  });
  it('the rows still to move still write their own, so the list shrinks as each goes', () => {
    expect([...OWN_KEYS.keys()].filter((row) => !found.includes(row))).toEqual([]);
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

/** The one module that listens for keys. */
const KEY_ROUTER = 'lib/keyrouter.js';
/** What a text field is: what is typed into. */
const FIELDS = ['input', 'Input', 'textarea'];
/** A key event's handler as an attribute, and a key event named to listen for. */
const KEY_ATTRIBUTE = /^onkey(down|up|press)(capture)?$/, KEY_LISTENED = /['"`]key(down|up|press)['"`]/;

/** The elements of component `source` that hear a key, as `tag attribute`. */
function keyHandlers(source) {
  const found = [];
  const walk = (node) => {
    if (!node || typeof node !== 'object') return;
    if (Array.isArray(node)) return node.forEach(walk);
    for (const a of Array.isArray(node.attributes) ? node.attributes : []) if (a.type === 'Attribute' && KEY_ATTRIBUTE.test(a.name)) found.push(`${node.name} ${a.name}`);
    Object.values(node).forEach(walk);
  };
  walk(parse(source, { modern: true }).fragment);
  return found;
}

describe('a key', () => {
  const files = sources(SRC).map((file) => [rel(file), fs.readFileSync(file, 'utf8')]);

  it('is found where an element hears it, on a field or not', () => {
    expect(keyHandlers('<div onkeydowncapture={f}><input onkeydown={g} /><Input onkeyup={h} /></div>')).toEqual(['div onkeydowncapture', 'input onkeydown', 'Input onkeyup']);
    expect(keyHandlers('<svelte:window onkeydown={f} /><button onclick={g}>go</button>')).toEqual(['svelte:window onkeydown']);
  });
  it('is heard by a component only on a text field', () => {
    const found = files.filter(([file]) => file.endsWith('.svelte')).flatMap(([file, source]) => keyHandlers(source).map((h) => `${file} ${h}`));
    expect(found.filter((h) => !FIELDS.includes(h.split(' ')[1]))).toEqual([]);
    expect(found.length).toBeGreaterThan(0);
  });
  it('is listened for by the key router alone', () => {
    const found = files
      .filter(([file]) => file !== KEY_ROUTER)
      .flatMap(([file, source]) => source.split('\n').map((line, i) => [file, i + 1, line]))
      .filter(([, , line]) => KEY_LISTENED.test(line) && !/^\s*(\/\/|\*)/.test(line))
      .map(([file, n]) => `${file}:${n}`);
    expect(found).toEqual([]);
  });
});
