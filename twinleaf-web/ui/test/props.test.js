// No component writes to a prop it was given, except through `$bindable`.
// A write to a prop, or to anything reached through it, makes Svelte read
// that prop through an override the component can set, so the component can
// stop following what its parent passes; and the parent's state gains a
// second writer. Changes go up through a callback or a method instead.
import { describe, expect, it } from 'vitest';
import { compile } from 'svelte/compiler';
import { PROPS_IS_BINDABLE, PROPS_IS_UPDATED } from '../node_modules/svelte/src/constants.js';
import fs from 'node:fs';
import path from 'node:path';

/** The writes still to move, as `file prop`, each with the step that moves it. */
const PENDING = new Map([
  ['lib/Tile.svelte node', 'tiles drawn from the layout'],
  ['lib/Dock.svelte docks', 'docks without panes'],
  ['lib/TabStrip.svelte ws', 'the document written by Workspace only'],
  ['lib/KindPicker.svelte plot', 'the document written by Workspace only'],
  ['lib/PlotCard.svelte plot', 'the document written by Workspace only'],
]);

const SRC = path.resolve('src');
const components = (dir) =>
  fs.readdirSync(dir, { withFileTypes: true }).flatMap((e) => {
    const p = path.join(dir, e.name);
    return e.isDirectory() ? components(p) : e.name.endsWith('.svelte') ? [p] : [];
  });

/** Every `file prop` whose component writes to it without `$bindable`. */
function writtenProps() {
  const out = [];
  for (const file of components(SRC)) {
    const { code } = compile(fs.readFileSync(file, 'utf8'), { filename: file, generate: 'client' }).js;
    for (const [, prop, flags] of code.matchAll(/\$\.prop\(\$\$props, '(\w+)', (\d+)/g)) {
      if (+flags & PROPS_IS_UPDATED && !(+flags & PROPS_IS_BINDABLE)) out.push(`${path.relative(SRC, file).replaceAll('\\', '/')} ${prop}`);
    }
  }
  return out.sort();
}

describe('props', () => {
  const written = writtenProps();
  it('are written only through $bindable, or by a write still to move', () => {
    expect(written.filter((w) => !PENDING.has(w))).toEqual([]);
  });
  it('still to move are still written, so the list shrinks as each moves', () => {
    expect([...PENDING.keys()].filter((w) => !written.includes(w))).toEqual([]);
  });
});
