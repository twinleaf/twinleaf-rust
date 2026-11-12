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
// the owner that matches its `writes` fails. Where the owner itself sets it
// in one place, `once` matches that line, and a second in the owner fails.
//
// A menu row that shows a key is a command's, drawn by CommandItem from the
// command table (commands.js), so its key is the key that runs it: no other
// component writes a `<kbd>` into a menu row, but the rows of commands the
// table does not hold yet (OWN_KEYS). Every menu row is drawn by
// CommandItem, but the rows of what the table does not hold (OWN_ROWS): a
// row written where it shows, in any component, fails until it is listed
// there. A command's keys are in the keymap (keymap.js) from the table
// alone: no entry there names a command by hand or changes what the table
// says of its keys. The command palette is handed no actions: the commands
// it lists are the table's. A command reads one way wherever it shows: no
// entry words itself or draws its icon by where it is made, an entry has
// only the fields the table lists, and an axis popover's buttons take their
// words and picture from the table.
//
// The key router (keyrouter.js) is the one listener for keys: no other
// module listens for a key on the window or an element, and a component
// hears keys only on a text field, for the field's own editing.
//
// The clock (clock.js) is the one reader of the time: no other module reads
// the browser's time, asks it for a frame or sets a timer, so a test that
// puts the clock in virtual mode holds all of the page's time. A page test
// reads the time from the clock and lets it pass through its harness
// (tests/harness/page.js), which alone waits real time.
//
// A plot reaches its surroundings through its PlotHost (plothost.js) alone,
// so it is drawn and edited the same wherever it lives, in a tab or as a
// copy on a stage of its own. PlotCard takes its settings and its host and
// no other prop. The plot's own modules (PLOT) import none of the page's
// (PAGE), nor the page's colours, and read the window's size and pixel
// ratio nowhere but in gl.js's `windowFrame`, the page's frame: a plot is
// drawn at the size and pixel ratio of the surface its host has. A plot's
// component asks its host which plots a command acts on only to tell its
// gestures what they move, and edits no other plot itself. And the
// host is the one maker of what its plots share (MADE): the cursors'
// writer, the view steps, the commands and the surface's renderer.
import { describe, expect, it } from 'vitest';
import { compile, parse } from 'svelte/compiler';
import fs from 'node:fs';
import path from 'node:path';
import { COMMANDS } from './lib/commands.js';

/** Svelte's PROPS_IS_UPDATED and PROPS_IS_BINDABLE flags (svelte/src/constants.js). */
const UPDATED = 1 << 2, BINDABLE = 1 << 3;

/** The writes the page still makes, as `file prop`, each with what it writes. */
const PENDING = new Map([]);

/** State one module owns: `writes` matches a line that changes it, and `once`, where there is one, the owner's one line that sets it. */
const OWNED = [
  {
    what: 'the maximized plots',
    owner: 'lib/workspace.svelte.js',
    writes: /\.#?maximized\s*(=[^=>]|\.(push|pop|shift|unshift|splice|sort|reverse|fill)\()/,
    once: /\.#maximized\s*=[^=>]/,
  },
  {
    what: 'the selection of plots and rows',
    owner: 'lib/selection.svelte.js',
    writes: /\.selected\.(add|delete|clear)\(|\bfocusedId\s*=[^=]|\.selection\.(plots\.(add|delete|clear)\(|last\s*=[^=]|rows\s*=[^=])/,
  },
  {
    what: 'which dock holds a panel, whether a dock is hidden, and whether a panel is dragged',
    owner: 'lib/docks.svelte.js',
    writes: /\bdocks\.(left|right|hidden|dragging)\b[\w.[\]]*\s*=[^=>]|\.#(places|hidden|dragging)\s*=[^=>]/,
    once: /\.#places\s*=[^=>]/,
  },
  {
    what: 'a plot’s cursors',
    owner: 'lib/cursors.js',
    writes: /setPlot\([^)]*\bcursors\b|\.cursors\s*=[^=]|\.cursors\.[\w.[\]]+\s*=[^=]/,
  },
  {
    what: 'the pickers the keys open on a plot',
    owner: 'lib/PlotCard.svelte',
    writes: /\.pickers\.(set|delete|clear)\(/,
  },
  {
    what: 'whether the hub holds its history for a host’s paused plots',
    owner: 'lib/plothost.js',
    writes: /\.hold\(/,
  },
  {
    what: 'the renderer of a host’s surface',
    owner: 'lib/plothost.js',
    writes: /\.renderer\s*=[^=>]/,
  },
  {
    what: 'what the page counts lost',
    owner: 'lib/loss.svelte.js',
    writes: /\.(behind|recent|resets)\s*(=[^=>]|\+\+)|\bloss\.devices\.(set|delete|clear)\(/,
  },
  {
    what: 'what the page serves its plots',
    owner: 'App.svelte',
    writes: /\.serve\(/,
    once: /\.serve\(/,
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

/** A row of a menu, as a component writes one: an item, a submenu's trigger, or a choice. */
const MENU_ROW = /^(ContextMenu|DropdownMenu)\.(Item|SubTrigger|RadioItem|CheckboxItem)$/;
/** The one component that draws a command's row. */
const COMMAND_ITEM = 'lib/CommandItem.svelte';
/**
 * The menu rows still written where they show, by file, each by its words,
 * or {…} for a row whose words are all worked out: the rows of what the
 * command table does not hold.
 */
const OWN_ROWS = {
  // A signal of a plot, on its chip and its legend row: what it shows, its colour, the plot it moves to, and its removal; and the heading of the peak commands' submenu.
  'lib/PlotCard.svelte': ['Show all', 'Show only this', '{…}', 'Colour', 'Move to', 'New plot', '{…}', 'Remove', 'X cursors to peaks'],
  // A dock's panels.
  'lib/Dock.svelte': ['{…}', 'Move up', 'Move down', 'Move to the dock', 'Hide the dock', 'Reset every panel'],
  // Property rows.
  'lib/Properties.svelte': ['Copy', 'Add to Favorites', 'Remove from Favorites'],
  // A signal of the Signals list: its colour and its name.
  'lib/SignalList.svelte': ['Colour', 'Copy name'],
};

/** The menu rows component `source` writes, each by its words, in order. */
function menuRows(source) {
  const found = [];
  const walk = (node) => {
    if (!node || typeof node !== 'object') return;
    if (Array.isArray(node)) return node.forEach(walk);
    if (node.type === 'Component' && MENU_ROW.test(node.name)) found.push((node.fragment?.nodes ?? []).filter((n) => n.type === 'Text').map((n) => n.data).join(' ').replace(/\s+/g, ' ').trim() || '{…}');
    Object.values(node).forEach(walk);
  };
  walk(parse(source, { modern: true }).fragment);
  return found;
}

describe('a menu row', () => {
  const found = Object.fromEntries(
    sources(SRC)
      .filter((file) => file.endsWith('.svelte') && rel(file) !== COMMAND_ITEM && !rel(file).startsWith('lib/components/ui/'))
      .map((file) => [rel(file), menuRows(fs.readFileSync(file, 'utf8'))])
      .filter(([, rows]) => rows.length),
  );

  it('is found where a component writes one, by its words', () => {
    expect(menuRows('<ContextMenu.Item onSelect={f}><Icon />Close<span>{n}</span></ContextMenu.Item><DropdownMenu.SubTrigger>{name}</DropdownMenu.SubTrigger><ContextMenu.Separator />')).toEqual(['Close', '{…}']);
    expect(menuRows('<CommandItem id="close" /><Command.Item value="x">Close</Command.Item><Select.Item value="a">A</Select.Item>')).toEqual([]);
  });
  it('is drawn by CommandItem from the command table, or is a row of what the table does not hold', () => {
    expect(found).toEqual(OWN_ROWS);
  });
});

describe('a command’s key', () => {
  const keymap = fs.readFileSync(path.join(SRC, 'lib/keymap.js'), 'utf8');
  /** The lines of keymap source `source` that give a command its keys by hand: an entry that names a command as its action, or one that changes what the table says. */
  const byHand = (source) => source.split('\n').flatMap((line, i) => (!/^\s*(\/\/|\*)/.test(line) && (COMMANDS.some((c) => new RegExp(`action:\\s*['"\`]${c.id}['"\`]`).test(line)) || /\{\s*\.\.\.keysOf\(/.test(line)) ? [i + 1] : []));

  it('is found where the keymap gives one by hand', () => {
    expect(byHand("{ show: ['F2'], keys: ['F2'], action: 'renameTab', what: 'Rename tab' },\n{ ...keysOf(commandOf('copyData')), keys: ['c'], ctrl: true },\n// action: 'close'\nkeysOf(commandOf('pause')),\n{ keys: ['Enter'], action: 'open' },")).toEqual([1, 2]);
  });
  it('is listed in the keymap from the command table alone', () => {
    expect(byHand(keymap)).toEqual([]);
    expect(keymap).toContain('keysOf(commandOf(');
  });
});

describe.each(OWNED)('$what', ({ owner, writes, once }) => {
  /** The lines of `files` that match `pattern`, comments left out, as `file:line`. */
  const matching = (files, pattern) =>
    files
      .flatMap((file) => fs.readFileSync(file, 'utf8').split('\n').map((line, i) => [file, i + 1, line]))
      .filter(([, , line]) => pattern.test(line) && !/^\s*(\/\/|\*)/.test(line))
      .map(([file, n]) => `${rel(file)}:${n}`);

  it('is written only by its owner', () => {
    expect(matching(sources(SRC).filter((file) => rel(file) !== owner), writes)).toEqual([]);
  });
  if (once) {
    it('is set by its owner in one place', () => {
      expect(matching([path.join(SRC, owner)], once)).toHaveLength(1);
    });
  }
});

/** The plot's own modules: what draws a plot and its controls, wherever it lives. */
const PLOT = ['lib/PlotCard.svelte', 'lib/PlotStage.svelte', 'lib/plot.js', 'lib/gl.js', 'lib/commands.js', 'lib/CommandItem.svelte', 'lib/KindPicker.svelte', 'lib/Swatch.svelte', 'lib/ColorPicker.svelte'];
/** The page's modules, by file name, which a plot reaches through its host alone. */
const PAGE = ['App.svelte', 'main.js', 'plothost.js', 'workspace.svelte.js', 'selection.svelte.js', 'history.svelte.js', 'viewhistory.svelte.js', 'views.js', 'drag.svelte.js', 'docks.svelte.js', 'hub.svelte.js', 'session.svelte.js'];
/** The one maker of what a host's plots share. */
const HOST = 'lib/plothost.js';
/** What a host makes for its plots: each of these is made there, once. */
const MADE = ['new CursorEdits(', 'new ViewHistory(', 'new Renderer(', 'plotCommands('];
/** A read of the window's size or its pixel ratio. */
const WINDOW_READ = /\bdevicePixelRatio\b|\binner(Width|Height)\b/;

/** The modules `source` imports, as written, each with the names it imports in braces. */
const imported = (source) => [...source.matchAll(/^\s*import\s+(?:[\w*\s]+,?\s*)?(?:\{([^}]*)\})?\s*(?:from\s*)?['"]([^'"]+)['"]/gm)].map(([, names = '', from]) => ({ from, names: names.split(',').map((n) => n.trim().split(/\s+as\s+/)[0]).filter(Boolean) }));
/** The props component `source` takes, by name. */
const props = (source) => (source.match(/let\s*\{([^}]*)\}\s*=\s*\$props\(\)/)?.[1] ?? '').split(',').map((p) => p.trim().split(/[\s=:]/)[0]).filter(Boolean);

describe('a plot', () => {
  const read = (file) => fs.readFileSync(path.join(SRC, file), 'utf8');
  /** The lines of `file` that `test(line)` holds of, comments left out, as `file:line`. */
  const lines = (file, test) => read(file).split('\n').flatMap((line, i) => (test(line) && !/^\s*(\/\/|\*|\/\*)/.test(line) ? [`${file}:${i + 1}`] : []));

  it('has its imports and its props found', () => {
    expect(imported("import { a, b as c } from './x.js';\n  import D from './D.svelte';\nimport * as e from '$lib/e.js';\nimport './f.css';")).toEqual([
      { from: './x.js', names: ['a', 'b'] },
      { from: './D.svelte', names: [] },
      { from: '$lib/e.js', names: [] },
      { from: './f.css', names: [] },
    ]);
    expect(props('<script>\n  let { plot, host = null, other: renamed } = $props();\n</script>')).toEqual(['plot', 'host', 'other']);
    expect(imported(read('lib/PlotCard.svelte')).map(({ from }) => from)).toContain('./plot.js');
  });
  it('takes its settings and its host, and no other prop', () => {
    expect(props(read('lib/PlotCard.svelte'))).toEqual(['plot', 'host']);
  });
  it('imports none of the page’s modules, nor the page’s colours: it reaches them through its host', () => {
    const found = PLOT.flatMap((file) =>
      imported(read(file))
        .filter(({ from, names }) => PAGE.includes(from.split('/').at(-1)) || (from.endsWith('/colors.svelte.js') && names.includes('colors')))
        .map(({ from }) => `${file} ${from}`),
    );
    expect(found).toEqual([]);
  });
  it('is drawn at its surface’s size and pixel ratio: the window’s are read by the page’s frame alone', () => {
    expect([...PLOT, HOST].flatMap((file) => lines(file, (line) => WINDOW_READ.test(line) && !line.includes('export const windowFrame')))).toEqual([]);
    expect(lines('lib/gl.js', (line) => WINDOW_READ.test(line))).toHaveLength(1);
  });
  it('asks its host what a command acts on for its gestures alone, and edits no other plot itself: what a command acts on is the table’s', () => {
    /** What gives a plot's gestures the plots they move. */
    const GESTURE = /^\s*(targets|cursorTargets):/;
    const asked = (source) => source.split('\n').flatMap((line, i) => (/\.(targets|each|editEach)\(/.test(line) && !GESTURE.test(line) && !/^\s*(\/\/|\*|\/\*)/.test(line) ? [i + 1] : []));
    expect(asked('const targetIds = () => host.targets(plot.id);\n  targets: () => host.targets(plot.id).map((id) => host.view(id)),\nconst onEach = (what, fn) => host.each(plot.id, what, fn);\n{#each items as item}')).toEqual([1, 3]);
    expect(PLOT.filter((file) => file.endsWith('.svelte')).flatMap((file) => asked(read(file)).map((n) => `${file}:${n}`))).toEqual([]);
    expect(read('lib/PlotCard.svelte')).toMatch(/^\s*targets: \(\) => host\.targets\(plot\.id\)/m);
  });
  it('shares what its host makes, each made by the host alone, once', () => {
    for (const made of MADE) {
      const found = sources(SRC).flatMap((file) => lines(rel(file), (line) => line.includes(made)));
      expect(found.map((at) => at.split(':')[0]), made).toEqual([HOST]);
    }
  });
});

describe('a command’s words', () => {
  const table = fs.readFileSync(path.join(SRC, 'lib/commands.js'), 'utf8');
  /** The fields an entry of the command table has, as its file lists them. */
  const FIELDS = table.match(/An entry is \{([^}]*)\}/)[1].replace(/\/\//g, '').split(',').map((f) => f.trim());
  /** What of a command's look may be a function of where it is made. */
  const LOOK = ['label', 'icon', 'iconOf', 'active', 'activeIcon'];
  /** The fields of entry `c` that word it or draw it by where it is made: a function that reads its origin. */
  const byPlace = (c) => LOOK.filter((f) => typeof c[f] === 'function' && /\bfrom\b/.test(String(c[f])));

  it('are found where an entry words itself by where it is made', () => {
    expect(byPlace({ label: (ctx) => (ctx.from === 'palette' ? 'Arrange as Rows' : 'Rows'), icon: () => null })).toEqual(['label']);
    expect(byPlace({ label: (ctx, how) => `Go to ${how.to}`, iconOf: (ctx, how) => how.to })).toEqual([]);
    expect(FIELDS).toContain('label');
    expect(FIELDS).toContain('run');
  });
  it('are one label and one icon for a command wherever it shows, with no field that words it again', () => {
    expect(COMMANDS.flatMap((c) => byPlace(c).map((f) => `${c.id}.${f}`))).toEqual([]);
    expect(COMMANDS.flatMap((c) => Object.keys(c).filter((k) => !FIELDS.includes(k)).map((k) => `${c.id}.${k}`))).toEqual([]);
    // An axis popover's buttons take their name, tip and picture from the table: the snippet that draws one takes the command and what it adds alone.
    expect(fs.readFileSync(path.join(SRC, 'lib/PlotCard.svelte'), 'utf8')).toMatch(/\{#snippet tool\(id, how\)\}/);
  });
});

describe('the command palette', () => {
  it('takes the signals and the devices it names, and no actions: the commands it lists and runs are the table’s', () => {
    expect(props(fs.readFileSync(path.join(SRC, 'lib/CommandPalette.svelte'), 'utf8'))).toEqual(['open', 'session', 'devices']);
  });
});

/** The one module that listens for keys. */
const KEY_ROUTER = 'lib/keyrouter.js';
/** What a text field is: what is typed into. */
const FIELDS = ['input', 'Input', 'textarea', 'Command.Input'];
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

/** The one module that reads the time, asks for a frame or sets a timer. */
const CLOCK = 'lib/clock.js';
/** A read of the time, a frame or a timer asked of the browser, or one called off. */
const TIMED = /\bperformance\s*\.\s*now\b|\bDate\s*\.\s*now\b|\bnew\s+Date\b\s*(\(\s*\)|(?![\s(]))|\b(request|cancel)AnimationFrame\b|\b(set|clear)(Timeout|Interval)\b/;
/** The files of tests/ that touch the browser's time: the harness's real frames and waits, and the mouse command's rests. */
const HARNESS_TIMED = ['harness/page.js', 'harness/commands.js'];

describe('the time', () => {
  /** The lines of `source` that match TIMED, comments left out, numbered from 1. */
  const timed = (source) => source.split('\n').flatMap((line, i) => (TIMED.test(line) && !/^\s*(\/\/|\*)/.test(line) ? [i + 1] : []));

  it('is found where it is read, a frame asked for or a timer set, and not where a date is made of a time', () => {
    const lines = ['performance.now()', 'Date.now()', 'new Date()', 'new Date', 'requestAnimationFrame(f)', 'cancelAnimationFrame(id)', 'setTimeout(f, 1)', 'clearTimeout(id)', 'setInterval(f, 1)', 'window.clearInterval(id)'];
    expect(timed(lines.join('\n'))).toEqual(lines.map((_, i) => i + 1));
    expect(timed(['new Date(whole * 1000)', 'clock.now()', 'clock.after(0, f)', '// setTimeout(f)', 'new DateFormat()'].join('\n'))).toEqual([]);
  });
  it('is read, and frames and timers asked for, by the clock alone', () => {
    const found = sources(SRC)
      .filter((file) => rel(file) !== CLOCK)
      .flatMap((file) => timed(fs.readFileSync(file, 'utf8')).map((n) => `${rel(file)}:${n}`));
    expect(found).toEqual([]);
  });
  it('of a page test is the page’s: only the harness waits real time or asks the browser for a frame', () => {
    const TESTS = path.resolve('tests');
    const found = fs
      .readdirSync(TESTS, { recursive: true })
      .map((name) => name.replaceAll('\\', '/'))
      .filter((name) => /\.(js|svelte)$/.test(name) && !HARNESS_TIMED.includes(name))
      .flatMap((name) => timed(fs.readFileSync(path.join(TESTS, name), 'utf8')).map((n) => `tests/${name}:${n}`));
    expect(found).toEqual([]);
  });
});
