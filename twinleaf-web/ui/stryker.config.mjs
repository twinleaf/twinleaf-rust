// Mutation testing of the page's logic modules against the unit tests
// (`npm run test:mutate`). Stryker's Vitest runner has no browser mode, so
// the tests in tests/ take no part; vitest.mutate.config.js is the unit
// project alone. The report is written to reports/mutation/.
//
// The score is of logic, not wording: no string is mutated anywhere. A
// mutator is left out for a whole run, not for a file, so the two modules
// that are tables of words, keymap.js and words.js, are a run of their own
// (stryker.wording.config.mjs), which also leaves their arrays, objects
// and regexes as they are.

/** The modules that are tables of words, mutated by stryker.wording.config.mjs. */
export const WORDING = ['src/lib/keymap.js', 'src/lib/words.js'];

/** What both runs share. `name` is the report's file name in reports/mutation/. */
export const run = (name) => ({
  testRunner: 'vitest',
  vitest: { configFile: 'vitest.mutate.config.js' },
  reporters: ['clear-text', 'progress', 'html', 'json'],
  htmlReporter: { fileName: `reports/mutation/${name}.html` },
  jsonReporter: { fileName: `reports/mutation/${name}.json` },
  tempDirName: '.stryker-tmp',
  timeoutMS: 10000,
});

/** @type {import('@stryker-mutator/api/core').PartialStrykerOptions} */
export default {
  ...run('mutation'),
  mutate: [
    'src/lib/layout.js',
    'src/lib/moves.js',
    'src/lib/selection.svelte.js',
    'src/lib/history.svelte.js',
    'src/lib/viewhistory.svelte.js',
    'src/lib/cursors.js',
    'src/lib/writes.svelte.js',
    'src/lib/editbar.js',
    'src/lib/properties.js',
    'src/lib/commands.js',
    'src/lib/keyrouter.js',
    'src/lib/menus.js',
    'src/lib/tabs.js',
    'src/lib/window.js',
    'src/lib/units.js',
    'src/lib/workspace.svelte.js',
    'src/lib/copy.js',
    'src/lib/exportfile.js',
    'src/lib/focus.js',
    'src/lib/colors.svelte.js',
    'src/lib/docks.svelte.js',
    'src/lib/favorites.svelte.js',
    'src/lib/prefs.svelte.js',
    'src/lib/drag.svelte.js',
    'src/lib/session.svelte.js',
  ],
  mutator: { excludedMutations: ['StringLiteral'] },
};
