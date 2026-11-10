// Mutation testing of the modules that are tables of words, the second run
// of `npm run test:mutate`: their strings, arrays, objects and regexes are
// wording and stay as they are, so the score is of the logic that picks
// which words to say. The report is reports/mutation/wording.html.
import { WORDING, run } from './stryker.config.mjs';

/** @type {import('@stryker-mutator/api/core').PartialStrykerOptions} */
export default {
  ...run('wording'),
  mutate: WORDING,
  mutator: { excludedMutations: ['StringLiteral', 'ArrayDeclaration', 'ObjectLiteral', 'Regex'] },
};
