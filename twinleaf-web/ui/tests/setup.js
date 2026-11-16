import fc from 'fast-check';
import { inject } from 'vitest';

// FC_RUNS sets how many cases each property tries. FC_SEED and FC_PATH,
// as a failure prints them, replay that failure's shrunk case.
const { runs, seed, path } = inject('fc');
fc.configureGlobal({ numRuns: runs, ...(seed === undefined ? {} : { seed }), ...(path === undefined ? {} : { path }) });
