// The unit project alone, as one config, for Stryker's Vitest runner, which
// runs no browser tests and no projects (stryker.config.mjs).
import base from './vite.config.js';

const unit = base.test.projects.find((p) => p.test.name === 'unit');

export default {
  ...base,
  resolve: { ...base.resolve, ...unit.resolve },
  test: { ...base.test, ...unit.test, projects: undefined },
};
