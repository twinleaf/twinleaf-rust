import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import tailwindcss from '@tailwindcss/vite';
import { playwright } from '@vitest/browser-playwright';
import path from 'node:path';
import { key, mouse } from './tests/harness/commands.js';

/** A count from the environment, `fallback` when unset; anything but a positive integer is refused. */
function count(name, fallback) {
  const v = process.env[name];
  if (v === undefined) return fallback;
  if (!/^[1-9]\d*$/.test(v)) throw new Error(`${name} must be a positive integer, not "${v}"`);
  return +v;
}

/** The port the browser tests' runner listens on, TL_TEST_PORT, so checkouts can run them side by side. */
const testPort = count('TL_TEST_PORT', 9420);

/**
 * What tests/setup.js gives fast-check: how many cases each property tries,
 * FC_RUNS or `runs`, and a failure to replay, FC_SEED and FC_PATH as the
 * failure printed them.
 */
const fc = (runs) => ({ fc: { runs: count('FC_RUNS', runs), seed: process.env.FC_SEED === undefined ? undefined : +process.env.FC_SEED, path: process.env.FC_PATH } });

export default defineConfig({
  plugins: [tailwindcss(), svelte()],
  // Tests bundle dependencies apart from the dev server, which shares this
  // folder, and apart from tests run on another port.
  cacheDir: process.env.VITEST ? `node_modules/.vite-test-${testPort}` : 'node_modules/.vite',
  resolve: { alias: { $lib: path.resolve('./src/lib') } },
  test: {
    setupFiles: ['tests/setup.js'],
    // A module's own tests sit beside it in src and run Svelte's client
    // runtime in jsdom, where effects run and state is proxied as on the
    // page. The tests in tests/ drive the whole page in a real browser.
    projects: [
      {
        extends: true,
        resolve: { conditions: ['browser'] },
        test: { name: 'unit', environment: 'jsdom', include: ['src/**/*.test.js'], provide: fc(100) },
      },
      {
        extends: true,
        // The page's dependencies are bundled before the first test imports
        // them, so none is found late and reloads the test.
        optimizeDeps: { entries: ['src/main.js', 'tests/**/*.test.js'] },
        test: {
          name: 'browser',
          // A case here mounts the page and moves the real mouse, so a run
          // tries few; each property's `examples` are always tried.
          provide: fc(20),
          include: ['tests/**/*.test.js'],
          api: { host: '127.0.0.1', port: testPort, strictPort: true },
          browser: {
            enabled: true,
            // Edge comes with Windows; elsewhere Playwright's own Chromium.
            provider: playwright({ launchOptions: process.platform === 'win32' ? { channel: 'msedge' } : {} }),
            headless: true,
            viewport: { width: 1200, height: 900 },
            // A property's failure prints its case to replay; a picture adds nothing.
            screenshotFailures: false,
            commands: { key, mouse },
            instances: [{ browser: 'chromium' }],
          },
        },
      },
    ],
  },
  // The proxied requests keep the page's own Host, which the server's guard
  // matches against their Origin; Vite's string shorthand would rewrite it.
  // Tests never reach the hub: a page mounted in one finds no socket.
  server: process.env.VITEST ? {} : { proxy: { '/ws': { target: 'ws://localhost:7855', ws: true }, '/api': { target: 'http://localhost:7855' } } },
});
