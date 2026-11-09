import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import tailwindcss from '@tailwindcss/vite';
import path from 'node:path';
import { key, mouse } from './test/browser/commands.js';

/** A count from the environment, `fallback` when unset; anything but a positive integer is refused. */
function count(name, fallback) {
  const v = process.env[name];
  if (v === undefined) return fallback;
  if (!/^[1-9]\d*$/.test(v)) throw new Error(`${name} must be a positive integer, not "${v}"`);
  return +v;
}

export default defineConfig({
  plugins: [tailwindcss(), svelte()],
  // Tests bundle dependencies apart from the dev server, which shares this folder.
  cacheDir: process.env.VITEST ? 'node_modules/.vite-test' : 'node_modules/.vite',
  resolve: { alias: { $lib: path.resolve('./src/lib') } },
  test: {
    // FC_RUNS sets how many cases each property tries; FC_SEED and FC_PATH
    // name a failure to replay. They reach test/setup.js in either project.
    provide: { fc: { runs: count('FC_RUNS', 100), seed: process.env.FC_SEED === undefined ? undefined : +process.env.FC_SEED, path: process.env.FC_PATH } },
    setupFiles: ['test/setup.js'],
    // Unit tests run Svelte's client runtime in jsdom, where effects run and
    // state is proxied as on the page; browser tests run in a real browser.
    projects: [
      {
        extends: true,
        resolve: { conditions: ['browser'] },
        test: { name: 'unit', environment: 'jsdom', include: ['test/**/*.test.js'], exclude: ['test/**/*.browser.test.js'] },
      },
      {
        extends: true,
        // The page's dependencies are bundled before the first test imports
        // them, so none is found late and reloads the test.
        optimizeDeps: { entries: ['src/main.js', 'test/**/*.browser.test.js'] },
        test: {
          name: 'browser',
          include: ['test/**/*.browser.test.js'],
          browser: {
            enabled: true,
            provider: 'playwright',
            headless: true,
            api: { host: '127.0.0.1', port: 9420, strictPort: true },
            viewport: { width: 1200, height: 900 },
            // A property's failure prints its case to replay; a picture adds nothing.
            screenshotFailures: false,
            commands: { key, mouse },
            // Edge comes with Windows; elsewhere Playwright's own Chromium.
            instances: [{ browser: 'chromium', launch: process.platform === 'win32' ? { channel: 'msedge' } : {} }],
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
