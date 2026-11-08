import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import tailwindcss from '@tailwindcss/vite';
import path from 'node:path';

export default defineConfig({
  plugins: [tailwindcss(), svelte()],
  resolve: { alias: { $lib: path.resolve('./src/lib') } },
  test: { include: ['test/**/*.test.js'] },
  // The proxied requests keep the page's own Host, which the server's guard
  // matches against their Origin; Vite's string shorthand would rewrite it.
  server: { proxy: { '/ws': { target: 'ws://localhost:7855', ws: true }, '/api': { target: 'http://localhost:7855' } } },
});
