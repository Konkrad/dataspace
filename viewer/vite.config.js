import { defineConfig } from 'vite';

// The landing page (written by cdsemeta-index) loads the entry from
// .vite/manifest.json; everything is served under /_viewer/ on the bucket.
export default defineConfig({
  base: '/_viewer/',
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    manifest: true,
    target: 'es2022',
    rollupOptions: { input: 'src/main.js' },
  },
  worker: { format: 'es' },
});
