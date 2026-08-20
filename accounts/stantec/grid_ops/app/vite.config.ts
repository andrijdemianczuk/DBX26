import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Frontend lives at the repo root (index.html + src/); builds to dist/, which
// server.js serves. During `vite` dev, /api is proxied to the Express server.
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  },
  server: {
    port: 5173,
    proxy: {
      '/api': { target: 'http://localhost:8000', changeOrigin: true },
    },
  },
});
