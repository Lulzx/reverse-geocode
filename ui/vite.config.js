import { defineConfig } from 'vite';

export default defineConfig({
  base: '/reverse-geocode/',
  server: { port: 5173 },
  build: {
    target: 'esnext',
    rollupOptions: {
      output: { manualChunks: { maplibre: ['maplibre-gl'], h3: ['h3-js'] } },
    },
  },
});
