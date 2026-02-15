import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

const target = process.env.THORSTREAM_COMPAT_API_URL || 'http://127.0.0.1:8083';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target,
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ''),
      },
    },
  },
});
