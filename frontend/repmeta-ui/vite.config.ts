import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react-swc';

// Allow overriding the backend during local dev (optional)
const proxyTarget = process.env.VITE_API_PROXY_TARGET || 'http://127.0.0.1:8002';

export default defineConfig({
  plugins: [react()],
  server: {
    host: true,
    port: 5173,
    proxy: {
      // Everything under /api goes to FastAPI (so the UI can call /api/* without CORS headaches)
      '/api': {
        target: proxyTarget,
        changeOrigin: true,
        ws: false, // we're using SSE, not websockets
        rewrite: (path) => path.replace(/^\/api/, ''),
        configure: (proxy) => {
          // Helpful for SSE: ensure proxies don't cache/transform event streams
          proxy.on('proxyReq', (proxyReq) => {
            proxyReq.setHeader('Cache-Control', 'no-cache');
          });
        },
      },
    },
  },
});
