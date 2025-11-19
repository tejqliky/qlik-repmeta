// Vite config — minimal, Node-globals free, with a robust FastAPI proxy.
// Works for local dev and Docker builds without requiring @types/node.

import { defineConfig, type UserConfig } from 'vite'
import react from '@vitejs/plugin-react-swc'

export default defineConfig((): UserConfig => {
  return {
    plugins: [react()],

    // Keep paths simple; avoid Node's `path` so this compiles without Node types.
    resolve: {
      alias: {
        '@': '/src',
      },
    },

    server: {
      host: true,
      port: 5173,
      strictPort: true,
      // ❗ Proxy all API calls to FastAPI (uvicorn) on 8002
      proxy: {
        // Core REST endpoints used by RepMeta + Qlik Sense lanes
        '^/(customers|api/customers|qliksense|ingest|export|license|health)': {
          target: 'http://127.0.0.1:8002',
          changeOrigin: true,
          secure: false,
        },
      },
    },

    // Preview server for `vite preview` (not used in Docker build, but handy locally)
    preview: {
      host: true,
      port: 5174,
      strictPort: true,
    },

    // Only VITE_* variables will be exposed to the client
    envPrefix: 'VITE_',

    css: {
      devSourcemap: true,
    },
  }
})
