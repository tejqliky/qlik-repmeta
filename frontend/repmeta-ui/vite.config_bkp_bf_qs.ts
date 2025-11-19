// Vite config compatible with both local and Docker builds, *without* Node globals.
// It compiles under `tsc -b` even if @types/node is not installed.
//
// Design goals:
// - Zero dependency on Node types: no `process`, no `__dirname`.
// - Dev-only proxy (ignored during `vite build`) to avoid Docker build pitfalls.
// - Helpful defaults: alias '@' -> '/src', strict ports, minimal prod output.
// - Safe to use with your current App.tsx (which hardcodes API_BASE).
//   You can later switch App.tsx to `import.meta.env.VITE_API_BASE` without changing this file.
//
// If you add a `.env` with `VITE_*` vars, Vite will expose them under `import.meta.env` automatically.

import { defineConfig, loadEnv, type UserConfig } from 'vite'
import react from '@vitejs/plugin-react-swc'
import path from 'path'

export default defineConfig(({ command, mode }): UserConfig => {
  const isDev = command === 'serve'

  // Resolve envs relative to this config file directory *without* Node globals
  const envDir = new URL('.', import.meta.url).pathname
  const env = loadEnv(mode, envDir, '')

  // Optional dev proxy target for future /api calls.
  const proxyTarget = env.VITE_API_PROXY_TARGET || 'http://127.0.0.1:8002'

  return {
    plugins: [react()],

    // Only matters in dev; excluded from `vite build`
    server: isDev
      ? {
          host: true,
          port: 5173,
          strictPort: true,
          proxy: {
            // If you later fetch('/api/...') from the UI, this will forward to FastAPI during dev.
            '/api': {
              target: proxyTarget,
              changeOrigin: true,
              ws: false,
              // strip the '/api' prefix when forwarding
              rewrite: (p) => p.replace(/^\/api/, ''),
              configure: (proxy) => {
                proxy.on('proxyReq', (proxyReq) => {
                  proxyReq.setHeader('Cache-Control', 'no-cache')
                })
              },
            },
          },
        }
      : undefined,

    // Preview useful when running static files inside Docker
    preview: {
      host: true,
      port: 4173,
      strictPort: true,
    },

    // Keep build output deterministic and lean for the Docker image
    build: {
      target: 'es2020',
      outDir: 'dist',
      emptyOutDir: true,
      sourcemap: false,
      assetsInlineLimit: 4096,
      cssMinify: true,
      rollupOptions: {
        output: {
          // Stable hashing to avoid cache thrash between identical builds
          chunkFileNames: 'assets/[name]-[hash].js',
          entryFileNames: 'assets/[name]-[hash].js',
          assetFileNames: ({ name }) => {
            // group images/fonts vs other assets
            const ext = (name && name.split('.').pop()) || ''
            if (/png|jpe?g|gif|svg|webp|avif|ico|bmp|tiff/i.test(ext)) return 'assets/img/[name]-[hash][extname]'
            if (/woff2?|ttf|otf|eot/i.test(ext)) return 'assets/font/[name]-[hash][extname]'
            return 'assets/[name]-[hash][extname]'
          },
        },
      },
    },

    // Ergonomic imports
    resolve: {
      alias: {
        '@': path.posix.join('/', 'src'), // use POSIX to avoid Windows path backslashes in builds
      },
    },

    // Expose only VITE_* keys to the client
    envPrefix: 'VITE_',

    // Minor DX niceties
    css: {
      devSourcemap: true,
    },

    // Define can be handy if some libs read `import.meta.env.MODE` at runtime.
    define: {
      __DEV__: isDev,
    },
  }
})
