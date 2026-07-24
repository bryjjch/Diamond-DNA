import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import path from 'node:path'

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  // Dev: forward /api/* to the API Gateway named by VITE_DEV_API_PROXY (shell
  // env or frontend/.env.local — see README). No URL is hardcoded here; without
  // it the dev server runs with no proxy and /api/* requests fail loudly.
  // Proxying (vs. calling cross-origin from the browser) bypasses API
  // Gateway's CORS allow-list, which is locked to the Vercel domain in prod.
  const env = loadEnv(mode, __dirname, '')
  const proxyTarget = env.VITE_DEV_API_PROXY

  return {
    plugins: [react(), tailwindcss()],
    resolve: {
      alias: {
        '@': path.resolve(__dirname, './src'),
      },
    },
    server: {
      port: 5173,
      proxy: proxyTarget
        ? {
            '/api': {
              target: proxyTarget,
              changeOrigin: true,
              secure: true,
            },
          }
        : undefined,
    },
  }
})
