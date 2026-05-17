import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import path from 'node:path'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    port: 5173,
    proxy: {
      // Dev: forward /api/* to the deployed API Gateway by default — no local
      // Flask shim required. Override with VITE_DEV_API_PROXY to point at a
      // local server or a different stage. Proxying (vs. calling cross-origin
      // from the browser) bypasses API Gateway's CORS allow-list, which is
      // locked to the Vercel domain in prod.
      '/api': {
        target:
          process.env.VITE_DEV_API_PROXY ??
          'https://vk3hzz0cg9.execute-api.us-east-1.amazonaws.com',
        changeOrigin: true,
        secure: true,
      },
    },
  },
})
