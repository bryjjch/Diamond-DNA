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
      // Local dev: forward /api/* to the Flask shim that wraps the Lambda handler.
      // In production, VITE_API_BASE_URL points directly at API Gateway, so this proxy
      // is unused and the request goes cross-origin (CORS is configured on the API).
      '/api': {
        target: process.env.VITE_DEV_API_PROXY ?? 'http://127.0.0.1:5001',
        changeOrigin: true,
      },
    },
  },
})
