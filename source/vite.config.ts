import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  base: './',
  plugins: [react()],
  build: {
    outDir: '../web',
    emptyOutDir: true,
  },
  server: {
    port: 5181,
    proxy: {
      '/benchmarks': {
        target: 'https://localhost:9996',
        changeOrigin: true,
        secure: false,
      },
    },
  },
})
