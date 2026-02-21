import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { resolve } from 'path'
import { readFileSync } from 'fs'

// Read base path from yeti config.yaml automatically.
// Uses route_prefix if set, otherwise /{app_id}/.
function readYetiBasePath(): string {
  if (process.env.YETI_BASE_PATH) return process.env.YETI_BASE_PATH
  try {
    const config = readFileSync(resolve(__dirname, '../config.yaml'), 'utf-8')
    const prefixMatch = config.match(/^route_prefix:\s*["']?([^"'\n]+)["']?/m)
    if (prefixMatch) {
      const prefix = prefixMatch[1].trim()
      return prefix === '/' ? '/' : `${prefix.replace(/\/+$/, '')}/`
    }
    const idMatch = config.match(/^app_id:\s*["']?([^"'\n]+)["']?/m)
    if (idMatch) return `/${idMatch[1].trim()}/`
  } catch {}
  return './'
}
export default defineConfig({
  base: readYetiBasePath(),
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
