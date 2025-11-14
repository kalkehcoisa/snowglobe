import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

export default defineConfig({
  plugins: [vue()],
  server: {
    port: 3000,
    proxy: {
      '/api': {
        target: 'http://localhost:8084',
        changeOrigin: true
      },
      '/health': {
        target: 'http://localhost:8084',
        changeOrigin: true
      },
      '/session': {
        target: 'http://localhost:8084',
        changeOrigin: true
      },
      '/queries': {
        target: 'http://localhost:8084',
        changeOrigin: true
      }
    }
  },
  build: {
    outDir: 'dist',
    assetsDir: 'assets'
  }
})
