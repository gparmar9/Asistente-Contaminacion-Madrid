import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// El frontend llama SIEMPRE a rutas relativas `/api/...`. En desarrollo, Vite
// hace de proxy hacia ApiUsuario (uvicorn en :8000) quitando el prefijo; en
// producción lo hace nginx (ver nginx.conf). Así el código no conoce ningún host.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: process.env.VITE_DEV_API_PROXY ?? 'http://localhost:8000',
        changeOrigin: true,
        rewrite: (ruta) => ruta.replace(/^\/api/, ''),
        // Una respuesta del chat puede tardar ~120 s: no cortar antes que la API.
        timeout: 180_000,
        proxyTimeout: 180_000,
      },
    },
  },
  build: {
    sourcemap: false,
    rollupOptions: {
      output: {
        // Recharts pesa: lo separamos para que el resto de la app cachee aparte.
        manualChunks: {
          graficas: ['recharts'],
          mapa: ['leaflet'],
        },
      },
    },
  },
})
