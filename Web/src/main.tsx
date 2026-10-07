import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'

import App from './App'
import { Proveedores } from './estado/Proveedores'

import './estilos/tokens.css'
import './estilos/paleta-cielo.css'
import './estilos/base.css'
import './estilos/componentes.css'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 2 * 60_000,
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
})

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Proveedores>
          <App />
        </Proveedores>
      </BrowserRouter>
    </QueryClientProvider>
  </StrictMode>,
)
