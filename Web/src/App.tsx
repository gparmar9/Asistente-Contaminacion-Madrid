import { lazy, Suspense } from 'react'
import { Route, Routes } from 'react-router-dom'

import { PanelChat } from './componentes/chat/PanelChat'
import { Cabecera } from './componentes/layout/Cabecera'
import { Pie } from './componentes/layout/Pie'
import { Cargando } from './componentes/ui/Avisos'
import Asistente from './paginas/Asistente'
import Estaciones from './paginas/Estaciones'
import Hoy from './paginas/Hoy'
import Metodologia from './paginas/Metodologia'
import NoEncontrada from './paginas/NoEncontrada'

// La única pantalla con Recharts se carga aparte: el resto no paga su peso.
const DetalleEstacion = lazy(() => import('./paginas/DetalleEstacion'))

export default function App() {
  return (
    <>
      <a className="salto" href="#contenido">
        Saltar al contenido
      </a>
      <Cabecera />
      <main id="contenido" className="contenedor pagina">
        <Suspense fallback={<Cargando texto="Cargando la pantalla…" />}>
          <Routes>
            <Route path="/" element={<Hoy />} />
            <Route path="/estaciones" element={<Estaciones />} />
            <Route path="/estaciones/:codigo" element={<DetalleEstacion />} />
            <Route path="/asistente" element={<Asistente />} />
            <Route path="/metodologia" element={<Metodologia />} />
            <Route path="*" element={<NoEncontrada />} />
          </Routes>
        </Suspense>
      </main>
      <Pie />
      <PanelChat />
    </>
  )
}
