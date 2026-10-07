import { lazy, Suspense, useMemo, useState } from 'react'
import { Link, useNavigate, useSearchParams } from 'react-router-dom'

import { useEstaciones } from '../api/consultas'
import type { Estacion } from '../api/tipos'
import { Cargando, ErrorBloque } from '../componentes/ui/Avisos'
import { Segmentos } from '../componentes/ui/Segmentos'
import { Tabla, type Columna } from '../componentes/ui/Tabla'
import { contaminantesDisponibles } from '../dominio/contaminantes'
import { formatearEntero } from '../dominio/formato'
import { useEstacionSeleccionada } from '../estado/hooks'

// Leaflet solo se descarga cuando se muestra el mapa.
const MapaEstaciones = lazy(() => import('../componentes/mapa/MapaEstaciones'))

type Vista = 'mapa' | 'lista'

const VISTAS = [
  { valor: 'mapa' as const, etiqueta: 'Mapa' },
  { valor: 'lista' as const, etiqueta: 'Lista' },
]

export default function Estaciones() {
  const estaciones = useEstaciones()
  const navegar = useNavigate()
  const [params, setParams] = useSearchParams()
  const { codigo: seleccionada } = useEstacionSeleccionada()
  const [busqueda, setBusqueda] = useState('')
  const vista: Vista = params.get('vista') === 'lista' ? 'lista' : 'mapa'

  const filtradas = useMemo(() => {
    const lista = estaciones.data ?? []
    const q = busqueda.trim().toLocaleLowerCase('es')
    if (!q) return lista
    return lista.filter((e) =>
      [e.nombre, e.distrito ?? '', e.tipo, e.direccion, String(e.codigo_corto)].some((v) =>
        v.toLocaleLowerCase('es').includes(q),
      ),
    )
  }, [estaciones.data, busqueda])

  const sinCoordenadas = useMemo(() => filtradas.filter((e) => e.latitud == null || e.longitud == null), [filtradas])

  const columnas: Columna<Estacion>[] = [
    {
      clave: 'codigo',
      titulo: 'Código',
      numerica: true,
      render: (e) => e.codigo_corto,
      ordenar: (e) => e.codigo_corto,
    },
    {
      clave: 'nombre',
      titulo: 'Estación',
      render: (e) => (
        <Link to={`/estaciones/${e.codigo_corto}`} onClick={(ev) => ev.stopPropagation()}>
          {e.nombre}
        </Link>
      ),
      ordenar: (e) => e.nombre,
    },
    { clave: 'tipo', titulo: 'Tipo', render: (e) => e.tipo, ordenar: (e) => e.tipo },
    {
      clave: 'distrito',
      titulo: 'Distrito',
      render: (e) => e.distrito ?? <span className="texto-apagado">—</span>,
      ordenar: (e) => e.distrito ?? null,
    },
    {
      clave: 'altitud',
      titulo: 'Altitud (m)',
      numerica: true,
      render: (e) => formatearEntero(e.altitud),
      ordenar: (e) => e.altitud ?? null,
    },
    {
      clave: 'contaminantes',
      titulo: 'Contaminantes medidos',
      render: (e) => {
        const medidos = contaminantesDisponibles(e.contaminantes_medidos ?? [])
        return medidos.length === 0 ? (
          <span className="texto-apagado">ninguno de los 6 objetivo</span>
        ) : (
          <span className="chips">
            {medidos.map((c) => (
              <span key={c} className="chip">
                {c}
              </span>
            ))}
          </span>
        )
      },
      ordenar: (e) => (e.contaminantes_medidos ?? []).length,
    },
  ]

  return (
    <div className="apilado">
      <header className="pagina-cabecera pagina-cabecera-compacta">
        <h1>Estaciones</h1>
        <p>Red de estaciones de control del Ayuntamiento de Madrid. Elige una para ver sus series, el heatmap y las anomalías.</p>
      </header>

      <div className="filtros">
        <div className="campo">
          <label className="campo-etiqueta" htmlFor="buscar-estacion">
            Buscar
          </label>
          <input
            id="buscar-estacion"
            className="entrada"
            type="search"
            placeholder="Nombre, distrito, tipo…"
            value={busqueda}
            onChange={(e) => setBusqueda(e.target.value)}
            style={{ minWidth: 260 }}
          />
        </div>
        <div className="campo">
          <span className="campo-etiqueta">Vista</span>
          <Segmentos
            opciones={VISTAS}
            valor={vista}
            onChange={(v) => setParams(v === 'mapa' ? {} : { vista: v }, { replace: true })}
            etiquetaAria="Mapa o lista de estaciones"
          />
        </div>
        {estaciones.data && (
          <span className="t-s texto-2" style={{ marginLeft: 'auto', alignSelf: 'center' }}>
            {filtradas.length} de {estaciones.data.length} estaciones
          </span>
        )}
      </div>

      {estaciones.isPending && <Cargando texto="Cargando el catálogo de estaciones…" />}
      {estaciones.isError && <ErrorBloque error={estaciones.error} reintentar={() => void estaciones.refetch()} />}

      {estaciones.data && vista === 'mapa' && (
        <div className="apilado-s">
          <Suspense fallback={<Cargando texto="Cargando el mapa…" />}>
            <MapaEstaciones estaciones={filtradas} seleccionada={seleccionada} onElegir={(c) => navegar(`/estaciones/${c}`)} />
          </Suspense>
          <div className="leyenda">
            <span className="leyenda-item">
              <span className="leyenda-muestra" style={{ borderRadius: '50%', background: 'var(--dato)', border: '2px solid var(--superficie)' }} aria-hidden="true" />
              Estación de control
            </span>
            {seleccionada !== undefined && (
              <span className="leyenda-item">
                <span className="leyenda-muestra" style={{ borderRadius: '50%', border: '2px solid var(--dato)', background: 'transparent' }} aria-hidden="true" />
                Anillo: estación elegida en «Hoy»
              </span>
            )}
            <span>Mapa base © Esri</span>
          </div>
          {sinCoordenadas.length > 0 && (
            <p className="t-s texto-2">
              Sin coordenadas en el catálogo ({sinCoordenadas.length}):{' '}
              {sinCoordenadas.map((e, i) => (
                <span key={e.codigo_corto}>
                  {i > 0 && ', '}
                  <Link to={`/estaciones/${e.codigo_corto}`}>{e.nombre}</Link>
                </span>
              ))}
              . Están en la vista de lista.
            </p>
          )}
        </div>
      )}

      {estaciones.data && vista === 'lista' && (
        <Tabla
          filas={filtradas}
          columnas={columnas}
          claveFila={(e) => e.codigo_corto}
          ordenInicial={{ clave: 'nombre', direccion: 'asc' }}
          onClickFila={(e) => navegar(`/estaciones/${e.codigo_corto}`)}
          caption="Pulsa una fila para abrir la estación, o la cabecera de una columna para ordenar."
          vacio="Ninguna estación coincide con la búsqueda."
        />
      )}
    </div>
  )
}
