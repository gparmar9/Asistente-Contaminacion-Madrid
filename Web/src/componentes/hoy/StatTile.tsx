import { Link } from 'react-router-dom'

import type { Contaminante, PuntoSerie } from '../../api/tipos'
import { esAnomaliaFiable, estadoPunto } from '../../dominio/anomalias'
import { NOMBRE_CONTAMINANTE, UNIDAD } from '../../dominio/constantes'
import { formatearFechaCorta } from '../../dominio/fechas'
import { formatearValor } from '../../dominio/formato'
import { etiquetaBloque } from '../graficas/datos'
import { Sparkline } from '../graficas/Sparkline'
import { Estado } from '../ui/Estado'
import { Medidor } from '../ui/Medidor'

interface Props {
  contaminante: Contaminante
  codigoEstacion: number
  /** Serie de los últimos 7 días (orden cronológico). */
  puntos: readonly PuntoSerie[]
  cargando?: boolean
  error?: unknown
}

/** Último valor por contaminante: stat tile con sparkline, estado y medidor de cobertura. */
export function StatTile({ contaminante, codigoEstacion, puntos, cargando, error }: Props) {
  const ultimo = [...puntos].reverse().find((p) => p.media != null)
  const destino = `/estaciones/${codigoEstacion}?contaminante=${encodeURIComponent(contaminante)}`

  return (
    <Link to={destino} className={`tarjeta tarjeta-enlace stat ${cargando ? 'atenuado' : ''}`} aria-label={`${contaminante}, ver detalle`}>
      <div className="stat-etiqueta">
        <strong>{contaminante}</strong>
        <span>{NOMBRE_CONTAMINANTE[contaminante]}</span>
      </div>

      {error ? (
        <p className="t-s" style={{ color: 'var(--critical)' }}>
          No se pudo cargar la serie.
        </p>
      ) : !ultimo ? (
        <>
          <div className="stat-valor texto-apagado">—</div>
          <p className="stat-nota">Sin datos en los últimos 7 días</p>
        </>
      ) : (
        <>
          <div className="stat-valor">
            {formatearValor(ultimo.media)}
            <span className="stat-unidad">{UNIDAD}</span>
          </div>
          <p className="stat-nota">
            Bloque {etiquetaBloque(ultimo.bloque).toLowerCase()} · {formatearFechaCorta(ultimo.fecha)}
          </p>
          <div>
            <Estado estado={estadoPunto(ultimo)} pildora />
          </div>
        </>
      )}

      <Sparkline
        valores={puntos.map((p) => p.media ?? null)}
        anomalias={puntos.map(esAnomaliaFiable)}
        etiqueta={`${contaminante}, últimos 7 días por bloque`}
      />

      {ultimo && <Medidor valor={ultimo.cobertura} etiqueta="Cobertura del bloque" />}
    </Link>
  )
}
