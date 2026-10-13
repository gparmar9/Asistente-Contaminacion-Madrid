import type { TooltipContentProps } from 'recharts'

import { UNIDAD } from '../../dominio/constantes'
import { formatearCobertura, formatearScore, formatearValor } from '../../dominio/formato'
import { Estado } from '../ui/Estado'
import { tituloClave, type FilaGrafica } from './datos'

/** Tooltip de la gráfica de una serie: valores primero, estado con icono y texto. */
export function TooltipSerie({ active, payload }: TooltipContentProps) {
  if (!active || !payload || payload.length === 0) return null
  const fila = payload[0]?.payload as FilaGrafica | undefined
  if (!fila) return null
  const p = fila.punto
  return (
    <div className="tooltip-grafica" role="tooltip">
      <div className="tooltip-grafica-titulo">{tituloClave(fila.clave)}</div>
      <div className="tooltip-fila">
        <span>Media</span>
        <span className="tooltip-valor">
          {formatearValor(p.media)} {p.media != null && UNIDAD}
        </span>
      </div>
      <div className="tooltip-fila">
        <span>Máx / mín</span>
        <span className="tooltip-valor">
          {formatearValor(p.maximo)} / {formatearValor(p.minimo)}
        </span>
      </div>
      <div className="tooltip-fila">
        <span>Cobertura</span>
        <span className="tooltip-valor">{formatearCobertura(p.cobertura)}</span>
      </div>
      <div className="tooltip-fila">
        <span>z-score</span>
        <span className="tooltip-valor">{formatearScore(p.z_score)}</span>
      </div>
      <div className="tooltip-fila">
        <span title="Score negado del Isolation Forest: alto = más anómalo. No es una probabilidad.">Score IF</span>
        <span className="tooltip-valor">{formatearScore(p.anomaly_score)}</span>
      </div>
      <div className="tooltip-fila" style={{ marginTop: 6 }}>
        <Estado estado={fila.estado} conDescripcion={false} />
      </div>
    </div>
  )
}

interface SerieTooltip {
  dataKey: string
  nombre: string
  color: string
}

type PropsMulti = TooltipContentProps & {
  series: readonly SerieTooltip[]
}

/** Tooltip de la comparación: una fila por estación con su clave de línea. */
export function TooltipMultiserie({ active, payload, label, series }: PropsMulti) {
  if (!active || !payload || payload.length === 0) return null
  const fila = payload[0]?.payload as Record<string, number | null | string> | undefined
  if (!fila) return null
  return (
    <div className="tooltip-grafica" role="tooltip">
      <div className="tooltip-grafica-titulo">{typeof label === 'string' ? tituloClave(label) : ''}</div>
      {series.map((s) => {
        const valor = fila[s.dataKey]
        return (
          <div key={s.dataKey} className="tooltip-fila">
            <span>
              <i className="tooltip-clave" style={{ background: s.color }} aria-hidden="true" />
              {s.nombre}
            </span>
            <span className="tooltip-valor">{typeof valor === 'number' ? `${formatearValor(valor)} ${UNIDAD}` : '—'}</span>
          </div>
        )
      })}
    </div>
  )
}
