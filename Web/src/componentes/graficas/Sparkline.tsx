import { useRef } from 'react'

import { formatearValor } from '../../dominio/formato'
import { useAncho } from '../../hooks/useAncho'

interface Props {
  /** Un valor por bloque, en orden cronológico; null = hueco. */
  valores: readonly (number | null)[]
  /** Anomalías fiables (cobertura suficiente) alineadas con `valores`. */
  anomalias?: readonly boolean[]
  etiqueta: string
  alto?: number
}

/**
 * Sparkline SVG sin dependencias. Los huecos se dibujan como huecos; el último
 * dato se marca en el color de dato y las anomalías fiables en el de estado.
 */
export function Sparkline({ valores, anomalias = [], etiqueta, alto = 36 }: Props) {
  const ref = useRef<HTMLDivElement>(null)
  const ancho = useAncho(ref, 160) || 160

  const conDato = valores.map((v, i) => ({ v, i })).filter((d): d is { v: number; i: number } => d.v != null)
  const n = valores.length

  if (conDato.length === 0 || n < 2) {
    return (
      <div ref={ref} className="sparkline texto-apagado t-xs" style={{ height: alto, display: 'grid', placeItems: 'center' }}>
        sin datos en 7 días
      </div>
    )
  }

  const min = Math.min(...conDato.map((d) => d.v))
  const max = Math.max(...conDato.map((d) => d.v))
  const pad = 5
  const x = (i: number) => pad + (i / (n - 1)) * (ancho - 2 * pad)
  const y = (v: number) => (max === min ? alto / 2 : alto - pad - ((v - min) / (max - min)) * (alto - 2 * pad))

  // Segmentos separados por los huecos: un valor nulo corta la línea.
  const segmentos: string[] = []
  let actual: string[] = []
  valores.forEach((v, i) => {
    if (v == null) {
      if (actual.length) segmentos.push(actual.join(' '))
      actual = []
      return
    }
    actual.push(`${actual.length === 0 ? 'M' : 'L'}${x(i).toFixed(1)},${y(v).toFixed(1)}`)
  })
  if (actual.length) segmentos.push(actual.join(' '))

  const ultimo = conDato[conDato.length - 1]
  const descripcion = `${etiqueta}: ${conDato.length} de ${n} bloques con dato, mínimo ${formatearValor(min)}, máximo ${formatearValor(max)}, último ${formatearValor(ultimo.v)}`

  return (
    <div ref={ref}>
      <svg className="sparkline" width={ancho} height={alto} viewBox={`0 0 ${ancho} ${alto}`} role="img" aria-label={descripcion}>
        {segmentos.map((d, i) => (
          <path key={i} d={d} fill="none" strokeWidth={2} strokeLinejoin="round" strokeLinecap="round" style={{ stroke: 'var(--apagado)' }} />
        ))}
        {/* Puntos aislados (un dato entre dos huecos) no forman segmento: se marcan para no perderlos */}
        {valores.map((v, i) =>
          v != null && (valores[i - 1] ?? null) == null && (valores[i + 1] ?? null) == null ? (
            <circle key={`a-${i}`} cx={x(i)} cy={y(v)} r={2} style={{ fill: 'var(--apagado)' }} />
          ) : null,
        )}
        {valores.map((v, i) =>
          v != null && anomalias[i] ? (
            <circle key={`an-${i}`} cx={x(i)} cy={y(v)} r={4} strokeWidth={2} style={{ fill: 'var(--critical)', stroke: 'var(--superficie)' }} />
          ) : null,
        )}
        {!anomalias[ultimo.i] && (
          <circle cx={x(ultimo.i)} cy={y(ultimo.v)} r={4} strokeWidth={2} style={{ fill: 'var(--dato)', stroke: 'var(--superficie)' }} />
        )}
      </svg>
    </div>
  )
}
