import { useMemo } from 'react'

import { cubosHistograma, formatearUsd, posicionEnEje } from '../../dominio/kpis'
import { formatearEntero, pluralizar } from '../../dominio/formato'
import { Vacio } from '../ui/Avisos'

interface Props {
  /** Coste de cada turno con tokens completos, en USD. */
  valores: readonly number[]
  p50: number | null | undefined
  p90: number | null | undefined
}

/** Histograma del coste por turno con las marcas de P50 y P90 sobre el mismo eje. */
export function HistogramaCoste({ valores, p50, p90 }: Props) {
  const cubos = useMemo(() => cubosHistograma(valores), [valores])
  if (cubos.length === 0) return <Vacio>Ningún turno del periodo tiene coste calculable.</Vacio>

  const maxN = Math.max(...cubos.map((c) => c.n))
  const marcas = [
    { etiqueta: 'P50', valor: p50 },
    { etiqueta: 'P90', valor: p90 },
  ].filter((m): m is { etiqueta: string; valor: number } => m.valor != null)
  const resumen = `Coste por turno de ${formatearUsd(cubos[0].desde)} a ${formatearUsd(cubos[cubos.length - 1].hasta)}; ${marcas
    .map((m) => `${m.etiqueta} ${formatearUsd(m.valor)}`)
    .join(', ')}`

  return (
    <figure>
      <div className="histograma" role="img" aria-label={resumen}>
        <div className="histograma-barras">
          {cubos.map((c) => (
            <div
              key={c.desde}
              className="histograma-cubo"
              title={`${formatearUsd(c.desde)} – ${formatearUsd(c.hasta)}: ${c.n} ${pluralizar(c.n, 'turno', 'turnos')}`}
            >
              <i style={{ height: `${(c.n / maxN) * 100}%` }} />
            </div>
          ))}
          {marcas.map((m) => (
            <span key={m.etiqueta} className="histograma-marca" style={{ left: `${posicionEnEje(m.valor, cubos) * 100}%` }}>
              <span>{m.etiqueta}</span>
            </span>
          ))}
        </div>
        <div className="histograma-eje tabular">
          <span>{formatearUsd(cubos[0].desde)}</span>
          <span>{formatearUsd(cubos[cubos.length - 1].hasta)}</span>
        </div>
      </div>
      <figcaption className="stat-nota">
        {formatearEntero(valores.length)} {pluralizar(valores.length, 'turno', 'turnos')} con coste. Altura: turnos por
        tramo de coste. Marcas: {marcas.map((m) => `${m.etiqueta} ${formatearUsd(m.valor)}`).join(' · ')}.
      </figcaption>
    </figure>
  )
}
