import { useMemo } from 'react'
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'

import type { PuntoSerie } from '../../api/tipos'
import { UNIDAD } from '../../dominio/constantes'
import { formatearEntero } from '../../dominio/formato'
import { useColoresTema } from '../../hooks/useColoresTema'
import { Vacio } from '../ui/Avisos'
import { aFilasGrafica, etiquetaClave, ticksPorDia, type FilaGrafica } from './datos'
import { LeyendaEstados } from './LeyendaEstados'
import { TooltipSerie } from './TooltipSerie'

interface Props {
  puntos: readonly PuntoSerie[]
  /** Título accesible de la figura (p. ej. "NO2 · Escuelas Aguirre, 7 días"). */
  titulo: string
  alto?: number
}

interface PropsMarcador {
  cx?: number
  cy?: number
  index?: number
  payload?: FilaGrafica
}

/**
 * Tendencia de una serie por bloques: una línea, sin leyenda de series (el
 * título la nombra). Los nulos son huecos (`connectNulls={false}`). El color
 * solo codifica estado de anomalía en los marcadores: rojo = anomalía fiable,
 * ámbar hueco = cobertura baja (no evaluada).
 */
export function GraficaLinea({ puntos, titulo, alto = 280 }: Props) {
  const colores = useColoresTema()
  const filas = useMemo(() => aFilasGrafica(puntos), [puntos])
  const ticks = useMemo(() => ticksPorDia(filas), [filas])

  if (filas.length === 0) {
    return <Vacio>Sin datos para esta estación, contaminante y rango.</Vacio>
  }

  const marcador = (props: PropsMarcador) => {
    const { cx, cy, index, payload } = props
    const clave = `m-${index ?? 0}`
    if (cx == null || cy == null || !payload || payload.media == null) return <g key={clave} />
    if (payload.estado === 'anomalia') {
      return <circle key={clave} cx={cx} cy={cy} r={5} fill={colores.critical} stroke={colores.superficie} strokeWidth={2} />
    }
    if (payload.estado === 'baja_confianza') {
      return <circle key={clave} cx={cx} cy={cy} r={4} fill={colores.superficie} stroke={colores.warning} strokeWidth={2} />
    }
    return <g key={clave} />
  }

  return (
    <figure aria-label={titulo}>
      <div style={{ width: '100%', height: alto }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={filas} margin={{ top: 12, right: 16, bottom: 4, left: 0 }}>
            <CartesianGrid stroke={colores.rejilla} vertical={false} />
            <XAxis
              dataKey="clave"
              ticks={ticks}
              tickFormatter={etiquetaClave}
              tick={{ fontSize: 11, fill: colores.apagado }}
              axisLine={{ stroke: colores.rejilla }}
              tickLine={false}
              interval={0}
              minTickGap={24}
            />
            <YAxis
              domain={[0, 'auto']}
              tickFormatter={(v: number) => formatearEntero(v)}
              tick={{ fontSize: 11, fill: colores.apagado, className: 'tabular' }}
              axisLine={false}
              tickLine={false}
              width={44}
            />
            <Tooltip
              content={(props) => <TooltipSerie {...props} />}
              cursor={{ stroke: colores.apagado, strokeWidth: 1 }}
              isAnimationActive={false}
            />
            <Line
              type="monotone"
              dataKey="media"
              stroke={colores.dato}
              strokeWidth={2}
              connectNulls={false}
              dot={marcador}
              activeDot={{ r: 5, fill: colores.dato, stroke: colores.superficie, strokeWidth: 2 }}
              isAnimationActive={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
      <figcaption>
        <LeyendaEstados prefijo={`Línea: media del bloque en ${UNIDAD}. Huecos: sin dato.`} />
      </figcaption>
    </figure>
  )
}
