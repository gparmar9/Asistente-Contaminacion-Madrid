import { useMemo } from 'react'
import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'

import type { PuntoSerie } from '../../api/tipos'
import { UNIDAD } from '../../dominio/constantes'
import { formatearEntero } from '../../dominio/formato'
import { useColoresTema } from '../../hooks/useColoresTema'
import { Vacio } from '../ui/Avisos'
import { clavePunto, compararPuntos, etiquetaClave, ticksPorDia } from './datos'
import { TooltipMultiserie } from './TooltipSerie'

export interface SerieComparada {
  codigo: number
  nombre: string
  puntos: readonly PuntoSerie[]
  /** Posición fija en la paleta categórica (0..2): el color sigue a la estación, no a su orden actual. */
  colorIndice: 0 | 1 | 2
}

interface Props {
  series: readonly SerieComparada[]
  titulo: string
  alto?: number
}

type FilaMulti = Record<string, number | null | string>

interface PropsEtiqueta {
  x?: number | string
  y?: number | string
  index?: number
}

function acortar(nombre: string, max = 20): string {
  return nombre.length > max ? `${nombre.slice(0, max - 1)}…` : nombre
}

/**
 * Comparar hasta 3 estaciones: multilínea con un solo eje Y (misma magnitud y
 * unidad), colores categóricos en orden fijo, etiqueta directa al final de cada
 * línea y leyenda. Dos contaminantes de escala distinta NUNCA van en la misma gráfica.
 */
export function GraficaMultilinea({ series, titulo, alto = 300 }: Props) {
  const colores = useColoresTema()

  const { filas, ultimos } = useMemo(() => {
    const porClave = new Map<string, FilaMulti>()
    const orden: Array<Pick<PuntoSerie, 'fecha' | 'bloque'>> = []
    for (const s of series) {
      for (const p of s.puntos) {
        const clave = clavePunto(p)
        if (!porClave.has(clave)) {
          porClave.set(clave, { clave })
          orden.push({ fecha: p.fecha, bloque: p.bloque })
        }
        porClave.get(clave)![`s${s.codigo}`] = p.media ?? null
      }
    }
    orden.sort(compararPuntos)
    const filasOrdenadas = orden.map((o) => porClave.get(clavePunto(o))!)
    // Índice del último valor no nulo de cada serie: ahí va la etiqueta directa.
    const ultimos = new Map<number, number>()
    for (const s of series) {
      for (let i = filasOrdenadas.length - 1; i >= 0; i -= 1) {
        if (filasOrdenadas[i][`s${s.codigo}`] != null) {
          ultimos.set(s.codigo, i)
          break
        }
      }
    }
    return { filas: filasOrdenadas, ultimos }
  }, [series])

  const ticks = useMemo(
    () => ticksPorDia(filas.map((f) => ({ clave: String(f.clave), fecha: String(f.clave).split('|')[0] }))),
    [filas],
  )

  if (series.length === 0 || filas.length === 0) {
    return <Vacio>Sin datos que comparar en este rango.</Vacio>
  }

  const descriptores = series.map((s) => ({
    dataKey: `s${s.codigo}`,
    nombre: s.nombre,
    color: colores.categoricos[s.colorIndice],
  }))

  return (
    <figure aria-label={titulo}>
      <div style={{ width: '100%', height: alto }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={filas} margin={{ top: 12, right: 150, bottom: 4, left: 0 }}>
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
              content={(props) => <TooltipMultiserie {...props} series={descriptores} />}
              cursor={{ stroke: colores.apagado, strokeWidth: 1 }}
              isAnimationActive={false}
            />
            {series.map((s, i) => {
              const ultimo = ultimos.get(s.codigo)
              const etiqueta = (props: PropsEtiqueta) => {
                const { x, y, index } = props
                const clave = `e-${s.codigo}-${index ?? 0}`
                if (x == null || y == null || index !== ultimo) return <g key={clave} />
                return (
                  <text key={clave} x={Number(x) + 10} y={Number(y)} dy={4} fontSize={11} fontWeight={600} fill={colores.tinta1}>
                    {acortar(s.nombre)}
                  </text>
                )
              }
              return (
                <Line
                  key={s.codigo}
                  type="monotone"
                  dataKey={`s${s.codigo}`}
                  name={s.nombre}
                  stroke={descriptores[i].color}
                  strokeWidth={2}
                  connectNulls={false}
                  dot={false}
                  activeDot={{ r: 5, fill: descriptores[i].color, stroke: colores.superficie, strokeWidth: 2 }}
                  label={etiqueta}
                  isAnimationActive={false}
                />
              )
            })}
          </LineChart>
        </ResponsiveContainer>
      </div>
      <figcaption>
        <div className="leyenda" aria-label="Leyenda de estaciones">
          {descriptores.map((d) => (
            <span key={d.dataKey} className="leyenda-item">
              <i className="tooltip-clave" style={{ background: d.color, width: 16 }} aria-hidden="true" />
              {d.nombre}
            </span>
          ))}
          <span>Media del bloque en {UNIDAD} · huecos = sin dato</span>
        </div>
      </figcaption>
    </figure>
  )
}
