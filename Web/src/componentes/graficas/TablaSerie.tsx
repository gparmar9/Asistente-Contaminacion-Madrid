import { useMemo } from 'react'

import type { PuntoSerie } from '../../api/tipos'
import { estadoPunto } from '../../dominio/anomalias'
import { UNIDAD } from '../../dominio/constantes'
import { formatearFechaMedia } from '../../dominio/fechas'
import { formatearCobertura, formatearEntero, formatearScore, formatearValor } from '../../dominio/formato'
import { Estado } from '../ui/Estado'
import { Tabla, type Columna } from '../ui/Tabla'
import { aFilasGrafica, etiquetaBloque, type FilaGrafica } from './datos'

interface Props {
  puntos: readonly PuntoSerie[]
  /** Columna extra inicial (p. ej. contaminante) cuando la tabla mezcla series. */
  columnaInicial?: Columna<FilaGrafica>
  caption?: string
}

/** Alternativa accesible a la línea y al heatmap: todos los valores, nulos como "—". */
export function TablaSerie({ puntos, columnaInicial, caption }: Props) {
  const filas = useMemo(() => aFilasGrafica(puntos), [puntos])

  const columnas: Columna<FilaGrafica>[] = [
    ...(columnaInicial ? [columnaInicial] : []),
    { clave: 'fecha', titulo: 'Fecha', render: (f) => formatearFechaMedia(f.fecha), ordenar: (f) => f.clave },
    { clave: 'bloque', titulo: 'Bloque', render: (f) => etiquetaBloque(f.bloque) },
    { clave: 'media', titulo: `Media (${UNIDAD})`, numerica: true, render: (f) => formatearValor(f.punto.media), ordenar: (f) => f.punto.media ?? null },
    { clave: 'maximo', titulo: 'Máx', numerica: true, render: (f) => formatearValor(f.punto.maximo), ordenar: (f) => f.punto.maximo ?? null },
    { clave: 'minimo', titulo: 'Mín', numerica: true, render: (f) => formatearValor(f.punto.minimo), ordenar: (f) => f.punto.minimo ?? null },
    { clave: 'n_horas', titulo: 'Horas', numerica: true, render: (f) => formatearEntero(f.punto.n_horas), ordenar: (f) => f.punto.n_horas ?? null },
    { clave: 'cobertura', titulo: 'Cobertura', numerica: true, render: (f) => formatearCobertura(f.punto.cobertura), ordenar: (f) => f.punto.cobertura ?? null },
    { clave: 'z', titulo: 'z-score', numerica: true, render: (f) => formatearScore(f.punto.z_score), ordenar: (f) => f.punto.z_score ?? null },
    { clave: 'score', titulo: 'Score IF', numerica: true, render: (f) => formatearScore(f.punto.anomaly_score), ordenar: (f) => f.punto.anomaly_score ?? null },
    { clave: 'estado', titulo: 'Estado', render: (f) => <Estado estado={estadoPunto(f.punto)} suave /> },
  ]

  return (
    <Tabla
      filas={filas}
      columnas={columnas}
      claveFila={(f) => (columnaInicial ? `${columnaInicial.render(f)}-${f.clave}` : f.clave)}
      ordenInicial={{ clave: 'fecha', direccion: 'asc' }}
      caption={
        caption ??
        'Score IF: score negado del Isolation Forest (alto = más anómalo), no es una probabilidad. El estado solo se evalúa con cobertura ≥ 70 %.'
      }
      vacio="Sin bloques en este rango."
    />
  )
}
