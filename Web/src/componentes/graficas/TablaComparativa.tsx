import { useMemo } from 'react'

import { UNIDAD } from '../../dominio/constantes'
import { formatearFechaMedia } from '../../dominio/fechas'
import { formatearValor } from '../../dominio/formato'
import { Tabla, type Columna } from '../ui/Tabla'
import { clavePunto, compararPuntos, etiquetaBloque } from './datos'
import type { SerieComparada } from './GraficaMultilinea'

interface Fila {
  clave: string
  fecha: string
  bloque: string
  valores: Record<number, number | null>
}

/** Alternativa en tabla a la gráfica de comparación: una columna por estación. */
export function TablaComparativa({ series }: { series: readonly SerieComparada[] }) {
  const filas = useMemo(() => {
    const porClave = new Map<string, Fila>()
    for (const s of series) {
      for (const p of s.puntos) {
        const clave = clavePunto(p)
        const fila = porClave.get(clave) ?? { clave, fecha: p.fecha, bloque: p.bloque, valores: {} }
        fila.valores[s.codigo] = p.media ?? null
        porClave.set(clave, fila)
      }
    }
    return [...porClave.values()].sort(compararPuntos)
  }, [series])

  const columnas: Columna<Fila>[] = [
    { clave: 'fecha', titulo: 'Fecha', render: (f) => formatearFechaMedia(f.fecha), ordenar: (f) => f.clave },
    { clave: 'bloque', titulo: 'Bloque', render: (f) => etiquetaBloque(f.bloque) },
    ...series.map<Columna<Fila>>((s) => ({
      clave: `s${s.codigo}`,
      titulo: `${s.nombre} (${UNIDAD})`,
      numerica: true,
      render: (f) => formatearValor(f.valores[s.codigo]),
      ordenar: (f) => f.valores[s.codigo] ?? null,
    })),
  ]

  return (
    <Tabla
      filas={filas}
      columnas={columnas}
      claveFila={(f) => f.clave}
      ordenInicial={{ clave: 'fecha', direccion: 'asc' }}
      caption="Media del bloque por estación. Un guion indica que no hay dato."
      vacio="Sin bloques que comparar."
    />
  )
}
