/** Preparación de datos compartida por las gráficas de serie. */
import type { Bloque, PuntoSerie } from '../../api/tipos'
import { estadoPunto, type EstadoPunto } from '../../dominio/anomalias'
import { BLOQUES, ETIQUETA_BLOQUE, HORAS_BLOQUE } from '../../dominio/constantes'
import { formatearFechaCorta, formatearFechaLarga } from '../../dominio/fechas'

export interface FilaGrafica {
  /** `fecha|bloque`, único por punto y ordenable cronológicamente. */
  clave: string
  fecha: string
  bloque: string
  /** null = hueco en la gráfica, nunca cero. */
  media: number | null
  estado: EstadoPunto
  punto: PuntoSerie
}

export function indiceBloque(bloque: string): number {
  const i = BLOQUES.indexOf(bloque as Bloque)
  return i < 0 ? BLOQUES.length : i
}

export function etiquetaBloque(bloque: string): string {
  return ETIQUETA_BLOQUE[bloque as Bloque] ?? bloque
}

export function horasBloque(bloque: string): string {
  return HORAS_BLOQUE[bloque as Bloque] ?? ''
}

export function clavePunto(p: Pick<PuntoSerie, 'fecha' | 'bloque'>): string {
  return `${p.fecha}|${p.bloque}`
}

export function compararPuntos(a: Pick<PuntoSerie, 'fecha' | 'bloque'>, b: Pick<PuntoSerie, 'fecha' | 'bloque'>): number {
  if (a.fecha !== b.fecha) return a.fecha < b.fecha ? -1 : 1
  return indiceBloque(a.bloque) - indiceBloque(b.bloque)
}

export function aFilasGrafica(puntos: readonly PuntoSerie[]): FilaGrafica[] {
  return [...puntos].sort(compararPuntos).map((p) => ({
    clave: clavePunto(p),
    fecha: p.fecha,
    bloque: p.bloque,
    media: p.media ?? null,
    estado: estadoPunto(p),
    punto: p,
  }))
}

/** Claves del primer bloque de cada día, muestreadas para que quepan ~`maxTicks` etiquetas. */
export function ticksPorDia(filas: readonly Pick<FilaGrafica, 'clave' | 'fecha'>[], maxTicks = 10): string[] {
  const primeros: string[] = []
  let ultimaFecha = ''
  for (const f of filas) {
    if (f.fecha !== ultimaFecha) {
      primeros.push(f.clave)
      ultimaFecha = f.fecha
    }
  }
  const paso = Math.max(1, Math.ceil(primeros.length / maxTicks))
  return primeros.filter((_, i) => i % paso === 0)
}

export function etiquetaClave(clave: string): string {
  const [fecha] = clave.split('|')
  return formatearFechaCorta(fecha)
}

export function tituloClave(clave: string): string {
  const [fecha, bloque] = clave.split('|')
  return `${formatearFechaLarga(fecha)} · ${etiquetaBloque(bloque)} (${horasBloque(bloque)})`
}
