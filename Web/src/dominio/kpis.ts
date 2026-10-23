/**
 * Formato y cálculos de la página «Uso del asistente» (GET /kpis).
 *
 * Los agregados llegan calculados del agente; aquí solo se derivan porcentajes,
 * cubos del histograma y etiquetas. Un dato ausente se muestra como "—" y una
 * proporción sin denominador es null, nunca 0 %.
 */
import type { Satisfaccion, Votos } from '../api/tipos'
import { formatearEntero, pluralizar, SIN_DATO } from './formato'

export const DIAS_KPIS = [1, 7, 30] as const
export type DiasKpis = (typeof DIAS_KPIS)[number]

const NBSP = ' '

const formatoEntero = new Intl.NumberFormat('es-ES', { maximumFractionDigits: 0 })
const formatoSegundos = new Intl.NumberFormat('es-ES', { maximumFractionDigits: 1 })
const formatoUsd = new Intl.NumberFormat('es-ES', { maximumSignificantDigits: 2 })
const formatoMomento = new Intl.DateTimeFormat('es-ES', { dateStyle: 'medium', timeStyle: 'short', timeZone: 'Europe/Madrid' })

/** Rutas del bucle del agente (`agente.ruta`). Una ruta desconocida se muestra tal cual. */
const ETIQUETA_RUTA: Record<string, string> = {
  datos: 'Datos (SQL)',
  documental: 'Documental (RAG)',
  mixta: 'Mixta',
  fija: 'Frase fija',
  sin_evidencia: 'Sin evidencia',
}

/** Resultado del turno tal como lo clasifica `business/kpis.py`. */
const ETIQUETA_RESULTADO: Record<string, string> = {
  respondida: 'Respondida',
  frase_fija: 'Frase fija',
  sin_evidencia: 'Sin evidencia',
  servicio_no_disponible: 'Servicio no disponible',
  bloqueada: 'Bloqueada',
  error: 'Error',
}

export function etiquetaRuta(ruta: string): string {
  return ETIQUETA_RUTA[ruta] ?? ruta
}

export function etiquetaResultado(resultado: string): string {
  return ETIQUETA_RESULTADO[resultado] ?? resultado
}

/** Proporción 0..1, o null si no hay denominador. */
export function proporcion(parte: number, total: number): number | null {
  return total > 0 ? parte / total : null
}

/** 0..1 → "83 %". */
export function formatearPorcentaje(valor: number | null | undefined): string {
  if (valor == null || Number.isNaN(valor)) return SIN_DATO
  return formatoEntero.format(valor * 100) + NBSP + '%'
}

/** Por debajo de un segundo en ms ("850 ms"); por encima en segundos ("3,2 s"). */
export function formatearMs(ms: number | null | undefined): string {
  if (ms == null || Number.isNaN(ms)) return SIN_DATO
  if (ms < 1000) return formatoEntero.format(ms) + NBSP + 'ms'
  return formatoSegundos.format(ms / 1000) + NBSP + 's'
}

export function formatearSegundos(s: number | null | undefined): string {
  return s == null ? SIN_DATO : formatearMs(s * 1000)
}

/** Dos cifras significativas: los costes por turno son de milésimas de dólar. */
export function formatearUsd(usd: number | null | undefined): string {
  if (usd == null || Number.isNaN(usd)) return SIN_DATO
  return formatoUsd.format(usd) + NBSP + '$'
}

/** Instante ISO con zona → «23 oct 2026, 17:50» en hora de Madrid. */
export function formatearMomento(iso: string): string {
  return formatoMomento.format(new Date(iso))
}

export interface Cubo {
  desde: number
  hasta: number
  n: number
}

/** Histograma de anchura fija entre el mínimo y el máximo. Con un único valor distinto, un solo cubo. */
export function cubosHistograma(valores: readonly number[], numCubos = 10): Cubo[] {
  if (valores.length === 0) return []
  const min = Math.min(...valores)
  const max = Math.max(...valores)
  if (min === max) return [{ desde: min, hasta: max, n: valores.length }]
  const ancho = (max - min) / numCubos
  const cubos = Array.from({ length: numCubos }, (_, i) => ({ desde: min + i * ancho, hasta: min + (i + 1) * ancho, n: 0 }))
  for (const v of valores) {
    // El máximo cae en el último cubo, que es cerrado por la derecha.
    cubos[Math.min(numCubos - 1, Math.floor((v - min) / ancho))].n += 1
  }
  return cubos
}

/** Posición 0..1 de un valor sobre el eje del histograma (para las marcas P50/P90). */
export function posicionEnEje(valor: number, cubos: readonly Cubo[]): number {
  if (cubos.length === 0) return 0
  const desde = cubos[0].desde
  const hasta = cubos[cubos.length - 1].hasta
  if (hasta === desde) return 0.5
  return Math.max(0, Math.min(1, (valor - desde) / (hasta - desde)))
}

export interface FilaSatisfaccion extends Votos {
  /** Positivos / votos, 0..1. */
  tasa: number
}

/** Mismo orden que el agente (más votos primero); los grupos sin votos no llegan. */
export function filasSatisfaccion(votos: readonly Votos[]): FilaSatisfaccion[] {
  return votos.map((v) => ({ ...v, tasa: v.positivos / v.votos }))
}

export const SIN_VALORACIONES = 'Sin valoraciones en el periodo'

/** Indicador de satisfacción: % de 👍 con su n, o el aviso de que no hay votos (null ≠ 0 %). */
export function indicadorSatisfaccion(satisfaccion: Satisfaccion | null): { valor: string; nota: string } {
  if (satisfaccion === null) return { valor: SIN_DATO, nota: SIN_VALORACIONES }
  const { positivos, votos } = satisfaccion
  return {
    valor: formatearPorcentaje(proporcion(positivos, votos)),
    nota: `${formatearEntero(votos)} ${pluralizar(votos, 'voto', 'votos')}`,
  }
}

/** Sesiones con voto / sesiones que pudieron ver el aviso. */
export function participacion(satisfaccion: Satisfaccion): number | null {
  return proporcion(satisfaccion.sesiones_con_voto, satisfaccion.sesiones_elegibles)
}
