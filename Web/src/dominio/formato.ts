/**
 * Formato de números para la interfaz (es-ES). Un valor nulo se muestra como
 * "—": nunca como 0, que sería un dato falso.
 */

const formatoDecimal = new Intl.NumberFormat('es-ES', { maximumFractionDigits: 1 })
const formatoEntero = new Intl.NumberFormat('es-ES', { maximumFractionDigits: 0 })
const formatoScore = new Intl.NumberFormat('es-ES', { minimumFractionDigits: 2, maximumFractionDigits: 2 })

export const SIN_DATO = '—'

/** Espacio no rompible (U+00A0): separa la cifra del símbolo sin permitir el salto de línea. */
const NBSP = ' '

export function formatearValor(valor: number | null | undefined): string {
  if (valor == null || Number.isNaN(valor)) return SIN_DATO
  return formatoDecimal.format(valor)
}

export function formatearEntero(valor: number | null | undefined): string {
  if (valor == null || Number.isNaN(valor)) return SIN_DATO
  return formatoEntero.format(valor)
}

/** z-score y anomaly_score con 2 decimales. El score NO es un porcentaje. */
export function formatearScore(valor: number | null | undefined): string {
  if (valor == null || Number.isNaN(valor)) return SIN_DATO
  return formatoScore.format(valor)
}

/** Cobertura 0..1 → "86 %". */
export function formatearCobertura(valor: number | null | undefined): string {
  if (valor == null || Number.isNaN(valor)) return SIN_DATO
  return formatoEntero.format(Math.max(0, Math.min(1, valor)) * 100) + NBSP + '%'
}

export function pluralizar(n: number, singular: string, plural: string): string {
  return n === 1 ? singular : plural
}
