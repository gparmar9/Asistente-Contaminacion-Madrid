/**
 * Fechas en formato ISO `YYYY-MM-DD` (el de la API), sin zonas horarias:
 * las fechas de la API son días de calendario, no instantes.
 */

export type FechaISO = string

function aDosDigitos(n: number): string {
  return n < 10 ? `0${n}` : String(n)
}

export function aISO(fecha: Date): FechaISO {
  return `${fecha.getFullYear()}-${aDosDigitos(fecha.getMonth() + 1)}-${aDosDigitos(fecha.getDate())}`
}

/** Construye la fecha en hora local a partir del ISO (evita el desfase UTC de `new Date('YYYY-MM-DD')`). */
export function desdeISO(iso: FechaISO): Date {
  const [a, m, d] = iso.slice(0, 10).split('-').map(Number)
  return new Date(a, m - 1, d)
}

export function hoyISO(): FechaISO {
  return aISO(new Date())
}

export function sumarDias(iso: FechaISO, dias: number): FechaISO {
  const fecha = desdeISO(iso)
  fecha.setDate(fecha.getDate() + dias)
  return aISO(fecha)
}

export function esFechaISOValida(valor: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(valor)) return false
  return aISO(desdeISO(valor)) === valor
}

const formatoCorto = new Intl.DateTimeFormat('es-ES', { day: 'numeric', month: 'short' })
const formatoLargo = new Intl.DateTimeFormat('es-ES', { weekday: 'long', day: 'numeric', month: 'long', year: 'numeric' })
const formatoMedio = new Intl.DateTimeFormat('es-ES', { day: 'numeric', month: 'short', year: 'numeric' })
const formatoDiaSemana = new Intl.DateTimeFormat('es-ES', { weekday: 'short' })

/** "2 oct" */
export function formatearFechaCorta(iso: FechaISO): string {
  return formatoCorto.format(desdeISO(iso)).replace('.', '')
}

/** "2 oct 2026" */
export function formatearFechaMedia(iso: FechaISO): string {
  return formatoMedio.format(desdeISO(iso)).replace('.', '')
}

/** "viernes, 2 de octubre de 2026" */
export function formatearFechaLarga(iso: FechaISO): string {
  return formatoLargo.format(desdeISO(iso))
}

/** "vie" */
export function diaSemanaCorto(iso: FechaISO): string {
  return formatoDiaSemana.format(desdeISO(iso)).replace('.', '')
}

/** Lista de días ISO consecutivos entre `desde` y `hasta` (ambos incluidos). */
export function diasEntre(desde: FechaISO, hasta: FechaISO): FechaISO[] {
  const dias: FechaISO[] = []
  let actual = desde
  let guarda = 0
  while (actual <= hasta && guarda < 1000) {
    dias.push(actual)
    actual = sumarDias(actual, 1)
    guarda += 1
  }
  return dias
}

export type ClaveRango = '7d' | '30d' | '90d' | 'personalizado'

export interface Rango {
  desde: FechaISO
  hasta: FechaISO
}

export const RANGOS_PREESTABLECIDOS: readonly { clave: ClaveRango; etiqueta: string; dias: number }[] = [
  { clave: '7d', etiqueta: '7 días', dias: 7 },
  { clave: '30d', etiqueta: '30 días', dias: 30 },
  { clave: '90d', etiqueta: '90 días', dias: 90 },
]

/** Últimos `dias` días terminando hoy (como el valor por defecto de la API: 7 días incluye hoy). */
export function rangoUltimosDias(dias: number, hasta: FechaISO = hoyISO()): Rango {
  return { desde: sumarDias(hasta, -(dias - 1)), hasta }
}
