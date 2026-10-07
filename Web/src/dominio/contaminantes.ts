import type { Contaminante } from '../api/tipos'
import { CONTAMINANTES } from './constantes'

/**
 * Contaminantes objetivo que una estación mide, en orden canónico, a partir de
 * `contaminantes_medidos`. No todas miden los 6: pedir a /series uno que la
 * estación no mide devuelve una serie vacía, así que el selector se rellena con
 * esta lista y no con los 6 fijos.
 */
export function contaminantesDisponibles(medidos: readonly string[]): Contaminante[] {
  return CONTAMINANTES.filter((c) => medidos.includes(c))
}
