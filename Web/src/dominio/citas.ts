/**
 * Marcadores de cita `[D1]`, `[D2]`… del texto del asistente.
 *
 * No son markdown: se dejan visibles y se enlazan a la lista de `fuentes`. El
 * texto que devuelve el RAG incluye, al final, una sección "Evidencias
 * citadas:" con líneas `[Dn] Título — sección (archivo, fragmento …)`; de ahí
 * se obtiene el título de cada Dn y se casa con la fuente documental del mismo
 * título. Si no hay sección (o no casa), se usa el orden: la n-ésima fuente
 * documental.
 */
import type { FuenteChat } from '../api/tipos'

export interface TrozoTexto {
  tipo: 'texto'
  valor: string
}

export interface TrozoCita {
  tipo: 'cita'
  /** Número n de `[Dn]`. */
  n: number
}

export type Trozo = TrozoTexto | TrozoCita

const PATRON_CITA = /\[D(\d+)\]/g
const PATRON_LINEA_EVIDENCIA = /^\[D(\d+)\]\s+(.+?)(?:\s+—\s+.*)?$/

/** Divide el texto en trozos de texto plano y marcadores de cita. */
export function trocearConCitas(texto: string): Trozo[] {
  const trozos: Trozo[] = []
  let ultimo = 0
  for (const coincidencia of texto.matchAll(PATRON_CITA)) {
    const inicio = coincidencia.index
    if (inicio > ultimo) trozos.push({ tipo: 'texto', valor: texto.slice(ultimo, inicio) })
    trozos.push({ tipo: 'cita', n: Number(coincidencia[1]) })
    ultimo = inicio + coincidencia[0].length
  }
  if (ultimo < texto.length) trozos.push({ tipo: 'texto', valor: texto.slice(ultimo) })
  return trozos
}

export interface CitaResuelta {
  n: number
  titulo: string | null
  /** Índice en `fuentes` de la fuente documental asociada, o -1 si no se pudo asociar. */
  indiceFuente: number
}

/** Mapa Dn → fuente documental. */
export function resolverCitas(texto: string, fuentes: readonly FuenteChat[]): Map<number, CitaResuelta> {
  const titulos = new Map<number, string>()
  for (const linea of texto.split('\n')) {
    const m = PATRON_LINEA_EVIDENCIA.exec(linea.trim())
    if (m) titulos.set(Number(m[1]), m[2].trim())
  }

  const indicesDocumentales = fuentes
    .map((f, i) => (f.tipo === 'documento' ? i : -1))
    .filter((i) => i >= 0)

  const resultado = new Map<number, CitaResuelta>()
  const usados = new Set<number>()
  for (const { n } of trocearConCitas(texto).filter((t): t is TrozoCita => t.tipo === 'cita')) {
    if (resultado.has(n)) continue
    const titulo = titulos.get(n) ?? null
    let indice = -1
    if (titulo) {
      indice = fuentes.findIndex(
        (f) => f.tipo === 'documento' && (f.referencia === titulo || titulo.startsWith(f.referencia)),
      )
    }
    if (indice < 0) {
      // Sin título: la n-ésima fuente documental, saltando las ya asignadas por título.
      const candidata = indicesDocumentales[n - 1]
      if (candidata !== undefined && !usados.has(candidata)) indice = candidata
    }
    if (indice >= 0) usados.add(indice)
    resultado.set(n, { n, titulo, indiceFuente: indice })
  }
  return resultado
}

/** Id de ancla estable para enlazar una cita con su fuente dentro de una respuesta. */
export function idFuente(idRespuesta: string, indice: number): string {
  return `fuente-${idRespuesta}-${indice}`
}
