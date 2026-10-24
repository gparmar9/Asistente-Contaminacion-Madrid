/**
 * Marcadores de cita `[D1]`, `[D2]`… del texto del asistente.
 *
 * El texto que devuelve el RAG incluye, al final, una sección "Evidencias
 * citadas:" con líneas `[Dn] Título — sección (archivo, fragmento …)`; de ahí
 * se obtiene el título de cada Dn y se casa con la fuente documental del mismo
 * título. Si no hay sección (o no casa), se usa el orden: la n-ésima fuente
 * documental. La sección es material interno: se analiza pero no se muestra
 * (`quitarEvidencias`); la lista de fuentes ya la presenta el componente
 * `Fuentes`.
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
const MARCA_EVIDENCIAS = 'Evidencias citadas:'

/**
 * Quita la sección "Evidencias citadas:" del final del texto: es la relación
 * interna Dn → documento, no parte de la respuesta. Solo se corta si lo que
 * sigue a la marca son líneas de evidencia (o vacías), para no tocar un texto
 * que use la frase con otro sentido.
 */
export function quitarEvidencias(texto: string): string {
  const lineas = texto.split('\n')
  const inicio = lineas.findIndex((l) => l.trim() === MARCA_EVIDENCIAS)
  if (inicio < 0) return texto
  const resto = lineas.slice(inicio + 1).filter((l) => l.trim() !== '')
  if (resto.length === 0 || !resto.every((l) => PATRON_LINEA_EVIDENCIA.test(l.trim()))) return texto
  return lineas.slice(0, inicio).join('\n').trimEnd()
}

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

/**
 * Prepara la respuesta para mostrarla como markdown: quita la sección de
 * evidencias y convierte los `[Dn]` en enlaces al ancla de su fuente
 * (`[\[Dn\]](#fuente-…)`). Las citas se resuelven sobre el texto completo
 * (los títulos están en la sección que se quita). Las que no casan con
 * ninguna fuente quedan como texto escapado (`\[Dn\]`) para que markdown no
 * las confunda con un enlace.
 */
export function markdownConCitas(
  texto: string,
  fuentes: readonly FuenteChat[],
  idRespuesta: string,
): string {
  const citas = resolverCitas(texto, fuentes)
  return quitarEvidencias(texto).replace(PATRON_CITA, (_marcador, n) => {
    const cita = citas.get(Number(n))
    const escapado = `\\[D${n}\\]`
    if (!cita || cita.indiceFuente < 0) return escapado
    const titulo = fuentes[cita.indiceFuente].referencia.replaceAll('"', "'")
    return `[${escapado}](#${idFuente(idRespuesta, cita.indiceFuente)} "Fuente documental: ${titulo}")`
  })
}

/**
 * Prepara la respuesta para el chat: markdown sin material de citas. Quita
 * toda sección de evidencias y elimina los marcadores `[Dn]` (con el espacio
 * que los precede, para no dejar huecos antes de la puntuación). La
 * trazabilidad no se pierde: sigue en el backend y en el informe del día.
 */
export function markdownSinCitas(texto: string): string {
  return quitarSeccionesEvidencias(texto).replace(/\s*\[D\d+\]/g, '')
}

/**
 * Versión para el chat de `quitarEvidencias`: elimina la marca y sus líneas
 * de evidencia contiguas estén donde estén. En las respuestas compuestas la
 * sección puede no quedar al final (p. ej. con una nota añadida después), y
 * ahí el corte conservador del final no la elimina.
 */
function quitarSeccionesEvidencias(texto: string): string {
  const lineas = texto.split('\n')
  const resultado: string[] = []
  let i = 0
  while (i < lineas.length) {
    if (lineas[i].trim() === MARCA_EVIDENCIAS) {
      i += 1
      while (i < lineas.length && (lineas[i].trim() === '' || PATRON_LINEA_EVIDENCIA.test(lineas[i].trim()))) i += 1
      continue
    }
    resultado.push(lineas[i])
    i += 1
  }
  return resultado.join('\n').replace(/\n{3,}/g, '\n\n').trimEnd()
}
