/**
 * Una consulta al asistente: pregunta + estado de su respuesta.
 *
 * Las consultas forman una conversación: el agente recuerda los turnos
 * anteriores a través de un `session_id` opaco (vive en el ChatProvider;
 * «Nueva conversación» lo descarta). La lista solo vive en el estado de React
 * y desaparece al recargar.
 *
 * Aquí vive también el contrato de los eventos de `/chat/stream` (los define
 * el agente y la API los reenvía tal cual): status | token | passthrough |
 * error | done. El texto llega de UNA de dos formas: como fragmentos `token`
 * que se concatenan o como un único `passthrough` con el texto entero. Las
 * fuentes y la advertencia definitivas van siempre en `done`, junto con los ids
 * del turno (`traza_id`, `span_id`) que necesita la valoración 👍/👎.
 */
import type { FuenteChat, RespuestaChat } from '../api/tipos'
import type { EventoSSE } from './sse'

export type EstadoConsulta =
  | { tipo: 'esperando'; fase?: string }
  | { tipo: 'recibiendo'; texto: string }
  | { tipo: 'ok'; respuesta: RespuestaChat }
  | { tipo: 'error'; error: unknown }

export interface ConsultaChat {
  id: string
  numero: number
  pregunta: string
  estado: EstadoConsulta
}

// ------------------------------------------------- eventos de /chat/stream

export type EventoChat =
  | { tipo: 'status'; fase: string; herramienta: string | null }
  | { tipo: 'token'; texto: string }
  | { tipo: 'passthrough'; texto: string; fuentes: FuenteChat[]; advertencia: string | null }
  | {
      tipo: 'done'
      sessionId: string | null
      trazaId: string | null
      spanId: string | null
      fuentes: FuenteChat[]
      advertencia: string | null
    }
  | { tipo: 'error'; detalle: string | null }

/** Un evento SSE del stream del chat, validado. Tipos desconocidos o datos malformados → null (se ignoran). */
export function interpretarEventoChat({ evento, datos }: EventoSSE): EventoChat | null {
  let crudo: unknown
  try {
    crudo = JSON.parse(datos)
  } catch {
    return null
  }
  if (crudo === null || typeof crudo !== 'object') return null
  const d = crudo as Record<string, unknown>

  switch (evento) {
    case 'status':
      if (typeof d.fase !== 'string') return null
      return { tipo: 'status', fase: d.fase, herramienta: typeof d.herramienta === 'string' ? d.herramienta : null }
    case 'token':
      if (typeof d.texto !== 'string') return null
      return { tipo: 'token', texto: d.texto }
    case 'passthrough':
      if (typeof d.texto !== 'string') return null
      return { tipo: 'passthrough', texto: d.texto, fuentes: fuentesDe(d), advertencia: advertenciaDe(d) }
    case 'done':
      return {
        tipo: 'done',
        sessionId: typeof d.session_id === 'string' ? d.session_id : null,
        trazaId: typeof d.traza_id === 'string' ? d.traza_id : null,
        spanId: typeof d.span_id === 'string' ? d.span_id : null,
        fuentes: fuentesDe(d),
        advertencia: advertenciaDe(d),
      }
    case 'error':
      return { tipo: 'error', detalle: typeof d.detalle === 'string' ? d.detalle : null }
    default:
      return null
  }
}

function fuentesDe(d: Record<string, unknown>): FuenteChat[] {
  if (!Array.isArray(d.fuentes)) return []
  return d.fuentes.filter(
    (f): f is FuenteChat =>
      f !== null &&
      typeof f === 'object' &&
      ((f as FuenteChat).tipo === 'sql' || (f as FuenteChat).tipo === 'documento') &&
      typeof (f as FuenteChat).referencia === 'string',
  )
}

function advertenciaDe(d: Record<string, unknown>): string | null {
  return typeof d.advertencia === 'string' ? d.advertencia : null
}

// ------------------------------------------------- textos del indicador

/** Fases del turno que publica el agente (evento `status`). */
const TEXTO_FASE: Record<string, string> = {
  en_espera: 'En cola: hay otra consulta en curso',
  clasificando: 'Entendiendo la pregunta',
  buscando: 'Consultando las fuentes',
  redactando: 'Redactando la respuesta',
  validando: 'Comprobando la respuesta',
}

const TEXTO_HERRAMIENTA: Record<string, string> = {
  buscar_evidencias: 'Buscando en la documentación',
  consultar_datos: 'Consultando las mediciones',
}

/** Texto del indicador de progreso. Fases o herramientas nuevas caen en un genérico. */
export function textoFase(fase: string, herramienta: string | null = null): string {
  if (herramienta !== null && herramienta in TEXTO_HERRAMIENTA) return TEXTO_HERRAMIENTA[herramienta]
  return TEXTO_FASE[fase] ?? 'Procesando la pregunta'
}
