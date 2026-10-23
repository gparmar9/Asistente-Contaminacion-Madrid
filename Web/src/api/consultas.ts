/**
 * Funciones de acceso a la API y hooks de TanStack Query.
 *
 * Las funciones `obtener*`/`preguntar` hacen la llamada tipada y convierten
 * cualquier fallo en `ErrorApi`; los hooks añaden caché, estados de carga y
 * reintentos. Los componentes solo usan los hooks.
 */
import { useQueries, useQuery } from '@tanstack/react-query'

import { interpretarEventoChat, textoFase, type EventoChat } from '../dominio/chat'
import type { DiasKpis } from '../dominio/kpis'
import { crearParserSSE } from '../dominio/sse'
import { cliente } from './cliente'
import { ErrorApi } from './errores'
import type {
  Contaminante,
  Estacion,
  Kpis,
  ParametrosSerie,
  RespuestaChat,
  Salud,
  SerieBloques,
  ValoracionChat,
} from './tipos'

/** Techo del cliente para /chat: por encima del timeout de nginx (180 s) para que, si se agota, el 504 sea del servidor. */
export const TIEMPO_MAX_CHAT_MS = 185_000

export const claves = {
  salud: ['salud'] as const,
  estaciones: ['estaciones'] as const,
  serie: (codigo: number | undefined, params: ParametrosSerie) => ['serie', codigo, params] as const,
  kpis: (dias: DiasKpis) => ['kpis', dias] as const,
}

export async function obtenerSalud(signal?: AbortSignal): Promise<Salud> {
  try {
    const { data, response } = await cliente.GET('/health', { signal })
    if (!response.ok || data === undefined) throw ErrorApi.desdeRespuesta(response, data)
    return data
  } catch (error) {
    throw ErrorApi.desdeExcepcion(error)
  }
}

export async function obtenerEstaciones(signal?: AbortSignal): Promise<Estacion[]> {
  try {
    const { data, response } = await cliente.GET('/estaciones', { signal })
    if (!response.ok || data === undefined) throw ErrorApi.desdeRespuesta(response, data)
    return data
  } catch (error) {
    throw ErrorApi.desdeExcepcion(error)
  }
}

export async function obtenerSerie(
  codigo: number,
  params: ParametrosSerie,
  signal?: AbortSignal,
): Promise<SerieBloques> {
  try {
    const { data, error, response } = await cliente.GET('/estaciones/{codigo_corto}/series', {
      params: { path: { codigo_corto: codigo }, query: params },
      signal,
    })
    if (!response.ok || data === undefined) throw ErrorApi.desdeRespuesta(response, error)
    return data
  } catch (error) {
    throw ErrorApi.desdeExcepcion(error)
  }
}

export async function preguntar(pregunta: string, signal?: AbortSignal): Promise<RespuestaChat> {
  try {
    const { data, error, response } = await cliente.POST('/chat', {
      body: { pregunta },
      signal: signal ?? AbortSignal.timeout(TIEMPO_MAX_CHAT_MS),
    })
    if (!response.ok || data === undefined) throw ErrorApi.desdeRespuesta(response, error)
    return data
  } catch (error) {
    throw ErrorApi.desdeExcepcion(error)
  }
}

/** Avisos de progreso del stream; el resultado final lo da la promesa de `preguntarStream`. */
export interface ManejadoresStream {
  /** Nueva fase del turno, ya como texto para el indicador («Consultando las fuentes»…). */
  alCambiarFase?: (fase: string) => void
  /** Un fragmento más de la respuesta definitiva (se concatenan en orden). */
  alRecibirTexto?: (fragmento: string) => void
}

/**
 * POST /chat/stream: la misma pregunta que `preguntar`, con la respuesta en SSE.
 *
 * Con el agente, el texto llega token a token y `done` cierra con las fuentes y
 * la advertencia definitivas; sin él, la API manda un único `passthrough` + `done`
 * (puede tardar hasta ~120 s sin ningún evento previo). Un evento `error` o un
 * stream cortado sin `done` significan respuesta incompleta → ErrorApi.
 */
export async function preguntarStream(
  pregunta: string,
  sessionId: string | null,
  manejadores: ManejadoresStream = {},
  signal?: AbortSignal,
): Promise<RespuestaChat> {
  const cuerpo: { pregunta: string; session_id?: string } = { pregunta }
  if (sessionId !== null) cuerpo.session_id = sessionId

  let respuesta: Response
  try {
    respuesta = await fetch('/api/chat/stream', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'text/event-stream' },
      body: JSON.stringify(cuerpo),
      signal: signal ?? AbortSignal.timeout(TIEMPO_MAX_CHAT_MS),
    })
  } catch (error) {
    throw ErrorApi.desdeExcepcion(error)
  }
  if (!respuesta.ok || respuesta.body === null) {
    // Los fallos previos al stream (422, 503…) llegan como JSON normal de FastAPI.
    const detalle: unknown = await respuesta.json().catch(() => null)
    throw ErrorApi.desdeRespuesta(respuesta, detalle)
  }

  const lector = respuesta.body.getReader()
  const decodificador = new TextDecoder()
  const alimentar = crearParserSSE()
  let tokens = ''
  let textoEntero: string | null = null
  let cierre: Extract<EventoChat, { tipo: 'done' }> | null = null

  try {
    for (;;) {
      const { done, value } = await lector.read()
      for (const bruto of alimentar(decodificador.decode(value, { stream: !done }))) {
        const evento = interpretarEventoChat(bruto)
        if (evento === null) continue
        switch (evento.tipo) {
          case 'status':
            manejadores.alCambiarFase?.(textoFase(evento.fase, evento.herramienta))
            break
          case 'token':
            tokens += evento.texto
            manejadores.alRecibirTexto?.(evento.texto)
            break
          case 'passthrough':
            textoEntero = evento.texto
            break
          case 'error':
            // El turno falló con el stream abierto: mismo trato que un 503 de /chat.
            throw new ErrorApi(503, evento.detalle)
          case 'done':
            cierre = evento
            break
        }
      }
      if (done) break
    }
  } catch (error) {
    throw ErrorApi.desdeExcepcion(error)
  }

  if (cierre === null) throw new ErrorApi(0, 'La conexión se cortó antes de completarse la respuesta')
  return {
    respuesta: textoEntero ?? tokens,
    fuentes: cierre.fuentes,
    advertencia: cierre.advertencia,
    session_id: cierre.sessionId,
    traza_id: cierre.trazaId,
    span_id: cierre.spanId,
  }
}

/** Envía la valoración del turno sin reintentos. */
export async function valorar(valoracion: ValoracionChat): Promise<void> {
  try {
    const { error, response } = await cliente.POST('/chat/valoracion', { body: valoracion })
    if (!response.ok) throw ErrorApi.desdeRespuesta(response, error)
  } catch (error) {
    throw ErrorApi.desdeExcepcion(error)
  }
}

export async function obtenerKpis(dias: DiasKpis, signal?: AbortSignal): Promise<Kpis> {
  try {
    const { data, error, response } = await cliente.GET('/kpis', { params: { query: { dias } }, signal })
    if (!response.ok || data === undefined) throw ErrorApi.desdeRespuesta(response, error)
    return data
  } catch (error) {
    throw ErrorApi.desdeExcepcion(error)
  }
}

// ---------------------------------------------------------------- hooks

export function useSalud() {
  return useQuery({
    queryKey: claves.salud,
    queryFn: ({ signal }) => obtenerSalud(signal),
    staleTime: 60_000,
    retry: 0,
  })
}

export function useEstaciones() {
  return useQuery({
    queryKey: claves.estaciones,
    queryFn: ({ signal }) => obtenerEstaciones(signal),
    staleTime: 10 * 60_000,
  })
}

export function useSerie(codigo: number | undefined, params: ParametrosSerie | undefined) {
  return useQuery({
    queryKey: claves.serie(codigo, params ?? { contaminante: 'NO2' }),
    queryFn: ({ signal }) => obtenerSerie(codigo as number, params as ParametrosSerie, signal),
    enabled: codigo !== undefined && params !== undefined,
    // Un 404 (estación inexistente) o un 400 (rango inválido) no mejoran reintentando.
    retry: (intentos, error) => intentos < 1 && !(error instanceof ErrorApi && error.status >= 400 && error.status < 500),
    placeholderData: (anterior) => anterior,
  })
}

/** Una serie por contaminante (la pantalla Hoy y los stat tiles). */
export function useSeriesPorContaminante(
  codigo: number | undefined,
  contaminantes: readonly Contaminante[],
  rango: { desde: string; hasta: string },
) {
  return useQueries({
    queries: contaminantes.map((contaminante) => {
      const params: ParametrosSerie = { contaminante, desde: rango.desde, hasta: rango.hasta }
      return {
        queryKey: claves.serie(codigo, params),
        queryFn: ({ signal }: { signal: AbortSignal }) => obtenerSerie(codigo as number, params, signal),
        enabled: codigo !== undefined,
        retry: 1,
      }
    }),
  })
}

/** La misma serie (contaminante + rango) para varias estaciones (comparar, máx. 3). */
export function useSeriesPorEstacion(codigos: readonly number[], params: ParametrosSerie) {
  return useQueries({
    queries: codigos.map((codigo) => ({
      queryKey: claves.serie(codigo, params),
      queryFn: ({ signal }: { signal: AbortSignal }) => obtenerSerie(codigo, params, signal),
      retry: 1,
    })),
  })
}

/** KPIs del asistente. El agente ya los cachea 60 s: refrescar antes no trae nada nuevo. */
export function useKpis(dias: DiasKpis) {
  return useQuery({
    queryKey: claves.kpis(dias),
    queryFn: ({ signal }) => obtenerKpis(dias, signal),
    staleTime: 60_000,
    retry: 0,
    placeholderData: (anterior) => anterior,
  })
}
