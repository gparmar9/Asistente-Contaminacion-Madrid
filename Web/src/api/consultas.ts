/**
 * Funciones de acceso a la API y hooks de TanStack Query.
 *
 * Las funciones `obtener*`/`preguntar` hacen la llamada tipada y convierten
 * cualquier fallo en `ErrorApi`; los hooks añaden caché, estados de carga y
 * reintentos. Los componentes solo usan los hooks.
 */
import { useMutation, useQueries, useQuery } from '@tanstack/react-query'

import { cliente } from './cliente'
import { ErrorApi } from './errores'
import type { Contaminante, Estacion, ParametrosSerie, RespuestaChat, Salud, SerieBloques } from './tipos'

/** Techo del cliente para /chat: por encima del timeout de nginx (180 s) para que, si se agota, el 504 sea del servidor. */
export const TIEMPO_MAX_CHAT_MS = 185_000

export const claves = {
  salud: ['salud'] as const,
  estaciones: ['estaciones'] as const,
  serie: (codigo: number | undefined, params: ParametrosSerie) => ['serie', codigo, params] as const,
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

/** POST /chat. Sin reintentos: cada intento gasta una llamada al LLM. */
export function usePreguntar() {
  return useMutation<RespuestaChat, ErrorApi, string>({
    mutationFn: (pregunta) => preguntar(pregunta),
    retry: 0,
  })
}
