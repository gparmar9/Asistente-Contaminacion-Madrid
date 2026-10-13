/**
 * Alias de los tipos generados desde el OpenAPI de ApiUsuario.
 *
 * NUNCA se escriben a mano: `schema.d.ts` lo produce `npm run contrato:tipos`
 * a partir de `openapi.json`. Si el backend cambia el contrato, estos alias
 * dejan de compilar y el error aparece en `npm run build`, no en producción.
 */
import type { components, paths } from './schema'

export type Estacion = components['schemas']['Estacion']
export type PuntoSerie = components['schemas']['PuntoSerie']
export type SerieBloques = components['schemas']['SerieBloques']
export type Contaminante = components['schemas']['Contaminante']
export type Bloque = components['schemas']['Bloque']
export type PreguntaChat = components['schemas']['PreguntaChat']
export type RespuestaChat = components['schemas']['RespuestaChat']
export type FuenteChat = components['schemas']['FuenteChat']
export type TipoFuente = FuenteChat['tipo']

/** Query params de GET /estaciones/{codigo_corto}/series (contaminante obligatorio). */
export type ParametrosSerie = NonNullable<
  paths['/estaciones/{codigo_corto}/series']['get']['parameters']['query']
>

export type Salud = paths['/health']['get']['responses']['200']['content']['application/json']
