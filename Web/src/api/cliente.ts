/**
 * Cliente HTTP tipado contra el OpenAPI de ApiUsuario.
 *
 * La base es SIEMPRE la ruta relativa `/api`: en desarrollo la resuelve el proxy
 * de Vite y en producción nginx (mismo origen, sin CORS ni host configurado).
 */
import createClient from 'openapi-fetch'

import type { paths } from './schema'

export const cliente = createClient<paths>({ baseUrl: '/api' })
