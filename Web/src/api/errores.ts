/**
 * Errores de la API con su código HTTP y mensajes distintos por causa.
 *
 * El contrato del chat distingue tres fallos que el usuario debe poder
 * diferenciar: 422 (pregunta inválida), 503 (proveedor LLM caído) y 502
 * (respuesta del orquestador fuera de contrato). Un único "algo salió mal"
 * ocultaría cuál de los tres servicios falló.
 */
export class ErrorApi extends Error {
  readonly status: number
  readonly detalle: string | null
  readonly tiempoAgotado: boolean

  constructor(status: number, detalle: string | null, opciones?: { tiempoAgotado?: boolean }) {
    super(detalle ?? (status === 0 ? 'Sin conexión con la API' : `Error HTTP ${status}`))
    this.name = 'ErrorApi'
    this.status = status
    this.detalle = detalle
    this.tiempoAgotado = opciones?.tiempoAgotado ?? false
  }

  static desdeRespuesta(response: Response, cuerpo: unknown): ErrorApi {
    return new ErrorApi(response.status, extraerDetalle(cuerpo))
  }

  /** Normaliza cualquier excepción (red, abort, timeout) a ErrorApi. */
  static desdeExcepcion(error: unknown): ErrorApi {
    if (error instanceof ErrorApi) return error
    if (error instanceof DOMException && (error.name === 'TimeoutError' || error.name === 'AbortError')) {
      return new ErrorApi(0, null, { tiempoAgotado: true })
    }
    return new ErrorApi(0, error instanceof Error ? error.message : null)
  }
}

/** FastAPI devuelve `{detail: "texto"}` en HTTPException y `{detail: [{msg,...}]}` en 422. */
function extraerDetalle(cuerpo: unknown): string | null {
  if (!cuerpo || typeof cuerpo !== 'object' || !('detail' in cuerpo)) return null
  const detail = (cuerpo as { detail: unknown }).detail
  if (typeof detail === 'string') return detail
  if (Array.isArray(detail)) {
    const mensajes = detail.map((item) =>
      item && typeof item === 'object' && 'msg' in item ? String((item as { msg: unknown }).msg) : String(item),
    )
    return mensajes.join('; ')
  }
  return null
}

export interface MensajeError {
  titulo: string
  detalle: string
}

/** Mensajes del chat: uno distinto por cada fallo que el contrato distingue. */
export function mensajeErrorChat(error: unknown): MensajeError {
  const e = ErrorApi.desdeExcepcion(error)
  switch (e.status) {
    case 422:
      return {
        titulo: 'Pregunta no válida',
        detalle: 'La pregunta debe tener entre 1 y 2000 caracteres.',
      }
    case 503:
      return {
        titulo: 'El asistente no está disponible',
        detalle:
          'El proveedor del modelo de lenguaje no responde ahora mismo. No es un problema de tu pregunta: vuelve a intentarlo en unos minutos.',
      }
    case 502:
      return {
        titulo: 'Respuesta inválida del orquestador',
        detalle:
          'El servicio que coordina el asistente devolvió una respuesta que no cumple el contrato y la API la ha rechazado. Es un fallo del sistema, no de tu pregunta.',
      }
    case 504:
      return {
        titulo: 'Tiempo de espera agotado',
        detalle:
          'La respuesta tardó más de lo permitido por el servidor. Prueba de nuevo con una pregunta más concreta.',
      }
    case 0:
      return e.tiempoAgotado
        ? {
            titulo: 'Tiempo de espera agotado',
            detalle: 'El asistente no respondió en el tiempo máximo. Puedes volver a intentarlo.',
          }
        : {
            titulo: 'Sin conexión con la API',
            detalle: 'No se pudo contactar con el servidor. Comprueba la conexión o que la API esté levantada.',
          }
    default:
      return {
        titulo: `Error ${e.status}`,
        detalle: e.detalle ?? 'La API devolvió un error inesperado.',
      }
  }
}

/** Mensajes de las consultas de datos (estaciones y series). */
export function mensajeErrorDatos(error: unknown): MensajeError {
  const e = ErrorApi.desdeExcepcion(error)
  switch (e.status) {
    case 404:
      return { titulo: 'Estación no encontrada', detalle: e.detalle ?? 'La estación solicitada no existe.' }
    case 400:
      return { titulo: 'Rango de fechas no válido', detalle: e.detalle ?? "'desde' no puede ser posterior a 'hasta'." }
    case 422:
      return { titulo: 'Parámetros no válidos', detalle: e.detalle ?? 'Revisa el contaminante, el bloque o las fechas.' }
    case 0:
      return {
        titulo: 'Sin conexión con la API',
        detalle: 'No se pudo contactar con el servidor. Comprueba la conexión o que la API esté levantada.',
      }
    default:
      return {
        titulo: `Error ${e.status}`,
        detalle: e.detalle ?? 'La API devolvió un error inesperado al consultar los datos.',
      }
  }
}
