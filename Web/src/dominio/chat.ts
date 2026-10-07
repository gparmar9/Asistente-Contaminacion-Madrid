/**
 * Una consulta al asistente: pregunta + estado de su respuesta.
 *
 * Cada consulta es independiente (el backend no tiene memoria). La lista solo
 * vive en el estado de React y desaparece al recargar.
 */
import type { RespuestaChat } from '../api/tipos'

export type EstadoConsulta =
  | { tipo: 'esperando' }
  | { tipo: 'ok'; respuesta: RespuestaChat }
  | { tipo: 'error'; error: unknown }

export interface ConsultaChat {
  id: string
  numero: number
  pregunta: string
  estado: EstadoConsulta
}
