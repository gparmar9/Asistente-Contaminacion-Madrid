/** Pide valorar el último turno tras dos respuestas del agente, sin envíos en curso. */
import type { ConsultaChat } from './chat'

export const RESPUESTAS_ANTES_DEL_AVISO = 2

export interface TurnoValorable {
  sessionId: string
  trazaId: string
  spanId: string
}

function idsDelTurno(consulta: ConsultaChat): TurnoValorable | null {
  if (consulta.estado.tipo !== 'ok') return null
  const { session_id, traza_id, span_id } = consulta.estado.respuesta
  if (!session_id || !traza_id || !span_id) return null
  return { sessionId: session_id, trazaId: traza_id, spanId: span_id }
}

/** Devuelve el turno a valorar, o null si no corresponde mostrar el aviso. */
export function turnoAValorar(
  consultas: readonly ConsultaChat[],
  enviando: boolean,
  yaRespondidoHoy: boolean,
): TurnoValorable | null {
  if (enviando || yaRespondidoHoy) return null
  const valorables = consultas.map(idsDelTurno).filter((t): t is TurnoValorable => t !== null)
  if (valorables.length < RESPUESTAS_ANTES_DEL_AVISO) return null
  return valorables[valorables.length - 1]
}
