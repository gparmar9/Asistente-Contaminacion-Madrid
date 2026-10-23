import { describe, expect, it } from 'vitest'

import type { ConsultaChat } from './chat'
import { turnoAValorar } from './valoracion'

let numero = 0
function respondida(ids: { session_id?: string | null; traza_id?: string | null; span_id?: string | null } = {}): ConsultaChat {
  numero += 1
  return {
    id: `c${numero}`,
    numero,
    pregunta: `Pregunta ${numero}`,
    estado: {
      tipo: 'ok',
      respuesta: {
        respuesta: 'Respuesta',
        fuentes: [],
        advertencia: null,
        session_id: 'sesion',
        traza_id: `traza${numero}`,
        span_id: `span${numero}`,
        ...ids,
      },
    },
  }
}

const fallida: ConsultaChat = { id: 'f', numero: 99, pregunta: 'x', estado: { tipo: 'error', error: new Error('caído') } }
const enCurso: ConsultaChat = { id: 'e', numero: 98, pregunta: 'y', estado: { tipo: 'recibiendo', texto: 'Hola' } }

describe('turnoAValorar', () => {
  it('no aparece con una sola respuesta', () => {
    expect(turnoAValorar([respondida()], false, false)).toBeNull()
  })

  it('tras la segunda respuesta completa pide la valoración del último turno', () => {
    const consultas = [respondida(), respondida()]
    const ultima = consultas[1].estado.tipo === 'ok' ? consultas[1].estado.respuesta : null
    expect(turnoAValorar(consultas, false, false)).toEqual({
      sessionId: 'sesion',
      trazaId: ultima?.traza_id,
      spanId: ultima?.span_id,
    })
  })

  it('nunca con una pregunta en vuelo', () => {
    expect(turnoAValorar([respondida(), respondida(), enCurso], true, false)).toBeNull()
  })

  it('una vez al día: si ya se respondió hoy, no vuelve', () => {
    expect(turnoAValorar([respondida(), respondida()], false, true)).toBeNull()
  })

  it('las respuestas fallidas no cuentan', () => {
    expect(turnoAValorar([respondida(), fallida], false, false)).toBeNull()
  })

  it('sin los ids del agente (orquestador o stub) no se puede anotar', () => {
    const sinAgente = { traza_id: null, span_id: null }
    expect(turnoAValorar([respondida(sinAgente), respondida(sinAgente)], false, false)).toBeNull()
    expect(turnoAValorar([respondida({ session_id: null }), respondida()], false, false)).toBeNull()
  })

  it('con un error al final se valora la última respuesta completa', () => {
    const segunda = respondida()
    const turno = turnoAValorar([respondida(), segunda, fallida], false, false)
    expect(turno?.spanId).toBe(segunda.estado.tipo === 'ok' ? segunda.estado.respuesta.span_id : undefined)
  })
})
