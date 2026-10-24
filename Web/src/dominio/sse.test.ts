import { describe, expect, it } from 'vitest'

import { interpretarEventoChat, textoFase } from './chat'
import { crearParserSSE } from './sse'

describe('crearParserSSE', () => {
  it('interpreta un evento completo', () => {
    const alimentar = crearParserSSE()
    expect(alimentar('event: token\ndata: {"texto":"Hola"}\n\n')).toEqual([
      { evento: 'token', datos: '{"texto":"Hola"}' },
    ])
  })

  it('junta un evento partido en varios trozos', () => {
    const alimentar = crearParserSSE()
    expect(alimentar('event: tok')).toEqual([])
    expect(alimentar('en\ndata: {"texto":"Ho')).toEqual([])
    expect(alimentar('la"}\n\n')).toEqual([{ evento: 'token', datos: '{"texto":"Hola"}' }])
  })

  it('separa varios eventos que llegan en el mismo trozo', () => {
    const alimentar = crearParserSSE()
    const eventos = alimentar('event: status\ndata: {"fase":"buscando"}\n\nevent: token\ndata: {"texto":"a"}\n\n')
    expect(eventos.map((e) => e.evento)).toEqual(['status', 'token'])
  })

  it('une varias líneas data con salto de línea, como manda el formato', () => {
    const alimentar = crearParserSSE()
    expect(alimentar('data: línea 1\ndata: línea 2\n\n')).toEqual([{ evento: 'message', datos: 'línea 1\nlínea 2' }])
  })

  it('tolera finales de línea \\r\\n', () => {
    const alimentar = crearParserSSE()
    expect(alimentar('event: done\r\ndata: {}\r\n\r\n')).toEqual([{ evento: 'done', datos: '{}' }])
  })

  it('ignora comentarios y bloques sin data (el heartbeat nunca llega vacío, pero por si acaso)', () => {
    const alimentar = crearParserSSE()
    expect(alimentar(': comentario\n\nevent: status\n\n')).toEqual([])
  })

  it('no recorta espacios más allá del primero tras los dos puntos', () => {
    const alimentar = crearParserSSE()
    expect(alimentar('data:  con espacio\n\n')).toEqual([{ evento: 'message', datos: ' con espacio' }])
  })
})

describe('interpretarEventoChat', () => {
  it('token y status válidos', () => {
    expect(interpretarEventoChat({ evento: 'token', datos: '{"texto":"Hola"}' })).toEqual({
      tipo: 'token',
      texto: 'Hola',
    })
    expect(interpretarEventoChat({ evento: 'status', datos: '{"fase":"buscando","herramienta":"consultar_datos"}' })).toEqual({
      tipo: 'status',
      fase: 'buscando',
      herramienta: 'consultar_datos',
    })
  })

  it('done con y sin session_id, filtrando fuentes malformadas', () => {
    const datos = JSON.stringify({
      session_id: 'abc',
      traza_id: 't1',
      span_id: 's1',
      fuentes: [
        { tipo: 'sql', referencia: 'mediciones' },
        { tipo: 'prediccion', referencia: 'Predicción para el 2026-10-25' },
        { tipo: 'otro', referencia: 'x' },
        'basura',
      ],
      advertencia: null,
    })
    expect(interpretarEventoChat({ evento: 'done', datos })).toEqual({
      tipo: 'done',
      sessionId: 'abc',
      trazaId: 't1',
      spanId: 's1',
      fuentes: [
        { tipo: 'sql', referencia: 'mediciones' },
        { tipo: 'prediccion', referencia: 'Predicción para el 2026-10-25' },
      ],
      advertencia: null,
    })
    expect(interpretarEventoChat({ evento: 'done', datos: '{}' })).toEqual({
      tipo: 'done',
      sessionId: null,
      trazaId: null,
      spanId: null,
      fuentes: [],
      advertencia: null,
    })
  })

  it('passthrough lleva el texto entero con sus metadatos', () => {
    const datos = JSON.stringify({ texto: 'Respuesta', fuentes: [], advertencia: 'Demo académica', traza_id: null })
    expect(interpretarEventoChat({ evento: 'passthrough', datos })).toEqual({
      tipo: 'passthrough',
      texto: 'Respuesta',
      fuentes: [],
      advertencia: 'Demo académica',
    })
  })

  it('error con detalle y sin él', () => {
    expect(interpretarEventoChat({ evento: 'error', datos: '{"detalle":"caído"}' })).toEqual({
      tipo: 'error',
      detalle: 'caído',
    })
    expect(interpretarEventoChat({ evento: 'error', datos: '{}' })).toEqual({ tipo: 'error', detalle: null })
  })

  it('eventos desconocidos o JSON inválido se ignoran (compatibilidad hacia delante)', () => {
    expect(interpretarEventoChat({ evento: 'novedad', datos: '{}' })).toBeNull()
    expect(interpretarEventoChat({ evento: 'token', datos: 'no es json' })).toBeNull()
    expect(interpretarEventoChat({ evento: 'token', datos: '{"texto":3}' })).toBeNull()
  })
})

describe('textoFase', () => {
  it('traduce las fases conocidas y prioriza la herramienta', () => {
    expect(textoFase('clasificando')).toBe('Entendiendo la pregunta')
    expect(textoFase('buscando', 'consultar_datos')).toBe('Consultando las mediciones')
    expect(textoFase('buscando', 'buscar_evidencias')).toBe('Buscando en la documentación')
  })

  it('cae en un genérico con fases o herramientas nuevas', () => {
    expect(textoFase('fase_futura')).toBe('Procesando la pregunta')
    expect(textoFase('buscando', 'herramienta_futura')).toBe('Consultando las fuentes')
  })
})
