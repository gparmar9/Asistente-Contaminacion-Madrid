import { describe, expect, it } from 'vitest'

import {
  cubosHistograma,
  etiquetaResultado,
  etiquetaRuta,
  filasSatisfaccion,
  formatearMs,
  formatearPorcentaje,
  formatearUsd,
  indicadorSatisfaccion,
  participacion,
  posicionEnEje,
  proporcion,
  SIN_VALORACIONES,
} from './kpis'

const NBSP = ' '

describe('proporcion y porcentaje', () => {
  it('sin denominador es null y se muestra como guion, no como 0 %', () => {
    expect(proporcion(0, 0)).toBeNull()
    expect(formatearPorcentaje(proporcion(0, 0))).toBe('—')
  })

  it('redondea al entero', () => {
    expect(formatearPorcentaje(proporcion(5, 6))).toBe(`83${NBSP}%`)
    expect(formatearPorcentaje(proporcion(0, 4))).toBe(`0${NBSP}%`)
  })
})

describe('formato de tiempos y costes', () => {
  it('ms por debajo de un segundo y segundos por encima', () => {
    expect(formatearMs(850)).toBe(`850${NBSP}ms`)
    expect(formatearMs(3240)).toBe(`3,2${NBSP}s`)
    expect(formatearMs(null)).toBe('—')
  })

  it('el coste con dos cifras significativas', () => {
    expect(formatearUsd(0.003141)).toBe(`0,0031${NBSP}$`)
    expect(formatearUsd(1.25)).toBe(`1,3${NBSP}$`)
    expect(formatearUsd(undefined)).toBe('—')
  })
})

describe('etiquetas', () => {
  it('traduce las conocidas y deja las desconocidas tal cual', () => {
    expect(etiquetaRuta('documental')).toBe('Documental (RAG)')
    expect(etiquetaRuta('nueva')).toBe('nueva')
    expect(etiquetaResultado('servicio_no_disponible')).toBe('Servicio no disponible')
  })
})

describe('cubosHistograma', () => {
  it('sin valores no hay cubos', () => {
    expect(cubosHistograma([])).toEqual([])
  })

  it('con un único valor distinto hay un solo cubo con todos', () => {
    expect(cubosHistograma([0.002, 0.002, 0.002])).toEqual([{ desde: 0.002, hasta: 0.002, n: 3 }])
  })

  it('reparte entre el mínimo y el máximo y el máximo cae en el último cubo', () => {
    const cubos = cubosHistograma([0, 1, 2, 3, 4], 4)
    expect(cubos.map((c) => c.n)).toEqual([1, 1, 1, 2])
    expect(cubos[0].desde).toBe(0)
    expect(cubos[3].hasta).toBe(4)
    expect(cubos.reduce((s, c) => s + c.n, 0)).toBe(5)
  })

  it('las marcas se sitúan sobre el eje y se recortan a sus límites', () => {
    const cubos = cubosHistograma([0, 10], 5)
    expect(posicionEnEje(5, cubos)).toBe(0.5)
    expect(posicionEnEje(20, cubos)).toBe(1)
    expect(posicionEnEje(3, cubosHistograma([3]))).toBe(0.5)
  })
})

describe('filasSatisfaccion', () => {
  it('calcula la tasa de cada grupo y conserva el orden y el n', () => {
    const filas = filasSatisfaccion([
      { grupo: 'documental', positivos: 3, votos: 4 },
      { grupo: 'datos', positivos: 0, votos: 1 },
    ])
    expect(filas).toEqual([
      { grupo: 'documental', positivos: 3, votos: 4, tasa: 0.75 },
      { grupo: 'datos', positivos: 0, votos: 1, tasa: 0 },
    ])
  })
})

describe('satisfacción', () => {
  const satisfaccion = {
    votos: 6,
    positivos: 5,
    por_ruta: [],
    por_tema: [],
    sesiones_elegibles: 8,
    sesiones_con_voto: 6,
  }

  it('sin valoraciones en el periodo no muestra 0 %', () => {
    expect(indicadorSatisfaccion(null)).toEqual({ valor: '—', nota: SIN_VALORACIONES })
  })

  it('con votos muestra el % de positivos y su n', () => {
    expect(indicadorSatisfaccion(satisfaccion)).toEqual({ valor: `83${NBSP}%`, nota: '6 votos' })
    expect(indicadorSatisfaccion({ ...satisfaccion, votos: 1, positivos: 1 }).nota).toBe('1 voto')
  })

  it('la participación es sesiones con voto entre sesiones que vieron el aviso', () => {
    expect(participacion(satisfaccion)).toBe(0.75)
    expect(participacion({ ...satisfaccion, sesiones_elegibles: 0, sesiones_con_voto: 0 })).toBeNull()
  })
})
