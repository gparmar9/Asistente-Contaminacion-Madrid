import { describe, expect, it } from 'vitest'

import { aISO, desdeISO, diasEntre, esFechaISOValida, rangoUltimosDias, sumarDias } from './fechas'

describe('fechas ISO sin zona horaria', () => {
  it('desdeISO/aISO son inversas y no desplazan el día', () => {
    expect(aISO(desdeISO('2026-10-03'))).toBe('2026-10-03')
    expect(aISO(desdeISO('2026-01-01'))).toBe('2026-01-01')
  })

  it('sumarDias cruza meses y años', () => {
    expect(sumarDias('2026-09-30', 1)).toBe('2026-10-01')
    expect(sumarDias('2026-01-01', -1)).toBe('2025-12-31')
    expect(sumarDias('2024-02-28', 1)).toBe('2024-02-29')
  })

  it('rangoUltimosDias(7) incluye el día final (como el valor por defecto de la API)', () => {
    expect(rangoUltimosDias(7, '2026-10-03')).toEqual({ desde: '2026-09-27', hasta: '2026-10-03' })
    expect(rangoUltimosDias(1, '2026-10-03')).toEqual({ desde: '2026-10-03', hasta: '2026-10-03' })
  })

  it('diasEntre devuelve los días consecutivos, ambos incluidos', () => {
    expect(diasEntre('2026-09-29', '2026-10-02')).toEqual(['2026-09-29', '2026-09-30', '2026-10-01', '2026-10-02'])
    expect(diasEntre('2026-10-02', '2026-10-01')).toEqual([])
  })

  it('esFechaISOValida rechaza formatos y fechas inexistentes', () => {
    expect(esFechaISOValida('2026-10-03')).toBe(true)
    expect(esFechaISOValida('2026-02-30')).toBe(false)
    expect(esFechaISOValida('03/10/2026')).toBe(false)
    expect(esFechaISOValida('')).toBe(false)
  })
})
