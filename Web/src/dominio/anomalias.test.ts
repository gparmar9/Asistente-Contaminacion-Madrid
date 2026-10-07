import { describe, expect, it } from 'vitest'

import type { PuntoSerie } from '../api/tipos'
import { esAnomaliaFiable, estadoPunto, resumirAnomalias } from './anomalias'

const base: PuntoSerie = { fecha: '2026-10-02', bloque: 'manana', media: 40, cobertura: 1, is_anomaly: false }

describe('estadoPunto: is_anomaly solo se interpreta con cobertura >= 0,7', () => {
  it('sin media es "sin_dato", aunque el detector haya marcado el bloque', () => {
    expect(estadoPunto({ ...base, media: null, is_anomaly: true })).toBe('sin_dato')
    expect(estadoPunto({ ...base, media: undefined })).toBe('sin_dato')
  })

  it('con cobertura < 0,7 es "baja_confianza", nunca anomalía ni normal', () => {
    expect(estadoPunto({ ...base, cobertura: 0.69, is_anomaly: true })).toBe('baja_confianza')
    expect(estadoPunto({ ...base, cobertura: 0.5, is_anomaly: false })).toBe('baja_confianza')
    expect(estadoPunto({ ...base, cobertura: 0, is_anomaly: true })).toBe('baja_confianza')
  })

  it('con cobertura nula es "baja_confianza"', () => {
    expect(estadoPunto({ ...base, cobertura: null, is_anomaly: true })).toBe('baja_confianza')
  })

  it('la cobertura 0,7 exacta ya es evaluable', () => {
    expect(estadoPunto({ ...base, cobertura: 0.7, is_anomaly: true })).toBe('anomalia')
    expect(estadoPunto({ ...base, cobertura: 0.7, is_anomaly: false })).toBe('normal')
  })

  it('el bloque de noche (cobertura 0,75 por la hora 23 ausente) es evaluable', () => {
    expect(estadoPunto({ ...base, bloque: 'noche', cobertura: 0.75, is_anomaly: true })).toBe('anomalia')
  })

  it('con cobertura suficiente pero sin decisión del detector es "no_evaluado"', () => {
    expect(estadoPunto({ ...base, is_anomaly: null })).toBe('no_evaluado')
    expect(estadoPunto({ ...base, is_anomaly: undefined })).toBe('no_evaluado')
  })

  it('un cero medido es un dato, no un hueco', () => {
    expect(estadoPunto({ ...base, media: 0 })).toBe('normal')
  })
})

describe('esAnomaliaFiable', () => {
  it('exige marca del detector Y cobertura suficiente', () => {
    expect(esAnomaliaFiable({ ...base, is_anomaly: true })).toBe(true)
    expect(esAnomaliaFiable({ ...base, is_anomaly: true, cobertura: 0.3 })).toBe(false)
    expect(esAnomaliaFiable({ ...base, is_anomaly: false })).toBe(false)
  })
})

describe('resumirAnomalias', () => {
  it('cuenta las anomalías fiables aparte de las marcadas con cobertura baja', () => {
    const puntos: PuntoSerie[] = [
      { ...base, bloque: 'madrugada', is_anomaly: true }, // fiable
      { ...base, bloque: 'manana', is_anomaly: true, cobertura: 0.4 }, // marcada, no fiable
      { ...base, bloque: 'tarde', is_anomaly: false, cobertura: 0.5 }, // baja confianza
      { ...base, bloque: 'noche', is_anomaly: false, cobertura: 0.75 }, // normal
      { ...base, fecha: '2026-10-01', bloque: 'noche', media: null }, // sin dato
    ]
    expect(resumirAnomalias(puntos)).toEqual({
      total: 5,
      conDato: 4,
      evaluables: 2,
      anomaliasFiables: 1,
      bajaConfianza: 2,
      marcadosNoFiables: 1,
    })
  })

  it('con una lista vacía todo es cero', () => {
    expect(resumirAnomalias([]).anomaliasFiables).toBe(0)
    expect(resumirAnomalias([]).total).toBe(0)
  })
})
