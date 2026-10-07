import { describe, expect, it } from 'vitest'

import type { FuenteChat } from '../api/tipos'
import { idFuente, resolverCitas, trocearConCitas } from './citas'

const fuentes: FuenteChat[] = [
  { tipo: 'sql', referencia: 'resumen_datos_ml' },
  { tipo: 'documento', referencia: 'Ozono troposférico (O3) y salud' },
  { tipo: 'documento', referencia: 'Recomendaciones de exposición por perfil de persona' },
]

const textoConEvidencias = [
  'El ozono irrita las vías respiratorias [D1][D2]. Los niños son más sensibles [D3].',
  '',
  'Evidencias citadas:',
  '[D1] Ozono troposférico (O3) y salud — Efectos en la salud (ozono_salud.md, fragmento ozono_salud:efectos:0)',
  '[D2] Ozono troposférico (O3) y salud — Grupos sensibles (ozono_salud.md, fragmento ozono_salud:grupos:0)',
  '[D3] Recomendaciones de exposición por perfil de persona — Niños (recomendaciones_por_perfil.md, fragmento x:1)',
].join('\n')

describe('trocearConCitas', () => {
  it('separa texto y marcadores [Dn] conservando el texto intacto', () => {
    const trozos = trocearConCitas('Hola [D1] y [D12] adiós')
    expect(trozos).toEqual([
      { tipo: 'texto', valor: 'Hola ' },
      { tipo: 'cita', n: 1 },
      { tipo: 'texto', valor: ' y ' },
      { tipo: 'cita', n: 12 },
      { tipo: 'texto', valor: ' adiós' },
    ])
  })

  it('no confunde otros corchetes con citas', () => {
    expect(trocearConCitas('[nota] [S1] [d1]')).toEqual([{ tipo: 'texto', valor: '[nota] [S1] [d1]' }])
  })

  it('devuelve un único trozo de texto si no hay citas', () => {
    expect(trocearConCitas('sin citas')).toEqual([{ tipo: 'texto', valor: 'sin citas' }])
  })
})

describe('resolverCitas', () => {
  it('asocia cada Dn a la fuente documental por el título de la sección "Evidencias citadas"', () => {
    const citas = resolverCitas(textoConEvidencias, fuentes)
    expect(citas.get(1)?.indiceFuente).toBe(1)
    expect(citas.get(2)?.indiceFuente).toBe(1) // dos evidencias del mismo documento
    expect(citas.get(3)?.indiceFuente).toBe(2)
    expect(citas.get(1)?.titulo).toBe('Ozono troposférico (O3) y salud')
  })

  it('sin la sección de evidencias usa el orden de las fuentes documentales (saltando las SQL)', () => {
    const citas = resolverCitas('Afirmación [D1] y otra [D2].', fuentes)
    expect(citas.get(1)?.indiceFuente).toBe(1)
    expect(citas.get(2)?.indiceFuente).toBe(2)
  })

  it('deja la cita sin fuente (-1) si no hay documentos con los que casarla', () => {
    const citas = resolverCitas('Dato [D1].', [{ tipo: 'sql', referencia: 'resumen_datos_ml' }])
    expect(citas.get(1)?.indiceFuente).toBe(-1)
  })
})

describe('idFuente', () => {
  it('genera un ancla estable por respuesta e índice', () => {
    expect(idFuente('abc', 2)).toBe('fuente-abc-2')
  })
})
