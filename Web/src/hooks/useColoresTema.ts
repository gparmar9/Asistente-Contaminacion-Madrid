/**
 * Lee los tokens de color del tema activo como valores hex, para pasárselos a
 * Recharts (que no siempre acepta `var(--x)` en todas sus props). Se vuelve a
 * leer al cambiar el tema.
 */
import { useMemo } from 'react'

import { useTema } from '../estado/hooks'

export interface ColoresTema {
  dato: string
  datoSuave: string
  apagado: string
  rejilla: string
  lineaBase: string
  superficie: string
  tinta1: string
  tinta2: string
  categoricos: [string, string, string]
  heat: string[]
  good: string
  warning: string
  critical: string
}

function leer(nombre: string): string {
  return getComputedStyle(document.documentElement).getPropertyValue(nombre).trim()
}

export function useColoresTema(): ColoresTema {
  const { tema } = useTema()
  return useMemo<ColoresTema>(
    () => {
      void tema // dependencia explícita: los valores cambian con el tema
      return {
        dato: leer('--dato'),
        datoSuave: leer('--dato-suave'),
        apagado: leer('--apagado'),
        rejilla: leer('--rejilla'),
        lineaBase: leer('--linea-base'),
        superficie: leer('--superficie'),
        tinta1: leer('--tinta-1'),
        tinta2: leer('--tinta-2'),
        categoricos: [leer('--cat-1'), leer('--cat-2'), leer('--cat-3')],
        heat: [1, 2, 3, 4, 5, 6, 7].map((i) => leer(`--heat-${i}`)),
        good: leer('--good'),
        warning: leer('--warning'),
        critical: leer('--critical'),
      }
    },
    [tema],
  )
}
