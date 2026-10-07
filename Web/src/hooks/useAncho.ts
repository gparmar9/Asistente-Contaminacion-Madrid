/** Ancho en píxeles de un contenedor, actualizado con ResizeObserver (para dimensionar SVG y celdas). */
import { useEffect, useState, type RefObject } from 'react'

export function useAncho<T extends HTMLElement>(ref: RefObject<T | null>, inicial = 0): number {
  const [ancho, setAncho] = useState(inicial)

  useEffect(() => {
    const elemento = ref.current
    if (!elemento) return
    const observador = new ResizeObserver((entradas) => {
      const medida = entradas[0]?.contentRect.width
      if (medida !== undefined) setAncho(Math.floor(medida))
    })
    observador.observe(elemento)
    return () => observador.disconnect()
  }, [ref])

  return ancho
}
