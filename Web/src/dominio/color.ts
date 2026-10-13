/** Utilidades de color para el heatmap: elegir tinta legible sobre una celda coloreada. */

function hexARgb(hex: string): [number, number, number] | null {
  const limpio = hex.trim().replace('#', '')
  if (limpio.length !== 6) return null
  const n = Number.parseInt(limpio, 16)
  if (Number.isNaN(n)) return null
  return [(n >> 16) & 255, (n >> 8) & 255, n & 255]
}

/** Luminancia relativa (WCAG) de un color hex; 0 = negro, 1 = blanco. */
export function luminancia(hex: string): number {
  const rgb = hexARgb(hex)
  if (!rgb) return 0.5
  const [r, g, b] = rgb.map((c) => {
    const s = c / 255
    return s <= 0.03928 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4
  })
  return 0.2126 * r + 0.7152 * g + 0.0722 * b
}

/** Blanco o tinta oscura según el fondo, para un glifo dentro de una celda coloreada. */
export function tintaSobre(hex: string): string {
  return luminancia(hex) > 0.4 ? '#0b0b0b' : '#ffffff'
}

/** Índice 0..(pasos-1) de un valor en una rampa lineal entre min y max. */
export function pasoRampa(valor: number, min: number, max: number, pasos: number): number {
  if (!(max > min)) return Math.floor(pasos / 2)
  const t = (valor - min) / (max - min)
  return Math.max(0, Math.min(pasos - 1, Math.floor(t * pasos)))
}
