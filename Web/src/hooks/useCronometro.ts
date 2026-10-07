/**
 * Segundos transcurridos mientras `activo` es true (contador de espera del
 * asistente: una respuesta puede tardar hasta ~120 s).
 */
import { useEffect, useState } from 'react'

export function useCronometro(activo: boolean): number {
  const [segundos, setSegundos] = useState(0)

  useEffect(() => {
    if (!activo) return
    const inicio = Date.now()
    const id = window.setInterval(() => setSegundos(Math.floor((Date.now() - inicio) / 1000)), 250)
    return () => {
      window.clearInterval(id)
      // Al terminar la espera se reinicia para la siguiente pregunta.
      setSegundos(0)
    }
  }, [activo])

  return segundos
}
