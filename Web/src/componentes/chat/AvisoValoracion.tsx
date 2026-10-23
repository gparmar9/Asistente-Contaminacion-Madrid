import { useEffect, useState } from 'react'

import { valorar } from '../../api/consultas'
import type { ValorValoracion } from '../../api/tipos'
import { hoyISO } from '../../dominio/fechas'
import { turnoAValorar } from '../../dominio/valoracion'
import { useChat } from '../../estado/hooks'

const CLAVE_FECHA = 'jupiter.valoracion.fecha'
const DURACION_GRACIAS_MS = 2000

// Conserva la fecha si localStorage no está disponible.
let fechaEnMemoria: string | null = null

function respondidoHoy(): boolean {
  let fecha = fechaEnMemoria
  try {
    fecha = fecha ?? localStorage.getItem(CLAVE_FECHA)
  } catch {
    // Sin almacenamiento: vale la copia en memoria.
  }
  return fecha === hoyISO()
}

function recordarRespuesta(): void {
  fechaEnMemoria = hoyISO()
  try {
    localStorage.setItem(CLAVE_FECHA, fechaEnMemoria)
  } catch {
    // Sin almacenamiento: vale la copia en memoria.
  }
}

/** Votar o descartar cierra el aviso del día, aunque falle el envío. */
export function AvisoValoracion() {
  const { consultas, enviando } = useChat()
  const [respondido, setRespondido] = useState(respondidoHoy)
  const [gracias, setGracias] = useState(false)

  useEffect(() => {
    if (!gracias) return
    const temporizador = setTimeout(() => setGracias(false), DURACION_GRACIAS_MS)
    return () => clearTimeout(temporizador)
  }, [gracias])

  if (gracias) {
    return (
      <div className="aviso-valoracion" role="status">
        Gracias
      </div>
    )
  }

  const turno = turnoAValorar(consultas, enviando, respondido)
  if (turno === null) return null

  const responder = (valor: ValorValoracion | null) => {
    recordarRespuesta()
    setRespondido(true)
    if (valor === null) return
    setGracias(true)
    valorar({ session_id: turno.sessionId, traza_id: turno.trazaId, span_id: turno.spanId, valor }).catch(() => {
      // Un fallo al guardar el voto no interrumpe el chat.
    })
  }

  return (
    <div className="aviso-valoracion" role="group" aria-label="Valoración del asistente">
      <span>¿Qué tal lo está haciendo el asistente hoy?</span>
      <div className="fila" style={{ gap: 6, flexWrap: 'nowrap' }}>
        <button
          type="button"
          className="boton boton-suave boton-icono boton-s"
          onClick={() => responder('positiva')}
          aria-label="Bien"
          title="Bien"
        >
          <span aria-hidden="true">👍</span>
        </button>
        <button
          type="button"
          className="boton boton-suave boton-icono boton-s"
          onClick={() => responder('negativa')}
          aria-label="Mal"
          title="Mal"
        >
          <span aria-hidden="true">👎</span>
        </button>
        <button type="button" className="boton boton-suave boton-s" onClick={() => responder(null)}>
          Ahora no
        </button>
      </div>
    </div>
  )
}
