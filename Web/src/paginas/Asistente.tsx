import { useEffect, useRef } from 'react'

import { ChipsSugeridas } from '../componentes/chat/ChipsSugeridas'
import { Consulta } from '../componentes/chat/Consulta'
import { EntradaChat } from '../componentes/chat/EntradaChat'
import { useChat } from '../estado/hooks'

/**
 * Asistente a pantalla completa. Comparte estado con el panel flotante
 * (misma lista de consultas de la sesión).
 *
 * Cada pregunta es INDEPENDIENTE: el backend no guarda historial ni contexto,
 * así que las preguntas de seguimiento ("¿y ayer?") no funcionan. La lista
 * vive solo en el estado de React y desaparece al recargar.
 */
export default function Asistente() {
  const { consultas, enviando, enviar, vaciar, marcarVisto } = useChat()
  const finalRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    // En esta pantalla las respuestas se ven directamente: no cuentan como novedades del panel.
    marcarVisto()
    finalRef.current?.scrollIntoView({ block: 'nearest', behavior: 'smooth' })
  }, [consultas, marcarVisto])

  return (
    <div className="asistente">
      <header className="pagina-cabecera">
        <div className="apilado-s">
          <h1>Asistente</h1>
          <p>
            Pregunta en lenguaje natural sobre los datos de las estaciones o sobre salud y normativa. El asistente
            consulta la base de datos y la documentación del proyecto y cita sus fuentes.
          </p>
        </div>
      </header>

      <div className="aviso-bloque" role="note">
        <strong>Cada pregunta se responde por separado.</strong> El asistente no recuerda las anteriores: incluye en
        cada pregunta la estación, el contaminante o la fecha que te interesen. Lo que ves aquí solo existe en esta
        pestaña y se pierde al recargar.
      </div>

      <section aria-labelledby="sugeridas">
        <h2 id="sugeridas" className="campo-etiqueta" style={{ marginBottom: 8 }}>
          Preguntas de ejemplo
        </h2>
        <ChipsSugeridas deshabilitado={enviando} onElegir={(p) => enviar(p)} />
      </section>

      {consultas.length > 0 && (
        <section className="consultas" aria-label="Consultas de esta sesión" aria-live="polite">
          {consultas.map((c) => (
            <Consulta key={c.id} consulta={c} onReintentar={(con) => enviar(con.pregunta, con.id)} />
          ))}
          <div ref={finalRef} />
        </section>
      )}

      <EntradaChat enviando={enviando} onEnviar={(p) => enviar(p)} />

      {consultas.length > 0 && (
        <div className="fila-entre t-xs texto-apagado">
          <span>
            {consultas.length} {consultas.length === 1 ? 'consulta' : 'consultas'} en esta sesión
          </span>
          <button type="button" className="boton boton-suave boton-s" onClick={vaciar} disabled={enviando}>
            Vaciar la lista
          </button>
        </div>
      )}
    </div>
  )
}
