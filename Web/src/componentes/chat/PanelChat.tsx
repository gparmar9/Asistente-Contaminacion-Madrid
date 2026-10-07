import { useEffect, useId, useRef } from 'react'
import { Link, useLocation } from 'react-router-dom'

import { useChat } from '../../estado/hooks'
import { ChipsSugeridas } from './ChipsSugeridas'
import { Consulta } from './Consulta'
import { EntradaChat } from './EntradaChat'

/**
 * Burbuja fija en todas las pantallas que despliega el asistente en un panel.
 * Comparte estado con la página /asistente (misma lista de consultas), donde
 * la burbuja se oculta para no duplicar el campo de pregunta.
 */
export function PanelChat() {
  const { consultas, enviando, enviar, vaciar, abierto, cerrar, alternar, hayNovedades } = useChat()
  const location = useLocation()
  const burbujaRef = useRef<HTMLButtonElement>(null)
  const finalRef = useRef<HTMLDivElement>(null)
  const idPanel = useId()

  // Al abrir o al añadir una consulta, llevar la vista hasta la última.
  useEffect(() => {
    if (abierto) finalRef.current?.scrollIntoView({ block: 'end' })
  }, [abierto, consultas.length])

  // Escape cierra el panel y devuelve el foco a la burbuja.
  useEffect(() => {
    if (!abierto) return
    const alPulsar = (e: globalThis.KeyboardEvent) => {
      if (e.key === 'Escape') {
        cerrar()
        burbujaRef.current?.focus()
      }
    }
    window.addEventListener('keydown', alPulsar)
    return () => window.removeEventListener('keydown', alPulsar)
  }, [abierto, cerrar])

  if (location.pathname === '/asistente') return null

  function cerrarYEnfocar() {
    cerrar()
    burbujaRef.current?.focus()
  }

  return (
    <>
      {abierto && (
        <section className="panel-chat" role="dialog" aria-label="Asistente" id={idPanel}>
          <header className="panel-chat-cabecera">
            <div>
              <strong>Asistente</strong>
            </div>
            <div className="fila" style={{ gap: 6, flexWrap: 'nowrap' }}>
              <Link to="/asistente" className="boton boton-s" onClick={cerrar} title="Abrir el asistente a pantalla completa">
                Ampliar
              </Link>
              <button type="button" className="boton boton-suave boton-icono boton-s" onClick={cerrarYEnfocar} aria-label="Cerrar el asistente">
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
                  <path d="M6 6l12 12M18 6L6 18" />
                </svg>
              </button>
            </div>
          </header>

          <div className="panel-chat-cuerpo">
            {consultas.length === 0 ? (
              <div className="apilado-s">
                <p className="t-s texto-2">
                  Pregunta por los datos de una estación o por salud y normativa. Incluye la estación, el contaminante o la
                  fecha en cada pregunta.
                </p>
                <ChipsSugeridas deshabilitado={enviando} onElegir={(p) => enviar(p)} />
              </div>
            ) : (
              <div className="consultas" aria-live="polite">
                {consultas.map((c) => (
                  <Consulta key={c.id} consulta={c} onReintentar={(con) => enviar(con.pregunta, con.id)} />
                ))}
                <div ref={finalRef} />
              </div>
            )}
          </div>

          <footer className="panel-chat-pie">
            <EntradaChat enviando={enviando} onEnviar={(p) => enviar(p)} compacta autoFocus />
            {consultas.length > 0 && (
              <div className="fila-entre t-xs texto-apagado">
                <span>
                  {consultas.length} {consultas.length === 1 ? 'consulta' : 'consultas'} · se pierden al recargar
                </span>
                <button type="button" className="boton boton-suave boton-s" onClick={vaciar} disabled={enviando}>
                  Vaciar
                </button>
              </div>
            )}
          </footer>
        </section>
      )}

      <button
        ref={burbujaRef}
        type="button"
        className="burbuja-chat"
        onClick={alternar}
        aria-expanded={abierto}
        aria-controls={abierto ? idPanel : undefined}
        aria-label={abierto ? 'Cerrar el asistente' : 'Abrir el asistente'}
        title={abierto ? 'Cerrar el asistente' : 'Preguntar al asistente'}
      >
        {abierto ? (
          <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" aria-hidden="true">
            <path d="M6 6l12 12M18 6L6 18" />
          </svg>
        ) : (
          <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
            <path d="M21 12a8 8 0 0 1-8 8H8l-5 3 1.2-4.6A8 8 0 1 1 21 12z" />
            <path d="M8 11h8M8 14.5h5" />
          </svg>
        )}
        {hayNovedades && !abierto && (
          <span className="burbuja-novedades">
            <span className="visualmente-oculto">Hay una respuesta nueva</span>
          </span>
        )}
        {enviando && !abierto && <span className="burbuja-espera" aria-hidden="true" />}
      </button>
    </>
  )
}
