import { useId, useState, type KeyboardEvent } from 'react'

import { MAX_CARACTERES_PREGUNTA } from '../../dominio/constantes'

interface Props {
  /** Hay una petición en vuelo: se bloquea el envío (evita doble gasto de LLM). */
  enviando: boolean
  onEnviar: (pregunta: string) => void
  /** Versión reducida para el panel flotante. */
  compacta?: boolean
  autoFocus?: boolean
}

export function EntradaChat({ enviando, onEnviar, compacta = false, autoFocus = false }: Props) {
  const [texto, setTexto] = useState('')
  const id = useId()
  const limpio = texto.trim()
  const puedeEnviar = !enviando && limpio.length > 0 && limpio.length <= MAX_CARACTERES_PREGUNTA

  function enviar() {
    if (!puedeEnviar) return
    onEnviar(limpio)
    setTexto('')
  }

  function alPulsar(e: KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      enviar()
    }
  }

  return (
    <form
      className={`entrada-chat ${compacta ? 'entrada-chat-compacta' : ''}`}
      onSubmit={(e) => {
        e.preventDefault()
        enviar()
      }}
    >
      <label htmlFor={id} className={compacta ? 'visualmente-oculto' : 'campo-etiqueta'}>
        Tu pregunta (independiente de las anteriores)
      </label>
      <textarea
        id={id}
        value={texto}
        onChange={(e) => setTexto(e.target.value)}
        onKeyDown={alPulsar}
        maxLength={MAX_CARACTERES_PREGUNTA}
        disabled={enviando}
        // El panel flotante se abre a petición de la persona: el foco debe ir directo al campo.
        autoFocus={autoFocus}
        placeholder={
          compacta
            ? 'Pregunta con estación, contaminante o fecha…'
            : 'Escribe una pregunta completa, con la estación o el contaminante que te interese…'
        }
        aria-describedby={`${id}-ayuda`}
      />
      <div className="fila-entre">
        <span id={`${id}-ayuda`} className="t-xs texto-apagado">
          {compacta ? 'Intro envía' : 'Intro envía · Mayús+Intro salto de línea'} ·{' '}
          <span className="tabular">
            {texto.length}/{MAX_CARACTERES_PREGUNTA}
          </span>
        </span>
        <button type="submit" className={`boton boton-primario ${compacta ? 'boton-s' : ''}`} disabled={!puedeEnviar}>
          {enviando ? 'Esperando…' : 'Preguntar'}
        </button>
      </div>
    </form>
  )
}
