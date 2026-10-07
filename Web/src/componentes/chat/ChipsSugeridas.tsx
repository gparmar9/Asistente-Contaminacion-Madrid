import { PREGUNTAS_SUGERIDAS } from '../../dominio/constantes'

interface Props {
  deshabilitado: boolean
  onElegir: (pregunta: string) => void
}

/** Preguntas de ejemplo: un clic las envía tal cual. */
export function ChipsSugeridas({ deshabilitado, onElegir }: Props) {
  return (
    <div className="chips" aria-label="Preguntas sugeridas">
      {PREGUNTAS_SUGERIDAS.map((p) => (
        <button key={p} type="button" className="chip chip-boton" disabled={deshabilitado} onClick={() => onElegir(p)}>
          {p}
        </button>
      ))}
    </div>
  )
}
