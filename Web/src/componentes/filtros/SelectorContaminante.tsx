import type { Contaminante } from '../../api/tipos'
import { NOMBRE_CONTAMINANTE } from '../../dominio/constantes'
import { contaminantesDisponibles } from '../../dominio/contaminantes'
import { Campo } from '../ui/Campo'

interface Props {
  id?: string
  /** `contaminantes_medidos` de la estación: no todas miden los 6. */
  medidos: readonly string[]
  valor: Contaminante | undefined
  onChange: (contaminante: Contaminante) => void
}

export function SelectorContaminante({ id = 'contaminante', medidos, valor, onChange }: Props) {
  const disponibles = contaminantesDisponibles(medidos)
  return (
    <Campo id={id} etiqueta="Contaminante">
      <select
        id={id}
        className="selector"
        value={valor ?? ''}
        onChange={(e) => onChange(e.target.value as Contaminante)}
        disabled={disponibles.length === 0}
      >
        {disponibles.length === 0 && <option value="">Esta estación no mide ninguno de los 6</option>}
        {disponibles.map((c) => (
          <option key={c} value={c}>
            {c} · {NOMBRE_CONTAMINANTE[c]}
          </option>
        ))}
      </select>
    </Campo>
  )
}
