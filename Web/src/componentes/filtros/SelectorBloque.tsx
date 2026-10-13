import type { Bloque } from '../../api/tipos'
import { BLOQUES, ETIQUETA_BLOQUE, HORAS_BLOQUE } from '../../dominio/constantes'
import { Campo } from '../ui/Campo'

interface Props {
  id?: string
  /** `undefined` = todos los bloques. */
  valor: Bloque | undefined
  onChange: (bloque: Bloque | undefined) => void
}

export function SelectorBloque({ id = 'bloque', valor, onChange }: Props) {
  return (
    <Campo id={id} etiqueta="Bloque">
      <select
        id={id}
        className="selector"
        value={valor ?? ''}
        onChange={(e) => onChange(e.target.value === '' ? undefined : (e.target.value as Bloque))}
      >
        <option value="">Todos los bloques</option>
        {BLOQUES.map((b) => (
          <option key={b} value={b}>
            {ETIQUETA_BLOQUE[b]} · {HORAS_BLOQUE[b]}
          </option>
        ))}
      </select>
    </Campo>
  )
}
