import { useMemo } from 'react'

import type { Estacion } from '../../api/tipos'
import { Campo } from '../ui/Campo'

interface Props {
  id?: string
  estaciones: readonly Estacion[]
  valor: number | undefined
  onChange: (codigo: number) => void
  /** Deja fuera estas estaciones (p. ej. la que ya se está viendo al comparar). */
  excluir?: readonly number[]
  etiqueta?: string
  placeholder?: string
}

export function SelectorEstacion({
  id = 'estacion',
  estaciones,
  valor,
  onChange,
  excluir = [],
  etiqueta = 'Estación',
  placeholder,
}: Props) {
  const ordenadas = useMemo(
    () =>
      estaciones
        .filter((e) => !excluir.includes(e.codigo_corto))
        .slice()
        .sort((a, b) => a.nombre.localeCompare(b.nombre, 'es')),
    [estaciones, excluir],
  )

  return (
    <Campo id={id} etiqueta={etiqueta}>
      <select
        id={id}
        className="selector"
        value={valor ?? ''}
        onChange={(e) => {
          const codigo = Number(e.target.value)
          if (Number.isInteger(codigo)) onChange(codigo)
        }}
      >
        {(valor === undefined || placeholder) && (
          <option value="" disabled={!placeholder}>
            {placeholder ?? 'Elige una estación'}
          </option>
        )}
        {ordenadas.map((e) => (
          <option key={e.codigo_corto} value={e.codigo_corto}>
            {e.nombre}
            {e.distrito ? ` · ${e.distrito}` : ''}
          </option>
        ))}
      </select>
    </Campo>
  )
}
