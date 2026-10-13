import type { ReactNode } from 'react'

interface Props {
  id: string
  etiqueta: string
  children: ReactNode
}

/** Etiqueta + control, para la fila de filtros. */
export function Campo({ id, etiqueta, children }: Props) {
  return (
    <div className="campo">
      <label className="campo-etiqueta" htmlFor={id}>
        {etiqueta}
      </label>
      {children}
    </div>
  )
}
