import type { CSSProperties } from 'react'

import { ESTADOS, type EstadoPunto } from '../../dominio/anomalias'

interface Props {
  estado: EstadoPunto
  /** Texto más discreto (en tablas densas). */
  suave?: boolean
  /** Píldora con fondo tintado (tarjetas y banda de Hoy). */
  pildora?: boolean
  /** Muestra la explicación del estado como título accesible. */
  conDescripcion?: boolean
}

/** Estado de anomalía: color + icono + texto. El color nunca va solo. */
export function Estado({ estado, suave, pildora, conDescripcion = true }: Props) {
  const d = ESTADOS[estado]
  const estilo = { '--color-estado': `var(--${d.color})` } as CSSProperties
  const clases = ['estado', suave ? 'estado-suave' : '', pildora ? 'estado-pildora' : ''].join(' ').trim()
  return (
    <span className={clases} style={estilo} title={conDescripcion ? d.descripcion : undefined}>
      <span className="estado-punto" aria-hidden="true" />
      <span className="estado-icono" aria-hidden="true">
        {d.icono}
      </span>
      {d.etiqueta}
    </span>
  )
}
