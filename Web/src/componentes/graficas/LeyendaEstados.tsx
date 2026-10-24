import type { CSSProperties } from 'react'

import { ESTADOS, type EstadoPunto } from '../../dominio/anomalias'

interface Props {
  estados?: readonly EstadoPunto[]
  /** Texto adicional al principio (p. ej. qué significa la línea). */
  prefijo?: string
}

/** Leyenda de marcadores de estado: color siempre con icono y texto. */
export function LeyendaEstados({ estados = ['anomalia', 'baja_confianza'], prefijo }: Props) {
  return (
    <div className="leyenda" aria-label="Leyenda de estados">
      {prefijo && <span>{prefijo}</span>}
      {estados.map((e) => {
        const d = ESTADOS[e]
        const estilo = { '--color-estado': `var(--${d.color})` } as CSSProperties
        return (
          <span key={e} className="leyenda-item estado estado-suave" style={estilo} title={d.descripcion}>
            <span className="estado-punto" aria-hidden="true" />
            <span className="estado-icono" aria-hidden="true">
              {d.icono}
            </span>
            {d.etiqueta}
          </span>
        )
      })}
    </div>
  )
}
