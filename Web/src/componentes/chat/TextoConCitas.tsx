import { useMemo } from 'react'

import type { FuenteChat } from '../../api/tipos'
import { idFuente, resolverCitas, trocearConCitas } from '../../dominio/citas'

interface Props {
  texto: string
  fuentes: readonly FuenteChat[]
  idRespuesta: string
}

/**
 * Texto de la respuesta con los marcadores `[Dn]` visibles y enlazados a la
 * fuente documental correspondiente. No se interpreta markdown: el texto se
 * muestra tal cual, respetando los saltos de línea.
 */
export function TextoConCitas({ texto, fuentes, idRespuesta }: Props) {
  const trozos = useMemo(() => trocearConCitas(texto), [texto])
  const citas = useMemo(() => resolverCitas(texto, fuentes), [texto, fuentes])

  return (
    <p className="respuesta-texto">
      {trozos.map((t, i) => {
        if (t.tipo === 'texto') return <span key={i}>{t.valor}</span>
        const cita = citas.get(t.n)
        const etiqueta = `[D${t.n}]`
        if (!cita || cita.indiceFuente < 0) {
          return (
            <span key={i} className="cita" title={cita?.titulo ?? 'Cita documental'}>
              {etiqueta}
            </span>
          )
        }
        const fuente = fuentes[cita.indiceFuente]
        return (
          <a
            key={i}
            className="cita"
            href={`#${idFuente(idRespuesta, cita.indiceFuente)}`}
            title={`Fuente documental: ${fuente.referencia}`}
          >
            {etiqueta}
          </a>
        )
      })}
    </p>
  )
}
