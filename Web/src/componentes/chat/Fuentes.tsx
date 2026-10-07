import { useMemo } from 'react'

import type { FuenteChat } from '../../api/tipos'
import { idFuente, resolverCitas } from '../../dominio/citas'

interface Props {
  fuentes: readonly FuenteChat[]
  texto: string
  idRespuesta: string
}

function IconoBaseDatos() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
      <ellipse cx="12" cy="5" rx="8" ry="3" />
      <path d="M4 5v14c0 1.7 3.6 3 8 3s8-1.3 8-3V5" />
      <path d="M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3" />
    </svg>
  )
}

function IconoDocumento() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
      <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
      <path d="M14 2v6h6M8 13h8M8 17h8" />
    </svg>
  )
}

/**
 * Fuentes de la respuesta separadas por tipo. Una cifra de la base de datos
 * (`sql`) y una afirmación documental (`documento`) no son lo mismo: se
 * presentan en grupos distintos y con estilo distinto.
 */
export function Fuentes({ fuentes, texto, idRespuesta }: Props) {
  const citas = useMemo(() => resolverCitas(texto, fuentes), [texto, fuentes])
  const marcadoresPorFuente = useMemo(() => {
    const mapa = new Map<number, string[]>()
    for (const c of citas.values()) {
      if (c.indiceFuente < 0) continue
      mapa.set(c.indiceFuente, [...(mapa.get(c.indiceFuente) ?? []), `D${c.n}`])
    }
    return mapa
  }, [citas])

  if (fuentes.length === 0) return null

  const sql = fuentes.map((f, i) => ({ f, i })).filter(({ f }) => f.tipo === 'sql')
  const documentos = fuentes.map((f, i) => ({ f, i })).filter(({ f }) => f.tipo === 'documento')

  return (
    <div className="fuentes" aria-label="Fuentes de la respuesta">
      {sql.length > 0 && (
        <div>
          <div className="fuentes-grupo-titulo">
            <IconoBaseDatos /> Datos consultados en la base de datos
          </div>
          <ul>
            {sql.map(({ f, i }) => (
              <li key={i} id={idFuente(idRespuesta, i)} className="fuente fuente-sql">
                <span className="fuente-id" aria-hidden="true">
                  SQL
                </span>
                <span>
                  tabla <code>{f.referencia}</code>
                </span>
              </li>
            ))}
          </ul>
        </div>
      )}
      {documentos.length > 0 && (
        <div>
          <div className="fuentes-grupo-titulo">
            <IconoDocumento /> Documentos del corpus citados
          </div>
          <ul>
            {documentos.map(({ f, i }) => (
              <li key={i} id={idFuente(idRespuesta, i)} className="fuente fuente-documento">
                <span className="fuente-id">{(marcadoresPorFuente.get(i) ?? []).join(', ') || '·'}</span>
                <span>{f.referencia}</span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  )
}
