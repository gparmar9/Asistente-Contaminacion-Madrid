import { useMemo } from 'react'
import ReactMarkdown from 'react-markdown'
import remarkBreaks from 'remark-breaks'
import remarkGfm from 'remark-gfm'

import type { FuenteChat } from '../../api/tipos'
import { markdownConCitas } from '../../dominio/citas'

interface Props {
  texto: string
  fuentes: readonly FuenteChat[]
  idRespuesta: string
}

const PLUGINS = [remarkGfm, remarkBreaks]

/**
 * Texto de la respuesta interpretado como markdown (tablas, listas, negritas),
 * sin la sección interna de evidencias y con los marcadores `[Dn]` visibles y
 * enlazados a su fuente documental (ver `markdownConCitas`).
 */
export function TextoConCitas({ texto, fuentes, idRespuesta }: Props) {
  const markdown = useMemo(() => markdownConCitas(texto, fuentes, idRespuesta), [texto, fuentes, idRespuesta])
  return (
    <div className="respuesta-texto">
      <ReactMarkdown remarkPlugins={PLUGINS} components={{ a: Enlace }}>
        {markdown}
      </ReactMarkdown>
    </div>
  )
}

/** Las citas `[Dn]` enlazan a un ancla de la propia página; el resto, fuera y en otra pestaña. */
function Enlace(props: React.ComponentProps<'a'>) {
  const { href, title, children } = props
  if (href?.startsWith('#fuente-')) {
    return (
      <a className="cita" href={href} title={title}>
        {children}
      </a>
    )
  }
  return (
    <a href={href} title={title} target="_blank" rel="noopener noreferrer">
      {children}
    </a>
  )
}
