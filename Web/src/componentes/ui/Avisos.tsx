import type { ReactNode } from 'react'

import { mensajeErrorDatos, type MensajeError } from '../../api/errores'

export function Cargando({ texto = 'Cargando…' }: { texto?: string }) {
  return (
    <p className="cargando" role="status">
      {texto}
    </p>
  )
}

export function Vacio({ children }: { children: ReactNode }) {
  return <p className="aviso-bloque">{children}</p>
}

interface PropsError {
  error: unknown
  /** Traductor de error a mensaje; por defecto el de las consultas de datos. */
  traducir?: (error: unknown) => MensajeError
  reintentar?: () => void
}

export function ErrorBloque({ error, traducir = mensajeErrorDatos, reintentar }: PropsError) {
  const m = traducir(error)
  return (
    <div className="aviso-bloque aviso-error" role="alert">
      <strong>{m.titulo}.</strong> {m.detalle}
      {reintentar && (
        <>
          {' '}
          <button type="button" className="boton boton-s" onClick={reintentar} style={{ marginLeft: 8 }}>
            Reintentar
          </button>
        </>
      )}
    </div>
  )
}
