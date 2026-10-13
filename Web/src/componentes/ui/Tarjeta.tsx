import type { ReactNode } from 'react'

interface Props {
  titulo?: ReactNode
  sub?: ReactNode
  acciones?: ReactNode
  /** Mientras se recargan datos la tarjeta mantiene su contenido atenuado (sin esqueletos). */
  atenuada?: boolean
  className?: string
  children: ReactNode
}

export function Tarjeta({ titulo, sub, acciones, atenuada, className, children }: Props) {
  return (
    <section className={`tarjeta ${atenuada ? 'atenuado' : ''} ${className ?? ''}`.trim()}>
      {(titulo || acciones) && (
        <header className="tarjeta-cabecera">
          <div>
            {titulo && <h2 className="tarjeta-titulo">{titulo}</h2>}
            {sub && <p className="tarjeta-sub">{sub}</p>}
          </div>
          {acciones}
        </header>
      )}
      {children}
    </section>
  )
}
