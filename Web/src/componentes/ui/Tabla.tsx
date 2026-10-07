import { useId, useMemo, useState, type ReactNode } from 'react'

export interface Columna<F> {
  clave: string
  titulo: ReactNode
  /** Alinea a la derecha y usa cifras tabulares. */
  numerica?: boolean
  render: (fila: F) => ReactNode
  /** Valor por el que ordenar; si falta, la columna no es ordenable. Los nulos van al final. */
  ordenar?: (fila: F) => string | number | null | undefined
}

export interface Orden {
  clave: string
  direccion: 'asc' | 'desc'
}

interface Props<F> {
  filas: readonly F[]
  columnas: readonly Columna<F>[]
  claveFila: (fila: F) => string | number
  ordenInicial?: Orden
  /** Nota descriptiva bajo la tabla (enlazada con aria-describedby). */
  caption?: ReactNode
  vacio?: ReactNode
  onClickFila?: (fila: F) => void
  /** Clase extra para la fila (p. ej. resaltar anomalías). */
  claseFila?: (fila: F) => string | undefined
}

function comparar(a: string | number | null | undefined, b: string | number | null | undefined): number {
  const aNulo = a == null || (typeof a === 'number' && Number.isNaN(a))
  const bNulo = b == null || (typeof b === 'number' && Number.isNaN(b))
  if (aNulo && bNulo) return 0
  if (aNulo) return 1
  if (bNulo) return -1
  if (typeof a === 'number' && typeof b === 'number') return a - b
  return String(a).localeCompare(String(b), 'es', { numeric: true, sensitivity: 'base' })
}

/** Tabla ordenable y accesible: `aria-sort` en la cabecera, nulos al final, sin dependencias. */
export function Tabla<F>({ filas, columnas, claveFila, ordenInicial, caption, vacio, onClickFila, claseFila }: Props<F>) {
  const [orden, setOrden] = useState<Orden | undefined>(ordenInicial)
  const idPie = useId()

  const ordenadas = useMemo(() => {
    if (!orden) return filas
    const columna = columnas.find((c) => c.clave === orden.clave)
    if (!columna?.ordenar) return filas
    const obtener = columna.ordenar
    const signo = orden.direccion === 'asc' ? 1 : -1
    return [...filas].sort((a, b) => {
      const r = comparar(obtener(a), obtener(b))
      // Los nulos siempre al final, independientemente de la dirección.
      if (obtener(a) == null || obtener(b) == null) return r
      return r * signo
    })
  }, [filas, columnas, orden])

  function alternarOrden(clave: string) {
    setOrden((actual) => {
      if (actual?.clave !== clave) return { clave, direccion: 'asc' }
      return { clave, direccion: actual.direccion === 'asc' ? 'desc' : 'asc' }
    })
  }

  return (
    <div>
      <div className="tabla-envoltorio">
        <table className="tabla" aria-describedby={caption ? idPie : undefined}>
          <thead>
            <tr>
              {columnas.map((c) => {
                const activa = orden?.clave === c.clave
                const ariaSort = activa ? (orden.direccion === 'asc' ? 'ascending' : 'descending') : undefined
                return (
                  <th key={c.clave} scope="col" className={c.numerica ? 'num' : undefined} aria-sort={ariaSort}>
                    {c.ordenar ? (
                      <button type="button" className="tabla-orden" onClick={() => alternarOrden(c.clave)}>
                        {c.titulo}
                      </button>
                    ) : (
                      c.titulo
                    )}
                  </th>
                )
              })}
            </tr>
          </thead>
          <tbody>
            {ordenadas.length === 0 && (
              <tr>
                <td colSpan={columnas.length} className="texto-apagado">
                  {vacio ?? 'Sin filas.'}
                </td>
              </tr>
            )}
            {ordenadas.map((fila) => (
              <tr
                key={claveFila(fila)}
                className={`${onClickFila ? 'fila-enlace' : ''} ${claseFila?.(fila) ?? ''}`.trim() || undefined}
                onClick={onClickFila ? () => onClickFila(fila) : undefined}
              >
                {columnas.map((c) => (
                  <td key={c.clave} className={c.numerica ? 'num' : undefined}>
                    {c.render(fila)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {caption && (
        <p id={idPie} className="tabla-pie">
          {caption}
        </p>
      )}
    </div>
  )
}
