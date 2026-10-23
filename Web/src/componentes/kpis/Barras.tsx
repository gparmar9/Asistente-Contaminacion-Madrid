export interface Barra {
  clave: string
  etiqueta: string
  valor: number
  /** Texto a la derecha de la barra; lleva la cifra para que el color nunca vaya solo. */
  texto: string
}

interface Props {
  titulo: string
  barras: readonly Barra[]
  /** Valor que ocupa la pista entera (el total, o 1 para tasas). */
  maximo: number
  vacio?: string
}

/** Barras horizontales sin ejes: etiqueta, pista y cifra en cada fila. */
export function Barras({ titulo, barras, maximo, vacio = 'Sin datos en el periodo.' }: Props) {
  return (
    <figure className="barras">
      <figcaption className="barras-titulo">{titulo}</figcaption>
      {barras.length === 0 ? (
        <p className="texto-apagado t-s">{vacio}</p>
      ) : (
        <ul>
          {barras.map((b) => (
            <li key={b.clave} className="barras-fila">
              <span className="barras-etiqueta">{b.etiqueta}</span>
              <span className="barras-pista" aria-hidden="true">
                <i style={{ width: `${maximo > 0 ? Math.min(1, b.valor / maximo) * 100 : 0}%` }} />
              </span>
              <span className="barras-valor tabular">{b.texto}</span>
            </li>
          ))}
        </ul>
      )}
    </figure>
  )
}
