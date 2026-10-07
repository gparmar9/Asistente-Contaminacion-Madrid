interface Opcion<V extends string> {
  valor: V
  etiqueta: string
}

interface Props<V extends string> {
  opciones: readonly Opcion<V>[]
  valor: V
  onChange: (valor: V) => void
  etiquetaAria: string
}

/** Control segmentado (rangos, gráfica/tabla): botones con `aria-pressed`. */
export function Segmentos<V extends string>({ opciones, valor, onChange, etiquetaAria }: Props<V>) {
  return (
    <div className="segmentos" role="group" aria-label={etiquetaAria}>
      {opciones.map((o) => (
        <button key={o.valor} type="button" aria-pressed={o.valor === valor} onClick={() => onChange(o.valor)}>
          {o.etiqueta}
        </button>
      ))}
    </div>
  )
}
