import { UMBRAL_COBERTURA } from '../../dominio/constantes'
import { formatearCobertura } from '../../dominio/formato'

interface Props {
  /** Cobertura 0..1 (horas válidas / horas del bloque). */
  valor: number | null | undefined
  etiqueta?: string
}

/** Medidor de cobertura: la pista es un paso más claro del mismo azul. Por debajo del umbral, gris. */
export function Medidor({ valor, etiqueta = 'Cobertura' }: Props) {
  const baja = valor == null || valor < UMBRAL_COBERTURA
  const ancho = valor == null ? 0 : Math.max(0, Math.min(1, valor)) * 100
  const texto = formatearCobertura(valor)
  return (
    <div>
      <div
        className={`medidor ${baja ? 'medidor-baja' : ''}`}
        role="meter"
        aria-label={etiqueta}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={valor == null ? undefined : Math.round(ancho)}
        aria-valuetext={texto}
      >
        <i style={{ width: `${ancho}%` }} />
      </div>
      <div className="stat-nota" style={{ marginTop: 4 }}>
        {etiqueta} {texto}
        {baja && valor != null && ` · < ${Math.round(UMBRAL_COBERTURA * 100)} %, anomalía no evaluada`}
      </div>
    </div>
  )
}
