import { useState } from 'react'

import {
  esFechaISOValida,
  hoyISO,
  RANGOS_PREESTABLECIDOS,
  rangoUltimosDias,
  type ClaveRango,
  type Rango,
} from '../../dominio/fechas'
import { Segmentos } from '../ui/Segmentos'

interface Props {
  clave: ClaveRango
  rango: Rango
  onChange: (clave: ClaveRango, rango: Rango) => void
}

const OPCIONES = [
  ...RANGOS_PREESTABLECIDOS.map((r) => ({ valor: r.clave, etiqueta: r.etiqueta })),
  { valor: 'personalizado' as const, etiqueta: 'Personalizado' },
]

/** Rangos preestablecidos (7/30/90 días) y rango personalizado con validación desde <= hasta. */
export function SelectorRango({ clave, rango, onChange }: Props) {
  const [borrador, setBorrador] = useState<Rango>(rango)
  const hoy = hoyISO()

  const desdeValida = esFechaISOValida(borrador.desde)
  const hastaValida = esFechaISOValida(borrador.hasta)
  const ordenValido = desdeValida && hastaValida && borrador.desde <= borrador.hasta
  const error = !desdeValida || !hastaValida ? 'Introduce dos fechas válidas.' : !ordenValido ? "'Desde' no puede ser posterior a 'Hasta'." : null

  function elegirPreestablecido(nueva: ClaveRango) {
    if (nueva === 'personalizado') {
      setBorrador(rango)
      onChange('personalizado', rango)
      return
    }
    const dias = RANGOS_PREESTABLECIDOS.find((r) => r.clave === nueva)?.dias ?? 7
    const nuevo = rangoUltimosDias(dias)
    setBorrador(nuevo)
    onChange(nueva, nuevo)
  }

  return (
    <div className="campo">
      <span className="campo-etiqueta">Rango</span>
      <div className="fila" style={{ gap: 8 }}>
        <Segmentos opciones={OPCIONES} valor={clave} onChange={elegirPreestablecido} etiquetaAria="Rango de fechas" />
        {clave === 'personalizado' && (
          <form
            className="fila"
            style={{ gap: 6 }}
            onSubmit={(e) => {
              e.preventDefault()
              if (ordenValido) onChange('personalizado', borrador)
            }}
          >
            <label className="visualmente-oculto" htmlFor="rango-desde">
              Desde
            </label>
            <input
              id="rango-desde"
              className="entrada"
              type="date"
              value={borrador.desde}
              max={hoy}
              onChange={(e) => setBorrador((b) => ({ ...b, desde: e.target.value }))}
              aria-invalid={!desdeValida || !ordenValido}
            />
            <span className="texto-apagado t-s">a</span>
            <label className="visualmente-oculto" htmlFor="rango-hasta">
              Hasta
            </label>
            <input
              id="rango-hasta"
              className="entrada"
              type="date"
              value={borrador.hasta}
              max={hoy}
              onChange={(e) => setBorrador((b) => ({ ...b, hasta: e.target.value }))}
              aria-invalid={!hastaValida || !ordenValido}
            />
            <button type="submit" className="boton boton-s" disabled={!ordenValido}>
              Aplicar
            </button>
            {error && (
              <span className="t-xs" style={{ color: 'var(--critical)' }} role="alert">
                {error}
              </span>
            )}
          </form>
        )}
      </div>
    </div>
  )
}
