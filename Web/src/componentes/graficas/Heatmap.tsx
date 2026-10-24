import { useMemo, useRef, useState, type CSSProperties } from 'react'

import type { PuntoSerie } from '../../api/tipos'
import { estadoPunto } from '../../dominio/anomalias'
import { pasoRampa, tintaSobre } from '../../dominio/color'
import { BLOQUES, ETIQUETA_BLOQUE, UNIDAD } from '../../dominio/constantes'
import { diaSemanaCorto, diasEntre, formatearFechaCorta, formatearFechaLarga } from '../../dominio/fechas'
import { formatearCobertura, formatearScore, formatearValor } from '../../dominio/formato'
import { useAncho } from '../../hooks/useAncho'
import { useColoresTema } from '../../hooks/useColoresTema'
import { Estado } from '../ui/Estado'
import { Vacio } from '../ui/Avisos'
import { clavePunto, etiquetaBloque, horasBloque } from './datos'

interface Props {
  puntos: readonly PuntoSerie[]
  desde: string
  hasta: string
  titulo: string
}

const PASOS = 7
const ANCHO_ETIQUETAS = 92

/**
 * Rejilla días × bloques. El color codifica MAGNITUD (rampa secuencial azul,
 * 7 pasos entre el mínimo y el máximo del rango visible). Celda sin dato =
 * vacía (borde discontinuo), nunca cero. Anomalía fiable = recuadro rojo + ●;
 * cobertura baja = trama + !, y nunca se presenta como anomalía.
 */
export function Heatmap({ puntos, desde, hasta, titulo }: Props) {
  const colores = useColoresTema()
  const ref = useRef<HTMLDivElement>(null)
  const anchoDisponible = useAncho(ref, 800) || 800
  const [activa, setActiva] = useState<PuntoSerie | null>(null)

  const dias = useMemo(() => diasEntre(desde, hasta), [desde, hasta])
  const indice = useMemo(() => new Map(puntos.map((p) => [clavePunto(p), p])), [puntos])
  const { min, max, hayDatos } = useMemo(() => {
    const valores = puntos.map((p) => p.media).filter((v): v is number => v != null)
    return valores.length ? { min: Math.min(...valores), max: Math.max(...valores), hayDatos: true } : { min: 0, max: 0, hayDatos: false }
  }, [puntos])

  if (!hayDatos || dias.length === 0) {
    return <Vacio>Sin datos para dibujar el heatmap en este rango.</Vacio>
  }

  const celda = Math.max(8, Math.min(30, Math.floor((anchoDisponible - ANCHO_ETIQUETAS) / dias.length) - 2))
  const conGlifo = celda >= 16
  const cadaN = Math.max(1, Math.ceil(dias.length / Math.max(1, Math.floor((anchoDisponible - ANCHO_ETIQUETAS) / 52))))
  const estiloTabla = { '--celda': `${celda}px` } as CSSProperties

  function describir(p: PuntoSerie): string {
    const estado = estadoPunto(p)
    const partes = [
      `${formatearFechaLarga(p.fecha)}, ${etiquetaBloque(p.bloque)}`,
      p.media == null ? 'sin dato' : `media ${formatearValor(p.media)} ${UNIDAD}`,
      `cobertura ${formatearCobertura(p.cobertura)}`,
      estado === 'anomalia' ? 'anomalía fiable' : estado === 'baja_confianza' ? 'cobertura baja, anomalía no evaluada' : estado === 'normal' ? 'normal' : estado === 'no_evaluado' ? 'sin evaluar' : '',
    ]
    return partes.filter(Boolean).join(' · ')
  }

  return (
    <figure aria-label={titulo}>
      <div className="heatmap-envoltorio" ref={ref}>
        <table className="heatmap" style={estiloTabla}>
          <thead>
            <tr>
              <th scope="col">
                <span className="visualmente-oculto">Bloque</span>
              </th>
              {dias.map((d, i) => (
                <th key={d} scope="col" style={{ width: celda }}>
                  {i % cadaN === 0 ? (
                    <span style={{ display: 'inline-block', transform: dias.length > 20 ? 'none' : 'none' }}>
                      {celda >= 26 && dias.length <= 14 && <span style={{ display: 'block' }}>{diaSemanaCorto(d)}</span>}
                      {formatearFechaCorta(d)}
                    </span>
                  ) : (
                    <span className="visualmente-oculto">{formatearFechaCorta(d)}</span>
                  )}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {BLOQUES.map((bloque) => (
              <tr key={bloque}>
                <th scope="row">
                  {ETIQUETA_BLOQUE[bloque]} <span className="texto-apagado">{horasBloque(bloque)}</span>
                </th>
                {dias.map((d) => {
                  const p = indice.get(`${d}|${bloque}`)
                  if (!p || p.media == null) {
                    return (
                      <td key={d}>
                        <span className="celda celda-vacia" title={`${formatearFechaCorta(d)} · ${ETIQUETA_BLOQUE[bloque]}: sin dato`} aria-label={`${formatearFechaCorta(d)}, ${ETIQUETA_BLOQUE[bloque]}: sin dato`} />
                      </td>
                    )
                  }
                  const estado = estadoPunto(p)
                  const paso = pasoRampa(p.media, min, max, PASOS)
                  const color = colores.heat[paso]
                  const clases = ['celda', estado === 'anomalia' ? 'celda-anomalia' : '', estado === 'baja_confianza' ? 'celda-baja' : ''].join(' ').trim()
                  const estilo = { '--color-celda': color } as CSSProperties
                  const glifo = estado === 'anomalia' ? '●' : estado === 'baja_confianza' ? '!' : ''
                  return (
                    <td key={d}>
                      <button
                        type="button"
                        className={clases}
                        style={estilo}
                        aria-label={describir(p)}
                        title={describir(p)}
                        onMouseEnter={() => setActiva(p)}
                        onFocus={() => setActiva(p)}
                        onClick={() => setActiva(p)}
                      >
                        {conGlifo && glifo && (
                          <span className="celda-glifo" style={{ color: tintaSobre(color) }} aria-hidden="true">
                            {glifo}
                          </span>
                        )}
                      </button>
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="tooltip-grafica" style={{ marginTop: 8, pointerEvents: 'auto', minHeight: 56 }} aria-live="polite">
        {activa ? (
          <>
            <div className="tooltip-grafica-titulo">
              {formatearFechaLarga(activa.fecha)} · {etiquetaBloque(activa.bloque)} ({horasBloque(activa.bloque)})
            </div>
            <div className="fila" style={{ gap: 16 }}>
              <span>
                Media <strong className="tabular">{formatearValor(activa.media)}</strong> {activa.media != null && UNIDAD}
              </span>
              <span>
                Máx/mín <strong className="tabular">{formatearValor(activa.maximo)} / {formatearValor(activa.minimo)}</strong>
              </span>
              <span>
                Cobertura <strong className="tabular">{formatearCobertura(activa.cobertura)}</strong>
              </span>
              <span>
                z <strong className="tabular">{formatearScore(activa.z_score)}</strong>
              </span>
              <span title="Score negado del Isolation Forest: alto = más anómalo. No es una probabilidad.">
                Score IF <strong className="tabular">{formatearScore(activa.anomaly_score)}</strong>
              </span>
              <Estado estado={estadoPunto(activa)} />
            </div>
          </>
        ) : (
          <span className="texto-apagado">Pasa el cursor o el foco por una celda para ver su detalle.</span>
        )}
      </div>

      <figcaption className="leyenda">
        <span className="leyenda-item">
          <span className="tabular">{formatearValor(min)}</span>
          <span className="leyenda-rampa" aria-hidden="true">
            {colores.heat.map((c) => (
              <i key={c} style={{ background: c }} />
            ))}
          </span>
          <span className="tabular">{formatearValor(max)}</span> {UNIDAD} (media del bloque)
        </span>
        <span className="leyenda-item">
          <span className="leyenda-muestra" style={{ boxShadow: `inset 0 0 0 2px ${colores.critical}`, background: colores.heat[3] }} aria-hidden="true" />● Anomalía (cobertura ≥ 70 %)
        </span>
        <span className="leyenda-item">
          <span className="leyenda-muestra celda-baja" style={{ background: colores.heat[3], position: 'relative' }} aria-hidden="true" />! Cobertura baja (no evaluada)
        </span>
        <span className="leyenda-item">
          <span className="leyenda-muestra" style={{ border: '1px dashed var(--rejilla)' }} aria-hidden="true" />
          Sin dato
        </span>
      </figcaption>
    </figure>
  )
}
