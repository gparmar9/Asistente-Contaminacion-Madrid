import { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'

import { useEstaciones, useSeriesPorContaminante } from '../api/consultas'
import type { Contaminante, PuntoSerie } from '../api/tipos'
import { SelectorEstacion } from '../componentes/filtros/SelectorEstacion'
import { BloquesMarcados, type FilaBloque } from '../componentes/hoy/BloquesMarcados'
import { InformeDia } from '../componentes/hoy/InformeDia'
import { StatTile } from '../componentes/hoy/StatTile'
import { DecoracionCielo } from '../componentes/layout/DecoracionCielo'
import { Cargando, ErrorBloque, Vacio } from '../componentes/ui/Avisos'
import { Tarjeta } from '../componentes/ui/Tarjeta'
import { estadoPunto, resumirAnomalias } from '../dominio/anomalias'
import { contaminantesDisponibles } from '../dominio/contaminantes'
import { formatearFechaLarga, hoyISO, rangoUltimosDias } from '../dominio/fechas'
import { pluralizar } from '../dominio/formato'
import { useEstacionSeleccionada } from '../estado/hooks'

/**
 * Pantalla Hoy: cifra protagonista (anomalías fiables del día de referencia),
 * stat tiles por contaminante, informe del día y bloques marcados.
 *
 * El pipeline consolida el día a las 23:45, así que durante la jornada lo
 * normal es que "hoy" aún no tenga bloques. En ese caso se usa el último día
 * con datos y se dice explícitamente, en lugar de mostrar un cero engañoso.
 */
export default function Hoy() {
  const estaciones = useEstaciones()
  const { codigo: seleccion, seleccionar } = useEstacionSeleccionada()
  const [hoy] = useState(hoyISO)
  const rango = useMemo(() => rangoUltimosDias(7, hoy), [hoy])

  const lista = estaciones.data ?? []
  const codigo = seleccion !== undefined && lista.some((e) => e.codigo_corto === seleccion) ? seleccion : lista[0]?.codigo_corto
  const estacion = lista.find((e) => e.codigo_corto === codigo)
  const contaminantes = useMemo(
    () => (estacion ? contaminantesDisponibles(estacion.contaminantes_medidos ?? []) : []),
    [estacion],
  )

  const series = useSeriesPorContaminante(codigo, contaminantes, rango)
  const cargandoSeries = series.length > 0 && series.some((s) => s.isPending)
  const refrescando = series.some((s) => s.isFetching && !s.isPending)

  // Día de referencia: el más reciente con algún dato en cualquier contaminante.
  const puntosPorContaminante = useMemo(() => {
    const mapa = new Map<Contaminante, PuntoSerie[]>()
    contaminantes.forEach((c, i) => mapa.set(c, series[i]?.data?.puntos ?? []))
    return mapa
  }, [contaminantes, series])

  const fechaReferencia = useMemo(() => {
    let max: string | undefined
    for (const puntos of puntosPorContaminante.values()) {
      for (const p of puntos) if (p.media != null && (max === undefined || p.fecha > max)) max = p.fecha
    }
    return max
  }, [puntosPorContaminante])

  const filasDia: FilaBloque[] = useMemo(() => {
    if (!fechaReferencia) return []
    const filas: FilaBloque[] = []
    for (const [contaminante, puntos] of puntosPorContaminante) {
      for (const p of puntos) if (p.fecha === fechaReferencia) filas.push({ contaminante, punto: p, estado: estadoPunto(p) })
    }
    return filas
  }, [puntosPorContaminante, fechaReferencia])

  const resumen = useMemo(() => resumirAnomalias(filasDia.map((f) => f.punto)), [filasDia])
  const esHoy = fechaReferencia === hoy

  if (estaciones.isPending) return <Cargando texto="Cargando el catálogo de estaciones…" />
  if (estaciones.isError) return <ErrorBloque error={estaciones.error} reintentar={() => void estaciones.refetch()} />
  if (!estacion || codigo === undefined) return <Vacio>No hay estaciones en el catálogo.</Vacio>

  return (
    <div className="apilado">
      <header className="pagina-cabecera pagina-cabecera-compacta">
        <h1>Hoy</h1>
        <p>Último día consolidado de la estación elegida: lecturas por contaminante, anomalías fiables e informe.</p>
      </header>

      <div className="filtros">
        <SelectorEstacion estaciones={lista} valor={codigo} onChange={seleccionar} />
        <div className="campo">
          <span className="campo-etiqueta">Día de referencia</span>
          <span style={{ minHeight: 34, display: 'inline-flex', alignItems: 'center', flexWrap: 'wrap', gap: 8 }}>
            {fechaReferencia ? (
              <>
                {formatearFechaLarga(fechaReferencia)}
                {!esHoy && (
                  <span className="chip" title="Los bloques de hoy se consolidan a las 23:45: se muestra el último día con datos">
                    último día con datos · hoy se consolida a las 23:45
                  </span>
                )}
              </>
            ) : cargandoSeries ? (
              'calculando…'
            ) : (
              'sin datos en 7 días'
            )}
          </span>
        </div>
        <div className="campo" style={{ marginLeft: 'auto' }}>
          <span className="campo-etiqueta">Detalle</span>
          <Link to={`/estaciones/${codigo}`} className="boton boton-s">
            Ver series y heatmap →
          </Link>
        </div>
      </div>

      <section aria-labelledby="ultimo-valor" className="apilado-s">
        <h2 id="ultimo-valor" className="t-m">
          Último valor por contaminante <span className="texto-apagado t-s">· tendencia de 7 días por bloque</span>
        </h2>
        {contaminantes.length === 0 ? (
          <Vacio>Esta estación no mide ninguno de los seis contaminantes objetivo.</Vacio>
        ) : (
          <div className="rejilla-tarjetas">
            {contaminantes.map((c, i) => (
              <StatTile
                key={c}
                contaminante={c}
                codigoEstacion={codigo}
                puntos={puntosPorContaminante.get(c) ?? []}
                cargando={series[i]?.isPending}
                error={series[i]?.error ?? undefined}
              />
            ))}
          </div>
        )}
      </section>

      <Tarjeta atenuada={refrescando} className="hero-bienvenida">
        <DecoracionCielo />
        <div className="hero hero-compacto">
          <div className="hero-principal">
            <div className="hero-cifra" aria-live="polite">
              {cargandoSeries && !fechaReferencia ? '…' : resumen.anomaliasFiables}
            </div>
            <div>
              <div className="hero-etiqueta">
                {pluralizar(resumen.anomaliasFiables, 'anomalía fiable detectada', 'anomalías fiables detectadas')}{' '}
                {esHoy ? 'hoy' : 'el último día con datos'} en {estacion.nombre}
              </div>
              <p className="hero-nota">
                Bloques marcados por el Isolation Forest con cobertura ≥ 70 %. {contaminantes.length}{' '}
                {pluralizar(contaminantes.length, 'contaminante', 'contaminantes')} × 4 bloques.
                {resumen.marcadosNoFiables > 0 &&
                  ` Otros ${resumen.marcadosNoFiables} ${pluralizar(resumen.marcadosNoFiables, 'bloque marcado', 'bloques marcados')} con cobertura baja no se cuentan: no son evaluables.`}
              </p>
            </div>
          </div>
          <div className="hero-desglose">
            <div>
              <div className="hero-desglose-valor tabular">{resumen.evaluables}</div>
              <div className="hero-desglose-etiqueta">bloques evaluables</div>
            </div>
            <div>
              <div className="hero-desglose-valor tabular">{resumen.bajaConfianza}</div>
              <div className="hero-desglose-etiqueta">con cobertura baja</div>
            </div>
            <div>
              <div className="hero-desglose-valor tabular">{resumen.total - resumen.conDato}</div>
              <div className="hero-desglose-etiqueta">sin dato</div>
            </div>
          </div>
        </div>
      </Tarjeta>

      <InformeDia estacion={estacion} fecha={fechaReferencia ?? hoy} />

      <Tarjeta
        titulo="Bloques marcados"
        sub={fechaReferencia ? `Día de referencia: ${formatearFechaLarga(fechaReferencia)}` : 'Sin día de referencia'}
        atenuada={refrescando}
      >
        <BloquesMarcados filas={filasDia} codigoEstacion={codigo} />
      </Tarjeta>
    </div>
  )
}
