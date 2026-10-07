import { useMemo, useState } from 'react'
import { Link, useParams, useSearchParams } from 'react-router-dom'

import { useEstaciones, useSerie, useSeriesPorEstacion } from '../api/consultas'
import { ErrorApi } from '../api/errores'
import type { Bloque, Contaminante, ParametrosSerie } from '../api/tipos'
import { SelectorBloque } from '../componentes/filtros/SelectorBloque'
import { SelectorContaminante } from '../componentes/filtros/SelectorContaminante'
import { SelectorEstacion } from '../componentes/filtros/SelectorEstacion'
import { SelectorRango } from '../componentes/filtros/SelectorRango'
import { GraficaLinea } from '../componentes/graficas/GraficaLinea'
import { GraficaMultilinea, type SerieComparada } from '../componentes/graficas/GraficaMultilinea'
import { Heatmap } from '../componentes/graficas/Heatmap'
import { TablaComparativa } from '../componentes/graficas/TablaComparativa'
import { TablaSerie } from '../componentes/graficas/TablaSerie'
import { Cargando, ErrorBloque, Vacio } from '../componentes/ui/Avisos'
import { Medidor } from '../componentes/ui/Medidor'
import { Segmentos } from '../componentes/ui/Segmentos'
import { Tarjeta } from '../componentes/ui/Tarjeta'
import { resumirAnomalias } from '../dominio/anomalias'
import { BLOQUES, ETIQUETA_BLOQUE, MAX_ESTACIONES_COMPARADAS, NOMBRE_CONTAMINANTE } from '../dominio/constantes'
import { contaminantesDisponibles } from '../dominio/contaminantes'
import {
  esFechaISOValida,
  formatearFechaMedia,
  hoyISO,
  RANGOS_PREESTABLECIDOS,
  rangoUltimosDias,
  type ClaveRango,
  type Rango,
} from '../dominio/fechas'
import { formatearValor, pluralizar } from '../dominio/formato'
import { useEstacionSeleccionada } from '../estado/hooks'

type Vista = 'grafica' | 'tabla'

const VISTAS = [
  { valor: 'grafica' as const, etiqueta: 'Gráficas' },
  { valor: 'tabla' as const, etiqueta: 'Tabla' },
]

/**
 * Detalle de una estación: línea de tendencia, heatmap días × bloques, tabla
 * alternativa y comparación con hasta dos estaciones más. Todo el estado de
 * los filtros vive en la URL para que una vista se pueda compartir.
 */
export default function DetalleEstacion() {
  const { codigo: codigoParam } = useParams()
  const codigo = Number(codigoParam)
  const [params, setParams] = useSearchParams()
  const estaciones = useEstaciones()
  const { seleccionar } = useEstacionSeleccionada()
  const [hoy] = useState(hoyISO)

  const lista = useMemo(() => estaciones.data ?? [], [estaciones.data])
  const estacion = lista.find((e) => e.codigo_corto === codigo)
  const disponibles = useMemo(() => contaminantesDisponibles(estacion?.contaminantes_medidos ?? []), [estacion])

  // ---- filtros desde la URL (con valores por defecto seguros)
  const contaminanteParam = params.get('contaminante')
  const contaminante: Contaminante | undefined = disponibles.includes(contaminanteParam as Contaminante)
    ? (contaminanteParam as Contaminante)
    : disponibles[0]

  const claveRangoParam = params.get('rango')
  const claveRango: ClaveRango = ['7d', '30d', '90d', 'personalizado'].includes(claveRangoParam ?? '')
    ? (claveRangoParam as ClaveRango)
    : '7d'
  const rango: Rango = useMemo(() => {
    if (claveRango === 'personalizado') {
      const desde = params.get('desde') ?? ''
      const hasta = params.get('hasta') ?? ''
      if (esFechaISOValida(desde) && esFechaISOValida(hasta) && desde <= hasta) return { desde, hasta }
    }
    const dias = RANGOS_PREESTABLECIDOS.find((r) => r.clave === claveRango)?.dias ?? 7
    return rangoUltimosDias(dias, hoy)
  }, [claveRango, params, hoy])

  const bloqueParam = params.get('bloque')
  const bloque: Bloque | undefined = BLOQUES.includes(bloqueParam as Bloque) ? (bloqueParam as Bloque) : undefined
  const vista: Vista = params.get('vista') === 'tabla' ? 'tabla' : 'grafica'
  const comparar = useMemo(
    () =>
      params
        .getAll('comparar')
        .map(Number)
        .filter((c) => Number.isInteger(c) && c !== codigo && lista.some((e) => e.codigo_corto === c))
        .slice(0, MAX_ESTACIONES_COMPARADAS - 1),
    [params, codigo, lista],
  )

  function actualizar(cambios: Record<string, string | string[] | null>) {
    const siguientes = new URLSearchParams(params)
    for (const [clave, valor] of Object.entries(cambios)) {
      siguientes.delete(clave)
      if (valor === null) continue
      for (const v of Array.isArray(valor) ? valor : [valor]) siguientes.append(clave, v)
    }
    setParams(siguientes, { replace: true })
  }

  // ---- datos
  const parametros: ParametrosSerie | undefined = contaminante
    ? { contaminante, desde: rango.desde, hasta: rango.hasta, ...(bloque ? { bloque } : {}) }
    : undefined
  const serie = useSerie(estacion ? codigo : undefined, parametros)
  const comparadas = useSeriesPorEstacion(parametros ? comparar : [], parametros ?? { contaminante: 'NO2' })

  const puntos = useMemo(() => serie.data?.puntos ?? [], [serie.data])
  const resumen = useMemo(() => resumirAnomalias(puntos), [puntos])
  const coberturaMedia = useMemo(() => {
    const valores = puntos.map((p) => p.cobertura).filter((c): c is number => c != null)
    return valores.length ? valores.reduce((a, b) => a + b, 0) / valores.length : null
  }, [puntos])

  const seriesComparadas: SerieComparada[] = useMemo(() => {
    if (!estacion || comparar.length === 0) return []
    const base: SerieComparada = { codigo, nombre: estacion.nombre, puntos, colorIndice: 0 }
    const otras = comparar.map((c, i) => ({
      codigo: c,
      nombre: lista.find((e) => e.codigo_corto === c)?.nombre ?? `Estación ${c}`,
      puntos: comparadas[i]?.data?.puntos ?? [],
      colorIndice: (i + 1) as 1 | 2,
    }))
    return [base, ...otras]
  }, [estacion, codigo, puntos, comparar, comparadas, lista])

  // ---- estados globales
  if (!Number.isInteger(codigo)) return <ErrorBloque error={new ErrorApi(404, 'El código de estación no es válido')} />
  if (estaciones.isPending) return <Cargando texto="Cargando la estación…" />
  if (estaciones.isError) return <ErrorBloque error={estaciones.error} reintentar={() => void estaciones.refetch()} />
  if (!estacion) {
    return (
      <div className="apilado">
        <ErrorBloque error={new ErrorApi(404, `La estación ${codigo} no existe`)} />
        <Link to="/estaciones">← Volver al catálogo</Link>
      </div>
    )
  }

  const tituloSerie = contaminante
    ? `${contaminante} · ${estacion.nombre} · ${formatearFechaMedia(rango.desde)} a ${formatearFechaMedia(rango.hasta)}${bloque ? ` · bloque ${ETIQUETA_BLOQUE[bloque]}` : ''}`
    : estacion.nombre
  const refrescando = serie.isFetching && !serie.isPending

  return (
    <div className="apilado">
      <header className="pagina-cabecera">
        <div className="apilado-s">
          <p className="t-s">
            <Link to="/estaciones">Estaciones</Link> / {estacion.codigo_corto}
          </p>
          <h1>{estacion.nombre}</h1>
          <p>
            {estacion.tipo}
            {estacion.distrito && ` · ${estacion.distrito}`} · {estacion.direccion}
            {estacion.altitud != null && ` · ${estacion.altitud} m`}
            {estacion.latitud != null && estacion.longitud != null && (
              <span className="texto-apagado tabular">
                {' '}
                · {estacion.latitud.toFixed(4)}, {estacion.longitud.toFixed(4)}
              </span>
            )}
          </p>
        </div>
        <button type="button" className="boton boton-s" onClick={() => seleccionar(codigo)}>
          Usar esta estación en «Hoy»
        </button>
      </header>

      {disponibles.length === 0 ? (
        <Vacio>Esta estación no mide ninguno de los seis contaminantes objetivo, así que no tiene series consultables.</Vacio>
      ) : (
        <>
          <div className="filtros">
            <SelectorContaminante medidos={estacion.contaminantes_medidos ?? []} valor={contaminante} onChange={(c) => actualizar({ contaminante: c })} />
            <SelectorRango
              clave={claveRango}
              rango={rango}
              onChange={(clave, nuevo) =>
                actualizar(
                  clave === 'personalizado'
                    ? { rango: clave, desde: nuevo.desde, hasta: nuevo.hasta }
                    : { rango: clave, desde: null, hasta: null },
                )
              }
            />
            <SelectorBloque valor={bloque} onChange={(b) => actualizar({ bloque: b ?? null })} />
            <div className="campo">
              <span className="campo-etiqueta">Vista</span>
              <Segmentos opciones={VISTAS} valor={vista} onChange={(v) => actualizar({ vista: v === 'grafica' ? null : v })} etiquetaAria="Gráficas o tabla" />
            </div>
            {comparar.length < MAX_ESTACIONES_COMPARADAS - 1 && (
              <SelectorEstacion
                id="comparar"
                etiqueta={`Comparar con (máx. ${MAX_ESTACIONES_COMPARADAS})`}
                placeholder="Añadir estación…"
                estaciones={lista.filter((e) => contaminante && (e.contaminantes_medidos ?? []).includes(contaminante))}
                valor={undefined}
                excluir={[codigo, ...comparar]}
                onChange={(c) => actualizar({ comparar: [...comparar, c].map(String) })}
              />
            )}
            {comparar.length > 0 && (
              <div className="campo">
                <span className="campo-etiqueta">Comparando</span>
                <div className="chips" style={{ minHeight: 34, alignItems: 'center' }}>
                  {comparar.map((c) => (
                    <span key={c} className="chip">
                      {lista.find((e) => e.codigo_corto === c)?.nombre ?? c}
                      <button
                        type="button"
                        className="chip-quitar"
                        aria-label={`Quitar ${lista.find((e) => e.codigo_corto === c)?.nombre ?? c} de la comparación`}
                        onClick={() => actualizar({ comparar: comparar.filter((x) => x !== c).map(String) })}
                      >
                        ×
                      </button>
                    </span>
                  ))}
                </div>
              </div>
            )}
          </div>

          {serie.isError && <ErrorBloque error={serie.error} reintentar={() => void serie.refetch()} />}

          <div className="rejilla-tarjetas">
            <Tarjeta atenuada={refrescando}>
              <div className="stat">
                <div className="stat-etiqueta">
                  <strong>Anomalías fiables</strong>
                </div>
                <div className="stat-valor">{serie.isPending ? '…' : resumen.anomaliasFiables}</div>
                <p className="stat-nota">
                  en {resumen.evaluables} {pluralizar(resumen.evaluables, 'bloque evaluable', 'bloques evaluables')} del rango
                </p>
              </div>
            </Tarjeta>
            <Tarjeta atenuada={refrescando}>
              <div className="stat">
                <div className="stat-etiqueta">
                  <strong>Cobertura media</strong>
                </div>
                <div className="stat-valor">{coberturaMedia == null ? '—' : `${Math.round(coberturaMedia * 100)} %`}</div>
                <Medidor valor={coberturaMedia} etiqueta="Media de los bloques con dato" />
              </div>
            </Tarjeta>
            <Tarjeta atenuada={refrescando}>
              <div className="stat">
                <div className="stat-etiqueta">
                  <strong>Bloques con dato</strong>
                </div>
                <div className="stat-valor">
                  {resumen.conDato}
                  <span className="stat-unidad">de {resumen.total}</span>
                </div>
                <p className="stat-nota">
                  {resumen.bajaConfianza} con cobertura baja · media del rango {formatearValor(mediaDe(puntos.map((p) => p.media)))} µg/m³
                </p>
              </div>
            </Tarjeta>
          </div>

          {vista === 'tabla' ? (
            <Tarjeta titulo="Serie por bloques" sub={tituloSerie} atenuada={refrescando}>
              {serie.isPending ? <Cargando /> : <TablaSerie puntos={puntos} />}
            </Tarjeta>
          ) : (
            <>
              <Tarjeta
                titulo={contaminante ? `Tendencia de ${contaminante}` : 'Tendencia'}
                sub={contaminante ? `${NOMBRE_CONTAMINANTE[contaminante]} · media por bloque${bloque ? `, solo ${ETIQUETA_BLOQUE[bloque].toLowerCase()}` : ', cuatro bloques por día'}` : undefined}
                atenuada={refrescando}
              >
                {serie.isPending ? <Cargando /> : <GraficaLinea puntos={puntos} titulo={`Tendencia: ${tituloSerie}`} />}
              </Tarjeta>

              <Tarjeta titulo="Días × bloques" sub="Color = magnitud de la media (rampa azul). Anomalía y cobertura baja, con icono." atenuada={refrescando}>
                {serie.isPending ? <Cargando /> : <Heatmap puntos={puntos} desde={rango.desde} hasta={rango.hasta} titulo={`Heatmap: ${tituloSerie}`} />}
              </Tarjeta>
            </>
          )}

          {comparar.length > 0 && contaminante && (
            <Tarjeta
              titulo={`Comparación de ${contaminante}`}
              sub={`${seriesComparadas.length} estaciones, mismo contaminante y rango. Misma escala y un solo eje.`}
              atenuada={comparadas.some((c) => c.isFetching)}
            >
              {comparadas.some((c) => c.isError) && (
                <p className="aviso-bloque aviso-error" role="alert">
                  No se pudo cargar alguna de las estaciones comparadas.
                </p>
              )}
              {vista === 'tabla' ? (
                <TablaComparativa series={seriesComparadas} />
              ) : (
                <GraficaMultilinea series={seriesComparadas} titulo={`Comparación: ${tituloSerie}`} />
              )}
            </Tarjeta>
          )}
        </>
      )}
    </div>
  )
}

function mediaDe(valores: readonly (number | null | undefined)[]): number | null {
  const v = valores.filter((x): x is number => x != null)
  return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null
}
