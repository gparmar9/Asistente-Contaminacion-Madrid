import { useState } from 'react'

import { useKpis } from '../api/consultas'
import { mensajeErrorKpis } from '../api/errores'
import type { Conteo, Kpis, PorRuta } from '../api/tipos'
import { Barras, type Barra } from '../componentes/kpis/Barras'
import { HistogramaCoste } from '../componentes/kpis/HistogramaCoste'
import { Cargando, ErrorBloque, Vacio } from '../componentes/ui/Avisos'
import { Segmentos } from '../componentes/ui/Segmentos'
import { Tabla, type Columna } from '../componentes/ui/Tabla'
import { Tarjeta } from '../componentes/ui/Tarjeta'
import { formatearEntero, formatearValor, pluralizar } from '../dominio/formato'
import {
  DIAS_KPIS,
  etiquetaResultado,
  etiquetaRuta,
  filasSatisfaccion,
  formatearMomento,
  formatearMs,
  formatearPorcentaje,
  formatearSegundos,
  formatearUsd,
  indicadorSatisfaccion,
  participacion,
  proporcion,
  SIN_VALORACIONES,
  type DiasKpis,
  type FilaSatisfaccion,
} from '../dominio/kpis'

const OPCIONES_DIAS = DIAS_KPIS.map((d) => ({ valor: String(d), etiqueta: d === 1 ? '24 h' : `${d} días` }))

/**
 * KPIs de uso del asistente, calculados por el agente a partir de las trazas
 * de Phoenix. Lleva el rótulo «Solo administradores» pero no lo protege: el
 * prototipo no tiene gestión de usuarios y simularla daría una falsa seguridad.
 * Solo muestra agregados, nunca texto de los usuarios.
 */
export default function UsoAsistente() {
  const [dias, setDias] = useState<DiasKpis>(7)
  const kpis = useKpis(dias)

  return (
    <div className="apilado">
      <header className="pagina-cabecera pagina-cabecera-compacta">
        <div className="apilado-s">
          <span className="chip etiqueta-admin">Solo administradores</span>
          <h1>Uso del asistente</h1>
          <p>
            En producción, esta sección requeriría iniciar sesión con un perfil de administrador; el prototipo no tiene
            gestión de usuarios.
          </p>
        </div>
        <Segmentos
          opciones={OPCIONES_DIAS}
          valor={String(dias)}
          onChange={(v) => setDias(Number(v) as DiasKpis)}
          etiquetaAria="Periodo"
        />
      </header>

      {kpis.isPending ? (
        <Cargando texto="Leyendo las trazas del asistente…" />
      ) : kpis.isError ? (
        <ErrorBloque error={kpis.error} traducir={mensajeErrorKpis} reintentar={() => void kpis.refetch()} />
      ) : kpis.data.resumen.preguntas === 0 ? (
        <Vacio>No hay preguntas al asistente en el periodo.</Vacio>
      ) : (
        <div className={`apilado ${kpis.isFetching ? 'atenuado' : ''}`.trim()}>
          <Contenido kpis={kpis.data} />
        </div>
      )}
    </div>
  )
}

function Contenido({ kpis }: { kpis: Kpis }) {
  const { periodo, resumen, satisfaccion } = kpis
  const indicador = indicadorSatisfaccion(satisfaccion)

  return (
    <>
      <p className="t-s texto-apagado">
        {formatearMomento(periodo.desde)} – {formatearMomento(periodo.hasta)} · {resumen.dias_con_uso}{' '}
        {pluralizar(resumen.dias_con_uso, 'día con uso', 'días con uso')} · calculado {formatearMomento(periodo.generado)}{' '}
        (se recalcula cada minuto)
        {kpis.huerfanas > 0 &&
          ` · ${kpis.huerfanas} ${pluralizar(kpis.huerfanas, 'traza', 'trazas')} sin turno, fuera del recuento`}
      </p>

      <div className="rejilla-tarjetas">
        <Indicador etiqueta="Preguntas" valor={formatearEntero(resumen.preguntas)} nota={`en ${periodo.dias} ${pluralizar(periodo.dias, 'día', 'días')}`} />
        <Indicador
          etiqueta="Tasa de resolución"
          valor={formatearPorcentaje(proporcion(resumen.resueltas, resumen.preguntas))}
          nota={`${formatearEntero(resumen.resueltas)} respondidas o con frase fija`}
        />
        <Indicador etiqueta="Coste P50" valor={formatearUsd(resumen.coste_usd.p50)} nota={notaN(resumen.coste_usd.n)} />
        <Indicador etiqueta="Latencia P50" valor={formatearMs(resumen.latencia_ms.p50)} nota={notaN(resumen.latencia_ms.n)} />
        <Indicador
          etiqueta="Primer token P50"
          valor={formatearMs(resumen.primer_token_ms.p50)}
          nota={notaN(resumen.primer_token_ms.n)}
        />
        <Indicador etiqueta="Satisfacción 👍" valor={indicador.valor} nota={indicador.nota} />
      </div>

      <div className="rejilla-2">
        <Tarjeta titulo="Embudo" sub="Qué se pregunta, por dónde lo resuelve el agente y cómo acaba">
          <div className="apilado">
            <Barras titulo="Intención" barras={barrasConteo(kpis.intenciones, resumen.preguntas)} maximo={resumen.preguntas} />
            <Barras
              titulo="Ruta"
              barras={barrasConteo(kpis.rutas, resumen.preguntas, etiquetaRuta)}
              maximo={resumen.preguntas}
            />
            <Barras
              titulo="Resultado"
              barras={barrasConteo(kpis.resultados, resumen.preguntas, etiquetaResultado)}
              maximo={resumen.preguntas}
            />
          </div>
        </Tarjeta>

        <Tarjeta titulo="Coste por turno" sub={kpis.precios}>
          {kpis.coste === null ? (
            <Vacio>Ningún turno del periodo tiene coste calculable.</Vacio>
          ) : (
            <div className="apilado">
              <HistogramaCoste valores={kpis.coste.valores_usd} p50={resumen.coste_usd.p50} p90={resumen.coste_usd.p90} />
              <dl className="datos-clave">
                <div>
                  <dt>Total</dt>
                  <dd className="tabular">{formatearUsd(kpis.coste.total_usd)}</dd>
                </div>
                <div>
                  <dt>Por pregunta resuelta</dt>
                  <dd className="tabular">{formatearUsd(kpis.coste.por_resuelta_usd)}</dd>
                </div>
                <div>
                  <dt>Modelos</dt>
                  <dd>{kpis.coste.modelos.join(', ') || '—'}</dd>
                </div>
              </dl>
            </div>
          )}
        </Tarjeta>
      </div>

      <div className="rejilla-2">
        <Tarjeta titulo="Satisfacción por ruta" sub="% de 👍 sobre los votos de cada ruta">
          {satisfaccion === null ? (
            <Vacio>{SIN_VALORACIONES}.</Vacio>
          ) : (
            <Barras
              titulo={`${formatearEntero(satisfaccion.votos)} ${pluralizar(satisfaccion.votos, 'voto', 'votos')}`}
              barras={filasSatisfaccion(satisfaccion.por_ruta).map((f) => ({
                clave: f.grupo,
                etiqueta: etiquetaRuta(f.grupo),
                valor: f.tasa,
                texto: `${formatearPorcentaje(f.tasa)} · n = ${f.votos}`,
              }))}
              maximo={1}
            />
          )}
        </Tarjeta>

        <Tarjeta titulo="Satisfacción por tema">
          {satisfaccion === null ? (
            <Vacio>{SIN_VALORACIONES}.</Vacio>
          ) : (
            <Tabla
              filas={filasSatisfaccion(satisfaccion.por_tema)}
              columnas={COLUMNAS_SATISFACCION}
              claveFila={(f) => f.grupo}
              caption={`Participación: ${formatearPorcentaje(participacion(satisfaccion))} (${satisfaccion.sesiones_con_voto} de ${satisfaccion.sesiones_elegibles} sesiones con 2 o más respuestas completas, las que ven el aviso, votaron).`}
            />
          )}
        </Tarjeta>
      </div>

      <Tarjeta titulo="Rutas" sub="Latencia del turno completo y del primer token (solo streaming)">
        <Tabla
          filas={filasRutas(kpis)}
          columnas={COLUMNAS_RUTAS}
          claveFila={(f) => f.ruta}
          ordenInicial={{ clave: 'turnos', direccion: 'desc' }}
        />
      </Tarjeta>

      <div className="rejilla-2">
        <Tarjeta titulo="Temas">
          <Tabla
            filas={kpis.temas}
            columnas={columnasConteo('Tema', resumen.preguntas)}
            claveFila={(f) => f.categoria}
            ordenInicial={{ clave: 'n', direccion: 'desc' }}
          />
        </Tarjeta>

        <Tarjeta
          titulo="Fallos de herramienta"
          sub={`${formatearEntero(kpis.herramientas.llamadas)} llamadas · ${kpis.herramientas.datos_no_disponibles} de ${kpis.herramientas.preguntas_de_datos} preguntas de datos sin base`}
        >
          <Tabla
            filas={kpis.herramientas.fallos}
            columnas={COLUMNAS_FALLOS}
            claveFila={(f) => `${f.herramienta}-${f.causa}`}
            ordenInicial={{ clave: 'fallos', direccion: 'desc' }}
            vacio="Ninguna herramienta ha fallado en el periodo."
          />
        </Tarjeta>
      </div>

      <Tarjeta titulo="Sesiones">
        {kpis.sesiones === null ? (
          <Vacio>Los turnos del periodo no llevan identificador de sesión.</Vacio>
        ) : (
          <Tabla
            filas={filasSesiones(kpis.sesiones)}
            columnas={COLUMNAS_CLAVE_VALOR}
            claveFila={(f) => f.indicador}
          />
        )}
      </Tarjeta>
    </>
  )
}

function Indicador({ etiqueta, valor, nota }: { etiqueta: string; valor: string; nota: string }) {
  return (
    <section className="tarjeta stat">
      <h2 className="stat-etiqueta">{etiqueta}</h2>
      <div className="stat-valor tabular">{valor}</div>
      <p className="stat-nota">{nota}</p>
    </section>
  )
}

function notaN(n: number): string {
  return n === 0 ? 'Sin turnos con este dato' : `n = ${formatearEntero(n)}`
}

function barrasConteo(conteos: readonly Conteo[], total: number, etiqueta: (c: string) => string = (c) => c): Barra[] {
  return conteos.map((c) => ({
    clave: c.categoria,
    etiqueta: etiqueta(c.categoria),
    valor: c.n,
    texto: `${formatearEntero(c.n)} · ${formatearPorcentaje(proporcion(c.n, total))}`,
  }))
}

function columnasConteo(titulo: string, total: number): Columna<Conteo>[] {
  return [
    { clave: 'categoria', titulo, render: (f) => f.categoria, ordenar: (f) => f.categoria },
    { clave: 'n', titulo: 'Preguntas', numerica: true, render: (f) => formatearEntero(f.n), ordenar: (f) => f.n },
    { clave: 'pct', titulo: '%', numerica: true, render: (f) => formatearPorcentaje(proporcion(f.n, total)) },
  ]
}

const COLUMNAS_SATISFACCION: Columna<FilaSatisfaccion>[] = [
  { clave: 'grupo', titulo: 'Tema', render: (f) => f.grupo },
  { clave: 'votos', titulo: 'Votos', numerica: true, render: (f) => f.votos, ordenar: (f) => f.votos },
  { clave: 'positivos', titulo: '👍', numerica: true, render: (f) => f.positivos },
  { clave: 'tasa', titulo: '% 👍', numerica: true, render: (f) => formatearPorcentaje(f.tasa), ordenar: (f) => f.tasa },
]

interface FilaRuta {
  ruta: string
  turnos: number
  latencia: PorRuta['ms'] | undefined
  primerToken: PorRuta['ms'] | undefined
}

function filasRutas(kpis: Kpis): FilaRuta[] {
  return kpis.rutas.map((r) => ({
    ruta: r.categoria,
    turnos: r.n,
    latencia: kpis.latencia_por_ruta.find((l) => l.ruta === r.categoria)?.ms,
    primerToken: kpis.primer_token_por_ruta.find((l) => l.ruta === r.categoria)?.ms,
  }))
}

const COLUMNAS_RUTAS: Columna<FilaRuta>[] = [
  { clave: 'ruta', titulo: 'Ruta', render: (f) => etiquetaRuta(f.ruta) },
  { clave: 'turnos', titulo: 'Turnos', numerica: true, render: (f) => formatearEntero(f.turnos), ordenar: (f) => f.turnos },
  {
    clave: 'p50',
    titulo: 'Latencia P50',
    numerica: true,
    render: (f) => formatearMs(f.latencia?.p50),
    ordenar: (f) => f.latencia?.p50,
  },
  { clave: 'p90', titulo: 'Latencia P90', numerica: true, render: (f) => formatearMs(f.latencia?.p90) },
  {
    clave: 'token',
    titulo: 'Primer token P50',
    numerica: true,
    render: (f) => (f.primerToken ? `${formatearMs(f.primerToken.p50)} (n = ${f.primerToken.n})` : '—'),
  },
]

const COLUMNAS_FALLOS: Columna<Kpis['herramientas']['fallos'][number]>[] = [
  { clave: 'herramienta', titulo: 'Herramienta', render: (f) => f.herramienta, ordenar: (f) => f.herramienta },
  { clave: 'causa', titulo: 'Causa', render: (f) => f.causa },
  { clave: 'fallos', titulo: 'Fallos', numerica: true, render: (f) => f.fallos, ordenar: (f) => f.fallos },
]

interface FilaClaveValor {
  indicador: string
  valor: string
}

function filasSesiones(s: NonNullable<Kpis['sesiones']>): FilaClaveValor[] {
  return [
    { indicador: 'Sesiones', valor: formatearEntero(s.sesiones) },
    { indicador: 'Turnos por sesión (mediana · máximo)', valor: `${formatearValor(s.turnos_mediana)} · ${s.turnos_maximo}` },
    { indicador: 'Sesiones de un solo turno', valor: `${s.de_un_turno} (${formatearPorcentaje(proporcion(s.de_un_turno, s.sesiones))})` },
    {
      indicador: 'Duración P50 de las sesiones con 2 o más turnos',
      valor: `${formatearSegundos(s.duracion_s.p50)} (n = ${s.duracion_s.n})`,
    },
    { indicador: 'Turnos que siguen a otro de la misma sesión', valor: formatearEntero(s.turnos_con_previo) },
    {
      indicador: 'Turnos tras un rechazo (reformulación) · resueltos',
      valor: `${s.tras_rechazo} · ${s.resueltos_tras_rechazo}`,
    },
  ]
}

const COLUMNAS_CLAVE_VALOR: Columna<FilaClaveValor>[] = [
  { clave: 'indicador', titulo: 'Indicador', render: (f) => f.indicador },
  { clave: 'valor', titulo: 'Valor', numerica: true, render: (f) => f.valor },
]
