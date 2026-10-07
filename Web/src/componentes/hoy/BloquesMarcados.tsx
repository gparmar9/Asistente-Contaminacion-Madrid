import { Link } from 'react-router-dom'

import type { Contaminante, PuntoSerie } from '../../api/tipos'
import { estadoPunto, type EstadoPunto } from '../../dominio/anomalias'
import { UNIDAD } from '../../dominio/constantes'
import { formatearCobertura, formatearScore, formatearValor } from '../../dominio/formato'
import { etiquetaBloque, indiceBloque } from '../graficas/datos'
import { Estado } from '../ui/Estado'
import { Tabla, type Columna } from '../ui/Tabla'

export interface FilaBloque {
  contaminante: Contaminante
  punto: PuntoSerie
  estado: EstadoPunto
}

interface Props {
  filas: readonly FilaBloque[]
  codigoEstacion: number
}

const ORDEN_ESTADO: Record<EstadoPunto, number> = { anomalia: 0, baja_confianza: 1, no_evaluado: 2, normal: 3, sin_dato: 4 }

/**
 * Bloques del día que requieren atención: anomalías fiables primero y, aparte,
 * los de cobertura baja (se listan, pero NO como anomalías: no son evaluables).
 */
export function BloquesMarcados({ filas, codigoEstacion }: Props) {
  const relevantes = filas
    .filter((f) => f.estado === 'anomalia' || f.estado === 'baja_confianza')
    .sort((a, b) => ORDEN_ESTADO[a.estado] - ORDEN_ESTADO[b.estado] || indiceBloque(a.punto.bloque) - indiceBloque(b.punto.bloque))

  const columnas: Columna<FilaBloque>[] = [
    {
      clave: 'contaminante',
      titulo: 'Contaminante',
      render: (f) => (
        <Link to={`/estaciones/${codigoEstacion}?contaminante=${encodeURIComponent(f.contaminante)}`}>{f.contaminante}</Link>
      ),
      ordenar: (f) => f.contaminante,
    },
    { clave: 'bloque', titulo: 'Bloque', render: (f) => etiquetaBloque(f.punto.bloque), ordenar: (f) => indiceBloque(f.punto.bloque) },
    { clave: 'media', titulo: `Media (${UNIDAD})`, numerica: true, render: (f) => formatearValor(f.punto.media), ordenar: (f) => f.punto.media ?? null },
    { clave: 'cobertura', titulo: 'Cobertura', numerica: true, render: (f) => formatearCobertura(f.punto.cobertura), ordenar: (f) => f.punto.cobertura ?? null },
    { clave: 'z', titulo: 'z-score', numerica: true, render: (f) => formatearScore(f.punto.z_score), ordenar: (f) => f.punto.z_score ?? null },
    { clave: 'score', titulo: 'Score IF', numerica: true, render: (f) => formatearScore(f.punto.anomaly_score), ordenar: (f) => f.punto.anomaly_score ?? null },
    { clave: 'estado', titulo: 'Estado', render: (f) => <Estado estado={estadoPunto(f.punto)} suave />, ordenar: (f) => ORDEN_ESTADO[f.estado] },
  ]

  return (
    <Tabla
      filas={relevantes}
      columnas={columnas}
      claveFila={(f) => `${f.contaminante}-${f.punto.bloque}`}
      caption="Solo se presentan como anomalías los bloques marcados por el detector con cobertura ≥ 70 %. Los de cobertura baja se listan para que se vea el hueco, pero no se evalúan. Score IF: alto = más anómalo; no es una probabilidad."
      vacio="Ningún bloque marcado ni con cobertura baja en el día de referencia."
    />
  )
}
