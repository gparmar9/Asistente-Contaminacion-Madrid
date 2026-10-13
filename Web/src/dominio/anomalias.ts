/**
 * Lectura correcta de la salida del detector.
 *
 * Regla central: `is_anomaly` solo se interpreta con `cobertura >= 0,7`. Por
 * debajo, el bloque se muestra con su dato y su cobertura como "baja
 * confianza", nunca como anomalía ni como normal. `anomaly_score` es el score
 * negado del Isolation Forest (alto = más anómalo): NO es una probabilidad.
 */
import type { PuntoSerie } from '../api/tipos'
import { UMBRAL_COBERTURA } from './constantes'

export type EstadoPunto = 'anomalia' | 'normal' | 'baja_confianza' | 'no_evaluado' | 'sin_dato'

export interface DescripcionEstado {
  etiqueta: string
  icono: string
  /** Token de color (variable CSS sin `--`). Siempre acompañado de icono y texto. */
  color: 'critical' | 'good' | 'warning' | 'apagado'
  descripcion: string
}

export const ESTADOS: Record<EstadoPunto, DescripcionEstado> = {
  anomalia: {
    etiqueta: 'Anomalía',
    icono: '●',
    color: 'critical',
    descripcion: 'El detector marca el bloque y la cobertura es suficiente (≥ 70 %).',
  },
  normal: {
    etiqueta: 'Normal',
    icono: '✓',
    color: 'good',
    descripcion: 'El detector no marca el bloque y la cobertura es suficiente (≥ 70 %).',
  },
  baja_confianza: {
    etiqueta: 'Cobertura baja',
    icono: '!',
    color: 'warning',
    descripcion: 'Cobertura < 70 %: el dato se muestra, pero la anomalía no se evalúa.',
  },
  no_evaluado: {
    etiqueta: 'Sin evaluar',
    icono: '–',
    color: 'apagado',
    descripcion: 'Hay dato pero el detector no devolvió resultado para este bloque.',
  },
  sin_dato: {
    etiqueta: 'Sin dato',
    icono: '∅',
    color: 'apagado',
    descripcion: 'Ningún valor medido en el bloque.',
  },
}

export function coberturaSuficiente(punto: Pick<PuntoSerie, 'cobertura'>): boolean {
  return punto.cobertura != null && punto.cobertura >= UMBRAL_COBERTURA
}

export function estadoPunto(punto: PuntoSerie): EstadoPunto {
  if (punto.media == null) return 'sin_dato'
  if (!coberturaSuficiente(punto)) return 'baja_confianza'
  if (punto.is_anomaly == null) return 'no_evaluado'
  return punto.is_anomaly ? 'anomalia' : 'normal'
}

/** Anomalía que se puede presentar como tal: marcada por el detector Y con cobertura suficiente. */
export function esAnomaliaFiable(punto: PuntoSerie): boolean {
  return estadoPunto(punto) === 'anomalia'
}

export interface ResumenAnomalias {
  total: number
  conDato: number
  evaluables: number
  anomaliasFiables: number
  bajaConfianza: number
  /** Bloques marcados por el detector pero con cobertura insuficiente: NO se presentan como anomalías. */
  marcadosNoFiables: number
}

export function resumirAnomalias(puntos: readonly PuntoSerie[]): ResumenAnomalias {
  const resumen: ResumenAnomalias = {
    total: puntos.length,
    conDato: 0,
    evaluables: 0,
    anomaliasFiables: 0,
    bajaConfianza: 0,
    marcadosNoFiables: 0,
  }
  for (const p of puntos) {
    const estado = estadoPunto(p)
    if (estado !== 'sin_dato') resumen.conDato += 1
    if (estado === 'anomalia' || estado === 'normal') resumen.evaluables += 1
    if (estado === 'anomalia') resumen.anomaliasFiables += 1
    if (estado === 'baja_confianza') {
      resumen.bajaConfianza += 1
      if (p.is_anomaly) resumen.marcadosNoFiables += 1
    }
  }
  return resumen
}
