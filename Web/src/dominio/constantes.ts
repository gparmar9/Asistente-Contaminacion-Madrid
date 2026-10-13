/**
 * Constantes de dominio. El vocabulario es el de la API (en español y sin
 * tildes en los valores que viajan por la URL: `manana`, no `mañana`).
 */
import type { Bloque, Contaminante } from '../api/tipos'

/** Orden cronológico de los bloques, tal como los nombra la API. */
export const BLOQUES: readonly Bloque[] = ['madrugada', 'manana', 'tarde', 'noche']

/** Etiquetas para la interfaz (aquí sí con tilde). */
export const ETIQUETA_BLOQUE: Record<Bloque, string> = {
  madrugada: 'Madrugada',
  manana: 'Mañana',
  tarde: 'Tarde',
  noche: 'Noche',
}

export const HORAS_BLOQUE: Record<Bloque, string> = {
  madrugada: '00–06 h',
  manana: '07–12 h',
  tarde: '13–19 h',
  noche: '20–23 h',
}

/** Los 6 contaminantes objetivo, en el orden canónico del proyecto. */
export const CONTAMINANTES: readonly Contaminante[] = ['NO', 'NO2', 'NOx', 'O3', 'PM10', 'PM2.5']

export const NOMBRE_CONTAMINANTE: Record<Contaminante, string> = {
  NO: 'Monóxido de nitrógeno',
  NO2: 'Dióxido de nitrógeno',
  NOx: 'Óxidos de nitrógeno',
  O3: 'Ozono',
  PM10: 'Partículas < 10 µm',
  'PM2.5': 'Partículas < 2,5 µm',
}

/** Unidad de todos los contaminantes de `resumen_datos_ml`. */
export const UNIDAD = 'µg/m³'

/** `is_anomaly` solo es interpretable con cobertura >= 0,7 (requisito de correctitud). */
export const UMBRAL_COBERTURA = 0.7

export const MAX_ESTACIONES_COMPARADAS = 3

export const MAX_CARACTERES_PREGUNTA = 2000

/** Texto fijo del banner cuando la API no envía `advertencia`. */
export const AVISO_MEDICO_POR_DEFECTO = 'Demo académica: esta respuesta no constituye consejo médico.'

/** Preguntas sugeridas (del banco `Preguntas.txt`, las que el sistema puede responder sin predecir). */
export const PREGUNTAS_SUGERIDAS: readonly string[] = [
  '¿Qué estación de Madrid tiene hoy la peor calidad del aire?',
  '¿Qué síntomas puede empeorar el ozono?',
  '¿En qué se diferencia el PM10 del PM2.5 en términos de riesgo para la salud?',
  '¿Qué significa una lectura alta de NO para una persona que está junto al tráfico?',
  '¿Está Madrid en aviso o alerta por contaminación según el protocolo de NO2?',
  '¿Qué contaminante debería vigilar primero si tengo asma inducida por el ejercicio?',
]

/** Prompts del "informe del día" (rotan por fecha; `{estacion}` se sustituye por el nombre). */
export const PROMPTS_INFORME: readonly string[] = [
  'Resume en pocas líneas la situación de la calidad del aire hoy en la estación {estacion}: niveles por contaminante y si hay bloques anómalos.',
  '¿Qué contaminante destaca hoy en la estación {estacion} y qué implica para la salud según la documentación?',
  'Compara los bloques de hoy (madrugada, mañana, tarde, noche) en la estación {estacion} y señala si alguno es anómalo.',
  '¿Hay anomalías fiables hoy en la estación {estacion}? Explica brevemente qué significa una anomalía del detector.',
  'Describe la calidad del aire de hoy en {estacion} para una persona con asma, citando la documentación de salud.',
]
