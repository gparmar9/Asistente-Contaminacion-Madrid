---
titulo: Glosario de magnitudes y estructura de los datos
tema: referencia
contaminantes: ["NO", NO2, PM2.5, PM10, NOx, O3]
fuente: "Ayuntamiento de Madrid — datos abiertos; OMS 2021"
---

# Glosario de magnitudes y estructura de los datos

Referencia para que el asistente traduzca los códigos de la base de datos a lenguaje humano.

## Códigos de magnitud (contaminantes del proyecto)

| Código | Fórmula | Nombre | Unidad | Guía OMS 24h |
|---|---|---|---|---|
| 7  | NO | Monóxido de nitrógeno | µg/m³ | — |
| 8  | NO2 | Dióxido de nitrógeno | µg/m³ | 25 |
| 9  | PM2.5 | Partículas < 2,5 µm | µg/m³ | 15 |
| 10 | PM10 | Partículas < 10 µm | µg/m³ | 45 |
| 12 | NOx | Óxidos de nitrógeno (como NO2) | µg/m³ | — |
| 14 | O3 | Ozono troposférico | µg/m³ | 100 (8h) |

## Bloques del día

Los datos se agregan en cuatro bloques fijos en la tabla `resumen_datos_ml`:

| Bloque | Horas | Sentido |
|---|---|---|
| madrugada | 0-6 | Tráfico mínimo; suele ser el momento más limpio para NO2 |
| manana | 7-12 | Incluye la punta de tráfico de la mañana (NO2 alto) |
| tarde | 13-19 | Máximo de ozono en verano |
| noche | 20-23 | Incluye la punta de tráfico de la tarde-noche |

## Estructura de la base de datos

Los nombres reales de las tablas y columnas en PostgreSQL van en minúscula (así se consultan por SQL):

- **`calidad_aire_horas_live`**: dato bruto hora a hora (`estacion`, `magnitud`, `fecha`, `hora`,
  `valor`, `validacion`). El campo `validacion` vale `'V'` (medición válida) o `'N'` (placeholder,
  que el modelo descarta). Se consulta para el detalle exacto de una hora concreta.
- **`resumen_datos_ml`**: agregación por bloque con el scoring del modelo. Es la tabla principal.
  Campos relevantes para interpretar:
  - `media`, `maximo`, `minimo`: estadísticas del bloque.
  - `expected_value`: valor que el modelo esperaba para ese contexto (estación, contaminante,
    bloque, mes). Es la referencia de normalidad (la media histórica del baseline).
  - `anomaly_score`: puntuación de rareza del Isolation Forest (su *score* de aislamiento
    negado): **cuanto más alto, más anómalo**. No es una probabilidad ni un percentil —no está
    acotado a 0-1— y solo es comparable entre bloques del mismo contaminante. No lo describas al
    usuario como "un X % de rareza".
  - `is_anomaly`: booleano que marca el bloque como anómalo. Lo decide el propio Isolation Forest
    según la tasa de contaminación fijada al entrenar (`predict == -1`), **no** un percentil fijo.
    Es la señal autorizada de "anómalo / no anómalo".

## Cómo interpretar una anomalía para el usuario

Una anomalía no significa necesariamente "peligro": significa "inusual respecto al histórico".
El modelo puntúa cada bloque a partir de tres features y salta por dos motivos distintos:

- **Ambiental (nivel inusual)**: el valor se aleja de lo esperado (`z_score` alto). Puede ser
  inusualmente **alto** (episodio de contaminación, lo más relevante para salud) o inusualmente
  **bajo** (por ejemplo, un festivo con poco tráfico).
- **Operativa (fallo del sensor)**: faltan horas del bloque (`cobertura` baja → sensor caído) o
  la señal no varía (`cv ≈ 0` → sensor congelado). No es un problema de salud, sino de dato.

Para distinguirlos, el asistente debe mirar `media` frente a `expected_value`: si la media supera
con mucho el esperado, es un episodio ambiental al alza y conviene recomendar prudencia; si la
`media` es normal pero el bloque es anómalo, probablemente sea una anomalía operativa del sensor.

## Fuentes

- Ayuntamiento de Madrid — Portal de datos abiertos: especificación de los códigos de magnitud y
  del formato de los datos de calidad del aire. <https://datos.madrid.es>
- OMS. *WHO global air quality guidelines* (2021), para los umbrales de referencia.
  <https://www.who.int/publications/i/item/9789240034228>

> Este documento es una **síntesis divulgativa** elaborada para el proyecto a partir de las fuentes citadas; no reproduce literalmente su contenido. Verifica las cifras exactas y su vigencia en los documentos originales antes de cualquier uso operativo. Consulta también el [aviso médico](aviso_medico.md).
