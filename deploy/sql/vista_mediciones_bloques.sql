-- Vista `mediciones_bloques`: la única relación que consulta la herramienta SQL del agente.
--
-- Grano: una fila por estación, contaminante, fecha y bloque horario. Los bloques duran 7, 6, 7
-- y 4 horas (madrugada 0-6, manana 7-12, tarde 13-19, noche 20-23; src/etl/features_bloques.py)
-- y `n_horas` cuenta las horas válidas de cada uno. Por eso la media de un día, una estación o
-- un distrito es SUM(media * n_horas) / SUM(n_horas), nunca AVG(media).
--
-- Solo los 2 últimos años, contados desde el último día cargado y no desde CURRENT_DATE: así el
-- Postgres local (parquet antiguo) y RDS se comportan igual.
--
-- La vista se ejecuta con los permisos de su propietario: el rol del agente solo necesita SELECT
-- sobre ella (rol_agente_lectura.sql), no sobre las tablas.
--
-- Idempotente. Lo ejecuta la persona con el usuario propietario de las tablas (en local,
-- postgres; en RDS, el usuario maestro), después de cargar los datos y antes del rol:
--
--   psql "$DATABASE_URL" -f deploy/sql/vista_mediciones_bloques.sql

\set ON_ERROR_STOP on

-- El índice existente empieza por (estacion, magnitud) y no sirve a las consultas por periodo o
-- distrito; este también acelera el MAX(fecha) del filtro de 2 años.
CREATE INDEX IF NOT EXISTS idx_resumen_datos_ml_fecha ON resumen_datos_ml (fecha);

CREATE OR REPLACE VIEW mediciones_bloques AS
SELECT
    r.fecha::date  AS fecha,
    r.estacion     AS estacion,          -- código corto de la estación
    e.nombre       AS nombre_estacion,
    e.distrito     AS distrito,
    e.tipo         AS tipo_estacion,
    r.contaminante AS contaminante,      -- NO, NO2, NOx, O3, PM10, PM2.5 (µg/m³)
    r.bloque       AS bloque,            -- madrugada, manana, tarde, noche
    r.hora_inicio  AS hora_inicio,
    r.hora_fin     AS hora_fin,
    r.media        AS media,
    r.maximo       AS maximo,            -- máximo horario del bloque
    r.minimo       AS minimo,
    r.n_horas      AS n_horas,           -- horas válidas del bloque
    r.cobertura    AS cobertura,         -- n_horas / duración del bloque
    r.z_score      AS z_score,
    r.is_anomaly   AS is_anomaly,
    r.dia_semana   AS dia_semana,        -- 0 = lunes ... 6 = domingo
    r.es_fin_semana AS es_fin_semana,
    r.ano          AS ano,
    r.mes          AS mes
FROM resumen_datos_ml r
LEFT JOIN estaciones e ON e.codigo_corto = r.estacion
WHERE r.fecha >= (SELECT MAX(fecha) FROM resumen_datos_ml) - INTERVAL '2 years';
