-- Gold de `casos_datos_nuevos.json`: lote no visto para comparar SQL libre y catálogo.
--
-- Mismas reglas que `gold_datos.sql`: solo la vista `mediciones_bloques`, FECHA_REFERENCIA=2026-05-01
-- (viernes), último día 2026-04-30, la vista empieza el 2024-04-30, media ponderada por horas
-- SUM(media * n_horas) / SUM(n_horas). Los periodos son los que calcula `business/periodos.py`.
-- Anomalía = is_anomaly AND cobertura >= 0.7 (como dat-09).
--
--   docker exec -i jupiter_postgres psql -U postgres < Agente/evaluacion/gold_datos_nuevos.sql

\pset footer off

\echo '== nd-01: PM2.5 por estación, mes pasado (2026-04-01 a 2026-04-30), de más a menos'
SELECT nombre_estacion, distrito, COUNT(DISTINCT fecha) AS dias,
       ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1) AS media_pond
FROM mediciones_bloques
WHERE contaminante = 'PM2.5' AND fecha BETWEEN DATE '2026-04-01' AND DATE '2026-04-30'
GROUP BY 1, 2 ORDER BY media_pond DESC;

\echo '== nd-02: NO2 por distrito, bloque noche, últimos 30 días (2026-04-01 a 2026-04-30)'
SELECT distrito, COUNT(DISTINCT nombre_estacion) AS estaciones,
       ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1) AS media_pond
FROM mediciones_bloques
WHERE contaminante = 'NO2' AND bloque = 'noche' AND fecha BETWEEN DATE '2026-04-01' AND DATE '2026-04-30'
GROUP BY 1 ORDER BY media_pond DESC;

\echo '== nd-03: PM10 en Escuelas Aguirre, por mes de este año (2026-01-01 a 2026-04-30)'
SELECT mes, COUNT(DISTINCT fecha) AS dias,
       ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1) AS media_pond
FROM mediciones_bloques
WHERE contaminante = 'PM10' AND nombre_estacion = 'Escuelas Aguirre'
  AND fecha BETWEEN DATE '2026-01-01' AND DATE '2026-04-30'
GROUP BY 1 ORDER BY 1;

\echo '== nd-04: NO2 en Plaza Elíptica por día, semana pasada (2026-04-20 a 2026-04-26)'
SELECT fecha, dia_semana, ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1) AS media_pond
FROM mediciones_bloques
WHERE contaminante = 'NO2' AND nombre_estacion = 'Plaza Elíptica'
  AND fecha BETWEEN DATE '2026-04-20' AND DATE '2026-04-26'
GROUP BY 1, 2 ORDER BY media_pond DESC;

\echo '== nd-05: O3, bloque tarde frente al resto del día, por estación (histórico)'
WITH d AS (
    SELECT nombre_estacion, distrito,
           SUM(CASE WHEN bloque = 'tarde' THEN media * n_horas END)
             / NULLIF(SUM(CASE WHEN bloque = 'tarde' THEN n_horas END), 0) AS tarde,
           SUM(CASE WHEN bloque <> 'tarde' THEN media * n_horas END)
             / NULLIF(SUM(CASE WHEN bloque <> 'tarde' THEN n_horas END), 0) AS resto
    FROM mediciones_bloques WHERE contaminante = 'O3' GROUP BY 1, 2)
SELECT nombre_estacion, distrito, ROUND(tarde::numeric, 1) AS tarde, ROUND(resto::numeric, 1) AS resto,
       ROUND((tarde - resto)::numeric, 1) AS diferencia
FROM d ORDER BY tarde - resto DESC;

\echo '== nd-06: NO2 en Plaza Castilla, laborables frente a fin de semana, año pasado (2025)'
WITH d AS (
    SELECT SUM(CASE WHEN NOT es_fin_semana THEN media * n_horas END)
             / NULLIF(SUM(CASE WHEN NOT es_fin_semana THEN n_horas END), 0) AS lab,
           SUM(CASE WHEN es_fin_semana THEN media * n_horas END)
             / NULLIF(SUM(CASE WHEN es_fin_semana THEN n_horas END), 0) AS finde
    FROM mediciones_bloques
    WHERE contaminante = 'NO2' AND nombre_estacion = 'Plaza Castilla'
      AND fecha BETWEEN DATE '2025-01-01' AND DATE '2025-12-31')
SELECT ROUND(lab::numeric, 1) AS laborables, ROUND(finde::numeric, 1) AS fin_de_semana,
       ROUND((lab - finde)::numeric, 1) AS diferencia, ROUND((100 * (lab - finde) / lab)::numeric, 1) AS pct
FROM d;

\echo '== nd-07: días con media diaria de NO2 > 40 en Plaza Elíptica, últimos 12 meses (2025-05-01 a 2026-04-30)'
WITH dia AS (
    SELECT fecha, SUM(media * n_horas) / NULLIF(SUM(n_horas), 0) AS media_dia
    FROM mediciones_bloques
    WHERE contaminante = 'NO2' AND nombre_estacion = 'Plaza Elíptica'
      AND fecha BETWEEN DATE '2025-05-01' AND DATE '2026-04-30'
    GROUP BY 1)
SELECT SUM(CASE WHEN media_dia > 40 THEN 1 ELSE 0 END) AS dias_superados, COUNT(*) AS dias_con_dato,
       SUM(CASE WHEN media_dia > 25 THEN 1 ELSE 0 END) AS dias_sobre_25
FROM dia;

\echo '== nd-08: bloques anómalos en Escuelas Aguirre, marzo de 2026'
SELECT contaminante, COUNT(*) AS bloques, COUNT(DISTINCT fecha) AS dias,
       string_agg(fecha::text || ' ' || bloque, ', ' ORDER BY fecha, hora_inicio) AS detalle
FROM mediciones_bloques
WHERE nombre_estacion = 'Escuelas Aguirre' AND is_anomaly AND cobertura >= 0.7
  AND fecha BETWEEN DATE '2026-03-01' AND DATE '2026-03-31'
GROUP BY ROLLUP (contaminante) ORDER BY contaminante NULLS LAST;

\echo '== nd-09: contaminantes medidos en Farolillo'
SELECT contaminante, COUNT(DISTINCT fecha) AS dias FROM mediciones_bloques
WHERE nombre_estacion = 'Farolillo' GROUP BY 1 ORDER BY 1;

\echo '== nd-10: PM10 en Madrid (todas las estaciones), verano pasado (2025-06-01 a 2025-08-31)'
SELECT COUNT(DISTINCT nombre_estacion) AS estaciones,
       ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1) AS media_pond
FROM mediciones_bloques
WHERE contaminante = 'PM10' AND fecha BETWEEN DATE '2025-06-01' AND DATE '2025-08-31';

\echo '== nd-11: NO2 en Castellana y Plaza de España, este año (2026-01-01 a 2026-04-30)'
SELECT nombre_estacion, COUNT(DISTINCT fecha) AS dias,
       ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1) AS media_pond
FROM mediciones_bloques
WHERE contaminante = 'NO2' AND nombre_estacion IN ('Castellana', 'Plaza de España')
  AND fecha BETWEEN DATE '2026-01-01' AND DATE '2026-04-30'
GROUP BY 1 ORDER BY media_pond DESC;

\echo '== nd-12: NO2 por estación, marzo de 2026, de más a menos (los 3 primeros)'
SELECT nombre_estacion, distrito,
       ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1) AS media_pond
FROM mediciones_bloques
WHERE contaminante = 'NO2' AND fecha BETWEEN DATE '2026-03-01' AND DATE '2026-03-31'
GROUP BY 1, 2 ORDER BY media_pond DESC LIMIT 3;

\echo '== nd-13: máximo horario de NO2 del año pasado (2025), con estación, día y bloque'
SELECT nombre_estacion, distrito, fecha, bloque, maximo, ROUND(media::numeric, 1) AS media_bloque
FROM mediciones_bloques
WHERE contaminante = 'NO2' AND fecha BETWEEN DATE '2025-01-01' AND DATE '2025-12-31'
ORDER BY maximo DESC LIMIT 3;

\echo '== nd-14: NO2 en Madrid por bloque horario (histórico)'
SELECT bloque, ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1) AS media_pond
FROM mediciones_bloques WHERE contaminante = 'NO2'
GROUP BY 1 ORDER BY media_pond DESC;

\echo '== nd-15: NO2 en Madrid por día de la semana (histórico; 0 = lunes)'
SELECT dia_semana, ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1) AS media_pond
FROM mediciones_bloques WHERE contaminante = 'NO2'
GROUP BY 1 ORDER BY media_pond DESC;

\echo '== nd-16: O3 por estación, verano 2024 frente a verano 2025 (junio a agosto)'
WITH d AS (
    SELECT nombre_estacion, distrito,
           SUM(CASE WHEN ano = 2024 THEN media * n_horas END)
             / NULLIF(SUM(CASE WHEN ano = 2024 THEN n_horas END), 0) AS v2024,
           SUM(CASE WHEN ano = 2025 THEN media * n_horas END)
             / NULLIF(SUM(CASE WHEN ano = 2025 THEN n_horas END), 0) AS v2025
    FROM mediciones_bloques
    WHERE contaminante = 'O3' AND mes IN (6, 7, 8) AND ano IN (2024, 2025)
    GROUP BY 1, 2)
SELECT nombre_estacion, distrito, ROUND(v2024::numeric, 1) AS v2024, ROUND(v2025::numeric, 1) AS v2025,
       ROUND((v2025 - v2024)::numeric, 1) AS diferencia
FROM d ORDER BY v2025 - v2024 DESC;

\echo '== nd-17: porcentaje de bloques anómalos por estación, último año (2025-05-01 a 2026-04-30)'
SELECT nombre_estacion, COUNT(*) AS bloques,
       SUM(CASE WHEN is_anomaly AND cobertura >= 0.7 THEN 1 ELSE 0 END) AS anomalos,
       ROUND((100.0 * SUM(CASE WHEN is_anomaly AND cobertura >= 0.7 THEN 1 ELSE 0 END) / COUNT(*))::numeric, 2) AS pct
FROM mediciones_bloques
WHERE fecha BETWEEN DATE '2025-05-01' AND DATE '2026-04-30'
GROUP BY 1 ORDER BY pct DESC;

\echo '== nd-18: estaciones con media anual de NO2 > 40 en 2025 (y las 3 más altas)'
SELECT nombre_estacion, distrito, COUNT(DISTINCT fecha) AS dias,
       ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1) AS media_pond
FROM mediciones_bloques
WHERE contaminante = 'NO2' AND fecha BETWEEN DATE '2025-01-01' AND DATE '2025-12-31'
GROUP BY 1, 2 ORDER BY media_pond DESC LIMIT 3;

\echo '== nd-19: NO2 en Madrid, invierno pasado: meteorológico (2025-12-01 a 2026-02-28) y astronómico (2025-12-21 a 2026-03-20)'
SELECT 'meteorologico' AS definicion,
       ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1) AS media_pond
FROM mediciones_bloques
WHERE contaminante = 'NO2' AND fecha BETWEEN DATE '2025-12-01' AND DATE '2026-02-28'
UNION ALL
SELECT 'astronomico',
       ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1)
FROM mediciones_bloques
WHERE contaminante = 'NO2' AND fecha BETWEEN DATE '2025-12-21' AND DATE '2026-03-20'
UNION ALL
SELECT 'todo el historico',
       ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1)
FROM mediciones_bloques WHERE contaminante = 'NO2';

\echo '== nd-19b: NO2 en Madrid por mes del invierno pasado'
SELECT ano, mes, ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1) AS media_pond
FROM mediciones_bloques
WHERE contaminante = 'NO2' AND fecha BETWEEN DATE '2025-12-01' AND DATE '2026-02-28'
GROUP BY 1, 2 ORDER BY 1, 2;

\echo '== nd-20: PM10 en las estaciones con «Vallecas», mes pasado (2026-04-01 a 2026-04-30)'
SELECT nombre_estacion, distrito, COUNT(DISTINCT fecha) AS dias,
       ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1) AS media_pond
FROM mediciones_bloques
WHERE contaminante = 'PM10' AND (nombre_estacion ILIKE '%vallecas%' OR distrito ILIKE '%vallecas%')
  AND fecha BETWEEN DATE '2026-04-01' AND DATE '2026-04-30'
GROUP BY 1, 2 ORDER BY 1;

\echo '== nm-01: O3 en Casa de Campo, bloque mañana, esta semana (2026-04-24 a 2026-04-30)'
SELECT fecha, ROUND(media::numeric, 1) AS media_manana, maximo
FROM mediciones_bloques
WHERE contaminante = 'O3' AND nombre_estacion = 'Casa de Campo' AND bloque = 'manana'
  AND fecha BETWEEN DATE '2026-04-24' AND DATE '2026-04-30'
ORDER BY fecha;
SELECT 'manana' AS ambito, ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1) AS media_pond,
       MAX(maximo) AS maximo_horario
FROM mediciones_bloques
WHERE contaminante = 'O3' AND nombre_estacion = 'Casa de Campo' AND bloque = 'manana'
  AND fecha BETWEEN DATE '2026-04-24' AND DATE '2026-04-30'
UNION ALL
SELECT 'dia completo', ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1), MAX(maximo)
FROM mediciones_bloques
WHERE contaminante = 'O3' AND nombre_estacion = 'Casa de Campo'
  AND fecha BETWEEN DATE '2026-04-24' AND DATE '2026-04-30';

\echo '== nm-02: NO2 en Plaza Elíptica, último mes (2026-04-01 a 2026-04-30)'
WITH dia AS (
    SELECT fecha, SUM(media * n_horas) / NULLIF(SUM(n_horas), 0) AS media_dia
    FROM mediciones_bloques
    WHERE contaminante = 'NO2' AND nombre_estacion = 'Plaza Elíptica'
      AND fecha BETWEEN DATE '2026-04-01' AND DATE '2026-04-30'
    GROUP BY 1)
SELECT COUNT(*) AS dias, ROUND(MIN(media_dia)::numeric, 1) AS min_dia, ROUND(MAX(media_dia)::numeric, 1) AS max_dia,
       SUM(CASE WHEN media_dia > 25 THEN 1 ELSE 0 END) AS dias_sobre_25
FROM dia;
SELECT ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1) AS media_pond, MAX(maximo) AS maximo_horario
FROM mediciones_bloques
WHERE contaminante = 'NO2' AND nombre_estacion = 'Plaza Elíptica'
  AND fecha BETWEEN DATE '2026-04-01' AND DATE '2026-04-30';

\echo '== nm-03: PM2.5 en Madrid, año pasado (2025), en conjunto y por estación'
SELECT 'Madrid' AS ambito, COUNT(DISTINCT nombre_estacion) AS estaciones,
       ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1) AS media_pond
FROM mediciones_bloques
WHERE contaminante = 'PM2.5' AND fecha BETWEEN DATE '2025-01-01' AND DATE '2025-12-31';
SELECT nombre_estacion, ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1) AS media_pond
FROM mediciones_bloques
WHERE contaminante = 'PM2.5' AND fecha BETWEEN DATE '2025-01-01' AND DATE '2025-12-31'
GROUP BY 1 ORDER BY media_pond;

\echo '== nl-01: NO2 en enero de 2023 (fuera de la vista: 0 filas)'
SELECT COUNT(*) AS filas FROM mediciones_bloques WHERE fecha BETWEEN DATE '2023-01-01' AND DATE '2023-01-31';

\echo '== nl-03: estaciones en Getafe (0 filas)'
SELECT COUNT(*) AS filas FROM mediciones_bloques WHERE nombre_estacion ILIKE '%getafe%' OR distrito ILIKE '%getafe%';

\echo '== nl-04: este mes (mayo de 2026, literal; 0 filas)'
SELECT COUNT(*) AS filas FROM mediciones_bloques WHERE fecha BETWEEN DATE '2026-05-01' AND DATE '2026-05-31';

\echo '== nl-05: O3 en Plaza de España (no lo mide: 0 filas en todo el histórico)'
SELECT COUNT(*) AS filas FROM mediciones_bloques WHERE nombre_estacion = 'Plaza de España' AND contaminante = 'O3';
