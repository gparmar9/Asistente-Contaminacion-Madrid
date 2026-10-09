-- Gold de `casos_datos.json`: la consulta de referencia de cada caso, escrita a mano.
--
-- Solo lee la vista `mediciones_bloques`, como la herramienta. Fechas fijas respecto a
-- FECHA_REFERENCIA=2026-05-01 (viernes) sobre el Postgres local cargado con --desde 2024-04-01:
-- último día disponible 2026-04-30; la vista empieza el 2024-04-30.
-- Toda media es ponderada por horas: SUM(media * n_horas) / SUM(n_horas), nunca AVG(media).
--
-- Los resultados se copian a mano en el campo `gold` de cada caso y se revisan por persona.
--
--   docker exec -i jupiter_postgres psql -U postgres < Agente/evaluacion/gold_datos.sql

\pset footer off

\echo '== dat-01: peor estación el último día (2026-04-30), un ranking por contaminante'
WITH d AS (
    SELECT contaminante, nombre_estacion, distrito, SUM(n_horas) AS horas,
           ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1) AS media_pond
    FROM mediciones_bloques
    WHERE fecha = DATE '2026-04-30' AND contaminante IN ('NO2', 'PM10')
    GROUP BY 1, 2, 3)
SELECT * FROM (SELECT d.*, RANK() OVER (PARTITION BY contaminante ORDER BY media_pond DESC) AS puesto FROM d) x
WHERE puesto <= 3 ORDER BY contaminante, puesto;

\echo '== dat-02: mejor estación el último día (2026-04-30), orden inverso'
WITH d AS (
    SELECT contaminante, nombre_estacion, distrito, SUM(n_horas) AS horas,
           ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1) AS media_pond
    FROM mediciones_bloques
    WHERE fecha = DATE '2026-04-30' AND contaminante IN ('NO2', 'PM10')
    GROUP BY 1, 2, 3)
SELECT * FROM (SELECT d.*, RANK() OVER (PARTITION BY contaminante ORDER BY media_pond ASC) AS puesto FROM d) x
WHERE puesto <= 3 ORDER BY contaminante, puesto;

\echo '== dat-03: NO2 en Parque del Retiro, últimos 7 días disponibles (2026-04-24 a 2026-04-30)'
SELECT fecha, SUM(n_horas) AS horas,
       ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1) AS media_pond
FROM mediciones_bloques
WHERE nombre_estacion = 'Parque del Retiro' AND contaminante = 'NO2'
  AND fecha BETWEEN DATE '2026-04-24' AND DATE '2026-04-30'
GROUP BY ROLLUP (fecha) ORDER BY fecha NULLS LAST;

\echo '== dat-04: mejor distrito en el mes natural anterior (abril de 2026), un ranking por contaminante'
WITH d AS (
    SELECT contaminante, distrito, COUNT(DISTINCT estacion) AS estaciones, COUNT(DISTINCT fecha) AS dias,
           ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1) AS media_pond
    FROM mediciones_bloques
    WHERE fecha BETWEEN DATE '2026-04-01' AND DATE '2026-04-30' AND contaminante IN ('NO2', 'PM10')
    GROUP BY 1, 2)
SELECT * FROM (SELECT d.*, RANK() OVER (PARTITION BY contaminante ORDER BY media_pond ASC) AS puesto FROM d) x
WHERE puesto <= 3 ORDER BY contaminante, puesto;

\echo '== dat-05: NO2 del bloque manana (7 a 12) frente al resto del día, por estación, 2 años'
WITH d AS (
    SELECT nombre_estacion, distrito,
           SUM(media * n_horas) FILTER (WHERE bloque = 'manana') / NULLIF(SUM(n_horas) FILTER (WHERE bloque = 'manana'), 0) AS manana,
           SUM(media * n_horas) FILTER (WHERE bloque <> 'manana') / NULLIF(SUM(n_horas) FILTER (WHERE bloque <> 'manana'), 0) AS resto
    FROM mediciones_bloques
    WHERE contaminante = 'NO2'
    GROUP BY 1, 2)
SELECT nombre_estacion, distrito, ROUND(manana::numeric, 1) AS manana, ROUND(resto::numeric, 1) AS resto,
       ROUND((manana - resto)::numeric, 1) AS diferencia, ROUND((manana / resto)::numeric, 2) AS cociente,
       RANK() OVER (ORDER BY manana - resto DESC) AS puesto_dif,
       RANK() OVER (ORDER BY manana / resto DESC) AS puesto_cociente
FROM d ORDER BY puesto_dif LIMIT 5;

\echo '== dat-06: laborables frente a fin de semana por distrito, 2 años (diferencia = laborable - fin de semana)'
WITH d AS (
    SELECT contaminante, distrito,
           SUM(media * n_horas) FILTER (WHERE NOT es_fin_semana) / NULLIF(SUM(n_horas) FILTER (WHERE NOT es_fin_semana), 0) AS laborable,
           SUM(media * n_horas) FILTER (WHERE es_fin_semana) / NULLIF(SUM(n_horas) FILTER (WHERE es_fin_semana), 0) AS fin_semana
    FROM mediciones_bloques
    WHERE contaminante IN ('NO2', 'PM10')
    GROUP BY 1, 2)
SELECT contaminante, distrito, ROUND(laborable::numeric, 1) AS laborable, ROUND(fin_semana::numeric, 1) AS fin_semana,
       ROUND((laborable - fin_semana)::numeric, 1) AS diferencia,
       ROUND((100 * (laborable - fin_semana) / laborable)::numeric, 0) AS pct
FROM d ORDER BY contaminante, diferencia DESC;

\echo '== dat-07: O3 en Madrid, junio a agosto de cada año'
SELECT ano, COUNT(DISTINCT estacion) AS estaciones, COUNT(DISTINCT fecha) AS dias,
       ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1) AS media_pond
FROM mediciones_bloques
WHERE contaminante = 'O3' AND mes BETWEEN 6 AND 8
GROUP BY ano ORDER BY ano;

\echo '== dat-08: días de 2025 con media diaria ponderada de PM10 > 50 en Plaza de España'
WITH dia AS (
    SELECT fecha, SUM(media * n_horas) / NULLIF(SUM(n_horas), 0) AS media_dia, MAX(maximo) AS maximo_dia
    FROM mediciones_bloques
    WHERE nombre_estacion = 'Plaza de España' AND contaminante = 'PM10'
      AND fecha BETWEEN DATE '2025-01-01' AND DATE '2025-12-31'
    GROUP BY fecha)
SELECT COUNT(*) FILTER (WHERE media_dia > 50) AS dias_media_sobre_50,
       COUNT(*) AS dias_con_dato,
       COUNT(*) FILTER (WHERE maximo_dia > 50) AS dias_maximo_horario_sobre_50  -- respuesta incorrecta típica
FROM dia;

\echo '== dat-09: bloques anómalos con cobertura >= 0,7 la semana natural anterior (lunes 2026-04-20 a domingo 2026-04-26)'
SELECT nombre_estacion, COUNT(*) AS bloques, string_agg(DISTINCT contaminante, ', ' ORDER BY contaminante) AS contaminantes
FROM mediciones_bloques
WHERE is_anomaly AND cobertura >= 0.7 AND fecha BETWEEN DATE '2026-04-20' AND DATE '2026-04-26'
GROUP BY 1 ORDER BY bloques DESC, nombre_estacion;

\echo '== dat-09 (variante): últimos 7 días disponibles (2026-04-24 a 2026-04-30)'
SELECT nombre_estacion, COUNT(*) AS bloques, string_agg(DISTINCT contaminante, ', ' ORDER BY contaminante) AS contaminantes
FROM mediciones_bloques
WHERE is_anomaly AND cobertura >= 0.7 AND fecha BETWEEN DATE '2026-04-24' AND DATE '2026-04-30'
GROUP BY 1 ORDER BY bloques DESC, nombre_estacion;

\echo '== dat-10: O3 en julio (2024 y 2025 juntos) por distrito, menor primero'
WITH d AS (
    SELECT distrito, COUNT(DISTINCT estacion) AS estaciones, COUNT(DISTINCT fecha) AS dias,
           SUM(media * n_horas) / NULLIF(SUM(n_horas), 0) AS julios,
           SUM(media * n_horas) FILTER (WHERE ano = 2024) / NULLIF(SUM(n_horas) FILTER (WHERE ano = 2024), 0) AS julio_2024,
           SUM(media * n_horas) FILTER (WHERE ano = 2025) / NULLIF(SUM(n_horas) FILTER (WHERE ano = 2025), 0) AS julio_2025
    FROM mediciones_bloques
    WHERE contaminante = 'O3' AND mes = 7
    GROUP BY 1)
SELECT distrito, estaciones, dias, ROUND(julios::numeric, 1) AS julios, ROUND(julio_2024::numeric, 1) AS julio_2024,
       ROUND(julio_2025::numeric, 1) AS julio_2025
FROM d ORDER BY julios;

\echo '== dat-11: estaciones con datos de PM2.5 en los 2 años'
SELECT nombre_estacion, distrito, COUNT(DISTINCT fecha) AS dias
FROM mediciones_bloques
WHERE contaminante = 'PM2.5'
GROUP BY 1, 2 ORDER BY 1;

\echo '== mix-01: bloque tarde (13 a 19) del último día (2026-04-30); O3 solo informativo'
SELECT contaminante, COUNT(DISTINCT estacion) AS estaciones,
       ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1) AS media_madrid,
       ROUND(MAX(media)::numeric, 1) AS media_estacion_max, ROUND(MAX(maximo)::numeric, 1) AS maximo_horario
FROM mediciones_bloques
WHERE fecha = DATE '2026-04-30' AND bloque = 'tarde' AND contaminante IN ('NO2', 'PM10', 'O3')
GROUP BY 1 ORDER BY 1;

\echo '== mix-02: media anual por distrito, últimos 12 meses (2025-05-01 a 2026-04-30), menor primero'
WITH d AS (
    SELECT contaminante, distrito, COUNT(DISTINCT estacion) AS estaciones, COUNT(DISTINCT fecha) AS dias,
           ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1) AS media_pond
    FROM mediciones_bloques
    WHERE fecha BETWEEN DATE '2025-05-01' AND DATE '2026-04-30' AND contaminante IN ('NO2', 'PM10')
    GROUP BY 1, 2)
SELECT * FROM (SELECT d.*, RANK() OVER (PARTITION BY contaminante ORDER BY media_pond ASC) AS puesto FROM d) x
WHERE puesto <= 3 ORDER BY contaminante, puesto;

\echo '== mix-02 (variante): todo el histórico (2024-04-30 a 2026-04-30); la pregunta no trae periodo'
WITH d AS (
    SELECT contaminante, distrito, COUNT(DISTINCT estacion) AS estaciones, COUNT(DISTINCT fecha) AS dias,
           ROUND((SUM(media * n_horas) / NULLIF(SUM(n_horas), 0))::numeric, 1) AS media_pond
    FROM mediciones_bloques
    WHERE contaminante IN ('NO2', 'PM10')
    GROUP BY 1, 2)
SELECT * FROM (SELECT d.*, RANK() OVER (PARTITION BY contaminante ORDER BY media_pond ASC) AS puesto FROM d) x
WHERE puesto <= 3 ORDER BY contaminante, puesto;

\echo '== lim-01: filas de 2019 (fuera de los 2 años)'
SELECT COUNT(*) AS filas_2019, (SELECT MIN(fecha) FROM mediciones_bloques) AS primer_dia
FROM mediciones_bloques WHERE ano = 2019;

\echo '== lim-02: SO2 no está entre los contaminantes; Escuelas Aguirre sí tiene datos el último día'
SELECT (SELECT COUNT(*) FROM mediciones_bloques WHERE contaminante ILIKE 'SO2') AS filas_so2,
       (SELECT string_agg(DISTINCT contaminante, ', ' ORDER BY contaminante) FROM mediciones_bloques
         WHERE nombre_estacion = 'Escuelas Aguirre' AND fecha = DATE '2026-04-30') AS medidos_2026_04_30;
