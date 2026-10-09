"""Validador del SQL del redactor: una regla por caso, sin base de datos."""
from datetime import date

import pytest

from agente.business.validar_sql import SQLNoValido, validar
from agente.entities.datos import Periodo


@pytest.mark.parametrize("sql, motivo", [
    ("SELECT media FROM mediciones_bloques; SELECT 1", "exactamente una sentencia"),
    ("UPDATE mediciones_bloques SET media = 0", "Solo se permiten consultas SELECT"),
    ("WITH d AS (DELETE FROM mediciones_bloques RETURNING *) SELECT * FROM d", "no permitida"),
    ("SELECT nombre FROM estaciones", "Solo se puede consultar la vista"),
    ("SELECT pg_sleep(10) FROM mediciones_bloques", "Función no permitida"),
])
def test_sql_fuera_de_las_reglas_se_rechaza(sql, motivo):
    with pytest.raises(SQLNoValido, match=motivo):
        validar(sql, 60)


# En el segundo lote salía «Hortaleza 12,1»: la media de NO2, NOx, O3... juntos
def test_media_que_mezcla_contaminantes_se_rechaza():
    media = "SUM(media * n_horas) / SUM(n_horas)"
    with pytest.raises(SQLNoValido, match="mezclar contaminantes"):
        validar(f"SELECT distrito, {media} FROM mediciones_bloques "
                "WHERE contaminante IN ('NO2', 'PM10') GROUP BY distrito", 60)
    # Agrupada por contaminante, o con uno solo filtrado en la CTE de la que lee, es válida
    validar(f"SELECT contaminante, distrito, {media} FROM mediciones_bloques "
            "WHERE contaminante IN ('NO2', 'PM10') GROUP BY contaminante, distrito", 60)
    validar(f"WITH b AS (SELECT * FROM mediciones_bloques WHERE contaminante = 'O3') "
            f"SELECT distrito, {media} FROM b GROUP BY distrito", 60)


# Si falta o llega al tope: tope + 1, para que el DAL detecte el truncado
@pytest.mark.parametrize("limite, impuesto", [("", 61), (" LIMIT 500", 61), (" LIMIT 60", 61), (" LIMIT 5", 5)])
def test_limit_se_impone(limite, impuesto):
    assert validar(f"SELECT media FROM mediciones_bloques{limite}", 60).endswith(f"LIMIT {impuesto}")


# Periodo calculado «la semana pasada»: la fecha de fuera y el «hoy» de PostgreSQL se rechazan
@pytest.mark.parametrize("condicion, motivo", [
    ("fecha BETWEEN '2026-04-20' AND '2026-04-28'", "fuera del periodo"),
    ("fecha >= CURRENT_DATE - INTERVAL '7 days'", "fecha actual"),
])
def test_fechas_fuera_del_periodo_calculado_se_rechazan(condicion, motivo):
    semana = Periodo("la semana pasada", ((date(2026, 4, 20), date(2026, 4, 26)),))
    with pytest.raises(SQLNoValido, match=motivo) as error:
        validar(f"SELECT media FROM mediciones_bloques WHERE {condicion}", 60, [semana])
    assert "fecha BETWEEN '2026-04-20' AND '2026-04-26'" in str(error.value)  # el error dice el periodo
    # El extremo abierto (un día de margen) es válido
    validar("SELECT media FROM mediciones_bloques WHERE fecha >= '2026-04-20' AND fecha < '2026-04-27'", 60, [semana])
