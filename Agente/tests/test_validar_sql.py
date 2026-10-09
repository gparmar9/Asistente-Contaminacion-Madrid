"""Validador del SQL del redactor: una regla por caso, sin base de datos."""
import pytest

from agente.business.validar_sql import SQLNoValido, validar


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


@pytest.mark.parametrize("limite, impuesto", [("", 60), (" LIMIT 500", 60), (" LIMIT 5", 5)])
def test_limit_se_impone(limite, impuesto):
    assert validar(f"SELECT media FROM mediciones_bloques{limite}", 60).endswith(f"LIMIT {impuesto}")
