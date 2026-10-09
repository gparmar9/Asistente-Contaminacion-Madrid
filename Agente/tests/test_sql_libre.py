"""`consultar_datos` sobre SQLite con un redactor falso."""
import pytest

from agente.llm.falso import texto
from tests.conftest import herramienta_datos

pytestmark = pytest.mark.anyio

PREGUNTA = "¿Qué estación tuvo más NO2 el 30 de abril de 2026?"
SQL_VALIDO = ("SELECT nombre_estacion, media FROM mediciones_bloques "
              "WHERE fecha = '2026-04-30' AND contaminante = 'NO2' ORDER BY media DESC")


async def test_sql_rechazado_se_reintenta_con_el_error(bd_mediciones, spans):
    llm_sql, herramienta = herramienta_datos(bd_mediciones, [texto("SELECT * FROM resumen_datos_ml"),
                                                             texto(SQL_VALIDO)])
    r = await herramienta.ejecutar({"pregunta": PREGUNTA})

    assert r.ok and r.internos["intentos"] == 2 and r.internos["sql"].endswith("LIMIT 60")
    assert r.datos["filas"][0] == ["Plaza Elíptica", 45.0] and "sql" not in r.datos
    assert [f.tipo for f in r.fuentes] == ["sql"]
    # El segundo intento lleva el SQL anterior y el error del validador
    reintento = llm_sql.registro[1]["mensajes"][-1].content
    assert "SELECT * FROM resumen_datos_ml" in reintento and "Solo se puede consultar la vista" in reintento
    eventos = [e.name for s in spans.get_finished_spans() if s.name == "redactar_sql" for e in s.events]
    assert eventos == ["sql_invalido"]
