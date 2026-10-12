"""`consultar_datos` sobre SQLite con un redactor falso."""
import pytest

from agente.llm.falso import texto
from tests.conftest import herramienta_datos

pytestmark = pytest.mark.anyio

PREGUNTA = "¿Qué estación tuvo más NO2 el 30 de abril de 2026?"
PREGUNTA_USUARIO = "¿Qué estación tuvo más NO2 ayer?"
SQL_VALIDO = ("SELECT nombre_estacion, media FROM mediciones_bloques "
              "WHERE fecha = '2026-04-30' AND contaminante = 'NO2' ORDER BY media DESC")


async def test_sql_rechazado_se_reintenta_con_el_error(bd_mediciones, spans):
    llm_sql, herramienta = herramienta_datos(bd_mediciones, [texto("SELECT * FROM resumen_datos_ml"),
                                                             texto(SQL_VALIDO)])
    r = await herramienta.ejecutar({"pregunta": PREGUNTA, "pregunta_usuario": PREGUNTA_USUARIO})

    assert r.ok and r.internos["intentos"] == 2 and r.internos["sql"].endswith("LIMIT 61")
    assert r.datos["filas"][0] == ["Plaza Elíptica", 45.0] and "sql" not in r.datos
    assert [f.tipo for f in r.fuentes] == ["sql"] and r.datos["periodo"] == "«ayer»: el 2026-04-30"
    # El redactor recibe la pregunta del usuario y el periodo calculado en Python, no solo la del modelo
    mensaje = llm_sql.registro[0]["mensajes"][-1].content
    assert PREGUNTA_USUARIO in mensaje and PREGUNTA in mensaje and "«ayer» → fecha = '2026-04-30'" in mensaje
    # El segundo intento lleva el SQL anterior y el error del validador
    reintento = llm_sql.registro[1]["mensajes"][-1].content
    assert "SELECT * FROM resumen_datos_ml" in reintento and "Solo se puede consultar la vista" in reintento
    eventos = [e.name for s in spans.get_finished_spans() if s.name == "redactar_sql" for e in s.events]
    assert eventos == ["sql_invalido"]


async def test_resultado_con_mas_filas_que_el_tope_avisa_de_truncado(bd_mediciones, spans):
    # El modelo pone LIMIT igual al tope: antes el corte llegaba como datos completos
    sql = "SELECT fecha, nombre_estacion, media FROM mediciones_bloques ORDER BY fecha LIMIT 3"
    _, herramienta = herramienta_datos(bd_mediciones, [texto(sql)], max_filas=3)
    r = await herramienta.ejecutar({"pregunta": PREGUNTA})

    assert r.ok and len(r.datos["filas"]) == 3 and r.datos["truncado"] is True


async def test_sin_periodo_en_la_pregunta_se_dice_todo_el_historico(bd_mediciones, spans):
    # Sin periodo, la síntesis se inventaba la ventana («últimos 12 meses» con los 2 años)
    sql = "SELECT nombre_estacion, media FROM mediciones_bloques WHERE contaminante = 'NO2'"
    _, herramienta = herramienta_datos(bd_mediciones, [texto(sql)])
    r = await herramienta.ejecutar({"pregunta": "¿Qué estación tiene más NO2?"})

    assert r.ok and r.datos["periodo"] == "todo el histórico: del 2024-04-30 al 2026-04-30"


async def test_periodo_fuera_de_las_mediciones_responde_sin_datos_sin_redactor(bd_mediciones, spans):
    llm_sql, herramienta = herramienta_datos(bd_mediciones, [])
    r = await herramienta.ejecutar({"pregunta": "¿Cuántas anomalías hubo en 2019?"})

    assert not r.ok and r.internos["estado"] == "sin_datos" and llm_sql.registro == []
    assert r.internos["frase"] == ("No hay mediciones de ese periodo («2019»: del 2019-01-01 al 2019-12-31). "
                                   "Las mediciones disponibles van del 2024-04-30 al 2026-04-30.")
