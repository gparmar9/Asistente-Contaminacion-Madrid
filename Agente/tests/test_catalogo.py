"""`consultar_datos` con el catálogo cerrado sobre SQLite y un selector falso (fase 5)."""
import json

import pytest

from agente.business import catalogo_sql
from agente.llm.falso import texto
from tests.conftest import herramienta_catalogo

pytestmark = pytest.mark.anyio


def _json(**seleccion) -> texto:
    return texto(json.dumps(seleccion, ensure_ascii=False))


async def test_ranking_con_periodo_calculado_y_contaminantes_de_referencia(bd_mediciones, spans):
    llm, herramienta = herramienta_catalogo(bd_mediciones, [_json(consulta="ranking", contaminantes=[])])
    r = await herramienta.ejecutar({"pregunta": "¿Qué estación tiene la peor calidad del aire?",
                                    "pregunta_usuario": "¿Qué estación tiene la peor calidad del aire ahora mismo?"})

    assert r.ok and r.internos["consulta"] == "ranking" and r.internos["sql"].endswith("LIMIT 61")
    assert r.datos["periodo"] == "«ahora mismo»: el 2026-04-30"
    assert r.datos["columnas"] == ["contaminante", "nombre_estacion", "distrito", "media", "dias_con_dato"]
    # Sin contaminante en la pregunta: NO2 y PM10, una lista por contaminante, peor primero
    assert r.datos["filas"][0] == ["NO2", "Plaza Elíptica", "Carabanchel", 45.0, 1]
    assert r.datos["filas"][3] == ["PM10", "Plaza de España", "Centro", 30.0, 1]
    # El selector recibe la pregunta del usuario y el periodo ya calculado: no pone fechas
    mensaje = llm.registro[0]["mensajes"][-1].content
    assert "ahora mismo?" in mensaje and "el 2026-04-30. Ya calculado" in mensaje


async def test_eleccion_no_valida_se_reintenta_con_el_error(bd_mediciones, spans):
    llm, herramienta = herramienta_catalogo(bd_mediciones, [
        _json(consulta="media_diaria"),
        _json(consulta="dias_superados", contaminantes=["PM10"], estaciones=["plaza de espana"], umbral=50)])
    r = await herramienta.ejecutar({"pregunta": "¿Cuántos días del año pasado superó el PM10 los 50 µg/m³ "
                                                "en Plaza de España?"})

    # Media diaria ponderada > 50 en 2025: 62, 55 y 51,5 (41 no)
    assert r.ok and r.internos["intentos"] == 2
    assert r.datos["filas"] == [["PM10", "Plaza de España", "Centro", 3, 4]]
    assert "'consulta' debe ser uno de" in llm.registro[1]["mensajes"][-1].content
    eventos = [e.name for s in spans.get_finished_spans() if s.name == "elegir_consulta" for e in s.events]
    assert eventos == ["sql_invalido"]


async def test_contaminante_no_medido_responde_sin_consultar(bd_mediciones, spans):
    _, herramienta = herramienta_catalogo(bd_mediciones, [
        _json(consulta="ranking", contaminantes=["SO2"], estaciones=["Plaza Elíptica"])])
    r = await herramienta.ejecutar({"pregunta": "¿Cuál fue el nivel de SO2 ayer en Plaza Elíptica?"})

    assert r.ok and r.datos["filas"] == [] and r.internos["sql"] == ""
    assert r.datos["nota"].startswith("No se miden: SO2.")


@pytest.mark.parametrize("consulta", catalogo_sql.CONSULTAS)
async def test_cada_consulta_del_catalogo_corre_en_sqlite(bd_mediciones, spans, consulta):
    seleccion = {"consulta": consulta, "umbral": 50}
    _, herramienta = herramienta_catalogo(bd_mediciones, [_json(**seleccion)])
    r = await herramienta.ejecutar({"pregunta": "¿Cómo están los niveles?"})

    # La semilla no tiene anomalías ni más bloques que `manana`: basta con que el SQL corra
    assert r.ok and r.datos["columnas"] and r.internos["sql"], r.error
