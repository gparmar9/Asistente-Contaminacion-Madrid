"""Cinco casos de `evaluacion/casos_datos.json` de punta a punta con el LLM falso y SQLite.

Clasificador -> bucle -> `consultar_datos` (del modelo o lanzada por el código) -> redactor falso
-> validador -> SQLite -> síntesis de datos. Comprueba el camino, no el acierto del modelo: eso
se mide con Bedrock y el gold en la fase 4.
"""
import json
from pathlib import Path

import pytest

from agente.business import frases
from agente.business.bucle import Bucle
from agente.llm.falso import LLMFalso, llamada, texto
from agente.tools import sql_libre
from tests.conftest import herramienta_datos

pytestmark = pytest.mark.anyio

CASOS = {c["id"]: c for c in json.loads(
    (Path(__file__).resolve().parents[1] / "evaluacion" / "casos_datos.json").read_text(encoding="utf-8"))}
CABECERA_RESULTADOS = "Resultados de las consultas:\n"

# id, ¿consulta el modelo? (si no, la lanza el código), SQL del redactor, primera fila, síntesis
GUIONES = [
    ("dat-01", True,
     "SELECT contaminante, nombre_estacion, SUM(media * n_horas) / SUM(n_horas) AS media, "
     "COUNT(DISTINCT fecha) AS dias_con_dato FROM mediciones_bloques WHERE fecha = '2026-04-30' "
     "AND contaminante IN ('NO2', 'PM10') GROUP BY contaminante, nombre_estacion ORDER BY contaminante, media DESC",
     ["NO2", "Plaza Elíptica", 45.0, 1],
     "Con datos del 30 de abril de 2026, último día disponible, y con el NO2 y el PM10 como "
     "contaminantes de referencia: en NO2 la peor estación fue Plaza Elíptica (45,0 µg/m³) y en PM10, "
     "Plaza de España (30,0 µg/m³). Los dos rankings no coinciden."),
    ("dat-03", False,
     "SELECT fecha, SUM(media * n_horas) / SUM(n_horas) AS media FROM mediciones_bloques "
     "WHERE nombre_estacion LIKE '%Retiro%' AND contaminante = 'NO2' "
     "AND fecha BETWEEN '2026-04-24' AND '2026-04-30' GROUP BY fecha ORDER BY fecha",
     ["2026-04-24", 18.0],
     "Del 24 al 30 de abril de 2026, el NO2 en el Parque del Retiro estuvo entre 18,0 y 25,0 µg/m³."),
    ("dat-08", True,
     "SELECT COUNT(*) AS dias FROM (SELECT fecha FROM mediciones_bloques WHERE nombre_estacion LIKE '%España%' "
     "AND contaminante = 'PM10' AND fecha BETWEEN '2025-01-01' AND '2025-12-31' GROUP BY fecha "
     "HAVING SUM(media * n_horas) / SUM(n_horas) > 50) AS d",
     [3],
     "En 2025, la media diaria de PM10 en Plaza de España superó los 50 µg/m³ en 3 días."),
    ("lim-01", True,
     "SELECT COUNT(DISTINCT fecha) AS dias_con_dato FROM mediciones_bloques WHERE ano = 2019",
     [0],
     "No hay datos de 2019: el asistente consulta las mediciones del 30 de abril de 2024 al 30 de abril de 2026."),
    ("lim-03", False, None, None, None),
]


@pytest.mark.parametrize("id_caso, consulta_el_modelo, sql, primera_fila, sintesis", GUIONES)
async def test_caso_de_datos_con_llm_falso(bd_mediciones, id_caso, consulta_el_modelo, sql, primera_fila, sintesis):
    caso, esperado = CASOS[id_caso], CASOS[id_caso]["esperado"]
    llm_sql, herramienta = herramienta_datos(bd_mediciones, [texto(sql)] if sql else [])
    guion = [] if sintesis is None else [
        *([llamada(sql_libre.NOMBRE, {"pregunta": caso["pregunta"]}, "c1")] if consulta_el_modelo else []),
        texto("Texto libre que la síntesis de datos sustituye."), texto(sintesis)]
    llm = LLMFalso(guion=guion)
    clasificador = LLMFalso(guion=[texto(f"intencion: {esperado['intencion']}\ntema: ninguno")])
    r = await Bucle(llm, [herramienta], llm_clasificador=clasificador).responder(caso["pregunta"])

    assert (r.intencion, r.ruta) == (esperado["intencion"], esperado["ruta"])
    assert (sql_libre.NOMBRE in r.herramientas_usadas) is esperado["datos"]
    if not esperado["datos"]:  # predicción: frase fija, ni modelo ni redactor
        assert r.respuesta == frases.FRASE_PREDICCION and llm.registro == [] and llm_sql.registro == []
        return

    assert r.consulta_forzada is not consulta_el_modelo
    assert llm_sql.registro[0]["mensajes"][-1].content == caso["pregunta"]
    assert "Fecha de hoy: 2026-05-01" in llm.registro[0]["mensajes"][0].content  # prompt del bucle
    # La síntesis va sin herramientas, con las fechas y las filas ya redondeadas
    entrada = llm.registro[-1]
    usuario = entrada["mensajes"][-1].content
    assert entrada["herramientas"] == [] and "el 2026-04-30 es el último día disponible" in usuario
    assert json.loads(usuario.split(CABECERA_RESULTADOS)[1])["filas"][0] == primera_fila
    assert r.respuesta == sintesis and r.hallazgos == []  # sus cifras están en las filas o las fechas
    assert [f.tipo for f in r.fuentes] == ["sql"]
