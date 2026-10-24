"""Casos de `evaluacion/casos_prediccion.json` de punta a punta con el LLM falso y el servicio fingido.

Clasificador -> bucle -> `predecir_calidad_aire` (del modelo o lanzada por el código) -> servicio
fingido -> síntesis de predicción, o frase fija (fuera de alcance, ya medido, servicio caído, sin
URL). Comprueba el camino, no el acierto del modelo: eso se mide con Bedrock y el gold en el lote.
"""
import json
from pathlib import Path

import httpx
import pytest

from agente.business import frases
from agente.business.bucle import Bucle
from agente.llm.falso import LLMFalso, llamada, texto
from agente.tools import prediccion
from agente.tools.prediccion import HerramientaPrediccion
from tests.conftest import FECHA_REFERENCIA, PrediccionFingida

pytestmark = pytest.mark.anyio

CASOS = {c["id"]: c for c in json.loads(
    (Path(__file__).resolve().parents[1] / "evaluacion" / "casos_prediccion.json").read_text(encoding="utf-8"))}
SINTESIS = "Para mañana, la calidad del aire estimada en Madrid es buena; recuerda que es una estimación."


async def _responder(id_caso, guion, fake=None, con_herramienta=True):
    caso = CASOS[id_caso]
    fake = fake if fake is not None else PrediccionFingida()
    herramientas = ([HerramientaPrediccion("http://prediccion:8011", transport=httpx.MockTransport(fake),
                                            fecha_referencia=FECHA_REFERENCIA)] if con_herramienta else [])
    llm = LLMFalso(guion=guion)
    clasificador = LLMFalso(guion=[texto(f"intencion: {caso['esperado']['intencion']}\ntema: ninguno")])
    r = await Bucle(llm, herramientas, llm_clasificador=clasificador).responder(caso["pregunta"])
    return r, fake, llm


# --------------------------------------------------------------------------- ruta de predicción

async def test_prediccion_cuando_el_modelo_llama_a_la_herramienta():
    guion = [llamada(prediccion.NOMBRE, {}, "c1"), texto("Texto libre que la síntesis sustituye."),
             texto(SINTESIS)]
    r, fake, _ = await _responder("pre-02", guion)
    assert (r.intencion, r.ruta) == ("PREDICCION", "prediccion")
    assert prediccion.NOMBRE in r.herramientas_usadas and not r.prediccion_forzada
    assert fake.peticiones[-1]["fecha_objetivo"] == "2026-05-02"       # mañana (h2)
    assert r.respuesta == SINTESIS and r.hallazgos == []
    assert [f.tipo for f in r.fuentes] == ["prediccion"]


async def test_prediccion_forzada_cuando_el_modelo_no_llama():
    r, fake, _ = await _responder("pre-08", [texto("Texto libre que la síntesis sustituye."), texto(SINTESIS)])
    assert r.ruta == "prediccion" and r.prediccion_forzada
    assert fake.peticiones[-1]["fecha_objetivo"] == "2026-05-01"       # sin expresión → hoy (h1)
    assert r.respuesta == SINTESIS


async def test_sintesis_recibe_el_contexto_y_las_filas_de_referencia():
    r, _, llm = await _responder("pre-02", [texto("libre"), texto(SINTESIS)])
    usuario = llm.registro[-1]["mensajes"][-1].content   # la síntesis va sin herramientas
    assert llm.registro[-1]["herramientas"] == []
    assert "estimación del modelo" in usuario and "2026-04-30" in usuario   # contexto: último día medido
    filas = json.loads(usuario.split("Resultados de las consultas:\n")[1])["filas"]
    assert {f["contaminante"] for f in filas} == {"NO2", "PM10"}            # ciudad → solo de referencia


# --------------------------------------------------------------------------- estados terminales → frase

async def test_fuera_de_alcance_cierra_con_frase():
    r, _, _ = await _responder("pre-04", [texto("Texto libre que la frase sustituye.")])
    assert r.ruta == "fija" and prediccion.NOMBRE in r.herramientas_usadas  # se intentó
    assert r.respuesta == frases.FRASE_PREDICCION_FUERA_DE_ALCANCE.format(hasta="2026-05-02")


async def test_fecha_ya_medida_redirige_a_datos():
    r, _, _ = await _responder("pre-05", [texto("Texto libre que la frase sustituye.")])
    assert r.ruta == "fija" and r.respuesta == frases.FRASE_PREDICCION_YA_MEDIDA


async def test_servicio_caido_da_la_frase_de_no_disponible():
    r, _, llm = await _responder("pre-02", [], fake=PrediccionFingida(caido=True))
    assert r.ruta == "fija" and r.respuesta == frases.FRASE_PREDICCION_NO_DISPONIBLE
    assert llm.registro == []  # ni bucle ni síntesis: la herramienta no se ofrece


async def test_sin_url_mantiene_la_frase_fija_de_siempre():
    """Compatibilidad: sin PREDICCION_URL la herramienta no se registra y el intent da la frase fija."""
    r, _, llm = await _responder("pre-02", [], con_herramienta=False)
    assert r.ruta == "fija" and r.respuesta == frases.FRASE_PREDICCION and llm.registro == []
