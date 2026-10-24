"""`HerramientaPrediccion`: cliente del servicio de predicción, con `prediccion.api` fingida.

La fecha la calcula el código a partir de la pregunta del usuario (nunca el LLM); los estados del
servicio se mapean a `internos`. Sin red: `httpx.MockTransport(PrediccionFingida())`.
"""
from datetime import date

import httpx
import pytest

from agente.tools import prediccion
from agente.tools.prediccion import HerramientaPrediccion
from agente.tools.sql_libre import ARG_PREGUNTA_USUARIO, ARG_PREGUNTAS_PREVIAS
from tests.conftest import PrediccionFingida

pytestmark = pytest.mark.anyio

FECHA_REFERENCIA = date(2026, 5, 1)  # ancla del servicio fingido = 2026-04-30


def _herramienta(fake: PrediccionFingida | None = None) -> tuple[HerramientaPrediccion, PrediccionFingida]:
    fake = fake or PrediccionFingida()
    tool = HerramientaPrediccion("http://prediccion:8011", transport=httpx.MockTransport(fake),
                                 fecha_referencia=FECHA_REFERENCIA)
    return tool, fake


def _args(pregunta: str, **extra) -> dict:
    return {ARG_PREGUNTA_USUARIO: pregunta, **extra}


# --------------------------------------------------------------------------- definición / salud

async def test_definicion_no_disponible_si_el_servicio_esta_caido():
    tool, _ = _herramienta(PrediccionFingida(caido=True))
    assert await tool.definicion() is None


async def test_definicion_y_contexto_tras_salud():
    tool, _ = _herramienta()
    definicion = await tool.definicion()
    assert definicion["function"]["name"] == prediccion.NOMBRE
    contexto = tool.contexto_prompt()
    assert "2026-05-01" in contexto and "2026-04-30" in contexto and "2026-05-02" in contexto


# --------------------------------------------------------------------------- fecha por código

async def test_la_fecha_la_calcula_el_codigo_no_el_llm():
    """El LLM puede mandar un fecha_objetivo; se ignora y se usa el de la pregunta del usuario."""
    tool, fake = _herramienta()
    await tool.ejecutar(_args("¿cómo estará mañana?", fecha_objetivo="2020-01-01"))
    assert fake.peticiones[-1]["fecha_objetivo"] == "2026-05-02"  # hoy+1, no lo del LLM


async def test_sin_expresion_el_defecto_es_hoy():
    tool, fake = _herramienta()
    await tool.ejecutar(_args("¿cómo estará el aire en Retiro?"))
    assert fake.peticiones[-1]["fecha_objetivo"] == "2026-05-01"  # hoy


async def test_hereda_el_periodo_de_una_pregunta_previa():
    tool, fake = _herramienta()
    await tool.ejecutar({ARG_PREGUNTA_USUARIO: "¿y en Vallecas?",
                         ARG_PREGUNTAS_PREVIAS: ["¿cómo estará mañana en el centro?"]})
    assert fake.peticiones[-1]["fecha_objetivo"] == "2026-05-02"


async def test_el_bloque_de_la_expresion_viaja_al_servicio():
    tool, fake = _herramienta()
    await tool.ejecutar(_args("¿y mañana por la tarde?"))
    assert fake.peticiones[-1]["fecha_objetivo"] == "2026-05-02"
    assert fake.peticiones[-1]["bloque"] == "tarde"


# --------------------------------------------------------------------------- entidades y filtros

async def test_estacion_y_contaminante_del_llm_viajan_al_servicio():
    tool, fake = _herramienta()
    r = await tool.ejecutar(_args("¿cómo estará mañana?", estacion="Plaza de España", contaminante="NO2"))
    assert fake.peticiones[-1]["estacion"] == "Plaza de España"
    assert fake.peticiones[-1]["contaminante"] == "NO2"
    assert r.ok and {f["contaminante"] for f in r.datos["filas"]} == {"NO2"}


async def test_sin_contaminante_filtra_a_los_de_referencia():
    """Ciudad entera sin contaminante: solo NO2 y PM10, no O3."""
    tool, _ = _herramienta()
    r = await tool.ejecutar(_args("¿cómo estará mañana?"))
    assert r.ok and {f["contaminante"] for f in r.datos["filas"]} == {"NO2", "PM10"}


# --------------------------------------------------------------------------- estados

async def test_ok_lleva_horizonte_y_fecha_en_internos():
    tool, _ = _herramienta()
    r = await tool.ejecutar(_args("¿cómo estará mañana?"))
    assert r.ok and r.internos["estado"] == "ok"
    assert r.internos["fecha_objetivo"] == "2026-05-02" and r.internos["horizonte"] == 2


async def test_fuera_de_alcance_trae_frase_con_el_limite():
    tool, fake = _herramienta()
    r = await tool.ejecutar(_args("¿cómo estará el 2026-05-05?"))  # fecha ISO más allá del horizonte
    assert fake.peticiones[-1]["fecha_objetivo"] == "2026-05-05"
    assert not r.ok and r.internos["estado"] == "fuera_de_alcance"
    assert "2026-05-02" in r.internos["frase"]


async def test_ya_medido_redirige_a_datos():
    tool, _ = _herramienta()
    r = await tool.ejecutar(_args("¿cómo estuvo el 2026-04-20?"))
    assert not r.ok and r.internos["estado"] == "ya_medido"


async def test_sin_datos_para_estacion_desconocida():
    tool, _ = _herramienta()
    r = await tool.ejecutar(_args("¿cómo estará mañana?", estacion="Marte"))
    assert not r.ok and r.internos["estado"] == "sin_datos"


async def test_servicio_caido_en_la_peticion_es_no_disponible():
    tool, _ = _herramienta(PrediccionFingida(prediccion_caida=True))
    r = await tool.ejecutar(_args("¿cómo estará mañana?"))
    assert not r.ok and r.internos["estado"] == "no_disponible"
