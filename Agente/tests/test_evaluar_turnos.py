"""Evaluador de turnos y de conversaciones con el LLM falso: marca lo que no cumple lo esperado."""
import httpx
import pytest

from agente.business.bucle import Bucle
from agente.llm.falso import LLMFalso, texto
from agente.tools.rag import HerramientaRag
from evaluacion.evaluar_turnos import evaluar_caso, evaluar_conversacion
from tests.conftest import RagFingido

pytestmark = pytest.mark.anyio


async def test_marca_el_caso_que_no_cumple_lo_esperado():
    caso = {"id": "doc", "pregunta": "¿Qué efectos tiene el NO2 en el asma?",
            "esperado": {"intencion": "DOCUMENTAL", "ruta": "documental", "busqueda": True, "cita": True}}
    clasificador = LLMFalso(guion=[texto("intencion: CHARLA\ntema: ninguno")])  # se equivoca
    llm = LLMFalso(guion=[texto("¡Hola! Pregúntame por la calidad del aire.")])
    rag = HerramientaRag("http://rag:8010", transport=httpx.MockTransport(RagFingido()))

    rc = await evaluar_caso(Bucle(llm, [rag], llm_clasificador=clasificador), caso)

    assert not rc.cumple
    assert rc.fallos == ["intencion: esperado DOCUMENTAL, obtenido CHARLA", "ruta: esperado documental, obtenido libre",
                         "busqueda: esperado sí, obtenido no", "cita: esperado sí, obtenido no"]


async def test_conversacion_marca_el_turno_que_no_cumple():
    conversacion = {"id": "con", "turnos": [
        {"pregunta": "Hola", "esperado": {"intencion": "CHARLA", "ruta": "libre", "busqueda": False}},
        {"pregunta": "¿Y en niños?", "esperado": {"intencion": "DOCUMENTAL", "ruta": "documental"}},
    ]}
    clasificador = LLMFalso(guion=[texto("intencion: CHARLA\ntema: ninguno"),
                                   texto("intencion: CHARLA\ntema: ninguno")])  # el segundo se equivoca
    llm = LLMFalso(guion=[texto("¡Hola!"), texto("Pregúntame por la calidad del aire.")])
    rag = HerramientaRag("http://rag:8010", transport=httpx.MockTransport(RagFingido()))

    primero, segundo = await evaluar_conversacion(Bucle(llm, [rag], llm_clasificador=clasificador), conversacion)

    assert (primero.caso["id"], primero.cumple) == ("con.1", True)
    assert segundo.caso["id"] == "con.2"
    assert segundo.fallos == ["intencion: esperado DOCUMENTAL, obtenido CHARLA",
                              "ruta: esperado documental, obtenido libre"]
    assert "Usuario: Hola" in clasificador.registro[1]["mensajes"][-1].content  # misma sesión, con memoria
