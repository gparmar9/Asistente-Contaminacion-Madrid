"""El bucle de herramientas con el LLM falso y el RAG fingido."""
import json

import httpx
import pytest
from llama_index.core.base.llms.types import MessageRole

from agente.business import frases
from agente.business.bucle import Bucle
from agente.llm.falso import LLMFalso, llamada, texto
from agente.tools.rag import HerramientaRag
from tests.conftest import EVIDENCIAS_LIMITES, EVIDENCIAS_NO2, SALIDA_NO2, SIN_EVIDENCIA, TEXTO_NO2, RagFingido

pytestmark = pytest.mark.anyio

PREGUNTA = "¿Qué efectos tiene el NO2 en el asma?"
FUENTE_NO2 = {"tipo": "documento", "referencia": "Efectos del NO2 en la salud"}
JSON_NO2 = json.dumps(SALIDA_NO2, ensure_ascii=False)
BUSCAR = llamada("buscar_evidencias", {"pregunta": PREGUNTA, "tema": "salud"}, "c1")
LIBRE = texto("Texto libre que la ruta documental descarta.")


def _bucle(guion, rag: RagFingido, max_vueltas: int = 3):
    llm = LLMFalso(guion=list(guion))
    herramienta = HerramientaRag("http://rag:8010", transport=httpx.MockTransport(rag))
    return llm, Bucle(llm, [herramienta], max_vueltas=max_vueltas)


async def test_cero_llamadas_a_herramienta():
    rag = RagFingido()
    llm, bucle = _bucle([texto("Hola, puedo ayudarte con la calidad del aire.")], rag)
    r = await bucle.responder("Hola")
    assert r.respuesta == "Hola, puedo ayudarte con la calidad del aire."
    assert r.fuentes == [] and rag.busquedas == [] and r.ruta == "libre"
    assert llm.registro[0]["herramientas"] == ["buscar_evidencias"]  # se ofreció, no se usó


async def test_json_valido_entrega_el_texto_del_rag():
    rag = RagFingido()
    llm, bucle = _bucle([BUSCAR, LIBRE, texto(JSON_NO2)], rag)
    r = await bucle.responder(PREGUNTA)

    assert r.ruta == "documental" and r.respuesta == TEXTO_NO2  # passthrough, no el texto libre
    assert [f.model_dump() for f in r.fuentes] == [FUENTE_NO2]
    assert r.advertencia == frases.AVISO_SANITARIO
    # El modelo recibió el resultado como mensaje `tool` casado con su petición
    tool = llm.registro[1]["mensajes"][-1]
    assert tool.role == MessageRole.TOOL and tool.additional_kwargs["tool_call_id"] == "c1"
    # La síntesis va sin herramientas y se valida con los chunk_id de la búsqueda
    assert llm.registro[2]["herramientas"] == []
    assert rag.validaciones[0]["evidencias"] == [{"id": "D1", "chunk_id": "salud_no2:efectos:0"}]


async def test_dos_busquedas_renumeran_las_evidencias():
    rag = RagFingido(respuestas=[EVIDENCIAS_NO2, EVIDENCIAS_LIMITES])
    salida = {**SALIDA_NO2, "afirmaciones": [{"texto": "El límite anual es 40 µg/m³.", "evidencias": ["D2"]}]}
    llm, bucle = _bucle([
        llamada("buscar_evidencias", {"pregunta": "efectos del NO2"}, "c1"),
        llamada("buscar_evidencias", {"pregunta": "límites del NO2"}, "c2"),
        LIBRE, texto(json.dumps(salida, ensure_ascii=False)),
    ], rag)
    r = await bucle.responder(PREGUNTA)

    segunda = json.loads(llm.registro[2]["mensajes"][-1].content)
    assert [e["id"] for e in segunda["evidencias"]] == ["D2"]  # el RAG la numeró D1
    assert rag.validaciones[0]["evidencias"] == [
        {"id": "D1", "chunk_id": "salud_no2:efectos:0"}, {"id": "D2", "chunk_id": "normativa_limites:no2:0"}]
    assert r.respuesta == "El límite anual es 40 µg/m³. [D2]"


async def test_json_invalido_se_repara_una_vez():
    rag = RagFingido()
    llm, bucle = _bucle([BUSCAR, LIBRE, texto("Esto no es JSON"), texto(JSON_NO2)], rag)
    r = await bucle.responder(PREGUNTA)

    assert r.respuesta == TEXTO_NO2 and len(rag.validaciones) == 2
    reparacion = llm.registro[3]["mensajes"][-1]
    assert reparacion.role == MessageRole.USER and reparacion.content == "Corrige el JSON."


async def test_doble_fallo_da_insuficiencia():
    rag = RagFingido()
    llm, bucle = _bucle([BUSCAR, LIBRE, texto("mal"), texto("mal otra vez")], rag)
    r = await bucle.responder(PREGUNTA)
    assert r.respuesta == frases.INSUFICIENCIA
    assert r.fuentes == [] and r.advertencia is None


async def test_sin_evidencia_cierra_el_turno_sin_modelo():
    rag = RagFingido(respuestas=[SIN_EVIDENCIA])
    llm, bucle = _bucle([BUSCAR], rag)
    r = await bucle.responder("¿Cuál es el mejor restaurante de Madrid?")
    assert r.respuesta == frases.SIN_EVIDENCIA and r.ruta == "sin_evidencia"
    assert len(llm.registro) == 1  # solo la llamada que pidió la herramienta


async def test_limite_de_vueltas_cierra_sin_herramientas():
    rag = RagFingido()
    pedir = llamada("buscar_evidencias", {"pregunta": "otra vez"})
    llm, bucle = _bucle([pedir, pedir, pedir, texto(JSON_NO2)], rag, max_vueltas=3)
    r = await bucle.responder(PREGUNTA)
    assert r.respuesta == TEXTO_NO2
    assert [ll["herramientas"] for ll in llm.registro] == [["buscar_evidencias"]] * 3 + [[]]


async def test_rag_caido_no_ofrece_la_herramienta():
    rag = RagFingido(caido=True)
    llm, bucle = _bucle([texto("Sin documentación ahora mismo.")], rag)
    r = await bucle.responder(PREGUNTA)
    assert r.respuesta == "Sin documentación ahora mismo."
    assert llm.registro[0]["herramientas"] == []
