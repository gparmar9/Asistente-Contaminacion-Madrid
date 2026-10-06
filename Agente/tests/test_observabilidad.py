"""Spans del turno con un exportador en memoria: sin red, sin Phoenix."""
import asyncio
import json
from collections import defaultdict

import httpx
import pytest
from openinference.instrumentation import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode, format_trace_id

from agente import observabilidad
from agente.business.bucle import Bucle, LLMNoDisponible
from agente.llm.falso import LLMFalso, llamada, texto
from agente.observabilidad.jsonl import ExportadorJsonl
from agente.tools.rag import HerramientaRag
from tests.conftest import SALIDA_NO2, TEXTO_NO2, RagFingido

pytestmark = pytest.mark.anyio

PREGUNTA = "¿Qué efectos tiene el NO2 en el asma?"
JSON_NO2 = json.dumps(SALIDA_NO2, ensure_ascii=False)
LIBRE = texto("Texto libre que la ruta documental descarta.")


@pytest.fixture
def spans():
    exportador = InMemorySpanExporter()
    proveedor = TracerProvider()
    proveedor.add_span_processor(SimpleSpanProcessor(exportador))
    observabilidad.usar_proveedor(proveedor)
    yield exportador
    observabilidad.usar_proveedor(TracerProvider())


def _rag(rag: RagFingido, espera_s: float = 0.0) -> HerramientaRag:
    async def transporte(peticion: httpx.Request) -> httpx.Response:
        await asyncio.sleep(espera_s)  # cede el control: los turnos concurrentes se intercalan
        return rag(peticion)
    return HerramientaRag("http://rag:8010", transport=httpx.MockTransport(transporte))


def _arbol(spans) -> list:
    """Nombres de los spans anidados por `parent_id`, en orden de inicio."""
    hijos = defaultdict(list)
    for s in sorted(spans, key=lambda s: s.start_time):
        hijos[s.parent.span_id if s.parent else None].append(s)

    def nodo(s):
        sub = [nodo(h) for h in hijos[s.context.span_id]]
        return (s.name, sub) if sub else s.name
    return [nodo(s) for s in hijos[None]]


async def test_turno_documental_con_reparacion_produce_el_arbol(spans):
    llm = LLMFalso(guion=[llamada("buscar_evidencias", {"pregunta": PREGUNTA, "tema": "salud"}, "c1"),
                          LIBRE, texto("Esto no es JSON"), texto(JSON_NO2)])
    clasificador = LLMFalso(guion=[texto("intencion: DOCUMENTAL\ntema: salud")])
    bucle = Bucle(llm, [_rag(RagFingido())], llm_clasificador=clasificador)
    r = await bucle.responder(PREGUNTA)

    terminados = spans.get_finished_spans()
    assert _arbol(terminados) == [("turno", [
        ("clasificar", ["llm"]),
        ("bucle", ["llm", "buscar_evidencias", "llm"]),
        ("sintesis_documental", ["llm", "validar", "llm", "validar"]),
    ])]
    assert all(s.attributes["llm.token_count.prompt"] == 100 and s.attributes["llm.token_count.completion"] == 20
               for s in terminados if s.name == "llm")
    turno = next(s for s in terminados if s.name == "turno")
    assert turno.attributes["agente.ruta"] == "documental" and turno.attributes["agente.reparaciones"] == 1
    assert format_trace_id(turno.context.trace_id) == r.traza_id


async def test_turnos_concurrentes_no_mezclan_spans(spans):
    preguntas = ["¿Qué efectos tiene el NO2?", "¿Cuál es el límite anual del NO2?"]
    bucles = [Bucle(LLMFalso(guion=[llamada("buscar_evidencias", {"pregunta": p}, "c1"), LIBRE, texto(JSON_NO2)]),
                    [_rag(RagFingido(), espera_s=0.01)]) for p in preguntas]
    resultados = await asyncio.gather(*(b.responder(p) for b, p in zip(bucles, preguntas)))

    por_traza = defaultdict(list)
    for s in spans.get_finished_spans():
        por_traza[format_trace_id(s.context.trace_id)].append(s)
    assert set(por_traza) == {r.traza_id for r in resultados} and len(por_traza) == 2
    turnos = [next(s for s in por_traza[r.traza_id] if s.name == "turno") for r in resultados]
    assert turnos[0].start_time < turnos[1].end_time and turnos[1].start_time < turnos[0].end_time  # se solapan
    for r, pregunta in zip(resultados, preguntas):
        propios = por_traza[r.traza_id]
        assert _arbol(propios) == [("turno", [("bucle", ["llm", "buscar_evidencias", "llm"]),
                                              ("sintesis_documental", ["llm", "validar"])])]
        busqueda = next(s for s in propios if s.name == "buscar_evidencias")
        assert json.loads(busqueda.attributes["input.value"])["pregunta"] == pregunta


async def test_llm_caido_cierra_el_turno_con_error(spans):
    bucle = Bucle(LLMFalso(guion=[RuntimeError("sin red")]), [])
    with pytest.raises(LLMNoDisponible):
        await bucle.responder("Hola")

    turno = next(s for s in spans.get_finished_spans() if s.name == "turno")
    assert turno.status.status_code is StatusCode.ERROR and turno.end_time is not None


async def test_exportador_jsonl_que_falla_no_altera_la_respuesta(tmp_path):
    proveedor = TracerProvider()
    proveedor.add_span_processor(SimpleSpanProcessor(ExportadorJsonl(tmp_path)))  # una carpeta: no se abre
    observabilidad.usar_proveedor(proveedor)
    try:
        llm = LLMFalso(guion=[llamada("buscar_evidencias", {"pregunta": PREGUNTA}, "c1"), LIBRE, texto(JSON_NO2)])
        r = await Bucle(llm, [_rag(RagFingido())]).responder(PREGUNTA)
    finally:
        observabilidad.usar_proveedor(TracerProvider())
    assert r.respuesta == TEXTO_NO2 and r.ruta == "documental"
