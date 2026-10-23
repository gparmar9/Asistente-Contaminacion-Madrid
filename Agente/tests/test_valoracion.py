"""Valoración 👍/👎: el span del turno que la recibe y POST /valoracion con un Phoenix fingido."""
import json

import httpx
import pytest
from fastapi.testclient import TestClient
from opentelemetry.trace import format_span_id, format_trace_id

from agente.api import sse
from agente.business.bucle import Bucle
from agente.business.sesiones import Sesiones
from agente.datos.valoraciones import AnotadorPhoenix
from agente.llm.falso import LLMFalso, texto
from agente.main import _turno_en_sesion, app, get_anotador

pytestmark = pytest.mark.anyio

CHARLA = texto("intencion: CHARLA\ntema: ninguno")
VOTO = {"session_id": "s1", "traza_id": "0" * 31 + "a", "span_id": "0" * 15 + "b", "valor": "negativa"}


async def test_el_turno_lleva_la_sesion_y_done_su_span_id(spans):
    llm = LLMFalso(guion=[texto("Hola.")])
    bucle = Bucle(llm, [], llm_clasificador=LLMFalso(guion=[CHARLA]))
    turno = lambda emitir: _turno_en_sesion(bucle, Sesiones(), "sesion-1", "Hola", emitir)  # noqa: E731
    bloques = [b async for b in sse.eventos(turno, "sesion-1")]

    done = json.loads(bloques[-1].split("data: ")[1])
    terminados = spans.get_finished_spans()
    span_turno = next(s for s in terminados if s.name == "turno")
    assert done["span_id"] == format_span_id(span_turno.context.span_id)
    assert done["traza_id"] == format_trace_id(span_turno.context.trace_id)
    assert {s.attributes.get("session.id") for s in terminados} == {"sesion-1"}


@pytest.fixture
def cliente():
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def _phoenix(respuesta: httpx.Response, recibidas: list) -> AnotadorPhoenix:
    def transporte(peticion: httpx.Request) -> httpx.Response:
        recibidas.append((str(peticion.url), json.loads(peticion.content)))
        return respuesta
    return AnotadorPhoenix("http://phoenix:6006/", transport=httpx.MockTransport(transporte))


def test_la_valoracion_se_anota_en_phoenix_sobre_el_span(cliente):
    recibidas = []
    app.dependency_overrides[get_anotador] = lambda: _phoenix(httpx.Response(200, json={"data": []}), recibidas)

    r = cliente.post("/valoracion", json=VOTO)
    assert r.status_code == 204
    assert recibidas == [("http://phoenix:6006/v1/span_annotations", {"data": [{
        "span_id": VOTO["span_id"], "name": "valoracion_usuario", "annotator_kind": "HUMAN",
        "result": {"label": "negativa", "score": 0}, "identifier": "s1",
        "metadata": {"session_id": "s1", "traza_id": VOTO["traza_id"]},
    }]})]


@pytest.mark.parametrize("anotador", [
    None,  # sin PHOENIX_ENDPOINT
    _phoenix(httpx.Response(500), []),
])
def test_sin_phoenix_devuelve_503(cliente, anotador):
    app.dependency_overrides[get_anotador] = lambda: anotador
    assert cliente.post("/valoracion", json=VOTO).status_code == 503


@pytest.mark.parametrize("cambio", [{"valor": "regular"}, {"span_id": "no-es-hex"}, {"traza_id": "abc"}])
def test_valoracion_mal_formada_devuelve_422(cliente, cambio):
    app.dependency_overrides[get_anotador] = lambda: _phoenix(httpx.Response(200), [])
    assert cliente.post("/valoracion", json={**VOTO, **cambio}).status_code == 422
