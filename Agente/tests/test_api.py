"""Contrato HTTP de POST /responder con el bucle sustituido por un doble."""
import json

import httpx
import pytest
from fastapi.testclient import TestClient

from agente.business import frases
from agente.business.bucle import Bucle, LLMNoDisponible
from agente.llm.falso import LLMFalso, llamada, texto
from agente.main import app, get_bucle
from agente.tools.rag import HerramientaRag
from tests.conftest import SALIDA_NO2, TEXTO_NO2, RagFingido


@pytest.fixture
def cliente():
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def test_responder_devuelve_el_contrato(cliente):
    llm = LLMFalso(guion=[llamada("buscar_evidencias", {"pregunta": "NO2"}), texto("Texto libre."),
                          texto(json.dumps(SALIDA_NO2))])
    rag = HerramientaRag("http://rag:8010", transport=httpx.MockTransport(RagFingido()))
    app.dependency_overrides[get_bucle] = lambda: Bucle(llm, [rag])

    r = cliente.post("/responder", json={"pregunta": "¿Qué efectos tiene el NO2?"})
    assert r.status_code == 200
    cuerpo = r.json()
    assert len(cuerpo.pop("traza_id")) == 32  # id de la traza, para buscar el turno en Phoenix
    assert cuerpo == {
        "respuesta": TEXTO_NO2,
        "fuentes": [{"tipo": "documento", "referencia": "Efectos del NO2 en la salud"}],
        "advertencia": frases.AVISO_SANITARIO,
    }


def test_llm_caido_devuelve_503(cliente):
    class _BucleRoto:
        async def responder(self, pregunta):
            raise LLMNoDisponible("sin red")

    app.dependency_overrides[get_bucle] = lambda: _BucleRoto()
    assert cliente.post("/responder", json={"pregunta": "Hola"}).status_code == 503
