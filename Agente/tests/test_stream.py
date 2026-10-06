"""POST /responder/stream: eventos del turno, errores, cancelación, heartbeat y espera de sesión.

Salvo el primero (contrato HTTP), los tests leen el generador SSE directamente, sin servidor.
El LLM lento espera a un `asyncio.Event`, no a un `sleep`.
"""
import asyncio
import json
from functools import partial
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient
from llama_index.core.bridge.pydantic import Field

from agente.api import sse
from agente.business import frases
from agente.business.bucle import Bucle
from agente.business.sesiones import Sesiones
from agente.llm.falso import LLMFalso, llamada, texto
from agente.main import _turno_en_sesion, app, get_bucle
from agente.tools.rag import HerramientaRag
from tests.conftest import SALIDA_NO2, TEXTO_NO2, RagFingido

pytestmark = pytest.mark.anyio

PREGUNTA = "¿Qué efectos tiene el NO2 en el asma?"
CHARLA = texto("intencion: CHARLA\ntema: ninguno")


class LLMLento(LLMFalso):
    """Cada llamada espera a `seguir`: el turno queda en vuelo el tiempo que el test quiera."""

    seguir: Any = Field(default_factory=asyncio.Event)

    async def astream_chat(self, messages, **kwargs):
        await self.seguir.wait()
        return await super().astream_chat(messages, **kwargs)


def _evento(bloque: str) -> tuple[str, dict]:
    tipo, datos = bloque.strip().split("\n")
    return tipo.removeprefix("event: "), json.loads(datos.removeprefix("data: "))


def _eventos(bucle: Bucle, sesiones: Sesiones | None = None, session_id: str = "s",
             pregunta: str = PREGUNTA, intervalo_s: float = 5.0):
    turno = partial(_turno_en_sesion, bucle, sesiones or Sesiones(), session_id, pregunta)
    return sse.eventos(turno, session_id, intervalo_s=intervalo_s)


async def _siguiente(generador) -> tuple[str, dict]:
    """El siguiente evento; si no llega en 1 s, el test falla en vez de colgarse."""
    return _evento(await asyncio.wait_for(anext(generador), timeout=1))


async def _todos(generador) -> list[tuple[str, dict]]:
    return [_evento(b) async for b in generador]


def test_charla_llega_en_tokens_y_termina_sin_passthrough():
    respuesta = "Hola, te ayudo con la calidad del aire de Madrid."
    llm = LLMFalso(guion=[texto(respuesta)], trozos=3)
    app.dependency_overrides[get_bucle] = lambda: Bucle(llm, [], llm_clasificador=LLMFalso(guion=[CHARLA]))
    try:
        with TestClient(app) as cliente:
            r = cliente.post("/responder/stream", json={"pregunta": "Hola", "session_id": "s1"})
    finally:
        app.dependency_overrides.clear()

    assert r.status_code == 200 and r.headers["content-type"].startswith("text/event-stream")
    eventos = [_evento(b) for b in r.text.split("\n\n") if b.strip()]
    assert [d["fase"] for t, d in eventos if t == "status"][:2] == ["clasificando", "redactando"]
    assert [t for t, _ in eventos if t != "status"] == ["token"] * 3 + ["done"]
    assert "".join(d["texto"] for t, d in eventos if t == "token") == respuesta
    assert eventos[-1][1]["session_id"] == "s1" and len(eventos[-1][1]["traza_id"]) == 32


async def test_documental_sale_entero_como_passthrough():
    llm = LLMFalso(guion=[llamada("buscar_evidencias", {"pregunta": PREGUNTA}), texto("Texto libre."),
                          texto(json.dumps(SALIDA_NO2))])
    clasificador = LLMFalso(guion=[texto("intencion: DOCUMENTAL\ntema: salud")])
    rag = HerramientaRag("http://rag:8010", transport=httpx.MockTransport(RagFingido()))
    eventos = await _todos(_eventos(Bucle(llm, [rag], llm_clasificador=clasificador)))

    assert [(d["fase"], d.get("herramienta")) for t, d in eventos if t == "status"] == [
        ("clasificando", None), ("redactando", None), ("buscando", "buscar_evidencias"),
        ("redactando", None), ("redactando", None), ("validando", None)]
    assert [t for t, _ in eventos if t != "status"] == ["passthrough", "done"]  # el JSON no sale como tokens
    passthrough = eventos[-2][1]
    assert passthrough["texto"] == TEXTO_NO2 and passthrough["advertencia"] == frases.AVISO_SANITARIO
    assert passthrough["fuentes"] == [{"tipo": "documento", "referencia": "Efectos del NO2 en la salud"}]


async def test_llm_caido_con_el_stream_abierto_da_error_sin_done():
    eventos = await _todos(_eventos(Bucle(LLMFalso(guion=[RuntimeError("sin red")]), [])))
    assert eventos == [("status", {"fase": "redactando"}), ("error", {"detalle": sse.NO_DISPONIBLE})]


async def test_cerrar_el_stream_cancela_el_turno_y_libera_la_sesion(spans):
    llm, sesiones = LLMLento(guion=[texto("Hola.")]), Sesiones()
    generador = _eventos(Bucle(llm, []), sesiones)
    assert await _siguiente(generador) == ("status", {"fase": "redactando"})
    await generador.aclose()  # lo que hace Starlette cuando el cliente se desconecta

    assert len(sesiones) == 0 and llm.registro == []  # sesión libre y ninguna llamada al LLM
    turno = next(s for s in spans.get_finished_spans() if s.name == "turno")
    assert turno.attributes["agente.cancelado"] is True


async def test_sin_eventos_se_repite_el_ultimo_status():
    llm = LLMLento(guion=[texto("Hola.")])
    generador = _eventos(Bucle(llm, []), intervalo_s=0.01)
    primeros = [await _siguiente(generador) for _ in range(3)]
    llm.seguir.set()
    resto = await _todos(generador)

    assert primeros == [("status", {"fase": "redactando"})] * 3
    assert [t for t, _ in resto if t != "status"] == ["token", "token", "done"]


async def test_segundo_turno_de_la_sesion_espera_al_primero():
    sesiones, entro, salir = Sesiones(), asyncio.Event(), asyncio.Event()

    async def primero():
        async with sesiones.turno("s"):
            entro.set()
            await salir.wait()
    ocupante = asyncio.create_task(primero())
    await asyncio.wait_for(entro.wait(), timeout=1)

    llm = LLMFalso(guion=[texto("Hola.")])
    generador = _eventos(Bucle(llm, []), sesiones)
    assert await _siguiente(generador) == ("status", {"fase": "en_espera"})
    assert llm.registro == []
    salir.set()
    await ocupante
    resto = await _todos(generador)
    assert [t for t, _ in resto if t != "status"] == ["token", "token", "done"] and len(llm.registro) == 1
