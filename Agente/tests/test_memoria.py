"""Memoria de la conversación: almacén, ventana, guardado y contexto en cada llamada."""
import asyncio
import json

import httpx
import pytest
from fastapi.testclient import TestClient
from llama_index.core.base.llms.types import MessageRole, ToolCallBlock

from agente.business import frases
from agente.business.bucle import Bucle, LLMNoDisponible, ResultadoTurno
from agente.business.memoria import ventana
from agente.business.sesiones import Sesiones
from agente.datos.memoria import AlmacenEnMemoria
from agente.entities.memoria import TurnoGuardado
from agente.llm.falso import LLMFalso, llamada, texto
from agente.main import _turno_en_sesion, app, get_bucle, get_memoria
from agente.tools.rag import HerramientaRag
from tests.conftest import SALIDA_NO2, RagFingido

pytestmark = pytest.mark.anyio

PREVIA = TurnoGuardado(pregunta="¿Qué efectos tiene el NO2?",
                       respuesta="El NO2 agrava el asma. [D1]\n\nAviso: …\n\nBibliografía: …",
                       respuesta_contexto="El NO2 agrava el asma.", intencion="DOCUMENTAL", ruta="documental")
SEGUIMIENTO = "¿Y en niños?"
JSON_NO2 = json.dumps(SALIDA_NO2, ensure_ascii=False)


class _BucleEspia:
    """Registra el historial que recibe cada turno y responde con un eco."""

    def __init__(self, fallo: BaseException | None = None):
        self.historiales: list[list[str]] = []
        self.fallo = fallo

    async def responder(self, pregunta, emitir=None, historial=()):
        self.historiales.append([t.pregunta for t in historial])
        if self.fallo is not None:
            raise self.fallo
        return ResultadoTurno(respuesta=f"Eco: {pregunta}")


def _bucle(clasificador, guion, rag: RagFingido, max_vueltas: int = 3):
    llm, llm_clasificador = LLMFalso(guion=list(guion)), LLMFalso(guion=list(clasificador))
    herramienta = HerramientaRag("http://rag:8010", transport=httpx.MockTransport(rag))
    bucle = Bucle(llm, [herramienta], max_vueltas=max_vueltas,
                  llm_clasificador=llm_clasificador if clasificador else None)
    return llm, llm_clasificador, bucle


# ---------------------------------------------------------------------- paso 1: almacén y guardado

def test_la_sesion_recibe_sus_turnos_y_otra_no():
    espia, memoria = _BucleEspia(), AlmacenEnMemoria()
    app.dependency_overrides[get_bucle] = lambda: espia
    app.dependency_overrides[get_memoria] = lambda: memoria
    try:
        with TestClient(app) as cliente:
            cliente.post("/responder", json={"pregunta": "Primera", "session_id": "a"})
            cliente.post("/responder", json={"pregunta": "Segunda", "session_id": "a"})
            cliente.post("/responder", json={"pregunta": "Otra", "session_id": "b"})
    finally:
        app.dependency_overrides.clear()
    assert espia.historiales == [[], ["Primera"], []]


@pytest.mark.parametrize("fallo", [LLMNoDisponible("sin red"), asyncio.CancelledError()])
async def test_turno_fallido_o_cancelado_no_se_guarda(fallo):
    memoria = AlmacenEnMemoria()
    with pytest.raises(type(fallo)):
        await _turno_en_sesion(_BucleEspia(fallo), Sesiones(), "s", "Hola", memoria=memoria)
    assert await memoria.cargar("s") == [] and len(memoria) == 0


def test_ventana_corta_por_turnos_completos():
    turnos = [TurnoGuardado(pregunta="p" * 20, respuesta="", respuesta_contexto="r" * 20) for _ in range(3)]
    assert ventana(turnos, 25) == turnos[-2:]  # 10 tokens por turno: caben dos, no dos y medio
    assert ventana(turnos, 9) == []            # ni el más reciente cabe: sin historial


async def test_sesion_inactiva_caduca():
    ahora = [0.0]
    memoria = AlmacenEnMemoria(ttl_s=100, reloj=lambda: ahora[0])
    await memoria.guardar("vieja", PREVIA)
    ahora[0] = 50
    await memoria.guardar("reciente", PREVIA)
    ahora[0] = 101
    await memoria.guardar("nueva", PREVIA)  # purga perezosa: se va la vieja, sin haberla cargado
    assert len(memoria) == 2
    assert await memoria.cargar("vieja") == [] and await memoria.cargar("reciente") == [PREVIA]


async def test_respuesta_contexto_documental_sin_marcas_ni_aviso():
    salida = {"estado": "respondida", "limitaciones": [],
              "afirmaciones": [{"texto": "El NO2 agrava el asma [D1].", "evidencias": ["D1"]}]}
    rag = RagFingido()
    _, _, bucle = _bucle([], [llamada("buscar_evidencias", {"pregunta": "NO2"}), texto("libre"),
                              texto(json.dumps(salida, ensure_ascii=False))], rag)
    r = await bucle.responder("¿Qué efectos tiene el NO2?")
    assert r.ruta == "documental" and "[D1]" in r.respuesta and r.advertencia
    assert r.respuesta_contexto == "El NO2 agrava el asma."


# ---------------------------------------------------------------------- paso 2: contexto en las rutas

async def test_seguimiento_documental_lleva_el_contexto_a_cada_llamada():
    rag = RagFingido()
    llm, clasificador, bucle = _bucle([texto("intencion: DOCUMENTAL\ntema: salud")],
                                      [texto("Sin buscar."), texto(JSON_NO2)], rag)
    r = await bucle.responder(SEGUIMIENTO, historial=[PREVIA])

    assert r.ruta == "documental" and r.busqueda_forzada
    a_clasificador = clasificador.registro[0]["mensajes"][-1].content
    assert PREVIA.pregunta in a_clasificador and a_clasificador.endswith(f"Pregunta actual: {SEGUIMIENTO}")
    assert [m.content for m in llm.registro[0]["mensajes"][1:]] == [
        PREVIA.pregunta, PREVIA.respuesta_contexto, SEGUIMIENTO]
    assert llm.registro[1]["mensajes"][-1].content.startswith(frases.CONVERSACION_PREVIA_SINTESIS)
    assert rag.busquedas[0]["pregunta"] == f"{PREVIA.pregunta} {SEGUIMIENTO}"
    assert rag.validaciones[0]["pregunta"] == SEGUIMIENTO


async def test_historial_del_bucle_solo_pares_de_texto():
    _, _, bucle = _bucle([], [texto("Hola de nuevo.")], RagFingido())
    llm = bucle._llm
    await bucle.responder("Gracias", historial=[PREVIA, PREVIA])

    mensajes = llm.registro[0]["mensajes"]
    assert [m.role for m in mensajes] == [MessageRole.SYSTEM] + [MessageRole.USER, MessageRole.ASSISTANT] * 2 \
        + [MessageRole.USER]
    historial = mensajes[1:-1]
    assert all(m.additional_kwargs == {} for m in historial)
    assert not any(isinstance(b, ToolCallBlock) for m in historial for b in m.blocks)
    assert all("[D" not in m.content and "Aviso" not in m.content for m in historial)


async def test_sintesis_forzada_recibe_el_contexto():
    pedir = llamada("consultar_mediciones", {})
    llm, _, bucle = _bucle([], [pedir, texto("No puedo consultarlo.")], RagFingido(), max_vueltas=1)
    r = await bucle.responder(SEGUIMIENTO, historial=[PREVIA])

    assert r.sintesis_forzada
    usuario = llm.registro[-1]["mensajes"][-1].content
    assert usuario.startswith(frases.CONVERSACION_PREVIA_SINTESIS) and PREVIA.pregunta in usuario
