"""Comprobaciones posteriores (fase 6): cifras, fuga del prompt e internos; observar y bloquear."""
import json
from functools import partial

import httpx
import pytest

from agente.api import sse
from agente.business import frases
from agente.business.bucle import Bucle
from agente.business.comprobaciones import comprobar
from agente.business.sesiones import Sesiones
from agente.datos.memoria import AlmacenEnMemoria
from agente.entities.comprobaciones import MaterialTurno, TextoComprobado
from agente.entities.memoria import TurnoGuardado
from agente.llm.falso import LLMFalso, llamada, texto
from agente.main import _turno_en_sesion
from agente.tools.rag import HerramientaRag
from tests.conftest import EVIDENCIAS_LIMITES, EVIDENCIAS_NO2, RagFingido

pytestmark = pytest.mark.anyio

PREGUNTA = "¿Qué límites tiene el NO2?"


def _bucle_documental(salida: dict, **opciones) -> Bucle:
    """Dos búsquedas (límites -> D1, NO2 -> D2) y la síntesis devuelve `salida`."""
    guion = [llamada("buscar_evidencias", {"pregunta": "límites"}, "c1"),
             llamada("buscar_evidencias", {"pregunta": "salud"}, "c2"),
             texto("Texto libre que se descarta."), texto(json.dumps(salida, ensure_ascii=False))]
    rag = RagFingido(respuestas=[EVIDENCIAS_LIMITES, EVIDENCIAS_NO2])
    herramienta = HerramientaRag("http://rag:8010", transport=httpx.MockTransport(rag))
    return Bucle(LLMFalso(guion=guion), [herramienta], **opciones)


def _libre(respuesta: str, base: str = PREGUNTA) -> MaterialTurno:
    return MaterialTurno("libre", (TextoComprobado(respuesta, base),))


# ---------------------------------------------------------------------- paso 1: observar

async def test_cifra_sin_respaldo_en_ruta_libre_se_anota_y_no_cambia_la_respuesta(spans):
    respuesta = "El límite anual del PM2.5 es 25 µg/m³."
    resultado = await Bucle(LLMFalso(guion=[texto(respuesta)]), []).responder("¿Cuál es el límite del PM2.5?")

    assert resultado.respuesta == respuesta and not resultado.bloqueada
    assert [(h.regla, h.detalle) for h in resultado.hallazgos] == [("cifras", "25")]
    turno = next(s for s in spans.get_finished_spans() if s.name == "turno")
    evento = next(e for e in turno.events if e.name == "comprobacion")
    assert evento.attributes["agente.regla"] == "cifras" and not evento.attributes["agente.bloquea"]
    assert list(turno.attributes["agente.comprobaciones"]) == ["cifras"]


async def test_cifras_de_cada_afirmacion_se_buscan_en_las_evidencias_que_cita():
    salida = {"estado": "respondida", "limitaciones": [], "afirmaciones": [
        {"texto": "El límite anual es 40 µg/m³.", "evidencias": ["D1"]},            # está en D1
        {"texto": "Por encima de 40 µg/m³ agrava el asma.", "evidencias": ["D2"]},  # solo en D1
        {"texto": "El límite horario es 200 µg/m³.", "evidencias": ["D1"]},         # en ninguna
    ]}
    resultado = await _bucle_documental(salida).responder(PREGUNTA)

    assert resultado.ruta == "documental" and resultado.valida
    assert [(h.regla, h.detalle) for h in resultado.hallazgos] == [("cifras", "40"), ("cifras", "200")]


async def test_no_cuentan_anos_contaminantes_ni_cifras_de_la_pregunta_y_el_historial_no_respalda():
    previo = TurnoGuardado(pregunta="¿Y el umbral de alerta?", respuesta="Es 400 µg/m³.",
                           respuesta_contexto="Es 400 µg/m³.")
    respuesta = "Desde 2008 el NO2, el O3 y el PM2.5 tienen umbral de 180; el de alerta es 400."
    llm = LLMFalso(guion=[texto(respuesta)])
    resultado = await Bucle(llm, []).responder("¿El umbral de 180 es de ozono?", historial=[previo])

    assert [(h.regla, h.detalle) for h in resultado.hallazgos] == [("cifras", "400")]


def test_fuga_salta_con_diez_palabras_de_un_prompt_y_no_con_la_presentacion():
    fuga = "Tengo orden de que no inventes cifras ni mediciones: este asistente aún no consulta datos."
    presentacion = f"¡Hola! {frases.IDENTIDAD}. {frases.CAPACIDADES_CHARLA.replace('Sabes', 'Sé')}."

    assert [h.regla for h in comprobar(_libre(fuga), [])] == ["fuga"]
    assert comprobar(_libre(presentacion), []) == []


@pytest.mark.parametrize("respuesta", [
    "He usado buscar_evidencias para responderte.",
    "Según oxidos_nitrogeno_salud:efectos-en-la-salud:0, el NO2 irrita.",
    "El NO2 irrita las vías respiratorias [D1].",
    '{"estado": "respondida", "afirmaciones": []}',
])
def test_internos_en_ruta_libre(respuesta):
    assert [h.regla for h in comprobar(_libre(respuesta), ["buscar_evidencias"])] == ["internos"]


async def test_la_respuesta_documental_entregada_no_se_comprueba():
    salida = {"estado": "respondida", "limitaciones": [],
              "afirmaciones": [{"texto": "El NO2 agrava el asma. [D2]", "evidencias": ["D2"]}]}
    resultado = await _bucle_documental(salida).responder(PREGUNTA)

    assert "[D2]" in resultado.respuesta and resultado.hallazgos == []


# ---------------------------------------------------------------------- paso 2: bloquear

async def test_regla_que_bloquea_sustituye_la_respuesta_y_la_memoria_guarda_la_frase():
    salida = {"estado": "respondida", "limitaciones": [],
              "afirmaciones": [{"texto": "El límite horario es 200 µg/m³.", "evidencias": ["D1"]}]}
    bucle, memoria = _bucle_documental(salida, comprobaciones_bloquean=["cifras"]), AlmacenEnMemoria()
    resultado = await _turno_en_sesion(bucle, Sesiones(), "s", PREGUNTA, memoria=memoria)

    assert resultado.bloqueada and resultado.ruta == "documental"
    assert (resultado.respuesta, resultado.fuentes, resultado.advertencia) == (frases.INSUFICIENCIA, [], None)
    [guardado] = await memoria.cargar("s")
    assert guardado.respuesta == guardado.respuesta_contexto == frases.INSUFICIENCIA


async def test_con_una_regla_que_bloquea_la_charla_sale_como_passthrough():
    respuesta = "Hola, te ayudo con la calidad del aire de Madrid."
    bucle = Bucle(LLMFalso(guion=[texto(respuesta)], trozos=3), [], comprobaciones_bloquean=["fuga"])
    turno = partial(_turno_en_sesion, bucle, Sesiones(), "s", "Hola")
    eventos = [b.strip().split("\n")[0].removeprefix("event: ") async for b in sse.eventos(turno, "s")]

    assert [e for e in eventos if e != "status"] == ["passthrough", "done"]
