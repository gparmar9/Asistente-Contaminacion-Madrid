"""Servicio del agente LLM: FastAPI con /salud, POST /responder y POST /responder/stream (SSE).

Un turno a la vez por sesión (`business/sesiones.py`): una segunda pregunta de la misma sesión
espera a que termine la primera. Los dos endpoints comparten sesión, bloqueo y log del turno.

Arranque:
    uvicorn agente.main:app --app-dir src --port 8200 --env-file .env
"""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse

from agente import observabilidad
from agente.api import sse
from agente.business.bucle import Bucle, LLMNoDisponible, ResultadoTurno
from agente.business.sesiones import Sesiones
from agente.config.settings import Settings, get_settings
from agente.entities.chat import Pregunta, Respuesta
from agente.entities.eventos import Emisor, EventoEstado, Fase
from agente.llm.cliente import ConfiguracionLLMInvalida, crear_llm
from agente.tools.rag import HerramientaRag

logger = logging.getLogger("agente")
log_turnos = observabilidad.configurar_resumen()

settings = get_settings()


def construir_bucle(settings: Settings = settings) -> Bucle | None:
    """Monta LLM + clasificador + herramientas. Devuelve None si la configuración no permite arrancar el LLM.
    `settings`: la del proceso; el evaluador de turnos pasa la suya."""
    try:
        llm = crear_llm(settings)
        llm_clasificador = crear_llm(settings, temperatura=0.0)
    except ConfiguracionLLMInvalida as exc:
        logger.warning("LLM sin configurar: %s. /responder devolverá 503 hasta corregirlo", exc)
        return None
    herramientas = []
    if settings.rag_url:
        herramientas.append(HerramientaRag(settings.rag_url, timeout_s=settings.rag_timeout_s))
    else:
        logger.warning("RAG_URL vacía: el agente no tendrá la herramienta documental")
    return Bucle(llm, herramientas, max_vueltas=settings.max_vueltas, llm_clasificador=llm_clasificador,
                 clasificador_timeout_s=settings.clasificador_timeout_s)


@asynccontextmanager
async def lifespan(app_: FastAPI):
    observabilidad.configurar(settings)
    app_.state.bucle = construir_bucle()
    app_.state.sesiones = Sesiones()
    yield
    observabilidad.cerrar()


app = FastAPI(
    title=settings.app_name,
    version=settings.api_version,
    description="Agente conversacional del asistente de calidad del aire de Madrid.",
    lifespan=lifespan,
)


def get_bucle(request: Request) -> Bucle | None:
    return getattr(request.app.state, "bucle", None)


def get_sesiones(request: Request) -> Sesiones:
    return request.app.state.sesiones


@app.get("/salud", summary="Estado del servicio")
def salud() -> dict:
    return {
        "estado": "ok",
        "servicio": settings.app_name,
        "version": settings.api_version,
        "entorno": settings.app_env,
        "llm": {"proveedor": settings.llm_proveedor, "modelo": settings.llm_modelo or None},
        "rag_url": settings.rag_url or None,
    }


@app.post("/responder", response_model=Respuesta, summary="Pregunta en lenguaje natural al agente")
async def responder(entrada: Pregunta, bucle: Bucle | None = Depends(get_bucle),
                    sesiones: Sesiones = Depends(get_sesiones)) -> Respuesta:
    _comprobar_llm(bucle)
    session_id = entrada.session_id or uuid4().hex
    try:
        resultado = await _turno_en_sesion(bucle, sesiones, session_id, entrada.pregunta)
    except LLMNoDisponible as exc:
        raise HTTPException(status_code=503, detail=sse.NO_DISPONIBLE) from exc
    return Respuesta(respuesta=resultado.respuesta, fuentes=resultado.fuentes,
                     advertencia=resultado.advertencia, traza_id=resultado.traza_id, session_id=session_id)


@app.post("/responder/stream", summary="Pregunta al agente con la respuesta en streaming (SSE)",
          response_class=StreamingResponse)
async def responder_stream(entrada: Pregunta, bucle: Bucle | None = Depends(get_bucle),
                           sesiones: Sesiones = Depends(get_sesiones)) -> StreamingResponse:
    """Eventos: status, token, passthrough, error y done. Los fallos después de abrir el stream
    llegan como evento `error` (sin `done`); antes, como código HTTP."""
    _comprobar_llm(bucle)
    session_id = entrada.session_id or uuid4().hex

    async def turno(emitir: Emisor) -> ResultadoTurno:
        return await _turno_en_sesion(bucle, sesiones, session_id, entrada.pregunta, emitir)

    return StreamingResponse(
        sse.eventos(turno, session_id, settings.stream_heartbeat_s, settings.stream_cola),
        media_type="text/event-stream", headers=sse.CABECERAS)


def _comprobar_llm(bucle: Bucle | None) -> None:
    if bucle is None:
        raise HTTPException(status_code=503, detail="El agente no tiene un LLM configurado")


async def _turno_en_sesion(bucle: Bucle, sesiones: Sesiones, session_id: str, pregunta: str,
                 emitir: Emisor | None = None) -> ResultadoTurno:
    """Un turno dentro de su sesión, con su línea de log. `emitir`: solo en el stream."""
    async def al_esperar() -> None:
        await emitir(EventoEstado(Fase.EN_ESPERA))

    inicio = time.perf_counter()
    try:
        async with sesiones.turno(session_id, al_esperar if emitir else None):
            resultado = await bucle.responder(pregunta, emitir)
    except LLMNoDisponible:
        log_turnos.info("turno fallido: LLM no disponible (%.0f ms)", (time.perf_counter() - inicio) * 1000)
        raise
    except asyncio.CancelledError:
        log_turnos.info("turno cancelado: el cliente se desconectó (%.0f ms)", (time.perf_counter() - inicio) * 1000)
        raise
    log_turnos.info("turno traza=%s ruta=%s intencion=%s vueltas=%d duracion_ms=%.0f", resultado.traza_id,
                    resultado.ruta, resultado.intencion, resultado.vueltas, (time.perf_counter() - inicio) * 1000)
    return resultado
