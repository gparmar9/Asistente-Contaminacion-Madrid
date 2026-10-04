"""Servicio del agente LLM: FastAPI con /salud y POST /responder.

Arranque:
    uvicorn agente.main:app --app-dir src --port 8200 --env-file .env
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Request

from agente.business.bucle import Bucle, LLMNoDisponible
from agente.config.settings import get_settings
from agente.entities.chat import Pregunta, Respuesta
from agente.llm.cliente import ConfiguracionLLMInvalida, crear_llm
from agente.tools.rag import HerramientaRag

logger = logging.getLogger("agente")

settings = get_settings()


def construir_bucle() -> Bucle | None:
    """Monta LLM + clasificador + herramientas. Devuelve None si la configuración no permite arrancar el LLM."""
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
    app_.state.bucle = construir_bucle()
    yield


app = FastAPI(
    title=settings.app_name,
    version=settings.api_version,
    description="Agente conversacional del asistente de calidad del aire de Madrid.",
    lifespan=lifespan,
)


def get_bucle(request: Request) -> Bucle | None:
    return getattr(request.app.state, "bucle", None)


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
async def responder(entrada: Pregunta, bucle: Bucle | None = Depends(get_bucle)) -> Respuesta:
    if bucle is None:
        raise HTTPException(status_code=503, detail="El agente no tiene un LLM configurado")
    try:
        resultado = await bucle.responder(entrada.pregunta)
    except LLMNoDisponible as exc:
        raise HTTPException(status_code=503, detail="El asistente no está disponible en este momento") from exc
    return Respuesta(respuesta=resultado.respuesta, fuentes=resultado.fuentes,
                     advertencia=resultado.advertencia)
