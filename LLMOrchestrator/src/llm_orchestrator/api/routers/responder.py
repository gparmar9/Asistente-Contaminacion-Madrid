"""POST /responder: el punto de entrada del agente.

Es el endpoint que consume `ApiUsuario` desde su proxy `/chat`. El endpoint es
síncrono a propósito: FastAPI lo ejecuta en su threadpool y este servicio
existe solo para esperar al LLM — con el volumen de una demo académica no hay
riesgo de agotar los hilos.
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.engine import Engine

from llm_orchestrator.business.agente import ErrorLLM, responder
from llm_orchestrator.config.settings import Settings, get_settings
from llm_orchestrator.data_access.sql_connection import get_engine
from llm_orchestrator.entities.chat import PreguntaChat, RespuestaChat
from llm_orchestrator.integrations.llm_client import get_cliente_llm

router = APIRouter(tags=["responder"])


@router.post(
    "/responder",
    response_model=RespuestaChat,
    summary="Responde una pregunta usando el LLM con tools (SQL + documentos)",
)
def post_responder(
    entrada: PreguntaChat,
    settings: Settings = Depends(get_settings),
    cliente=Depends(get_cliente_llm),
    engine: Engine = Depends(get_engine),
) -> RespuestaChat:
    try:
        return responder(
            entrada.pregunta,
            cliente=cliente,
            engine=engine,
            modelo=settings.llm_model,
            max_iteraciones=settings.max_iteraciones,
        )
    except ErrorLLM as exc:
        raise HTTPException(
            status_code=503, detail="El modelo de lenguaje no está disponible en este momento"
        ) from exc
