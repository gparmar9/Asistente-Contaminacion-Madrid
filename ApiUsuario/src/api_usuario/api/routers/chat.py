"""Endpoint de chat: proxy sin estado hacia el servicio LLMOrchestrator.

Mientras el orquestador no exista (o `ORCHESTRATOR_URL` esté vacía), responde
con un stub que respeta el mismo contrato, para que el dashboard pueda
integrarse desde el primer día.
"""
import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import ValidationError

from api_usuario.config.settings import Settings, get_settings
from api_usuario.entities.chat import PreguntaChat, RespuestaChat

router = APIRouter(tags=["chat"])

AVISO_MEDICO = "Demo académica: esta respuesta no constituye consejo médico."
RESPUESTA_STUB = (
    "El asistente conversacional aún no está conectado. Esta es una respuesta "
    "provisional del esqueleto de la API."
)


@router.post(
    "/chat",
    response_model=RespuestaChat,
    summary="Pregunta en lenguaje natural al asistente",
)
async def post_chat(
    entrada: PreguntaChat, settings: Settings = Depends(get_settings)
) -> RespuestaChat:
    if not settings.orchestrator_url:
        return RespuestaChat(respuesta=RESPUESTA_STUB, fuentes=[], advertencia=AVISO_MEDICO)

    url = settings.orchestrator_url.rstrip("/") + "/responder"
    try:
        # Cliente asíncrono: una pregunta al LLM puede tardar decenas de segundos
        # y no debe ocupar un hilo del threadpool mientras espera.
        async with httpx.AsyncClient(timeout=settings.orchestrator_timeout_s) as cliente:
            respuesta = await cliente.post(url, json={"pregunta": entrada.pregunta})
        respuesta.raise_for_status()
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=503, detail="El asistente no está disponible en este momento"
        ) from exc

    # El cuerpo puede no ser JSON o no cumplir el contrato (deriva de esquema
    # entre servicios): eso es un fallo del orquestador, no de esta API -> 502.
    try:
        return RespuestaChat.model_validate(respuesta.json())
    except (ValueError, ValidationError) as exc:
        raise HTTPException(
            status_code=502, detail="El asistente devolvió una respuesta inválida"
        ) from exc
