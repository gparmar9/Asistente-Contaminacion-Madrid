"""Endpoints de chat: proxy hacia el servicio Agente o, sin él, hacia LLMOrchestrator.

- `AGENTE_URL` definida: `/chat` y `/chat/stream` van al agente (con `session_id`).
- Si no, el camino anterior: `ORCHESTRATOR_URL` o, si también está vacía, un stub
  que respeta el mismo contrato. `/chat/stream` lo entrega como un único evento
  `passthrough` + `done`. El `session_id` se devuelve para mantener el contrato, pero
  el orquestador no conserva el contexto de la conversación.
"""
import json
from uuid import uuid4

import httpx
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import ValidationError

from api_usuario.config.settings import Settings, get_settings
from api_usuario.entities.chat import PreguntaChat, RespuestaChat

router = APIRouter(tags=["chat"])

AVISO_MEDICO = "Demo académica: esta respuesta no constituye consejo médico."
RESPUESTA_STUB = (
    "El asistente conversacional aún no está conectado. Esta es una respuesta "
    "provisional del esqueleto de la API."
)
NO_DISPONIBLE = "El asistente no está disponible en este momento"
CABECERAS_SSE = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}


@router.post(
    "/chat",
    response_model=RespuestaChat,
    summary="Pregunta en lenguaje natural al asistente",
)
async def post_chat(
    entrada: PreguntaChat, settings: Settings = Depends(get_settings)
) -> RespuestaChat:
    return await _preguntar(entrada, settings)


@router.post(
    "/chat/stream",
    response_class=StreamingResponse,
    summary="Pregunta al asistente con la respuesta en streaming (SSE)",
)
async def post_chat_stream(
    entrada: PreguntaChat, settings: Settings = Depends(get_settings)
) -> StreamingResponse:
    """Eventos: status, token, passthrough, error y done (ver README). Con el agente se
    reenvían byte a byte; sin él, la respuesta de `/chat` sale como passthrough + done."""
    if settings.agente_url:
        return await _proxy_stream(entrada, settings)

    respuesta = await _preguntar(entrada, settings)  # errores: código HTTP, antes de abrir

    async def fallback():
        yield _sse("passthrough", {
            "texto": respuesta.respuesta,
            "fuentes": [f.model_dump() for f in respuesta.fuentes],
            "advertencia": respuesta.advertencia,
            "traza_id": None,
        })
        yield _sse("done", {"session_id": respuesta.session_id, "traza_id": None})

    return StreamingResponse(fallback(), media_type="text/event-stream", headers=CABECERAS_SSE)


async def _preguntar(entrada: PreguntaChat, settings: Settings) -> RespuestaChat:
    """Camino de `/chat`, reutilizado por el fallback de `/chat/stream`."""
    if settings.agente_url:
        url = settings.agente_url.rstrip("/") + "/responder"
        cuerpo = entrada.model_dump(exclude_none=True)
        return await _post_json(url, cuerpo, settings.agente_timeout_s)

    session_id = entrada.session_id or uuid4().hex
    if not settings.orchestrator_url:
        return RespuestaChat(respuesta=RESPUESTA_STUB, fuentes=[], advertencia=AVISO_MEDICO,
                             session_id=session_id)

    # El orquestador no tiene sesiones: solo recibe la pregunta.
    url = settings.orchestrator_url.rstrip("/") + "/responder"
    respuesta = await _post_json(url, {"pregunta": entrada.pregunta}, settings.orchestrator_timeout_s)
    return respuesta.model_copy(update={"session_id": session_id, "traza_id": None})


async def _post_json(url: str, cuerpo: dict, timeout_s: float) -> RespuestaChat:
    try:
        # Cliente asíncrono: una pregunta al LLM puede tardar decenas de segundos
        # y no debe ocupar un hilo del threadpool mientras espera.
        async with httpx.AsyncClient(timeout=timeout_s) as cliente:
            respuesta = await cliente.post(url, json=cuerpo)
        respuesta.raise_for_status()
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail=NO_DISPONIBLE) from exc

    # El cuerpo puede no ser JSON o no cumplir el contrato (deriva de esquema
    # entre servicios): eso es un fallo del servicio remoto, no de esta API -> 502.
    try:
        return RespuestaChat.model_validate(respuesta.json())
    except (ValueError, ValidationError) as exc:
        raise HTTPException(
            status_code=502, detail="El asistente devolvió una respuesta inválida"
        ) from exc


async def _proxy_stream(entrada: PreguntaChat, settings: Settings) -> StreamingResponse:
    """Abre el SSE del agente y lo reenvía. Si el agente no responde 200, 503 sin abrir el
    nuestro. Respuesta y cliente se cierran al terminar o al irse el cliente."""
    url = settings.agente_url.rstrip("/") + "/responder/stream"
    cliente = httpx.AsyncClient(timeout=settings.agente_timeout_s)
    try:
        peticion = cliente.build_request("POST", url, json=entrada.model_dump(exclude_none=True))
        respuesta = await cliente.send(peticion, stream=True)
    except httpx.HTTPError as exc:
        await cliente.aclose()
        raise HTTPException(status_code=503, detail=NO_DISPONIBLE) from exc
    if respuesta.status_code != 200:
        await respuesta.aclose()
        await cliente.aclose()
        raise HTTPException(status_code=503, detail=NO_DISPONIBLE)

    async def reenviar():
        try:
            async for trozo in respuesta.aiter_raw():
                yield trozo
        except httpx.HTTPError:
            # El agente se cayó con el stream abierto: mismo evento que manda él (sin done).
            # La línea en blanco previa cierra un evento que hubiera quedado a medias.
            yield ("\n\n" + _sse("error", {"detalle": NO_DISPONIBLE})).encode()
        finally:
            await respuesta.aclose()
            await cliente.aclose()

    return StreamingResponse(reenviar(), media_type="text/event-stream", headers=CABECERAS_SSE)


def _sse(tipo: str, datos: dict) -> str:
    return f"event: {tipo}\ndata: {json.dumps(datos, ensure_ascii=False)}\n\n"
