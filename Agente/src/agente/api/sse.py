"""Un turno como Server-Sent Events. Sin servidor: el generador se prueba solo.

    turno(emitir) en una tarea -> eventos a una cola acotada (si el cliente lee despacio, el turno espera)
      -> el generador los formatea: status | token
      -> si en `intervalo_s` no sale nada: repite el último status (heartbeat, fuera de la cola)
      -> al terminar: passthrough (si la respuesta no salió ya como tokens) y done;
         si el turno falla: error, sin done
      -> si el generador se cierra antes (el cliente se fue): cancela el turno y espera a que limpie

La cancelación corta el turno local y sus conexiones, pero no garantiza que el proveedor deje de
procesar una inferencia ya iniciada.
"""
from __future__ import annotations

import asyncio
import json
import logging
from contextlib import suppress
from typing import AsyncIterator, Awaitable, Callable

import anyio

from agente.business.bucle import LLMNoDisponible, ResultadoTurno
from agente.entities.eventos import Emisor, Evento, EventoEstado, EventoTexto

logger = logging.getLogger("agente.sse")

CABECERAS = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
NO_DISPONIBLE = "El asistente no está disponible en este momento"
ERROR_INTERNO = "No he podido completar la respuesta. Vuelve a intentarlo."

Turno = Callable[[Emisor], Awaitable[ResultadoTurno]]


def formatear(tipo: str, datos: dict) -> str:
    return f"event: {tipo}\ndata: {json.dumps(datos, ensure_ascii=False)}\n\n"


async def eventos(turno: Turno, session_id: str, intervalo_s: float = 0.7,
                  tam_cola: int = 64) -> AsyncIterator[str]:
    """`turno(emitir)`: ejecuta el turno completo (sesión incluida) enviando sus eventos a `emitir`."""
    cola: asyncio.Queue[Evento] = asyncio.Queue(maxsize=tam_cola)
    tarea = asyncio.create_task(turno(cola.put))
    obtener: asyncio.Task | None = None
    ultimo_estado: EventoEstado | None = None
    try:
        while True:
            if obtener is None:
                obtener = asyncio.ensure_future(cola.get())
            hechos, _ = await asyncio.wait({obtener, tarea}, timeout=intervalo_s,
                                           return_when=asyncio.FIRST_COMPLETED)
            if obtener in hechos:  # antes que el fin del turno: primero lo que ya emitió
                evento, obtener = obtener.result(), None
                if isinstance(evento, EventoEstado):
                    ultimo_estado = evento
                yield _sse(evento)
            elif tarea in hechos:
                break
            elif ultimo_estado is not None:
                yield _sse(ultimo_estado)
        obtener.cancel()  # Queue.get cancelado no pierde el elemento
        obtener = None
        while not cola.empty():
            yield _sse(cola.get_nowait())

        error = tarea.exception()
        if error is not None:
            if not isinstance(error, LLMNoDisponible):
                logger.error("El turno falló con el stream abierto", exc_info=error)
            detalle = NO_DISPONIBLE if isinstance(error, LLMNoDisponible) else ERROR_INTERNO
            yield formatear("error", {"detalle": detalle})
            return
        resultado = tarea.result()
        if not resultado.emitida:
            yield formatear("passthrough", {
                "texto": resultado.respuesta,
                "fuentes": [f.model_dump() for f in resultado.fuentes],
                "advertencia": resultado.advertencia,
                "traza_id": resultado.traza_id,
            })
        yield formatear("done", {"session_id": session_id, "traza_id": resultado.traza_id})
    finally:
        if obtener is not None:
            obtener.cancel()
        if not tarea.done():
            tarea.cancel()
            # Starlette cancela el generador al desconectarse el cliente; el escudo deja esperar
            # a que el turno salga de la sesión y cierre sus conexiones.
            with anyio.CancelScope(shield=True), suppress(asyncio.CancelledError, Exception):
                await tarea


def _sse(evento: Evento) -> str:
    if isinstance(evento, EventoTexto):
        return formatear("token", {"texto": evento.texto})
    datos = {"fase": evento.fase.value}
    if evento.herramienta:
        datos["herramienta"] = evento.herramienta
    return formatear("status", datos)
