"""Eventos que el turno emite mientras avanza. La capa HTTP los convierte a SSE (`api/sse.py`).

- `EventoEstado`: el turno cambia de fase (`status`).
- `EventoTexto`: un fragmento de una respuesta definitiva (`token`). Solo lo emiten las llamadas
  cuyo texto es la respuesta final: sin herramientas ofrecidas o síntesis forzada.
"""
from dataclasses import dataclass
from enum import StrEnum
from typing import Awaitable, Callable


class Fase(StrEnum):
    EN_ESPERA = "en_espera"        # otro turno de la misma sesión está en curso
    CLASIFICANDO = "clasificando"
    BUSCANDO = "buscando"
    REDACTANDO = "redactando"
    VALIDANDO = "validando"


@dataclass(frozen=True)
class EventoEstado:
    fase: Fase
    herramienta: str | None = None


@dataclass(frozen=True)
class EventoTexto:
    texto: str


Evento = EventoEstado | EventoTexto

# Asíncrono: la cola del stream es acotada y puede hacer esperar al turno.
Emisor = Callable[[Evento], Awaitable[None]]
