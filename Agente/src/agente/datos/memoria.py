"""Almacén de las conversaciones: los turnos completados de cada sesión.

`AlmacenConversaciones` es el contrato; `AlmacenEnMemoria`, la única implementación. Vive en el
proceso: se pierde al reiniciar o desplegar, y vale para un solo proceso del agente (como el
bloqueo por sesión). Pasar a una base de datos solo cambiaría esta clase.

No sabe nada de ventanas ni de LLM: guarda y devuelve turnos. Lo que se manda al modelo lo decide
`business/memoria.py`.

Retención: como mucho `max_turnos` por sesión (se descartan los más antiguos) y una sesión sin
turnos nuevos durante `ttl_s` se olvida. La purga es perezosa, al guardar; sin tareas en segundo plano.
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from typing import Callable, Protocol

from agente.entities.memoria import TurnoGuardado


class AlmacenConversaciones(Protocol):
    async def cargar(self, session_id: str) -> list[TurnoGuardado]:
        """Turnos de la sesión, del más antiguo al más reciente. Vacía si no existe o caducó."""

    async def guardar(self, session_id: str, turno: TurnoGuardado) -> None:
        """Añade un turno completado a la sesión."""


@dataclass
class _Conversacion:
    turnos: deque[TurnoGuardado]
    ultima_actividad: float = 0.0


class AlmacenEnMemoria:
    """`reloj`: segundos monótonos; los tests inyectan uno propio para la caducidad."""

    def __init__(self, max_turnos: int = 20, ttl_s: float = 7 * 24 * 3600,
                 reloj: Callable[[], float] = time.monotonic) -> None:
        self._max_turnos = max(1, max_turnos)
        self._ttl_s = ttl_s
        self._reloj = reloj
        self._conversaciones: dict[str, _Conversacion] = {}

    def __len__(self) -> int:
        """Sesiones guardadas (incluidas las caducadas que aún no se han purgado)."""
        return len(self._conversaciones)

    async def cargar(self, session_id: str) -> list[TurnoGuardado]:
        conversacion = self._conversaciones.get(session_id)
        if conversacion is None:
            return []
        if self._caducada(conversacion, self._reloj()):
            del self._conversaciones[session_id]
            return []
        return list(conversacion.turnos)

    async def guardar(self, session_id: str, turno: TurnoGuardado) -> None:
        ahora = self._reloj()
        self._purgar(ahora)
        conversacion = self._conversaciones.setdefault(
            session_id, _Conversacion(deque(maxlen=self._max_turnos)))
        conversacion.turnos.append(turno)
        conversacion.ultima_actividad = ahora

    def _purgar(self, ahora: float) -> None:
        caducadas = [s for s, c in self._conversaciones.items() if self._caducada(c, ahora)]
        for session_id in caducadas:
            del self._conversaciones[session_id]

    def _caducada(self, conversacion: _Conversacion, ahora: float) -> bool:
        return ahora - conversacion.ultima_actividad > self._ttl_s
