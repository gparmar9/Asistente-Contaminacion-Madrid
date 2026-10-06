"""Un turno a la vez por sesión.

Cada sesión con turnos en curso o esperando tiene una entrada con su bloqueo y un contador de
usuarios (el turno que corre más los que esperan). La entrada se borra cuando el contador llega a 0,
también si el turno se cancela. Borrarla al soltar el bloqueo crearía una carrera: A termina, B
espera en el bloqueo viejo, C crea uno nuevo y corre a la vez que B.

Vale para un solo proceso del agente (uvicorn arranca hoy con uno); varios procesos necesitarían
otra coordinación. Sin dependencias de HTTP.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import AsyncIterator, Awaitable, Callable


@dataclass
class _Entrada:
    bloqueo: asyncio.Lock = field(default_factory=asyncio.Lock)
    usuarios: int = 0


class Sesiones:
    def __init__(self) -> None:
        self._entradas: dict[str, _Entrada] = {}

    def __len__(self) -> int:
        """Sesiones con algún turno en curso o esperando."""
        return len(self._entradas)

    @asynccontextmanager
    async def turno(self, session_id: str,
                    al_esperar: Callable[[], Awaitable[None]] | None = None) -> AsyncIterator[None]:
        """Entra cuando la sesión no tiene otro turno en curso. `al_esperar` se llama si hay que esperar."""
        entrada = self._entradas.setdefault(session_id, _Entrada())
        entrada.usuarios += 1  # antes de cualquier await: la entrada no se puede borrar mientras tanto
        try:
            if entrada.bloqueo.locked() and al_esperar is not None:
                await al_esperar()
            async with entrada.bloqueo:
                yield
        finally:
            entrada.usuarios -= 1
            if entrada.usuarios == 0:
                del self._entradas[session_id]
