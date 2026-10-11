"""Un turno completado de una conversación, tal como se guarda en la memoria de la sesión.

Solo datos. `respuesta` es lo que vio el usuario; `respuesta_contexto`, lo que se usa como
historial en los turnos siguientes (en la ruta documental, las afirmaciones sin `[Dn]`, sin aviso
ni bibliografía). Los metadatos no llegan nunca al modelo.
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone

from agente.entities.chat import Fuente


@dataclass(frozen=True)
class TurnoGuardado:
    pregunta: str
    respuesta: str
    respuesta_contexto: str
    intencion: str = ""
    ruta: str = ""
    fuentes: tuple[Fuente, ...] = ()
    advertencia: str | None = None
    traza_id: str | None = None
    momento: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
