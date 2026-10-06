"""Qué parte de la conversación se manda al modelo y en qué forma.

El historial interpreta, las evidencias fundamentan: los turnos previos ayudan a entender un
seguimiento («¿Y en niños?»), pero no son fuente de nada.

- `ventana()`: turnos completos más recientes que caben en el presupuesto de tokens.
- `mensajes_historial()`: pares `user`/`assistant` solo con texto, para el bucle.
- `texto_historial()`: la conversación como bloque de texto, para el clasificador y las síntesis.
- `pregunta_contextual()`: las dos preguntas anteriores y la actual, para la búsqueda que lanza el código.

Nada de esto toca el almacén (`datos/memoria.py`) ni los metadatos del turno.
"""
from __future__ import annotations

import math
from typing import Sequence

from llama_index.core.base.llms.types import ChatMessage, MessageRole

from agente.entities.memoria import TurnoGuardado

CARACTERES_POR_TOKEN = 4  # estimación: no hay tokenizador local para los modelos de Bedrock
PREGUNTAS_PREVIAS_BUSQUEDA = 2


def tokens_estimados(turno: TurnoGuardado) -> int:
    return math.ceil((len(turno.pregunta) + len(turno.respuesta_contexto)) / CARACTERES_POR_TOKEN)


def ventana(turnos: Sequence[TurnoGuardado], presupuesto_tokens: int) -> list[TurnoGuardado]:
    """Del más reciente hacia atrás, turnos completos mientras quepan. Orden cronológico.
    Si el más reciente ya no cabe, vacía: el turno va sin historial."""
    elegidos: list[TurnoGuardado] = []
    usados = 0
    for turno in reversed(turnos):
        usados += tokens_estimados(turno)
        if usados > presupuesto_tokens:
            break
        elegidos.append(turno)
    return elegidos[::-1]


def mensajes_historial(turnos: Sequence[TurnoGuardado]) -> list[ChatMessage]:
    """[USER, ASSISTANT, ...]: empieza por `user` y alterna, como exige Bedrock Converse."""
    mensajes: list[ChatMessage] = []
    for turno in turnos:
        mensajes.append(ChatMessage(role=MessageRole.USER, content=turno.pregunta))
        mensajes.append(ChatMessage(role=MessageRole.ASSISTANT, content=turno.respuesta_contexto))
    return mensajes


def texto_historial(turnos: Sequence[TurnoGuardado], max_turnos: int | None = None,
                    max_car_respuesta: int | None = None) -> str:
    """«Usuario: …» / «Asistente: …» de los últimos `max_turnos`, con la respuesta recortada."""
    if max_turnos is not None:
        turnos = turnos[-max_turnos:] if max_turnos > 0 else []
    lineas = []
    for turno in turnos:
        respuesta = turno.respuesta_contexto
        if max_car_respuesta is not None and len(respuesta) > max_car_respuesta:
            respuesta = respuesta[:max_car_respuesta].rstrip() + "…"
        lineas += [f"Usuario: {turno.pregunta}", f"Asistente: {respuesta}"]
    return "\n".join(lineas)


def pregunta_contextual(turnos: Sequence[TurnoGuardado], pregunta: str) -> str:
    """Con una sola pregunta previa, la cadena NO2 → niños → largo plazo perdería el NO2 en el tercer turno."""
    previas = [t.pregunta for t in turnos[-PREGUNTAS_PREVIAS_BUSQUEDA:]] if turnos else []
    return " ".join([*previas, pregunta])
