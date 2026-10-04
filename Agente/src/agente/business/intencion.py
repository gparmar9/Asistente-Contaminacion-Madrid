"""El modelo clasifica, el código decide.

- `clasificar()`: una llamada corta al LLM (temperatura 0) que responde en dos líneas
  `intencion: ...` y `tema: ...`. Parser estricto: cualquier desviación, error o tiempo
  agotado da el valor seguro `DESCONOCIDA`.
- `decidir()`: tabla en código con lo que se permite en el turno según la intención:
  herramientas ofrecidas, búsqueda obligatoria, frase fija o prompt del bucle.

`DESCONOCIDA` ofrece todas las herramientas, no obliga ninguna y no usa frases fijas:
un clasificador caído no debe quitar herramientas ni dar respuestas enlatadas.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from llama_index.core.base.llms.types import ChatMessage, MessageRole
from llama_index.core.llms.function_calling import FunctionCallingLLM

from agente.business import frases
from agente.entities.intencion import DESCONOCIDA, Clasificacion, Intencion, Tema
from agente.tools import rag

logger = logging.getLogger("agente.intencion")

_CLAVES = {"intencion": "intencion", "intención": "intencion", "tema": "tema"}


async def clasificar(llm: FunctionCallingLLM, pregunta: str, timeout_s: float) -> Clasificacion:
    mensajes = [
        ChatMessage(role=MessageRole.SYSTEM, content=frases.PROMPT_CLASIFICADOR),
        ChatMessage(role=MessageRole.USER, content=pregunta),
    ]
    try:
        respuesta = await asyncio.wait_for(llm.achat(mensajes), timeout=timeout_s)
    except Exception as exc:  # caída, cuota o tiempo agotado: el turno sigue con el valor seguro
        logger.warning("Clasificador no disponible (%s): se usa DESCONOCIDA", type(exc).__name__)
        return DESCONOCIDA
    clasificacion = interpretar(respuesta.message.content or "")
    if clasificacion is None:
        logger.warning("Respuesta del clasificador con formato inválido: %r", respuesta.message.content)
        return DESCONOCIDA
    return clasificacion


def interpretar(texto: str) -> Clasificacion | None:
    """Exactamente dos líneas `clave: valor` con valores conocidos; si no, None."""
    lineas = [ln.strip() for ln in texto.strip().splitlines() if ln.strip()]
    if len(lineas) != 2:
        return None
    valores: dict[str, str] = {}
    for linea in lineas:
        clave, separador, valor = linea.partition(":")
        clave = _CLAVES.get(clave.strip().lower())
        if not separador or clave is None or clave in valores:
            return None
        valores[clave] = valor.strip()
    try:
        intencion = Intencion(valores["intencion"].upper())
        tema = Tema(valores["tema"].lower())
    except (KeyError, ValueError):
        return None
    if intencion is Intencion.DESCONOCIDA:  # reservada para el código
        return None
    return Clasificacion(intencion, tema)


# ---------------------------------------------------------------------- decisión

@dataclass(frozen=True)
class Decision:
    frase: str | None = None                     # respuesta fija: el turno se cierra sin modelo
    herramientas: frozenset[str] | None = None   # las que se pueden ofrecer; None = todas
    busqueda_obligada: bool = False              # si el modelo no busca en el RAG, busca el código
    prompt: str = frases.PROMPT_SISTEMA


_DECISIONES = {
    Intencion.DOCUMENTAL: Decision(herramientas=frozenset({rag.NOMBRE}), busqueda_obligada=True),
    Intencion.DATOS: Decision(frase=frases.FRASE_DATOS),
    Intencion.PREDICCION: Decision(frase=frases.FRASE_PREDICCION),
    Intencion.CHARLA: Decision(herramientas=frozenset(), prompt=frases.PROMPT_CHARLA),
    Intencion.FUERA_DE_ALCANCE: Decision(frase=frases.FRASE_FUERA_DE_ALCANCE),
    Intencion.DESCONOCIDA: Decision(),
}


def decidir(intencion: Intencion) -> Decision:
    return _DECISIONES[intencion]
