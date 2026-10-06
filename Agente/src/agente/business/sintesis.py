"""Ruta documental: evidencias del turno y síntesis JSON validada por el RAG.

    evidencias de una o varias búsquedas -> renumeradas D1..Dn sin repetir chunk_id
      -> LLM sin herramientas devuelve {estado, afirmaciones, limitaciones}
      -> POST /rag/validar: si no es válida, una reparación con su mensaje
      -> válida: passthrough del texto que renderiza el RAG; si no, frase de insuficiencia

El modelo nunca escribe el texto final con citas: lo renderiza el RAG.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field, replace
from typing import Any, Awaitable, Callable

from llama_index.core.base.llms.types import ChatMessage, ChatResponse, MessageRole

from agente import observabilidad
from agente.business import frases
from agente.entities.chat import Fuente
from agente.entities.eventos import Emisor, EventoEstado, Fase
from agente.tools.base import ResultadoHerramienta
from agente.tools.rag import HerramientaRag

logger = logging.getLogger("agente.sintesis")

MAX_REPARACIONES = 1
_RE_BLOQUE_CODIGO = re.compile(r"^```(?:json)?\s*|\s*```$")

Llamar = Callable[[list[ChatMessage], list], Awaitable[ChatResponse]]


class EvidenciasTurno:
    """Evidencias acumuladas en el turno. Cada búsqueda del RAG numera desde D1; aquí se
    renumeran en orden de llegada para que el modelo y `/rag/validar` vean IDs únicos."""

    def __init__(self) -> None:
        self._id_por_chunk: dict[str, str] = {}
        self._para_sintesis: list[dict] = []

    def __bool__(self) -> bool:
        return bool(self._id_por_chunk)

    def incorporar(self, resultado: ResultadoHerramienta) -> ResultadoHerramienta:
        """Renumera una búsqueda con evidencias y devuelve el resultado que verá el modelo."""
        chunk_por_id = {e["id"]: e["chunk_id"] for e in resultado.internos["evidencias"]}
        renumeradas = []
        for e in resultado.datos["evidencias"]:
            chunk_id = chunk_por_id[e["id"]]
            nuevo_id = self._id_por_chunk.get(chunk_id)
            if nuevo_id is None:
                nuevo_id = f"D{len(self._id_por_chunk) + 1}"
                self._id_por_chunk[chunk_id] = nuevo_id
                self._para_sintesis.append({**e, "id": nuevo_id})
            renumeradas.append({**e, "id": nuevo_id})
        return replace(resultado, datos={**resultado.datos, "evidencias": renumeradas})

    def referencias(self) -> list[dict]:
        """[{id, chunk_id}] para `/rag/validar`."""
        return [{"id": i, "chunk_id": c} for c, i in self._id_por_chunk.items()]

    def para_sintesis(self) -> list[dict]:
        return list(self._para_sintesis)


@dataclass
class ResultadoDocumental:
    respuesta: str
    fuentes: list[Fuente] = field(default_factory=list)
    advertencia: str | None = None
    valida: bool = False
    reparaciones: int = 0


async def sintesis_documental(llamar: Llamar, rag: HerramientaRag, pregunta: str,
                              evidencias: EvidenciasTurno, emitir: Emisor) -> ResultadoDocumental:
    """`emitir` solo recibe las fases: el JSON de la síntesis nunca sale como tokens."""
    with observabilidad.span("sintesis_documental", "chain") as span:
        resultado = await _sintesis(llamar, rag, pregunta, evidencias, emitir)
        span.set_attributes({"agente.valida": resultado.valida, "agente.reparaciones": resultado.reparaciones})
        if not resultado.valida:
            observabilidad.decision("insuficiencia")
        return resultado


async def _sintesis(llamar: Llamar, rag: HerramientaRag, pregunta: str,
                    evidencias: EvidenciasTurno, emitir: Emisor) -> ResultadoDocumental:
    mensajes = _mensajes_sintesis(pregunta, evidencias, rag.esquema_salida)
    for intento in range(MAX_REPARACIONES + 1):
        await emitir(EventoEstado(Fase.REDACTANDO))
        respuesta = await llamar(mensajes, [])
        salida_texto = respuesta.message.content or ""
        await emitir(EventoEstado(Fase.VALIDANDO))
        validacion = await rag.validar(pregunta, evidencias.referencias(), _parsear(salida_texto))
        if validacion is None:
            break
        if validacion["valida"]:
            return ResultadoDocumental(
                respuesta=validacion["texto"],
                fuentes=validacion["fuentes"],
                advertencia=frases.AVISO_SANITARIO if validacion["aviso_sanitario"] else None,
                valida=True,
                reparaciones=intento,
            )
        logger.warning("Salida documental inválida (intento %d): %s", intento + 1,
                    validacion["mensaje_reparacion"])
        mensajes = mensajes + [
            ChatMessage(role=MessageRole.ASSISTANT, content=salida_texto),
            ChatMessage(role=MessageRole.USER, content=validacion["mensaje_reparacion"]),
        ]
    return ResultadoDocumental(respuesta=frases.INSUFICIENCIA, reparaciones=intento)


def _mensajes_sintesis(pregunta: str, evidencias: EvidenciasTurno,
                       esquema: dict | None) -> list[ChatMessage]:
    # Mensajes nuevos, sin el historial de herramientas: la síntesis no tiene herramientas y
    # Bedrock Converse rechaza bloques toolUse/toolResult sin toolConfig.
    sistema = frases.PROMPT_SINTESIS_DOCUMENTAL
    if esquema:
        sistema += "\n\nEsquema JSON de la respuesta:\n" + json.dumps(esquema, ensure_ascii=False)
    usuario = (f"Pregunta: {pregunta}\n\nEvidencias:\n"
               + json.dumps(evidencias.para_sintesis(), ensure_ascii=False, indent=1))
    return [ChatMessage(role=MessageRole.SYSTEM, content=sistema),
            ChatMessage(role=MessageRole.USER, content=usuario)]


def _parsear(texto: str) -> Any:
    """JSON de la salida del modelo. Si no lo es, se devuelve el texto tal cual: `/rag/validar`
    lo rechaza con un mensaje de reparación, igual que cualquier otro error de contrato."""
    try:
        return json.loads(_RE_BLOQUE_CODIGO.sub("", texto.strip()))
    except ValueError:
        return texto
