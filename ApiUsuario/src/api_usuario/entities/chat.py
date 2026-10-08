"""Contrato del chat: lo que entra del usuario y lo que devuelve el asistente.

La respuesta replica el contrato de `POST /responder` del servicio Agente (o del
LLMOrchestrator): `ApiUsuario` solo hace de proxy y no añade nada. `session_id` es
opaco: no identifica a nadie, solo agrupa los turnos de una conversación.
"""
from typing import Literal

from pydantic import BaseModel, Field


class PreguntaChat(BaseModel):
    pregunta: str = Field(min_length=1, max_length=2000,
                          description="Pregunta del usuario en lenguaje natural")
    session_id: str | None = Field(default=None, min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_-]+$",
                                   description="Sesión de la conversación; si no llega, se genera una")


class FuenteChat(BaseModel):
    """De dónde salió la información: consulta SQL o documento del corpus RAG."""

    tipo: Literal["sql", "documento"]
    referencia: str


class RespuestaChat(BaseModel):
    respuesta: str
    fuentes: list[FuenteChat] = []
    advertencia: str | None = None
    session_id: str | None = None
    traza_id: str | None = Field(default=None, description="Traza del turno en el agente; null sin agente")
