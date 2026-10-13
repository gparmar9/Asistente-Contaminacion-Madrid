"""Contrato HTTP del agente: lo que entra del usuario y lo que se devuelve.

Es el mismo contrato que `ApiUsuario` reenvía desde `/chat`, ampliado con campos opcionales.
`session_id` es opaco: no identifica a nadie, solo agrupa los turnos de una conversación.
"""
from typing import Literal

from pydantic import BaseModel, Field


class Pregunta(BaseModel):
    pregunta: str = Field(min_length=1, max_length=2000,
                          description="Pregunta del usuario en lenguaje natural")
    session_id: str | None = Field(default=None, min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_-]+$",
                                   description="Sesión de la conversación; si no llega, se genera una")


class Fuente(BaseModel):
    """De dónde salió la información: consulta SQL o documento del corpus RAG."""

    tipo: Literal["sql", "documento"]
    referencia: str


class Respuesta(BaseModel):
    respuesta: str
    fuentes: list[Fuente] = []
    advertencia: str | None = None
    traza_id: str | None = Field(default=None, description="Traza del turno en Phoenix y en el JSONL")
    session_id: str = Field(description="Sesión del turno: la recibida o la generada")
