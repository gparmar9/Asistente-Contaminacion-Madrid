"""Contrato HTTP del agente: lo que entra del usuario y lo que se devuelve.

Es el mismo contrato que `ApiUsuario` reenvía desde `/chat`. Los campos de sesión
y los eventos de streaming llegarán en fases posteriores como campos opcionales.
"""
from typing import Literal

from pydantic import BaseModel, Field


class Pregunta(BaseModel):
    pregunta: str = Field(min_length=1, max_length=2000,
                          description="Pregunta del usuario en lenguaje natural")


class Fuente(BaseModel):
    """De dónde salió la información: consulta SQL o documento del corpus RAG."""

    tipo: Literal["sql", "documento"]
    referencia: str


class Respuesta(BaseModel):
    respuesta: str
    fuentes: list[Fuente] = []
    advertencia: str | None = None
    traza_id: str | None = Field(default=None, description="Traza del turno en Phoenix y en el JSONL")
