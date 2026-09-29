"""Contrato de POST /responder.

Es el mismo contrato que reenvía `ApiUsuario` por `/chat` (duplicado a
propósito: los dos servicios son autocontenidos y el contrato es la costura).
"""
from typing import Literal

from pydantic import BaseModel, Field


class PreguntaChat(BaseModel):
    pregunta: str = Field(min_length=1, max_length=2000,
                          description="Pregunta del usuario en lenguaje natural")


class FuenteChat(BaseModel):
    """De dónde salió la información: consulta SQL o documento del corpus RAG."""

    tipo: Literal["sql", "documento"]
    referencia: str


class RespuestaChat(BaseModel):
    respuesta: str
    fuentes: list[FuenteChat] = []
    advertencia: str | None = None
