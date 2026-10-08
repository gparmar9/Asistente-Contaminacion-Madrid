"""Contrato de herramienta del agente y su adaptador a LlamaIndex.

Reglas:
- Una herramienta nunca lanza: devuelve `ResultadoHerramienta` con `ok=False` y un
  `error` legible, que el bucle entrega al modelo como resultado de la llamada.
- La definición (nombre, descripción, esquema de parámetros) va en formato OpenAI
  (`{"type": "function", "function": {...}}`): es el formato que publica `rag.api` y
  el que LlamaIndex serializa para cualquier proveedor.
- `definicion()` puede devolver `None` cuando la herramienta no está disponible en
  este momento (p. ej. el servicio del que depende no responde): el bucle no la ofrece.
- Las herramientas las ejecuta el bucle, no LlamaIndex: el adaptador solo transporta
  la definición hacia el cliente del LLM.
"""
from __future__ import annotations

import copy
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from llama_index.core.tools import BaseTool, ToolMetadata

from agente.entities.chat import Fuente


@dataclass(frozen=True)
class ResultadoHerramienta:
    ok: bool
    datos: dict[str, Any] | None = None   # lo que ve el modelo si ok
    error: str | None = None              # lo que ve el modelo si no ok
    fuentes: tuple[Fuente, ...] = field(default=())  # de dónde salió la información
    internos: dict[str, Any] | None = None  # para el código, nunca para el modelo

    @staticmethod
    def exito(datos: dict[str, Any], fuentes: tuple[Fuente, ...] = (),
              internos: dict[str, Any] | None = None) -> "ResultadoHerramienta":
        return ResultadoHerramienta(ok=True, datos=datos, fuentes=fuentes, internos=internos)

    @staticmethod
    def fallo(error: str, internos: dict[str, Any] | None = None) -> "ResultadoHerramienta":
        return ResultadoHerramienta(ok=False, error=error, internos=internos)

    def para_el_modelo(self) -> str:
        """Texto del mensaje `tool`: JSON compacto, nunca vacío."""
        cuerpo = self.datos if self.ok else {"error": self.error}
        return json.dumps(cuerpo, ensure_ascii=False)


class Herramienta(ABC):
    nombre: str

    @abstractmethod
    async def definicion(self) -> dict | None:
        """Definición en formato OpenAI, o None si no se puede ofrecer ahora."""

    @abstractmethod
    async def ejecutar(self, argumentos: dict[str, Any]) -> ResultadoHerramienta:
        """Ejecuta la herramienta. Nunca lanza."""


# --------------------------------------------------------------------------- adaptador LlamaIndex

class _MetadatosCrudos(ToolMetadata):
    """ToolMetadata que devuelve un esquema JSON ya hecho en vez de derivarlo de Pydantic.

    Se devuelve una copia: `OpenAI._prepare_chat_with_tools` modifica el dict en sitio.
    """

    def __init__(self, nombre: str, descripcion: str, parametros: dict):
        super().__init__(description=descripcion, name=nombre, fn_schema=None)
        object.__setattr__(self, "_parametros", parametros)

    def get_parameters_dict(self) -> dict:
        return copy.deepcopy(self._parametros)


class _HerramientaLlamaIndex(BaseTool):
    def __init__(self, metadatos: ToolMetadata):
        self._metadatos = metadatos

    @property
    def metadata(self) -> ToolMetadata:
        return self._metadatos

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("Las herramientas las ejecuta el bucle del agente, no LlamaIndex")


def como_llamaindex(definicion: dict) -> BaseTool:
    """Convierte una definición OpenAI en el `BaseTool` que espera `chat_with_tools`."""
    funcion = definicion["function"]
    return _HerramientaLlamaIndex(
        _MetadatosCrudos(
            nombre=funcion["name"],
            descripcion=funcion.get("description", ""),
            parametros=funcion.get("parameters", {"type": "object", "properties": {}}),
        )
    )
