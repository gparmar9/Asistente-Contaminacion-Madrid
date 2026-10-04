"""LLM falso con guion para los tests: sin red, registra cada llamada.

Hereda de `FunctionCallingLLM`, así el bucle lo usa exactamente igual que a un
proveedor real (`astream_chat_with_tools`, `get_tool_calls_from_response`).

Guion: lista de `ChatMessage` de asistente (ver `texto()` y `llamada()`) o de
excepciones, que se consumen en orden. Cada llamada queda en `registro` con los
mensajes recibidos y los nombres de las herramientas ofrecidas.
"""
from __future__ import annotations

import json
from typing import Any, Sequence

from llama_index.core.base.llms.types import (
    ChatMessage,
    ChatResponse,
    ChatResponseAsyncGen,
    ChatResponseGen,
    CompletionResponse,
    CompletionResponseAsyncGen,
    CompletionResponseGen,
    LLMMetadata,
    MessageRole,
    TextBlock,
    ToolCallBlock,
)
from llama_index.core.bridge.pydantic import Field
from llama_index.core.llms.function_calling import FunctionCallingLLM
from llama_index.core.llms.llm import ToolSelection
from llama_index.core.tools.types import BaseTool


def texto(contenido: str) -> ChatMessage:
    """Respuesta final del modelo (sin herramientas)."""
    return ChatMessage(role=MessageRole.ASSISTANT, content=contenido)


def llamada(nombre: str, argumentos: dict[str, Any], id_llamada: str = "call_1",
            *otras: tuple[str, dict[str, Any], str]) -> ChatMessage:
    """El modelo pide una o varias herramientas; `argumentos` viaja como JSON en texto."""
    bloques = [ToolCallBlock(tool_call_id=id_llamada, tool_name=nombre,
                             tool_kwargs=json.dumps(argumentos, ensure_ascii=False))]
    for n, a, i in otras:
        bloques.append(ToolCallBlock(tool_call_id=i, tool_name=n,
                                     tool_kwargs=json.dumps(a, ensure_ascii=False)))
    return ChatMessage(role=MessageRole.ASSISTANT, blocks=bloques)


class LLMFalso(FunctionCallingLLM):
    guion: list[Any] = Field(default_factory=list)
    registro: list[dict[str, Any]] = Field(default_factory=list)
    trozos: int = Field(default=2, description="En cuántos deltas se emite cada texto")

    @classmethod
    def class_name(cls) -> str:
        return "LLMFalso"

    @property
    def metadata(self) -> LLMMetadata:
        return LLMMetadata(model_name="falso", is_chat_model=True, is_function_calling_model=True,
                           context_window=32_000, num_output=1_024)

    # ----------------------------------------------------------------- guion

    def _siguiente(self, messages: Sequence[ChatMessage], kwargs: dict[str, Any]) -> ChatMessage:
        herramientas = [t["function"]["name"] for t in (kwargs.get("tools") or [])]
        self.registro.append({
            "mensajes": list(messages),
            "herramientas": herramientas,
            "kwargs": {k: v for k, v in kwargs.items() if k not in ("tools", "messages")},
        })
        if not self.guion:
            raise AssertionError("El guion del LLM falso se agotó: llamada inesperada")
        siguiente = self.guion.pop(0)
        if isinstance(siguiente, Exception):
            raise siguiente
        return siguiente

    def _deltas(self, mensaje: ChatMessage) -> list[ChatResponse]:
        contenido = mensaje.content or ""
        if not contenido or self.trozos <= 1:
            return [ChatResponse(message=mensaje, delta=contenido)]
        paso = max(1, -(-len(contenido) // self.trozos))  # división hacia arriba
        respuestas = []
        for i in range(0, len(contenido), paso):
            parcial = contenido[: i + paso]
            respuestas.append(ChatResponse(
                message=ChatMessage(role=MessageRole.ASSISTANT, content=parcial),
                delta=contenido[i: i + paso],
            ))
        respuestas[-1] = ChatResponse(message=mensaje, delta=respuestas[-1].delta)
        return respuestas

    # ----------------------------------------------------------------- tools

    def _prepare_chat_with_tools(self, tools: Sequence[BaseTool],
                                 user_msg: str | ChatMessage | None = None,
                                 chat_history: list[ChatMessage] | None = None,
                                 verbose: bool = False, allow_parallel_tool_calls: bool = False,
                                 tool_required: bool = False, **kwargs: Any) -> dict[str, Any]:
        if isinstance(user_msg, str):
            user_msg = ChatMessage(role=MessageRole.USER, content=user_msg)
        messages = list(chat_history or [])
        if user_msg:
            messages.append(user_msg)
        return {
            "messages": messages,
            "tools": [t.metadata.to_openai_tool(skip_length_check=True) for t in tools],
            **kwargs,
        }

    def get_tool_calls_from_response(self, response: ChatResponse,
                                     error_on_no_tool_call: bool = True, **kwargs: Any) -> list[ToolSelection]:
        bloques = [b for b in response.message.blocks if isinstance(b, ToolCallBlock)]
        if not bloques and error_on_no_tool_call:
            raise ValueError("Se esperaba al menos una llamada a herramienta")
        selecciones = []
        for b in bloques:
            argumentos = b.tool_kwargs
            if isinstance(argumentos, str):
                try:
                    argumentos = json.loads(argumentos)
                except ValueError:
                    argumentos = {}
            selecciones.append(ToolSelection(tool_id=b.tool_call_id or "", tool_name=b.tool_name,
                                             tool_kwargs=argumentos))
        return selecciones

    # ----------------------------------------------------------------- chat

    def chat(self, messages: Sequence[ChatMessage], **kwargs: Any) -> ChatResponse:
        return ChatResponse(message=self._siguiente(messages, kwargs))

    async def achat(self, messages: Sequence[ChatMessage], **kwargs: Any) -> ChatResponse:
        return self.chat(messages, **kwargs)

    def stream_chat(self, messages: Sequence[ChatMessage], **kwargs: Any) -> ChatResponseGen:
        respuestas = self._deltas(self._siguiente(messages, kwargs))

        def gen():
            yield from respuestas
        return gen()

    async def astream_chat(self, messages: Sequence[ChatMessage], **kwargs: Any) -> ChatResponseAsyncGen:
        respuestas = self._deltas(self._siguiente(messages, kwargs))

        async def gen():
            for r in respuestas:
                yield r
        return gen()

    # ----------------------------------------------------------------- complete (no se usa)

    def complete(self, prompt: str, formatted: bool = False, **kwargs: Any) -> CompletionResponse:
        mensaje = self._siguiente([ChatMessage(role=MessageRole.USER, content=prompt)], kwargs)
        return CompletionResponse(text=mensaje.content or "")

    async def acomplete(self, prompt: str, formatted: bool = False, **kwargs: Any) -> CompletionResponse:
        return self.complete(prompt, formatted, **kwargs)

    def stream_complete(self, prompt: str, formatted: bool = False, **kwargs: Any) -> CompletionResponseGen:
        respuesta = self.complete(prompt, formatted, **kwargs)

        def gen():
            yield respuesta
        return gen()

    async def astream_complete(self, prompt: str, formatted: bool = False, **kwargs: Any) -> CompletionResponseAsyncGen:
        respuesta = self.complete(prompt, formatted, **kwargs)

        async def gen():
            yield respuesta
        return gen()


__all__ = ["LLMFalso", "texto", "llamada", "TextBlock"]
