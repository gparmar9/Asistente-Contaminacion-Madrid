"""LLM falso para los tests: sigue un guion y registra cada llamada.

Imita la superficie del cliente OpenAI que usa el agente
(`cliente.chat.completions.create(...)` → `.choices[0].message`), sin red.
"""
from types import SimpleNamespace


def respuesta_llm(content: str | None = None, tool_calls: list | None = None):
    """Una respuesta del LLM: texto final o petición de tools."""
    mensaje = SimpleNamespace(content=content, tool_calls=tool_calls)
    return SimpleNamespace(choices=[SimpleNamespace(message=mensaje)])


def tool_call(id_llamada: str, nombre: str, argumentos: str):
    """Una tool_call como la produce el SDK (argumentos = JSON en texto)."""
    return SimpleNamespace(
        id=id_llamada,
        function=SimpleNamespace(name=nombre, arguments=argumentos),
    )


class ClienteLLMFalso:
    """Devuelve las respuestas del guion en orden; una excepción en el guion se lanza."""

    def __init__(self, guion: list):
        self.guion = list(guion)
        self.llamadas: list[dict] = []
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))

    def _create(self, **kwargs):
        self.llamadas.append(kwargs)
        if not self.guion:
            raise AssertionError("El guion del LLM falso se agotó: llamada inesperada")
        siguiente = self.guion.pop(0)
        if isinstance(siguiente, Exception):
            raise siguiente
        return siguiente
