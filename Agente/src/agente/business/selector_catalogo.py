"""Selector del catálogo: una llamada al LLM (temperatura 0) que elige consulta y parámetros en JSON.

Es el equivalente del redactor SQL (`business/redactor_sql.py`) en la opción del catálogo: mismo
cliente (`LLM_MODELO_SQL`), mismo mensaje (pregunta del usuario, la reescrita por el modelo de
conversación y el periodo calculado en Python) y mismo reintento con el error. La diferencia es la
salida: un objeto JSON con la consulta del catálogo y sus parámetros, nunca SQL. Lo interpreta y
construye `business/catalogo_sql.py`.

JSON en texto y no una llamada a herramienta: así sirve igual con cualquier modelo de Bedrock, sin
depender de que admita forzar la herramienta (`toolChoice`). Los fallos del LLM se propagan; la
herramienta los convierte en su resultado de fallo.
"""
from __future__ import annotations

from typing import Sequence

from llama_index.core.base.llms.types import ChatMessage, MessageRole
from llama_index.core.llms.function_calling import FunctionCallingLLM

from agente import observabilidad
from agente.business import frases
from agente.business.periodos import calendario, texto
from agente.entities.datos import ContextoDatos, Periodo


class SelectorCatalogo:
    def __init__(self, llm: FunctionCallingLLM):
        self._llm = llm

    async def elegir(self, pregunta: str, contexto: ContextoDatos, anterior: tuple[str, str] | None = None,
                     pregunta_usuario: str | None = None, periodos: Sequence[Periodo] = (),
                     sin_fechas: bool = False) -> str:
        """Texto de la respuesta (debe ser un objeto JSON). `anterior`: (respuesta, error) del intento
        fallido, si lo hay. El resto, como en `RedactorSQL.redactar`."""
        usuario = _mensaje(pregunta, pregunta_usuario, periodos, sin_fechas)
        if anterior is not None:  # en el mismo mensaje: Bedrock Converse exige alternar los roles
            respuesta, error = anterior
            usuario += "\n\n" + frases.REINTENTO_CATALOGO.format(respuesta=respuesta, error=error)
        mensajes = [ChatMessage(role=MessageRole.SYSTEM, content=self._prompt(contexto)),
                    ChatMessage(role=MessageRole.USER, content=usuario)]
        with observabilidad.llamada_llm(self._llm, mensajes, []) as span:
            respuesta = await self._llm.achat(mensajes)
            observabilidad.anotar_respuesta(span, respuesta)
        return (respuesta.message.content or "").strip()

    @staticmethod
    def _prompt(contexto: ContextoDatos) -> str:
        estaciones = "\n".join(f"{nombre} | {distrito}" for _, nombre, distrito in contexto.estaciones)
        return frases.PROMPT_SELECTOR_CATALOGO.format(
            estaciones=estaciones, hoy=contexto.hoy.isoformat(), primera=contexto.primera_fecha.isoformat(),
            ultima=contexto.ultima_fecha.isoformat(),
            calendario=calendario(contexto.hoy, contexto.primera_fecha, contexto.ultima_fecha))


def _mensaje(pregunta: str, pregunta_usuario: str | None, periodos: Sequence[Periodo], sin_fechas: bool) -> str:
    usuario = pregunta_usuario.strip() if pregunta_usuario and pregunta_usuario.strip() else pregunta.strip()
    reescrita = "" if usuario == pregunta.strip() else frases.PREGUNTA_REESCRITA_SQL.format(pregunta=pregunta.strip())
    if periodos:
        periodo = frases.PERIODO_CALCULADO_SELECTOR.format(periodo=texto(periodos))
    else:
        periodo = frases.PERIODO_HISTORICO_SELECTOR if sin_fechas else frases.PERIODO_SIN_CALCULAR_SELECTOR
    return frases.MENSAJE_SELECTOR.format(usuario=usuario, reescrita=reescrita, periodo=periodo)
