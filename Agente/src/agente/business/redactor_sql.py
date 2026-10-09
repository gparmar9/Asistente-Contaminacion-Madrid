"""Redactor SQL: una llamada al LLM (temperatura 0) que convierte una pregunta en una consulta.

El cliente es el de `LLM_MODELO_SQL` (vacío = el mismo modelo del agente): comparar «mismo modelo»
frente a «modelo aparte» es un cambio de `.env`. El prompt lleva el esquema de la vista, las
estaciones, las fechas, el calendario y las reglas de agregación (`frases.PROMPT_REDACTOR_SQL`).

El mensaje lleva la pregunta del usuario (manda), la del modelo de conversación (lugares y
contaminantes) y el periodo calculado en Python (`business/periodos.py`): el redactor no calcula fechas.

En el reintento, el redactor recibe su SQL anterior y el error (del validador o de PostgreSQL).
No valida ni ejecuta: eso lo hace la herramienta (`tools/sql_libre.py`). Los fallos del LLM se
propagan; la herramienta los convierte en su resultado de fallo.
"""
from __future__ import annotations

import re
from typing import Sequence

from llama_index.core.base.llms.types import ChatMessage, MessageRole
from llama_index.core.llms.function_calling import FunctionCallingLLM

from agente import observabilidad
from agente.business import frases
from agente.business.periodos import calendario, condicion_sql
from agente.entities.datos import ContextoDatos, Periodo

# Algunos modelos envuelven el SQL en un bloque de código aunque el prompt lo prohíba.
_RE_BLOQUE = re.compile(r"^```(?:sql)?\s*|\s*```$", re.IGNORECASE)


class RedactorSQL:
    def __init__(self, llm: FunctionCallingLLM, max_filas: int = 60):
        self._llm = llm
        self._max_filas = max_filas

    async def redactar(self, pregunta: str, contexto: ContextoDatos, anterior: tuple[str, str] | None = None,
                       pregunta_usuario: str | None = None, periodos: Sequence[Periodo] = ()) -> str:
        """SQL para `pregunta` (la del modelo). `anterior`: (sql, error) del intento fallido, si lo hay.
        `pregunta_usuario`: la que escribió el usuario; sin ella, manda `pregunta`."""
        usuario = _mensaje(pregunta, pregunta_usuario, periodos)
        if anterior is not None:  # en el mismo mensaje: Bedrock Converse exige alternar los roles
            sql, error = anterior
            usuario += "\n\n" + frases.REINTENTO_SQL.format(sql=sql, error=error)
        mensajes = [ChatMessage(role=MessageRole.SYSTEM, content=self._prompt(contexto)),
                    ChatMessage(role=MessageRole.USER, content=usuario)]
        with observabilidad.llamada_llm(self._llm, mensajes, []) as span:
            respuesta = await self._llm.achat(mensajes)
            observabilidad.anotar_respuesta(span, respuesta)
        return _RE_BLOQUE.sub("", (respuesta.message.content or "").strip()).strip()

    def _prompt(self, contexto: ContextoDatos) -> str:
        # Sin el código: con él, los modelos copiaban la lista en VALUES o CASE en vez de usar la vista.
        estaciones = "\n".join(f"{nombre} | {distrito}" for _, nombre, distrito in contexto.estaciones)
        return frases.PROMPT_REDACTOR_SQL.format(
            estaciones=estaciones, hoy=contexto.hoy.isoformat(), primera=contexto.primera_fecha.isoformat(),
            ultima=contexto.ultima_fecha.isoformat(), max_filas=self._max_filas,
            calendario=calendario(contexto.hoy, contexto.primera_fecha, contexto.ultima_fecha, condicion_sql))


def _mensaje(pregunta: str, pregunta_usuario: str | None, periodos: Sequence[Periodo]) -> str:
    usuario = pregunta_usuario.strip() if pregunta_usuario and pregunta_usuario.strip() else pregunta.strip()
    reescrita = "" if usuario == pregunta.strip() else frases.PREGUNTA_REESCRITA_SQL.format(pregunta=pregunta.strip())
    periodo = condicion_sql(periodos) if periodos else frases.PERIODO_SIN_CALCULAR
    return frases.MENSAJE_REDACTOR_SQL.format(usuario=usuario, reescrita=reescrita, periodo=periodo)
