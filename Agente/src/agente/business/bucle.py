"""El turno del agente: bucle de herramientas escrito a mano sobre el cliente LLM de LlamaIndex.

    pregunta
      -> CLASIFICAR (intencion.py): intención y tema; si falla, DESCONOCIDA
      -> DECIDIR (código): frase fija y fin, o qué herramientas se ofrecen y con qué prompt
      -> vuelta 1..MAX: el modelo pide herramientas -> el código las ejecuta y añade el mensaje `tool`
      -> el modelo deja de pedir herramientas (o se agotan las vueltas) y:
           búsqueda obligada sin hacer -> la hace el código con la pregunta y el tema
           hubo evidencias del RAG     -> ruta documental (sintesis.py): JSON validado y passthrough
           no las hubo                 -> su texto es el final (o síntesis forzada sin herramientas)

Intervención: si una búsqueda devuelve `sin_evidencia` y el turno no tiene evidencias,
el código cierra el turno con una frase fija sin volver a llamar al modelo.

Las fases de sesión y comprobaciones se añaden encima de este esqueleto sin cambiar su forma.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Sequence

from llama_index.core.base.llms.types import ChatMessage, ChatResponse, MessageRole
from llama_index.core.llms.function_calling import FunctionCallingLLM
from llama_index.core.llms.llm import ToolSelection
from llama_index.core.tools.types import BaseTool

from agente.business import frases
from agente.business.intencion import Decision, clasificar, decidir
from agente.business.sintesis import EvidenciasTurno, sintesis_documental
from agente.entities.chat import Fuente
from agente.entities.intencion import DESCONOCIDA, Clasificacion, Tema
from agente.tools import rag
from agente.tools.base import Herramienta, ResultadoHerramienta, como_llamaindex

logger = logging.getLogger("agente.bucle")


class LLMNoDisponible(RuntimeError):
    """El proveedor del LLM falló (red, cuota, error interno)."""


@dataclass
class ResultadoTurno:
    respuesta: str
    fuentes: list[Fuente] = field(default_factory=list)
    advertencia: str | None = None
    intencion: str = DESCONOCIDA.intencion.value
    vueltas: int = 0                                   # llamadas al LLM con herramientas ofrecidas
    herramientas_usadas: list[str] = field(default_factory=list)
    busqueda_forzada: bool = False                     # la búsqueda obligada la lanzó el código
    sintesis_forzada: bool = False
    ruta: str = "libre"                                # libre | documental | sin_evidencia | fija


class Bucle:
    """`llm_clasificador` None = sin clasificador: todos los turnos son DESCONOCIDA."""

    def __init__(self, llm: FunctionCallingLLM, herramientas: Sequence[Herramienta],
                 max_vueltas: int = 3, llm_clasificador: FunctionCallingLLM | None = None,
                 clasificador_timeout_s: float = 10.0):
        self._llm = llm
        self._herramientas = {h.nombre: h for h in herramientas}
        self._max_vueltas = max(1, max_vueltas)
        self._llm_clasificador = llm_clasificador
        self._clasificador_timeout_s = clasificador_timeout_s

    async def responder(self, pregunta: str) -> ResultadoTurno:
        clasificacion = await self._clasificar(pregunta)
        decision = decidir(clasificacion.intencion)
        resultado = ResultadoTurno(respuesta="", intencion=clasificacion.intencion.value)
        if decision.frase:
            return _fija(resultado, decision.frase)
        disponibles = await self._herramientas_disponibles(decision)
        if decision.busqueda_obligada and rag.NOMBRE not in disponibles:
            return _fija(resultado, frases.DOCUMENTACION_NO_DISPONIBLE)

        mensajes = [
            ChatMessage(role=MessageRole.SYSTEM, content=decision.prompt),
            ChatMessage(role=MessageRole.USER, content=pregunta),
        ]
        evidencias = EvidenciasTurno()
        final: ChatResponse | None = None  # respuesta del modelo sin peticiones de herramienta

        for _ in range(self._max_vueltas):
            resultado.vueltas += 1
            respuesta = await self._llamar(mensajes, list(disponibles.values()))
            llamadas = self._llamadas_pedidas(respuesta) if disponibles else []
            if not llamadas:
                final = respuesta
                break
            mensajes.append(respuesta.message)
            hubo_sin_evidencia = False
            for llamada in llamadas:
                r, sin_evidencia = await self._ejecutar_y_anotar(llamada, disponibles, evidencias, resultado)
                hubo_sin_evidencia |= sin_evidencia
                mensajes.append(_mensaje_tool(llamada, r))
            if hubo_sin_evidencia and not evidencias:
                return _sin_evidencia(resultado)

        if decision.busqueda_obligada and not evidencias:
            # El modelo no buscó (o su búsqueda falló): busca el código. Su texto se descarta.
            resultado.busqueda_forzada = True
            argumentos = {"pregunta": pregunta}
            if clasificacion.tema is not Tema.NINGUNO:
                argumentos["tema"] = clasificacion.tema.value
            llamada = ToolSelection(tool_id="busqueda_forzada", tool_name=rag.NOMBRE, tool_kwargs=argumentos)
            r, sin_evidencia = await self._ejecutar_y_anotar(llamada, disponibles, evidencias, resultado)
            if sin_evidencia:
                return _sin_evidencia(resultado)
            if not r.ok:
                return _fija(resultado, frases.DOCUMENTACION_NO_DISPONIBLE)

        if evidencias:  # el texto libre del modelo se descarta: lo sustituye la ruta documental
            return await self._cerrar_documental(pregunta, evidencias, resultado)
        if final is not None:
            resultado.respuesta = _texto(final)
            return resultado
        # Límite de vueltas: cerrar sin herramientas para que el usuario siempre reciba respuesta.
        mensajes.append(ChatMessage(role=MessageRole.SYSTEM, content=frases.PROMPT_SINTESIS_FORZADA))
        respuesta = await self._llamar(mensajes, [])
        resultado.respuesta = _texto(respuesta)
        resultado.sintesis_forzada = True
        return resultado

    # ------------------------------------------------------------------ pasos

    async def _cerrar_documental(self, pregunta: str, evidencias: EvidenciasTurno,
                                 resultado: ResultadoTurno) -> ResultadoTurno:
        documental = await sintesis_documental(self._llamar, self._herramientas[rag.NOMBRE],
                                               pregunta, evidencias)
        resultado.ruta = "documental"
        resultado.respuesta = documental.respuesta
        resultado.advertencia = documental.advertencia
        _acumular_fuentes(resultado.fuentes, documental.fuentes)  # solo los documentos citados
        return resultado

    async def _clasificar(self, pregunta: str) -> Clasificacion:
        if self._llm_clasificador is None:
            return DESCONOCIDA
        return await clasificar(self._llm_clasificador, pregunta, self._clasificador_timeout_s)

    async def _herramientas_disponibles(self, decision: Decision) -> dict[str, BaseTool]:
        """Se ofrecen las herramientas que permite la decisión y cuya definición se pudo obtener ahora."""
        disponibles: dict[str, BaseTool] = {}
        for nombre, herramienta in self._herramientas.items():
            if decision.herramientas is not None and nombre not in decision.herramientas:
                continue
            definicion = await herramienta.definicion()
            if definicion is None:
                logger.warning("La herramienta %s no se ofrece en este turno", nombre)
                continue
            disponibles[nombre] = como_llamaindex(definicion)
        return disponibles

    async def _llamar(self, mensajes: list[ChatMessage], herramientas: list[BaseTool]) -> ChatResponse:
        """Una llamada al LLM, drenando el stream. Devuelve la última respuesta (mensaje completo)."""
        try:
            if herramientas:
                flujo = await self._llm.astream_chat_with_tools(herramientas, chat_history=list(mensajes))
            else:
                flujo = await self._llm.astream_chat(list(mensajes))
            ultima: ChatResponse | None = None
            async for trozo in flujo:
                ultima = trozo
        except Exception as exc:  # el proveedor puede lanzar de todo: se traduce a un error propio
            logger.error("Fallo del LLM: %s", exc)
            raise LLMNoDisponible(str(exc)) from exc
        if ultima is None:
            raise LLMNoDisponible("El LLM no devolvió ninguna respuesta")
        return ultima

    def _llamadas_pedidas(self, respuesta: ChatResponse) -> list[ToolSelection]:
        return self._llm.get_tool_calls_from_response(respuesta, error_on_no_tool_call=False)

    async def _ejecutar(self, llamada: ToolSelection, disponibles: dict[str, BaseTool]) -> ResultadoHerramienta:
        herramienta = self._herramientas.get(llamada.tool_name)
        if herramienta is None or llamada.tool_name not in disponibles:
            plantilla = (frases.ERROR_HERRAMIENTA_DESCONOCIDA if herramienta is None
                         else frases.ERROR_HERRAMIENTA_NO_PERMITIDA)
            return ResultadoHerramienta.fallo(plantilla.format(
                nombre=llamada.tool_name, disponibles=", ".join(disponibles) or "ninguna"))
        argumentos = llamada.tool_kwargs if isinstance(llamada.tool_kwargs, dict) else {}
        try:
            return await herramienta.ejecutar(argumentos)
        except Exception as exc:  # las herramientas no deben lanzar, pero el bucle no se cae si lo hacen
            logger.exception("La herramienta %s lanzó una excepción", llamada.tool_name)
            return ResultadoHerramienta.fallo(f"Error interno al ejecutar {llamada.tool_name}: {exc}")


    async def _ejecutar_y_anotar(self, llamada: ToolSelection, disponibles: dict[str, BaseTool],
                                 evidencias: EvidenciasTurno, resultado: ResultadoTurno,
                                 ) -> tuple[ResultadoHerramienta, bool]:
        """Ejecuta una llamada, incorpora las evidencias del RAG y la anota en el resultado.
        Devuelve el resultado (renumerado si trae evidencias) y si la búsqueda salió vacía."""
        r = await self._ejecutar(llamada, disponibles)
        sin_evidencia = False
        if llamada.tool_name == rag.NOMBRE and r.ok and r.internos:
            if r.internos["estado"] == "sin_evidencia":
                sin_evidencia = True
            else:
                r = evidencias.incorporar(r)
        resultado.herramientas_usadas.append(llamada.tool_name)
        _acumular_fuentes(resultado.fuentes, r.fuentes)
        return r, sin_evidencia


# ---------------------------------------------------------------------- utilidades

def _fija(resultado: ResultadoTurno, frase: str) -> ResultadoTurno:
    resultado.respuesta = frase
    resultado.ruta = "fija"
    return resultado


def _sin_evidencia(resultado: ResultadoTurno) -> ResultadoTurno:
    resultado.respuesta = frases.SIN_EVIDENCIA
    resultado.ruta = "sin_evidencia"
    return resultado


def _texto(respuesta: ChatResponse) -> str:
    contenido = (respuesta.message.content or "").strip()
    return contenido or frases.RESPUESTA_VACIA


def _mensaje_tool(llamada: ToolSelection, resultado: ResultadoHerramienta) -> ChatMessage:
    # `tool_call_id` es lo que OpenAI y Bedrock Converse necesitan para casar resultado y petición.
    return ChatMessage(
        role=MessageRole.TOOL,
        content=resultado.para_el_modelo(),
        additional_kwargs={"tool_call_id": llamada.tool_id, "name": llamada.tool_name},
    )


def _acumular_fuentes(destino: list[Fuente], nuevas: Sequence[Fuente]) -> None:
    for f in nuevas:
        if f not in destino:
            destino.append(f)
