"""El modelo clasifica, el código decide.

- `clasificar()`: una llamada corta al LLM (temperatura 0) que responde en dos líneas
  `intencion: ...` y `tema: ...`; la intención puede ser una o dos separadas por coma
  (`DATOS, DOCUMENTAL`). Parser estricto: cualquier desviación (valor desconocido, tres o más,
  repetidos), error o tiempo agotado da el valor seguro `DESCONOCIDA`. Solo tolera adornos de
  markdown (`*`, `` ` ``): Ministral 14B en Bedrock escribe los valores en negrita aunque el
  prompt lo prohíba.
- `decidir()`: tabla en código con lo que se permite en el turno según la intención:
  herramientas ofrecidas, búsqueda obligatoria, frase fija o prompt del bucle. Con dos
  intenciones se combinan: herramientas y obligaciones se suman; la frase fija solo si las dos
  la tienen; si solo una la tiene (`DATOS, PREDICCION`), se responde la otra y su nota va al final.

Con conversación previa, el clasificador recibe los dos últimos turnos (respuestas recortadas)
como bloque de texto antes de la pregunta actual. Sin ella, el mensaje es solo la pregunta.

`DESCONOCIDA` ofrece todas las herramientas, no obliga ninguna y no usa frases fijas:
un clasificador caído no debe quitar herramientas ni dar respuestas enlatadas.
`DATOS` ofrece y obliga la consulta de mediciones; sin base de datos, frase fija (en el bucle).
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, replace
from typing import Sequence

from llama_index.core.base.llms.types import ChatMessage, MessageRole
from llama_index.core.llms.function_calling import FunctionCallingLLM

from agente import observabilidad
from agente.business import frases
from agente.business.memoria import texto_historial
from agente.entities.intencion import DESCONOCIDA, Clasificacion, Intencion, Tema
from agente.entities.memoria import TurnoGuardado
from agente.tools import rag, sql_libre

logger = logging.getLogger("agente.intencion")

_CLAVES = {"intencion": "intencion", "intención": "intencion", "tema": "tema"}
# Adornos que se quitan antes de comparar. No el guion bajo: forma parte de FUERA_DE_ALCANCE.
_ADORNOS = str.maketrans("", "", "*`")
# Conversación previa que ve el clasificador: corta, para que siga siendo una llamada rápida.
HISTORIAL_TURNOS = 2
HISTORIAL_CAR_RESPUESTA = 300
MAX_INTENCIONES = 2


async def clasificar(llm: FunctionCallingLLM, pregunta: str, timeout_s: float,
                     historial: Sequence[TurnoGuardado] = ()) -> Clasificacion:
    mensajes = _mensajes(pregunta, historial)
    with observabilidad.span("clasificar", "chain", entrada=pregunta) as span:
        try:
            with observabilidad.llamada_llm(llm, mensajes, []) as span_llm:
                respuesta = await asyncio.wait_for(llm.achat(mensajes), timeout=timeout_s)
                observabilidad.anotar_respuesta(span_llm, respuesta)
        except Exception as exc:  # caída, cuota o tiempo agotado: el turno sigue con el valor seguro
            logger.warning("Clasificador no disponible (%s): se usa DESCONOCIDA", type(exc).__name__)
            span.set_attribute("agente.clasificacion_valida", False)
            observabilidad.decision("clasificador_no_disponible")
            return DESCONOCIDA
        span.set_output(respuesta.message.content or "")
        clasificacion = interpretar(respuesta.message.content or "")
        span.set_attribute("agente.clasificacion_valida", clasificacion is not None)
        if clasificacion is None:
            logger.warning("Respuesta del clasificador con formato inválido: %r", respuesta.message.content)
            return DESCONOCIDA
        return clasificacion


def _mensajes(pregunta: str, historial: Sequence[TurnoGuardado]) -> list[ChatMessage]:
    if not historial:
        return [ChatMessage(role=MessageRole.SYSTEM, content=frases.PROMPT_CLASIFICADOR),
                ChatMessage(role=MessageRole.USER, content=pregunta)]
    previa = texto_historial(historial, HISTORIAL_TURNOS, HISTORIAL_CAR_RESPUESTA)
    return [ChatMessage(role=MessageRole.SYSTEM,
                        content=frases.PROMPT_CLASIFICADOR + frases.PROMPT_CLASIFICADOR_CONTEXTO),
            ChatMessage(role=MessageRole.USER,
                        content=f"{frases.CONVERSACION_PREVIA}\n{previa}\n\nPregunta actual: {pregunta}")]


def interpretar(texto: str) -> Clasificacion | None:
    """Exactamente dos líneas `clave: valor` con valores conocidos (una o dos intenciones); si no, None."""
    texto = texto.translate(_ADORNOS)
    lineas = [ln.strip() for ln in texto.strip().splitlines() if ln.strip()]
    if len(lineas) != 2:
        return None
    valores: dict[str, str] = {}
    for linea in lineas:
        clave, separador, valor = linea.partition(":")
        clave = _CLAVES.get(clave.strip().lower())
        if not separador or clave is None or clave in valores:
            return None
        valores[clave] = valor.strip()
    try:
        nombres = [v.strip().upper() for v in valores["intencion"].split(",")]
        intenciones = frozenset(Intencion(n) for n in nombres)
        tema = Tema(valores["tema"].lower())
    except (KeyError, ValueError):
        return None
    if not 1 <= len(nombres) <= MAX_INTENCIONES or len(intenciones) != len(nombres):
        return None
    if Intencion.DESCONOCIDA in intenciones:  # reservada para el código
        return None
    return Clasificacion(intenciones, tema)


# ---------------------------------------------------------------------- decisión

@dataclass(frozen=True)
class Decision:
    frase: str | None = None                     # respuesta fija: el turno se cierra sin modelo
    herramientas: frozenset[str] | None = None   # las que se pueden ofrecer; None = todas
    busqueda_obligada: bool = False              # si el modelo no busca en el RAG, busca el código
    datos_obligados: bool = False                # si el modelo no consulta los datos, consulta el código
    prompt: str = frases.PROMPT_SISTEMA
    nota: str | None = None                      # se añade al final de la respuesta (DATOS, PREDICCION)


_DECISIONES = {
    Intencion.DOCUMENTAL: Decision(herramientas=frozenset({rag.NOMBRE}), busqueda_obligada=True),
    Intencion.DATOS: Decision(herramientas=frozenset({sql_libre.NOMBRE}), datos_obligados=True,
                              prompt=frases.PROMPT_DATOS),
    Intencion.PREDICCION: Decision(frase=frases.FRASE_PREDICCION),
    Intencion.CHARLA: Decision(herramientas=frozenset(), prompt=frases.PROMPT_CHARLA),
    Intencion.FUERA_DE_ALCANCE: Decision(frase=frases.FRASE_FUERA_DE_ALCANCE),
    Intencion.DESCONOCIDA: Decision(),
}


# Nota de las intenciones con frase fija cuando van con otra que sí se responde.
_NOTAS = {Intencion.PREDICCION: frases.NOTA_PREDICCION,
          Intencion.FUERA_DE_ALCANCE: frases.NOTA_FUERA_DE_ALCANCE}


def decidir(intenciones: frozenset[Intencion]) -> Decision:
    ordenadas = [i for i in Intencion if i in intenciones]
    con_frase = [i for i in ordenadas if _DECISIONES[i].frase]
    resto = [_DECISIONES[i] for i in ordenadas if not _DECISIONES[i].frase]
    if not resto:
        return _DECISIONES[con_frase[0]]
    if len(resto) == 1:
        decision = resto[0]
    else:  # dos intenciones que se responden: el prompt general nombra las dos herramientas
        herramientas = (None if any(d.herramientas is None for d in resto)
                        else frozenset().union(*(d.herramientas for d in resto)))
        decision = Decision(herramientas=herramientas,
                            busqueda_obligada=any(d.busqueda_obligada for d in resto),
                            datos_obligados=any(d.datos_obligados for d in resto))
    nota = " ".join(_NOTAS[i] for i in con_frase)
    return replace(decision, nota=nota) if nota else decision
