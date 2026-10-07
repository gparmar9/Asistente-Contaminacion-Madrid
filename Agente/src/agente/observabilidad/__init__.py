"""Observabilidad del turno: spans OpenTelemetry con atributos OpenInference.

Una sola instrumentación y dos destinos:
- Phoenix (cascada interactiva) por OTLP/HTTP, si hay `PHOENIX_ENDPOINT`.
- Un fichero JSONL propio, un span por línea, para el informe agregado, si hay `TRAZAS_RUTA`.

El SDK propaga el contexto por `contextvars` (los turnos concurrentes no se mezclan), cierra
cada span al salir de su `with`, registra las excepciones y aísla los fallos de los exportadores.
Sin `configurar()` (tests) los spans se crean pero no se exportan.

    turno (agent)
      clasificar | bucle | busqueda_forzada | sintesis_documental | sintesis_forzada (chain)
        llm (llm) y herramientas (tool)

Las decisiones del código (frase fija, sin evidencia, límite de vueltas...) son eventos
`decision` en el span en curso, y cada hallazgo de las comprobaciones posteriores, un evento
`comprobacion` en el span `turno`.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator, Sequence

from llama_index.core.base.llms.types import ChatMessage, ChatResponse, ThinkingBlock, ToolCallBlock
from openinference.instrumentation import OITracer, TraceConfig, TracerProvider, get_llm_attributes
from openinference.semconv.resource import ResourceAttributes
from openinference.semconv.trace import SpanAttributes
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Span, Status, StatusCode, format_trace_id

from agente.config.settings import Settings
from agente.observabilidad.jsonl import ExportadorJsonl

logger = logging.getLogger("agente.observabilidad")

# Estado del proceso. Por defecto, un proveedor sin procesadores: se crean spans y no se exportan.
_proveedor = TracerProvider()
_tracer: OITracer = _proveedor.get_tracer("agente")
_guardar_texto = True
_razonamiento = False
_etiqueta = ""


def configurar(settings: Settings) -> Path | None:
    """Una vez por proceso, en el arranque. Los destinos vacíos no se configuran.
    Devuelve el fichero JSONL de este proceso (None sin `TRAZAS_RUTA`)."""
    guardar = settings.traza_guardar_texto
    proveedor = TracerProvider(
        resource=Resource.create({ResourceAttributes.PROJECT_NAME: settings.phoenix_proyecto}),
        # Sin texto: prompts, preguntas, argumentos y respuestas quedan como __REDACTED__.
        config=TraceConfig(hide_inputs=not guardar, hide_outputs=not guardar),
    )
    if settings.phoenix_endpoint:
        from phoenix.otel import BatchSpanProcessor as ProcesadorPhoenix
        proveedor.add_span_processor(ProcesadorPhoenix(endpoint=settings.phoenix_endpoint))
    fichero = None
    if settings.trazas_ruta:
        fichero = _fichero_trazas(Path(settings.trazas_ruta), settings.traza_etiqueta)
        if fichero is not None:
            proveedor.add_span_processor(BatchSpanProcessor(ExportadorJsonl(fichero)))
    usar_proveedor(proveedor, guardar_texto=guardar, razonamiento=settings.traza_razonamiento,
                   etiqueta=settings.traza_etiqueta)
    logger.info("Trazas: Phoenix=%s, JSONL=%s, texto=%s", settings.phoenix_endpoint or "no",
                settings.trazas_ruta or "no", guardar)
    return fichero


def usar_proveedor(proveedor: TracerProvider, *, guardar_texto: bool = True,
                   razonamiento: bool = False, etiqueta: str = "") -> None:
    """Sustituye el proveedor del proceso (lo usan `configurar` y los tests)."""
    global _proveedor, _tracer, _guardar_texto, _razonamiento, _etiqueta
    _proveedor, _tracer = proveedor, proveedor.get_tracer("agente")
    _guardar_texto, _razonamiento, _etiqueta = guardar_texto, razonamiento, etiqueta


def cerrar() -> None:
    """Vacía los lotes pendientes. En el cierre del proceso."""
    _proveedor.shutdown()


# ---------------------------------------------------------------------- spans

@contextmanager
def span(nombre: str, tipo: str, entrada: Any = None) -> Iterator[Span]:
    """Span hijo del span en curso. `tipo`: agent | chain | llm | tool."""
    with _tracer.start_as_current_span(nombre, openinference_span_kind=tipo) as s:
        if entrada is not None:
            s.set_input(entrada)
        yield s
        if getattr(s, "status", None) is not None and s.status.status_code is StatusCode.UNSET:
            s.set_status(Status(StatusCode.OK))


@contextmanager
def turno(pregunta: str) -> Iterator[Span]:
    with span("turno", "agent", entrada=pregunta) as s:
        if _etiqueta:
            s.set_attribute("agente.etiqueta", _etiqueta)
        yield s


@contextmanager
def herramienta(nombre: str, argumentos: dict) -> Iterator[Span]:
    with span(nombre, "tool", entrada=argumentos) as s:
        s.set_attribute(SpanAttributes.TOOL_NAME, nombre)
        yield s


@contextmanager
def llamada_llm(llm: Any, mensajes: Sequence[ChatMessage], herramientas: Sequence[str]) -> Iterator[Span]:
    """Span `llm` con modelo, mensajes de entrada y nombres de las herramientas ofrecidas.
    La salida la anota `anotar_respuesta` cuando llega."""
    with span("llm", "llm") as s:
        s.set_attributes(get_llm_attributes(
            model_name=_modelo(llm),
            input_messages=[_mensaje(m) for m in mensajes],
            tools=[{"name": n, "json_schema": {"name": n}} for n in herramientas],
        ))
        yield s


def anotar_respuesta(s: Span, respuesta: ChatResponse) -> None:
    """Mensaje de salida y tokens. Tokens ausentes quedan ausentes, nunca a cero."""
    extra = respuesta.additional_kwargs or {}
    tokens = {clave: extra[origen] for clave, origen in (("prompt", "prompt_tokens"),
                                                        ("completion", "completion_tokens"))
              if isinstance(extra.get(origen), int)}
    s.set_attributes(get_llm_attributes(output_messages=[_mensaje(respuesta.message)],
                                        token_count=tokens or None))
    if _razonamiento and _guardar_texto:
        razonamiento = "\n".join(b.content for b in respuesta.message.blocks
                                 if isinstance(b, ThinkingBlock) and b.content)
        if razonamiento:
            s.set_attribute("agente.razonamiento", razonamiento)


def decision(texto: str) -> None:
    """Decisión del código, como evento en el span en curso."""
    trace.get_current_span().add_event("decision", {"agente.decision": texto})


def comprobacion(regla: str, detalle: str, bloquea: bool) -> None:
    """Hallazgo de una comprobación posterior, como evento en el span en curso (el del turno)."""
    trace.get_current_span().add_event("comprobacion", {
        "agente.regla": regla, "agente.detalle": detalle, "agente.bloquea": bloquea})


def trace_id_actual() -> str | None:
    contexto = trace.get_current_span().get_span_context()
    return format_trace_id(contexto.trace_id) if contexto.is_valid else None


def configurar_resumen() -> logging.Logger:
    """Logger `agente.turno` a nivel info con salida propia: una línea por turno en `docker logs`."""
    log = logging.getLogger("agente.turno")
    if not log.handlers:
        manejador = logging.StreamHandler()
        manejador.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        log.addHandler(manejador)
        log.setLevel(logging.INFO)
        log.propagate = False
    return log


# ---------------------------------------------------------------------- utilidades

def _fichero_trazas(carpeta: Path, etiqueta: str) -> Path | None:
    try:
        carpeta.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        logger.warning("Sin fichero de trazas: no se pudo crear %s (%s)", carpeta, exc)
        return None
    return carpeta / f"trazas_{etiqueta or 'agente'}_{datetime.now():%Y%m%d_%H%M%S}.jsonl"


def _modelo(llm: Any) -> str | None:
    modelo = getattr(llm, "model", None)
    if isinstance(modelo, str):
        return modelo
    try:
        return llm.metadata.model_name
    except Exception:  # metadatos de proveedor: no deben romper el turno
        return None


def _mensaje(m: ChatMessage) -> dict:
    """ChatMessage de LlamaIndex -> mensaje OpenInference (rol, texto, llamadas a herramientas)."""
    mensaje: dict[str, Any] = {"role": m.role.value}
    if m.content:
        mensaje["content"] = m.content
    llamadas = [{"id": b.tool_call_id or "", "function": {"name": b.tool_name, "arguments": b.tool_kwargs}}
                for b in m.blocks if isinstance(b, ToolCallBlock)]
    if llamadas:
        mensaje["tool_calls"] = llamadas
    if isinstance(m.additional_kwargs.get("tool_call_id"), str):
        mensaje["tool_call_id"] = m.additional_kwargs["tool_call_id"]
    return mensaje
