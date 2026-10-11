"""Exportador propio: cada span terminado, una línea JSON en un fichero (lo lee el informe).

Escribe en modo `append`. Si falla, devuelve FAILURE y el SDK lo registra como aviso: la
respuesta al usuario no se ve afectada.
"""
from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.trace import format_span_id, format_trace_id

logger = logging.getLogger("agente.observabilidad")


class ExportadorJsonl(SpanExporter):
    def __init__(self, ruta: Path):
        self._ruta = ruta
        self._cerrojo = threading.Lock()

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        lineas = [json.dumps(_a_dict(s), ensure_ascii=False, default=str) for s in spans]
        try:
            with self._cerrojo, self._ruta.open("a", encoding="utf-8") as f:
                f.write("".join(linea + "\n" for linea in lineas))
        except OSError as exc:
            logger.warning("No se pudieron escribir %d spans en %s: %s", len(spans), self._ruta, exc)
            return SpanExportResult.FAILURE
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        pass


def _a_dict(span: ReadableSpan) -> dict:
    atributos = dict(span.attributes or {})
    return {
        "trace_id": format_trace_id(span.context.trace_id),
        "span_id": format_span_id(span.context.span_id),
        "parent_id": format_span_id(span.parent.span_id) if span.parent else None,
        "name": span.name,
        "kind": atributos.get("openinference.span.kind"),
        "start": _iso(span.start_time),
        "end": _iso(span.end_time),
        "duracion_ms": round((span.end_time - span.start_time) / 1e6, 3)
        if span.start_time and span.end_time else None,
        "status": span.status.status_code.name,
        "status_descripcion": span.status.description,
        "attributes": atributos,
        "events": [{"name": e.name, "time": _iso(e.timestamp), "attributes": dict(e.attributes or {})}
                   for e in span.events],
    }


def _iso(nanos: int | None) -> str | None:
    if nanos is None:
        return None
    return datetime.fromtimestamp(nanos / 1e9, tz=timezone.utc).isoformat()
