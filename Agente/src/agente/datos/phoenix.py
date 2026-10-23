"""Lee spans y valoraciones de la API REST de Phoenix para calcular los KPIs."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Iterator

import httpx

logger = logging.getLogger("agente.phoenix")

TAM_PAGINA = 1000  # máximo que admite el endpoint de spans
SPANS_POR_PETICION = 100  # los ids van en la URL: limita su longitud
VALORACION = "valoracion_usuario"
VOTOS = ("positiva", "negativa")


class PhoenixNoDisponible(RuntimeError):
    """Phoenix no respondió o rechazó la petición."""


class LectorPhoenix:
    def __init__(self, url_base: str, timeout_s: float = 15.0,
                 transport: httpx.AsyncBaseTransport | None = None):
        self._url_base = url_base.rstrip("/")
        self._timeout_s = timeout_s
        self._transport = transport

    async def spans(self, proyecto: str, desde: datetime, hasta: datetime) -> list[dict]:
        """Spans que empiezan en [desde, hasta), en el formato del JSONL."""
        params: dict[str, Any] = {"limit": TAM_PAGINA, "start_time": desde.isoformat(),
                                  "end_time": hasta.isoformat()}
        crudos = await self._todas(f"/v1/projects/{proyecto}/spans", [params])
        try:
            return [a_formato_jsonl(s) for s in crudos]
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning("Span de Phoenix con un formato inesperado: %r", exc)
            raise PhoenixNoDisponible(f"formato inesperado: {exc!r}") from exc

    async def votos(self, proyecto: str, span_ids: list[str]) -> dict[str, str]:
        """Último voto positivo o negativo de cada span."""
        peticiones = [{"span_ids": span_ids[i:i + SPANS_POR_PETICION], "include_annotation_names": [VALORACION],
                       "limit": TAM_PAGINA} for i in range(0, len(span_ids), SPANS_POR_PETICION)]
        anotaciones = await self._todas(f"/v1/projects/{proyecto}/span_annotations", peticiones)
        votos: dict[str, str] = {}
        for a in sorted(anotaciones, key=lambda a: a.get("updated_at") or ""):
            if (a.get("result") or {}).get("label") in VOTOS:
                votos[a["span_id"]] = a["result"]["label"]
        return votos

    async def _todas(self, ruta: str, peticiones: list[dict[str, Any]]) -> list[dict]:
        """Recoge todas las páginas de cada petición."""
        datos: list[dict] = []
        try:
            async with httpx.AsyncClient(base_url=self._url_base, timeout=self._timeout_s,
                                         transport=self._transport) as cliente:
                for params in peticiones:
                    cursor = None
                    while True:
                        r = await cliente.get(ruta, params={**params, **({"cursor": cursor} if cursor else {})})
                        r.raise_for_status()
                        cuerpo = r.json()
                        datos.extend(cuerpo["data"])
                        cursor = cuerpo.get("next_cursor")
                        if not cursor:
                            break
        except (httpx.HTTPError, ValueError, KeyError) as exc:  # ValueError: cuerpo que no es JSON
            logger.warning("No se pudo leer %s de Phoenix: %s", ruta, exc)
            raise PhoenixNoDisponible(str(exc)) from exc
        return datos


def a_formato_jsonl(span: dict) -> dict:
    """Convierte un span de Phoenix al formato de observabilidad/jsonl.py."""
    inicio, fin = datetime.fromisoformat(span["start_time"]), datetime.fromisoformat(span["end_time"])
    tipo = span.get("span_kind")
    tipo = None if tipo in (None, "UNKNOWN") else tipo
    return {
        "trace_id": span["context"]["trace_id"], "span_id": span["context"]["span_id"],
        "parent_id": span.get("parent_id"), "name": span["name"], "kind": tipo,
        "start": _iso(inicio), "end": _iso(fin), "duracion_ms": round((fin - inicio).total_seconds() * 1000, 3),
        "status": span.get("status_code") or "UNSET", "status_descripcion": span.get("status_message") or None,
        "attributes": {"openinference.span.kind": tipo, **(span.get("attributes") or {})},
        "events": [{"name": e["name"], "time": _iso(datetime.fromisoformat(e["timestamp"])),
                    "attributes": dict(_aplanar(e.get("attributes") or {}))} for e in span.get("events") or []],
    }


def _aplanar(atributos: dict, prefijo: str = "") -> Iterator[tuple[str, Any]]:
    for clave, valor in atributos.items():
        if isinstance(valor, dict):
            yield from _aplanar(valor, f"{prefijo}{clave}.")
        else:
            yield f"{prefijo}{clave}", valor


def _iso(momento: datetime) -> str:
    """Siempre con microsegundos: `turnos()` ordena los spans por el texto del inicio."""
    return momento.isoformat(timespec="microseconds")
