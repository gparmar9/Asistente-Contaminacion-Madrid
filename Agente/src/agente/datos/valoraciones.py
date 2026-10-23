"""Guarda la valoración en el span del turno en Phoenix.

Phoenix encola el voto por si la traza aún no ha llegado. El cliente no reintenta.
La misma sesión puede sustituir su voto sobre un turno.
"""
from __future__ import annotations

import logging

import httpx

from agente.datos.phoenix import VALORACION, PhoenixNoDisponible
from agente.entities.chat import Valoracion

logger = logging.getLogger("agente.valoraciones")

PUNTUACION = {"positiva": 1, "negativa": 0}


class AnotadorPhoenix:
    def __init__(self, url_base: str, timeout_s: float = 5.0,
                 transport: httpx.AsyncBaseTransport | None = None):
        self._url_base = url_base.rstrip("/")
        self._timeout_s = timeout_s
        self._transport = transport

    async def anotar(self, valoracion: Valoracion) -> None:
        anotacion = {
            "span_id": valoracion.span_id,
            "name": VALORACION,
            "annotator_kind": "HUMAN",
            "result": {"label": valoracion.valor, "score": PUNTUACION[valoracion.valor]},
            "identifier": valoracion.session_id,
            "metadata": {"session_id": valoracion.session_id, "traza_id": valoracion.traza_id},
        }
        try:
            async with httpx.AsyncClient(base_url=self._url_base, timeout=self._timeout_s,
                                         transport=self._transport) as cliente:
                r = await cliente.post("/v1/span_annotations", json={"data": [anotacion]})
            r.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("Phoenix no guardó la valoración del span %s: %s", valoracion.span_id, exc)
            raise PhoenixNoDisponible(str(exc)) from exc
