"""`buscar_evidencias`: cliente HTTP del servicio RAG (`rag.api`).

- La definición de la herramienta y el esquema de salida de la ruta documental los
  publica el propio RAG (`GET /rag/herramienta`), única fuente de verdad. Se piden una
  vez y se cachean por proceso; si falla, la herramienta no se ofrece en esa vuelta y
  se reintenta en la siguiente.
- `POST /rag/evidencias` devuelve las evidencias D1..Dn. Al modelo se le entrega
  `para_el_modelo` (solo lo que debe citar); el estado y los `chunk_id` van en
  `internos`, que usa el bucle para renumerar y validar.
- `POST /rag/validar` comprueba la salida JSON del modelo y renderiza el texto final
  con citas, aviso y bibliografía. Lo llama el bucle, no el modelo.
"""
from __future__ import annotations

import logging
from typing import Any

import httpx

from agente.entities.chat import Fuente
from agente.tools.base import Herramienta, ResultadoHerramienta

logger = logging.getLogger("agente.tools.rag")

NOMBRE = "buscar_evidencias"
ERROR_NO_DISPONIBLE = "El servicio documental no está disponible en este momento"


class HerramientaRag(Herramienta):
    nombre = NOMBRE

    def __init__(self, url_base: str, timeout_s: float = 20.0,
                 transport: httpx.AsyncBaseTransport | None = None):
        self._url_base = url_base.rstrip("/")
        self._timeout_s = timeout_s
        self._transport = transport  # para los tests (httpx.MockTransport)
        self._definicion: dict | None = None
        self._esquema_salida: dict | None = None

    def _cliente(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=self._url_base, timeout=self._timeout_s, transport=self._transport
        )

    @property
    def esquema_salida(self) -> dict | None:
        """JSON Schema de la salida documental; disponible tras una `definicion()` correcta."""
        return self._esquema_salida

    async def definicion(self) -> dict | None:
        if self._definicion is not None:
            return self._definicion
        try:
            async with self._cliente() as cliente:
                r = await cliente.get("/rag/herramienta")
            r.raise_for_status()
            cuerpo = r.json()
            definicion = cuerpo["herramienta"]
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
            logger.warning("No se pudo obtener la definición de %s del RAG: %s", NOMBRE, exc)
            return None
        if definicion.get("function", {}).get("name") != NOMBRE:
            logger.warning("El RAG publica una herramienta con otro nombre: %r", definicion)
            return None
        self._definicion = definicion
        self._esquema_salida = cuerpo.get("esquema_salida") or None
        return definicion

    async def ejecutar(self, argumentos: dict[str, Any]) -> ResultadoHerramienta:
        pregunta = argumentos.get("pregunta")
        if not isinstance(pregunta, str) or not pregunta.strip():
            return ResultadoHerramienta.fallo("Falta el parámetro obligatorio 'pregunta'")
        cuerpo: dict[str, Any] = {"pregunta": pregunta}
        if argumentos.get("tema"):
            cuerpo["tema"] = argumentos["tema"]

        try:
            async with self._cliente() as cliente:
                r = await cliente.post("/rag/evidencias", json=cuerpo)
        except httpx.HTTPError as exc:
            logger.warning("Fallo al llamar a %s: %s", NOMBRE, exc)
            return ResultadoHerramienta.fallo(ERROR_NO_DISPONIBLE)

        if r.status_code >= 400:
            return ResultadoHerramienta.fallo(_detalle_error(r))
        try:
            datos = r.json()
            para_el_modelo = datos["para_el_modelo"]
            internos = {
                "estado": datos["estado"],
                "evidencias": [{"id": e["id"], "chunk_id": e["chunk_id"]} for e in datos["evidencias"]],
            }
        except (ValueError, KeyError, TypeError):
            logger.warning("Respuesta del RAG sin el formato esperado")
            return ResultadoHerramienta.fallo(ERROR_NO_DISPONIBLE)

        return ResultadoHerramienta.exito(para_el_modelo, internos=internos)

    async def validar(self, pregunta: str, evidencias: list[dict], salida: Any) -> dict | None:
        """POST /rag/validar. Devuelve {valida, mensaje_reparacion, texto, aviso_sanitario, fuentes}
        o None si el RAG no responde o rechaza la petición."""
        try:
            async with self._cliente() as cliente:
                r = await cliente.post("/rag/validar", json={
                    "pregunta": pregunta, "evidencias": evidencias, "salida": salida})
            r.raise_for_status()
            datos = r.json()
            return {
                "valida": bool(datos["valida"]),
                "mensaje_reparacion": datos.get("mensaje_reparacion") or "",
                "texto": datos.get("texto") or "",
                "aviso_sanitario": bool(datos.get("aviso_sanitario")),
                "fuentes": _fuentes(datos),
            }
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
            logger.warning("No se pudo validar la salida documental: %s", exc)
            return None


def _detalle_error(r: httpx.Response) -> str:
    try:
        detalle = r.json().get("detalle")
    except ValueError:
        detalle = None
    return detalle or f"{ERROR_NO_DISPONIBLE} (HTTP {r.status_code})"


def _fuentes(datos: dict) -> list[Fuente]:
    """Un `Fuente` por documento de la bibliografía, en orden y sin repetir."""
    vistos: set[str] = set()
    fuentes: list[Fuente] = []
    for ref in datos.get("bibliografia") or []:
        titulo = ref.get("titulo") or ref.get("archivo")
        if titulo and titulo not in vistos:
            vistos.add(titulo)
            fuentes.append(Fuente(tipo="documento", referencia=titulo))
    return fuentes
