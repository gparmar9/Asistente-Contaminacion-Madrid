"""`predecir_calidad_aire`: cliente HTTP del servicio de predicción (`prediccion.api`, ml.api).

- La definición de la herramienta la publica el propio servicio (`GET /prediccion/herramienta`),
  única fuente de verdad; se pide una vez y se cachea por proceso. En cada turno se consulta además
  `GET /salud` (ancla y rango predecible, para `contexto_prompt`); si cualquiera falla, la
  herramienta no se ofrece y se reintenta en la siguiente vuelta.
- `POST /prediccion {fecha_objetivo, estacion?, contaminante?, bloque?}` devuelve las filas estimadas
  o un estado (`ya_medido`, `fuera_de_alcance`, `sin_datos`).
- **La fecha la calcula el código, no el modelo** (mismo convenio que `consultar_datos`): el bucle
  inyecta la pregunta del usuario y aquí se resuelve con `periodos.resolver_futuro`. El
  `fecha_objetivo` que escriba el LLM se ignora. Estación y contaminante sí los pone el modelo.

Estados en `internos` (el bucle actúa, nunca excepción): `no_disponible` (servicio caído: no se
repite), `ok`, y los terminales con `internos["frase"]` que responde el bucle.
"""
from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any, Callable

import httpx

from agente.business import frases
from agente.business.periodos import resolver_futuro
from agente.entities.chat import Fuente
from agente.tools.base import Herramienta, ResultadoHerramienta
from agente.tools.sql_libre import ARG_PREGUNTA_USUARIO, ARG_PREGUNTAS_PREVIAS

logger = logging.getLogger("agente.tools.prediccion")

NOMBRE = "predecir_calidad_aire"
CONTAMINANTES_REFERENCIA = ("NO2", "PM10")
BLOQUES = ("madrugada", "manana", "tarde", "noche")
# Tope del JSON de filas que entra en el contexto del modelo; por encima se quitan del final.
MAX_CARACTERES = 6000

ESTADO_OK = "ok"
ESTADO_NO_DISPONIBLE = "no_disponible"
ESTADO_YA_MEDIDO = "ya_medido"
ESTADO_FUERA_DE_ALCANCE = "fuera_de_alcance"
ESTADO_SIN_DATOS = "sin_datos"


class HerramientaPrediccion(Herramienta):
    """`fecha_referencia`: «hoy» fijo (evaluación); None = `hoy()`, la fecha del sistema."""

    nombre = NOMBRE

    def __init__(self, url_base: str, timeout_s: float = 30.0,
                 transport: httpx.AsyncBaseTransport | None = None,
                 fecha_referencia: date | None = None, hoy: Callable[[], date] = date.today):
        self._url_base = url_base.rstrip("/")
        self._timeout_s = timeout_s
        self._transport = transport  # para los tests (httpx.MockTransport)
        self._fecha_referencia = fecha_referencia
        self._hoy = hoy
        self._definicion: dict | None = None
        self._salud: dict | None = None

    def _cliente(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(base_url=self._url_base, timeout=self._timeout_s,
                                 transport=self._transport)

    async def definicion(self) -> dict | None:
        if self._definicion is None:
            try:
                async with self._cliente() as cliente:
                    r = await cliente.get("/prediccion/herramienta")
                r.raise_for_status()
                definicion = r.json()["herramienta"]
            except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
                logger.warning("No se pudo obtener la definición de %s: %s", NOMBRE, exc)
                return None
            if definicion.get("function", {}).get("name") != NOMBRE:
                logger.warning("El servicio publica una herramienta con otro nombre: %r", definicion)
                return None
            self._definicion = definicion
        return self._definicion if await self._cargar_salud() else None

    async def _cargar_salud(self) -> bool:
        """Ancla y rango predecible para el contexto del prompt. False si el servicio no responde."""
        try:
            async with self._cliente() as cliente:
                r = await cliente.get("/salud")
            r.raise_for_status()
            datos = r.json()
            self._salud = {"ultima": datos["ultima_fecha_completa"], "hasta": datos["predecible_hasta"]}
            return True
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
            logger.warning("El servicio de predicción no está disponible: %s", exc)
            return False

    def contexto_prompt(self) -> str:
        if self._salud is None:
            return ""
        hoy = (self._fecha_referencia or self._hoy()).isoformat()
        return frases.CONTEXTO_PREDICCION.format(hoy=hoy, ultima=self._salud["ultima"],
                                                 hasta=self._salud["hasta"])

    async def ejecutar(self, argumentos: dict[str, Any]) -> ResultadoHerramienta:
        fecha, bloque = self._fecha_y_bloque(argumentos)
        cuerpo: dict[str, Any] = {"fecha_objetivo": fecha.isoformat()}
        estacion = argumentos.get("estacion")
        if estacion is not None and str(estacion).strip():
            cuerpo["estacion"] = estacion
        contaminante = argumentos.get("contaminante")
        nombro_contaminante = isinstance(contaminante, str) and contaminante.strip()
        if nombro_contaminante:
            cuerpo["contaminante"] = contaminante
        bloque = bloque or (argumentos.get("bloque") if argumentos.get("bloque") in BLOQUES else None)
        if bloque:
            cuerpo["bloque"] = bloque

        try:
            async with self._cliente() as cliente:
                r = await cliente.post("/prediccion", json=cuerpo)
        except httpx.HTTPError as exc:
            logger.warning("Fallo al llamar a %s: %s", NOMBRE, exc)
            return _no_disponible()
        if r.status_code >= 400:  # 5xx (base/bundles) o 422 (no debería, el cuerpo lo arma el código)
            return _no_disponible()
        try:
            datos = r.json()
            estado = datos["estado"]
        except (ValueError, KeyError, TypeError):
            logger.warning("Respuesta del servicio de predicción sin el formato esperado")
            return _no_disponible()

        if estado == ESTADO_OK:
            return _exito(datos, bool(nombro_contaminante))
        frase = {
            ESTADO_FUERA_DE_ALCANCE: frases.FRASE_PREDICCION_FUERA_DE_ALCANCE.format(
                hasta=datos.get("predecible_hasta", "")),
            ESTADO_YA_MEDIDO: frases.FRASE_PREDICCION_YA_MEDIDA,
            ESTADO_SIN_DATOS: frases.FRASE_PREDICCION_SIN_DATOS,
        }.get(estado, frases.FRASE_PREDICCION_NO_DISPONIBLE)
        return ResultadoHerramienta.fallo(frase, internos={"estado": estado, "frase": frase})

    def _fecha_y_bloque(self, argumentos: dict[str, Any]) -> tuple[date, str | None]:
        """La fecha objetivo (y el bloque, si la expresión lo trae) de la pregunta del usuario; sus
        preguntas previas como respaldo (la más reciente primero); por defecto, hoy."""
        hoy = self._fecha_referencia or self._hoy()
        previas = argumentos.get(ARG_PREGUNTAS_PREVIAS)
        textos = [argumentos.get(ARG_PREGUNTA_USUARIO),
                  *(previas[::-1] if isinstance(previas, list) else [])]
        for texto in textos:
            if isinstance(texto, str) and (periodos := resolver_futuro(texto, hoy)):
                elegido = max(periodos, key=lambda p: p.rangos[0][0])  # varias → la fecha máxima
                return elegido.rangos[0][0], elegido.bloque
        return hoy, None  # defecto: hoy


def _no_disponible() -> ResultadoHerramienta:
    return ResultadoHerramienta.fallo(
        frases.FRASE_PREDICCION_NO_DISPONIBLE,
        internos={"estado": ESTADO_NO_DISPONIBLE, "frase": frases.FRASE_PREDICCION_NO_DISPONIBLE})


def _exito(datos: dict, nombro_contaminante: bool) -> ResultadoHerramienta:
    filas = datos.get("filas") or []
    if not nombro_contaminante:  # ciudad entera sin contaminante → los dos de referencia
        filas = [f for f in filas if f.get("contaminante") in CONTAMINANTES_REFERENCIA]
    filas, recortado = _recortar(filas)
    descripcion = f"Predicción de calidad del aire para el {datos.get('fecha_objetivo')}"
    cuerpo = {"descripcion": descripcion, "fecha_objetivo": datos.get("fecha_objetivo"),
              "unidades": datos.get("unidades", "µg/m³"), "filas": filas, "truncado": recortado}
    internos = {"estado": ESTADO_OK, "fecha_objetivo": datos.get("fecha_objetivo"),
                "horizonte": datos.get("horizonte"), "filas": len(filas)}
    return ResultadoHerramienta.exito(cuerpo, fuentes=(Fuente(tipo="prediccion", referencia=descripcion),),
                                      internos=internos)


def _recortar(filas: list[dict]) -> tuple[list[dict], bool]:
    """Filas que caben en MAX_CARACTERES de JSON, desde la primera; y si se quitó alguna."""
    total = 0
    for i, fila in enumerate(filas):
        total += len(json.dumps(fila, ensure_ascii=False)) + 1
        if total > MAX_CARACTERES:
            return filas[:i], True
    return filas, False
