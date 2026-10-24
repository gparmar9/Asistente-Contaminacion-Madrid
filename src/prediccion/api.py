"""Servicio HTTP de la predicción: la herramienta `PREDICCION` que consultará el agente.

Arranque:
    python -m prediccion.api                 # uvicorn en 0.0.0.0:${PREDICCION_API_PUERTO:-8011}
    uvicorn prediccion.api:app --reload      # desarrollo

Endpoints (espejo de rag.api):
    GET  /salud                    bundles cargados, último día completo y rango predecible
    GET  /prediccion/herramienta   definición de la function tool (formato OpenAI)
    POST /prediccion               {fecha_objetivo, estacion?, contaminante?, bloque?} →
                                   filas por (estación, contaminante, bloque) con la media
                                   estimada en µg/m³, o un estado (ya_medido, fuera_de_alcance,
                                   sin_datos)

La fecha objetivo la calcula el CÓDIGO del cliente (el agente) a partir de la pregunta del
usuario, nunca el LLM — el mismo convenio que los periodos de `consultar_datos`. El horizonte
(h1/h2) es interno: se deduce de `fecha_objetivo − último día completo` y elige el bundle.

Códigos HTTP: 422 entrada inválida (pydantic); 503 base de datos o bundles no disponibles.
"""
from __future__ import annotations

import datetime as dt
import os
from typing import Literal

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from prediccion import datos, motor
from prediccion.errores import DatosNoDisponibles, ModelosNoDisponibles

VERSION = "0.1.0"
PUERTO_POR_DEFECTO = 8011

CONTAMINANTES = ("NO", "NO2", "NOx", "O3", "PM10", "PM2.5")
BLOQUES = ("madrugada", "manana", "tarde", "noche")

HERRAMIENTA: dict = {
    "type": "function",
    "function": {
        "name": "predecir_calidad_aire",
        "description": (
            "Predice la calidad del aire de Madrid a corto plazo: la media estimada (µg/m³) por "
            "estación, contaminante y bloque horario (madrugada, mañana, tarde, noche) para una "
            "fecha próxima (normalmente hoy o mañana). No devuelve mediciones pasadas (para eso "
            "está consultar_datos) ni predicciones a más de dos días del último dato."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "fecha_objetivo": {
                    "type": "string",
                    "description": "Fecha a predecir, AAAA-MM-DD. La calcula el sistema a partir "
                                   "de la pregunta del usuario.",
                },
                "estacion": {
                    "type": "string",
                    "description": "Estación: nombre oficial o código corto. Sin ella, todas.",
                },
                "contaminante": {"type": "string", "enum": list(CONTAMINANTES)},
                "bloque": {"type": "string", "enum": list(BLOQUES)},
            },
            "required": ["fecha_objetivo"],
        },
    },
}

app = FastAPI(
    title="Predicción · calidad del aire de Madrid",
    version=VERSION,
    description="Modelo de predicción a corto plazo (t+1 y t+2 por bloques) para un LLM externo.",
)


class PeticionPrediccion(BaseModel):
    fecha_objetivo: dt.date = Field(..., examples=["2026-05-01"])
    estacion: int | str | None = Field(None, examples=["Plaza de España"])
    contaminante: Literal["NO", "NO2", "NOx", "O3", "PM10", "PM2.5"] | None = None
    bloque: Literal["madrugada", "manana", "tarde", "noche"] | None = None


def _handler(status: int):
    async def handler(_request: Request, exc: Exception):
        return JSONResponse(status_code=status,
                            content={"error": type(exc).__name__, "detalle": str(exc)})
    return handler


for _exc, _status in ((DatosNoDisponibles, 503), (ModelosNoDisponibles, 503)):
    app.add_exception_handler(_exc, _handler(_status))


# El motor se construye en el primer uso (los bundles tardan en cargar y los tests lo sustituyen).
MOTOR: motor.Motor | None = None


def _obtener_motor() -> motor.Motor:
    global MOTOR
    if MOTOR is None:
        MOTOR = motor.Motor(motor.cargar_bundles(), datos.leer_bloques_recientes)
    return MOTOR


@app.get("/salud")
def salud() -> dict:
    return {"estado": "ok", "version": VERSION, **_obtener_motor().salud()}


@app.get("/prediccion/herramienta")
def herramienta() -> dict:
    return {"herramienta": HERRAMIENTA}


@app.post("/prediccion")
def prediccion(peticion: PeticionPrediccion) -> dict:
    return _obtener_motor().predecir(peticion.fecha_objetivo, peticion.estacion,
                                     peticion.contaminante, peticion.bloque)


def main() -> None:
    import uvicorn

    puerto = int(os.getenv("PREDICCION_API_PUERTO") or PUERTO_POR_DEFECTO)
    uvicorn.run("prediccion.api:app", host="0.0.0.0", port=puerto)


if __name__ == "__main__":
    main()
