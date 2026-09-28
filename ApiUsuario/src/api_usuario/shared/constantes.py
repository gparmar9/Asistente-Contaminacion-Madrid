"""Constantes de dominio de la API.

La lista de contaminantes está duplicada a propósito respecto a
`src/etl/limpiar_datos.py` (CONTAMINANTES_OBJETIVO): `ApiUsuario` es un paquete
autocontenido y no importa código del pipeline. Si cambia la lista canónica,
actualizar ambos sitios.
"""
from enum import Enum

# Los 6 contaminantes objetivo del proyecto, tal como figuran en la columna
# `contaminante` de `resumen_datos_ml` (siempre en µg/m³).
CONTAMINANTES = ("NO", "NO2", "NOx", "O3", "PM10", "PM2.5")

# Código de magnitud de cada contaminante (espejo de MAGNITUDES_OBJETIVO del ETL).
# Las consultas filtran por `magnitud` (entero) y no por `contaminante` (texto)
# para aprovechar el índice (estacion, magnitud, fecha) de `resumen_datos_ml`.
MAGNITUD_POR_CONTAMINANTE = {
    "NO": 7, "NO2": 8, "PM2.5": 9, "PM10": 10, "NOx": 12, "O3": 14,
}

# Los 4 bloques horarios del día, tal como figuran en la columna `bloque`.
BLOQUES = ("madrugada", "manana", "tarde", "noche")


def _como_enum(nombre: str, valores: tuple[str, ...]) -> type[Enum]:
    """Enum de strings para validar query params (FastAPI lo documenta en OpenAPI)."""
    return Enum(nombre, {v.replace(".", "_"): v for v in valores}, type=str)


Contaminante = _como_enum("Contaminante", CONTAMINANTES)
Bloque = _como_enum("Bloque", BLOQUES)
