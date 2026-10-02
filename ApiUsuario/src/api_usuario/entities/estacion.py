"""Modelo de respuesta de una estación de control (tabla `estaciones`)."""
from pydantic import BaseModel


class Estacion(BaseModel):
    codigo_corto: int
    nombre: str
    direccion: str
    tipo: str
    distrito: str | None = None
    latitud: float | None = None
    longitud: float | None = None
    altitud: int | None = None
    # Contaminantes objetivo consultables en /series para esta estación,
    # derivados de las columnas `mide_*` del catálogo (p. ej. ["NO", "NO2", "NOx", "O3"])
    contaminantes_medidos: list[str] = []
