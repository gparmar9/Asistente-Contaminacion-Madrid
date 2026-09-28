"""Modelos de respuesta de las series por bloques (tabla `resumen_datos_ml`)."""
from datetime import date

from pydantic import BaseModel


class PuntoSerie(BaseModel):
    """Un bloque horario de un día: estadísticos + salida del detector de anomalías."""

    fecha: date
    bloque: str
    media: float | None = None
    maximo: float | None = None
    minimo: float | None = None
    n_horas: int | None = None
    # Horas válidas / horas del bloque. Interpretar anomalías solo con cobertura >= 0,7.
    cobertura: float | None = None
    z_score: float | None = None
    # Score negado del Isolation Forest: alto = más anómalo. No es una probabilidad.
    anomaly_score: float | None = None
    is_anomaly: bool | None = None


class SerieBloques(BaseModel):
    estacion: int
    contaminante: str
    desde: date
    hasta: date
    puntos: list[PuntoSerie]
