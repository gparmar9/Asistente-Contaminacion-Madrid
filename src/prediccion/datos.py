"""Única puerta del servicio a PostgreSQL: lectura de la vista `mediciones_bloques`.

La misma vista de solo lectura que consulta el agente (`deploy/sql/vista_mediciones_bloques.sql`).
Devuelve los bloques recientes con las columnas que espera la inferencia (MAYÚSCULAS y
MAGNITUD numérica); la vista publica el contaminante por nombre, se mapea con el
diccionario del ETL.
"""
from __future__ import annotations

import logging
import os

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from prediccion.errores import DatosNoDisponibles
from limpiar_datos import MAGNITUDES_OBJETIVO  # src/etl, en sys.path vía prediccion/__init__

logger = logging.getLogger("prediccion.datos")

MAGNITUD_POR_CONTAMINANTE = {nombre: codigo for codigo, nombre in MAGNITUDES_OBJETIVO.items()}

_SQL_RECIENTES = text("""
    SELECT fecha, estacion, nombre_estacion, contaminante, bloque, media, cobertura
    FROM mediciones_bloques
    WHERE fecha > (SELECT MAX(fecha) FROM mediciones_bloques) - make_interval(days => :dias)
""")

_ENGINE = None


def _engine():
    global _ENGINE
    if _ENGINE is None:
        url = os.getenv("DATABASE_URL")
        if not url:
            raise DatosNoDisponibles("DATABASE_URL no está configurada")
        _ENGINE = create_engine(url, pool_pre_ping=True, connect_args={"connect_timeout": 5})
    return _ENGINE


def leer_bloques_recientes(dias: int) -> pd.DataFrame:
    """Los últimos `dias` días de la vista, listos para la inferencia. Lanza
    `DatosNoDisponibles` si la base no responde."""
    try:
        df = pd.read_sql(_SQL_RECIENTES, _engine(), params={"dias": int(dias)})
    except SQLAlchemyError as exc:
        logger.warning("mediciones_bloques no disponible: %s", exc)
        raise DatosNoDisponibles(f"La vista mediciones_bloques no responde: {exc}") from exc
    df.columns = df.columns.str.upper()
    df["FECHA"] = pd.to_datetime(df["FECHA"])
    df["MAGNITUD"] = df["CONTAMINANTE"].map(MAGNITUD_POR_CONTAMINANTE)
    df = df[df["MAGNITUD"].notna()].copy()
    df["MAGNITUD"] = df["MAGNITUD"].astype(int)
    return df
