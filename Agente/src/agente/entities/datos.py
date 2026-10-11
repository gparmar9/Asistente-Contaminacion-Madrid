"""Datos de la herramienta SQL: resultado de una consulta, contexto del redactor y periodos. Solo datos.

Los valores de `ResultadoConsulta` ya son serializables en JSON (None, bool, int, float, str): las
fechas van como texto ISO y los `numeric` de PostgreSQL como float.
"""
from dataclasses import dataclass
from datetime import date
from typing import Any


@dataclass(frozen=True)
class ResultadoConsulta:
    columnas: tuple[str, ...]
    filas: tuple[tuple[Any, ...], ...]
    # Había más filas que el tope: `filas` lleva solo las primeras.
    truncado: bool = False


@dataclass(frozen=True)
class ContextoDatos:
    """Lo que necesitan el redactor y los prompts para resolver fechas y lugares.
    `hoy`: la fecha de referencia (`FECHA_REFERENCIA` o la del sistema)."""
    hoy: date
    primera_fecha: date                       # la vista cubre 2 años hasta `ultima_fecha`
    ultima_fecha: date
    estaciones: tuple[tuple[Any, ...], ...]   # (código, nombre, distrito)


@dataclass(frozen=True)
class Periodo:
    """Periodo calculado en Python a partir de una expresión de la pregunta (`business/periodos.py`).
    `rangos`: días (inicio, fin) incluidos; varios en «en julio» o «los últimos dos veranos»."""
    expresion: str                            # tal como aparece en la pregunta, en minúsculas y sin tildes
    rangos: tuple[tuple[date, date], ...]
    bloque: str | None = None                 # «esta tarde» → 'tarde'
