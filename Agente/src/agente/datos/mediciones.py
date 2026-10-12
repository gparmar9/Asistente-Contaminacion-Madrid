"""Única puerta del agente a PostgreSQL: consultas de solo lectura sobre `mediciones_bloques`.

Sin reglas de negocio: ejecuta el SQL que le dan (la herramienta lo valida antes) con sus propios
límites y devuelve filas con valores serializables en JSON. `tools/` y `business/` no abren
conexiones.

Capas del lado de la conexión, en cada sesión de PostgreSQL:
- `default_transaction_read_only=on` y `statement_timeout` (`DB_TIMEOUT_S`). El cliente podría
  cambiarlos con SET: son una capa más.
- La barrera real es el rol `agente_lectura` de `DATABASE_URL`
  (`deploy/sql/rol_agente_lectura.sql`), que solo puede leer la vista.
- Tope de filas (`DB_MAX_FILAS`): se lee una fila de más para saber si hay truncado.

Un fallo al abrir la conexión (`ErrorBaseDatos.conexion`) es la base caída, no un error del SQL:
la herramienta no reintenta y el turno da la frase de datos no disponibles.

Síncrono (SQLAlchemy): desde código async, llamarlo con `asyncio.to_thread`.
"""
from __future__ import annotations

import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Mapping

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from agente.entities.datos import ResultadoConsulta

VISTA = "mediciones_bloques"
# `ultima_fecha()` y `estaciones()` cambian como mucho una vez al día (el pipeline cierra el día
# a las 23:45)
CACHE_ULTIMA_FECHA_S = 3600


class ErrorBaseDatos(Exception):
    """La consulta no se pudo ejecutar: conexión, sintaxis, permisos o tiempo agotado.

    El mensaje es el de PostgreSQL, sin la traza de SQLAlchemy: sirve para que el redactor
    corrija su SQL en el reintento. `conexion`: no se pudo abrir la conexión (base caída o
    inaccesible), así que repetir la consulta no sirve.
    """

    def __init__(self, mensaje: str, conexion: bool = False) -> None:
        super().__init__(mensaje)
        self.conexion = conexion


def crear_motor(database_url: str, timeout_s: float = 5) -> Engine:
    """Motor de SQLAlchemy con la sesión de solo lectura y el tiempo límite por sentencia.

    No conecta todavía: la primera conexión se abre con la primera consulta.
    """
    connect_args: dict[str, Any] = {}
    if database_url.startswith("postgresql"):
        ms = int(timeout_s * 1000)
        connect_args = {
            "connect_timeout": max(1, round(timeout_s)),
            "options": f"-c default_transaction_read_only=on -c statement_timeout={ms}",
        }
    return create_engine(database_url, connect_args=connect_args, pool_pre_ping=True)


class Mediciones:
    """`reloj`: segundos monótonos; los tests inyectan uno propio para la caché."""

    def __init__(self, engine: Engine, max_filas: int = 60,
                 reloj: Callable[[], float] = time.monotonic) -> None:
        self._engine = engine
        self._max_filas = max(1, max_filas)
        self._reloj = reloj
        self._cache: dict[str, tuple[float, Any]] = {}  # clave -> (momento, valor)

    def ejecutar(self, sql: str, params: Mapping[str, Any] | None = None,
                 max_filas: int | None = None) -> ResultadoConsulta:
        """Ejecuta una sentencia y devuelve como mucho `max_filas` (por defecto, `DB_MAX_FILAS`)."""
        tope = self._max_filas if max_filas is None else max(1, max_filas)
        try:
            conn = self._engine.connect()
        except SQLAlchemyError as exc:
            raise ErrorBaseDatos(_mensaje(exc), conexion=True) from exc
        try:
            with conn:
                resultado = conn.execute(text(sql), dict(params or {}))
                columnas = tuple(resultado.keys())
                filas = resultado.fetchmany(tope + 1)
        except SQLAlchemyError as exc:
            raise ErrorBaseDatos(_mensaje(exc)) from exc
        return ResultadoConsulta(
            columnas=columnas,
            filas=tuple(tuple(_serializable(v) for v in fila) for fila in filas[:tope]),
            truncado=len(filas) > tope,
        )

    def ultima_fecha(self) -> date | None:
        """Último día con datos en la vista (None si está vacía). Se cachea una hora."""
        def consultar() -> date | None:
            resultado = self.ejecutar(f"SELECT MAX(fecha) AS fecha FROM {VISTA}")
            valor = resultado.filas[0][0] if resultado.filas else None
            return None if valor is None else date.fromisoformat(str(valor)[:10])
        return self._cacheado("ultima_fecha", consultar)

    def estaciones(self) -> tuple[tuple[Any, ...], ...]:
        """(código, nombre, distrito) de cada estación con datos en la vista, por distrito y
        nombre. Se cachea una hora."""
        def consultar() -> tuple[tuple[Any, ...], ...]:
            return self.ejecutar(
                f"SELECT DISTINCT estacion, nombre_estacion, distrito FROM {VISTA} "
                "ORDER BY distrito, nombre_estacion", max_filas=500).filas
        return self._cacheado("estaciones", consultar)

    def _cacheado(self, clave: str, consultar: Callable[[], Any]) -> Any:
        """Valor guardado si tiene menos de una hora. Un valor vacío no se guarda: se repite la consulta."""
        ahora = self._reloj()
        guardado = self._cache.get(clave)
        if guardado is not None and ahora - guardado[0] < CACHE_ULTIMA_FECHA_S:
            return guardado[1]
        valor = consultar()
        if valor:
            self._cache[clave] = (ahora, valor)
        return valor


def _serializable(valor: Any) -> Any:
    if isinstance(valor, Decimal):
        return float(valor)
    if isinstance(valor, (date, datetime)):
        return valor.isoformat()
    return valor


def _mensaje(exc: SQLAlchemyError) -> str:
    """Error del driver (el de PostgreSQL) si lo hay; recortado para el prompt y la traza."""
    original = getattr(exc, "orig", None)
    return str(original if original is not None else exc).strip()[:500]
