"""`consultar_datos` con catálogo cerrado (fase 5): la alternativa al SQL libre, para compararlas.

Mismo nombre, misma definición y mismos pasos previos que `tools/sql_libre.py` (hereda de
`HerramientaDatos`): pregunta del usuario, periodos calculados en Python, periodo fuera de la vista
→ `sin_datos`. Solo cambia cómo se obtiene el SQL:

    selector (LLM_MODELO_SQL) elige consulta y parámetros en JSON      span `elegir_consulta`
      -> catálogo (`business/catalogo_sql.py`) valida y construye el SQL: el modelo no escribe SQL
      -> elección no válida: un reintento con el error; después, fallo
      -> contaminante no medido o lugar inexistente: resultado vacío con su nota, sin consultar
      -> DAL de solo lectura (statement_timeout, tope de filas)
      -> {descripcion, periodo, nota?, columnas, filas, truncado}, como el SQL libre

El bucle no distingue las dos opciones: `HERRAMIENTA_DATOS=libre|catalogo` en `main.py`. Un error de
PostgreSQL con SQL del catálogo es un fallo del código, no del modelo: no se reintenta.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import Callable

from opentelemetry.trace import Status, StatusCode

from agente import observabilidad
from agente.business import periodos
from agente.business.catalogo_sql import SeleccionNoValida, construir, interpretar
from agente.business.selector_catalogo import SelectorCatalogo
from agente.datos.mediciones import ErrorBaseDatos, Mediciones
from agente.entities.datos import ContextoDatos, Periodo, ResultadoConsulta
from agente.tools.base import ResultadoHerramienta
from agente.tools.sql_libre import (ERROR_NO_DISPONIBLE, ESTADO_NO_DISPONIBLE, ESTADO_SQL_INVALIDO, MAX_INTENTOS,
                                    HerramientaDatos, _exito, _fallo)

logger = logging.getLogger("agente.tools.catalogo")


class HerramientaCatalogo(HerramientaDatos):
    def __init__(self, mediciones: Mediciones, selector: SelectorCatalogo, fecha_referencia: date | None = None,
                 max_filas: int = 60, hoy: Callable[[], date] = date.today):
        super().__init__(mediciones, None, fecha_referencia, max_filas, hoy)
        self._selector = selector

    async def _consultar(self, pregunta: str, contexto: ContextoDatos, pregunta_usuario: str | None,
                         calculados: list[Periodo], sin_fechas: bool) -> ResultadoHerramienta:
        anterior: tuple[str, str] | None = None  # (respuesta, error) del intento que falló
        for intento in range(1, MAX_INTENTOS + 1):
            with observabilidad.span("elegir_consulta", "chain", entrada=pregunta) as span:
                span.set_attribute("agente.periodo", periodos.condicion_sql(calculados) or "sin calcular")
                try:
                    respuesta = await self._selector.elegir(pregunta, contexto, anterior, pregunta_usuario,
                                                            calculados, sin_fechas)
                except Exception as exc:  # el proveedor puede lanzar de todo
                    logger.warning("Selector del catálogo no disponible: %s", exc)
                    span.set_status(Status(StatusCode.ERROR, f"selector no disponible: {exc}"))
                    return _fallo(ESTADO_NO_DISPONIBLE, ERROR_NO_DISPONIBLE, intentos=intento)
                span.set_output(respuesta)
                try:
                    consulta = construir(interpretar(respuesta), contexto, calculados, sin_fechas, self._max_filas)
                except SeleccionNoValida as exc:
                    observabilidad.sql_invalido("catalogo", str(exc))
                    anterior = (respuesta, str(exc))
                    continue
                span.set_attribute("agente.consulta", consulta.nombre)
            if consulta.vacia:
                return _exito(consulta.descripcion, "", ResultadoConsulta((), ()), intento, consulta.periodo,
                              consulta.nota, consulta=consulta.nombre)
            try:
                filas = await asyncio.to_thread(self._mediciones.ejecutar, consulta.sql, consulta.params,
                                                self._max_filas)
            except ErrorBaseDatos as exc:
                if exc.conexion:
                    logger.warning("Base de datos no disponible: %s", exc)
                    return _fallo(ESTADO_NO_DISPONIBLE, ERROR_NO_DISPONIBLE, sql=consulta.sql, intentos=intento)
                logger.error("Consulta del catálogo %s rechazada por PostgreSQL: %s", consulta.nombre, exc)
                observabilidad.sql_invalido("postgres", str(exc))
                return _fallo(ESTADO_SQL_INVALIDO, f"No se pudo resolver la consulta: {exc}", sql=consulta.sql,
                              intentos=intento, consulta=consulta.nombre)
            observabilidad.consulta_sql(consulta.sql, intento)
            return _exito(consulta.descripcion, consulta.sql, filas, intento, consulta.periodo, consulta.nota,
                          consulta=consulta.nombre, params=consulta.params)

        respuesta, error = anterior
        return _fallo(ESTADO_SQL_INVALIDO, f"No se pudo elegir la consulta: {error}", seleccion=respuesta,
                      intentos=MAX_INTENTOS)
