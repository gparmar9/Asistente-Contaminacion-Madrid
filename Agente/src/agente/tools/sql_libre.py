"""`consultar_datos`: SQL libre sobre la vista `mediciones_bloques`.

    pregunta en lenguaje natural (la escribe el modelo de conversación) + la del usuario (la añade el bucle)
      -> periodos calculados en Python con la pregunta del usuario (`business/periodos.py`)
      -> periodo entero fuera de la vista: estado `sin_datos` con su frase, sin redactor ni base
      -> redactor (LLM_MODELO_SQL) escribe el SQL                 span `redactar_sql`
      -> validador (sqlglot): una sentencia, solo la vista, fechas del periodo, LIMIT impuesto
      -> DAL de solo lectura (statement_timeout, tope de filas)
      -> si el validador o PostgreSQL lo rechazan: un reintento con el error; después, fallo
      -> {descripcion, periodo, columnas, filas, truncado}, cifras a 1 decimal y tope de tamaño;
         `periodo` va siempre: el calculado, «todo el histórico» si el SQL no filtra por fecha o,
         si filtra sin periodo calculado, el aviso de que no está calculado

El SQL no llega nunca al modelo de conversación ni a la respuesta: va en `internos` y a la traza.
`definicion()` comprueba que la base responde (`ultima_fecha()`, cacheada una hora); si no, la
herramienta no se ofrece y una pregunta DATOS recibe la frase de datos no disponibles.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import date
from typing import Any, Callable, Sequence

from opentelemetry.trace import Status, StatusCode

from agente import observabilidad
from agente.business import frases, periodos
from agente.business.redactor_sql import RedactorSQL
from agente.business.validar_sql import SQLNoValido, filtra_por_fecha, validar
from agente.datos.mediciones import ErrorBaseDatos, Mediciones
from agente.entities.chat import Fuente
from agente.entities.datos import ContextoDatos, Periodo, ResultadoConsulta
from agente.tools.base import Herramienta, ResultadoHerramienta

logger = logging.getLogger("agente.tools.sql_libre")

NOMBRE = "consultar_datos"
# Argumentos que añade el bucle (no están en la definición que ve el modelo): de ellos salen las fechas.
ARG_PREGUNTA_USUARIO, ARG_PREGUNTAS_PREVIAS = "pregunta_usuario", "preguntas_previas"
# Estados en `internos`. `no_disponible`: base caída o redactor sin LLM; el bucle no repite la consulta.
# `sin_datos`: el periodo cae entero fuera de la vista; `internos["frase"]` es la respuesta del bucle.
ESTADO_OK, ESTADO_NO_DISPONIBLE, ESTADO_SQL_INVALIDO = "ok", "no_disponible", "sql_invalido"
ESTADO_SIN_DATOS = "sin_datos"
ERROR_NO_DISPONIBLE = "Las mediciones no están disponibles en este momento"
MAX_INTENTOS = 2
# Tope del JSON de filas que entra en el contexto del modelo; por encima se quitan filas del final.
MAX_CARACTERES = 8000

DEFINICION = {
    "type": "function",
    "function": {
        "name": NOMBRE,
        "description": (
            "Consulta las mediciones de las estaciones de calidad del aire de Madrid de los dos "
            "últimos años, por estación, distrito, día y bloque horario. Recibe la pregunta en "
            "lenguaje natural y devuelve una tabla con el resultado."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "pregunta": {
                    "type": "string",
                    "description": "La pregunta sobre las mediciones, en español, con los lugares "
                                   "explícitos y solo los contaminantes que nombra el usuario. El "
                                   "periodo, con las palabras del usuario: las fechas las calcula "
                                   "el sistema.",
                },
            },
            "required": ["pregunta"],
        },
    },
}


class HerramientaDatos(Herramienta):
    """`fecha_referencia`: «hoy» fijo (evaluación); None = `hoy()`, la fecha del sistema."""

    nombre = NOMBRE

    def __init__(self, mediciones: Mediciones, redactor: RedactorSQL, fecha_referencia: date | None = None,
                 max_filas: int = 60, hoy: Callable[[], date] = date.today):
        self._mediciones = mediciones
        self._redactor = redactor
        self._fecha_referencia = fecha_referencia
        self._max_filas = max(1, max_filas)
        self._hoy = hoy
        self._contexto: ContextoDatos | None = None

    async def definicion(self) -> dict | None:
        return DEFINICION if await self._cargar_contexto() else None

    def contexto_prompt(self) -> str:
        c = self._contexto
        if c is None:
            return ""
        return frases.CONTEXTO_FECHAS.format(hoy=c.hoy.isoformat(), primera=c.primera_fecha.isoformat(),
                                             ultima=c.ultima_fecha.isoformat(),
                                             calendario=periodos.calendario(c.hoy, c.primera_fecha, c.ultima_fecha))

    async def ejecutar(self, argumentos: dict[str, Any]) -> ResultadoHerramienta:
        pregunta = argumentos.get("pregunta")
        if not isinstance(pregunta, str) or not pregunta.strip():
            return ResultadoHerramienta.fallo("Falta el parámetro obligatorio 'pregunta'")
        contexto = await self._cargar_contexto()
        if contexto is None:
            return _fallo(ESTADO_NO_DISPONIBLE, ERROR_NO_DISPONIBLE)
        pregunta_usuario = argumentos.get(ARG_PREGUNTA_USUARIO)
        pregunta_usuario = pregunta_usuario if isinstance(pregunta_usuario, str) and pregunta_usuario.strip() else None
        previas = argumentos.get(ARG_PREGUNTAS_PREVIAS)
        calculados = _periodos([pregunta_usuario or pregunta, *(previas if isinstance(previas, list) else [])[::-1]],
                               contexto)
        if calculados and _fuera_de_la_vista(calculados, contexto):
            frase = frases.FRASE_SIN_DATOS_PERIODO.format(periodo=periodos.texto(calculados),
                                                         primera=contexto.primera_fecha.isoformat(),
                                                         ultima=contexto.ultima_fecha.isoformat())
            return _fallo(ESTADO_SIN_DATOS, frase, frase=frase, periodo=periodos.condicion_sql(calculados))

        anterior: tuple[str, str] | None = None  # (sql, error) del intento que falló
        for intento in range(1, MAX_INTENTOS + 1):
            with observabilidad.span("redactar_sql", "chain", entrada=pregunta) as span:
                span.set_attribute("agente.periodo", periodos.condicion_sql(calculados) or "sin calcular")
                try:
                    sql = await self._redactor.redactar(pregunta, contexto, anterior, pregunta_usuario, calculados)
                except Exception as exc:  # el proveedor puede lanzar de todo
                    logger.warning("Redactor SQL no disponible: %s", exc)
                    span.set_status(Status(StatusCode.ERROR, f"redactor no disponible: {exc}"))
                    return _fallo(ESTADO_NO_DISPONIBLE, ERROR_NO_DISPONIBLE, intentos=intento)
                span.set_output(sql)
                try:
                    sql = validar(sql, self._max_filas, calculados)
                except SQLNoValido as exc:
                    observabilidad.sql_invalido("validador", str(exc))
                    anterior = (sql, str(exc))
                    continue
            try:
                consulta = await asyncio.to_thread(self._mediciones.ejecutar, sql, None, self._max_filas)
            except ErrorBaseDatos as exc:
                if exc.conexion:
                    logger.warning("Base de datos no disponible: %s", exc)
                    return _fallo(ESTADO_NO_DISPONIBLE, ERROR_NO_DISPONIBLE, sql=sql, intentos=intento)
                observabilidad.sql_invalido("postgres", str(exc))
                anterior = (sql, str(exc))
                continue
            observabilidad.consulta_sql(sql, intento)
            return _exito(pregunta, sql, consulta, intento, _periodo_respuesta(sql, calculados, contexto))

        sql, error = anterior
        return _fallo(ESTADO_SQL_INVALIDO, f"No se pudo resolver la consulta: {error}", sql=sql,
                      intentos=MAX_INTENTOS)

    async def _cargar_contexto(self) -> ContextoDatos | None:
        """Fechas y estaciones (cacheadas en el DAL). None si la base no responde o está vacía."""
        try:
            ultima = await asyncio.to_thread(self._mediciones.ultima_fecha)
            estaciones = await asyncio.to_thread(self._mediciones.estaciones) if ultima else ()
        except ErrorBaseDatos as exc:
            logger.warning("La herramienta %s no se ofrece: %s", NOMBRE, exc)
            return None
        if ultima is None:
            logger.warning("La herramienta %s no se ofrece: la vista está vacía", NOMBRE)
            return None
        self._contexto = ContextoDatos(hoy=self._fecha_referencia or self._hoy(),
                                       primera_fecha=_dos_anos_antes(ultima), ultima_fecha=ultima,
                                       estaciones=estaciones)
        return self._contexto


def _periodos(textos: list[Any], contexto: ContextoDatos) -> list[Periodo]:
    """Los de la primera pregunta que traiga alguno: la del usuario y después sus previas, de la más
    reciente a la más antigua («¿y en Vallecas?» hereda el periodo). Vacía si ninguna lo trae."""
    for texto in textos:
        if isinstance(texto, str) and (encontrados := periodos.resolver(
                texto, contexto.hoy, contexto.primera_fecha, contexto.ultima_fecha)):
            return encontrados
    return []


def _fuera_de_la_vista(calculados: Sequence[Periodo], contexto: ContextoDatos) -> bool:
    """Ningún día de ningún rango cae entre la primera y la última fecha con datos («en 2019»)."""
    return all(fin < contexto.primera_fecha or inicio > contexto.ultima_fecha
               for p in calculados for inicio, fin in p.rangos)


def _periodo_respuesta(sql: str, calculados: Sequence[Periodo], contexto: ContextoDatos) -> str:
    """El periodo que se dice en la respuesta. Sin periodo calculado, el SQL decide: si no filtra por
    fecha es todo el histórico (sin él, la síntesis se inventaba la ventana: «últimos 12 meses»)."""
    if calculados:
        return periodos.texto(calculados)
    if not filtra_por_fecha(sql):
        return frases.PERIODO_HISTORICO.format(primera=contexto.primera_fecha.isoformat(),
                                               ultima=contexto.ultima_fecha.isoformat())
    return frases.PERIODO_NO_CALCULADO


def _exito(pregunta: str, sql: str, consulta: ResultadoConsulta, intentos: int,
           periodo: str) -> ResultadoHerramienta:
    filas, recortado = _recortar([[_redondear(v) for v in fila] for fila in consulta.filas])
    descripcion = f"Mediciones de las estaciones de Madrid: {pregunta.strip()}"
    datos = {"descripcion": descripcion, "periodo": periodo,
             "columnas": list(consulta.columnas), "filas": filas, "truncado": consulta.truncado or recortado}
    internos = {"estado": ESTADO_OK, "sql": sql, "intentos": intentos, "filas": len(filas),
                "periodo": periodo}
    return ResultadoHerramienta.exito(datos, fuentes=(Fuente(tipo="sql", referencia=descripcion),),
                                      internos=internos)


def _fallo(estado: str, error: str, **internos: Any) -> ResultadoHerramienta:
    return ResultadoHerramienta.fallo(error, internos={"estado": estado, **internos})


def _redondear(valor: Any) -> Any:
    """Las cifras salen ya redondeadas: el modelo las copia tal cual, sin redondear por su cuenta."""
    return round(valor, 1) if isinstance(valor, float) else valor


def _recortar(filas: list[list[Any]]) -> tuple[list[list[Any]], bool]:
    """Filas que caben en `MAX_CARACTERES` de JSON, desde la primera; y si se quitó alguna."""
    total = 0
    for i, fila in enumerate(filas):
        total += len(json.dumps(fila, ensure_ascii=False)) + 1
        if total > MAX_CARACTERES:
            return filas[:i], True
    return filas, False


def _dos_anos_antes(dia: date) -> date:
    """Primer día de la vista: corta 2 años antes del último (`vista_mediciones_bloques.sql`)."""
    try:
        return dia.replace(year=dia.year - 2)
    except ValueError:  # 29 de febrero
        return dia.replace(year=dia.year - 2, day=28)
