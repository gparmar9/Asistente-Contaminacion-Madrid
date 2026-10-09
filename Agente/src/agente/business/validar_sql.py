"""Validador del SQL que escribe el redactor: funciones puras sobre el árbol de `sqlglot`.

Reglas (dialecto PostgreSQL):
- exactamente una sentencia, y es una consulta (SELECT, con WITH o UNION si hace falta);
- ninguna parte modifica datos (DML dentro de un WITH), ni `SELECT ... INTO` ni `FOR UPDATE`;
- solo lee la vista `mediciones_bloques` (las CTE propias cuentan como suyas);
- sin funciones de sistema (`pg_*`, `current_setting`, `set_config`...);
- sin la fecha actual de PostgreSQL (`CURRENT_DATE`, `NOW()`...): no es la de `FECHA_REFERENCIA`;
- con periodo calculado (`business/periodos.py`), toda fecha literal cae dentro de sus rangos (con
  un día de margen por los extremos abiertos: `fecha < '2026-04-27'` para acabar el 26);
- una media (o un máximo o mínimo) no mezcla contaminantes: si agrega `media`, `maximo` o `minimo`,
  esa consulta filtra un solo contaminante o agrupa por `contaminante` (en el segundo lote salía
  «Hortaleza 12,1», la media de NO2, NOx, O3... juntos);
- `LIMIT` impuesto: un `LIMIT` menor que el tope es del modelo («los 5 primeros») y se respeta;
  si falta o llega al tope, se pone `tope + 1` para que el DAL lea la fila de más y sepa que
  hay truncado (con `LIMIT tope` el corte pasaba por datos completos).

Es la primera barrera, no la única: detrás están la sesión de solo lectura y el rol
`agente_lectura`, que solo puede leer la vista (`datos/mediciones.py`).
"""
from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Sequence

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from agente.business.periodos import condicion_sql
from agente.entities.datos import Periodo

VISTA = "mediciones_bloques"
_ESQUEMAS = ("", "public")
_PROHIBIDAS = (exp.Insert, exp.Update, exp.Delete, exp.Merge, exp.Create, exp.Drop, exp.Alter,
               exp.Command, exp.Into, exp.Lock)
_FUNCIONES_SISTEMA = {"current_setting", "set_config", "dblink", "lo_import", "lo_export",
                      "query_to_xml", "version", "inet_server_addr", "current_user", "session_user"}
_FECHA_ACTUAL = (exp.CurrentDate, exp.CurrentTimestamp, exp.CurrentTime, exp.Localtimestamp, exp.Localtime)
_FUNCIONES_FECHA_ACTUAL = {"clock_timestamp", "statement_timestamp", "transaction_timestamp", "timeofday"}
_TEXTOS_FECHA_ACTUAL = {"now", "today", "yesterday", "tomorrow"}  # 'today'::date
_RE_FECHA = re.compile(r"^\s*(\d{4}-\d{2}-\d{2})")
_MARGEN = timedelta(days=1)
_RE_ANSI = re.compile(r"\x1b\[[0-9;]*m")  # sqlglot subraya el error con códigos de color
_MEDIDAS = {"media", "maximo", "minimo"}
_COLUMNAS_FECHA = {"fecha", "ano", "mes"}


class SQLNoValido(ValueError):
    """El SQL no cumple las reglas. El mensaje se entrega al redactor para que lo corrija."""


def validar(sql: str, max_filas: int, periodos: Sequence[Periodo] = ()) -> str:
    """Devuelve el SQL normalizado (PostgreSQL) con el `LIMIT` impuesto, o lanza `SQLNoValido`.

    `max_filas`: filas que se entregan; el `LIMIT` puede ser `max_filas + 1` (ver arriba).
    `periodos`: los calculados para la pregunta; vacío = sin comprobar las fechas literales."""
    try:
        sentencias = [s for s in sqlglot.parse(sql, read="postgres") if s is not None]
    except SqlglotError as exc:
        raise SQLNoValido(f"El SQL no se puede analizar: {_RE_ANSI.sub('', str(exc))[:300]}") from exc
    if len(sentencias) != 1:
        raise SQLNoValido(f"Debe haber exactamente una sentencia y hay {len(sentencias)}")
    consulta = sentencias[0]
    if not isinstance(consulta, (exp.Select, exp.SetOperation)):
        raise SQLNoValido(f"Solo se permiten consultas SELECT, no {consulta.key.upper()}")
    if prohibido := next(consulta.find_all(*_PROHIBIDAS), None):
        raise SQLNoValido(f"La consulta contiene una operación no permitida ({prohibido.key.upper()})")
    _comprobar_relaciones(consulta)
    _comprobar_funciones(consulta)
    _comprobar_fechas(consulta, periodos)
    _comprobar_contaminantes(consulta)
    return _imponer_limite(consulta, max_filas).sql(dialect="postgres")


def filtra_por_fecha(sql: str) -> bool:
    """El SQL (ya validado) acota el periodo: una fecha literal o `fecha`, `ano` o `mes` en un WHERE o
    un HAVING. Si no, consulta todo el histórico."""
    consulta = sqlglot.parse_one(sql, read="postgres")
    if any(l.is_string and _RE_FECHA.match(l.this) for l in consulta.find_all(exp.Literal)):
        return True
    return any(c.name.lower() in _COLUMNAS_FECHA and c.find_ancestor(exp.Where, exp.Having)
               for c in consulta.find_all(exp.Column))


def _comprobar_relaciones(consulta: exp.Expression) -> None:
    propias = {cte.alias_or_name.lower() for cte in consulta.find_all(exp.CTE)}
    for tabla in consulta.find_all(exp.Table):
        nombre = tabla.name.lower()
        if nombre in propias and not tabla.db:
            continue
        if nombre != VISTA or tabla.db.lower() not in _ESQUEMAS or tabla.catalog:
            raise SQLNoValido(f"Solo se puede consultar la vista {VISTA}, no '{tabla.sql(dialect='postgres')}'")


def _comprobar_funciones(consulta: exp.Expression) -> None:
    for funcion in consulta.find_all(exp.Func):
        nombre = (funcion.name if isinstance(funcion, exp.Anonymous) else funcion.sql_name()).lower()
        if nombre.startswith("pg_") or nombre in _FUNCIONES_SISTEMA:
            raise SQLNoValido(f"Función no permitida: {nombre}")


def _comprobar_fechas(consulta: exp.Expression, periodos: Sequence[Periodo]) -> None:
    usar = (f"Usa el periodo calculado tal cual: {condicion_sql(periodos)}" if periodos
            else "Usa fechas literales del calendario.")
    for nodo in consulta.walk():
        if (isinstance(nodo, _FECHA_ACTUAL)
                or (isinstance(nodo, exp.Anonymous) and nodo.name.lower() in _FUNCIONES_FECHA_ACTUAL)
                or (isinstance(nodo, exp.Literal) and nodo.is_string
                    and nodo.this.strip().lower() in _TEXTOS_FECHA_ACTUAL)):
            raise SQLNoValido("No uses la fecha actual de la base de datos (CURRENT_DATE, NOW()...): "
                              f"no es la fecha de referencia. {usar}")
    if not periodos:
        return
    rangos = [r for p in periodos for r in p.rangos]
    for literal in consulta.find_all(exp.Literal):
        if not literal.is_string or not (m := _RE_FECHA.match(literal.this)):
            continue
        try:
            dia = date.fromisoformat(m.group(1))
        except ValueError:
            continue
        if not any(inicio - _MARGEN <= dia <= fin + _MARGEN for inicio, fin in rangos):
            raise SQLNoValido(f"La fecha '{dia}' está fuera del periodo de la pregunta. {usar}")


def _comprobar_contaminantes(consulta: exp.Expression) -> None:
    ctes = {cte.alias_or_name.lower(): cte.this for cte in consulta.find_all(exp.CTE)}
    for select in consulta.find_all(exp.Select):
        mezcla = any(
            agregado.find_ancestor(exp.Select) is select and not agregado.find_ancestor(exp.Window)
            and any(c.name.lower() in _MEDIDAS for c in agregado.find_all(exp.Column))
            for agregado in select.find_all(exp.AggFunc))
        if mezcla and not _agrupa_contaminante(select) and not _un_contaminante(select, ctes):
            raise SQLNoValido("Una media no puede mezclar contaminantes: agrupa también por contaminante "
                              "(GROUP BY contaminante, ...) o filtra uno solo")


def _agrupa_contaminante(select: exp.Select) -> bool:
    grupo = select.args.get("group")
    if not grupo:
        return False
    for expresion in grupo.expressions:
        if isinstance(expresion, exp.Literal) and expresion.is_int:  # GROUP BY 1
            posicion = int(expresion.this) - 1
            expresion = select.expressions[posicion] if 0 <= posicion < len(select.expressions) else expresion
        expresion = expresion.unalias() if isinstance(expresion, exp.Alias) else expresion
        if isinstance(expresion, exp.Column) and expresion.name.lower() == "contaminante":
            return True
    return False


def _un_contaminante(select: exp.Select, ctes: dict[str, exp.Expression], vistos: frozenset = frozenset()) -> bool:
    """El WHERE fija un solo contaminante (`= 'NO2'` o `IN ('NO2')`), aquí o en la CTE de la que lee."""
    filtro = select.args.get("where")
    for nodo in (filtro.find_all(exp.EQ, exp.In) if filtro else ()):
        columna = nodo.this if isinstance(nodo, exp.In) else next(
            (lado for lado in (nodo.this, nodo.expression) if isinstance(lado, exp.Column)), None)
        if not isinstance(columna, exp.Column) or columna.name.lower() != "contaminante":
            continue
        valores = nodo.expressions if isinstance(nodo, exp.In) else [nodo.expression, nodo.this]
        if sum(isinstance(v, exp.Literal) for v in valores) == 1 and not nodo.find_ancestor(exp.Or):
            return True
    origen = select.args.get("from_")
    tabla = origen.this if origen else None
    nombre = tabla.name.lower() if isinstance(tabla, exp.Table) else None
    if nombre in ctes and nombre not in vistos and isinstance(ctes[nombre], exp.Select):
        return _un_contaminante(ctes[nombre], ctes, vistos | {nombre})
    return False


def _imponer_limite(consulta: exp.Query, max_filas: int) -> exp.Query:
    limite = consulta.args.get("limit")
    valor = limite.expression if isinstance(limite, exp.Limit) else None
    if isinstance(valor, exp.Literal) and valor.is_int and int(valor.this) < max_filas:
        return consulta
    return consulta.limit(max_filas + 1, copy=False)
