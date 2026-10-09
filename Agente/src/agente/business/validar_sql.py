"""Validador del SQL que escribe el redactor: funciones puras sobre el árbol de `sqlglot`.

Reglas (dialecto PostgreSQL):
- exactamente una sentencia, y es una consulta (SELECT, con WITH o UNION si hace falta);
- ninguna parte modifica datos (DML dentro de un WITH), ni `SELECT ... INTO` ni `FOR UPDATE`;
- solo lee la vista `mediciones_bloques` (las CTE propias cuentan como suyas);
- sin funciones de sistema (`pg_*`, `current_setting`, `set_config`...);
- `LIMIT` impuesto: se añade si falta y se recorta si pasa del tope.

Es la primera barrera, no la única: detrás están la sesión de solo lectura y el rol
`agente_lectura`, que solo puede leer la vista (`datos/mediciones.py`).
"""
from __future__ import annotations

import re

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

VISTA = "mediciones_bloques"
_ESQUEMAS = ("", "public")
_PROHIBIDAS = (exp.Insert, exp.Update, exp.Delete, exp.Merge, exp.Create, exp.Drop, exp.Alter,
               exp.Command, exp.Into, exp.Lock)
_FUNCIONES_SISTEMA = {"current_setting", "set_config", "dblink", "lo_import", "lo_export",
                      "query_to_xml", "version", "inet_server_addr", "current_user", "session_user"}
_RE_ANSI = re.compile(r"\x1b\[[0-9;]*m")  # sqlglot subraya el error con códigos de color


class SQLNoValido(ValueError):
    """El SQL no cumple las reglas. El mensaje se entrega al redactor para que lo corrija."""


def validar(sql: str, max_filas: int) -> str:
    """Devuelve el SQL normalizado (PostgreSQL) con el `LIMIT` impuesto, o lanza `SQLNoValido`."""
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
    return _imponer_limite(consulta, max_filas).sql(dialect="postgres")


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


def _imponer_limite(consulta: exp.Query, max_filas: int) -> exp.Query:
    limite = consulta.args.get("limit")
    valor = limite.expression if isinstance(limite, exp.Limit) else None
    if isinstance(valor, exp.Literal) and valor.is_int and int(valor.this) <= max_filas:
        return consulta
    return consulta.limit(max_filas, copy=False)
