"""Catálogo cerrado de consultas sobre `mediciones_bloques`: la alternativa al SQL libre (fase 5).

El selector (`business/selector_catalogo.py`, un LLM) elige una consulta y sus parámetros en JSON;
este módulo valida la elección y construye el SQL. El modelo no escribe SQL ni calcula fechas:

- consulta, agrupación, orden, bloque y granularidad: valores cerrados; otro valor → `SeleccionNoValida`
  (la herramienta reintenta una vez con el error);
- contaminantes y lugares: los que nombra el usuario. Uno que no existe no es un error del modelo:
  la consulta sigue con los demás y la `nota` lo dice («el SO2 no se mide»); si no queda ninguno,
  la consulta no se ejecuta (`vacia`);
- sin contaminante, en las consultas de niveles: NO2 y PM10, los de referencia (decisión del
  2026-10-08). En anomalías y estaciones, todos;
- fechas: las del periodo calculado en Python (`business/periodos.py`); sin periodo ni mención de
  tiempo, todo el histórico; con mención no reconocida, `fecha_inicio`/`fecha_fin` del selector, que
  las toma del calendario del prompt;
- toda media es ponderada por horas, SUM(media * n_horas) / SUM(n_horas), y los rankings y
  comparaciones llevan los días con dato. Las comparaciones traen la diferencia ya calculada: la
  síntesis no calcula.

SQL en el subconjunto común de PostgreSQL y SQLite (los tests): sin `FILTER`, `ILIKE` ni
`date_trunc`. Las fechas van como literales ISO (las escribe Python, no el modelo); contaminantes,
lugares, bloque y umbral, como parámetros. `LIMIT tope + 1`: el DAL lee la fila de más para avisar
del truncado. Funciones puras, sin LLM ni base de datos.
"""
from __future__ import annotations

import json
import re
import unicodedata
from datetime import date
from typing import Any, Sequence

from agente.business import frases
from agente.business import periodos as periodos_
from agente.entities.datos import ConsultaCatalogo, ContextoDatos, Periodo

VISTA = "mediciones_bloques"
CONSULTAS = ("ranking", "serie", "patron_horario", "fin_de_semana", "dias_superados", "anomalias", "estaciones")
CONTAMINANTES = ("NO", "NO2", "NOx", "O3", "PM10", "PM2.5")
DE_REFERENCIA = ("NO2", "PM10")
BLOQUES = {"madrugada": "0 a 6 h", "manana": "7 a 12 h", "tarde": "13 a 19 h", "noche": "20 a 23 h"}
AGRUPACIONES = ("estacion", "distrito", "madrid")
ORDENES = ("peor", "mejor")
GRANULARIDADES = ("dia", "mes", "ano")
# Sin contaminante en la pregunta, las de niveles usan los de referencia; anomalías y estaciones, todos.
_NIVELES = frozenset({"ranking", "serie", "patron_horario", "fin_de_semana", "dias_superados"})
_ALIAS = {"NOX": "NOx", "OZONO": "O3", "PM25": "PM2.5"}
_ENTIDAD = {"estacion": ("nombre_estacion", "distrito"), "distrito": ("distrito",), "madrid": ()}
_TEXTO_ENTIDAD = {"estacion": "por estación", "distrito": "por distrito", "madrid": "en toda la ciudad"}
_TIEMPO = {"dia": ("fecha",), "mes": ("ano", "mes"), "ano": ("ano",)}
_TEXTO_TIEMPO = {"dia": "diaria", "mes": "mensual", "ano": "anual"}
_MEDIA = "SUM(media * n_horas) / NULLIF(SUM(n_horas), 0)"
_RE_JSON = re.compile(r"\{.*\}", re.DOTALL)


class SeleccionNoValida(ValueError):
    """La elección del selector no se puede construir; el mensaje va al reintento."""


def interpretar(texto: str) -> dict[str, Any]:
    """El objeto JSON de la respuesta del selector (tolera texto o un bloque de código alrededor)."""
    encontrado = _RE_JSON.search(texto or "")
    if encontrado is None:
        raise SeleccionNoValida("La respuesta no contiene un objeto JSON")
    try:
        seleccion = json.loads(encontrado.group(0))
    except ValueError as exc:
        raise SeleccionNoValida(f"El JSON no es válido: {exc}") from exc
    if not isinstance(seleccion, dict):
        raise SeleccionNoValida("La respuesta debe ser un objeto JSON")
    return seleccion


def construir(seleccion: dict[str, Any], contexto: ContextoDatos, periodos: Sequence[Periodo] = (),
              sin_fechas: bool = False, max_filas: int = 60) -> ConsultaCatalogo:
    """SQL, parámetros y descripción de la consulta elegida. `periodos` y `sin_fechas`: como en el
    SQL libre (`tools/sql_libre.py`)."""
    nombre = _cerrado(seleccion, "consulta", CONSULTAS, None)
    estaciones, distritos, desconocidos = _lugares(seleccion, contexto)
    # Por defecto, por estación; la serie sin lugar, una cifra para toda la ciudad («¿ha empeorado el ozono?»).
    defecto = "madrid" if nombre == "serie" and not (estaciones or distritos) else "estacion"
    agrupar = "estacion" if nombre == "estaciones" else _cerrado(seleccion, "agrupar_por", AGRUPACIONES, defecto)
    orden = _cerrado(seleccion, "orden", ORDENES, "peor")
    granularidad = _cerrado(seleccion, "granularidad", GRANULARIDADES, "dia")
    bloque = _cerrado(seleccion, "bloque", tuple(BLOQUES), None)
    umbral = _umbral(seleccion.get("umbral")) if nombre == "dias_superados" else None
    rangos, periodo = _fechas(seleccion, contexto, periodos, sin_fechas)

    notas: list[str] = []
    contaminantes, no_medidos = _contaminantes(seleccion.get("contaminantes"))
    if no_medidos:
        notas.append(frases.NOTA_NO_MEDIDO.format(pedidos=", ".join(no_medidos), medidos=", ".join(CONTAMINANTES)))
    if desconocidos:
        notas.append(frases.NOTA_LUGAR_DESCONOCIDO.format(lugares=", ".join(desconocidos)))
    if (no_medidos and not contaminantes) or (desconocidos and not estaciones and not distritos):
        return ConsultaCatalogo(nombre, "", {}, frases.DESCRIPCION_VACIA, periodo, " ".join(notas), vacia=True)
    if not contaminantes and nombre in _NIVELES:
        contaminantes = list(DE_REFERENCIA)
    if nombre == "patron_horario":
        bloque = bloque or "manana"  # «hora punta»; el bloque es el que se compara, no un filtro
    elif bloque is None:
        bloque = next((p.bloque for p in periodos if p.bloque), None)  # «esta tarde»

    params: dict[str, Any] = {}
    condiciones = []
    if contaminantes:
        condiciones.append(_en("contaminante", contaminantes, "c", params))
    lugares = [_en(columna, valores, prefijo, params)
               for columna, valores, prefijo in (("nombre_estacion", estaciones, "e"), ("distrito", distritos, "d"))
               if valores]
    if lugares:
        condiciones.append(lugares[0] if len(lugares) == 1 else "(" + " OR ".join(lugares) + ")")
    if rangos:
        condiciones.append(_condicion_fechas(rangos))
    if bloque:
        params["bloque"] = bloque
        if nombre != "patron_horario":
            condiciones.append("bloque = :bloque")
    if nombre == "anomalias":
        condiciones.append("is_anomaly AND cobertura >= 0.7")
    where = " WHERE " + " AND ".join(condiciones) if condiciones else ""

    entidad = _ENTIDAD[agrupar]
    if nombre == "ranking":
        sql = _ranking(where, entidad, orden)
    elif nombre == "serie":
        sql = _serie(where, entidad, _TIEMPO[granularidad])
    elif nombre == "patron_horario":
        sql = _patron_horario(where, entidad, bloque)
    elif nombre == "fin_de_semana":
        sql = _fin_de_semana(where, entidad)
    elif nombre == "dias_superados":
        sql = _dias_superados(where, entidad)
        params["umbral"] = umbral
    elif nombre == "anomalias":
        sql = _anomalias(where, entidad)
    else:
        sql = _estaciones(where, bool(contaminantes))
    descripcion = _descripcion(nombre, agrupar, orden, granularidad, bloque, umbral, contaminantes,
                               [*estaciones, *distritos])
    return ConsultaCatalogo(nombre, f"{sql} LIMIT {max(1, max_filas) + 1}", params, descripcion, periodo,
                            " ".join(notas) or None)


# ---------------------------------------------------------------------- consultas

def _ranking(where: str, entidad: tuple[str, ...], orden: str) -> str:
    estaciones = "" if entidad == _ENTIDAD["estacion"] else ", COUNT(DISTINCT nombre_estacion) AS estaciones"
    return (f"SELECT contaminante{_cols(entidad)}, {_MEDIA} AS media{estaciones}, "
            f"COUNT(DISTINCT fecha) AS dias_con_dato FROM {VISTA}{where} GROUP BY contaminante{_cols(entidad)} "
            f"ORDER BY contaminante, media {'DESC' if orden == 'peor' else 'ASC'}")


def _serie(where: str, entidad: tuple[str, ...], tiempo: tuple[str, ...]) -> str:
    dias = "" if tiempo == ("fecha",) else ", COUNT(DISTINCT fecha) AS dias_con_dato"
    estaciones = "" if entidad == _ENTIDAD["estacion"] else ", COUNT(DISTINCT nombre_estacion) AS estaciones"
    t = ", ".join(tiempo)
    return (f"SELECT {t}, contaminante{_cols(entidad)}, {_MEDIA} AS media{dias}{estaciones} FROM {VISTA}{where} "
            f"GROUP BY {t}, contaminante{_cols(entidad)} ORDER BY contaminante{_cols(entidad)}, {t}")


def _patron_horario(where: str, entidad: tuple[str, ...], bloque: str) -> str:
    del_bloque, resto = f"media_{bloque}", "media_resto_del_dia"
    return (f"WITH d AS (SELECT contaminante{_cols(entidad)}, "
            f"{_media_si('bloque = :bloque')} AS {del_bloque}, {_media_si('bloque <> :bloque')} AS {resto}, "
            f"COUNT(DISTINCT fecha) AS dias_con_dato FROM {VISTA}{where} GROUP BY contaminante{_cols(entidad)}) "
            f"SELECT contaminante{_cols(entidad)}, {del_bloque}, {resto}, {del_bloque} - {resto} AS diferencia, "
            f"dias_con_dato FROM d WHERE {del_bloque} IS NOT NULL AND {resto} IS NOT NULL "
            f"ORDER BY contaminante, diferencia DESC")


def _fin_de_semana(where: str, entidad: tuple[str, ...]) -> str:
    return (f"WITH d AS (SELECT contaminante{_cols(entidad)}, {_media_si('NOT es_fin_semana')} AS media_laborables, "
            f"{_media_si('es_fin_semana')} AS media_fin_de_semana, COUNT(DISTINCT fecha) AS dias_con_dato "
            f"FROM {VISTA}{where} GROUP BY contaminante{_cols(entidad)}) "
            f"SELECT contaminante{_cols(entidad)}, media_laborables, media_fin_de_semana, "
            f"media_laborables - media_fin_de_semana AS diferencia, "
            f"100 * (media_laborables - media_fin_de_semana) / media_laborables AS diferencia_pct, dias_con_dato "
            f"FROM d WHERE media_laborables > 0 AND media_fin_de_semana IS NOT NULL "
            f"ORDER BY contaminante, diferencia DESC")


def _dias_superados(where: str, entidad: tuple[str, ...]) -> str:
    return (f"WITH dia AS (SELECT contaminante{_cols(entidad)}, fecha, {_MEDIA} AS media_dia FROM {VISTA}{where} "
            f"GROUP BY contaminante{_cols(entidad)}, fecha) "
            f"SELECT contaminante{_cols(entidad)}, SUM(CASE WHEN media_dia > :umbral THEN 1 ELSE 0 END) "
            f"AS dias_superados, COUNT(*) AS dias_con_dato FROM dia GROUP BY contaminante{_cols(entidad)} "
            f"ORDER BY contaminante, dias_superados DESC")


def _anomalias(where: str, entidad: tuple[str, ...]) -> str:
    grupo = ", ".join((*entidad, "contaminante"))
    return (f"SELECT {grupo}, COUNT(*) AS bloques_anomalos, COUNT(DISTINCT fecha) AS dias FROM {VISTA}{where} "
            f"GROUP BY {grupo} ORDER BY bloques_anomalos DESC, {grupo}")


def _estaciones(where: str, filtrada: bool) -> str:
    if filtrada:  # qué estaciones miden esos contaminantes
        return (f"SELECT contaminante, nombre_estacion, distrito, tipo_estacion, COUNT(DISTINCT fecha) AS "
                f"dias_con_dato FROM {VISTA}{where} GROUP BY contaminante, nombre_estacion, distrito, tipo_estacion "
                f"ORDER BY contaminante, distrito, nombre_estacion")
    mide = ", ".join(f"MAX(CASE WHEN contaminante = '{c}' THEN 1 ELSE 0 END) AS mide_{_columna(c)}"
                     for c in CONTAMINANTES)
    return (f"SELECT nombre_estacion, distrito, tipo_estacion, {mide} FROM {VISTA}{where} "
            f"GROUP BY nombre_estacion, distrito, tipo_estacion ORDER BY distrito, nombre_estacion")


def _media_si(condicion: str) -> str:
    """Media ponderada de las filas que cumplen `condicion` (CASE: SQLite no tiene FILTER en todas las versiones)."""
    return (f"SUM(CASE WHEN {condicion} THEN media * n_horas END) / "
            f"NULLIF(SUM(CASE WHEN {condicion} THEN n_horas END), 0)")


def _cols(columnas: Sequence[str]) -> str:
    return "".join(f", {c}" for c in columnas)


def _columna(contaminante: str) -> str:
    return contaminante.lower().replace(".", "")


def _en(columna: str, valores: Sequence[str], prefijo: str, params: dict[str, Any]) -> str:
    nombres = []
    for i, valor in enumerate(valores):
        params[f"{prefijo}{i}"] = valor
        nombres.append(f":{prefijo}{i}")
    return f"{columna} = {nombres[0]}" if len(nombres) == 1 else f"{columna} IN ({', '.join(nombres)})"


def _condicion_fechas(rangos: Sequence[tuple[date, date]]) -> str:
    partes = [f"fecha = '{i.isoformat()}'" if i == f else f"fecha BETWEEN '{i.isoformat()}' AND '{f.isoformat()}'"
              for i, f in rangos]
    return partes[0] if len(partes) == 1 else "(" + " OR ".join(partes) + ")"


# ---------------------------------------------------------------------- parámetros

def _cerrado(seleccion: dict[str, Any], clave: str, validos: Sequence[str], defecto: str | None) -> str | None:
    valor = seleccion.get(clave)
    if valor is None or (isinstance(valor, str) and not valor.strip()):
        if defecto is None and clave == "consulta":
            raise SeleccionNoValida(f"Falta 'consulta': una de {', '.join(validos)}")
        return defecto
    normalizado = _clave(str(valor)).replace(" ", "_")
    if normalizado not in validos:
        raise SeleccionNoValida(f"'{clave}' debe ser uno de {', '.join(validos)}, no {valor!r}")
    return normalizado


def _umbral(valor: Any) -> float:
    try:
        umbral = float(str(valor).replace(",", "."))
    except (TypeError, ValueError):
        raise SeleccionNoValida("'dias_superados' necesita 'umbral': un número en µg/m³") from None
    if umbral <= 0:
        raise SeleccionNoValida("'umbral' debe ser mayor que 0")
    return umbral


def _contaminantes(valor: Any) -> tuple[list[str], list[str]]:
    """(medidos, no medidos) de los que nombra la selección, sin repetir."""
    medidos: list[str] = []
    no_medidos: list[str] = []
    for nombre in _como_lista(valor):
        clave = re.sub(r"[\s_\-]", "", nombre.upper()).replace(",", ".")
        clave = _ALIAS.get(clave, clave)
        canonico = next((c for c in CONTAMINANTES if c.upper() == clave.upper()), None)
        destino, texto = (medidos, canonico) if canonico else (no_medidos, nombre.strip())
        if texto not in destino:
            destino.append(texto)
    return medidos, no_medidos


def _lugares(seleccion: dict[str, Any], contexto: ContextoDatos) -> tuple[list[str], list[str], list[str]]:
    """(nombres de estación, distritos, desconocidos) con los nombres exactos de la vista. Coincidencia
    sin tildes ni mayúsculas, por contenido: «Vallecas» encuentra «Vallecas» y «Ensanche de Vallecas».
    Un término se busca, venga de la lista que venga: nombre exacto de estación, de distrito, y
    después por contenido en estaciones y en distritos («Villa de Vallecas» es el distrito, no la
    estación «Vallecas»). «Madrid» no filtra."""
    estaciones: list[str] = []
    distritos: list[str] = []
    desconocidos: list[str] = []
    nombres = sorted({e[1] for e in contexto.estaciones if e[1]})
    todos_distritos = sorted({e[2] for e in contexto.estaciones if e[2]})
    for termino in [*_como_lista(seleccion.get("estaciones")), *_como_lista(seleccion.get("distritos"))]:
        clave = _clave(termino)
        if not clave or clave == "madrid":
            continue
        for exacto in (True, False):
            if encontradas := _coinciden(clave, nombres, exacto):
                estaciones.extend(n for n in encontradas if n not in estaciones)
                break
            if encontrados := _coinciden(clave, todos_distritos, exacto):
                distritos.extend(d for d in encontrados if d not in distritos)
                break
        else:
            if termino.strip() not in desconocidos:
                desconocidos.append(termino.strip())
    return estaciones, distritos, desconocidos


def _coinciden(clave: str, candidatos: Sequence[str], exacto: bool) -> list[str]:
    if exacto:
        return [c for c in candidatos if _clave(c) == clave]
    return [c for c in candidatos if clave in _clave(c) or _clave(c) in clave]


def _fechas(seleccion: dict[str, Any], contexto: ContextoDatos, periodos: Sequence[Periodo],
            sin_fechas: bool) -> tuple[list[tuple[date, date]], str]:
    """(rangos, texto del periodo para la respuesta). Manda el periodo calculado; sin él y sin mención
    de tiempo, todo el histórico; con mención no reconocida, las fechas del selector (si las da)."""
    if periodos:
        return [r for p in periodos for r in p.rangos], periodos_.texto(periodos)
    historico = frases.PERIODO_HISTORICO.format(primera=contexto.primera_fecha.isoformat(),
                                                ultima=contexto.ultima_fecha.isoformat())
    inicio, fin = seleccion.get("fecha_inicio"), seleccion.get("fecha_fin")
    if sin_fechas or not (inicio or fin):
        return [], historico
    try:
        desde = date.fromisoformat(str(inicio)) if inicio else contexto.primera_fecha
        hasta = date.fromisoformat(str(fin)) if fin else contexto.ultima_fecha
    except ValueError:
        raise SeleccionNoValida("'fecha_inicio' y 'fecha_fin' van en formato YYYY-MM-DD") from None
    if desde > hasta:
        raise SeleccionNoValida("'fecha_inicio' no puede ser posterior a 'fecha_fin'")
    return [(desde, hasta)], f"del {desde.isoformat()} al {hasta.isoformat()}"


def _descripcion(nombre: str, agrupar: str, orden: str, granularidad: str, bloque: str | None,
                 umbral: float | None, contaminantes: Sequence[str], lugares: Sequence[str]) -> str:
    conts = " y ".join(contaminantes) if contaminantes else "todos los contaminantes"
    por = "en conjunto" if agrupar == "madrid" and lugares else _TEXTO_ENTIDAD[agrupar]
    franja = f"bloque {bloque} ({BLOQUES[bloque]})" if bloque else ""
    if nombre == "ranking":
        texto = (f"Media ponderada de {conts} {por}, de "
                 + ("más a menos" if orden == "peor" else "menos a más") + " contaminación")
    elif nombre == "serie":
        texto = f"Evolución {_TEXTO_TIEMPO[granularidad]} de la media ponderada de {conts} {por}"
    elif nombre == "patron_horario":
        texto = f"Media ponderada de {conts} en el {franja} frente al resto del día {por}; diferencia = bloque − resto"
        franja = ""
    elif nombre == "fin_de_semana":
        texto = (f"Media ponderada de {conts} en días laborables y en fines de semana {por}; "
                 "diferencia = laborables − fin de semana")
    elif nombre == "dias_superados":
        texto = f"Días con media diaria ponderada de {conts} por encima de {umbral:g} µg/m³ {por}"
    elif nombre == "anomalias":
        texto = f"Bloques horarios anómalos según el modelo de ML (cobertura >= 0,7) de {conts} {por}"
    else:
        texto = f"Estaciones que miden {conts}" if contaminantes else "Estaciones y contaminantes que mide cada una"
    if franja:
        texto += f", {franja}"
    if lugares:
        texto += f"; solo {', '.join(lugares)}"
    return texto + " (µg/m³)" if nombre not in ("anomalias", "estaciones") else texto


def _como_lista(valor: Any) -> list[str]:
    if valor is None:
        return []
    if isinstance(valor, str):
        valor = valor.split(",")
    if not isinstance(valor, (list, tuple)):
        return [str(valor)]
    return [str(v) for v in valor if v is not None and str(v).strip()]


def _clave(texto: str) -> str:
    """Minúsculas, sin tildes y con los espacios normalizados."""
    sin_tildes = "".join(c for c in unicodedata.normalize("NFD", texto) if not unicodedata.combining(c))
    return " ".join(sin_tildes.lower().split())
