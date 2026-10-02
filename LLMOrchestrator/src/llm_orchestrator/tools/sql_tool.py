"""Tool `query_sql`: consultas predefinidas y parametrizadas sobre PostgreSQL.

Decisión D3 del proyecto: el LLM NO genera SQL libre. Elige una consulta de un
catálogo cerrado y rellena parámetros validados (enums, enteros, fechas ISO);
este módulo construye el único SELECT posible con parámetros ligados. Así es
imposible inyectar SQL o escribir en la base de datos, y las respuestas son
deterministas y fáciles de defender.

Todas las consultas filtran por `magnitud` (entero) para encajar con el índice
(estacion, magnitud, fecha) de `resumen_datos_ml`, y limitan las filas devueltas.
"""
from datetime import date, datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Engine

from llm_orchestrator.shared.constantes import BLOQUES, CONTAMINANTES, MAGNITUD_POR_CONTAMINANTE

LIMITE_FILAS = 60
VENTANA_POR_DEFECTO_DIAS = 7

ESQUEMA_TOOL = {
    "type": "function",
    "function": {
        "name": "query_sql",
        "description": (
            "Consulta las mediciones reales de calidad del aire de Madrid "
            "(PostgreSQL, bloques horarios con detección de anomalías). "
            "Usa SIEMPRE esta tool para cualquier cifra o dato: nunca inventes valores. "
            "Consultas: 'ultimos_niveles' (último día disponible de un contaminante), "
            "'serie_bloques' (evolución de una estación y contaminante entre fechas), "
            "'anomalias' (bloques anómalos detectados por el modelo), "
            "'comparar_estaciones' (ranking de estaciones por nivel medio), "
            "'info_estaciones' (catálogo: distrito, tipo, qué mide cada estación)."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "consulta": {
                    "type": "string",
                    "enum": ["ultimos_niveles", "serie_bloques", "anomalias",
                             "comparar_estaciones", "info_estaciones"],
                    "description": "Qué consulta ejecutar",
                },
                "contaminante": {
                    "type": "string",
                    "enum": list(CONTAMINANTES),
                    "description": "Contaminante en µg/m³. Obligatorio salvo en 'info_estaciones' y opcional en 'anomalias'",
                },
                "estacion": {
                    "type": "integer",
                    "description": "Código corto de la estación (p. ej. 49 = Parque del Retiro). Obligatorio en 'serie_bloques'",
                },
                "distrito": {
                    "type": "string",
                    "description": "Filtrar por distrito de Madrid (solo 'info_estaciones')",
                },
                "fecha_inicio": {"type": "string", "description": "Primer día, formato YYYY-MM-DD"},
                "fecha_fin": {"type": "string", "description": "Último día incluido, formato YYYY-MM-DD"},
                "bloque": {
                    "type": "string",
                    "enum": list(BLOQUES),
                    "description": "Filtrar por un bloque del día",
                },
            },
            "required": ["consulta"],
        },
    },
}

# Columnas devueltas al LLM en las consultas de bloques (con nombre de estación)
_COLS_BLOQUE = (
    "r.estacion, e.nombre AS nombre_estacion, e.distrito, r.contaminante, "
    "r.fecha, r.bloque, r.media, r.maximo, r.cobertura, r.z_score, r.is_anomaly"
)
_DESDE_BLOQUE = "FROM resumen_datos_ml r JOIN estaciones e ON e.codigo_corto = r.estacion"


def _a_fecha(valor) -> date:
    """Normaliza `fecha` (timestamp en PostgreSQL, texto en SQLite) a `date`."""
    if isinstance(valor, datetime):
        return valor.date()
    if isinstance(valor, date):
        return valor
    return date.fromisoformat(str(valor)[:10])


def _parsear_fecha(valor, nombre: str) -> date:
    try:
        return date.fromisoformat(str(valor))
    except ValueError as exc:
        raise ValueError(f"'{nombre}' debe tener formato YYYY-MM-DD, no {valor!r}") from exc


def _rango(args: dict) -> tuple[date, date]:
    """Rango [inicio, fin] con la ventana por defecto de los últimos 7 días."""
    fin = _parsear_fecha(args["fecha_fin"], "fecha_fin") if args.get("fecha_fin") else date.today()
    inicio = (_parsear_fecha(args["fecha_inicio"], "fecha_inicio")
              if args.get("fecha_inicio") else fin - timedelta(days=VENTANA_POR_DEFECTO_DIAS - 1))
    if inicio > fin:
        raise ValueError("'fecha_inicio' no puede ser posterior a 'fecha_fin'")
    return inicio, fin


def _magnitud(args: dict) -> int:
    contaminante = args.get("contaminante")
    if contaminante not in CONTAMINANTES:
        raise ValueError(f"'contaminante' debe ser uno de {list(CONTAMINANTES)}, no {contaminante!r}")
    return MAGNITUD_POR_CONTAMINANTE[contaminante]


def _filas(engine: Engine, sql: str, params: dict) -> list[dict]:
    with engine.connect() as conn:
        filas = [dict(f) for f in conn.execute(text(sql), params).mappings().all()]
    for f in filas:
        if "fecha" in f:
            f["fecha"] = _a_fecha(f["fecha"]).isoformat()
    return filas


def _ultimos_niveles(engine: Engine, args: dict) -> dict:
    magnitud = _magnitud(args)
    filtro_estacion = "AND r.estacion = :estacion" if args.get("estacion") is not None else ""
    filtro_bloque = "AND r.bloque = :bloque" if args.get("bloque") else ""
    # El MAX(fecha) respeta el filtro de estación: si una estación va con retraso,
    # se devuelve SU último día con datos, no un vacío.
    sub_estacion = "AND estacion = :estacion" if args.get("estacion") is not None else ""
    sql = (
        f"SELECT {_COLS_BLOQUE} {_DESDE_BLOQUE} "
        "WHERE r.magnitud = :magnitud "
        "  AND r.fecha = (SELECT MAX(fecha) FROM resumen_datos_ml "
        f"                 WHERE magnitud = :magnitud {sub_estacion}) "
        f"  {filtro_estacion} {filtro_bloque} "
        f"ORDER BY r.estacion, r.hora_inicio LIMIT {LIMITE_FILAS}"
    )
    params: dict = {"magnitud": magnitud}
    if args.get("estacion") is not None:
        params["estacion"] = int(args["estacion"])
    if args.get("bloque"):
        params["bloque"] = args["bloque"]
    filas = _filas(engine, sql, params)
    return {"descripcion": f"Bloques del último día disponible de {args['contaminante']} (µg/m³)",
            "filas": filas}


def _serie_bloques(engine: Engine, args: dict) -> dict:
    if args.get("estacion") is None:
        raise ValueError("'serie_bloques' necesita el parámetro 'estacion' (código corto)")
    magnitud = _magnitud(args)
    inicio, fin = _rango(args)
    filtro_bloque = "AND r.bloque = :bloque" if args.get("bloque") else ""
    sql = (
        f"SELECT {_COLS_BLOQUE} {_DESDE_BLOQUE} "
        "WHERE r.estacion = :estacion AND r.magnitud = :magnitud "
        "  AND r.fecha >= :inicio AND r.fecha < :fin_mas_1 "
        f"  {filtro_bloque} "
        f"ORDER BY r.fecha, r.hora_inicio LIMIT {LIMITE_FILAS}"
    )
    params: dict = {"estacion": int(args["estacion"]), "magnitud": magnitud,
                    "inicio": inicio, "fin_mas_1": fin + timedelta(days=1)}
    if args.get("bloque"):
        params["bloque"] = args["bloque"]
    return {"descripcion": (f"Serie de {args['contaminante']} en la estación {args['estacion']} "
                            f"del {inicio.isoformat()} al {fin.isoformat()} (µg/m³)"),
            "filas": _filas(engine, sql, params)}


def _anomalias(engine: Engine, args: dict) -> dict:
    inicio, fin = _rango(args)
    filtros, params = [], {"inicio": inicio, "fin_mas_1": fin + timedelta(days=1)}
    if args.get("contaminante"):
        filtros.append("AND r.magnitud = :magnitud")
        params["magnitud"] = _magnitud(args)
    if args.get("estacion") is not None:
        filtros.append("AND r.estacion = :estacion")
        params["estacion"] = int(args["estacion"])
    sql = (
        f"SELECT {_COLS_BLOQUE}, r.anomaly_score {_DESDE_BLOQUE} "
        "WHERE r.is_anomaly AND r.cobertura >= 0.7 "
        "  AND r.fecha >= :inicio AND r.fecha < :fin_mas_1 "
        f"  {' '.join(filtros)} "
        f"ORDER BY r.anomaly_score DESC LIMIT {LIMITE_FILAS}"
    )
    filas = _filas(engine, sql, params)
    return {"descripcion": (f"Bloques anómalos del {inicio.isoformat()} al {fin.isoformat()} "
                            "(solo con cobertura >= 0,7; anomaly_score alto = más anómalo)"),
            "filas": filas}


def _comparar_estaciones(engine: Engine, args: dict) -> dict:
    magnitud = _magnitud(args)
    inicio, fin = _rango(args)
    sql = (
        "SELECT r.estacion, e.nombre AS nombre_estacion, e.distrito, "
        "       AVG(r.media) AS media_periodo, MAX(r.maximo) AS maximo_periodo, "
        "       COUNT(*) AS n_bloques "
        f"{_DESDE_BLOQUE} "
        "WHERE r.magnitud = :magnitud AND r.fecha >= :inicio AND r.fecha < :fin_mas_1 "
        "GROUP BY r.estacion, e.nombre, e.distrito "
        f"ORDER BY media_periodo DESC LIMIT {LIMITE_FILAS}"
    )
    filas = _filas(engine, sql, {"magnitud": magnitud, "inicio": inicio,
                                 "fin_mas_1": fin + timedelta(days=1)})
    for f in filas:
        if f.get("media_periodo") is not None:
            f["media_periodo"] = round(float(f["media_periodo"]), 1)
    return {"descripcion": (f"Estaciones ordenadas por media de {args['contaminante']} "
                            f"del {inicio.isoformat()} al {fin.isoformat()} (µg/m³)"),
            "filas": filas}


def _info_estaciones(engine: Engine, args: dict) -> dict:
    filtros, params = [], {}
    if args.get("estacion") is not None:
        filtros.append("AND codigo_corto = :estacion")
        params["estacion"] = int(args["estacion"])
    if args.get("distrito"):
        # Coincidencia parcial: "Vallecas" debe encontrar "Puente de Vallecas" y
        # "Villa de Vallecas" (el LLM no conoce los nombres compuestos exactos).
        filtros.append("AND LOWER(distrito) LIKE '%' || LOWER(:distrito) || '%'")
        params["distrito"] = str(args["distrito"]).strip()
    sql = (
        "SELECT codigo_corto, nombre, distrito, tipo, direccion, "
        "       mide_no2, mide_pm10, mide_pm25, mide_o3 "
        "FROM estaciones WHERE 1=1 "
        f"{' '.join(filtros)} ORDER BY codigo_corto LIMIT {LIMITE_FILAS}"
    )
    return {"descripcion": "Catálogo de estaciones de control (mide_no2 implica NO, NO2 y NOx)",
            "filas": _filas(engine, sql, params)}


_CONSULTAS = {
    "ultimos_niveles": _ultimos_niveles,
    "serie_bloques": _serie_bloques,
    "anomalias": _anomalias,
    "comparar_estaciones": _comparar_estaciones,
    "info_estaciones": _info_estaciones,
}

# Qué tabla citar como fuente según la consulta ejecutada
TABLA_POR_CONSULTA = {nombre: ("estaciones" if nombre == "info_estaciones" else "resumen_datos_ml")
                      for nombre in _CONSULTAS}


def query_sql(engine: Engine, consulta: str = "", **args) -> dict:
    """Ejecuta una consulta del catálogo. Errores como {'error': ...}, nunca excepción."""
    try:
        if consulta not in _CONSULTAS:
            return {"error": (f"Consulta desconocida: {consulta!r}. "
                              f"Debe ser una de {sorted(_CONSULTAS)}")}
        resultado = _CONSULTAS[consulta](engine, args)
        if not resultado["filas"]:
            resultado["mensaje"] = "La consulta no devolvió filas para esos filtros"
        elif len(resultado["filas"]) == LIMITE_FILAS:
            # Truncamiento explícito: el LLM debe saber que no lo tiene todo
            resultado["mensaje"] = (
                f"Resultado truncado a {LIMITE_FILAS} filas: filtra por estación "
                "o bloque, o acorta el rango de fechas"
            )
        return resultado
    except ValueError as exc:
        return {"error": str(exc)}
    except Exception as exc:  # error de BBDD u otro imprevisto: que el LLM lo sepa
        return {"error": f"La consulta falló: {exc}"}
