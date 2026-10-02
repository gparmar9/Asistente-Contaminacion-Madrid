"""Consultas de solo lectura sobre `resumen_datos_ml` (bloques + anomalías)."""
from datetime import date, datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Engine


def _a_fecha(valor) -> date:
    """Normaliza `fecha` (timestamp en PostgreSQL, texto en SQLite) a `date`."""
    if isinstance(valor, datetime):
        return valor.date()
    if isinstance(valor, date):
        return valor
    return date.fromisoformat(str(valor)[:10])


def serie_bloques(
    engine: Engine,
    estacion: int,
    magnitud: int,
    desde: date,
    hasta: date,
    bloque: str | None = None,
) -> list[dict]:
    """Serie de bloques horarios de una estación y magnitud en [desde, hasta].

    Se filtra por `magnitud` (entero) y no por `contaminante` (texto) para que
    el predicado (estacion, magnitud, fecha) encaje con el índice existente de
    `resumen_datos_ml` (~1,3 M filas). `fecha` es un timestamp a medianoche,
    así que el rango se cierra con `fecha < hasta + 1 día` (funciona igual en
    PostgreSQL y en SQLite).
    """
    filtro_bloque = "AND bloque = :bloque" if bloque is not None else ""
    sql = text(
        "SELECT fecha, bloque, media, maximo, minimo, n_horas, cobertura, "
        "       z_score, anomaly_score, is_anomaly "
        "FROM resumen_datos_ml "
        "WHERE estacion = :estacion AND magnitud = :magnitud "
        f"  AND fecha >= :desde AND fecha < :hasta_fin {filtro_bloque} "
        "ORDER BY fecha, hora_inicio"
    )
    params = {
        "estacion": estacion,
        "magnitud": magnitud,
        "desde": desde,
        "hasta_fin": hasta + timedelta(days=1),
    }
    if bloque is not None:
        params["bloque"] = bloque

    with engine.connect() as conn:
        filas = conn.execute(sql, params).mappings().all()

    puntos = []
    for f in filas:
        d = dict(f)
        d["fecha"] = _a_fecha(d["fecha"])
        puntos.append(d)
    return puntos
