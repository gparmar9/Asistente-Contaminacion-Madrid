"""Consultas de solo lectura sobre la tabla de dimensión `estaciones`."""
from sqlalchemy import text
from sqlalchemy.engine import Engine

from api_usuario.shared.constantes import CONTAMINANTES

# Columna `mide_*` del catálogo -> contaminantes objetivo que implica.
# Los analizadores de NO2 miden NO, NO2 y NOx a la vez (las series de
# `resumen_datos_ml` lo confirman), y SO2/CO/BTX quedan fuera del alcance del
# proyecto: así `contaminantes_medidos` coincide con lo consultable en /series.
_MIDE_A_CONTAMINANTES = {
    "mide_no2": ("NO", "NO2", "NOx"),
    "mide_o3": ("O3",),
    "mide_pm10": ("PM10",),
    "mide_pm25": ("PM2.5",),
}

_COLUMNAS = (
    "codigo_corto, nombre, direccion, tipo, distrito, latitud, longitud, altitud, "
    + ", ".join(_MIDE_A_CONTAMINANTES)
)


def _a_dict(fila) -> dict:
    """Aplana las columnas booleanas `mide_*` a la lista de contaminantes consultables."""
    d = dict(fila)
    medidos = set()
    for col, contaminantes in _MIDE_A_CONTAMINANTES.items():
        if d.pop(col, False):
            medidos.update(contaminantes)
    d["contaminantes_medidos"] = [c for c in CONTAMINANTES if c in medidos]
    return d


def listar_estaciones(engine: Engine) -> list[dict]:
    sql = text(f"SELECT {_COLUMNAS} FROM estaciones ORDER BY codigo_corto")
    with engine.connect() as conn:
        filas = conn.execute(sql).mappings().all()
    return [_a_dict(f) for f in filas]


def obtener_estacion(engine: Engine, codigo_corto: int) -> dict | None:
    sql = text(f"SELECT {_COLUMNAS} FROM estaciones WHERE codigo_corto = :codigo")
    with engine.connect() as conn:
        fila = conn.execute(sql, {"codigo": codigo_corto}).mappings().first()
    return _a_dict(fila) if fila is not None else None
