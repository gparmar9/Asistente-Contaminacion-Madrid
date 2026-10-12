"""DAL de mediciones sobre SQLite en memoria con una tabla `mediciones_bloques` sembrada.

SQLite no prueba el dialecto de PostgreSQL ni los permisos: eso va en `test_integracion_pg.py`.
"""
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

from agente.datos.mediciones import ErrorBaseDatos, Mediciones

_DDL = """
CREATE TABLE mediciones_bloques (
    fecha DATE, estacion INTEGER, nombre_estacion TEXT, distrito TEXT, contaminante TEXT,
    bloque TEXT, media FLOAT, n_horas INTEGER
)
"""

# Retiro, NO2, 6 de octubre: madrugada (7 h) y mañana (6 h). Media ponderada del día:
# (10*7 + 36*6) / 13 = 22,0. `AVG(media)` daría 23,0.
_FILAS = [
    ("2026-10-06", 49, "Parque del Retiro", "Retiro", "NO2", "madrugada", 10.0, 7),
    ("2026-10-06", 49, "Parque del Retiro", "Retiro", "NO2", "manana", 36.0, 6),
    ("2026-10-05", 8, "Escuelas Aguirre", "Salamanca", "NO2", "madrugada", 30.0, 7),
]


class Reloj:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


@pytest.fixture()
def engine():
    eng = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    with eng.begin() as conn:
        conn.execute(text(_DDL))
        conn.execute(
            text("INSERT INTO mediciones_bloques VALUES (:f, :e, :n, :d, :c, :b, :m, :h)"),
            [dict(zip("fendcbmh", fila)) for fila in _FILAS],
        )
    return eng


def test_ejecutar_devuelve_columnas_y_filas_serializables(engine):
    resultado = Mediciones(engine).ejecutar(
        "SELECT fecha, nombre_estacion, SUM(media * n_horas) / SUM(n_horas) AS media "
        "FROM mediciones_bloques WHERE estacion = :estacion GROUP BY fecha, nombre_estacion",
        {"estacion": 49},
    )

    assert resultado.columnas == ("fecha", "nombre_estacion", "media")
    assert resultado.filas == (("2026-10-06", "Parque del Retiro", pytest.approx(22.0)),)
    assert resultado.truncado is False


def test_ejecutar_trunca_al_tope_y_lo_avisa(engine):
    resultado = Mediciones(engine, max_filas=60).ejecutar(
        "SELECT estacion FROM mediciones_bloques", max_filas=2)

    assert len(resultado.filas) == 2
    assert resultado.truncado is True


def test_error_de_la_base_de_datos_se_traduce(engine):
    with pytest.raises(ErrorBaseDatos, match="resumen_datos_ml"):
        Mediciones(engine).ejecutar("SELECT * FROM resumen_datos_ml")


def test_ultima_fecha_se_cachea_una_hora(engine):
    reloj = Reloj()
    mediciones = Mediciones(engine, reloj=reloj)
    assert mediciones.ultima_fecha().isoformat() == "2026-10-06"

    with engine.begin() as conn:
        conn.execute(text("INSERT INTO mediciones_bloques (fecha) VALUES ('2026-10-07')"))
    reloj.t = 3599
    assert mediciones.ultima_fecha().isoformat() == "2026-10-06"
    reloj.t = 3600
    assert mediciones.ultima_fecha().isoformat() == "2026-10-07"
