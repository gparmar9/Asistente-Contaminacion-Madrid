"""Fixtures de los tests de ApiUsuario.

No hace falta PostgreSQL: se usa SQLite en memoria con las mismas tablas
(`estaciones`, `resumen_datos_ml`) y la dependencia `get_engine` sobreescrita.
El SQL de los repositorios es portable entre ambos motores.
"""
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

# El paquete api_usuario vive bajo src/ (mismo patrón que los tests del ETL)
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from fastapi.testclient import TestClient  # noqa: E402

from api_usuario.config.settings import Settings, get_settings  # noqa: E402
from api_usuario.data_access.sql_connection import get_engine  # noqa: E402
from api_usuario.main import app  # noqa: E402

_DDL_ESTACIONES = """
CREATE TABLE estaciones (
    codigo_corto INTEGER PRIMARY KEY, codigo BIGINT, nombre TEXT, direccion TEXT,
    tipo TEXT, longitud FLOAT, latitud FLOAT, altitud INTEGER, distrito TEXT,
    mide_no2 BOOLEAN, mide_pm10 BOOLEAN, mide_pm25 BOOLEAN, mide_o3 BOOLEAN,
    mide_so2 BOOLEAN, mide_co BOOLEAN, mide_btx BOOLEAN
)
"""

_DDL_RESUMEN = """
CREATE TABLE resumen_datos_ml (
    estacion INTEGER, magnitud INTEGER, contaminante TEXT, fecha TIMESTAMP,
    bloque TEXT, hora_inicio INTEGER, hora_fin INTEGER, ano INTEGER, mes INTEGER,
    dia_semana INTEGER, es_fin_semana BOOLEAN, n_horas INTEGER, cobertura FLOAT,
    media FLOAT, maximo FLOAT, hora_maximo INTEGER, minimo FLOAT, hora_minimo INTEGER,
    std FLOAT, rango FLOAT, media_esperada FLOAT, std_esperada FLOAT, desviacion FLOAT,
    z_score FLOAT, expected_value FLOAT, anomaly_score FLOAT, is_anomaly BOOLEAN
)
"""

_ESTACIONES = [
    # Escuelas Aguirre: mide de todo
    dict(codigo_corto=8, codigo=28079008, nombre="Escuelas Aguirre",
         direccion="C/ Alcalá, 63", tipo="Urbana Tráfico", longitud=-3.6823,
         latitud=40.4215, altitud=672, distrito="Salamanca",
         mide_no2=1, mide_pm10=1, mide_pm25=1, mide_o3=1, mide_so2=1, mide_co=1, mide_btx=1),
    # Retiro: solo NO2 y O3
    dict(codigo_corto=49, codigo=28079049, nombre="Parque del Retiro",
         direccion="Paseo Venezuela, 2", tipo="Urbana Fondo", longitud=-3.6824,
         latitud=40.4144, altitud=662, distrito="Retiro",
         mide_no2=1, mide_pm10=0, mide_pm25=0, mide_o3=1, mide_so2=0, mide_co=0, mide_btx=0),
]

# Bloques de prueba: estación 49 con NO2 (20 y 21 de sept.) y un O3;
# estación 8 con un NO2, para comprobar los filtros.
_RESUMEN = [
    dict(estacion=49, magnitud=8, contaminante="NO2", fecha="2026-09-20 00:00:00",
         bloque="madrugada", hora_inicio=0, hora_fin=6, n_horas=7, cobertura=1.0,
         media=12.5, maximo=20.0, minimo=5.0, z_score=-0.3, anomaly_score=0.02, is_anomaly=0),
    dict(estacion=49, magnitud=8, contaminante="NO2", fecha="2026-09-20 00:00:00",
         bloque="manana", hora_inicio=7, hora_fin=12, n_horas=6, cobertura=1.0,
         media=68.0, maximo=95.0, minimo=40.0, z_score=3.4, anomaly_score=0.31, is_anomaly=1),
    dict(estacion=49, magnitud=8, contaminante="NO2", fecha="2026-09-21 00:00:00",
         bloque="madrugada", hora_inicio=0, hora_fin=6, n_horas=7, cobertura=1.0,
         media=14.0, maximo=22.0, minimo=6.0, z_score=-0.1, anomaly_score=0.03, is_anomaly=0),
    dict(estacion=49, magnitud=14, contaminante="O3", fecha="2026-09-20 00:00:00",
         bloque="tarde", hora_inicio=13, hora_fin=19, n_horas=7, cobertura=1.0,
         media=80.0, maximo=110.0, minimo=55.0, z_score=0.9, anomaly_score=0.05, is_anomaly=0),
    dict(estacion=8, magnitud=8, contaminante="NO2", fecha="2026-09-20 00:00:00",
         bloque="madrugada", hora_inicio=0, hora_fin=6, n_horas=7, cobertura=1.0,
         media=9.0, maximo=15.0, minimo=4.0, z_score=-0.5, anomaly_score=0.01, is_anomaly=0),
]

_INSERT_ESTACION = text(
    "INSERT INTO estaciones (codigo_corto, codigo, nombre, direccion, tipo, longitud, "
    "latitud, altitud, distrito, mide_no2, mide_pm10, mide_pm25, mide_o3, mide_so2, "
    "mide_co, mide_btx) VALUES (:codigo_corto, :codigo, :nombre, :direccion, :tipo, "
    ":longitud, :latitud, :altitud, :distrito, :mide_no2, :mide_pm10, :mide_pm25, "
    ":mide_o3, :mide_so2, :mide_co, :mide_btx)"
)

_INSERT_RESUMEN = text(
    "INSERT INTO resumen_datos_ml (estacion, magnitud, contaminante, fecha, bloque, "
    "hora_inicio, hora_fin, n_horas, cobertura, media, maximo, minimo, z_score, "
    "anomaly_score, is_anomaly) VALUES (:estacion, :magnitud, :contaminante, :fecha, "
    ":bloque, :hora_inicio, :hora_fin, :n_horas, :cobertura, :media, :maximo, :minimo, "
    ":z_score, :anomaly_score, :is_anomaly)"
)


@pytest.fixture()
def engine():
    eng = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    with eng.begin() as conn:
        conn.execute(text(_DDL_ESTACIONES))
        conn.execute(text(_DDL_RESUMEN))
        for estacion in _ESTACIONES:
            conn.execute(_INSERT_ESTACION, estacion)
        for fila in _RESUMEN:
            conn.execute(_INSERT_RESUMEN, fila)
    return eng


@pytest.fixture()
def cliente(engine):
    app.dependency_overrides[get_engine] = lambda: engine
    yield TestClient(app)
    app.dependency_overrides.clear()


def _settings_de_prueba(orchestrator_url: str = "", timeout: float = 0.2,
                        agente_url: str = "") -> Settings:
    """Settings explícitos para los tests del chat (sin leer variables de entorno)."""
    return Settings(
        app_name="ApiUsuario", app_env="test", api_host="127.0.0.1", api_port=8000,
        api_version="test", database_url="", orchestrator_url=orchestrator_url,
        orchestrator_timeout_s=timeout, agente_url=agente_url, agente_timeout_s=timeout,
    )


@pytest.fixture()
def con_settings():
    """Factoría que instala unos Settings de prueba como dependencia del chat.

    La limpieza la hace la fixture `cliente` (dependency_overrides.clear()).
    """
    def _instalar(**kwargs):
        app.dependency_overrides[get_settings] = lambda: _settings_de_prueba(**kwargs)

    return _instalar
