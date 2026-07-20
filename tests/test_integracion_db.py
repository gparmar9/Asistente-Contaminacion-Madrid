"""Tests de integración contra un PostgreSQL real (efímero).

Solo se ejecutan si RUN_DB_TESTS=1 (lo define el job de CI con su Postgres de
servicio). En local no corren por defecto, para no tocar tu base de datos: la
fixture BORRA y RECREA tablas.

Ejecución manual segura (contra un contenedor de usar y tirar):
    RUN_DB_TESTS=1 DATABASE_URL=postgresql://postgres:postgres@localhost:5433/postgres \
        pytest tests/test_integracion_db.py -v
"""
import sys
import os

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'etl'))
from features_bloques import FEATURES_MODELO
import pipeline_tiempo_real as ptr

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_DB_TESTS") != "1",
    reason="Test de integración: define RUN_DB_TESTS=1 y DATABASE_URL (Postgres de usar y tirar).",
)

_TABLAS = ["resumen_datos_ml", "calidad_aire_horas_live", "baseline_historico",
           "_stg_horario", "_stg_resumen"]


def _seed_resumen() -> pd.DataFrame:
    """Histórico mínimo para que se pueda construir el baseline (mes de mayo)."""
    filas = []
    for bloque, (ini, fin) in [("madrugada", (0, 6)), ("manana", (7, 12))]:
        filas.append(dict(
            estacion=4, magnitud=8, contaminante="NO2", fecha=pd.Timestamp("2024-05-15"),
            bloque=bloque, hora_inicio=ini, hora_fin=fin, ano=2024, mes=5, dia_semana=2,
            es_fin_semana=False, n_horas=fin - ini + 1, cobertura=1.0, media=20.0,
            maximo=25.0, hora_maximo=ini, minimo=15.0, hora_minimo=fin, std=3.0, rango=10.0,
            media_esperada=20.0, std_esperada=5.0, desviacion=0.0, z_score=0.0,
            expected_value=20.0, anomaly_score=0.3, is_anomaly=False))
    return pd.DataFrame(filas)


def _bundle_minimo() -> dict:
    rng = np.random.default_rng(0)
    X = rng.normal(0, 1, size=(300, len(FEATURES_MODELO)))
    sc = StandardScaler().fit(X)
    m = IsolationForest(contamination=0.05, random_state=0).fit(sc.transform(X))
    return {"modelos": {"NO2": m}, "scalers": {"NO2": sc},
            "features": FEATURES_MODELO, "contaminacion": 0.05}


def _horario_limpio() -> pd.DataFrame:
    """Lote horario 'limpio' (esquema de limpiar_datos_live): estación 4, NO2, 1-may-2026."""
    filas = [dict(
        provincia=28, municipio=79, estacion=4, magnitud=8, contaminante="NO2",
        punto_muestreo="28079004_8_8", nombre_estacion="Test", ano=2026, mes=5, dia=1,
        hora=h, valor=float(20 + h), validacion="V", fecha=pd.Timestamp(2026, 5, 1, h),
        dia_semana=4, es_fin_semana=False) for h in range(12)]
    return pd.DataFrame(filas)


@pytest.fixture()
def engine():
    eng = create_engine(os.environ["DATABASE_URL"])
    with eng.begin() as c:
        for t in _TABLAS:
            c.execute(text(f"DROP TABLE IF EXISTS {t} CASCADE"))
    _seed_resumen().to_sql("resumen_datos_ml", eng, if_exists="replace", index=False)
    yield eng
    with eng.begin() as c:
        for t in _TABLAS:
            c.execute(text(f"DROP TABLE IF EXISTS {t} CASCADE"))


def test_upsert_horario_idempotente(engine):
    ptr.asegurar_esquema(engine)
    df = _horario_limpio()

    n1 = ptr.upsert_horario(df, engine)
    n2 = ptr.upsert_horario(df, engine)   # mismo lote otra vez

    assert n1 == len(df)      # primera vez: entran todas
    assert n2 == 0            # segunda vez: ON CONFLICT DO NOTHING
    with engine.begin() as c:
        total = c.execute(text("SELECT count(*) FROM calidad_aire_horas_live")).scalar()
    assert total == len(df)


def test_procesar_no_duplica_y_detecta(engine):
    bundle = _bundle_minimo()
    df = _horario_limpio()

    r1 = ptr.procesar(df, engine, bundle)
    with engine.begin() as c:
        tras1 = c.execute(text("SELECT count(*) FROM resumen_datos_ml")).scalar()

    r2 = ptr.procesar(df, engine, bundle)   # segunda pasada del MISMO lote
    with engine.begin() as c:
        tras2 = c.execute(text("SELECT count(*) FROM resumen_datos_ml")).scalar()
        dup = c.execute(text(
            "SELECT count(*) FROM (SELECT 1 FROM resumen_datos_ml "
            "GROUP BY estacion, magnitud, fecha, bloque HAVING count(*) > 1) t"
        )).scalar()
        base = c.execute(text("SELECT count(*) FROM baseline_historico")).scalar()

    # El lote genera 2 bloques nuevos (madrugada + mañana) sobre las 2 filas sembradas
    assert r1["bloques_upsert"] == 2
    assert tras1 == 4
    assert tras2 == 4            # la 2ª pasada actualiza en sitio, no inserta
    assert dup == 0             # sin duplicados por clave
    assert r2["horas_nuevas"] == 0
    assert base == 2            # baseline materializado desde el histórico sembrado
