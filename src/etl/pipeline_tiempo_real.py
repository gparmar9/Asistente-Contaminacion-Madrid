"""Pipeline de tiempo real: API -> Postgres -> inferencia -> anomalías.

Encadena todo el flujo de la Fase 1 sobre los datos que llegan de la API de Madrid:

    1. Baja las mediciones y las limpia a formato largo (limpiar_datos_live).
    2. Guarda el horario crudo en la tabla `calidad_aire_horas_live` (idempotente).
    3. Agrega a bloques, calcula features y aplica el Isolation Forest (nb03).
    4. Escribe/actualiza las filas resultantes en `resumen_datos_ml` (upsert).

El baseline histórico se materializa una vez en la tabla `baseline_historico`.

Uso:
    python src/etl/pipeline_tiempo_real.py          # baja de la API y procesa
"""
import os
import sys
import time
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from limpiar_datos import limpiar_datos_live
from features_bloques import CLAVES_BASE
from inferencia import cargar_bundle, detectar

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[2]
RUTA_MODELOS    = BASE_DIR / "models" / "isolation_forest.joblib"
RUTA_ESTACIONES = BASE_DIR / "data" / "raw" / "estaciones-de-control.csv"
URL_API = "https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_tiemporeal.json?pageSize=5000"

TABLA_HORARIO = "calidad_aire_horas_live"
TABLA_RESUMEN = "resumen_datos_ml"
TABLA_BASELINE = "baseline_historico"

# Columnas de la tabla resumen_datos_ml, en orden (minúsculas)
COLS_RESUMEN = [
    "estacion", "magnitud", "contaminante", "fecha", "bloque", "hora_inicio", "hora_fin",
    "ano", "mes", "dia_semana", "es_fin_semana", "n_horas", "cobertura", "media", "maximo",
    "hora_maximo", "minimo", "hora_minimo", "std", "rango", "media_esperada", "std_esperada",
    "desviacion", "z_score", "expected_value", "anomaly_score", "is_anomaly",
]
CLAVES_RESUMEN = ["estacion", "magnitud", "fecha", "bloque"]

DDL_HORARIO = f"""
CREATE TABLE IF NOT EXISTS {TABLA_HORARIO} (
    provincia INTEGER, municipio INTEGER, estacion INTEGER, magnitud INTEGER,
    contaminante TEXT, punto_muestreo TEXT, nombre_estacion TEXT,
    ano INTEGER, mes INTEGER, dia INTEGER, hora INTEGER, valor FLOAT,
    validacion TEXT, fecha TIMESTAMP, dia_semana INTEGER, es_fin_semana BOOLEAN,
    CONSTRAINT uq_estacion_magnitud_fecha UNIQUE (estacion, magnitud, fecha)
)
"""


# --------------------------------------------------------------------------- infra

def asegurar_esquema(engine):
    """Crea la tabla horaria y el índice único de resumen si no existen."""
    with engine.begin() as conn:
        conn.execute(text(DDL_HORARIO))
        conn.execute(text(
            f"CREATE UNIQUE INDEX IF NOT EXISTS uq_{TABLA_RESUMEN} "
            f"ON {TABLA_RESUMEN} (estacion, magnitud, fecha, bloque)"
        ))


def asegurar_baseline(engine) -> pd.DataFrame:
    """Materializa baseline_historico desde resumen_datos_ml (una vez) y lo devuelve."""
    with engine.begin() as conn:
        existe = conn.execute(text(f"SELECT to_regclass('{TABLA_BASELINE}')")).scalar()
        if existe is None:
            conn.execute(text(
                f"CREATE TABLE {TABLA_BASELINE} AS "
                f"SELECT DISTINCT estacion, magnitud, bloque, mes, media_esperada, std_esperada "
                f"FROM {TABLA_RESUMEN}"
            ))
    base = pd.read_sql(f"SELECT * FROM {TABLA_BASELINE}", engine)
    base.columns = [c.upper() for c in base.columns]   # a mayúsculas para features_bloques
    return base


# --------------------------------------------------------------------------- upserts

def upsert_horario(df_limpio: pd.DataFrame, engine) -> int:
    """Inserta el horario crudo ignorando duplicados (ON CONFLICT DO NOTHING)."""
    df_limpio.to_sql("_stg_horario", engine, if_exists="replace", index=False)
    cols = ", ".join(df_limpio.columns)
    with engine.begin() as conn:
        res = conn.execute(text(
            f"INSERT INTO {TABLA_HORARIO} ({cols}) SELECT {cols} FROM _stg_horario "
            f"ON CONFLICT (estacion, magnitud, fecha) DO NOTHING"
        ))
        conn.execute(text("DROP TABLE IF EXISTS _stg_horario"))
    return res.rowcount


def upsert_resumen(df_resumen: pd.DataFrame, engine) -> int:
    """Inserta/actualiza las filas de bloque con la salida del modelo (upsert)."""
    df_resumen = df_resumen.copy()
    df_resumen["fecha"] = pd.to_datetime(df_resumen["fecha"])   # date -> timestamp (medianoche)
    df_resumen[COLS_RESUMEN].to_sql("_stg_resumen", engine, if_exists="replace", index=False)

    cols = ", ".join(COLS_RESUMEN)
    set_clause = ", ".join(f"{c}=EXCLUDED.{c}" for c in COLS_RESUMEN if c not in CLAVES_RESUMEN)
    with engine.begin() as conn:
        res = conn.execute(text(
            f"INSERT INTO {TABLA_RESUMEN} ({cols}) SELECT {cols} FROM _stg_resumen "
            f"ON CONFLICT (estacion, magnitud, fecha, bloque) DO UPDATE SET {set_clause}"
        ))
        conn.execute(text("DROP TABLE IF EXISTS _stg_resumen"))
    return res.rowcount


# --------------------------------------------------------------------------- pipeline

def _a_horario_valido(df_limpio: pd.DataFrame) -> pd.DataFrame:
    """Formato largo en mayúsculas y solo válidas, tal como espera features_bloques.a_bloques."""
    v = df_limpio[df_limpio["validacion"] == "V"].copy()
    return pd.DataFrame({
        "ESTACION": v["estacion"].astype(int),
        "MAGNITUD": v["magnitud"].astype(int),
        "FECHA":    pd.to_datetime(v["fecha"]).dt.date,
        "HORA":     v["hora"].astype(int),
        "VALOR":    v["valor"].astype(float),
    })


def procesar(df_limpio: pd.DataFrame, engine, bundle) -> dict:
    """Guarda el horario, infiere anomalías por bloque y hace upsert del resumen."""
    asegurar_esquema(engine)
    baseline = asegurar_baseline(engine)

    n_horas = upsert_horario(df_limpio, engine)

    largo = _a_horario_valido(df_limpio)
    resumen = detectar(largo, baseline, bundle)

    # a minúsculas para casar con el esquema de la tabla
    resumen_bd = resumen.rename(columns={c: c.lower() for c in resumen.columns})
    n_bloques = upsert_resumen(resumen_bd, engine)
    n_anom = int(resumen["IS_ANOMALY"].sum())

    return {"horas_nuevas": n_horas, "bloques_upsert": n_bloques, "anomalias": n_anom}


def bajar_de_api(max_intentos: int = 3) -> pd.DataFrame:
    """Baja las mediciones de la API con reintentos; devuelve el DataFrame ancho."""
    for intento in range(1, max_intentos + 1):
        resp = requests.get(URL_API)
        resp.raise_for_status()
        data = resp.json()
        if "records" in data:
            return pd.DataFrame(data["records"])
        print(f"Intento {intento}/{max_intentos}: respuesta sin 'records'.")
        if intento < max_intentos:
            time.sleep(10)
    return pd.DataFrame()


def main():
    engine = create_engine(os.environ["DATABASE_URL"])
    bundle = cargar_bundle(RUTA_MODELOS)

    df_ancho = bajar_de_api()
    if df_ancho.empty:
        print("La API no devolvió datos válidos. Se omite esta ejecución.")
        return

    df_limpio = limpiar_datos_live(df_ancho, str(RUTA_ESTACIONES))
    res = procesar(df_limpio, engine, bundle)
    print(f"Horario: {res['horas_nuevas']} filas nuevas | "
          f"Resumen: {res['bloques_upsert']} bloques upsert "
          f"({res['anomalias']} anomalías detectadas).")


if __name__ == "__main__":
    main()
