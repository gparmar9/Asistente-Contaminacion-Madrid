"""Adaptador de AWS Lambda para el pipeline de tiempo real.

No duplica lógica: importa `pipeline_tiempo_real` y solo resuelve de dónde salen
en la nube las tres cosas que en local vienen del disco y del `.env`:

    - el modelo entrenado  -> S3 (se cachea en /tmp)
    - la cadena de conexión -> SSM Parameter Store (SecureString)
    - el catálogo de estaciones -> dentro de la propia imagen

Todo lo que está fuera de `handler()` se ejecuta UNA vez por contenedor (arranque
en frío), no en cada invocación: Lambda reutiliza el contenedor mientras esté
caliente, así que el modelo y la conexión se pagan una sola vez.
"""
import os
from pathlib import Path

import boto3

BUCKET    = os.environ["S3_BUCKET"]
MODEL_KEY = os.environ.get("MODEL_KEY", "models/isolation_forest.joblib")
SSM_PARAM = os.environ.get("SSM_PARAM", "/jupiter/database_url")
MODELO    = Path("/tmp/isolation_forest.joblib")   # único directorio escribible en Lambda

# --- Arranque en frío -------------------------------------------------------
os.environ["DATABASE_URL"] = boto3.client("ssm").get_parameter(
    Name=SSM_PARAM, WithDecryption=True
)["Parameter"]["Value"]

if not MODELO.exists():
    boto3.client("s3").download_file(BUCKET, MODEL_KEY, str(MODELO))

import pipeline_tiempo_real as pipe          # noqa: E402  (necesita DATABASE_URL ya resuelta)
from sqlalchemy import create_engine          # noqa: E402

# En local estas rutas se calculan desde la raíz del repositorio, que en la
# imagen no existe: las apuntamos a donde están de verdad.
pipe.RUTA_ESTACIONES = Path(os.environ["LAMBDA_TASK_ROOT"]) / "estaciones-de-control.csv"
pipe.RUTA_MODELOS    = MODELO

ENGINE = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)
BUNDLE = pipe.cargar_bundle(MODELO)


def handler(event, context):
    """Una ejecución del pipeline: API -> Postgres -> inferencia -> upsert."""
    df_ancho = pipe.bajar_de_api()
    if df_ancho.empty:
        print("La API no devolvió datos válidos. Se omite esta ejecución.")
        return {"ok": False, "motivo": "la API no devolvió registros"}

    df_limpio = pipe.limpiar_datos_live(df_ancho, str(pipe.RUTA_ESTACIONES))
    res = pipe.procesar(df_limpio, ENGINE, BUNDLE)

    print(f"Horario: {res['horas_nuevas']} filas nuevas | "
          f"Resumen: {res['bloques_upsert']} bloques upsert "
          f"({res['anomalias']} anomalías detectadas).")
    return {"ok": True, **res}
