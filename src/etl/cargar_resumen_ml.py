"""Carga la tabla ResumenDatosML (salida del nb03) en PostgreSQL.

Lee el parquet `data/processed/resumen_datos_ml.parquet` y lo vuelca a la tabla
`resumen_datos_ml`. Es la tabla que consultará el chatbot por SQL.

Como es una tabla derivada (se regenera desde los notebooks), la recreamos entera
en cada carga y usamos COPY (carga masiva nativa de PostgreSQL) para que sea
rápido incluso con más de un millón de filas.

Uso (con el contenedor de Postgres levantado y DATABASE_URL en .env):
    python src/etl/cargar_resumen_ml.py
"""
import io
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[2]
RUTA_PARQUET = BASE_DIR / "data" / "processed" / "resumen_datos_ml.parquet"
TABLA = "resumen_datos_ml"


def cargar(ruta_parquet: Path = RUTA_PARQUET, tabla: str = TABLA) -> int:
    db_url = os.environ["DATABASE_URL"]

    df = pd.read_parquet(ruta_parquet)
    # PostgreSQL trabaja mejor con identificadores en minúscula (sin comillas)
    df.columns = [c.lower() for c in df.columns]

    engine = create_engine(db_url)

    # 1) Crear la tabla vacía con el esquema correcto (0 filas define columnas/tipos)
    df.head(0).to_sql(tabla, engine, if_exists="replace", index=False)

    # 2) Volcar los datos con COPY (mucho más rápido que INSERT fila a fila)
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)   # NaN -> campo vacío -> NULL en CSV
    buffer.seek(0)

    columnas = ", ".join(df.columns)
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            cur.copy_expert(
                f"COPY {tabla} ({columnas}) FROM STDIN WITH (FORMAT CSV)",
                buffer,
            )
        raw.commit()
    finally:
        raw.close()

    # 3) Índice para las consultas típicas del chatbot
    with engine.begin() as conn:
        conn.execute(text(
            f"CREATE INDEX IF NOT EXISTS idx_{tabla}_est_mag_fecha "
            f"ON {tabla} (estacion, magnitud, fecha)"
        ))

    return len(df)


if __name__ == "__main__":
    n = cargar()
    print(f"Cargadas {n:,} filas en la tabla '{TABLA}'.")
