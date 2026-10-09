"""Carga la tabla ResumenDatosML (salida del nb03) en PostgreSQL.

Lee el parquet `data/processed/resumen_datos_ml.parquet` y lo vuelca a la tabla
`resumen_datos_ml`. Es la tabla que consultará el chatbot por SQL.

Como es una tabla derivada (se regenera desde los notebooks), la recargamos entera
en cada carga y usamos COPY (carga masiva nativa de PostgreSQL) para que sea
rápido incluso con más de un millón de filas.

Si la tabla ya existe se vacía (TRUNCATE) en vez de borrarla: la vista
`mediciones_bloques` del agente depende de ella, y un DROP fallaría (o, con
CASCADE, se llevaría la vista y los permisos del rol de lectura). Si cambian las
columnas del parquet, COPY falla: borrar la tabla a mano y volver a ejecutar los
scripts de `deploy/sql/`.

Uso (con el contenedor de Postgres levantado y DATABASE_URL en .env):
    python src/etl/cargar_resumen_ml.py
    python src/etl/cargar_resumen_ml.py --desde 2024-01-01   # solo desde esa fecha
"""
import argparse
import io
import os
from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[2]
RUTA_PARQUET = BASE_DIR / "data" / "processed" / "resumen_datos_ml.parquet"
TABLA = "resumen_datos_ml"


def cargar(ruta_parquet: Path = RUTA_PARQUET, tabla: str = TABLA,
           desde: date | None = None) -> int:
    db_url = os.environ["DATABASE_URL"]

    df = pd.read_parquet(ruta_parquet)
    # PostgreSQL trabaja mejor con identificadores en minúscula (sin comillas)
    df.columns = [c.lower() for c in df.columns]
    # Menos filas, carga más rápida y menos disco (el agente solo consulta 2 años)
    if desde is not None:
        df = df[df["fecha"] >= pd.Timestamp(desde)]

    engine = create_engine(db_url)

    # 1) Tabla vacía con el esquema correcto: se vacía si existe (conserva índices,
    #    vista y permisos); si no, 0 filas del DataFrame definen columnas/tipos
    if inspect(engine).has_table(tabla):
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE {tabla}"))
    else:
        df.head(0).to_sql(tabla, engine, index=False)

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

    # 3) Índices: uno para consultas del chatbot y uno ÚNICO para permitir el
    #    upsert de la inferencia en tiempo real (ON CONFLICT en pipeline_tiempo_real.py)
    with engine.begin() as conn:
        conn.execute(text(
            f"CREATE INDEX IF NOT EXISTS idx_{tabla}_est_mag_fecha "
            f"ON {tabla} (estacion, magnitud, fecha)"
        ))
        conn.execute(text(
            f"CREATE UNIQUE INDEX IF NOT EXISTS uq_{tabla} "
            f"ON {tabla} (estacion, magnitud, fecha, bloque)"
        ))

    return len(df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Carga resumen_datos_ml en PostgreSQL.")
    parser.add_argument("--desde", type=date.fromisoformat, default=None,
                        help="Carga solo las filas con fecha >= YYYY-MM-DD (por defecto, todas)")
    args = parser.parse_args()
    n = cargar(desde=args.desde)
    print(f"Cargadas {n:,} filas en la tabla '{TABLA}'.")
