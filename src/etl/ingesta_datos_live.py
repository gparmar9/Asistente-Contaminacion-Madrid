# Librerias
import pandas as pd
import numpy as np
import requests
from sqlalchemy import create_engine, text

from limpiar_datos import limpiar_datos_live

url = "https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_tiemporeal.json?pageSize=5000"
archivo_estaciones = '././data/raw/estaciones-de-control.csv'

DB_URL = "postgresql://postgres:guillermo9@localhost:5432/postgres"

def insertar_sin_duplicados(df, engine, tabla="mediciones_live"):
    # Crear tabla si no existe, con constraint única sobre estacion+magnitud+fecha
    with engine.connect() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {tabla} (
                provincia       INTEGER,
                municipio       INTEGER,
                estacion        INTEGER,
                magnitud        INTEGER,
                contaminante    TEXT,
                punto_muestreo  TEXT,
                nombre_estacion TEXT,
                ano             INTEGER,
                mes             INTEGER,
                dia             INTEGER,
                hora            INTEGER,
                valor           FLOAT,
                validacion      TEXT,
                fecha           TIMESTAMP,
                dia_semana      INTEGER,
                es_fin_semana   BOOLEAN,
                CONSTRAINT uq_estacion_magnitud_fecha UNIQUE (estacion, magnitud, fecha)
            )
        """))
        conn.commit()

    # Insertar fila a fila ignorando duplicados
    insert_sql = text(f"""
        INSERT INTO {tabla}
            (provincia, municipio, estacion, magnitud, contaminante, punto_muestreo,
             nombre_estacion, ano, mes, dia, hora, valor, validacion, fecha, dia_semana, es_fin_semana)
        VALUES
            (:provincia, :municipio, :estacion, :magnitud, :contaminante, :punto_muestreo,
             :nombre_estacion, :ano, :mes, :dia, :hora, :valor, :validacion, :fecha, :dia_semana, :es_fin_semana)
        ON CONFLICT (estacion, magnitud, fecha) DO NOTHING
    """)

    with engine.connect() as conn:
        conn.execute(insert_sql, df.to_dict(orient="records"))
        conn.commit()

# Hacer la petición GET a la URL
response = requests.get(url)
response.raise_for_status()
print('Petición realizada con éxito. Código de estado:', response.status_code)

if response.status_code == 200:
    data = response.json()
    df = pd.DataFrame(data['records'])

    df_limpio = limpiar_datos_live(df, archivo_estaciones)

    engine = create_engine(DB_URL)
    insertar_sin_duplicados(df_limpio, engine)
    print(f'Insertados {len(df_limpio)} registros (duplicados ignorados)')
else:
    print('Error: No se pudo obtener la información. Código de estado:', response.status_code)