"""Carga la tabla de dimensión `estaciones` en PostgreSQL.

Toma el catálogo `data/raw/estaciones-de-control.csv` (24 estaciones), lo limpia
y lo enriquece con el **distrito** de Madrid al que pertenece cada estación.

Es la tabla que da contexto geográfico a las mediciones: convierte el código de
estación (p. ej. 8) en "Escuelas Aguirre, distrito Salamanca, urbana tráfico".
El chatbot la usará con JOINs sobre `resumen_datos_ml` para responder preguntas
por zona/distrito.

Uso (con el contenedor de Postgres levantado y DATABASE_URL en .env):
    python src/etl/cargar_estaciones.py
"""
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[2]
RUTA_CSV = BASE_DIR / "data" / "raw" / "estaciones-de-control.csv"
TABLA = "estaciones"

# Distrito de Madrid por código de estación (CODIGO_CORTO).
# Mapeo curado a partir de la dirección de cada estación; conviene contrastarlo
# con el catálogo oficial del Ayuntamiento (son 24 filas). Los casos de borde
# entre distritos están marcados con "# revisar".
DISTRITOS = {
    4:  "Centro",
    8:  "Salamanca",
    11: "Chamartín",
    16: "Ciudad Lineal",
    17: "Villaverde",
    18: "Carabanchel",
    24: "Moncloa-Aravaca",
    27: "Barajas",
    35: "Centro",
    36: "Moratalaz",
    38: "Tetuán",              # revisar (Cuatro Caminos, borde Tetuán/Chamberí)
    39: "Fuencarral-El Pardo",
    40: "Puente de Vallecas",
    47: "Arganzuela",
    48: "Chamartín",           # revisar (Castellana, borde Chamartín/Chamberí)
    49: "Retiro",
    50: "Chamartín",
    54: "Villa de Vallecas",
    55: "Barajas",
    56: "Carabanchel",         # revisar (Plaza Elíptica, borde Carabanchel/Usera)
    57: "Hortaleza",
    58: "Fuencarral-El Pardo",
    59: "Barajas",
    60: "Fuencarral-El Pardo",  # revisar (Tres Olivos, borde Fuencarral/Chamartín)
}


def _preparar(ruta_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(ruta_csv, sep=";")

    out = pd.DataFrame({
        "codigo_corto": df["CODIGO_CORTO"].astype(int),
        "codigo": df["CODIGO"].astype("int64"),
        "nombre": df["ESTACION"].str.strip(),
        "direccion": df["DIRECCION"].str.strip(),
        "tipo": df["NOM_TIPO"].str.strip(),
        "longitud": pd.to_numeric(df["LONGITUD"], errors="coerce"),
        "latitud": pd.to_numeric(df["LATITUD"], errors="coerce"),
        "altitud": pd.to_numeric(df["ALTITUD"], errors="coerce").astype("Int64"),
    })
    out["distrito"] = out["codigo_corto"].map(DISTRITOS)

    # Qué contaminantes mide cada estación (columnas marcadas con 'X' en el CSV)
    marcadas = {"mide_no2": "NO2", "mide_pm10": "PM10", "mide_pm25": "PM2_5",
                "mide_o3": "O3", "mide_so2": "SO2", "mide_co": "CO", "mide_btx": "BTX"}
    for col_out, col_in in marcadas.items():
        out[col_out] = df[col_in].astype(str).str.upper().str.strip().eq("X")

    return out


def cargar(ruta_csv: Path = RUTA_CSV, tabla: str = TABLA) -> pd.DataFrame:
    df = _preparar(ruta_csv)

    sin_distrito = df[df["distrito"].isna()]["codigo_corto"].tolist()
    if sin_distrito:
        print(f"AVISO: estaciones sin distrito asignado: {sin_distrito}")

    engine = create_engine(os.environ["DATABASE_URL"])
    df.to_sql(tabla, engine, if_exists="replace", index=False)
    with engine.begin() as conn:
        conn.execute(text(f"ALTER TABLE {tabla} ADD PRIMARY KEY (codigo_corto)"))

    return df


if __name__ == "__main__":
    df = cargar()
    print(f"Cargadas {len(df)} estaciones en la tabla '{TABLA}'.")
    print(df[["codigo_corto", "nombre", "tipo", "distrito"]].to_string(index=False))
