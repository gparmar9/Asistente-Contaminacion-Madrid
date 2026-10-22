"""Inferencia del modelo de predicción t+1 (bundle `models/prediccion_bloques.joblib`,
entrenado en el nb04). Un regresor por contaminante; produce PREDICCION, la MEDIA
estimada del mismo bloque en t+horizonte. Espejo de `inferencia.py` del detector.
"""
import joblib
import numpy as np
import pandas as pd

from limpiar_datos import MAGNITUDES_OBJETIVO
from features_bloques import calcular_baseline
from features_prediccion import preparar_dataset


def cargar_bundle(ruta):
    """Carga el .joblib con {modelos, features, horizonte, cobertura_min, ...}."""
    return joblib.load(ruta)


def predecir(filas_feat: pd.DataFrame, bundle: dict) -> pd.DataFrame:
    """Aplica el modelo de cada contaminante y añade PREDICCION (µg/m³, >= 0).

    Las magnitudes sin modelo en el bundle quedan con PREDICCION NaN. Se pasa el
    DataFrame (no .to_numpy()): HistGradientBoosting acepta los NaN de los lags.
    """
    df = filas_feat.copy()
    features = list(bundle["features"])
    modelos = bundle["modelos"]

    df["PREDICCION"] = np.nan
    for magnitud, nombre in MAGNITUDES_OBJETIVO.items():
        if nombre not in modelos:
            continue
        mascara = df["MAGNITUD"] == magnitud
        if not mascara.any():
            continue
        df.loc[mascara, "PREDICCION"] = modelos[nombre].predict(df.loc[mascara, features])
    # Una concentración no puede ser negativa; el regresor no lo sabe.
    df["PREDICCION"] = df["PREDICCION"].clip(lower=0.0)
    return df


def predecir_manana(bloques_recientes: pd.DataFrame, bundle: dict,
                    baseline: pd.DataFrame | None = None) -> pd.DataFrame:
    """Del resumen por bloques reciente (>= max(LAGS)+1 días para lags completos) a
    una fila por (estación, contaminante, bloque) con la PREDICCION para t+horizonte.

    `baseline`: la climatología del train. Por defecto se toma la del bundle
    (`bundle["baseline"]`, la del nb04); como último recurso se calcula sobre
    `bloques_recientes` — peor que la del train, pero autónomo. Devuelve solo las
    filas del último día con datos; FECHA_OBJETIVO es ese día más el horizonte.
    """
    if baseline is None:
        baseline = bundle.get("baseline")
    if baseline is None:
        baseline = calcular_baseline(bloques_recientes)
    horizonte = int(bundle.get("horizonte", 1))

    # exigir_objetivo=False: al predecir el futuro el TARGET aún no existe.
    df = preparar_dataset(bloques_recientes, baseline, horizonte, exigir_objetivo=False)
    ultimo_dia = df["FECHA"].max()
    hoy = df[(df["FECHA"] == ultimo_dia) & df["MEDIA"].notna()].copy()
    hoy = predecir(hoy, bundle)
    hoy["CONTAMINANTE"] = hoy["MAGNITUD"].map(MAGNITUDES_OBJETIVO)
    columnas = ["ESTACION", "MAGNITUD", "CONTAMINANTE", "BLOQUE",
                "FECHA", "FECHA_OBJETIVO", "MEDIA", "PREDICCION"]
    return hoy[columnas].reset_index(drop=True)
