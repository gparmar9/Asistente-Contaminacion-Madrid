"""Motor de inferencia del detector de anomalías.

Aplica los Isolation Forest entrenados en el nb03 (guardados en
`models/isolation_forest.joblib`) sobre bloques ya provistos de features,
produciendo ANOMALY_SCORE (alto = más anómalo) e IS_ANOMALY.
"""
import joblib
import numpy as np
import pandas as pd

from limpiar_datos import MAGNITUDES_OBJETIVO
from features_bloques import a_bloques, anadir_features


def cargar_bundle(ruta):
    """Carga el .joblib con {modelos, scalers, features, contaminacion}."""
    return joblib.load(ruta)


def puntuar(bloques_feat: pd.DataFrame, bundle: dict) -> pd.DataFrame:
    """Aplica un modelo por contaminante y añade ANOMALY_SCORE e IS_ANOMALY."""
    df = bloques_feat.copy()
    features = bundle["features"]
    modelos, scalers = bundle["modelos"], bundle["scalers"]

    df["ANOMALY_SCORE"] = np.nan
    df["IS_ANOMALY"] = False

    for mag, nombre in MAGNITUDES_OBJETIVO.items():
        if nombre not in modelos:
            continue
        mask = df["MAGNITUD"] == mag
        if not mask.any():
            continue
        X = df.loc[mask, features].to_numpy()
        Xs = scalers[nombre].transform(X)
        # score_samples: cuanto más bajo, más anómalo -> negamos para que ALTO = MÁS ANÓMALO
        df.loc[mask, "ANOMALY_SCORE"] = -modelos[nombre].score_samples(Xs)
        df.loc[mask, "IS_ANOMALY"]    = modelos[nombre].predict(Xs) == -1

    return df


def detectar(largo: pd.DataFrame, baseline: pd.DataFrame, bundle: dict) -> pd.DataFrame:
    """Pipeline completo: horario válido -> bloques -> features -> puntuación."""
    bloques = a_bloques(largo)
    feat = anadir_features(bloques, baseline)
    return puntuar(feat, bundle)
