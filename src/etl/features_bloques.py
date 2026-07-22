"""Agregación horario -> bloques y features del detector de anomalías.

Es la MISMA lógica del notebook 02 (bloques + baseline + z-score) y del nb03
(feature CV), extraída a funciones para que los notebooks y la inferencia en
tiempo real construyan las features exactamente igual (sin deriva).

Entrada esperada de `a_bloques`: DataFrame en formato largo con columnas
    ESTACION, MAGNITUD, FECHA (date), HORA (0-23), VALOR
y **solo mediciones válidas** (el flag N ya descartado).
"""
import numpy as np
import pandas as pd

from limpiar_datos import MAGNITUDES_OBJETIVO   # {7: 'NO', 8: 'NO2', ...}

# Los 4 bloques del día: nombre -> (hora_inicio, hora_fin), ambas incluidas
BLOQUES = {"madrugada": (0, 6), "manana": (7, 12), "tarde": (13, 19), "noche": (20, 23)}
ORDEN_BLOQUES = ["madrugada", "manana", "tarde", "noche"]
_DURACION = {b: fin - ini + 1 for b, (ini, fin) in BLOQUES.items()}
_LIMITES = [-1, 6, 12, 19, 23]   # (-1,6]=madrugada ... (19,23]=noche

CLAVES = ["ESTACION", "MAGNITUD", "FECHA", "BLOQUE"]
CLAVES_BASE = ["ESTACION", "MAGNITUD", "BLOQUE", "MES"]
FEATURES_MODELO = ["Z_SCORE", "COBERTURA", "CV"]


def a_bloques(largo: pd.DataFrame) -> pd.DataFrame:
    """Agrega el horario válido a una fila por (estación, magnitud, fecha, bloque)."""
    df = largo.copy()
    df["BLOQUE"] = pd.cut(df["HORA"], bins=_LIMITES, labels=ORDEN_BLOQUES).astype(str)

    resumen = (
        df.groupby(CLAVES, observed=True)
        .agg(MEDIA=("VALOR", "mean"), MAXIMO=("VALOR", "max"), MINIMO=("VALOR", "min"),
             STD=("VALOR", "std"), N_HORAS=("VALOR", "size"))
        .reset_index()
    )
    # Hora exacta del máximo y del mínimo (mismo orden de grupos que el agg anterior)
    idx_max = df.groupby(CLAVES, observed=True)["VALOR"].idxmax()
    idx_min = df.groupby(CLAVES, observed=True)["VALOR"].idxmin()
    resumen["HORA_MAXIMO"] = df.loc[idx_max, "HORA"].to_numpy()
    resumen["HORA_MINIMO"] = df.loc[idx_min, "HORA"].to_numpy()

    resumen["HORA_INICIO"] = resumen["BLOQUE"].map(lambda b: BLOQUES[b][0])
    resumen["HORA_FIN"]    = resumen["BLOQUE"].map(lambda b: BLOQUES[b][1])
    resumen["COBERTURA"]   = (resumen["N_HORAS"] / resumen["BLOQUE"].map(_DURACION)).round(3)
    resumen["RANGO"]       = resumen["MAXIMO"] - resumen["MINIMO"]

    # Calendario
    f = pd.to_datetime(resumen["FECHA"])
    resumen["ANO"]           = f.dt.year
    resumen["MES"]           = f.dt.month
    resumen["DIA_SEMANA"]    = f.dt.dayofweek
    resumen["ES_FIN_SEMANA"] = resumen["DIA_SEMANA"].isin([5, 6])
    resumen["CONTAMINANTE"]  = resumen["MAGNITUD"].map(MAGNITUDES_OBJETIVO)
    return resumen


def calcular_baseline(bloques: pd.DataFrame) -> pd.DataFrame:
    """Baseline histórico: valor esperado por (estación, magnitud, bloque, mes)."""
    return (bloques.groupby(CLAVES_BASE, observed=True)["MEDIA"]
            .agg(MEDIA_ESPERADA="mean", STD_ESPERADA="std").reset_index())


def anadir_features(bloques: pd.DataFrame, baseline: pd.DataFrame) -> pd.DataFrame:
    """Añade EXPECTED_VALUE, DESVIACION, Z_SCORE y CV usando el baseline histórico."""
    df = bloques.merge(baseline, on=CLAVES_BASE, how="left")
    df["EXPECTED_VALUE"] = df["MEDIA_ESPERADA"]
    df["DESVIACION"]     = df["MEDIA"] - df["MEDIA_ESPERADA"]
    df["Z_SCORE"]        = (df["DESVIACION"] / df["STD_ESPERADA"].replace(0, np.nan)).fillna(0.0)
    df["CV"]             = df["STD"].fillna(0.0) / (df["MEDIA"].abs() + 1.0)
    return df
