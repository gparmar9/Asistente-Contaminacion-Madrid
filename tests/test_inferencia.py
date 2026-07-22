import sys
import os

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'etl'))
from inferencia import puntuar
from features_bloques import FEATURES_MODELO


def _bundle_minimo():
    """Entrena un Isolation Forest diminuto para NO2 (magnitud 8) con datos 'normales'."""
    rng = np.random.default_rng(0)
    X = rng.normal(0, 1, size=(300, len(FEATURES_MODELO)))
    scaler = StandardScaler().fit(X)
    modelo = IsolationForest(contamination=0.05, random_state=0).fit(scaler.transform(X))
    return {"modelos": {"NO2": modelo}, "scalers": {"NO2": scaler},
            "features": FEATURES_MODELO, "contaminacion": 0.05}


def test_puntuar_añade_columnas_del_modelo():
    out = puntuar(pd.DataFrame([{"MAGNITUD": 8, "Z_SCORE": 0.0, "COBERTURA": 1.0, "CV": 0.3}]),
                  _bundle_minimo())
    assert "ANOMALY_SCORE" in out.columns
    assert "IS_ANOMALY" in out.columns


def test_puntuar_marca_el_outlier_evidente():
    bloques = pd.DataFrame([
        {"MAGNITUD": 8, "Z_SCORE": 0.0,  "COBERTURA": 1.0, "CV": 0.3},   # normal
        {"MAGNITUD": 8, "Z_SCORE": 50.0, "COBERTURA": 0.1, "CV": 0.0},   # claramente anómalo
    ])
    out = puntuar(bloques, _bundle_minimo())
    # el bloque extremo debe puntuar más alto y quedar marcado como anomalía
    assert out.iloc[1]["ANOMALY_SCORE"] > out.iloc[0]["ANOMALY_SCORE"]
    assert bool(out.iloc[1]["IS_ANOMALY"]) is True


def test_puntuar_ignora_magnitud_sin_modelo():
    """Si no hay modelo para esa magnitud, no falla; deja score NaN."""
    out = puntuar(pd.DataFrame([{"MAGNITUD": 14, "Z_SCORE": 0.0, "COBERTURA": 1.0, "CV": 0.3}]),
                  _bundle_minimo())
    assert np.isnan(out.iloc[0]["ANOMALY_SCORE"])
    assert bool(out.iloc[0]["IS_ANOMALY"]) is False
