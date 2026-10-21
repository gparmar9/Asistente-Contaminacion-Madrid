import datetime as dt
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'etl'))
from features_prediccion import FEATURES_PREDICCION
from inferencia_prediccion import predecir, predecir_manana


class _ModeloFijo:
    """Doble de un regresor: predice siempre el mismo valor (duck typing de .predict)."""

    def __init__(self, valor):
        self._valor = valor

    def predict(self, X):
        return [self._valor] * len(X)


def _bundle(valor=42.0, nombre="NO2"):
    return {"modelos": {nombre: _ModeloFijo(valor)}, "features": list(FEATURES_PREDICCION),
            "horizonte": 1, "cobertura_min": 0.7}


def _filas_feat():
    """Dos filas con features mínimas: una de NO2 (magnitud 8) y una de O3 (14)."""
    base = {c: 1.0 for c in FEATURES_PREDICCION}
    return pd.DataFrame([
        {**base, "ESTACION": 4, "MAGNITUD": 8, "BLOQUE": "manana"},
        {**base, "ESTACION": 4, "MAGNITUD": 14, "BLOQUE": "manana"},
    ])


def test_predecir_solo_las_magnitudes_con_modelo():
    r = predecir(_filas_feat(), _bundle(valor=42.0, nombre="NO2"))
    assert r.loc[r.MAGNITUD == 8, "PREDICCION"].iloc[0] == 42.0
    assert pd.isna(r.loc[r.MAGNITUD == 14, "PREDICCION"].iloc[0])


def test_predecir_recorta_las_negativas_a_cero():
    """El regresor puede extrapolar por debajo de 0; una concentración no."""
    r = predecir(_filas_feat(), _bundle(valor=-5.0))
    assert r.loc[r.MAGNITUD == 8, "PREDICCION"].iloc[0] == 0.0


def test_predecir_manana_devuelve_los_bloques_del_ultimo_dia():
    """9 días de un bloque de NO2 -> una fila, para el día siguiente al último."""
    inicio = dt.date(2026, 6, 1)
    bloques = pd.DataFrame([
        {"ESTACION": 4, "MAGNITUD": 8, "BLOQUE": "manana",
         "FECHA": inicio + dt.timedelta(days=d), "MEDIA": 10.0 + d,
         "COBERTURA": 1.0, "MES": 6}
        for d in range(9)
    ])
    r = predecir_manana(bloques, _bundle(valor=33.3))
    assert len(r) == 1
    fila = r.iloc[0]
    assert fila["CONTAMINANTE"] == "NO2"
    assert fila["FECHA"] == pd.Timestamp(2026, 6, 9)
    assert fila["FECHA_OBJETIVO"] == pd.Timestamp(2026, 6, 10)
    assert fila["PREDICCION"] == 33.3
