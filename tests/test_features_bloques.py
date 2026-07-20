import sys
import os
import datetime as dt

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'etl'))
from features_bloques import a_bloques, calcular_baseline, anadir_features, BLOQUES


def _largo_dia_completo():
    """Una estación/magnitud/día con las 24 horas; VALOR = HORA (0..23)."""
    fecha = dt.date(2025, 1, 6)   # lunes
    return pd.DataFrame(
        [{"ESTACION": 4, "MAGNITUD": 8, "FECHA": fecha, "HORA": h, "VALOR": float(h)}
         for h in range(24)]
    )


def test_a_bloques_genera_los_cuatro_bloques():
    b = a_bloques(_largo_dia_completo())
    assert set(b["BLOQUE"]) == set(BLOQUES)
    assert len(b) == 4


def test_a_bloques_estadisticos_de_madrugada():
    b = a_bloques(_largo_dia_completo())
    mad = b[b.BLOQUE == "madrugada"].iloc[0]
    # madrugada = horas 0..6 -> 7 horas, media(0..6)=3, máximo en la hora 6
    assert mad["N_HORAS"] == 7
    assert mad["COBERTURA"] == 1.0
    assert mad["MEDIA"] == pytest.approx(3.0)
    assert mad["MAXIMO"] == 6.0
    assert mad["HORA_MAXIMO"] == 6
    assert mad["HORA_INICIO"] == 0 and mad["HORA_FIN"] == 6


def test_a_bloques_cobertura_parcial():
    """Solo 3 de las 6 horas de la mañana -> cobertura 0.5 (anomalía operativa)."""
    fecha = dt.date(2025, 1, 6)
    largo = pd.DataFrame(
        [{"ESTACION": 4, "MAGNITUD": 8, "FECHA": fecha, "HORA": h, "VALOR": 10.0}
         for h in (7, 8, 9)]
    )
    man = a_bloques(largo)
    fila = man[man.BLOQUE == "manana"].iloc[0]
    assert fila["N_HORAS"] == 3
    assert fila["COBERTURA"] == 0.5


def test_calcular_baseline_es_media_por_grupo():
    """Mismo grupo en dos años con medias 10 y 20 -> valor esperado 15."""
    bloques = pd.DataFrame([
        {"ESTACION": 4, "MAGNITUD": 8, "BLOQUE": "manana", "MES": 1, "MEDIA": 10.0},
        {"ESTACION": 4, "MAGNITUD": 8, "BLOQUE": "manana", "MES": 1, "MEDIA": 20.0},
    ])
    base = calcular_baseline(bloques)
    assert len(base) == 1
    assert base.iloc[0]["MEDIA_ESPERADA"] == pytest.approx(15.0)


def test_anadir_features_calcula_zscore_y_cv():
    bloques = pd.DataFrame([{"ESTACION": 4, "MAGNITUD": 8, "BLOQUE": "manana",
                             "MES": 1, "MEDIA": 30.0, "STD": 5.0}])
    base = pd.DataFrame([{"ESTACION": 4, "MAGNITUD": 8, "BLOQUE": "manana",
                          "MES": 1, "MEDIA_ESPERADA": 20.0, "STD_ESPERADA": 5.0}])
    r = anadir_features(bloques, base).iloc[0]
    assert r["EXPECTED_VALUE"] == 20.0
    assert r["DESVIACION"] == pytest.approx(10.0)
    assert r["Z_SCORE"] == pytest.approx(2.0)            # (30-20)/5
    assert r["CV"] == pytest.approx(5.0 / (30.0 + 1.0))  # STD/(|MEDIA|+1)


def test_anadir_features_std_esperada_cero_no_rompe():
    """Si el histórico no tenía dispersión (STD=0), el z-score no debe ser inf/NaN."""
    bloques = pd.DataFrame([{"ESTACION": 4, "MAGNITUD": 8, "BLOQUE": "manana",
                             "MES": 1, "MEDIA": 30.0, "STD": 5.0}])
    base = pd.DataFrame([{"ESTACION": 4, "MAGNITUD": 8, "BLOQUE": "manana",
                          "MES": 1, "MEDIA_ESPERADA": 20.0, "STD_ESPERADA": 0.0}])
    r = anadir_features(bloques, base).iloc[0]
    assert r["Z_SCORE"] == 0.0
