import sys
import os

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'etl'))
from pipeline_tiempo_real import _a_horario_valido


def test_a_horario_valido_filtra_invalidas_y_normaliza():
    df = pd.DataFrame([
        {"estacion": 4, "magnitud": 8, "fecha": pd.Timestamp("2026-05-01 07:00:00"),
         "hora": 7, "valor": 25.0, "validacion": "V"},
        {"estacion": 4, "magnitud": 8, "fecha": pd.Timestamp("2026-05-01 08:00:00"),
         "hora": 8, "valor": 0.0, "validacion": "N"},   # inválida -> se descarta
    ])
    out = _a_horario_valido(df)

    # solo la fila válida, con el esquema en mayúsculas que espera a_bloques
    assert list(out.columns) == ["ESTACION", "MAGNITUD", "FECHA", "HORA", "VALOR"]
    assert len(out) == 1
    assert out.iloc[0]["HORA"] == 7
    assert out.iloc[0]["VALOR"] == 25.0
    assert str(out.iloc[0]["FECHA"]) == "2026-05-01"   # timestamp -> date del día


def test_calidad_datos_estaciones_y_pct_validas():
    from pipeline_tiempo_real import calidad_datos, estaciones_catalogo
    df = pd.DataFrame([
        {"estacion": 4, "validacion": "V"},
        {"estacion": 4, "validacion": "N"},
        {"estacion": 8, "validacion": "V"},
        {"estacion": 11, "validacion": "N"},   # solo horas sin medir -> sin datos
    ])
    out = calidad_datos(df)

    assert out["estaciones"] == 2
    assert out["pct_validas"] == 50.0
    assert out["estaciones_sin_datos"] == sorted(estaciones_catalogo() - {4, 8})
    assert 11 in out["estaciones_sin_datos"]
    assert len(estaciones_catalogo()) == 24


def test_calidad_datos_sin_filas():
    from pipeline_tiempo_real import calidad_datos
    out = calidad_datos(pd.DataFrame({"estacion": [], "validacion": []}))
    assert out["estaciones"] == 0
    assert out["pct_validas"] is None
    assert len(out["estaciones_sin_datos"]) == 24


def test_anomalias_por_magnitud_solo_las_que_tienen():
    from pipeline_tiempo_real import anomalias_por_magnitud
    resumen = pd.DataFrame({"MAGNITUD": [8, 8, 10, 14], "IS_ANOMALY": [1, 1, 0, 1]})
    assert anomalias_por_magnitud(resumen) == {"8": 2, "14": 1}
    assert anomalias_por_magnitud(resumen.iloc[0:0]) == {}
