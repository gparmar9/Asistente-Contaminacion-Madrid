import sys
import os
import datetime as dt

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'etl'))
from pipeline_tiempo_real import _a_horario_valido, bloques_cerrados


def _bloques_de(fecha):
    return pd.DataFrame([{"FECHA": fecha, "BLOQUE": b}
                         for b in ("madrugada", "manana", "tarde", "noche")])


def test_bloques_cerrados_segun_la_hora_de_la_ejecucion():
    hoy = dt.date(2026, 10, 23)
    cerrados = lambda h, m: list(bloques_cerrados(_bloques_de(hoy), dt.datetime(2026, 10, 23, h, m))["BLOQUE"])
    assert cerrados(6, 59) == []
    assert cerrados(8, 15) == ["madrugada"]           # la mañana sigue abierta
    assert cerrados(14, 15) == ["madrugada", "manana"]
    assert cerrados(21, 15) == ["madrugada", "manana", "tarde"]
    # la noche se cierra con la hora 22: la 23 no la publica la API
    assert cerrados(23, 45) == ["madrugada", "manana", "tarde", "noche"]


def test_bloques_cerrados_los_dias_anteriores_siempre_lo_estan():
    ayer = dt.date(2026, 10, 22)
    assert len(bloques_cerrados(_bloques_de(ayer), dt.datetime(2026, 10, 23, 0, 5))) == 4


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
