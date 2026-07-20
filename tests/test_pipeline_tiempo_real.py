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
