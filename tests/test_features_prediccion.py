import datetime as dt
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'etl'))
from features_prediccion import (anadir_objetivo, completar_calendario,
                                 construir_lags, preparar_dataset)
from features_bloques import calcular_baseline


def _serie(dias, estacion=4, magnitud=8, bloque="manana", inicio=dt.date(2026, 6, 1),
           media=None, cobertura=1.0):
    """Una serie diaria de un bloque; MEDIA = ordinal del día (0, 1, 2...) salvo que se fije."""
    return pd.DataFrame([
        {"ESTACION": estacion, "MAGNITUD": magnitud, "BLOQUE": bloque,
         "FECHA": inicio + dt.timedelta(days=d),
         "MEDIA": float(d) if media is None else media,
         "COBERTURA": cobertura, "MES": (inicio + dt.timedelta(days=d)).month}
        for d in dias
    ])


def test_lags_correctos_en_una_serie():
    """Con MEDIA = día (0..9), el LAG_k del día d es d-k y la móvil de 3 incluye el propio día."""
    df = construir_lags(completar_calendario(_serie(range(10))))
    ultimo = df[df.FECHA == df.FECHA.max()].iloc[0]
    assert ultimo["MEDIA"] == 9.0
    assert ultimo["MEDIA_LAG_1"] == 8.0
    assert ultimo["MEDIA_LAG_6"] == 3.0
    assert ultimo["MEDIA_MOVIL_3"] == pytest.approx((7 + 8 + 9) / 3)
    assert ultimo["MEDIA_MOVIL_7"] == pytest.approx((3 + 4 + 5 + 6 + 7 + 8 + 9) / 7)


def test_lags_no_cruzan_series():
    """Dos estaciones intercaladas: el lag de una nunca toma valores de la otra."""
    a = _serie(range(5), estacion=4, media=None)            # MEDIA = 0..4
    b = _serie(range(5), estacion=8, media=100.0)           # MEDIA = 100 fija
    df = construir_lags(completar_calendario(pd.concat([a, b], ignore_index=True)))
    ult_a = df[(df.ESTACION == 4) & (df.FECHA == df.FECHA.max())].iloc[0]
    ult_b = df[(df.ESTACION == 8) & (df.FECHA == df.FECHA.max())].iloc[0]
    assert ult_a["MEDIA_LAG_1"] == 3.0
    assert ult_b["MEDIA_LAG_1"] == 100.0


def test_hueco_no_arrastra_el_valor_anterior():
    """Falta el día 3: la rejilla lo crea con MEDIA NaN y el LAG_1 del día 4 es NaN
    (no el valor del día 2, que sería mentir sobre 'ayer')."""
    df = construir_lags(completar_calendario(_serie([0, 1, 2, 4])))
    assert len(df) == 5  # la rejilla añade el día 3
    dia3 = df.iloc[3]
    dia4 = df.iloc[4]
    assert pd.isna(dia3["MEDIA"])
    assert pd.isna(dia4["MEDIA_LAG_1"])
    assert dia4["MEDIA_LAG_2"] == 2.0


def test_serie_mas_corta_que_los_lags_no_rompe():
    """Con 3 días (menos que max(LAGS)=6), los lags que no existen quedan NaN, sin error."""
    df = construir_lags(completar_calendario(_serie(range(3))))
    ultimo = df[df.FECHA == df.FECHA.max()].iloc[0]
    assert ultimo["MEDIA_LAG_1"] == 1.0
    assert ultimo["MEDIA_LAG_2"] == 0.0
    assert pd.isna(ultimo["MEDIA_LAG_3"])     # no hay día -3
    assert pd.isna(ultimo["MEDIA_MOVIL_7"])   # no hay 7 días


def test_entrada_vacia_no_rompe():
    """Un DataFrame sin filas se propaga vacío (date_range(NaT) reventaría)."""
    vacio = _serie(range(1)).iloc[0:0]   # conserva las columnas, sin filas
    assert completar_calendario(vacio).empty
    base = pd.DataFrame(columns=["ESTACION", "MAGNITUD", "BLOQUE", "MES", "MEDIA_ESPERADA", "STD_ESPERADA"])
    assert preparar_dataset(vacio, base).empty


def test_objetivo_es_el_dia_siguiente():
    """TARGET del día t = MEDIA de t+1, y el calendario *_OBJ es el de t+1 (anti-fuga)."""
    df = anadir_objetivo(completar_calendario(_serie(range(3), inicio=dt.date(2026, 6, 5))))
    primero = df.iloc[0]   # viernes 5 de junio
    assert primero["TARGET"] == 1.0
    assert primero["FECHA_OBJETIVO"] == pd.Timestamp(2026, 6, 6)
    assert primero["DIA_SEMANA_OBJ"] == 5           # sábado
    assert bool(primero["ES_FIN_SEMANA_OBJ"]) is True
    assert pd.isna(df.iloc[-1]["TARGET"])           # el último día no tiene mañana


def test_preparar_dataset_filtra_por_cobertura_del_objetivo():
    """Un objetivo mal medido (cobertura 0.5) no es entrenable; uno bien medido sí."""
    bien = _serie(range(9))
    mal = _serie(range(9), estacion=8)
    mal.loc[mal.index[-1], "COBERTURA"] = 0.5       # el último día, que es objetivo del penúltimo
    base = calcular_baseline(pd.concat([bien, mal], ignore_index=True))
    df = preparar_dataset(pd.concat([bien, mal], ignore_index=True), base)
    penultimo_bien = df[(df.ESTACION == 4) & (df.FECHA == pd.Timestamp(2026, 6, 8))]
    penultimo_mal = df[(df.ESTACION == 8) & (df.FECHA == pd.Timestamp(2026, 6, 8))]
    assert len(penultimo_bien) == 1
    assert len(penultimo_mal) == 0


def test_entrada_con_climatologia_precalculada_no_colisiona():
    """El parquet histórico trae MEDIA_ESPERADA (calculada con TODO el histórico: fuga).
    Se descarta en la entrada: la del dataset es la del baseline pasado, sin sufijos _x/_y."""
    serie = _serie(range(9))
    base = calcular_baseline(serie)
    con_vieja = serie.assign(MEDIA_ESPERADA=999.0, STD_ESPERADA=9.0)
    df = preparar_dataset(con_vieja, base)
    assert "MEDIA_ESPERADA_x" not in df.columns and "MEDIA_ESPERADA_y" not in df.columns
    assert (df["MEDIA_ESPERADA"] != 999.0).all()


def test_preparar_dataset_cruza_la_climatologia_del_mes_objetivo():
    """La MEDIA_ESPERADA de una fila es la del MES de la fecha OBJETIVO, no la del día t:
    el 30 de junio debe recibir la climatología de julio."""
    serie = _serie(range(40), inicio=dt.date(2026, 6, 1))   # junio y julio
    base = calcular_baseline(serie)
    df = preparar_dataset(serie, base)
    fin_de_junio = df[df.FECHA == pd.Timestamp(2026, 6, 30)].iloc[0]
    julio = base[base.MES == 7].iloc[0]
    assert fin_de_junio["MES_OBJ"] == 7
    assert fin_de_junio["MEDIA_ESPERADA"] == pytest.approx(julio["MEDIA_ESPERADA"])
