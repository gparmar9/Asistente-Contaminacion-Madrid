"""Motor del servicio de predicción: ancla, elección de horizonte, filtros y estados.

Sin base de datos (lector inyectado) y sin sklearn (modelos dobles de valor fijo).
"""
import datetime as dt

import pandas as pd
import pytest

from prediccion import motor
from prediccion.errores import DatosNoDisponibles, ModelosNoDisponibles
from features_prediccion import FEATURES_PREDICCION  # src/etl (sys.path vía prediccion)

BLOQUES = ("madrugada", "manana", "tarde", "noche")


class _ModeloFijo:
    def __init__(self, valor):
        self._valor = valor

    def predict(self, X):
        return [self._valor] * len(X)


def _bloques(dias=16, ultimo_parcial=False, inicio=dt.date(2026, 3, 1)):
    """`dias` días completos de NO2 y O3 en Plaza de España (código 4). Con
    `ultimo_parcial`, el último día solo tiene madrugada y mañana (ingesta a medias)."""
    fechas = [inicio + dt.timedelta(days=d) for d in range(dias)]
    filas = []
    for f in fechas:
        for b in BLOQUES:
            if ultimo_parcial and f == fechas[-1] and b in ("tarde", "noche"):
                continue
            for mag in (8, 14):   # NO2, O3
                filas.append({"ESTACION": 4, "NOMBRE_ESTACION": "Plaza de España",
                              "MAGNITUD": mag, "FECHA": pd.Timestamp(f), "BLOQUE": b,
                              "MEDIA": 20.0, "COBERTURA": 1.0})
    return pd.DataFrame(filas)


def _baseline():
    return pd.DataFrame([
        {"ESTACION": 4, "MAGNITUD": m, "BLOQUE": b, "MES": mes,
         "MEDIA_ESPERADA": 20.0, "STD_ESPERADA": 1.0}
        for m in (8, 14) for b in BLOQUES for mes in (3, 4)
    ])


def _bundle(horizonte, valor, contaminantes=("NO2", "O3")):
    return {"modelos": {n: _ModeloFijo(valor) for n in contaminantes},
            "features": list(FEATURES_PREDICCION), "horizonte": horizonte,
            "cobertura_min": 0.7, "baseline": _baseline(),
            "configuracion": {"nombre": "test"}, "fecha_corte_train": "2026-01-01"}


def _motor(df=None, bundles=None):
    df = _bloques() if df is None else df
    bundles = bundles or {1: _bundle(1, 11.0), 2: _bundle(2, 22.0)}
    return motor.Motor(bundles, lambda dias: df)


# --------------------------------------------------------------------------- ancla

def test_ultimo_dia_completo_ignora_un_dia_a_medio_ingerir():
    """Un día con solo madrugada y mañana no es ancla: desplazaría los horizontes."""
    assert motor.ultimo_dia_completo(_bloques()) == pd.Timestamp(2026, 3, 16)
    assert motor.ultimo_dia_completo(_bloques(ultimo_parcial=True)) == pd.Timestamp(2026, 3, 15)


# --------------------------------------------------------------------------- horizontes

def test_el_dia_siguiente_al_ancla_usa_h1():
    r = _motor().predecir(dt.date(2026, 3, 17))
    assert (r["estado"], r["horizonte"]) == ("ok", 1)
    assert r["ultima_fecha_completa"] == "2026-03-16"
    assert len(r["filas"]) == 8                      # 2 contaminantes × 4 bloques
    assert all(f["prediccion"] == 11.0 for f in r["filas"])


def test_dos_dias_despues_del_ancla_usa_h2():
    r = _motor().predecir(dt.date(2026, 3, 18))
    assert (r["estado"], r["horizonte"]) == ("ok", 2)
    assert all(f["prediccion"] == 22.0 for f in r["filas"])


def test_una_fecha_ya_medida_no_se_predice():
    assert _motor().predecir(dt.date(2026, 3, 16))["estado"] == "ya_medido"


def test_mas_alla_del_ultimo_horizonte_es_fuera_de_alcance():
    r = _motor().predecir(dt.date(2026, 3, 19))
    assert r["estado"] == "fuera_de_alcance"
    assert r["predecible_hasta"] == "2026-03-18"


def test_un_dia_parcial_no_desplaza_los_horizontes():
    """Con el último día a medias, el ancla es el anterior: predecir ese mismo día
    (sus bloques de tarde y noche aún no medidos) es h1, no 'ya_medido'."""
    r = _motor(df=_bloques(ultimo_parcial=True)).predecir(dt.date(2026, 3, 16))
    assert (r["estado"], r["horizonte"]) == ("ok", 1)
    assert r["ultima_fecha_completa"] == "2026-03-15"


# --------------------------------------------------------------------------- filtros y estados

def test_filtra_por_estacion_contaminante_y_bloque():
    r = _motor().predecir(dt.date(2026, 3, 17), estacion="plaza de españa",
                          contaminante="NO2", bloque="tarde")
    assert r["estado"] == "ok"
    assert len(r["filas"]) == 1
    fila = r["filas"][0]
    assert (fila["estacion"], fila["contaminante"], fila["bloque"]) == (4, "NO2", "tarde")
    assert fila["nombre_estacion"] == "Plaza de España"


def test_estacion_desconocida_da_sin_datos():
    assert _motor().predecir(dt.date(2026, 3, 17), estacion="Sol")["estado"] == "sin_datos"


def test_contaminante_sin_modelo_se_descarta():
    """El bundle h2 solo trae NO2: las filas de O3 (PREDICCION NaN) no salen."""
    bundles = {1: _bundle(1, 11.0), 2: _bundle(2, 22.0, contaminantes=("NO2",))}
    r = _motor(bundles=bundles).predecir(dt.date(2026, 3, 18))
    assert {f["contaminante"] for f in r["filas"]} == {"NO2"}
    assert len(r["filas"]) == 4


def test_sin_filas_en_la_vista_es_datos_no_disponibles():
    vacio = _bloques().iloc[0:0]
    with pytest.raises(DatosNoDisponibles):
        _motor(df=vacio).predecir(dt.date(2026, 3, 17))


# --------------------------------------------------------------------------- bundles

def test_cargar_bundles_exige_el_horizonte_declarado(tmp_path):
    joblib = pytest.importorskip("joblib")
    joblib.dump(_bundle(2, 1.0), tmp_path / "prediccion_bloques.joblib")      # h1 con horizonte 2
    joblib.dump(_bundle(2, 1.0), tmp_path / "prediccion_bloques_h2.joblib")
    with pytest.raises(ModelosNoDisponibles):
        motor.cargar_bundles(tmp_path)


def test_cargar_bundles_falla_si_falta_un_fichero(tmp_path):
    with pytest.raises(ModelosNoDisponibles):
        motor.cargar_bundles(tmp_path)
