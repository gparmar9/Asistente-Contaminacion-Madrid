import sys
import os
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'deploy'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'etl'))
from metricas_emf import NAMESPACE, linea_emf
import pipeline_tiempo_real as pipe

MARCA = 1760000000000

RES = {"horas_nuevas": 48, "bloques_upsert": 200, "anomalias": 3,
       "estaciones": 23, "estaciones_sin_datos": [60], "pct_validas": 95.8,
       "anomalias_por_magnitud": {"8": 2, "10": 1}}


def test_linea_emf_forma_y_metricas():
    doc = json.loads(linea_emf(RES, MARCA))

    directiva = doc["_aws"]["CloudWatchMetrics"][0]
    assert doc["_aws"]["Timestamp"] == MARCA
    assert directiva["Namespace"] == NAMESPACE
    assert directiva["Dimensions"] == [[]]   # sin dimensiones: una métrica por nombre
    nombres = [m["Name"] for m in directiva["Metrics"]]
    assert nombres == ["HorasNuevas", "EstacionesConDatos", "BloquesAnomalos", "TasaAnomalias"]
    # cada métrica declarada tiene su valor numérico en la raíz
    for nombre in nombres:
        assert isinstance(doc[nombre], (int, float))

    assert doc["HorasNuevas"] == 48
    assert doc["EstacionesConDatos"] == 23
    assert doc["BloquesAnomalos"] == 3
    assert doc["TasaAnomalias"] == 1.5


def test_linea_emf_campos_extra_no_son_metricas():
    doc = json.loads(linea_emf(RES, MARCA))

    assert doc["pct_validas"] == 95.8
    assert doc["estaciones_sin_datos"] == [60]
    assert doc["anomalias_por_magnitud"] == {"8": 2, "10": 1}
    nombres = {m["Name"] for m in doc["_aws"]["CloudWatchMetrics"][0]["Metrics"]}
    assert not nombres & {"pct_validas", "estaciones_sin_datos", "anomalias_por_magnitud"}


def test_linea_emf_tasa_con_cero_bloques():
    doc = json.loads(linea_emf({**RES, "bloques_upsert": 0, "anomalias": 0}, MARCA))
    assert doc["TasaAnomalias"] == 0.0


def test_linea_emf_rama_sin_datos():
    # la API no devolvió nada: se publica igual, con ceros y las 24 estaciones como faltantes
    doc = json.loads(linea_emf(pipe.resultado_sin_datos(), MARCA))

    assert doc["HorasNuevas"] == 0
    assert doc["EstacionesConDatos"] == 0
    assert doc["BloquesAnomalos"] == 0
    assert doc["TasaAnomalias"] == 0.0
    assert doc["pct_validas"] is None
    assert len(doc["estaciones_sin_datos"]) == 24
    assert doc["anomalias_por_magnitud"] == {}
