"""prediccion.api con FastAPI TestClient: contrato HTTP y traducción de errores.

Requiere fastapi y httpx (se salta si faltan). Sin base de datos ni bundles reales:
el motor se sustituye por uno con lector inyectado y modelos de valor fijo.
"""
import datetime as dt

import pandas as pd
import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient  # noqa: E402

from prediccion import api, motor  # noqa: E402
from tests.test_prediccion_motor import _bloques, _bundle  # noqa: E402


@pytest.fixture
def cliente(monkeypatch):
    m = motor.Motor({1: _bundle(1, 11.0), 2: _bundle(2, 22.0)}, lambda dias: _bloques())
    monkeypatch.setattr(api, "MOTOR", m)
    return TestClient(app=api.app, raise_server_exceptions=False)


def test_salud(cliente):
    r = cliente.get("/salud")
    assert r.status_code == 200
    cuerpo = r.json()
    assert cuerpo["estado"] == "ok"
    assert cuerpo["horizontes"] == [1, 2]
    assert cuerpo["ultima_fecha_completa"] == "2026-03-16"
    assert cuerpo["predecible_hasta"] == "2026-03-18"


def test_herramienta_publica_la_definicion(cliente):
    r = cliente.get("/prediccion/herramienta")
    assert r.status_code == 200
    funcion = r.json()["herramienta"]["function"]
    assert funcion["name"] == "predecir_calidad_aire"
    assert funcion["parameters"]["required"] == ["fecha_objetivo"]


def test_prediccion_ok(cliente):
    r = cliente.post("/prediccion", json={"fecha_objetivo": "2026-03-17",
                                          "contaminante": "NO2", "bloque": "manana"})
    assert r.status_code == 200
    cuerpo = r.json()
    assert (cuerpo["estado"], cuerpo["horizonte"]) == ("ok", 1)
    assert cuerpo["filas"] == [{"estacion": 4, "nombre_estacion": "Plaza de España",
                                "contaminante": "NO2", "bloque": "manana", "prediccion": 11.0}]


def test_prediccion_ya_medido(cliente):
    r = cliente.post("/prediccion", json={"fecha_objetivo": "2026-03-10"})
    assert r.status_code == 200
    assert r.json()["estado"] == "ya_medido"


def test_prediccion_fuera_de_alcance(cliente):
    r = cliente.post("/prediccion", json={"fecha_objetivo": "2026-03-25"})
    assert r.status_code == 200
    cuerpo = r.json()
    assert cuerpo["estado"] == "fuera_de_alcance"
    assert cuerpo["predecible_hasta"] == "2026-03-18"


def test_entrada_invalida_es_422(cliente):
    assert cliente.post("/prediccion", json={"fecha_objetivo": "mañana"}).status_code == 422
    assert cliente.post("/prediccion", json={"fecha_objetivo": "2026-03-17",
                                             "contaminante": "SO2"}).status_code == 422


def test_base_caida_es_503(monkeypatch):
    m = motor.Motor({1: _bundle(1, 11.0), 2: _bundle(2, 22.0)},
                    lambda dias: _bloques().iloc[0:0])
    monkeypatch.setattr(api, "MOTOR", m)
    cliente = TestClient(app=api.app, raise_server_exceptions=False)
    r = cliente.post("/prediccion", json={"fecha_objetivo": "2026-03-17"})
    assert r.status_code == 503
    assert r.json()["error"] == "DatosNoDisponibles"
