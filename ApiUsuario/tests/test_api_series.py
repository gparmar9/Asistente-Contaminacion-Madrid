RUTA = "/estaciones/49/series"
RANGO = {"desde": "2026-09-20", "hasta": "2026-09-21"}


def test_serie_basica_ordenada_por_fecha_y_bloque(cliente):
    r = cliente.get(RUTA, params={"contaminante": "NO2", **RANGO})
    assert r.status_code == 200
    cuerpo = r.json()
    assert cuerpo["estacion"] == 49
    assert cuerpo["contaminante"] == "NO2"
    assert [(p["fecha"], p["bloque"]) for p in cuerpo["puntos"]] == [
        ("2026-09-20", "madrugada"),
        ("2026-09-20", "manana"),
        ("2026-09-21", "madrugada"),
    ]
    manana = cuerpo["puntos"][1]
    assert manana["media"] == 68.0
    assert manana["is_anomaly"] is True
    assert manana["cobertura"] == 1.0


def test_el_dia_hasta_esta_incluido(cliente):
    r = cliente.get(RUTA, params={"contaminante": "NO2", "desde": "2026-09-20", "hasta": "2026-09-20"})
    assert [p["fecha"] for p in r.json()["puntos"]] == ["2026-09-20", "2026-09-20"]


def test_filtra_por_contaminante(cliente):
    r = cliente.get(RUTA, params={"contaminante": "O3", **RANGO})
    puntos = r.json()["puntos"]
    assert len(puntos) == 1
    assert puntos[0]["bloque"] == "tarde"


def test_filtra_por_bloque(cliente):
    r = cliente.get(RUTA, params={"contaminante": "NO2", "bloque": "manana", **RANGO})
    puntos = r.json()["puntos"]
    assert len(puntos) == 1
    assert puntos[0]["media"] == 68.0


def test_no_mezcla_estaciones(cliente):
    r = cliente.get("/estaciones/8/series", params={"contaminante": "NO2", **RANGO})
    puntos = r.json()["puntos"]
    assert len(puntos) == 1
    assert puntos[0]["media"] == 9.0


def test_estacion_inexistente_devuelve_404(cliente):
    r = cliente.get("/estaciones/99/series", params={"contaminante": "NO2", **RANGO})
    assert r.status_code == 404


def test_contaminante_fuera_de_la_lista_devuelve_422(cliente):
    r = cliente.get(RUTA, params={"contaminante": "SO2", **RANGO})
    assert r.status_code == 422


def test_desde_posterior_a_hasta_devuelve_400(cliente):
    r = cliente.get(RUTA, params={"contaminante": "NO2", "desde": "2026-09-21", "hasta": "2026-09-20"})
    assert r.status_code == 400


def test_sin_fechas_usa_ventana_por_defecto(cliente):
    r = cliente.get(RUTA, params={"contaminante": "NO2"})
    assert r.status_code == 200
    assert isinstance(r.json()["puntos"], list)
