def test_listado_completo_y_ordenado(cliente):
    r = cliente.get("/estaciones")
    assert r.status_code == 200
    estaciones = r.json()
    assert [e["codigo_corto"] for e in estaciones] == [8, 49]
    retiro = estaciones[1]
    assert retiro["nombre"] == "Parque del Retiro"
    assert retiro["distrito"] == "Retiro"
    assert retiro["tipo"] == "Urbana Fondo"
    assert retiro["latitud"] == 40.4144


def test_columnas_mide_se_aplanan_a_contaminantes_consultables(cliente):
    estaciones = cliente.get("/estaciones").json()
    aguirre, retiro = estaciones
    # mide_no2 implica NO/NO2/NOx (el analizador mide los tres a la vez);
    # SO2/CO/BTX quedan fuera del alcance del proyecto y no se exponen.
    assert aguirre["contaminantes_medidos"] == ["NO", "NO2", "NOx", "O3", "PM10", "PM2.5"]
    assert retiro["contaminantes_medidos"] == ["NO", "NO2", "NOx", "O3"]
    # Las columnas booleanas crudas no se exponen en la API
    assert not any(clave.startswith("mide_") for clave in retiro)
