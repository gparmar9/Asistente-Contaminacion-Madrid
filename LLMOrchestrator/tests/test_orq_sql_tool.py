from llm_orchestrator.tools.sql_tool import query_sql

RANGO = {"fecha_inicio": "2026-09-20", "fecha_fin": "2026-09-21"}


def test_ultimos_niveles_devuelve_el_ultimo_dia(motor):
    r = query_sql(motor, consulta="ultimos_niveles", contaminante="NO2")
    fechas = {f["fecha"] for f in r["filas"]}
    assert fechas == {"2026-09-21"}
    assert {f["estacion"] for f in r["filas"]} == {8, 49}
    assert r["filas"][0]["nombre_estacion"]  # el JOIN con estaciones funciona


def test_ultimos_niveles_filtra_por_estacion(motor):
    r = query_sql(motor, consulta="ultimos_niveles", contaminante="NO2", estacion=49)
    assert [f["estacion"] for f in r["filas"]] == [49]


def test_serie_bloques_ordenada_y_acotada(motor):
    r = query_sql(motor, consulta="serie_bloques", contaminante="NO2", estacion=49, **RANGO)
    assert [(f["fecha"], f["bloque"]) for f in r["filas"]] == [
        ("2026-09-20", "madrugada"),
        ("2026-09-20", "manana"),
        ("2026-09-21", "madrugada"),
    ]


def test_serie_bloques_sin_estacion_es_error(motor):
    r = query_sql(motor, consulta="serie_bloques", contaminante="NO2", **RANGO)
    assert "estacion" in r["error"]


def test_anomalias_excluye_cobertura_baja(motor):
    r = query_sql(motor, consulta="anomalias", **RANGO)
    # La anomalía de la estación 8 (cobertura 0,4) no debe aparecer
    assert [(f["estacion"], f["bloque"]) for f in r["filas"]] == [(49, "manana")]
    assert r["filas"][0]["is_anomaly"]


def test_comparar_estaciones_ordena_por_media(motor):
    r = query_sql(motor, consulta="comparar_estaciones", contaminante="NO2", **RANGO)
    assert [f["estacion"] for f in r["filas"]] == [49, 8]
    assert r["filas"][0]["media_periodo"] == 31.5  # (12.5 + 68 + 14) / 3


def test_info_estaciones_por_distrito_ignora_mayusculas(motor):
    r = query_sql(motor, consulta="info_estaciones", distrito="retiro")
    assert [f["codigo_corto"] for f in r["filas"]] == [49]
    assert r["filas"][0]["distrito"] == "Retiro"


def test_info_estaciones_admite_distrito_parcial(motor):
    # "Sala" debe encontrar "Salamanca": el LLM no conoce los nombres exactos
    r = query_sql(motor, consulta="info_estaciones", distrito="sala")
    assert [f["codigo_corto"] for f in r["filas"]] == [8]


def test_resultado_truncado_avisa_al_llm(motor):
    from sqlalchemy import text

    # 16 días x 4 bloques = 64 filas de PM10 en la estación 49 (> LIMITE_FILAS)
    with motor.begin() as conn:
        for dia in range(1, 17):
            for bloque, hora in [("madrugada", 0), ("manana", 7), ("tarde", 13), ("noche", 20)]:
                conn.execute(text(
                    "INSERT INTO resumen_datos_ml (estacion, magnitud, contaminante, fecha, "
                    "bloque, hora_inicio, media, cobertura, is_anomaly) VALUES "
                    "(49, 10, 'PM10', :fecha, :bloque, :hora, 20.0, 1.0, 0)"
                ), {"fecha": f"2026-08-{dia:02d} 00:00:00", "bloque": bloque, "hora": hora})

    r = query_sql(motor, consulta="serie_bloques", contaminante="PM10", estacion=49,
                  fecha_inicio="2026-08-01", fecha_fin="2026-08-16")
    assert len(r["filas"]) == 60
    assert "truncado" in r["mensaje"]


def test_ultimos_niveles_de_estacion_retrasada_usa_su_ultimo_dia(motor):
    from sqlalchemy import text

    # La estación 8 deja de emitir O3 el 19; la 49 tiene O3 el 20.
    with motor.begin() as conn:
        conn.execute(text(
            "INSERT INTO resumen_datos_ml (estacion, magnitud, contaminante, fecha, "
            "bloque, hora_inicio, media, cobertura, is_anomaly) VALUES "
            "(8, 14, 'O3', '2026-09-19 00:00:00', 'tarde', 13, 70.0, 1.0, 0)"
        ))
    r = query_sql(motor, consulta="ultimos_niveles", contaminante="O3", estacion=8)
    # Su último día con datos (19), no el vacío del máximo global (20)
    assert [f["fecha"] for f in r["filas"]] == ["2026-09-19"]


def test_consulta_desconocida_es_error(motor):
    assert "Consulta desconocida" in query_sql(motor, consulta="drop_tables")["error"]


def test_contaminante_invalido_es_error(motor):
    r = query_sql(motor, consulta="ultimos_niveles", contaminante="SO2")
    assert "contaminante" in r["error"]


def test_fecha_malformada_es_error(motor):
    r = query_sql(motor, consulta="anomalias", fecha_inicio="ayer")
    assert "YYYY-MM-DD" in r["error"]


def test_rango_invertido_es_error(motor):
    r = query_sql(motor, consulta="anomalias",
                  fecha_inicio="2026-09-21", fecha_fin="2026-09-20")
    assert "posterior" in r["error"]


def test_sin_filas_incluye_mensaje(motor):
    r = query_sql(motor, consulta="serie_bloques", contaminante="PM10", estacion=49, **RANGO)
    assert r["filas"] == []
    assert "no devolvió filas" in r["mensaje"]
