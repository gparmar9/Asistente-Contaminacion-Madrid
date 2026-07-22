import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "etl"))
from cargar_estaciones import _preparar, RUTA_CSV


def test_preparar_24_estaciones_con_distrito():
    df = _preparar(RUTA_CSV)
    assert len(df) == 24
    assert df["distrito"].notna().all()        # todas con distrito asignado
    assert df["codigo_corto"].is_unique        # clave de JOIN única
    assert df["mide_no2"].dtype == bool         # flags de contaminantes booleanos


def test_escuelas_aguirre_en_salamanca():
    df = _preparar(RUTA_CSV).set_index("codigo_corto")
    fila = df.loc[8]
    assert fila["nombre"] == "Escuelas Aguirre"
    assert fila["distrito"] == "Salamanca"
    assert fila["tipo"] == "Urbana tráfico"
    assert bool(fila["mide_no2"]) is True
