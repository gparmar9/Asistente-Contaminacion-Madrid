import sys
import os
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'etl'))
from limpiar_datos import limpiar_datos_live, CONTAMINANTES_OBJETIVO

RUTA_ESTACIONES = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw', 'estaciones-de-control.csv')

# Test para verificar que solo se mantengan los contaminantes objetivo después de limpiar los datos
def test_solo_contaminantes_objetivo():
    fila_base = {
        'PROVINCIA': '79', 'MUNICIPIO': '79', 'ESTACION': '4',
        'PUNTO_MUESTREO': '28079004_8_N', 'ANO': '2026', 'MES': '5', 'DIA': '7',
    }
    for i in range(1, 25):
        fila_base[f'H{i:02d}'] = 10.0
        fila_base[f'V{i:02d}'] = 'V'

    filas = [{**fila_base, 'MAGNITUD': mag} for mag in ['7', '8', '12', '14', '10', '9', '1', '6']]
    df = pd.DataFrame(filas)

    resultado = limpiar_datos_live(df, RUTA_ESTACIONES)

    assert set(resultado['contaminante'].unique()) == CONTAMINANTES_OBJETIVO


# Test de regresión: la API devuelve PROVINCIA y MUNICIPIO como texto, pero la tabla
# calidad_aire_horas_live los declara INTEGER. Si la limpieza no los convierte, el
# INSERT desde la tabla de staging falla con DatatypeMismatch.
def test_provincia_y_municipio_son_enteros():
    fila = {
        'PROVINCIA': '28', 'MUNICIPIO': '79', 'ESTACION': '4', 'MAGNITUD': '8',
        'PUNTO_MUESTREO': '28079004_8_8', 'ANO': '2026', 'MES': '5', 'DIA': '7',
    }
    for i in range(1, 25):
        fila[f'H{i:02d}'] = 10.0
        fila[f'V{i:02d}'] = 'V'

    resultado = limpiar_datos_live(pd.DataFrame([fila]), RUTA_ESTACIONES)

    assert pd.api.types.is_integer_dtype(resultado['provincia'])
    assert pd.api.types.is_integer_dtype(resultado['municipio'])
    assert resultado['provincia'].iloc[0] == 28
