import pandas as pd


def combinar_sin_duplicados(df_existente, df_nuevo):
    """Replica la lógica de acumulación de ingesta_datos_live_csv.py."""
    df_combined = pd.concat([df_existente, df_nuevo], ignore_index=True)
    return df_combined.drop_duplicates(subset=['estacion', 'magnitud', 'fecha'])

# Test para verificar que no se acumulen duplicados al combinar DataFrames
def test_no_acumula_duplicados():
    df_existente = pd.DataFrame([
        {'estacion': 4, 'magnitud': 8, 'fecha': '2026-05-07 10:00:00', 'valor': 12.0},
        {'estacion': 4, 'magnitud': 8, 'fecha': '2026-05-07 11:00:00', 'valor': 15.0},
    ])

    df_nuevo = pd.DataFrame([
        {'estacion': 4, 'magnitud': 8, 'fecha': '2026-05-07 10:00:00', 'valor': 12.0},  # duplicado
        {'estacion': 4, 'magnitud': 8, 'fecha': '2026-05-07 12:00:00', 'valor': 18.0},  # nuevo
    ])

    resultado = combinar_sin_duplicados(df_existente, df_nuevo)

    assert len(resultado) == 3
