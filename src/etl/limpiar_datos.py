# Librerias
import pandas as pd
import numpy as np

# Diccionario de magnitudes (código de la API -> nombre del contaminante)
DICCIONARIO_MAGNITUDES = {
    1: 'SO2', 6: 'CO', 7: 'NO', 8: 'NO2', 9: 'PM2.5',
    10: 'PM10', 12: 'NOx', 14: 'O3', 20: 'TOL', 30: 'BEN',
    35: 'EBE', 37: 'MXY', 38: 'PXY', 39: 'OXY', 42: 'TCH',
    43: 'CH4', 44: 'NMHC', 431: 'MPX'
}

# Las 6 magnitudes objetivo del proyecto (código -> nombre) y el conjunto de nombres
MAGNITUDES_OBJETIVO = {7: 'NO', 8: 'NO2', 9: 'PM2.5', 10: 'PM10', 12: 'NOx', 14: 'O3'}
CONTAMINANTES_OBJETIVO = set(MAGNITUDES_OBJETIVO.values())

# --- Constantes del formato ancho de la fuente (H01..H24 / V01..V24) ---
_COLS_HORA = [f'H{h:02d}' for h in range(1, 25)]
_COLS_FLAG = [f'V{h:02d}' for h in range(1, 25)]
_COLS_ID_BASE = ['ESTACION', 'MAGNITUD', 'ANO', 'MES', 'DIA']

# Columnas identificadoras extra que la ingesta necesita arrastrar hasta la BBDD
_ID_EXTRA_INGESTA = ['PROVINCIA', 'MUNICIPIO', 'PUNTO_MUESTREO']


def wide_a_largo(
    df: pd.DataFrame,
    magnitudes: list[int] | None = None,
    solo_validas: bool = True,
    id_extra: list[str] | None = None,
) -> pd.DataFrame:
    """Convierte el formato ancho (H01..H24 / V01..V24) a formato largo.

    Es el **único** parser de despivotado del proyecto: lo usan tanto la
    ingesta (``limpiar_datos_live`` / ``limpiar_datos_csv``) como los
    notebooks de análisis y modelado.

    Devuelve un DataFrame con una fila por medición horaria y columnas:
    ``ESTACION, MAGNITUD, FECHA (date), HORA (0-23), VALOR (float),
    VALIDO (bool)`` más las columnas indicadas en ``id_extra``.

    Parámetros
    ----------
    df : DataFrame en formato ancho.
    magnitudes : lista de códigos a conservar (p. ej. ``[7, 8, 9, 10, 12, 14]``);
        ``None`` mantiene todas.
    solo_validas : si ``True`` (por defecto) descarta las horas con flag != 'V',
        cuyos valores son placeholders/corruptos (no son mediciones reales).
        La ingesta usa ``False`` para guardar el dato crudo en la BBDD.
    id_extra : columnas identificadoras adicionales del formato ancho que se
        quieren arrastrar al formato largo (p. ej. ``['PUNTO_MUESTREO']``).
    """
    id_extra = id_extra or []
    df = df.copy()

    # Las claves numéricas pueden venir como texto (API/JSON) -> a entero anulable
    for col in _COLS_ID_BASE:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    if magnitudes is not None:
        df = df[df['MAGNITUD'].isin(magnitudes)]

    id_vars = _COLS_ID_BASE + id_extra

    # Despivotamos valores y flags por separado. `melt` procesa las columnas
    # bloque a bloque conservando el orden, así que la fila i de `valores`
    # (H0x) se corresponde con la fila i de `flags` (V0x).
    valores = df.melt(id_vars=id_vars, value_vars=_COLS_HORA,
                      var_name='COL_HORA', value_name='VALOR')
    flags = df.melt(id_vars=id_vars, value_vars=_COLS_FLAG,
                    var_name='COL_FLAG', value_name='FLAG')

    valores['HORA'] = valores['COL_HORA'].str[1:].astype(int) - 1   # H01 -> hora 0
    valores['VALIDO'] = flags['FLAG'].astype(str).str.strip().eq('V').to_numpy()
    valores['VALOR'] = pd.to_numeric(valores['VALOR'], errors='coerce')

    if solo_validas:
        valores = valores[valores['VALIDO']]
    valores = valores.dropna(subset=['VALOR', *_COLS_ID_BASE])

    valores['FECHA'] = pd.to_datetime(
        dict(year=valores['ANO'], month=valores['MES'], day=valores['DIA']),
        errors='coerce',
    ).dt.date
    valores = valores.dropna(subset=['FECHA'])

    cols_out = ['ESTACION', 'MAGNITUD', 'FECHA', 'HORA', 'VALOR', 'VALIDO'] + id_extra
    largo = valores[cols_out].copy()
    largo['ESTACION'] = largo['ESTACION'].astype(int)
    largo['MAGNITUD'] = largo['MAGNITUD'].astype(int)
    return largo.sort_values(['ESTACION', 'MAGNITUD', 'FECHA', 'HORA']).reset_index(drop=True)


def _a_formato_ingesta(largo: pd.DataFrame, ruta_estaciones: str) -> pd.DataFrame:
    """Enriquece el formato largo canónico con el esquema que espera la BBDD.

    Añade fecha completa (timestamp), calendario, nombre de estación y la
    columna de texto ``validacion`` (V/N), y reordena a las columnas finales.
    """
    df = largo.copy()
    df.columns = [c.lower() for c in df.columns]   # ESTACION -> estacion, etc.

    # Fecha completa con hora (a partir de FECHA date + HORA) y features de calendario
    df['fecha'] = pd.to_datetime(df['fecha']) + pd.to_timedelta(df['hora'], unit='h')
    df['ano'] = df['fecha'].dt.year
    df['mes'] = df['fecha'].dt.month
    df['dia'] = df['fecha'].dt.day
    df['dia_semana'] = df['fecha'].dt.dayofweek
    df['es_fin_semana'] = df['dia_semana'].isin([5, 6])

    # Reconstruimos el flag de texto y el nombre del contaminante
    df['validacion'] = np.where(df['valido'], 'V', 'N')
    df['contaminante'] = df['magnitud'].map(DICCIONARIO_MAGNITUDES)

    # Cruzar el nombre de la estación
    df_est = pd.read_csv(ruta_estaciones, sep=';')
    df_est_temp = df_est[['CODIGO_CORTO', 'ESTACION']].rename(columns={'ESTACION': 'nombre_estacion'})
    df = df.merge(df_est_temp, left_on='estacion', right_on='CODIGO_CORTO')

    columnas_finales = ['provincia', 'municipio', 'estacion', 'magnitud', 'contaminante',
                        'punto_muestreo', 'nombre_estacion', 'ano', 'mes', 'dia', 'hora',
                        'valor', 'validacion', 'fecha', 'dia_semana', 'es_fin_semana']
    return df[columnas_finales].sort_values(['nombre_estacion', 'fecha'])


def limpiar_datos_csv(ruta_input, ruta_estaciones):
    """Limpia un CSV histórico (formato ancho) al esquema de la BBDD."""
    df = pd.read_csv(ruta_input, sep=';')
    largo = wide_a_largo(df, magnitudes=list(MAGNITUDES_OBJETIVO),
                         solo_validas=False, id_extra=_ID_EXTRA_INGESTA)
    return _a_formato_ingesta(largo, ruta_estaciones)


def limpiar_datos_live(data_live, ruta_estaciones):
    """Limpia el DataFrame en tiempo real de la API al esquema de la BBDD."""
    largo = wide_a_largo(data_live, magnitudes=list(MAGNITUDES_OBJETIVO),
                         solo_validas=False, id_extra=_ID_EXTRA_INGESTA)
    return _a_formato_ingesta(largo, ruta_estaciones)
