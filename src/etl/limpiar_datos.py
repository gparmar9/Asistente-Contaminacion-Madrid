# Librerias
import pandas as pd
import numpy as np
import os

# Diccionario de magnitudes
DICCIONARIO_MAGNITUDES = {
    1: 'SO2', 6: 'CO', 7: 'NO', 8: 'NO2', 9: 'PM2.5', 
    10: 'PM10', 12: 'NOx', 14: 'O3', 20: 'TOL', 30: 'BEN',
    35: 'EBE', 37: 'MXY', 38: 'PXY', 39: 'OXY', 42: 'TCH',
    43: 'CH4', 44: 'NMHC', 431: 'MPX'
}

def limpiar_datos(ruta_input, ruta_estaciones):
    
    # Cargar datos con delimitador ; para los CSVs
    df = pd.read_csv(ruta_input, sep=';')
    df_est = pd.read_csv(ruta_estaciones, sep=';')

    # Identificadores de fila que se van a quedar fijas
    id_vars = ['ESTACION', 'MAGNITUD', 'ANO', 'MES', 'DIA']
    
    # Columnas que queremos despivotar
    cols_h = [f'H{str(i).zfill(2)}' for i in range(1, 25)]
    cols_v = [f'V{str(i).zfill(2)}' for i in range(1, 25)]
    print(cols_h)
    print(cols_v)

    # Despivotar valores
    df_values = df.melt(id_vars=id_vars, value_vars=cols_h, var_name='hora_raw', value_name='valor')
    print('HORAS')
    print(df_values.head())
    
    # Despivotar validaciones
    df_valid = df.melt(id_vars=id_vars, value_vars=cols_v, var_name='val_raw', value_name='es_valido')
    print('\n')
    print('VALIDACIONES')
    print(df_valid.head())
    
    # Crear dataframe final y unir columna de valicación
    df_final = df_values.copy()
    df_final['es_valido'] = df_valid['es_valido']
    print('\n')
    print('FINAl')
    print(df_final)
    
    # Limpiar hora para tener directamente 00, 01, 02.. 21, 22, 23 etc. en las horas
    df_final['hora'] = df_final['hora_raw'].str.replace('H', '').astype(int) - 1
    
    # Formatear nueva columna fecha con estructura: DD/MM/AAAA HH:mm:SS
    df_final['fecha_hora'] = pd.to_datetime({
        'year': df_final['ANO'],
        'month': df_final['MES'],
        'day': df_final['DIA'],
        'hour': df_final['hora']
    })

    # Columnas con información importante para el entrenamiento
    df_final['hora'] = df_final['fecha_hora'].dt.hour
    df_final['dia_semana'] = df_final['fecha_hora'].dt.dayofweek
    df_final['mes'] = df_final['fecha_hora'].dt.month
    df_final['es_fin_semana'] = df_final['dia_semana'].isin([5, 6])

    # Nos quedamos solo con los datos válidos para no entrenar el modelo con errores (estoy hay que ver si nos interesa mantenerlo)
    df_final = df_final[df_final['es_valido'] == 'V'].copy()

    # Mapear nombres de magnitudes
    df_final['contaminante'] = df_final['MAGNITUD'].map(DICCIONARIO_MAGNITUDES)
    print('CONTAMINANTES')
    print(df_final.head())
    
    # Cruzar datos de estaciones
    df_est_temp = df_est[['CODIGO_CORTO', 'ESTACION']].rename(columns={'ESTACION': 'nombre_estacion'})
    print('\n')
    print(df_est_temp.head())

    df_final = df_final.merge(df_est_temp, left_on='ESTACION', right_on='CODIGO_CORTO')
    print('\n')
    print(df_final.head())

    # Seleccionar columnas finales
    columnas_finales = ['fecha_hora', 'hora', 'dia_semana', 'mes', 'es_fin_semana', 'nombre_estacion', 'contaminante', 'valor']
    df_final = df_final[columnas_finales].sort_values(['nombre_estacion', 'fecha_hora'])
    print('\n')
    print(df_final.head())

    return df_final

if __name__ == '__main__':

    archivo_historico = '././data/raw/calidad-aire-horario.csv'
    archivo_estaciones = '././data/raw/estaciones-de-control.csv'
    
    if os.path.exists(archivo_historico):
        df_limpio = limpiar_datos(archivo_historico, archivo_estaciones)
        
        # Guardar fichero
        os.makedirs('././data/processed', exist_ok=True)
        df_limpio.to_csv('././data/processed/aire_madrid_maestro.csv', index=False)
        print('Funciona')
    else:
        print('Error: Path no existe')