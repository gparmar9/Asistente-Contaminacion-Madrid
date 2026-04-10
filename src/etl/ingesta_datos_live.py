# Librerias
import pandas as pd
import numpy as np
import os
import requests

from limpiar_datos import limpiar_datos_live

url = "https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_tiemporeal.json?pageSize=5000"
archivo_estaciones = '././data/raw/estaciones-de-control.csv'

# Hacer la petición GET a la URL
response = requests.get(url)
response.raise_for_status()  # Verificar que la petición fue exitosa
print('Petición realizada con éxito. Código de estado:', response.status_code)

if response.status_code == 200:
    # Convertir la respuesta JSON a un DataFrame de pandas
    data = response.json()
    df = pd.DataFrame(data['records'])

    df_limpio = limpiar_datos_live(df, archivo_estaciones)
    
    # Guardar fichero
    os.makedirs('././data/processed', exist_ok=True)
    df_limpio.to_csv('././data/processed/aire_madrid_live.csv', index=False)
    print('Funciona')
else:
    print('Error: No se pudo obtener la información. Código de estado:', response.status_code)