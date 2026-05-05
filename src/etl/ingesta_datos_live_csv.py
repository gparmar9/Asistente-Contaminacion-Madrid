import os
import pandas as pd
import requests
from dotenv import load_dotenv

from limpiar_datos import limpiar_datos_live

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(BASE_DIR, '..', '..')

url = "https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_tiemporeal.json?pageSize=5000"
archivo_estaciones = os.path.join(PROJECT_ROOT, 'data', 'raw', 'estaciones-de-control.csv')
output_dir = os.path.join(PROJECT_ROOT, 'data', 'processed')
output_file = os.path.join(output_dir, 'calidad_aire_live.csv')

response = requests.get(url)
response.raise_for_status()
print('Petición realizada con éxito. Código de estado:', response.status_code)

data = response.json()
df = pd.DataFrame(data['records'])

df_limpio = limpiar_datos_live(df, archivo_estaciones)

os.makedirs(output_dir, exist_ok=True)

if os.path.exists(output_file):
    df_existente = pd.read_csv(output_file)
    df_combined = pd.concat([df_existente, df_limpio], ignore_index=True)
    df_combined = df_combined.drop_duplicates(subset=['estacion', 'magnitud', 'fecha'])
    df_combined.to_csv(output_file, index=False)
    nuevos = len(df_combined) - len(df_existente)
    print(f'CSV actualizado: {nuevos} registros nuevos añadidos ({len(df_combined)} total)')
else:
    df_limpio.to_csv(output_file, index=False)
    print(f'CSV creado con {len(df_limpio)} registros en {output_file}')
