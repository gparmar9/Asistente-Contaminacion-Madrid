"""Constantes de dominio del orquestador.

Duplicadas a propósito respecto a `src/etl/limpiar_datos.py` y a
`ApiUsuario/src/api_usuario/shared/constantes.py`: cada servicio es
autocontenido. Si cambia la lista canónica, actualizar los tres sitios.
"""

# Los 6 contaminantes objetivo, tal como figuran en `resumen_datos_ml.contaminante`
CONTAMINANTES = ("NO", "NO2", "NOx", "O3", "PM10", "PM2.5")

# Código de magnitud de cada contaminante (espejo de MAGNITUDES_OBJETIVO del ETL).
# Las consultas filtran por `magnitud` (entero) para aprovechar el índice
# (estacion, magnitud, fecha) de `resumen_datos_ml`.
MAGNITUD_POR_CONTAMINANTE = {
    "NO": 7, "NO2": 8, "PM2.5": 9, "PM10": 10, "NOx": 12, "O3": 14,
}

# Los 4 bloques horarios del día
BLOQUES = ("madrugada", "manana", "tarde", "noche")
