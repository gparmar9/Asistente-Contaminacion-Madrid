"""Métricas de negocio del pipeline en formato EMF (Embedded Metric Format) de CloudWatch.

Lambda manda a CloudWatch Logs todo lo que se imprime. Si una línea es un JSON con la clave
`_aws`, CloudWatch extrae de ella las métricas por su cuenta: sin PutMetricData, sin boto3 y
sin permisos nuevos en el rol.

Va aparte de `lambda_handler.py` para probarlo sin AWS: aquel lee SSM y S3 al importarse.
"""
import json

NAMESPACE = "Jupiter/Pipeline"

# Cada nombre es una métrica personalizada (4 de las 10 gratuitas). Sin dimensiones
# ("Dimensions": [[]]): cada combinación de dimensiones sería otra métrica de pago.
METRICAS = [
    ("HorasNuevas", "Count"),          # filas horarias insertadas o corregidas
    ("EstacionesConDatos", "Count"),   # estaciones con alguna medición válida (de 24)
    ("BloquesAnomalos", "Count"),      # bloques marcados por el Isolation Forest
    ("TasaAnomalias", "Percent"),      # BloquesAnomalos / bloques procesados
]


def linea_emf(res: dict, marca_ms: int) -> str:
    """Línea JSON con las cuatro métricas y el detalle de calidad de una ejecución.

    `res` es la salida de `procesar()` o de `resultado_sin_datos()`; `marca_ms`, el instante
    de la ejecución en milisegundos desde 1970 (UTC).
    """
    bloques = res["bloques_upsert"]
    return json.dumps({
        "_aws": {
            "Timestamp": marca_ms,
            "CloudWatchMetrics": [{
                "Namespace": NAMESPACE,
                "Dimensions": [[]],
                "Metrics": [{"Name": nombre, "Unit": unidad} for nombre, unidad in METRICAS],
            }],
        },
        "HorasNuevas": res["horas_nuevas"],
        "EstacionesConDatos": res["estaciones"],
        "BloquesAnomalos": res["anomalias"],
        "TasaAnomalias": round(100 * res["anomalias"] / bloques, 2) if bloques else 0.0,
        # Propiedades sin directiva: no son métricas (no cuestan) y se consultan con Logs Insights.
        "pct_validas": res["pct_validas"],
        "estaciones_sin_datos": res["estaciones_sin_datos"],
        "anomalias_por_magnitud": res["anomalias_por_magnitud"],
    })
