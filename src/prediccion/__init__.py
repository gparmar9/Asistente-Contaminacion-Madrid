"""Servicio de predicción a corto plazo: la herramienta `PREDICCION` del agente.

Espejo de `src/rag` (servicio HTTP con la herramienta publicada en `/herramienta`).
Las features y la inferencia viven en `src/etl` como módulos planos fuera del
paquete (mismo convenio que notebooks y tests): se añaden aquí a `sys.path`.
"""
import sys
from pathlib import Path

_ETL = str(Path(__file__).resolve().parent.parent / "etl")
if _ETL not in sys.path:
    sys.path.insert(0, _ETL)
