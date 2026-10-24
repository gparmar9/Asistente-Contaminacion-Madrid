"""La lógica del servicio: del día pedido al bundle correcto y a las filas predichas.

Las dos reglas que NO deben decidirse fuera de aquí (ni en el agente ni en el LLM):

- **Ancla** = el último día con los cuatro bloques presentes. `predecir_manana` toma el
  máximo de FECHA a ciegas: un día a medio ingerir (solo madrugada/mañana) desplazaría
  los horizontes en silencio. Por eso la ventana se corta en el ancla antes de inferir.
- **Horizonte** = `fecha_objetivo − ancla`, y con él se elige el bundle (h1 o h2).
  `<= 0` → ya está medido (consúltense los datos reales); `> max` → fuera de alcance.
"""
from __future__ import annotations

import unicodedata
from datetime import date
from pathlib import Path
from typing import Callable

import pandas as pd

from prediccion.errores import DatosNoDisponibles, ModelosNoDisponibles
from inferencia_prediccion import cargar_bundle, predecir_manana  # src/etl (sys.path)
from features_bloques import ORDEN_BLOQUES  # src/etl (sys.path)

RUTA_MODELOS = Path(__file__).resolve().parents[2] / "models"
FICHEROS = {1: "prediccion_bloques.joblib", 2: "prediccion_bloques_h2.joblib"}
# Ventana de lectura: los lags necesitan 13 días (max(LAGS)+móvil de 7); margen para huecos.
VENTANA_DIAS = 20

ESTADO_OK = "ok"
ESTADO_YA_MEDIDO = "ya_medido"
ESTADO_FUERA_DE_ALCANCE = "fuera_de_alcance"
ESTADO_SIN_DATOS = "sin_datos"

_COLUMNAS_INFERENCIA = ["ESTACION", "MAGNITUD", "FECHA", "BLOQUE", "MEDIA", "COBERTURA"]


def cargar_bundles(directorio: Path = RUTA_MODELOS) -> dict[int, dict]:
    """Los bundles por horizonte, validando que cada uno declara el horizonte esperado."""
    bundles: dict[int, dict] = {}
    for horizonte, nombre in FICHEROS.items():
        ruta = directorio / nombre
        try:
            bundle = cargar_bundle(ruta)
        except Exception as exc:
            raise ModelosNoDisponibles(f"No se pudo cargar {ruta.name}: {exc}") from exc
        declarado = int(bundle.get("horizonte", -1))
        if declarado != horizonte:
            raise ModelosNoDisponibles(
                f"{ruta.name} declara horizonte {declarado}, se esperaba {horizonte}")
        bundles[horizonte] = bundle
    return bundles


def ultimo_dia_completo(bloques: pd.DataFrame, cobertura_min: float = 0.7) -> pd.Timestamp | None:
    """La última fecha cuyos cuatro bloques están medidos de forma fiable (cobertura >= umbral).
    Exigir cobertura, y no solo presencia, evita anclar en un día a medio ingerir: con ingesta en
    tiempo real el bloque en curso aparece con una o dos horas; sin este filtro el día contaría como
    completo antes de tiempo y «esta noche» saldría como «ya medido»."""
    fiables = bloques[bloques["COBERTURA"] >= cobertura_min]
    por_dia = fiables.groupby("FECHA")["BLOQUE"].nunique()
    completos = por_dia[por_dia == len(ORDEN_BLOQUES)]
    return None if completos.empty else completos.index.max()


def _normalizar(texto: str) -> str:
    texto = unicodedata.normalize("NFKD", texto.lower().strip())
    return "".join(c for c in texto if not unicodedata.combining(c))


class Motor:
    """`leer_bloques`: callable(dias) -> DataFrame con ESTACION, NOMBRE_ESTACION, MAGNITUD,
    FECHA, BLOQUE, MEDIA y COBERTURA (en producción, `datos.leer_bloques_recientes`)."""

    def __init__(self, bundles: dict[int, dict], leer_bloques: Callable[[int], pd.DataFrame],
                 ventana_dias: int = VENTANA_DIAS):
        self._bundles = bundles
        self._leer = leer_bloques
        self._ventana = ventana_dias
        self._cobertura_min = min(b.get("cobertura_min", 0.7) for b in bundles.values())

    def salud(self) -> dict:
        ancla = ultimo_dia_completo(self._leer_ventana(), self._cobertura_min)
        return {
            "horizontes": sorted(self._bundles),
            "bundles": {h: {"configuracion": b.get("configuracion", {}).get("nombre"),
                            "fecha_corte_train": b.get("fecha_corte_train")}
                        for h, b in self._bundles.items()},
            "ultima_fecha_completa": str(ancla.date()),
            "predecible_hasta": str((ancla + pd.Timedelta(days=max(self._bundles))).date()),
        }

    def predecir(self, fecha_objetivo: date, estacion: int | str | None = None,
                 contaminante: str | None = None, bloque: str | None = None) -> dict:
        bloques = self._leer_ventana()
        ancla = ultimo_dia_completo(bloques, self._cobertura_min)
        objetivo = pd.Timestamp(fecha_objetivo)
        horizonte = (objetivo - ancla).days
        base = {"fecha_objetivo": str(objetivo.date()), "ultima_fecha_completa": str(ancla.date())}

        if horizonte <= 0:
            return {**base, "estado": ESTADO_YA_MEDIDO,
                    "detalle": "Esa fecha ya está medida: consúltense los datos reales."}
        if horizonte not in self._bundles:
            hasta = ancla + pd.Timedelta(days=max(self._bundles))
            return {**base, "estado": ESTADO_FUERA_DE_ALCANCE, "predecible_hasta": str(hasta.date()),
                    "detalle": f"El modelo llega hasta el {hasta.date()}."}

        vista = bloques.loc[bloques["FECHA"] <= ancla, _COLUMNAS_INFERENCIA]
        pred = predecir_manana(vista, self._bundles[horizonte])  # baseline: la del bundle
        pred = pred[pred["PREDICCION"].notna()]
        pred = self._filtrar(pred, bloques, estacion, contaminante, bloque)
        if pred.empty:
            return {**base, "estado": ESTADO_SIN_DATOS, "horizonte": horizonte,
                    "detalle": "Sin datos suficientes para esa combinación."}

        nombres = (bloques.drop_duplicates("ESTACION").set_index("ESTACION")["NOMBRE_ESTACION"]
                   if "NOMBRE_ESTACION" in bloques.columns else pd.Series(dtype=object))
        pred = pred.assign(ORDEN=pred["BLOQUE"].map(ORDEN_BLOQUES.index)).sort_values(
            ["ESTACION", "CONTAMINANTE", "ORDEN"])
        filas = [{
            "estacion": int(f.ESTACION),
            "nombre_estacion": nombres.get(f.ESTACION),
            "contaminante": f.CONTAMINANTE,
            "bloque": f.BLOQUE,
            "prediccion": round(float(f.PREDICCION), 1),
        } for f in pred.itertuples()]
        return {**base, "estado": ESTADO_OK, "horizonte": horizonte, "unidades": "µg/m³",
                "filas": filas}

    def _leer_ventana(self) -> pd.DataFrame:
        bloques = self._leer(self._ventana)
        if bloques is None or bloques.empty:
            raise DatosNoDisponibles("La vista no devolvió filas")
        if ultimo_dia_completo(bloques, self._cobertura_min) is None:
            raise DatosNoDisponibles("Ningún día completo en la ventana leída")
        return bloques

    def _filtrar(self, pred: pd.DataFrame, bloques: pd.DataFrame, estacion, contaminante,
                 bloque) -> pd.DataFrame:
        if estacion is not None:
            pred = pred[pred["ESTACION"] == self._codigo_estacion(estacion, bloques)]
        if contaminante is not None:
            pred = pred[pred["CONTAMINANTE"] == contaminante]
        if bloque is not None:
            pred = pred[pred["BLOQUE"] == bloque]
        return pred

    def _codigo_estacion(self, estacion: int | str, bloques: pd.DataFrame) -> int:
        """Acepta el código corto o el nombre (sin acentos, sin mayúsculas). Sin
        coincidencia devuelve -1: la predicción sale vacía → `sin_datos`."""
        if isinstance(estacion, int) or (isinstance(estacion, str) and estacion.strip().isdigit()):
            return int(estacion)
        if "NOMBRE_ESTACION" not in bloques.columns:
            return -1
        buscado = _normalizar(str(estacion))
        for codigo, nombre in bloques.drop_duplicates("ESTACION")[
                ["ESTACION", "NOMBRE_ESTACION"]].itertuples(index=False):
            if nombre and _normalizar(str(nombre)) == buscado:
                return int(codigo)
        return -1
