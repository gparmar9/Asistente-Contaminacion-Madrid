"""Fixtures de los tests del agente: sin red, sin claves, sin servicio RAG ni PostgreSQL.

- `LLMFalso` sigue un guion y registra las llamadas.
- El RAG se finge con `httpx.MockTransport`.
- La vista `mediciones_bloques` es una tabla de SQLite en memoria (`bd_mediciones`).
"""
import json
import sys
from datetime import date, timedelta
from pathlib import Path

import httpx
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool
from openinference.instrumentation import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agente import observabilidad  # noqa: E402  (necesita el sys.path de arriba)

HERRAMIENTA_RAG = {
    "type": "function",
    "function": {
        "name": "buscar_evidencias",
        "description": "Busca en la documentación revisada del asistente.",
        "parameters": {
            "type": "object",
            "properties": {"pregunta": {"type": "string"}, "tema": {"type": "string"}},
            "required": ["pregunta"],
        },
    },
}

EVIDENCIAS_NO2 = {
    "estado": "con_evidencias",
    "evidencias": [
        {"id": "D1", "chunk_id": "salud_no2:efectos:0", "titulo": "Efectos del NO2 en la salud",
         "seccion": "Efectos", "archivo": "salud_no2.md", "tema": "salud", "distancia": 0.12,
         "texto": "El NO2 irrita las vías respiratorias y agrava el asma."},
    ],
    "bibliografia": [
        {"archivo": "salud_no2.md", "titulo": "Efectos del NO2 en la salud", "fecha_revision": "2026-09-01",
         "ids": ["D1"], "fuentes": [{"titulo": "OMS 2021", "organismo": "OMS", "url": "https://who.int"}]},
    ],
    "avisos": {"sanitario": True, "actualidad": False, "textos": ["Aviso: no es consejo médico."]},
    "umbral": 0.1754,
    "descartados": 0,
    "para_el_modelo": {
        "estado": "con_evidencias",
        "evidencias": [{"id": "D1", "titulo": "Efectos del NO2 en la salud", "seccion": "Efectos",
                        "texto": "El NO2 irrita las vías respiratorias y agrava el asma."}],
        "avisos": ["Aviso: no es consejo médico."],
    },
}


EVIDENCIAS_LIMITES = {
    "estado": "con_evidencias",
    "evidencias": [
        {"id": "D1", "chunk_id": "normativa_limites:no2:0", "titulo": "Límites legales", "seccion": "NO2",
         "archivo": "normativa_limites.md", "tema": "normativa", "distancia": 0.10,
         "texto": "El valor límite anual del NO2 es 40 µg/m³."},
    ],
    "bibliografia": [{"archivo": "normativa_limites.md", "titulo": "Límites legales", "ids": ["D1"]}],
    "para_el_modelo": {
        "estado": "con_evidencias",
        "evidencias": [{"id": "D1", "titulo": "Límites legales", "seccion": "NO2",
                        "texto": "El valor límite anual del NO2 es 40 µg/m³."}],
        "avisos": [],
    },
}

SIN_EVIDENCIA = {
    "estado": "sin_evidencia", "evidencias": [], "bibliografia": [],
    "para_el_modelo": {"estado": "sin_evidencia", "evidencias": [], "avisos": []},
}

# Salida documental válida para EVIDENCIAS_NO2 y el texto que "renderiza" el RAG fingido.
SALIDA_NO2 = {"estado": "respondida",
              "afirmaciones": [{"texto": "El NO2 agrava el asma.", "evidencias": ["D1"]}],
              "limitaciones": []}
TEXTO_NO2 = "El NO2 agrava el asma. [D1]"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def spans():
    """Spans terminados en memoria, en lugar de Phoenix o el JSONL."""
    exportador = InMemorySpanExporter()
    proveedor = TracerProvider()
    proveedor.add_span_processor(SimpleSpanProcessor(exportador))
    observabilidad.usar_proveedor(proveedor)
    yield exportador
    observabilidad.usar_proveedor(TracerProvider())


class RagFingido:
    """Transporte HTTP que imita rag.api y guarda las búsquedas y validaciones pedidas.

    `respuestas`: lo que devuelve cada búsqueda, en orden (la última se repite).
    `busqueda_caida`: la herramienta se ofrece, pero cada búsqueda agota el timeout.
    `/rag/validar` acepta la salida si es un objeto con afirmaciones que citan IDs entregados,
    y la "renderiza" como `texto [Dn]` por afirmación.
    """

    def __init__(self, caido: bool = False, respuestas: list[dict] | None = None,
                 busqueda_caida: bool = False):
        self.caido = caido
        self.busqueda_caida = busqueda_caida  # ofrece la herramienta pero /rag/evidencias no responde
        self.respuestas = list(respuestas or [EVIDENCIAS_NO2])
        self.busquedas: list[dict] = []
        self.validaciones: list[dict] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if self.caido:
            raise httpx.ConnectError("conexión rechazada", request=request)
        if request.url.path == "/rag/herramienta":
            return httpx.Response(200, json={"herramienta": HERRAMIENTA_RAG, "esquema_salida": {}})
        if request.url.path == "/rag/evidencias":
            self.busquedas.append(json.loads(request.content))
            if self.busqueda_caida:
                raise httpx.ReadTimeout("sin respuesta", request=request)
            i = min(len(self.busquedas), len(self.respuestas)) - 1
            return httpx.Response(200, json=self.respuestas[i])
        if request.url.path == "/rag/validar":
            peticion = json.loads(request.content)
            self.validaciones.append(peticion)
            return httpx.Response(200, json=_validar(peticion))
        return httpx.Response(404)


def _validar(peticion: dict) -> dict:
    salida, ids = peticion["salida"], {e["id"] for e in peticion["evidencias"]}
    afirmaciones = salida.get("afirmaciones") if isinstance(salida, dict) else None
    if not afirmaciones or any(not set(a["evidencias"]) <= ids for a in afirmaciones):
        return {"valida": False, "mensaje_reparacion": "Corrige el JSON.", "texto": "",
                "aviso_sanitario": False, "bibliografia": []}
    texto = "\n".join(a["texto"] + " " + "".join(f"[{r}]" for r in a["evidencias"]) for a in afirmaciones)
    return {"valida": True, "mensaje_reparacion": "", "texto": texto, "aviso_sanitario": True,
            "bibliografia": [{"titulo": "Efectos del NO2 en la salud"}]}


# ---------------------------------------------------------------------- predicción (servicio fingido)

HERRAMIENTA_PREDICCION = {
    "type": "function",
    "function": {
        "name": "predecir_calidad_aire",
        "description": "Predice la calidad del aire de Madrid a corto plazo.",
        "parameters": {
            "type": "object",
            "properties": {
                "fecha_objetivo": {"type": "string"},
                "estacion": {"type": "string"},
                "contaminante": {"type": "string"},
                "bloque": {"type": "string"},
            },
            "required": ["fecha_objetivo"],
        },
    },
}

_BLOQUES_PRED = ("madrugada", "manana", "tarde", "noche")
_ESTACIONES_PRED = {4: "Plaza de España", 49: "Parque del Retiro"}
_BASE_PRED = {"NO2": 20.0, "PM10": 15.0, "O3": 70.0}


class PrediccionFingida:
    """Transporte HTTP que imita prediccion.api (ml.api) y guarda las peticiones.

    El ancla por defecto es el 2026-04-30 (víspera de `FECHA_REFERENCIA`), así que una fecha
    objetivo de hoy = horizonte 1 y de mañana = 2. `caido`: ni /salud responde (la herramienta
    no se ofrece). `prediccion_caida`: se ofrece, pero cada POST /prediccion agota el timeout.
    """

    def __init__(self, caido: bool = False, prediccion_caida: bool = False,
                 ancla: date = date(2026, 4, 30)):
        self.caido = caido
        self.prediccion_caida = prediccion_caida
        self.ancla = ancla
        self.peticiones: list[dict] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if self.caido:
            raise httpx.ConnectError("conexión rechazada", request=request)
        if request.url.path == "/prediccion/herramienta":
            return httpx.Response(200, json={"herramienta": HERRAMIENTA_PREDICCION})
        if request.url.path == "/salud":
            return httpx.Response(200, json={
                "estado": "ok", "horizontes": [1, 2],
                "ultima_fecha_completa": self.ancla.isoformat(),
                "predecible_hasta": (self.ancla + timedelta(days=2)).isoformat()})
        if request.url.path == "/prediccion":
            peticion = json.loads(request.content)
            self.peticiones.append(peticion)
            if self.prediccion_caida:
                raise httpx.ReadTimeout("sin respuesta", request=request)
            return httpx.Response(200, json=self._predecir(peticion))
        return httpx.Response(404)

    def _predecir(self, p: dict) -> dict:
        objetivo = date.fromisoformat(p["fecha_objetivo"])
        horizonte = (objetivo - self.ancla).days
        base = {"estado": "ok", "fecha_objetivo": p["fecha_objetivo"],
                "ultima_fecha_completa": self.ancla.isoformat()}
        if horizonte <= 0:
            return {**base, "estado": "ya_medido", "detalle": "ya está medido"}
        if horizonte > 2:
            return {**base, "estado": "fuera_de_alcance",
                    "predecible_hasta": (self.ancla + timedelta(days=2)).isoformat()}
        filas = [{"estacion": cod, "nombre_estacion": nom, "contaminante": cont, "bloque": b,
                  "prediccion": round(_BASE_PRED[cont] + horizonte, 1)}
                 for cod, nom in _ESTACIONES_PRED.items() for cont in _BASE_PRED for b in _BLOQUES_PRED]
        filas = [f for f in filas if self._coincide(f, p)]
        if not filas:
            return {**base, "estado": "sin_datos", "detalle": "sin datos"}
        return {**base, "horizonte": horizonte, "unidades": "µg/m³", "filas": filas}

    @staticmethod
    def _coincide(fila: dict, p: dict) -> bool:
        est = p.get("estacion")
        if est is not None and str(est) != str(fila["estacion"]) \
                and str(est).lower() not in fila["nombre_estacion"].lower():
            return False
        if p.get("contaminante") and p["contaminante"] != fila["contaminante"]:
            return False
        if p.get("bloque") and p["bloque"] != fila["bloque"]:
            return False
        return True


# ---------------------------------------------------------------------- mediciones (SQLite)

COLUMNAS_VISTA = ("fecha", "estacion", "nombre_estacion", "distrito", "tipo_estacion", "contaminante", "bloque",
                  "hora_inicio", "hora_fin", "media", "maximo", "minimo", "n_horas", "cobertura", "z_score",
                  "is_anomaly", "dia_semana", "es_fin_semana", "ano", "mes")
ESTACIONES = {56: ("Plaza Elíptica", "Carabanchel"), 49: ("Parque del Retiro", "Retiro"),
              4: ("Plaza de España", "Centro")}
# (fecha, estación, contaminante, media) del bloque `manana`. Con un bloque por día, la media
# diaria ponderada es la del bloque. Último día: 2026-04-30.
MEDIAS = [
    ("2026-04-30", 56, "NO2", 45.0), ("2026-04-30", 49, "NO2", 20.0), ("2026-04-30", 4, "NO2", 38.0),
    ("2026-04-30", 56, "PM10", 25.0), ("2026-04-30", 49, "PM10", 12.0), ("2026-04-30", 4, "PM10", 30.0),
    *[(f"2026-04-{d}", 49, "NO2", m) for d, m in zip(range(24, 30), (18.0, 22.0, 25.0, 19.0, 21.0, 24.0))],
    ("2025-01-10", 4, "PM10", 62.0), ("2025-02-11", 4, "PM10", 55.0), ("2025-03-12", 4, "PM10", 41.0),
    ("2025-11-20", 4, "PM10", 51.5),
]
FECHA_REFERENCIA = date(2026, 5, 1)


@pytest.fixture
def bd_mediciones():
    eng = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    with eng.begin() as conn:
        conn.execute(text(f"CREATE TABLE mediciones_bloques ({', '.join(COLUMNAS_VISTA)})"))
        conn.execute(text(f"INSERT INTO mediciones_bloques VALUES ({', '.join(':' + c for c in COLUMNAS_VISTA)})"),
                     [_fila(*m) for m in MEDIAS])
    return eng


def _fila(fecha: str, estacion: int, contaminante: str, media: float) -> dict:
    dia = date.fromisoformat(fecha)
    nombre, distrito = ESTACIONES[estacion]
    return {"fecha": fecha, "estacion": estacion, "nombre_estacion": nombre, "distrito": distrito,
            "tipo_estacion": "Urbana tráfico", "contaminante": contaminante, "bloque": "manana",
            "hora_inicio": 7, "hora_fin": 12, "media": media, "maximo": media + 10, "minimo": media - 5,
            "n_horas": 6, "cobertura": 1.0, "z_score": 0.5, "is_anomaly": False, "dia_semana": dia.weekday(),
            "es_fin_semana": dia.weekday() >= 5, "ano": dia.year, "mes": dia.month}


def herramienta_datos(engine, guion_sql: list, max_filas: int = 60):
    """`consultar_datos` sobre SQLite con un redactor falso que escribe el SQL del guion."""
    from agente.business.redactor_sql import RedactorSQL
    from agente.datos.mediciones import Mediciones
    from agente.llm.falso import LLMFalso
    from agente.tools.sql_libre import HerramientaDatos

    llm_sql = LLMFalso(guion=list(guion_sql))
    return llm_sql, HerramientaDatos(Mediciones(engine), RedactorSQL(llm_sql, max_filas), FECHA_REFERENCIA,
                                     max_filas)
