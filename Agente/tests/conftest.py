"""Fixtures de los tests del agente: sin red, sin claves, sin servicio RAG.

- `LLMFalso` sigue un guion y registra las llamadas.
- El RAG se finge con `httpx.MockTransport`.
"""
import json
import sys
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

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


class RagFingido:
    """Transporte HTTP que imita rag.api y guarda las búsquedas y validaciones pedidas.

    `respuestas`: lo que devuelve cada búsqueda, en orden (la última se repite).
    `/rag/validar` acepta la salida si es un objeto con afirmaciones que citan IDs entregados,
    y la "renderiza" como `texto [Dn]` por afirmación.
    """

    def __init__(self, caido: bool = False, respuestas: list[dict] | None = None):
        self.caido = caido
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
