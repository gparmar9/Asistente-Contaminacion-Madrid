"""Servicio HTTP del RAG: la herramienta que consulta el LLM de la API de chat.

Arranque:
    python -m rag.api                    # uvicorn en 0.0.0.0:${RAG_API_PUERTO:-8010}
    uvicorn rag.api:app --reload         # desarrollo

Endpoints:
    GET  /salud               estado del índice (modelo, fragmentos, commit) y umbral
    GET  /rag/herramienta     definición de la function tool (formato OpenAI) + esquema de salida
    POST /rag/evidencias      {pregunta, k?, tema?} → evidencias D1..Dn, bibliografía, avisos
    POST /rag/validar         {pregunta, evidencias:[{id, chunk_id}], salida} → validación + texto

Flujo esperado desde la API de chat:
    1. el modelo pide la tool `buscar_evidencias(pregunta)` → POST /rag/evidencias;
       la API de chat pasa `para_el_modelo` como resultado de la herramienta;
    2. el modelo devuelve {estado, afirmaciones, limitaciones};
    3. POST /rag/validar con esa salida y los {id, chunk_id} del paso 1. Si
       `valida` es false, reenviar `mensaje_reparacion` al modelo (una vez) y
       repetir; si sigue fallando, mostrar insuficiencia.

Códigos HTTP: 422 entrada inválida (ConsultaInvalida, EvidenciaDesconocida);
503 índice no preparado o construido con otro modelo; 500 corpus inválido.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from rag import buscar as mod_buscar
from rag import embeddings, evidencias
from rag.corpus import RUTA_CORPUS, TEMAS
from rag.errores import (
    ConsultaInvalida,
    CorpusInvalido,
    EvidenciaDesconocida,
    IndiceNoPreparado,
    ModeloNoCoincide,
)

VERSION = "0.2.0"
PUERTO_POR_DEFECTO = 8010
DIRECTORIO_CORPUS: Path = RUTA_CORPUS

HERRAMIENTA: dict = {
    "type": "function",
    "function": {
        "name": "buscar_evidencias",
        "description": (
            "Busca en la documentación revisada del asistente de calidad del aire de Madrid "
            "(efectos en la salud de NO2, ozono y partículas; guías OMS y límites legales; "
            "protocolo de episodios del Ayuntamiento; estaciones y zonas; glosario del proyecto). "
            "Devuelve fragmentos numerados D1..Dn; cada afirmación de la respuesta debe citar "
            "los IDs que la respaldan. No contiene mediciones actuales ni avisos en tiempo real."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "pregunta": {"type": "string", "description": "Pregunta en español, tal como la formula el usuario"},
                "tema": {
                    "type": "string",
                    "enum": list(TEMAS),
                    "description": "Filtro opcional: salud, normativa o proyecto",
                },
            },
            "required": ["pregunta"],
        },
    },
}

app = FastAPI(
    title="RAG documental · calidad del aire de Madrid",
    version=VERSION,
    description="Herramienta de recuperación con citas verificables para un LLM externo.",
)


# --------------------------------------------------------------------------- modelos

class PeticionEvidencias(BaseModel):
    pregunta: str = Field(..., examples=["¿qué son los bloques del día?"])
    k: int = Field(mod_buscar.K_POR_DEFECTO, ge=1, le=mod_buscar.K_MAXIMO)
    tema: str | None = Field(None, examples=["salud"])


class ReferenciaEvidencia(BaseModel):
    id: str = Field(..., examples=["D1"])
    chunk_id: str = Field(..., examples=["glosario_magnitudes:bloques-del-dia:0"])


class PeticionValidar(BaseModel):
    pregunta: str
    evidencias: list[ReferenciaEvidencia]
    # Se acepta cualquier JSON: si no cumple el contrato, la respuesta lo explica con
    # `valida=false` y `errores` para que la API de chat pida una reparación al modelo.
    salida: Any


# --------------------------------------------------------------------------- errores → HTTP

def _handler(status: int):
    async def handler(_request: Request, exc: Exception):
        return JSONResponse(status_code=status, content={"error": type(exc).__name__, "detalle": str(exc)})
    return handler


for _exc, _status in (
    (ConsultaInvalida, 422),
    (EvidenciaDesconocida, 422),
    (IndiceNoPreparado, 503),
    (ModeloNoCoincide, 503),
    (CorpusInvalido, 500),
):
    app.add_exception_handler(_exc, _handler(_status))


# --------------------------------------------------------------------------- rutas

def _buscar(*args, **kwargs):
    """Indirección para poder sustituir la búsqueda en las pruebas."""
    return mod_buscar.buscar(*args, **kwargs)


def _resumen_indice() -> dict:
    coleccion = embeddings.obtener_coleccion()
    meta = dict(coleccion.metadata or {})
    return {
        "modelo_embeddings": meta.get("modelo_embeddings"),
        "modelo_configurado": embeddings.MODELO_EMBEDDINGS,
        "commit": meta.get("commit"),
        "fecha_indexado": meta.get("fecha_indexado"),
        "n_fragmentos": coleccion.count(),
    }


@app.get("/salud")
def salud() -> dict:
    indice = _resumen_indice()
    return {
        "estado": "ok" if indice["modelo_embeddings"] == indice["modelo_configurado"] else "modelo_no_coincide",
        "version": VERSION,
        "umbral_distancia": evidencias.umbral_distancia(),
        "indice": indice,
    }


@app.get("/rag/herramienta")
def herramienta() -> dict:
    return {"herramienta": HERRAMIENTA, "esquema_salida": evidencias.ESQUEMA_SALIDA}


@app.post("/rag/evidencias")
def evidencias_endpoint(peticion: PeticionEvidencias) -> dict:
    r = evidencias.recuperar(
        peticion.pregunta, k=peticion.k, tema=peticion.tema,
        buscar=_buscar, directorio_corpus=DIRECTORIO_CORPUS,
    )
    d = r.a_dict()
    d["para_el_modelo"] = r.para_el_modelo()
    return d


@app.post("/rag/validar")
def validar_endpoint(peticion: PeticionValidar) -> dict:
    refs = [e.model_dump() for e in peticion.evidencias]
    evs = evidencias.resolver_evidencias(refs, DIRECTORIO_CORPUS)
    r = evidencias.validar_y_renderizar(peticion.pregunta, evs, peticion.salida, DIRECTORIO_CORPUS)
    return r.a_dict()


def main() -> None:
    import uvicorn

    puerto = int(os.getenv("RAG_API_PUERTO") or PUERTO_POR_DEFECTO)
    uvicorn.run("rag.api:app", host="0.0.0.0", port=puerto)


if __name__ == "__main__":
    main()
