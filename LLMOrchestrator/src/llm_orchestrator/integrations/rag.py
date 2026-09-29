"""Puente con el RAG de la Fase 2 (`src/rag` del repositorio).

La importación es perezosa y ocurre aquí dentro por dos motivos:
1. `src/rag` usa imports planos (`from embeddings import ...`), así que hay que
   añadir su carpeta a `sys.path` antes de importarlo.
2. Arrastra torch + sentence-transformers (~470 MB): los tests y el arranque
   del servicio no deben pagarlo; solo la primera búsqueda real.
"""
import sys
from pathlib import Path

# LLMOrchestrator/src/llm_orchestrator/integrations/rag.py -> raíz del repositorio
_RUTA_RAG = Path(__file__).resolve().parents[4] / "src" / "rag"


def buscar_en_corpus(consulta: str, k: int = 4, tema: str | None = None) -> list[dict]:
    """Llama al `buscar_documentos` de la Fase 2 (única fuente de verdad del RAG)."""
    ruta = str(_RUTA_RAG)
    if ruta not in sys.path:
        sys.path.insert(0, ruta)
    from buscar import buscar_documentos  # import perezoso: torch/chromadb

    return buscar_documentos(consulta, k=k, tema=tema)
