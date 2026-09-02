"""Configuración compartida del RAG: modelo de embeddings local + colección ChromaDB.

La ingesta (`ingesta_vector.py`) y la búsqueda (`buscar.py`) comparten estos ajustes
para no desincronizarse (mismo modelo, misma colección, misma métrica).

Modelo por defecto: `paraphrase-multilingual-MiniLM-L12-v2` — multilingüe (español
incluido), pequeño y sin necesidad de prefijos. Para más calidad puede cambiarse a
`intfloat/multilingual-e5-base`, pero ese exige anteponer "query:"/"passage:" a los
textos, así que habría que ajustar `_encode`.
"""
from pathlib import Path

import chromadb
from sentence_transformers import SentenceTransformer

BASE_DIR = Path(__file__).resolve().parents[2]
RUTA_CHROMA = BASE_DIR / "data" / "chroma"        # índice persistente (ignorado por git)
NOMBRE_COLECCION = "corpus_rag"
MODELO_EMBEDDINGS = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"

_modelo: SentenceTransformer | None = None


def cargar_modelo(nombre: str = MODELO_EMBEDDINGS) -> SentenceTransformer:
    """Carga el modelo una sola vez (se cachea en memoria)."""
    global _modelo
    if _modelo is None:
        _modelo = SentenceTransformer(nombre)
    return _modelo


def embeddings(textos: list[str]) -> list[list[float]]:
    """Vectoriza una lista de textos (vectores normalizados, aptos para coseno)."""
    modelo = cargar_modelo()
    return modelo.encode(
        list(textos), normalize_embeddings=True, convert_to_numpy=True
    ).tolist()


def cargar_coleccion(reset: bool = False):
    """Devuelve la colección ChromaDB persistente (opcionalmente recreada de cero)."""
    cliente = chromadb.PersistentClient(path=str(RUTA_CHROMA))
    if reset:
        try:
            cliente.delete_collection(NOMBRE_COLECCION)
        except Exception:
            pass  # aún no existía
    return cliente.get_or_create_collection(
        name=NOMBRE_COLECCION, metadata={"hnsw:space": "cosine"}
    )
