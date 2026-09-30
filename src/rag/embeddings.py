"""Modelo de embeddings, recuento de tokens y acceso a la colección ChromaDB.

Un solo modelo y un solo motor (principio "un motor, un modelo, un proveedor").
La ingesta (`indexar.py`) y la búsqueda (`buscar.py`) comparten estos ajustes para
no desincronizarse; además el índice guarda el nombre del modelo con el que se
construyó y `buscar` se niega a consultar si no coincide con el configurado.

Modelo por defecto: `intfloat/multilingual-e5-base` (multilingüe, 512 tokens, 768
dimensiones). Los modelos e5 exigen anteponer "query: " a las consultas y
"passage: " a los pasajes; este módulo lo hace por su cuenta cuando el nombre del
modelo contiene "e5".

Variables de entorno opcionales:
    RAG_MODELO_EMBEDDINGS   nombre del modelo (cambiarlo obliga a reindexar)
    RAG_RUTA_CHROMA         directorio del índice (por defecto data/chroma)

Las importaciones de torch y chromadb son perezosas para que importar el paquete
sea barato y las pruebas que no los necesitan puedan saltarse limpiamente.
"""
from __future__ import annotations

import os
from pathlib import Path

from rag.errores import IndiceNoPreparado

BASE_DIR = Path(__file__).resolve().parents[2]
RUTA_CHROMA = Path(os.getenv("RAG_RUTA_CHROMA") or BASE_DIR / "data" / "chroma")
NOMBRE_COLECCION = "corpus_rag"
MODELO_EMBEDDINGS = os.getenv("RAG_MODELO_EMBEDDINGS") or "intfloat/multilingual-e5-base"

# Tokens que se reservan por debajo del máximo del modelo, por si el tokenizer
# del modelo cuenta algo distinto de lo que se estimó al trocear.
MARGEN_TOKENS = 8

_ES_E5 = "e5" in MODELO_EMBEDDINGS.lower()
PREFIJO_PASAJE = "passage: " if _ES_E5 else ""
PREFIJO_CONSULTA = "query: " if _ES_E5 else ""

_modelo = None


# --------------------------------------------------------------------------- modelo

def cargar_modelo():
    """Carga el modelo una sola vez (se cachea en memoria)."""
    global _modelo
    if _modelo is None:
        from sentence_transformers import SentenceTransformer  # importación perezosa (torch)

        try:  # silencia la barra "Loading weights" en la consola
            from transformers.utils import logging as _hf_logging
            _hf_logging.disable_progress_bar()
        except Exception:
            pass
        _modelo = SentenceTransformer(MODELO_EMBEDDINGS)
    return _modelo


def max_tokens_modelo() -> int:
    """Longitud máxima de secuencia que el modelo acepta sin truncar."""
    return int(cargar_modelo().max_seq_length)


def presupuesto_tokens() -> int:
    """Tokens máximos por fragmento (y por consulta): máximo del modelo menos margen."""
    return max_tokens_modelo() - MARGEN_TOKENS


def contar_tokens(texto: str, consulta: bool = False) -> int:
    """Cuenta tokens tal y como los verá el modelo: con prefijo e5 y tokens especiales.

    Se cuenta con `modelo.tokenizer`, nunca por palabras o caracteres.
    """
    prefijo = PREFIJO_CONSULTA if consulta else PREFIJO_PASAJE
    tokenizer = cargar_modelo().tokenizer
    # verbose=False: contar un texto más largo que el máximo es normal al trocear y no
    # debe emitir el aviso "sequence length is longer than..." del tokenizer.
    return len(tokenizer(prefijo + texto, add_special_tokens=True, truncation=False,
                         verbose=False)["input_ids"])


def _encode(textos: list[str], prefijo: str) -> list[list[float]]:
    modelo = cargar_modelo()
    return modelo.encode(
        [prefijo + t for t in textos],
        normalize_embeddings=True,
        convert_to_numpy=True,
        batch_size=16,
    ).tolist()


def embed_pasajes(textos: list[str]) -> list[list[float]]:
    """Vectoriza fragmentos del corpus (vectores normalizados, aptos para coseno)."""
    return _encode(list(textos), PREFIJO_PASAJE)


def embed_consulta(texto: str) -> list[float]:
    """Vectoriza una consulta del usuario."""
    return _encode([texto], PREFIJO_CONSULTA)[0]


# --------------------------------------------------------------------------- chroma

def cliente(ruta: Path | None = None):
    """Cliente Chroma persistente sobre `ruta` (por defecto RUTA_CHROMA)."""
    import chromadb  # importación perezosa

    return chromadb.PersistentClient(path=str(ruta or RUTA_CHROMA))


def obtener_coleccion(ruta: Path | None = None):
    """Devuelve la colección existente. Lanza IndiceNoPreparado si no hay índice."""
    ruta = Path(ruta or RUTA_CHROMA)
    if not ruta.exists():
        raise IndiceNoPreparado(
            f"No existe el índice en {ruta}. Ejecuta `python -m rag.indexar`."
        )
    try:
        return cliente(ruta).get_collection(NOMBRE_COLECCION)
    except Exception as e:  # Chroma cambia el tipo de excepción entre versiones
        raise IndiceNoPreparado(
            f"No existe la colección '{NOMBRE_COLECCION}' en {ruta}. "
            f"Ejecuta `python -m rag.indexar`. ({type(e).__name__}: {e})"
        ) from e


def recrear_coleccion(metadatos: dict, ruta: Path | None = None):
    """Borra la colección anterior (si existe) y crea una nueva con `metadatos`.

    Llamar solo cuando los embeddings nuevos ya estén calculados: así, si el
    cálculo falla, el índice anterior sigue intacto.
    """
    c = cliente(ruta)
    try:
        c.delete_collection(NOMBRE_COLECCION)
    except Exception:
        pass  # aún no existía
    return c.create_collection(
        name=NOMBRE_COLECCION,
        metadata={"hnsw:space": "cosine", **metadatos},
    )
