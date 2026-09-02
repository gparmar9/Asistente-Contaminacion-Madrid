"""Ingesta del corpus RAG a ChromaDB.

Trocea `data/rag/*.md`, calcula los embeddings locales de cada fragmento y los
indexa en la colección persistente. Recrea la colección en cada ejecución para que
el índice refleje exactamente el corpus actual (idempotente).

Uso:
    python src/rag/ingesta_vector.py
"""
from embeddings import RUTA_CHROMA, NOMBRE_COLECCION, cargar_coleccion, embeddings
from trocear_corpus import trocear_corpus


def ingestar() -> int:
    """Reconstruye el índice vectorial desde el corpus y devuelve nº de fragmentos."""
    fragmentos = trocear_corpus()
    if not fragmentos:
        print("El corpus no produjo fragmentos. Nada que indexar.")
        return 0

    coleccion = cargar_coleccion(reset=True)
    coleccion.add(
        ids=[f["id"] for f in fragmentos],
        embeddings=embeddings([f["texto"] for f in fragmentos]),
        documents=[f["texto"] for f in fragmentos],
        metadatas=[f["metadatos"] for f in fragmentos],
    )
    return len(fragmentos)


def main():
    n = ingestar()
    print(f"Indexados {n} fragmentos en la colección '{NOMBRE_COLECCION}' ({RUTA_CHROMA}).")


if __name__ == "__main__":
    main()
