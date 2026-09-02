"""Búsqueda semántica sobre el corpus RAG indexado en ChromaDB.

`buscar_documentos` es la base de la futura tool `search_documents` del chatbot
(Fase 3): recibe una consulta en lenguaje natural y devuelve los fragmentos más
relevantes, con opción de filtrar por metadatos (p. ej. solo `tema="salud"`).

Uso rápido por consola:
    python src/rag/buscar.py "¿puedo correr hoy si soy asmático?"
"""
import sys

from embeddings import cargar_coleccion, embeddings


def buscar_documentos(consulta: str, k: int = 4, tema: str | None = None) -> list[dict]:
    """Devuelve los k fragmentos más cercanos a la consulta (opcionalmente por tema)."""
    coleccion = cargar_coleccion()
    resultado = coleccion.query(
        query_embeddings=embeddings([consulta]),
        n_results=k,
        where={"tema": tema} if tema else None,
    )
    return [
        {"texto": doc, "metadatos": meta, "distancia": dist}
        for doc, meta, dist in zip(
            resultado["documents"][0],
            resultado["metadatas"][0],
            resultado["distances"][0],
        )
    ]


def main():
    consulta = " ".join(sys.argv[1:]) or "¿puedo correr hoy si soy asmático?"
    print(f"Consulta: {consulta}\n")
    for r in buscar_documentos(consulta):
        m = r["metadatos"]
        print(f"[{r['distancia']:.3f}] {m['titulo']} — {m['seccion']}  (fuente: {m['fuente']})")


if __name__ == "__main__":
    main()
